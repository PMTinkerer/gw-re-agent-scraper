import json
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest

from src import incremental_main as cli
from src import open_houses as oh
from src.refresh_budget import BudgetExceeded
from src.refresh_transport import RefreshTransport
from tests.test_incremental_main import configure
from tests.test_refresh_transport import Response, proof

NOW = datetime(2026, 11, 6, 12, 0, tzinfo=timezone.utc)
URL_A = "https://mainelistings.com/listings/ME/York/03909/10376/1-ocean-ave-york-me-03909/800000001"
URL_B = "https://mainelistings.com/listings/ME/York/03909/10376/9-shore-rd-york-me-03909/800000002"


def card(url, price, extra=""):
    return {
        "url": url,
        "text": f"${price:,}\nActive\n1 Ocean Ave\nYork, ME 03909\n4 Beds{extra}",
    }


def page(cards, count=None, records=(), pages=None):
    return json.dumps(
        {
            "results_text": f"{len(cards) if count is None else count:,}",
            "pages_text": pages,
            "cards": cards,
            "records": list(records),
            "fields": [],
        }
    )


def record(listing_id, start, end):
    return {
        "own": {},
        "nested": [
            {
                "open_house_start_time": start,
                "open_house_end_time": end,
                "open_house_type": "Public",
            }
        ],
        "listing": {"id": listing_id, "list_price": 1500000},
    }


def test_search_url_uses_open_house_filter_town_and_price():
    query = parse_qs(
        urlsplit(oh.build_search_url("Old Orchard Beach", 2, 1000000)).query
    )
    assert query["open_house_end_time_after"] == ["now"]
    assert query["city"] == ["Old Orchard Beach"]
    assert query["min_list_price"] == ["1000000"]
    assert query["page"] == ["2"]


def test_page_state_times_attach_to_matching_listing_and_naive_is_eastern():
    result = page(
        [card(URL_A, 1500000), card(URL_B, 2100000)],
        records=[record(800000002, "2026-11-08T11:00:00", "2026-11-08T13:00:00")],
    )
    count, total, cards = oh.parse_open_house_page(result)
    assert (count, total) == (2, 1)
    by_url = {c["detail_url"]: c for c in cards}
    assert by_url[URL_A]["windows"] == []
    assert by_url[URL_B]["windows"] == [
        {
            "start": "2026-11-08T16:00:00+00:00",
            "end": "2026-11-08T18:00:00+00:00",
            "type": "Public",
        }
    ]
    assert by_url[URL_B]["list_price"] == 2100000
    assert by_url[URL_B]["address"] == "1 Ocean Ave"


def test_card_open_house_line_is_kept_as_text():
    result = page([card(URL_A, 1250000, "\nOpen House: Sat 11/8 11am-1pm")])
    _, _, cards = oh.parse_open_house_page(result)
    assert cards[0]["card_open_house_text"] == ["Open House: Sat 11/8 11am-1pm"]


@pytest.mark.parametrize(
    "bad",
    [
        "not json",
        json.dumps({"cards": []}),
        page([], count=3),
    ],
)
def test_untrustworthy_pages_raise(bad):
    with pytest.raises(oh.OpenHousePageError):
        oh.parse_open_house_page(bad)


def test_scan_town_reads_every_page_and_checks_the_count():
    cards = [card(URL_A[:-1] + str(i), 1500000) for i in range(30)]
    seen = []

    def fetch(url):
        number = int(parse_qs(urlsplit(url).query)["page"][0])
        seen.append(number)
        return page(
            cards[(number - 1) * 24 : number * 24], count=30, pages=[str(number), "2"]
        )

    assert len(oh.scan_town(fetch, "York", 1000000)) == 30
    assert seen == [1, 2]


def test_scan_town_rejects_missing_listings():
    with pytest.raises(oh.OpenHousePageError):
        oh.scan_town(lambda url: page([card(URL_A, 1500000)], count=2), "York", 1)


def test_pass_filters_price_and_past_times_and_isolates_failed_towns(tmp_path):
    def fetch(url):
        town = parse_qs(urlsplit(url).query)["city"][0]
        if town == "Wells":
            raise RuntimeError("source down")
        return page(
            [card(URL_A, 999000), card(URL_B, 1300000)],
            records=[
                record(800000002, "2026-11-01T11:00:00", "2026-11-01T13:00:00"),
                record(800000002, "2026-11-08T11:00:00", "2026-11-08T13:00:00"),
            ],
        )

    out = tmp_path / "open_houses.json"
    doc = oh.run_open_house_pass(
        fetch, ["York", "Wells"], run_id="7-1", now=NOW, output_path=out
    )
    assert json.loads(out.read_text()) == doc
    assert doc["towns"]["York"] == {"status": "complete", "count": 1}
    assert doc["towns"]["Wells"] == {"status": "failed", "error": "RuntimeError"}
    [home] = doc["open_houses"]
    assert home["detail_url"] == URL_B and home["city"] == "York"
    assert [w["start"] for w in home["windows"]] == ["2026-11-08T16:00:00+00:00"]


def test_budget_stop_marks_remaining_towns_not_scanned(tmp_path):
    def fetch(url):
        raise BudgetExceeded("Whole-run conservative allowance exhausted")

    doc = oh.run_open_house_pass(
        fetch,
        ["York", "Wells"],
        run_id="7-1",
        now=NOW,
        output_path=tmp_path / "o.json",
        stop_errors=(BudgetExceeded,),
    )
    assert {t["status"] for t in doc["towns"].values()} == {"not_scanned"}
    assert doc["open_houses"] == []


def test_transport_runs_page_script_through_reservation(tmp_path):
    reserved = []

    class Session:
        def get(self, *a, **k):
            return Response({"success": True, "data": {"remainingCredits": 10}})

        def post(self, *args, **kwargs):
            self.payload = kwargs["json"]
            return Response(
                {
                    "success": True,
                    "data": {
                        "rawHtml": "<html></html>",
                        "metadata": {"statusCode": 200},
                        "actions": {
                            "javascriptReturns": [{"type": "string", "value": "{}"}]
                        },
                    },
                }
            )

    session = Session()
    transport = RefreshTransport(
        api_key="fixture-key",
        policy_path=proof(tmp_path),
        reserve=lambda kind, url: reserved.append(kind),
        session=session,
    )
    url = oh.build_search_url("York", 1, 1000000)
    assert transport.open_house_page(url) == "{}"
    assert reserved == ["open_house"]
    assert session.payload["actions"][1]["script"] == oh.OPEN_HOUSE_JS


def _published_run(tmp_path, monkeypatch, open_house_page):
    policy, events = configure(tmp_path, monkeypatch)
    monkeypatch.setattr(
        cli.RefreshTransport, "open_house_page", open_house_page, raising=False
    )
    checkpoints = []
    monkeypatch.setattr(
        cli, "_checkpoint", lambda paths, run: checkpoints.append(paths)
    )

    def run(**kwargs):
        Path("data/accepted").mkdir(exist_ok=True)
        Path("data/accepted/current.json").write_text(
            json.dumps({"manifest_sha256": "a" * 64})
        )
        return {"run_id": "42-1", "active_mls_ids": []}

    monkeypatch.setattr(cli, "run_active_refresh", run)
    cli.main(["--billing-proof", str(policy), "--github-publish"])
    return checkpoints, events


def test_open_houses_publish_after_the_feed_and_reserve_each_request(
    tmp_path, monkeypatch
):
    def fetch(self, url):
        self.reserve("open_house", url)
        return page([card(URL_B, 1300000)])

    checkpoints, events = _published_run(tmp_path, monkeypatch, fetch)
    feed = next(i for i, p in enumerate(checkpoints) if "data/maine_listings.db" in p)
    opened = next(i for i, p in enumerate(checkpoints) if oh.OUTPUT_PATH in p)
    assert opened > feed
    assert "data/active_refresh_usage.db" in checkpoints[opened]
    doc = json.loads(Path(oh.OUTPUT_PATH).read_text())
    assert len(doc["open_houses"]) == 1
    assert all(t["status"] == "complete" for t in doc["towns"].values())


def test_open_house_failure_never_fails_the_published_feed(tmp_path, monkeypatch):
    def fetch(self, url):
        raise RuntimeError("provider down")

    checkpoints, _ = _published_run(tmp_path, monkeypatch, fetch)
    assert any("data/maine_listings.db" in p for p in checkpoints)
    doc = json.loads(Path(oh.OUTPUT_PATH).read_text())
    assert {t["status"] for t in doc["towns"].values()} == {"failed"}
