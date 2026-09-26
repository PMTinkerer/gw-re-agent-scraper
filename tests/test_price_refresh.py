"""Range collection must preserve the existing atomic publication boundary."""

import hashlib
import inspect
import json
from pathlib import Path

import pytest

from src import active_refresh
from src.active_refresh import RefreshIncomplete, run_active_refresh
from tests import test_active_refresh as baseline

card = baseline.card
refresh_setup = baseline.setup


def require_api():
    assert (
        "fetch_summary_range" in inspect.signature(run_active_refresh).parameters
    ), "Range integration missing"


def page_source(cards, calls):
    def fetch(town, minimum, maximum):
        from src.price_discovery import RangePage

        calls.append((town, minimum, maximum))
        selected = [
            r
            for r in cards
            if (minimum is None or r["list_price"] >= minimum)
            and (maximum is None or r["list_price"] <= maximum)
        ]
        selected.sort(key=lambda r: r["list_price"], reverse=True)
        return RangePage(
            town,
            minimum,
            maximum,
            1,
            max(1, (len(selected) + 23) // 24),
            len(selected),
            selected[:24],
        )

    return fetch


def execute(config, callback, **kwargs):
    require_api()
    return run_active_refresh(
        **config,
        fetch_summary_range=callback,
        fetch_detail=lambda url: dict(
            mls_number=url.rsplit("/", 1)[-1], status="Active", list_date="2026-09-01"
        ),
        fetch_status=lambda url: dict(status="Closed"),
        **kwargs,
    )


def test_completed_ranges_publish_hints_and_reuse_them_fresh(refresh_setup):
    records = [dict(card(str(i)), list_price=100000 + i * 1000) for i in range(50)]
    calls = []
    first = execute(refresh_setup, page_source(records, calls))
    cold = len(calls)
    assert first["price_range_hints"]["schema_version"] == 1
    assert first["discovery_requests"] == {"york": cold}
    first_bounds = first["price_range_hints"]["towns"]["york"]
    calls.clear()
    second = execute(refresh_setup, page_source(records, calls))
    assert len(calls) == len(first_bounds) + 2
    assert second["price_range_hints"] == first["price_range_hints"]
    assert len(calls) < cold
    assert second["active_mls_ids"] == first["active_mls_ids"]


def test_ambiguous_mechanism_rejected_before_io(refresh_setup):
    require_api()

    def forbidden(*a):
        pytest.fail("No discovery callback allowed")

    for kwargs in ({}, {"fetch_summary": forbidden, "fetch_summary_range": forbidden}):
        with pytest.raises(ValueError, match="exactly one"):
            run_active_refresh(
                **refresh_setup,
                fetch_detail=forbidden,
                fetch_status=forbidden,
                **kwargs,
            )


def test_failed_range_keeps_existing_manifest_database_and_hint_bytes(refresh_setup):
    calls = []
    execute(refresh_setup, page_source([card("old"), card("gone")], calls))
    before = {
        k: Path(refresh_setup[k]).read_bytes() for k in ("db_path", "manifest_path")
    }

    def fail(*args):
        raise RefreshIncomplete("range incomplete")

    with pytest.raises(RefreshIncomplete, match="range incomplete"):
        execute(refresh_setup, fail)
    assert all(Path(refresh_setup[k]).read_bytes() == v for k, v in before.items())


def test_failed_hint_manifest_publication_rolls_back_database(
    refresh_setup, monkeypatch
):
    calls = []
    execute(refresh_setup, page_source([card("old"), card("gone")], calls))
    before = {
        k: Path(refresh_setup[k]).read_bytes() for k in ("db_path", "manifest_path")
    }
    replace = active_refresh.os.replace

    def fail(src, dst):
        if Path(dst) == refresh_setup["manifest_path"]:
            raise OSError("manifest persistence failed")
        return replace(src, dst)

    monkeypatch.setattr(active_refresh.os, "replace", fail)
    with pytest.raises(OSError, match="manifest persistence failed"):
        execute(refresh_setup, page_source([dict(card("old"), list_price=600000)], []))
    assert all(Path(refresh_setup[k]).read_bytes() == v for k, v in before.items())


@pytest.mark.parametrize(
    "bad",
    [
        "broken json",
        "[]",
        '{"complete": false}',
        '{"complete": true,"database_sha256":"wrong"}',
    ],
)
def test_bad_prior_manifest_hints_are_not_coverage(refresh_setup, bad):
    require_api()
    refresh_setup["manifest_path"].write_text(bad)
    calls = []
    result = execute(refresh_setup, page_source([card("old")], calls))
    assert result["active_mls_ids"] == ["old"]
    assert len(calls) == 2
    assert (
        result["database_sha256"]
        == hashlib.sha256(refresh_setup["db_path"].read_bytes()).hexdigest()
    )
    assert json.loads(refresh_setup["manifest_path"].read_text()) == result


def test_new_only_detail_enrichment_survives_range_selection(refresh_setup):
    require_api()
    details = []

    def detail(url):
        details.append(url)
        return dict(mls_number="new", status="Active", list_date="2026-09-01")

    records = [card("old"), card("gone"), card("new")]
    for _ in range(3):
        run_active_refresh(
            **refresh_setup,
            fetch_summary_range=page_source(records, []),
            fetch_detail=detail,
            fetch_status=lambda url: pytest.fail("Unexpected status check"),
        )
    assert details == [card("new")["detail_url"]]


def test_captured_prices_through_transport_collector_and_atomic_refresh(refresh_setup):
    """Real parsing/publication with an offline provider document fixture.

    Only identity/price pairs are observed facts. Remaining card/detail fields
    below are explicit synthetic fixtures, never claims about these properties.
    """
    from src.refresh_transport import RefreshTransport
    from tests.test_price_transport import card_text
    from tests.test_refresh_transport import Response, proof
    from urllib.parse import parse_qs, urlsplit

    evidence = json.loads(
        (
            Path(__file__).parents[1]
            / "docs/evidence/wells-price-bands-2026-09-26.json"
        ).read_text()
    )
    observations = [row for band in evidence["bands"] for row in band["rows"]]
    requests, reservations = [], []

    class OfflineProvider:
        def post(self, endpoint, **kwargs):
            url = kwargs["json"]["url"]
            assert reservations[-1] == ("summary", url)
            requests.append(url)
            params = parse_qs(urlsplit(url).query)
            assert params["page"] == ["1"]
            low = int(params["min_list_price"][0]) if "min_list_price" in params else 0
            high = (
                int(params["max_list_price"][0])
                if "max_list_price" in params
                else float("inf")
            )
            rows = sorted(
                (r for r in observations if low <= r[1] <= high),
                key=lambda r: r[1],
                reverse=True,
            )
            total = len(rows)
            markdown = f"{total} Results\n1 of {max(1,(total+23)//24)}\n"
            markdown += "\n".join(
                card_text(str(key), price).replace("York, ME 03909", "Wells, ME 04090")
                for key, price in rows[:24]
            )
            return Response(
                {
                    "success": True,
                    "data": {"markdown": markdown, "metadata": {"statusCode": 200}},
                }
            )

    transport = RefreshTransport(
        api_key="fixture-key",
        policy_path=proof(refresh_setup["db_path"].parent),
        session=OfflineProvider(),
        reserve=lambda *args: reservations.append(args),
        diagnostics_path=refresh_setup["db_path"].parent / "diagnostics",
    )
    refresh_setup["towns"] = ["Wells"]
    result = execute(refresh_setup, transport.summary_range)
    assert len(result["active_mls_ids"]) == 200
    assert "760702704" in result["active_mls_ids"]
    assert result["discovery_requests"] == {"wells": len(requests)}
    assert len(requests) == len(reservations) == 34
    assert (
        len(list((refresh_setup["db_path"].parent / "diagnostics").glob("*.json")))
        == 34
    )
    assert (
        result["database_sha256"]
        == hashlib.sha256(refresh_setup["db_path"].read_bytes()).hexdigest()
    )

    requests.clear()
    result = execute(refresh_setup, transport.summary_range)
    assert len(result["active_mls_ids"]) == 200 and len(requests) == 19
