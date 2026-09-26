"""Offline provider boundary for actual one-page price-range responses."""

import json
from urllib.parse import parse_qs, urlsplit

import pytest

from src.active_refresh import RefreshIncomplete
from src.refresh_transport import RefreshTransport
from tests.test_refresh_transport import Session, proof


def card_text(key="one", price=500000):
    br = "\\\\\n\\\\\n"
    return (
        f"[${price:,}Active{br}**1 Main Street** **York, ME 03909**{br}"
        f"Brought to you by Fixture Office](https://mainelistings.com/listings/{key})"
    )


def transport_for(tmp_path, text, reserve=None):
    session = Session({"markdown": text, "metadata": {"statusCode": 200}})
    reserved = []
    transport = RefreshTransport(
        api_key="fixture-key",
        policy_path=proof(tmp_path),
        session=session,
        reserve=reserve or (lambda *args: reserved.append(args)),
        diagnostics_path=tmp_path / "diagnostics",
    )
    assert callable(
        getattr(transport, "summary_range", None)
    ), "Range transport missing"
    return transport, session, reserved


def test_range_filters_are_inclusive_and_reserved_before_io(tmp_path):
    transport, session, reserved = transport_for(tmp_path, "1 Results\n" + card_text())
    page = transport.summary_range("york", 500000, 500000)
    assert (
        page.minimum,
        page.maximum,
        page.page,
        page.total_pages,
        page.total_results,
    ) == (500000, 500000, 1, 1, 1)
    url = session.posts[0]["json"]["url"]
    assert parse_qs(urlsplit(url).query) == {
        "city": ["York"],
        "mls_status": ["Active"],
        "sort_by": ["list_price"],
        "sort_order": ["desc"],
        "page": ["1"],
        "min_list_price": ["500000"],
        "max_list_price": ["500000"],
    }
    assert reserved == [("summary", url)]
    retained = list((tmp_path / "diagnostics").glob("*.json"))
    assert len(retained) == 1
    assert json.loads(retained[0].read_text())["url"] == url


def test_unbounded_zero_has_no_arbitrary_price_cap(tmp_path):
    transport, session, _ = transport_for(tmp_path, "0 Results")
    assert transport.summary_range("york", None, None).listings == []
    query = parse_qs(urlsplit(session.posts[0]["json"]["url"]).query)
    assert "min_list_price" not in query and "max_list_price" not in query


@pytest.mark.parametrize(
    "minimum,maximum",
    [(True, None), (-1, None), (1.5, None), (0, False), (5, 4), (None, "5")],
)
def test_invalid_bounds_do_not_reserve_or_request(tmp_path, minimum, maximum):
    transport, session, reserved = transport_for(tmp_path, "0 Results")
    with pytest.raises((RefreshIncomplete, ValueError)):
        transport.summary_range("york", minimum, maximum)
    assert not session.posts and not reserved


@pytest.mark.parametrize(
    "text",
    [
        "No result evidence",
        "-1 Results",
        "1.5 Results",
        "1,0 Results",
        "1 Results\n2 Results",
        "1 Results",
        "25 Results",
        "0 Results\n1 of 2",
        "0 Results\n2 of 1",
        "1 Results\n1 of 1\n[Next](?page=2)",
        "0 Results\n[Next](?page=2)",
        "0 Results\n[2](?page=2)",
        "0 Results\nPage 1 of unknown",
        "0 Results\n1 of 1\n2 of 2",
        "2 Results\n" + card_text() * 2,
        "1 Results\n" + card_text() + "\nBrought to you by broken card",
    ],
)
def test_ambiguous_or_contradictory_evidence_never_fabricates_single_page(
    tmp_path, text
):
    transport, _, _ = transport_for(tmp_path, text)
    with pytest.raises(RefreshIncomplete):
        transport.summary_range("york", None, None)


def test_multipage_probe_keeps_actual_metadata_and_only_first_page(tmp_path):
    text = "25 Results\n1 of 2\n" + "\n".join(card_text(str(i)) for i in range(24))
    transport, session, reserved = transport_for(tmp_path, text)
    page = transport.summary_range("york", None, None)
    assert page.total_results == 25 and page.total_pages == 2
    assert len(page.listings) == 24
    assert len(session.posts) == len(reserved) == 1


def test_range_budget_failure_precedes_provider_call(tmp_path):
    def reject(*args):
        raise RuntimeError("budget exhausted")

    transport, session, _ = transport_for(tmp_path, "0 Results", reject)
    with pytest.raises(RuntimeError, match="budget exhausted"):
        transport.summary_range("york", None, None)
    assert not session.posts


@pytest.mark.parametrize(
    "pagination",
    ["-1 of 1", "1 of 1.5", "1 of 1\nPage 2 of unknown", "+1 of 1", "1 of -1"],
)
def test_entire_pagination_evidence_must_be_valid(tmp_path, pagination):
    transport, _, _ = transport_for(
        tmp_path, "1 Results\n" + pagination + "\n" + card_text()
    )
    with pytest.raises(RefreshIncomplete):
        transport.summary_range("york", None, None)


@pytest.mark.parametrize(
    "price", ["5,00,000", ",500000", "500,000,", "500000.00", "-500000"]
)
def test_raw_price_is_not_repaired_by_removing_punctuation(tmp_path, price):
    raw = card_text().replace("$500,000", "$" + price)
    transport, _, _ = transport_for(tmp_path, "1 Results\n" + raw)
    with pytest.raises(RefreshIncomplete):
        transport.summary_range("york", None, None)
