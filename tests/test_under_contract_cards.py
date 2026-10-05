"""Summary cards labelled "Active Under Contract" (canary 37314283983, 2026-10-05)."""

import pytest

from src.incremental_cards import ActiveCardParseError, parse_active_cards

URL = (
    "https://mainelistings.com/listings/ME/Wells/04090/10376/"
    "1135-bragdon-road-1-wells-me-04090/779845484"
)
BREAK = "\\\\\n\\\\\n"


def card(status):
    return (
        f"$128,000{status}{BREAK}"
        f"**1135 Bragdon Road, \\#1**  **Wells, ME 04090**{BREAK}"
        f"2 Beds{BREAK}1 Bath{BREAK}990 sqft{BREAK}"
        f"Brought to you by RE/MAX Home Connection]({URL})\n"
    )


def test_active_under_contract_card_counts_as_pending():
    [listing] = parse_active_cards(card("Active Under Contract"))
    assert listing["status"] == "Pending"
    assert listing["list_price"] == 128000
    assert listing["detail_url"] == URL
    assert (listing["beds"], listing["baths"], listing["sqft"]) == (2, 1, 990)


@pytest.mark.parametrize("status", ["Active Under Review", "Under Contract"])
def test_other_unknown_statuses_still_fail_closed(status):
    with pytest.raises(ActiveCardParseError):
        parse_active_cards(card(status))
