"""Active result cards in the layout mainelistings.com used from 2026-11-06.

Run 42276461389 stopped on the first summary page because every card had moved
to a new layout (status before price, facts on one line, "street, City" in
bold, an optional "Open House:" line). The saved page is replayed here.
"""

import json
from pathlib import Path

import pytest

from src.active_refresh import RefreshIncomplete
from src.incremental_cards import ActiveCardParseError, parse_active_cards
from tests.test_price_transport import transport_for

FIXTURE = Path(__file__).parent / "fixtures/biddeford-active-cards-2026-11-06.json"
BR = "\\\\\n\\\\\n"
URL = (
    "https://mainelistings.com/listings/ME/Biddeford/04005/10376/"
    "2-main-street-18-501-biddeford-me-04005/813227351"
)


def saved_page():
    return json.loads(FIXTURE.read_text())["markdown"]


def card(status="Active", middle="", price="$1,499,000", rest=None):
    rest = (
        rest
        if rest is not None
        else f"{BR}3 Beds · 4 Baths · 3,235 sqft{BR}**2 Main Street, \\#18-501, Biddeford** "
    )
    return (
        f"[![](https://cdn.photos.sparkplatform.com/me/x-o.jpg){BR}{status}{BR}"
        f"{middle}{price}{rest}{BR}Brought to you by Portside Real Estate Group]({URL})\n"
    )


def test_saved_page_reads_all_24_cards_without_inventing_facts():
    cards = parse_active_cards(saved_page())
    assert len(cards) == 24
    assert len({c["detail_url"] for c in cards}) == 24
    first = cards[0]
    assert first == {
        "status": "Active",
        "sale_price": None,
        "list_price": 5499000,
        "address": "20 Sea Spray Drive",
        "city": "Biddeford",
        "state": "ME",
        "zip": "04005",
        "beds": 3,
        "baths": 5,
        "sqft": 3490,
        "listing_office": "The Aland Realty Group",
        "detail_url": (
            "https://mainelistings.com/listings/ME/Biddeford/04005/10376/"
            "20-sea-spray-drive-biddeford-me-04005/833418009"
        ),
    }
    by_id = {c["detail_url"].rsplit("/", 1)[1]: c for c in cards}
    assert by_id["789942115"]["zip"] == "04006"
    bare = by_id["769622511"]
    assert (bare["beds"], bare["baths"], bare["sqft"]) == (None, None, None)
    assert bare["address"] == "1 St Martins Lane"
    sqft_only = by_id["776798990"]
    assert (sqft_only["beds"], sqft_only["baths"], sqft_only["sqft"]) == (
        None,
        None,
        1280,
    )
    open_house = by_id["813227351"]
    assert open_house["address"] == "2 Main Street, \\#18-501"
    assert open_house["list_price"] == 1499000


def test_saved_page_passes_the_price_range_transport(tmp_path):
    transport, session, reserved = transport_for(tmp_path, saved_page())
    page = transport.summary_range("biddeford", None, None)
    assert (page.total_results, len(page.listings)) == (109, 24)
    assert len(session.posts) == len(reserved) == 1


def test_under_contract_and_open_house_lines_are_understood():
    [listing] = parse_active_cards(
        card("Active Under Contract", f"Open House: Sat. 1:00pm-3:00pm{BR}")
    )
    assert listing["status"] == "Pending"
    assert (listing["beds"], listing["baths"], listing["sqft"]) == (3, 4, 3235)


@pytest.mark.parametrize(
    "text",
    [
        card("Coming Soon"),
        card(middle=f"Price Reduced{BR}"),
        card(rest=f"{BR}3 Beds · 2.5 Baths{BR}**2 Main Street, Biddeford** "),
        card(rest=f"{BR}3 Beds · 3 Beds{BR}**2 Main Street, Biddeford** "),
        card(rest=f"{BR}3 Beds · 4 Baths{BR}**2 Main Street** "),
        card(price="$"),
        card(price="Price on request"),
    ],
)
def test_anything_unrecognised_still_fails_closed(text):
    with pytest.raises(ActiveCardParseError):
        parse_active_cards(text)


def test_a_new_layout_card_that_cannot_be_read_stops_the_page(tmp_path):
    page = saved_page().replace("3 Beds · 5 Baths · 3,490 sqft", "3 Beds · 5½ Baths", 1)
    transport, _, _ = transport_for(tmp_path, page)
    with pytest.raises(RefreshIncomplete):
        transport.summary_range("biddeford", None, None)
