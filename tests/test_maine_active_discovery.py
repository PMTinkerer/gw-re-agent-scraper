"""Tests for active-listings discovery plumbing."""
from __future__ import annotations

from src.maine_parser import parse_search_cards


CLOSED_FIXTURE = r'''
[![Thumb](https://example.com/a.jpg)
$ 750,000 Closed\\ \\
**123 Ocean Ave** **Kittery, ME 03904**\\ \\
3 Beds\\ \\
2 Baths\\ \\
1,800 sqft\\ \\
Brought to you by ACME Realty](https://mainelistings.com/listings/abc-123)
'''

ACTIVE_FIXTURE = r'''
[![Thumb](https://example.com/b.jpg)
$ 875,000 Active\\ \\
**77 Shore Rd** **York, ME 03909**\\ \\
4 Beds\\ \\
3 Baths\\ \\
2,400 sqft\\ \\
Brought to you by Beach Realty](https://mainelistings.com/listings/def-456)
'''

NEW_LISTING_FIXTURE = r'''
[![Thumb](https://example.com/c.jpg)
$ 1,200,000 New Listing\\ \\
**9 Dock St** **Kennebunkport, ME 04046**\\ \\
5 Beds\\ \\
4 Baths\\ \\
3,100 sqft\\ \\
Brought to you by Coastal Homes](https://mainelistings.com/listings/ghi-789)
'''


class TestParseClosedCards:
    def test_closed_status_still_works(self):
        cards = parse_search_cards(CLOSED_FIXTURE, status='Closed')
        assert len(cards) == 1
        c = cards[0]
        assert c['sale_price'] == 750_000
        assert c['status'] == 'Closed'
        assert c['address'] == '123 Ocean Ave'
        assert c['city'] == 'Kittery'
        assert c.get('list_price') is None

    def test_default_status_is_closed_for_back_compat(self):
        """Old callers that don't pass status should still get closed parsing."""
        cards = parse_search_cards(CLOSED_FIXTURE)
        assert len(cards) == 1
        assert cards[0]['sale_price'] == 750_000


class TestParseActiveCards:
    def test_active_card_parsed(self):
        cards = parse_search_cards(ACTIVE_FIXTURE, status='Active')
        assert len(cards) == 1
        c = cards[0]
        assert c['list_price'] == 875_000
        assert c['sale_price'] is None
        assert c['status'] == 'Active'
        assert c['address'] == '77 Shore Rd'
        assert c['city'] == 'York'
        assert c['beds'] == 4
        assert c['baths'] == 3
        assert c['sqft'] == 2_400
        assert c['detail_url'] == 'https://mainelistings.com/listings/def-456'

    def test_new_listing_badge_parses_as_active(self):
        """Some active cards carry a 'New Listing' badge instead of 'Active'."""
        cards = parse_search_cards(NEW_LISTING_FIXTURE, status='Active')
        assert len(cards) == 1
        assert cards[0]['list_price'] == 1_200_000
        assert cards[0]['status'] == 'Active'

    def test_active_parser_ignores_closed_cards(self):
        """When scraping an Active page, Closed cards mixed in are not returned."""
        mixed = CLOSED_FIXTURE + ACTIVE_FIXTURE
        cards = parse_search_cards(mixed, status='Active')
        urls = [c['detail_url'] for c in cards]
        assert 'https://mainelistings.com/listings/def-456' in urls
        assert 'https://mainelistings.com/listings/abc-123' not in urls


class TestURLBuilder:
    def test_url_for_closed(self):
        from src.maine_firecrawl import build_search_url
        url = build_search_url(town='Kittery', page=1, status='Closed')
        assert 'mls_status=Closed' in url
        assert 'city=Kittery' in url

    def test_url_for_active(self):
        from src.maine_firecrawl import build_search_url
        url = build_search_url(town='York', page=1, status='Active')
        assert 'mls_status=Active' in url
        assert 'city=York' in url

    def test_pagination_preserved(self):
        from src.maine_firecrawl import build_search_url
        u1 = build_search_url(town='Wells', page=1, status='Active')
        u3 = build_search_url(town='Wells', page=3, status='Active')
        assert '&page=' not in u1
        assert '&page=3' in u3
