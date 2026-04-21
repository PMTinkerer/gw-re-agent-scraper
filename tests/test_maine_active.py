"""Tests for src/maine_active.py downstream read helpers."""
from __future__ import annotations

from datetime import datetime, timedelta
import pytest

from src.maine_database import (
    enrich_listing,
    get_connection,
    init_db,
    upsert_listing,
    write_history_if_changed,
)
from src.maine_active import (
    query_active_listings,
    query_listing_history,
    query_new_since,
    query_stale_listings,
)


@pytest.fixture
def active_db(tmp_path):
    """A DB with a realistic mix of Active, Pending, Closed, Withdrawn."""
    c = get_connection(str(tmp_path / 'a.db'))
    init_db(c)

    # Active listing, just appeared today
    upsert_listing(c, {
        'detail_url': '/l/active-1', 'status': 'Active',
        'list_price': 850_000,
        'address': '1 Ocean Ave', 'city': 'Kittery',
    })
    enrich_listing(c, '/l/active-1', {
        'listing_agent': 'Jane Agent', 'listing_agent_email': 'jane@example.com',
        'listing_office': 'Beach Realty', 'list_date': '2026-04-17',
        'year_built': 1987, 'lot_sqft': 15000,
        'description': 'Stunning oceanfront.',
        'photo_url': 'https://p.com/1.jpg', 'status': 'Active',
        'list_price': 850_000, 'days_on_market': 1,
    })

    # Stale active — 80 days on market
    upsert_listing(c, {
        'detail_url': '/l/stale-1', 'status': 'Active',
        'list_price': 1_200_000,
        'address': '2 Shore Rd', 'city': 'York',
    })
    enrich_listing(c, '/l/stale-1', {
        'listing_agent': 'John Seller', 'listing_agent_email': 'john@example.com',
        'listing_office': 'Coastal Homes', 'list_date': '2026-01-27',
        'year_built': 2001, 'status': 'Active',
        'list_price': 1_200_000, 'days_on_market': 80,
    })

    # Pending listing
    upsert_listing(c, {
        'detail_url': '/l/pending-1', 'status': 'Pending',
        'list_price': 650_000,
        'address': '3 Dock St', 'city': 'Kennebunkport',
    })

    # Closed transaction (archival)
    c.execute('''
        INSERT INTO maine_transactions (
            detail_url, status, sale_price, close_date, city,
            discovered_at, scraped_at
        ) VALUES (
            '/l/closed-1', 'Closed', 750000, '2026-01-01', 'Kittery',
            '2026-01-02', '2026-01-02'
        )
    ''')
    c.commit()

    # Withdrawn listing
    upsert_listing(c, {
        'detail_url': '/l/withdrawn-1', 'status': 'Active',
        'list_price': 500_000,
        'address': '4 Hill Rd', 'city': 'Wells',
    })
    c.execute("UPDATE maine_transactions SET status = 'Withdrawn' WHERE detail_url = '/l/withdrawn-1'")
    c.commit()

    return c


class TestQueryActiveListings:
    def test_returns_active_not_closed(self, active_db):
        rows = query_active_listings(active_db)
        urls = {r['detail_url'] for r in rows}
        assert '/l/active-1' in urls
        assert '/l/stale-1' in urls
        assert '/l/closed-1' not in urls
        assert '/l/withdrawn-1' not in urls

    def test_pending_excluded_by_default(self, active_db):
        rows = query_active_listings(active_db)
        urls = {r['detail_url'] for r in rows}
        assert '/l/pending-1' not in urls

    def test_include_pending_true(self, active_db):
        rows = query_active_listings(active_db, include_pending=True)
        urls = {r['detail_url'] for r in rows}
        assert '/l/pending-1' in urls

    def test_town_filter(self, active_db):
        rows = query_active_listings(active_db, towns=['Kittery'])
        urls = {r['detail_url'] for r in rows}
        assert '/l/active-1' in urls
        assert '/l/stale-1' not in urls  # York

    def test_min_days_on_market(self, active_db):
        rows = query_active_listings(active_db, min_days_on_market=60)
        urls = {r['detail_url'] for r in rows}
        assert '/l/stale-1' in urls
        assert '/l/active-1' not in urls  # DOM=1

    def test_returned_row_has_agent_contact(self, active_db):
        rows = query_active_listings(active_db)
        active1 = next(r for r in rows if r['detail_url'] == '/l/active-1')
        assert active1['listing_agent'] == 'Jane Agent'
        assert active1['listing_agent_email'] == 'jane@example.com'
        assert active1['listing_office'] == 'Beach Realty'


class TestQueryListingHistory:
    def test_returns_baseline_row(self, active_db):
        rows = query_listing_history(active_db, '/l/active-1')
        assert len(rows) >= 1
        assert rows[0]['status'] == 'Active'

    def test_withdrawn_listing_has_transition_row(self, active_db):
        write_history_if_changed(active_db, '/l/withdrawn-1', 'Withdrawn', 500_000)
        rows = query_listing_history(active_db, '/l/withdrawn-1')
        statuses = [r['status'] for r in rows]
        assert 'Active' in statuses
        assert 'Withdrawn' in statuses
        assert statuses.index('Active') < statuses.index('Withdrawn')

    def test_returns_empty_for_unknown_url(self, active_db):
        rows = query_listing_history(active_db, '/l/does-not-exist')
        assert rows == []


class TestQueryNewSince:
    def test_returns_listings_with_first_history_after_cutoff(self, active_db):
        yesterday = (datetime.utcnow() - timedelta(days=1)).isoformat()
        rows = query_new_since(active_db, since_iso=yesterday)
        urls = {r['detail_url'] for r in rows}
        assert '/l/active-1' in urls
        assert '/l/stale-1' in urls

    def test_old_listings_excluded(self, active_db):
        future = '2099-01-01T00:00:00'
        rows = query_new_since(active_db, since_iso=future)
        assert rows == []


class TestQueryStaleListings:
    def test_returns_active_with_high_dom(self, active_db):
        rows = query_stale_listings(active_db, min_dom=60)
        urls = {r['detail_url'] for r in rows}
        assert '/l/stale-1' in urls
        assert '/l/active-1' not in urls

    def test_threshold_respected(self, active_db):
        rows = query_stale_listings(active_db, min_dom=90)
        assert all(r['days_on_market'] >= 90 for r in rows)

    def test_closed_never_returned(self, active_db):
        rows = query_stale_listings(active_db, min_dom=0)
        statuses = {r['status'] for r in rows}
        assert 'Closed' not in statuses
