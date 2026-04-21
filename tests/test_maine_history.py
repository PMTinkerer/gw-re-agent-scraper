"""Tests for maine_listing_history change-detected writes."""
from __future__ import annotations

import pytest

from src.maine_database import (
    enrich_listing,
    get_connection,
    init_db,
    upsert_listing,
    write_history_if_changed,
)


@pytest.fixture
def conn(tmp_path):
    c = get_connection(str(tmp_path / 'h.db'))
    init_db(c)
    return c


def _history_rows(conn, url):
    return conn.execute(
        'SELECT status, list_price FROM maine_listing_history '
        'WHERE detail_url = ? ORDER BY id ASC',
        (url,),
    ).fetchall()


class TestWriteHistoryIfChanged:
    def test_baseline_row_on_first_see(self, conn):
        upsert_listing(conn, {
            'detail_url': 'https://mainelistings.com/listings/x1',
            'status': 'Active', 'list_price': 800_000,
            'address': '1 Main', 'city': 'Kittery',
        })
        rows = _history_rows(conn, 'https://mainelistings.com/listings/x1')
        assert len(rows) == 1
        assert rows[0][0] == 'Active'
        assert rows[0][1] == 800_000

    def test_no_row_on_unchanged_upsert(self, conn):
        url = 'https://mainelistings.com/listings/x2'
        upsert_listing(conn, {
            'detail_url': url, 'status': 'Active',
            'list_price': 700_000, 'address': '2 Main', 'city': 'York',
        })
        upsert_listing(conn, {
            'detail_url': url, 'status': 'Active',
            'list_price': 700_000, 'address': '2 Main', 'city': 'York',
        })
        rows = _history_rows(conn, url)
        assert len(rows) == 1, 'no history row should be written if nothing changed'

    def test_row_on_price_change(self, conn):
        url = 'https://mainelistings.com/listings/x3'
        upsert_listing(conn, {
            'detail_url': url, 'status': 'Active',
            'list_price': 900_000, 'address': '3 Main', 'city': 'Wells',
        })
        upsert_listing(conn, {
            'detail_url': url, 'status': 'Active',
            'list_price': 850_000, 'address': '3 Main', 'city': 'Wells',
        })
        rows = _history_rows(conn, url)
        assert len(rows) == 2
        assert rows[0][1] == 900_000
        assert rows[1][1] == 850_000

    def test_row_on_status_transition(self, conn):
        url = 'https://mainelistings.com/listings/x4'
        upsert_listing(conn, {
            'detail_url': url, 'status': 'Active',
            'list_price': 500_000, 'address': '4 Main', 'city': 'Saco',
        })
        upsert_listing(conn, {
            'detail_url': url, 'status': 'Pending',
            'list_price': 500_000, 'address': '4 Main', 'city': 'Saco',
        })
        rows = _history_rows(conn, url)
        assert len(rows) == 2
        assert rows[0][0] == 'Active'
        assert rows[1][0] == 'Pending'

    def test_direct_call_to_helper_works(self, conn):
        """write_history_if_changed is callable directly (used by sweeper)."""
        url = 'https://mainelistings.com/listings/x5'
        upsert_listing(conn, {
            'detail_url': url, 'status': 'Active',
            'list_price': 600_000, 'address': '5 Main', 'city': 'York',
        })
        wrote = write_history_if_changed(conn, url, 'Withdrawn', 600_000)
        assert wrote is True
        rows = _history_rows(conn, url)
        assert len(rows) == 2
        assert rows[1][0] == 'Withdrawn'

    def test_dom_not_in_history_columns(self, conn):
        """days_on_market deliberately excluded from history schema."""
        cols = {r[1] for r in conn.execute(
            'PRAGMA table_info(maine_listing_history)'
        ).fetchall()}
        assert 'days_on_market' not in cols


class TestUpsertStampsLastSeen:
    def test_last_seen_set_on_insert(self, conn):
        url = 'https://mainelistings.com/listings/ls1'
        upsert_listing(conn, {
            'detail_url': url, 'status': 'Active',
            'list_price': 500_000, 'address': 'A', 'city': 'Kittery',
        })
        row = conn.execute(
            'SELECT last_seen_at FROM maine_transactions WHERE detail_url = ?',
            (url,),
        ).fetchone()
        assert row[0] is not None
        assert len(row[0]) > 10

    def test_last_seen_refreshed_on_reupsert(self, conn):
        url = 'https://mainelistings.com/listings/ls2'
        upsert_listing(conn, {
            'detail_url': url, 'status': 'Active', 'list_price': 500_000,
            'address': 'B', 'city': 'Kittery',
        })
        first = conn.execute(
            'SELECT last_seen_at FROM maine_transactions WHERE detail_url = ?',
            (url,),
        ).fetchone()[0]
        import time; time.sleep(0.01)
        upsert_listing(conn, {
            'detail_url': url, 'status': 'Active', 'list_price': 500_000,
            'address': 'B', 'city': 'Kittery',
        })
        second = conn.execute(
            'SELECT last_seen_at FROM maine_transactions WHERE detail_url = ?',
            (url,),
        ).fetchone()[0]
        assert second > first
