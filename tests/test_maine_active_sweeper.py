"""Tests for the withdrawn-listing sweeper."""
from __future__ import annotations

from datetime import datetime, timedelta
import pytest

from src.maine_database import (
    get_connection,
    init_db,
    mark_withdrawn_stale,
    queue_stale_for_verification,
    upsert_listing,
)


@pytest.fixture
def conn(tmp_path):
    c = get_connection(str(tmp_path / 's.db'))
    init_db(c)
    return c


def _insert_with_last_seen(conn, *, url, status, days_ago,
                           verification_failed=False):
    """Insert a listing with a backdated last_seen_at.

    `verification_failed=True` simulates a listing whose detail page we tried
    to read and could not — the only state in which the withdraw fallback is
    allowed to act.
    """
    upsert_listing(conn, {
        'detail_url': url, 'status': status, 'list_price': 500_000,
        'address': 'X', 'city': 'Kittery',
    })
    iso = (datetime.utcnow() - timedelta(days=days_ago)).isoformat()
    conn.execute(
        'UPDATE maine_transactions SET last_seen_at = ? WHERE detail_url = ?',
        (iso, url),
    )
    if verification_failed:
        conn.execute(
            "UPDATE maine_transactions SET enrichment_status = 'error', "
            "enrichment_attempts = 2 WHERE detail_url = ?",
            (url,),
        )
    conn.commit()


class TestWithdrawnSweeper:
    def test_stale_listing_is_verified_not_assumed_withdrawn(self, conn):
        """A listing that leaves the Active feed has usually sold, not withdrawn."""
        _insert_with_last_seen(conn, url='/l/stale', status='Active', days_ago=10)
        assert queue_stale_for_verification(conn, stale_days=7) == 1
        row = conn.execute(
            'SELECT status, enrichment_status FROM maine_transactions '
            'WHERE detail_url = ?',
            ('/l/stale',),
        ).fetchone()
        assert row[0] == 'Active', 'status must await detail-page verification'
        assert row[1] is None, 'row must be re-queued for enrichment'

    def test_unverified_listing_is_never_withdrawn(self, conn):
        _insert_with_last_seen(conn, url='/l/stale2', status='Active', days_ago=90)
        assert mark_withdrawn_stale(conn, stale_days=30) == 0

    def test_listing_seen_recently_not_withdrawn(self, conn):
        _insert_with_last_seen(conn, url='/l/fresh', status='Active', days_ago=2)
        marked = mark_withdrawn_stale(conn, stale_days=7)
        assert marked == 0
        row = conn.execute(
            'SELECT status FROM maine_transactions WHERE detail_url = ?',
            ('/l/fresh',),
        ).fetchone()
        assert row[0] == 'Active'

    def test_pending_also_queued_for_verification(self, conn):
        _insert_with_last_seen(conn, url='/l/pending', status='Pending', days_ago=10)
        assert queue_stale_for_verification(conn, stale_days=7) == 1
        row = conn.execute(
            'SELECT enrichment_status FROM maine_transactions WHERE detail_url = ?',
            ('/l/pending',),
        ).fetchone()
        assert row[0] is None

    def test_closed_is_never_swept(self, conn):
        """Closed transactions are archival and must never be marked Withdrawn."""
        conn.execute('''
            INSERT INTO maine_transactions (
                detail_url, status, close_date, last_seen_at,
                discovered_at, scraped_at
            ) VALUES (
                '/l/closed-old', 'Closed', '2020-01-01',
                '2020-01-02', '2020-01-02', '2020-01-02'
            )
        ''')
        conn.commit()
        mark_withdrawn_stale(conn, stale_days=7)
        row = conn.execute(
            "SELECT status FROM maine_transactions WHERE detail_url = '/l/closed-old'"
        ).fetchone()
        assert row[0] == 'Closed'

    def test_boundary_6_days_not_stale(self, conn):
        """6-day-old listing is clearly not stale at stale_days=7."""
        _insert_with_last_seen(conn, url='/l/edge6', status='Active', days_ago=6)
        assert queue_stale_for_verification(conn, stale_days=7) == 0

    def test_boundary_8_days_is_stale(self, conn):
        """8-day-old listing is clearly stale at stale_days=7."""
        _insert_with_last_seen(conn, url='/l/edge8', status='Active', days_ago=8)
        assert queue_stale_for_verification(conn, stale_days=7) == 1

    def test_withdraw_fallback_writes_history_row(self, conn):
        _insert_with_last_seen(conn, url='/l/hist', status='Active', days_ago=90,
                               verification_failed=True)
        assert mark_withdrawn_stale(conn, stale_days=30) == 1
        rows = conn.execute(
            'SELECT status FROM maine_listing_history '
            'WHERE detail_url = ? ORDER BY id ASC',
            ('/l/hist',),
        ).fetchall()
        assert len(rows) == 2
        assert rows[0][0] == 'Active'
        assert rows[1][0] == 'Withdrawn'

    def test_returns_zero_when_nothing_to_sweep(self, conn):
        assert mark_withdrawn_stale(conn, stale_days=7) == 0
