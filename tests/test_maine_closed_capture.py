"""Tests for closed-transaction capture correctness.

Covers three defects that together caused recent closings (and their buyer
agents) to go missing from the leaderboard:

1. The Closed search was sorted by on-market date, not close date, so
   ``--recent-only`` paged through the oldest-listed properties and never
   reached the newest closings.
2. A listing enriched while Active kept ``enrichment_status='success'``
   forever, so when it later flipped to Closed it was never re-enriched and
   ``close_date`` / ``buyer_agent`` stayed NULL.
3. The sweeper marked any stale Active listing ``Withdrawn`` without
   verifying, but the usual reason a listing leaves the Active feed is that
   it sold.
"""
from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from src.maine_database import (
    get_connection,
    init_db,
    get_unenriched,
    mark_withdrawn_stale,
    queue_stale_for_verification,
    queue_withdrawn_for_reverification,
    upsert_listing,
)
from src.maine_firecrawl import build_search_url


@pytest.fixture
def conn(tmp_path):
    c = get_connection(str(tmp_path / 'capture.db'))
    init_db(c)
    return c


def _seen_days_ago(conn, url, days):
    iso = (datetime.utcnow() - timedelta(days=days)).isoformat()
    conn.execute(
        'UPDATE maine_transactions SET last_seen_at = ? WHERE detail_url = ?',
        (iso, url),
    )
    conn.commit()


def _row(conn, url, *cols):
    sql = f"SELECT {', '.join(cols)} FROM maine_transactions WHERE detail_url = ?"
    return conn.execute(sql, (url,)).fetchone()


class TestClosedSearchOrdering:
    """Defect 1: the Closed search must be ordered by close date."""

    def test_closed_search_sorts_by_close_date_desc(self):
        url = build_search_url(town='Wells', page=1, status='Closed')
        assert 'sort_by=close_date' in url
        assert 'sort_order=desc' in url

    def test_active_search_sorts_by_on_market_date_desc(self):
        url = build_search_url(town='Wells', page=1, status='Active')
        assert 'sort_by=on_market_date' in url
        assert 'sort_order=desc' in url

    def test_sort_params_preserved_on_later_pages(self):
        url = build_search_url(town='Wells', page=4, status='Closed')
        assert 'page=4' in url
        assert 'sort_by=close_date' in url
        assert 'sort_order=desc' in url


class TestReEnrichOnStatusChange:
    """Defect 2: a status change must re-open the row for enrichment."""

    def test_status_change_resets_enrichment(self, conn):
        upsert_listing(conn, {
            'detail_url': '/l/1', 'status': 'Active', 'list_price': 500_000,
            'address': '1 Main St', 'city': 'Wells',
        })
        conn.execute(
            "UPDATE maine_transactions SET enrichment_status='success', "
            "enrichment_attempts=1 WHERE detail_url='/l/1'"
        )
        conn.commit()

        upsert_listing(conn, {
            'detail_url': '/l/1', 'status': 'Closed', 'sale_price': 525_000,
            'address': '1 Main St', 'city': 'Wells',
        })

        status, attempts = _row(
            conn, '/l/1', 'enrichment_status', 'enrichment_attempts')
        assert status is None, 'status change must re-queue the row'
        assert attempts == 0

    def test_unchanged_status_preserves_enrichment(self, conn):
        upsert_listing(conn, {
            'detail_url': '/l/2', 'status': 'Active', 'list_price': 400_000,
            'address': '2 Main St', 'city': 'Wells',
        })
        conn.execute(
            "UPDATE maine_transactions SET enrichment_status='success', "
            "enrichment_attempts=1 WHERE detail_url='/l/2'"
        )
        conn.commit()

        upsert_listing(conn, {
            'detail_url': '/l/2', 'status': 'Active', 'list_price': 390_000,
            'address': '2 Main St', 'city': 'Wells',
        })

        status, attempts = _row(
            conn, '/l/2', 'enrichment_status', 'enrichment_attempts')
        assert status == 'success', 'no status change must not burn a credit'
        assert attempts == 1

    def test_requeued_row_appears_in_enrichment_queue(self, conn):
        upsert_listing(conn, {
            'detail_url': '/l/3', 'status': 'Active', 'list_price': 300_000,
            'address': '3 Main St', 'city': 'York',
        })
        conn.execute(
            "UPDATE maine_transactions SET enrichment_status='success', "
            "enrichment_attempts=1 WHERE detail_url='/l/3'"
        )
        conn.commit()
        upsert_listing(conn, {
            'detail_url': '/l/3', 'status': 'Closed', 'sale_price': 310_000,
            'address': '3 Main St', 'city': 'York',
        })

        urls = [r['detail_url'] for r in get_unenriched(conn, batch_size=10)]
        assert '/l/3' in urls


class TestClosedWithoutCloseDateBackfill:
    """Defect 2 backfill: already-corrupted rows must be recoverable."""

    def test_closed_row_missing_close_date_is_queued(self, conn):
        upsert_listing(conn, {
            'detail_url': '/l/4', 'status': 'Closed', 'sale_price': 800_000,
            'address': '4 Micelan Road', 'city': 'York',
        })
        conn.execute(
            "UPDATE maine_transactions SET enrichment_status='success', "
            "enrichment_attempts=1, close_date=NULL WHERE detail_url='/l/4'"
        )
        conn.commit()

        urls = [r['detail_url'] for r in get_unenriched(conn, batch_size=10)]
        assert '/l/4' in urls

    def test_complete_closed_row_is_not_requeued(self, conn):
        upsert_listing(conn, {
            'detail_url': '/l/5', 'status': 'Closed', 'sale_price': 800_000,
            'address': '5 Done St', 'city': 'York',
        })
        conn.execute(
            "UPDATE maine_transactions SET enrichment_status='success', "
            "enrichment_attempts=1, close_date='2026-06-26', "
            "buyer_agent='Erin Lamarche' WHERE detail_url='/l/5'"
        )
        conn.commit()

        urls = [r['detail_url'] for r in get_unenriched(conn, batch_size=10)]
        assert '/l/5' not in urls


class TestSerialEnrichmentTargetsCallerDatabase:
    """Serial enrichment must honour --db, not silently write the default DB."""

    def test_workers_1_writes_to_the_supplied_connection(self, conn, monkeypatch):
        from src import maine_firecrawl

        upsert_listing(conn, {
            'detail_url': '/l/serial', 'status': 'Closed', 'sale_price': 800_000,
            'address': '4 Micelan Road', 'city': 'York',
        })

        monkeypatch.setattr(maine_firecrawl, '_get_client', lambda: object())
        monkeypatch.setattr(
            maine_firecrawl, '_scrape',
            lambda client, url, fmt='markdown': type(
                'R', (), {'actions': {'javascriptReturns': ['{}']}})(),
        )
        monkeypatch.setattr(
            maine_firecrawl, 'parse_detail_response',
            lambda payload: {
                'close_date': '2026-06-26',
                'buyer_agent': 'Erin Lamarche',
                'buyer_office': 'Portside Real Estate Group',
                'status': 'Closed',
            },
        )

        result = maine_firecrawl.enrich_listings(conn, batch_size=5, workers=1)

        assert result['enriched'] == 1
        close_date, buyer = _row(
            conn, '/l/serial', 'close_date', 'buyer_agent')
        assert close_date == '2026-06-26'
        assert buyer == 'Erin Lamarche'

    def test_connection_stays_open_for_the_caller(self, conn, monkeypatch):
        from src import maine_firecrawl

        monkeypatch.setattr(maine_firecrawl, '_get_client', lambda: object())
        maine_firecrawl.enrich_listings(conn, batch_size=5, workers=1)

        # Would raise ProgrammingError if enrichment had closed it.
        conn.execute('SELECT 1').fetchone()


class TestVerifyBeforeWithdrawing:
    """Defect 3: never guess Withdrawn — re-verify against the detail page."""

    def test_stale_active_is_queued_for_verification_not_withdrawn(self, conn):
        upsert_listing(conn, {
            'detail_url': '/l/6', 'status': 'Active', 'list_price': 1_245_000,
            'address': '454 Ocean Avenue', 'city': 'Wells',
        })
        conn.execute(
            "UPDATE maine_transactions SET enrichment_status='success', "
            "enrichment_attempts=1 WHERE detail_url='/l/6'"
        )
        conn.commit()
        _seen_days_ago(conn, '/l/6', 10)

        queued = queue_stale_for_verification(conn, stale_days=7)

        assert queued == 1
        status, enrich, attempts = _row(
            conn, '/l/6', 'status', 'enrichment_status', 'enrichment_attempts')
        assert status == 'Active', 'must not guess Withdrawn before verifying'
        assert enrich is None
        assert attempts == 0

    def test_fresh_active_is_not_queued(self, conn):
        upsert_listing(conn, {
            'detail_url': '/l/7', 'status': 'Active', 'list_price': 600_000,
            'address': '7 Fresh St', 'city': 'Saco',
        })
        conn.execute(
            "UPDATE maine_transactions SET enrichment_status='success' "
            "WHERE detail_url='/l/7'"
        )
        conn.commit()
        _seen_days_ago(conn, '/l/7', 2)

        assert queue_stale_for_verification(conn, stale_days=7) == 0
        enrich, = _row(conn, '/l/7', 'enrichment_status')
        assert enrich == 'success'

    def test_pending_also_queued(self, conn):
        upsert_listing(conn, {
            'detail_url': '/l/8', 'status': 'Pending', 'list_price': 700_000,
            'address': '8 Pending Way', 'city': 'Kittery',
        })
        _seen_days_ago(conn, '/l/8', 10)
        assert queue_stale_for_verification(conn, stale_days=7) == 1

    def test_withdraw_fallback_requires_a_verification_attempt(self, conn):
        """A row never checked against its detail page is never withdrawn."""
        upsert_listing(conn, {
            'detail_url': '/l/9', 'status': 'Active', 'list_price': 500_000,
            'address': '9 Unverified Rd', 'city': 'Wells',
        })
        _seen_days_ago(conn, '/l/9', 90)

        marked = mark_withdrawn_stale(conn, stale_days=30)

        assert marked == 0
        status, = _row(conn, '/l/9', 'status')
        assert status == 'Active'

    def test_withdraw_fallback_applies_after_failed_verification(self, conn):
        """Verified-unreachable and long stale: withdrawing is now evidence-based."""
        upsert_listing(conn, {
            'detail_url': '/l/10', 'status': 'Active', 'list_price': 500_000,
            'address': '10 Gone Rd', 'city': 'Wells',
        })
        conn.execute(
            "UPDATE maine_transactions SET enrichment_status='error', "
            "enrichment_attempts=2 WHERE detail_url='/l/10'"
        )
        conn.commit()
        _seen_days_ago(conn, '/l/10', 90)

        marked = mark_withdrawn_stale(conn, stale_days=30)

        assert marked == 1
        status, = _row(conn, '/l/10', 'status')
        assert status == 'Withdrawn'

    def test_withdrawn_rows_can_be_requeued_for_repair(self, conn):
        """The backlog marked Withdrawn by the old sweeper must be recoverable."""
        upsert_listing(conn, {
            'detail_url': '/l/12', 'status': 'Withdrawn', 'list_price': 1_245_000,
            'address': '454 Ocean Avenue', 'city': 'Wells',
        })
        conn.execute(
            "UPDATE maine_transactions SET enrichment_status='success', "
            "enrichment_attempts=1 WHERE detail_url='/l/12'"
        )
        conn.commit()

        assert queue_withdrawn_for_reverification(conn) == 1

        enrich, attempts = _row(
            conn, '/l/12', 'enrichment_status', 'enrichment_attempts')
        assert enrich is None
        assert attempts == 0
        assert '/l/12' in [r['detail_url'] for r in get_unenriched(conn, batch_size=10)]

    def test_reverification_respects_limit(self, conn):
        for i in (20, 21, 22):
            upsert_listing(conn, {
                'detail_url': f'/l/{i}', 'status': 'Withdrawn',
                'list_price': 500_000, 'address': f'{i} Gone St', 'city': 'Wells',
            })
        conn.execute("UPDATE maine_transactions SET enrichment_status='success'")
        conn.commit()

        assert queue_withdrawn_for_reverification(conn, limit=2) == 2

    def test_reverification_leaves_other_statuses_alone(self, conn):
        upsert_listing(conn, {
            'detail_url': '/l/13', 'status': 'Active', 'list_price': 500_000,
            'address': '13 Active St', 'city': 'Wells',
        })
        conn.execute(
            "UPDATE maine_transactions SET enrichment_status='success' "
            "WHERE detail_url='/l/13'"
        )
        conn.commit()

        assert queue_withdrawn_for_reverification(conn) == 0
        assert _row(conn, '/l/13', 'enrichment_status')[0] == 'success'

    def test_recently_stale_not_withdrawn_by_fallback(self, conn):
        upsert_listing(conn, {
            'detail_url': '/l/11', 'status': 'Active', 'list_price': 500_000,
            'address': '11 Recent Rd', 'city': 'Wells',
        })
        conn.execute(
            "UPDATE maine_transactions SET enrichment_status='error', "
            "enrichment_attempts=2 WHERE detail_url='/l/11'"
        )
        conn.commit()
        _seen_days_ago(conn, '/l/11', 10)

        assert mark_withdrawn_stale(conn, stale_days=30) == 0
