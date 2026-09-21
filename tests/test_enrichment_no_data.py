"""Tests for classifying detail pages that carry no listing data.

A delisted listing still serves a full page, but its NUXT blob has no agent
data and the extractor returns {"error": "no agent data found in NUXT blob"}.
That is an expected, terminal outcome — especially during a repair backfill,
where much of the queue is old withdrawn listings. Counting it as an
infrastructure failure tripped the circuit breaker (20 failures) on every
run, so the backfill could never drain.
"""
from __future__ import annotations

import pytest

from src.maine_database import (
    get_connection,
    init_db,
    get_unenriched,
    mark_enrichment_no_data,
    upsert_listing,
)
from src.maine_parser import detail_response_error, parse_detail_response


NO_DATA = {'type': 'string',
           'value': '{"error":"no agent data found in NUXT blob","photo_url":null}'}
GOOD = {'type': 'string',
        'value': '{"close_date":"2026-06-26","buyer_agent":"Erin Lamarche"}'}


@pytest.fixture
def conn(tmp_path):
    c = get_connection(str(tmp_path / 'nd.db'))
    init_db(c)
    return c


class TestDetailResponseClassification:
    def test_delisted_page_reports_its_error(self):
        assert detail_response_error(NO_DATA) == 'no agent data found in NUXT blob'

    def test_usable_payload_has_no_error(self):
        assert detail_response_error(GOOD) is None

    def test_malformed_payload_is_not_a_no_data_page(self):
        """Genuinely unparseable output must stay an infrastructure failure."""
        assert detail_response_error({'type': 'string', 'value': 'not json'}) is None
        assert detail_response_error(None) is None

    def test_parse_still_rejects_both(self):
        assert parse_detail_response(NO_DATA) is None
        assert parse_detail_response(GOOD) is not None


class TestNoDataIsTerminal:
    def test_marking_no_data_removes_row_from_queue(self, conn):
        upsert_listing(conn, {
            'detail_url': '/l/dead', 'status': 'Active', 'list_price': 500_000,
            'address': '1 Sullivan Farm Road', 'city': 'Scarborough',
        })
        assert '/l/dead' in [r['detail_url'] for r in get_unenriched(conn)]

        mark_enrichment_no_data(conn, '/l/dead')

        assert '/l/dead' not in [r['detail_url'] for r in get_unenriched(conn)]
        status, attempts = conn.execute(
            'SELECT enrichment_status, enrichment_attempts '
            'FROM maine_transactions WHERE detail_url = ?', ('/l/dead',),
        ).fetchone()
        assert status == 'no_data'
        assert attempts == 1

    def test_closed_row_with_no_data_is_not_requeued_forever(self, conn):
        """The close_date backfill clause must not loop on a dead page."""
        upsert_listing(conn, {
            'detail_url': '/l/dead2', 'status': 'Closed', 'sale_price': 400_000,
            'address': '2 Gone St', 'city': 'Wells',
        })
        mark_enrichment_no_data(conn, '/l/dead2')

        assert '/l/dead2' not in [r['detail_url'] for r in get_unenriched(conn)]

    def test_no_data_still_eligible_for_withdrawn_fallback(self, conn):
        """no_data is not success, so a long-stale row can still be withdrawn."""
        from datetime import datetime, timedelta
        from src.maine_database import mark_withdrawn_stale

        upsert_listing(conn, {
            'detail_url': '/l/dead3', 'status': 'Active', 'list_price': 500_000,
            'address': '3 Gone St', 'city': 'Wells',
        })
        mark_enrichment_no_data(conn, '/l/dead3')
        conn.execute(
            'UPDATE maine_transactions SET last_seen_at = ? WHERE detail_url = ?',
            ((datetime.utcnow() - timedelta(days=90)).isoformat(), '/l/dead3'),
        )
        conn.commit()

        assert mark_withdrawn_stale(conn, stale_days=30) == 1


class TestCircuitBreakerIgnoresNoData:
    def _run(self, conn, monkeypatch, payload, n=30):
        from src import maine_firecrawl

        for i in range(n):
            upsert_listing(conn, {
                'detail_url': f'/l/b{i}', 'status': 'Active',
                'list_price': 500_000, 'address': f'{i} St', 'city': 'Wells',
            })
        monkeypatch.setattr(maine_firecrawl, '_get_client', lambda: object())
        monkeypatch.setattr(
            maine_firecrawl, '_scrape',
            lambda client, url, fmt='markdown': type(
                'R', (), {'actions': {'javascriptReturns': [payload]}})(),
        )
        return maine_firecrawl.enrich_listings(conn, batch_size=n, workers=1)

    def test_thirty_delisted_pages_do_not_abort_the_batch(self, conn, monkeypatch):
        result = self._run(conn, monkeypatch, NO_DATA, n=30)

        assert not result.get('aborted'), 'delisted pages must not trip the breaker'
        assert result['no_data'] == 30
        assert result['failed'] == 0

    def test_real_failures_still_trip_the_breaker(self, conn, monkeypatch):
        """An unparseable response is still an infrastructure fault."""
        result = self._run(
            conn, monkeypatch, {'type': 'string', 'value': 'garbage'}, n=30)

        assert result['failed'] > 0


class TestConcurrencyClamp:
    """Exceeding the account's concurrency ceiling fails requests, not queues them."""

    def test_workers_clamped_to_account_limit(self):
        from src.maine_firecrawl import clamp_workers

        assert clamp_workers(20, 5) == 5
        assert clamp_workers(25, 5) == 5

    def test_workers_under_limit_untouched(self):
        from src.maine_firecrawl import clamp_workers

        assert clamp_workers(3, 5) == 3
        assert clamp_workers(5, 5) == 5

    def test_unknown_limit_fails_open(self):
        """A transient API problem must never block a run."""
        from src.maine_firecrawl import clamp_workers

        assert clamp_workers(20, None) == 20
        assert clamp_workers(20, 0) == 20

    def test_get_max_concurrency_swallows_errors(self, monkeypatch):
        from src import maine_firecrawl

        monkeypatch.setattr(
            maine_firecrawl, 'require_firecrawl_key',
            lambda: (_ for _ in ()).throw(RuntimeError('no key')),
        )
        assert maine_firecrawl.get_max_concurrency() is None
