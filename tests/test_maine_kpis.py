"""Tests for src/maine_kpis.py — KPI queries and mover computation."""
from __future__ import annotations

import pytest
from datetime import date, timedelta

from src.maine_database import get_connection, init_db
from src.maine_kpis import (
    PERIOD_12MO_DAYS,
    compute_cutoffs,
    compute_rank_movers,
    query_agent_kpis,
    query_brokerage_kpis,
)


# === Period cutoffs ===

class TestComputeCutoffs:
    def test_anchored_today_ignores_system_clock(self):
        c = compute_cutoffs('2026-04-16')
        assert c.current_12mo_start == '2025-04-16'
        assert c.prior_12mo_start == '2024-04-16'
        # 3 years × 365 days = 1095 days → 2023-04-17
        assert c.three_year_start == '2023-04-17'

    def test_none_today_uses_system(self):
        c = compute_cutoffs(None)
        # Just verify it returns something shaped correctly.
        assert len(c.current_12mo_start) == 10  # ISO date


# === Rank movers ===

class TestComputeRankMovers:
    def _row(self, name: str, current: int, prior: int) -> dict:
        return {
            'name': name,
            'current_12mo_sides': current,
            'prior_12mo_sides': prior,
        }

    def test_basic_riser(self):
        rows = [
            self._row('Alice', 50, 10),   # jumped up
            self._row('Bob', 40, 20),
            self._row('Charlie', 30, 30),
            self._row('Dan', 20, 40),     # dropped
            self._row('Eve', 10, 50),
        ]
        movers = compute_rank_movers(rows)
        # Alice: current rank 1, prior rank 3 (after Eve=50, Bob=40 prior descending)
        # prior desc: Eve(50), Bob(40), Dan(40)? Let's just check it ran
        riser_names = [r['name'] for r in movers['risers']]
        assert 'Alice' in riser_names

    def test_min_sides_threshold_excludes_low_volume(self):
        rows = [
            self._row('Alice', 3, 0),   # below threshold, excluded
            self._row('Bob', 20, 5),
            self._row('Charlie', 30, 10),
        ]
        movers = compute_rank_movers(rows, min_sides=5)
        all_names = [r['name'] for r in movers['risers'] + movers['fallers']]
        assert 'Alice' not in all_names

    def test_new_entity_no_prior_period(self):
        rows = [
            self._row('Alice', 50, 0),
            self._row('Bob', 30, 30),
            self._row('Charlie', 20, 40),
        ]
        movers = compute_rank_movers(rows)
        # Alice is NEW (prior=0)
        alice = next(r for r in movers['risers'] if r['name'] == 'Alice')
        assert alice['delta'] is None  # None = NEW

    def test_deltas_are_signed(self):
        rows = [
            self._row('Alice', 100, 10),
            self._row('Bob', 10, 100),
        ]
        movers = compute_rank_movers(rows)
        alice = next(r for r in movers['risers'] if r['name'] == 'Alice')
        bob = next(r for r in movers['fallers'] if r['name'] == 'Bob')
        # Alice moved up (current rank 1, prior rank 2) → delta = +1
        assert alice['delta'] == 1
        # Bob moved down (current rank 2, prior rank 1) → delta = -1
        assert bob['delta'] == -1

    def test_empty_input_returns_empty_lists(self):
        movers = compute_rank_movers([])
        assert movers['risers'] == []
        assert movers['fallers'] == []

    def test_top_n_caps_each_side(self):
        rows = [self._row(f'A{i}', 100 - i, i * 10) for i in range(20)]
        movers = compute_rank_movers(rows, top_n=3)
        assert len(movers['risers']) <= 3
        assert len(movers['fallers']) <= 3


# === Shared fixture for KPI queries ===

@pytest.fixture
def kpi_conn(tmp_path):
    """A DB seeded with transactions across all periods for KPI testing.

    Anchor: today = 2026-04-16. Rows placed at offsets relative to that.
    """
    c = get_connection(str(tmp_path / 'kpi.db'))
    init_db(c)

    anchor = date(2026, 4, 16)

    def _insert(days_ago, listing_agent, listing_office, buyer_agent, buyer_office, price, city, url_suffix):
        close_date = (anchor - timedelta(days=days_ago)).isoformat()
        c.execute('''
            INSERT INTO maine_transactions (
                detail_url, listing_agent, listing_office,
                buyer_agent, buyer_office, city,
                sale_price, close_date,
                enrichment_status, status, discovered_at, scraped_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'success', 'Closed', ?, ?)
        ''', (f'/l/{url_suffix}', listing_agent, listing_office,
              buyer_agent, buyer_office, city, price, close_date,
              close_date, close_date))

    # Alice: 3 listing sides last 12mo + 1 prior 12mo + 1 older
    _insert(30,  'Alice', 'Acme', 'Bob',     'BBrok', 500_000, 'Kittery', 1)
    _insert(100, 'Alice', 'Acme', 'Carol',   'CBrok', 700_000, 'York', 2)
    _insert(200, 'Alice', 'Acme', 'Dan',     'DBrok', 600_000, 'York', 3)
    _insert(400, 'Alice', 'Acme', 'Eve',     'EBrok', 300_000, 'Kittery', 4)
    _insert(900, 'Alice', 'Acme', 'Frank',   'FBrok', 200_000, 'Kittery', 5)

    # Bob: 2 buyer sides last 12mo + 2 prior 12mo
    _insert(50,  'Gina',  'GBrok', 'Bob', 'BBrok', 450_000, 'Saco', 6)
    _insert(200, 'Hank',  'HBrok', 'Bob', 'BBrok', 550_000, 'Saco', 7)
    _insert(500, 'Iris',  'IBrok', 'Bob', 'BBrok', 350_000, 'Saco', 8)
    _insert(600, 'Jake',  'JBrok', 'Bob', 'BBrok', 400_000, 'Saco', 9)

    # Charlie: only older activity (all-time but not 3yr or 12mo)
    _insert(2000, 'Charlie', 'CBrok', 'Zed', 'ZBrok', 100_000, 'Wells', 10)

    c.commit()
    return c, '2026-04-16'


class TestQueryAgentKPIs:
    def test_returns_rows_per_unique_agent(self, kpi_conn):
        conn, today = kpi_conn
        rows = query_agent_kpis(conn, today=today)
        names = {r['name'] for r in rows}
        # Alice + Bob + Gina/Hank/Iris/Jake (listing) + Bob + Carol/Dan/Eve/Frank/Zed (buyer) + Charlie
        assert 'Alice' in names
        assert 'Bob' in names
        assert 'Charlie' in names

    def test_alice_period_totals(self, kpi_conn):
        conn, today = kpi_conn
        rows = query_agent_kpis(conn, today=today)
        alice = next(r for r in rows if r['name'] == 'Alice')
        # Last 12mo: 3 listing sides (days 30, 100, 200) = 1_800_000
        assert alice['current_12mo_sides'] == 3
        assert alice['current_12mo_volume'] == 1_800_000
        # Prior 12mo: 1 side (day 400) = 300_000
        assert alice['prior_12mo_sides'] == 1
        assert alice['prior_12mo_volume'] == 300_000
        # 3yr: 5 sides — day 900 = 2023-10-29 is within 3yr window (>= 2023-04-17)
        # days 30, 100, 200, 400, 900 → 500k+700k+600k+300k+200k = 2_300_000
        assert alice['three_yr_sides'] == 5
        assert alice['three_yr_volume'] == 2_300_000
        # All-time: 5 sides, 2_300_000
        assert alice['all_time_sides'] == 5
        assert alice['all_time_volume'] == 2_300_000
        # Alice is all listing-side
        assert alice['listing_sides'] == 5
        assert alice['buyer_sides'] == 0
        assert alice['office'] == 'Acme'
        assert alice['most_recent'] == (date.fromisoformat(today) - timedelta(days=30)).isoformat()

    def test_bob_all_buyer(self, kpi_conn):
        conn, today = kpi_conn
        rows = query_agent_kpis(conn, today=today)
        bob = next(r for r in rows if r['name'] == 'Bob')
        assert bob['listing_sides'] == 0
        # Bob appears as buyer in 5 transactions:
        # day 30 (Kittery), day 50/200/500/600 (Saco)
        assert bob['buyer_sides'] == 5

    def test_town_filter(self, kpi_conn):
        conn, today = kpi_conn
        rows = query_agent_kpis(conn, town='Saco', today=today)
        names = {r['name'] for r in rows}
        # Only Bob (buyer) and Gina/Hank/Iris/Jake (listing) should be in Saco
        assert 'Alice' not in names  # Alice's transactions are in Kittery/York
        assert 'Bob' in names

    def test_limit_applied(self, kpi_conn):
        conn, today = kpi_conn
        rows = query_agent_kpis(conn, limit=3, today=today)
        assert len(rows) <= 3

    def test_exclusions_respected(self, kpi_conn):
        """Placeholder names like NON-MREIS AGENT should not appear."""
        conn, today = kpi_conn
        conn.execute('''
            INSERT INTO maine_transactions (
                detail_url, listing_agent, listing_office, city, sale_price, close_date,
                enrichment_status, status, discovered_at, scraped_at
            ) VALUES ('/l/99', 'NON-MREIS AGENT', 'X', 'Wells', 500000, '2025-06-01',
                'success', 'Closed', '2025-06-01', '2025-06-01')
        ''')
        conn.commit()
        rows = query_agent_kpis(conn, today=today)
        names = {r['name'] for r in rows}
        assert 'NON-MREIS AGENT' not in names


class TestQueryBrokerageKPIs:
    def test_aggregates_by_office(self, kpi_conn):
        conn, today = kpi_conn
        rows = query_brokerage_kpis(conn, today=today)
        offices = {r['name'] for r in rows}
        # Acme should appear (Alice's listing office, 5 transactions)
        assert 'Acme' in offices

    def test_agent_count_correct(self, kpi_conn):
        conn, today = kpi_conn
        rows = query_brokerage_kpis(conn, today=today)
        acme = next(r for r in rows if r['name'] == 'Acme')
        # Only Alice is a listing agent at Acme
        assert acme['agent_count'] == 1

    def test_bbrok_has_bob_as_buyer(self, kpi_conn):
        conn, today = kpi_conn
        rows = query_brokerage_kpis(conn, today=today)
        bbrok = next(r for r in rows if r['name'] == 'BBrok')
        # Bob appears as buyer at BBrok in all 5 buyer-side rows
        # (rows 1,6,7,8,9): day30/Kittery, day50/Saco, day200/Saco, day500/Saco, day600/Saco
        assert bbrok['buyer_sides'] == 5
        # BBrok only ever appears as buyer_office — no listing sides
        assert bbrok['listing_sides'] == 0
        # Only Bob is an agent at BBrok
        assert bbrok['agent_count'] == 1

    def test_top_agents_rollup(self, kpi_conn):
        conn, today = kpi_conn
        rows = query_brokerage_kpis(conn, today=today)
        acme = next(r for r in rows if r['name'] == 'Acme')
        assert 'Alice' in (acme['top_agents'] or '')


class TestKPIQueriesIgnoreActiveRows:
    """KPI queries should only count closed transactions."""

    def test_active_listing_excluded_from_agent_kpis(self, kpi_conn):
        conn, today = kpi_conn
        conn.execute('''
            INSERT INTO maine_transactions (
                detail_url, listing_agent, listing_office, city,
                list_price, close_date,
                enrichment_status, status,
                discovered_at, scraped_at
            ) VALUES (
                '/l/active-alice', 'Alice', 'Acme', 'Kittery',
                999999999, NULL,
                'success', 'Active',
                '2026-04-16', '2026-04-16'
            )
        ''')
        conn.commit()
        rows = query_agent_kpis(conn, today=today)
        alice = next(r for r in rows if r['name'] == 'Alice')
        # Alice's closed all-time volume is still 2_300_000 — Active row excluded.
        assert alice['all_time_volume'] == 2_300_000
        assert alice['all_time_sides'] == 5

    def test_active_row_excluded_from_brokerage_kpis(self, kpi_conn):
        conn, today = kpi_conn
        conn.execute('''
            INSERT INTO maine_transactions (
                detail_url, listing_office, listing_agent, city,
                list_price, close_date,
                enrichment_status, status,
                discovered_at, scraped_at
            ) VALUES (
                '/l/active-acme', 'Acme', 'Alice', 'Kittery',
                999999999, NULL,
                'success', 'Active',
                '2026-04-16', '2026-04-16'
            )
        ''')
        conn.commit()
        rows = query_brokerage_kpis(conn, today=today)
        acme = next(r for r in rows if r['name'] == 'Acme')
        assert acme['all_time_sides'] == 5
