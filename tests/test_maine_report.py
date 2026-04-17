"""Tests for src/maine_report.py and src/maine_main.py helpers."""
from __future__ import annotations

import pytest

from src.maine_database import get_connection, init_db
from src.maine_main import _canonicalize_town, _parse_towns
from src.maine_report import (
    build_maine_search_index,
    format_currency,
    generate_leaderboard,
    query_top_agents,
    query_top_brokerages,
    query_top_combined_agents,
)


@pytest.fixture
def conn(tmp_path):
    """A fresh, empty maine_listings.db."""
    c = get_connection(str(tmp_path / 'test.db'))
    init_db(c)
    yield c
    c.close()


@pytest.fixture
def populated_conn(conn):
    """A connection pre-seeded with a few enriched transactions."""
    rows = [
        # (detail_url, listing_agent, listing_office, buyer_agent, buyer_office,
        #  city, sale_price, close_date)
        ('/l/1', 'Alice', 'Acme RE', 'Bob',   'Big Brokerage', 'Kittery', 500_000, '2025-01-10'),
        ('/l/2', 'Alice', 'Acme RE', 'Carol', 'Big Brokerage', 'York',    750_000, '2025-02-15'),
        ('/l/3', 'Alice', 'Acme RE', 'Bob',   'Big Brokerage', 'York',    400_000, '2025-03-20'),
        ('/l/4', 'Dan',   'Other',   'Alice', 'Acme RE',       'Biddeford', 600_000, '2025-04-01'),
    ]
    for detail_url, la, lo, ba, bo, city, price, close in rows:
        conn.execute('''
            INSERT INTO maine_transactions (
                detail_url, listing_agent, listing_office,
                buyer_agent, buyer_office, city,
                sale_price, close_date,
                enrichment_status, discovered_at, scraped_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'success', ?, ?)
        ''', (detail_url, la, lo, ba, bo, city, price, close, close, close))
    conn.commit()
    return conn


# === Town canonicalization ===

class TestCanonicalizeTown:
    def test_underscore_lowercase(self):
        assert _canonicalize_town('old_orchard_beach') == 'Old Orchard Beach'

    def test_space_titlecase(self):
        assert _canonicalize_town('Old Orchard Beach') == 'Old Orchard Beach'

    def test_all_caps_hyphen(self):
        assert _canonicalize_town('OLD-ORCHARD-BEACH') == 'Old Orchard Beach'

    def test_single_word(self):
        assert _canonicalize_town('york') == 'York'
        assert _canonicalize_town('KITTERY') == 'Kittery'

    def test_multi_word_compound(self):
        assert _canonicalize_town('kennebunkport') == 'Kennebunkport'

    def test_unknown_passes_through(self):
        # Unknown town returns input unchanged (with warning).
        assert _canonicalize_town('portland') == 'portland'


class TestParseTowns:
    def test_empty(self):
        assert _parse_towns('') is None
        assert _parse_towns(None) is None

    def test_canonicalizes_each(self):
        result = _parse_towns('saco,old_orchard_beach,scarborough')
        assert result == ['Saco', 'Old Orchard Beach', 'Scarborough']


# === Report queries on empty DB ===

class TestEmptyConnReturns:
    def test_combined_agents_empty(self, conn):
        assert query_top_combined_agents(conn) == []

    def test_top_listing_empty(self, conn):
        assert query_top_agents(conn, role='listing') == []

    def test_top_buyer_empty(self, conn):
        assert query_top_agents(conn, role='buyer') == []

    def test_top_brokerages_empty(self, conn):
        assert query_top_brokerages(conn) == []

    def test_search_index_empty(self, conn):
        assert build_maine_search_index(conn) == []

    def test_leaderboard_writes_file(self, conn, tmp_path):
        path = generate_leaderboard(conn, str(tmp_path / 'leaderboard.md'))
        with open(path) as f:
            content = f.read()
        assert 'Maine MLS Agent Leaderboard' in content
        assert 'No enriched data yet' in content


# === Report queries on populated DB ===

class TestPopulatedReport:
    def test_listing_top_ranks_alice_first(self, populated_conn):
        top = query_top_agents(populated_conn, role='listing')
        assert top[0]['agent_name'] == 'Alice'
        assert top[0]['sides'] == 3

    def test_buyer_top_includes_alice_and_bob(self, populated_conn):
        top = query_top_agents(populated_conn, role='buyer')
        names = {a['agent_name'] for a in top}
        assert 'Alice' in names
        assert 'Bob' in names

    def test_combined_totals_sum_correctly(self, populated_conn):
        top = query_top_combined_agents(populated_conn)
        alice = next(a for a in top if a['agent_name'] == 'Alice')
        # 3 listing + 1 buyer = 4 total
        assert alice['listing_sides'] == 3
        assert alice['buyer_sides'] == 1
        assert alice['total_sides'] == 4

    def test_town_filter(self, populated_conn):
        top = query_top_combined_agents(populated_conn, town='York')
        assert all('York' in (a['towns'] or '') for a in top)

    def test_brokerages_aggregate_both_sides(self, populated_conn):
        top = query_top_brokerages(populated_conn)
        acme = next(b for b in top if b['brokerage'] == 'Acme RE')
        # Alice listed 3 + Alice bought once = 4 sides total for Acme
        assert acme['sides'] == 4

    def test_search_index_returns_agents(self, populated_conn):
        idx = build_maine_search_index(populated_conn)
        names = {a['name'] for a in idx}
        assert {'Alice', 'Bob', 'Carol', 'Dan'}.issubset(names)

    def test_search_index_has_period_fields(self, populated_conn):
        idx = build_maine_search_index(populated_conn)
        alice = next(a for a in idx if a['name'] == 'Alice')
        # New fields the master table + detail modal rely on:
        for field in (
            'current_12mo_volume', 'current_12mo_sides',
            'prior_12mo_volume',   'prior_12mo_sides',
            'three_yr_volume',     'three_yr_sides',
            'all_time_volume',     'all_time_sides',
        ):
            assert field in alice, f'missing field: {field}'


# === Currency formatting ===

class TestFormatCurrency:
    def test_zero_and_none(self):
        assert format_currency(None) == '$0'
        assert format_currency(0) == '$0'

    def test_thousands(self):
        assert format_currency(500_000) == '$500K'

    def test_millions(self):
        assert format_currency(2_500_000) == '$2.5M'

    def test_small(self):
        assert format_currency(250) == '$250'
