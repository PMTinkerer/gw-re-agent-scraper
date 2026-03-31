"""Tests for report generation module."""
from __future__ import annotations

import sqlite3
import tempfile

import pytest

from src.database import init_db, upsert_transaction, rebuild_rankings
from src.report import format_currency, format_currency_full, generate_leaderboard, query_top_agents, query_top_brokerages


class TestFormatCurrency:
    def test_millions(self):
        assert format_currency(1500000) == '$1.5M'

    def test_millions_round(self):
        assert format_currency(2000000) == '$2.0M'

    def test_thousands(self):
        assert format_currency(339000) == '$339K'

    def test_low_thousands(self):
        assert format_currency(5000) == '$5K'

    def test_small(self):
        assert format_currency(999) == '$999'

    def test_zero(self):
        assert format_currency(0) == '$0'

    def test_none(self):
        assert format_currency(None) == '$0'


class TestFormatCurrencyFull:
    def test_full(self):
        assert format_currency_full(1500000) == '$1,500,000'


@pytest.fixture
def populated_db():
    """In-memory DB with sample data for report testing."""
    conn = sqlite3.connect(':memory:')
    conn.row_factory = sqlite3.Row
    init_db(conn)

    # Insert diverse test data
    agents = [
        ('Jane Doe', 'ABC Realty', 'York', 750000),
        ('Jane Doe', 'ABC Realty', 'York', 600000),
        ('Jane Doe', 'ABC Realty', 'Wells', 500000),
        ('John Smith', 'XYZ Realty', 'Kennebunk', 400000),
        ('John Smith', 'XYZ Realty', 'Kennebunk', 350000),
        ('Bob Jones', 'ABC Realty', 'Scarborough', 900000),
    ]
    for i, (agent, office, city, price) in enumerate(agents):
        upsert_transaction(conn, {
            'mls_number': f'MLS_TEST_{i}',
            'city': city,
            'sale_price': price,
            'listing_agent': agent,
            'listing_office': office,
            'data_source': 'redfin',
            'sale_date': f'2024-0{i+1}-15',
        })
    conn.commit()
    rebuild_rankings(conn)
    yield conn
    conn.close()


class TestGenerateLeaderboard:
    def test_generates_file(self, populated_db):
        with tempfile.NamedTemporaryFile(suffix='.md', delete=False) as f:
            path = f.name
        result = generate_leaderboard(populated_db, path)
        assert result == path

        with open(path) as f:
            content = f.read()

        assert '# Real Estate Agent Leaderboard' in content
        assert 'Jane Doe' in content
        assert 'John Smith' in content
        assert 'ABC Realty' in content
        assert 'Top 30 Listing Agents' in content
        assert 'Top 15 Brokerages' in content
        assert 'Top 5 Listing Agents by Town' in content
        assert 'Data Summary' in content

    def test_empty_db(self):
        conn = sqlite3.connect(':memory:')
        conn.row_factory = sqlite3.Row
        init_db(conn)
        with tempfile.NamedTemporaryFile(suffix='.md', delete=False) as f:
            path = f.name
        generate_leaderboard(conn, path)
        with open(path) as f:
            content = f.read()
        assert 'No data available' in content
        conn.close()


class TestQueryTopAgentsSinceDate:
    def test_date_filter_reduces_results(self, populated_db):
        """Filtering by date returns fewer or equal results."""
        all_agents = query_top_agents(populated_db)
        # Test data has dates 2024-01-15 through 2024-06-15
        filtered = query_top_agents(populated_db, since_date='2024-04-01')
        assert len(filtered) <= len(all_agents)

    def test_no_date_filter_returns_all(self, populated_db):
        """Without since_date, returns all agents (backward compat)."""
        agents = query_top_agents(populated_db)
        assert len(agents) == 3  # Jane Doe, John Smith, Bob Jones

    def test_future_date_returns_empty(self, populated_db):
        """A future since_date returns no results."""
        agents = query_top_agents(populated_db, since_date='2030-01-01')
        assert len(agents) == 0


class TestBrokerageAsAgentExclusion:
    def test_excluded_from_agent_rankings(self, populated_db):
        """An agent whose name matches their office is excluded from agent rankings."""
        upsert_transaction(populated_db, {
            'mls_number': 'MLS_BROKERAGE',
            'city': 'York',
            'sale_price': 999000,
            'listing_agent': 'Fake Brokerage LLC',
            'listing_office': 'Fake Brokerage LLC',
            'data_source': 'redfin',
            'sale_date': '2024-05-01',
        })
        populated_db.commit()
        agents = query_top_agents(populated_db)
        agent_names = [a['agent_name'] for a in agents]
        assert 'Fake Brokerage Llc' not in agent_names

    def test_included_in_brokerage_rankings(self, populated_db):
        """The same brokerage-as-agent entry still appears in brokerage rankings."""
        upsert_transaction(populated_db, {
            'mls_number': 'MLS_BROKERAGE2',
            'city': 'York',
            'sale_price': 999000,
            'listing_agent': 'Fake Brokerage LLC',
            'listing_office': 'Fake Brokerage LLC',
            'data_source': 'redfin',
            'sale_date': '2024-05-01',
        })
        populated_db.commit()
        brokerages = query_top_brokerages(populated_db)
        office_names = [b['office'] for b in brokerages]
        assert 'Fake Brokerage LLC' in office_names


class TestBrokerageQueryEnhancements:
    def test_since_date_filter(self, populated_db):
        """Filtering by date returns fewer or equal brokerage results."""
        all_brokerages = query_top_brokerages(populated_db)
        filtered = query_top_brokerages(populated_db, since_date='2024-04-01')
        assert len(filtered) <= len(all_brokerages)

    def test_no_date_filter_unchanged(self, populated_db):
        """Without since_date, returns all brokerages (backward compat)."""
        brokerages = query_top_brokerages(populated_db)
        assert len(brokerages) == 2  # ABC Realty, XYZ Realty

    def test_results_include_towns(self, populated_db):
        """Brokerage results include a towns field."""
        brokerages = query_top_brokerages(populated_db)
        for b in brokerages:
            assert 'towns' in b

    def test_future_date_returns_empty(self, populated_db):
        """A future since_date returns no brokerage results."""
        brokerages = query_top_brokerages(populated_db, since_date='2030-01-01')
        assert len(brokerages) == 0
