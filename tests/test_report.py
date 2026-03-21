"""Tests for report generation module."""
from __future__ import annotations

import sqlite3
import tempfile

import pytest

from src.database import init_db, upsert_transaction, rebuild_rankings
from src.report import format_currency, format_currency_full, generate_leaderboard


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
