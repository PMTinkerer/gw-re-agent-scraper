"""Tests for HTML dashboard generation."""
from __future__ import annotations

import sqlite3
import tempfile

import pytest

from src.database import init_db, upsert_transaction, rebuild_rankings
from src.dashboard import generate_dashboard, _compute_trend_indicators, _compute_brokerage_trends, _render_trend_badge


@pytest.fixture
def populated_db():
    """In-memory DB with sample data for dashboard testing."""
    conn = sqlite3.connect(':memory:')
    conn.row_factory = sqlite3.Row
    init_db(conn)

    agents = [
        ('Jane Doe', 'ABC Realty', 'York', 750000, '2024-06-15'),
        ('Jane Doe', 'ABC Realty', 'York', 600000, '2024-03-15'),
        ('Jane Doe', 'ABC Realty', 'Wells', 500000, '2023-01-10'),
        ('John Smith', 'XYZ Realty', 'Kennebunk', 400000, '2024-05-20'),
        ('John Smith', 'XYZ Realty', 'Kennebunk', 350000, '2024-04-10'),
        ('Bob Jones', 'ABC Realty', 'Scarborough', 900000, '2023-02-15'),
        ('Alice New', 'DEF Realty', 'Biddeford', 800000, '2024-08-01'),
    ]
    for i, (agent, office, city, price, date) in enumerate(agents):
        upsert_transaction(conn, {
            'mls_number': f'MLS_DASH_{i}',
            'city': city,
            'sale_price': price,
            'listing_agent': agent,
            'listing_office': office,
            'data_source': 'redfin',
            'sale_date': date,
        })
    conn.commit()
    rebuild_rankings(conn)
    yield conn
    conn.close()


class TestGenerateDashboard:
    def test_generates_html_file(self, populated_db):
        with tempfile.NamedTemporaryFile(suffix='.html', delete=False) as f:
            path = f.name
        result = generate_dashboard(populated_db, path)
        assert result == path

        with open(path) as f:
            content = f.read()

        assert '<!DOCTYPE html>' in content
        assert 'Agent Leaderboard' in content
        assert 'Jane Doe' in content
        assert 'All-Time' in content
        assert '365 Days' in content
        assert 'Brokerages' in content
        assert 'Top Agents by Town' in content
        assert '</html>' in content

    def test_empty_db(self):
        conn = sqlite3.connect(':memory:')
        conn.row_factory = sqlite3.Row
        init_db(conn)
        with tempfile.NamedTemporaryFile(suffix='.html', delete=False) as f:
            path = f.name
        generate_dashboard(conn, path)
        with open(path) as f:
            content = f.read()
        assert '<!DOCTYPE html>' in content
        assert 'No data available' in content
        conn.close()

    def test_html_escaping(self, populated_db):
        """Names with special chars are properly escaped."""
        upsert_transaction(populated_db, {
            'mls_number': 'MLS_XSS',
            'city': 'York',
            'sale_price': 500000,
            'listing_agent': 'O\'Brien & <script>alert(1)</script>',
            'listing_office': 'Test & Co',
            'data_source': 'redfin',
            'sale_date': '2024-07-01',
        })
        populated_db.commit()
        with tempfile.NamedTemporaryFile(suffix='.html', delete=False) as f:
            path = f.name
        generate_dashboard(populated_db, path)
        with open(path) as f:
            content = f.read()
        assert '<script>alert(1)</script>' not in content
        assert '&amp;' in content


class TestComputeTrendIndicators:
    def test_rank_improvement(self):
        all_time = [
            {'agent_name': 'A', 'volume': 100},
            {'agent_name': 'B', 'volume': 80},
        ]
        rolling = [
            {'agent_name': 'B', 'volume': 90},
            {'agent_name': 'A', 'volume': 50},
        ]
        trends = _compute_trend_indicators(all_time, rolling)
        assert trends['B']['rank_change'] == 1   # was #2, now #1
        assert trends['A']['rank_change'] == -1   # was #1, now #2

    def test_no_change(self):
        agents = [{'agent_name': 'A', 'volume': 100}]
        trends = _compute_trend_indicators(agents, agents)
        assert trends['A']['rank_change'] == 0
        assert trends['A']['is_new'] is False

    def test_new_agent(self):
        all_time = [{'agent_name': 'A', 'volume': 100}]
        rolling = [
            {'agent_name': 'A', 'volume': 80},
            {'agent_name': 'C', 'volume': 60},
        ]
        trends = _compute_trend_indicators(all_time, rolling)
        assert trends['C']['is_new'] is True
        assert trends['A']['is_new'] is False

    def test_empty_lists(self):
        trends = _compute_trend_indicators([], [])
        assert trends == {}


class TestRenderTrendBadge:
    def test_up(self):
        badge = _render_trend_badge({'rank_change': 3, 'rolling_volume': 2100000, 'is_new': False})
        assert '&#9650;3' in badge
        assert 'badge-up' in badge
        assert '$2.1M' in badge

    def test_down(self):
        badge = _render_trend_badge({'rank_change': -2, 'rolling_volume': 800000, 'is_new': False})
        assert '&#9660;2' in badge
        assert 'badge-down' in badge

    def test_flat(self):
        badge = _render_trend_badge({'rank_change': 0, 'rolling_volume': 500000, 'is_new': False})
        assert 'badge-flat' in badge
        assert '&mdash;' in badge

    def test_new(self):
        badge = _render_trend_badge({'rank_change': 0, 'rolling_volume': 300000, 'is_new': True})
        assert 'NEW' in badge
        assert 'badge-new' in badge


class TestComputeBrokerageTrends:
    def test_rank_improvement(self):
        all_time = [
            {'office': 'Alpha Realty', 'volume': 100},
            {'office': 'Beta Realty', 'volume': 80},
        ]
        rolling = [
            {'office': 'Beta Realty', 'volume': 90},
            {'office': 'Alpha Realty', 'volume': 50},
        ]
        trends = _compute_brokerage_trends(all_time, rolling)
        assert trends['Beta Realty']['rank_change'] == 1
        assert trends['Alpha Realty']['rank_change'] == -1

    def test_new_brokerage(self):
        all_time = [{'office': 'Alpha Realty', 'volume': 100}]
        rolling = [
            {'office': 'Alpha Realty', 'volume': 80},
            {'office': 'New Brokerage', 'volume': 60},
        ]
        trends = _compute_brokerage_trends(all_time, rolling)
        assert trends['New Brokerage']['is_new'] is True
        assert trends['Alpha Realty']['is_new'] is False

    def test_no_change(self):
        brokerages = [{'office': 'Alpha Realty', 'volume': 100}]
        trends = _compute_brokerage_trends(brokerages, brokerages)
        assert trends['Alpha Realty']['rank_change'] == 0

    def test_empty_lists(self):
        trends = _compute_brokerage_trends([], [])
        assert trends == {}


class TestDashboardBrokerageSections:
    def test_has_two_brokerage_sections(self, populated_db):
        """Dashboard contains both all-time and rolling brokerage sections."""
        with tempfile.NamedTemporaryFile(suffix='.html', delete=False) as f:
            path = f.name
        generate_dashboard(populated_db, path)
        with open(path) as f:
            content = f.read()
        assert 'Top Brokerages' in content
        assert 'All-Time' in content
        assert '365 Days' in content
        # Verify both brokerage section headings
        assert content.count('Top Brokerages') >= 2

    def test_brokerage_towns_in_html(self, populated_db):
        """Brokerage tables include town data."""
        with tempfile.NamedTemporaryFile(suffix='.html', delete=False) as f:
            path = f.name
        generate_dashboard(populated_db, path)
        with open(path) as f:
            content = f.read()
        # Towns from test data should appear somewhere in the brokerage sections
        assert 'York' in content
