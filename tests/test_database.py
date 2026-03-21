"""Tests for database module."""
from __future__ import annotations

import sqlite3

import pytest

from src.database import (
    get_connection, init_db, normalize_agent_name, upsert_transaction,
    rebuild_rankings, get_stats, _to_int, _to_float,
)


@pytest.fixture
def db():
    """In-memory SQLite database for testing."""
    conn = sqlite3.connect(':memory:')
    conn.row_factory = sqlite3.Row
    init_db(conn)
    yield conn
    conn.close()


class TestNormalizeAgentName:
    def test_basic(self):
        assert normalize_agent_name('JANE DOE') == 'Jane Doe'

    def test_strip_whitespace(self):
        assert normalize_agent_name('  john smith  ') == 'John Smith'

    def test_remove_crs(self):
        assert normalize_agent_name('Jane Doe, CRS') == 'Jane Doe'

    def test_remove_multiple_designations(self):
        assert normalize_agent_name('Jane Doe, CRS, ABR, GRI') == 'Jane Doe'

    def test_remove_broker(self):
        assert normalize_agent_name('John Smith, BROKER') == 'John Smith'

    def test_remove_parenthesized_suffix(self):
        assert normalize_agent_name('Jane Doe (Broker)') == 'Jane Doe'

    def test_remove_realtor(self):
        assert normalize_agent_name('Jane Doe, REALTOR') == 'Jane Doe'

    def test_collapse_spaces(self):
        assert normalize_agent_name('Jane   Doe') == 'Jane Doe'

    def test_none(self):
        assert normalize_agent_name(None) is None

    def test_empty(self):
        assert normalize_agent_name('') is None

    def test_only_designations(self):
        assert normalize_agent_name('CRS, ABR') is None

    def test_epro_hyphen(self):
        assert normalize_agent_name('Jane Doe, e-PRO') == 'Jane Doe'

    def test_title_case(self):
        assert normalize_agent_name('jane doe') == 'Jane Doe'

    def test_mixed_case_designation(self):
        assert normalize_agent_name('JANE DOE crs') == 'Jane Doe'


class TestUpsertTransaction:
    def test_insert(self, db):
        record = {
            'mls_number': 'MLS123',
            'address': '123 Main St',
            'city': 'York',
            'sale_price': 450000,
            'listing_agent': 'Jane Doe',
            'listing_office': 'ABC Realty',
            'data_source': 'redfin',
            'sale_date': '2024-06-15',
        }
        assert upsert_transaction(db, record) is True
        db.commit()

        row = db.execute('SELECT * FROM transactions WHERE mls_number = ?', ('MLS123',)).fetchone()
        assert row['city'] == 'York'
        assert row['sale_price'] == 450000
        assert row['listing_agent'] == 'Jane Doe'
        assert row['raw_listing_agent'] == 'Jane Doe'

    def test_dedup_on_mls(self, db):
        record1 = {
            'mls_number': 'MLS123',
            'city': 'York',
            'sale_price': 450000,
            'listing_agent': None,
            'data_source': 'redfin',
        }
        record2 = {
            'mls_number': 'MLS123',
            'city': 'York',
            'sale_price': 460000,
            'listing_agent': 'Jane Doe, CRS',
            'data_source': 'realtor',
        }
        upsert_transaction(db, record1)
        db.commit()
        upsert_transaction(db, record2)
        db.commit()

        count = db.execute('SELECT COUNT(*) FROM transactions WHERE mls_number = ?', ('MLS123',)).fetchone()[0]
        assert count == 1

        row = db.execute('SELECT * FROM transactions WHERE mls_number = ?', ('MLS123',)).fetchone()
        # Should have the updated agent (COALESCE prefers non-null)
        assert row['listing_agent'] == 'Jane Doe'
        assert row['sale_price'] == 460000

    def test_skip_no_mls(self, db):
        assert upsert_transaction(db, {'mls_number': None, 'data_source': 'redfin'}) is False
        assert upsert_transaction(db, {'mls_number': '', 'data_source': 'redfin'}) is False

    def test_price_parsing(self, db):
        record = {
            'mls_number': 'MLS456',
            'sale_price': '$1,234,567',
            'data_source': 'redfin',
        }
        upsert_transaction(db, record)
        db.commit()
        row = db.execute('SELECT sale_price FROM transactions WHERE mls_number = ?', ('MLS456',)).fetchone()
        assert row['sale_price'] == 1234567


class TestRebuildRankings:
    def test_rankings(self, db):
        # Insert test data
        for i in range(5):
            upsert_transaction(db, {
                'mls_number': f'MLS_A_{i}',
                'city': 'York',
                'sale_price': 500000 + i * 100000,
                'listing_agent': 'Jane Doe',
                'listing_office': 'ABC Realty',
                'data_source': 'redfin',
                'sale_date': f'2024-0{i+1}-15',
            })
        for i in range(3):
            upsert_transaction(db, {
                'mls_number': f'MLS_B_{i}',
                'city': 'Wells',
                'sale_price': 300000,
                'listing_agent': 'John Smith',
                'listing_office': 'XYZ Realty',
                'data_source': 'redfin',
                'sale_date': f'2024-0{i+1}-20',
            })
        db.commit()

        rebuild_rankings(db)

        rankings = db.execute(
            'SELECT * FROM agent_rankings ORDER BY listing_volume DESC'
        ).fetchall()

        assert len(rankings) == 2
        # Jane Doe should be #1 (higher volume)
        assert rankings[0]['agent_name'] == 'Jane Doe'
        assert rankings[0]['total_listing_sides'] == 5
        assert rankings[0]['listing_volume'] == 3500000  # 500+600+700+800+900=3500K
        assert rankings[1]['agent_name'] == 'John Smith'
        assert rankings[1]['total_listing_sides'] == 3


class TestGetStats:
    def test_empty_db(self, db):
        stats = get_stats(db)
        assert stats['total_transactions'] == 0

    def test_with_data(self, db):
        upsert_transaction(db, {
            'mls_number': 'MLS1',
            'city': 'York',
            'listing_agent': 'Jane Doe',
            'data_source': 'redfin',
            'sale_date': '2024-01-01',
        })
        db.commit()
        stats = get_stats(db)
        assert stats['total_transactions'] == 1
        assert stats['with_listing_agent'] == 1
        assert 'redfin' in stats['sources']


class TestHelpers:
    def test_to_int(self):
        assert _to_int('$1,234,567') == 1234567
        assert _to_int('500000') == 500000
        assert _to_int(None) is None
        assert _to_int('') is None
        assert _to_int('abc') is None

    def test_to_float(self):
        assert _to_float('2.5') == 2.5
        assert _to_float(None) is None
        assert _to_float('') is None
