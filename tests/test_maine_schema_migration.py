"""Tests for the additive schema migration (active-listings pipeline)."""
from __future__ import annotations

import sqlite3
import pytest

from src.maine_database import get_connection, init_db


class TestMigrationFreshDB:
    def test_new_columns_exist(self, tmp_path):
        conn = get_connection(str(tmp_path / 'fresh.db'))
        init_db(conn)
        cols = {r[1] for r in conn.execute('PRAGMA table_info(maine_transactions)').fetchall()}
        for expected in (
            'status', 'list_date', 'last_seen_at',
            'year_built', 'lot_sqft', 'description', 'photo_url',
        ):
            assert expected in cols, f'missing column: {expected}'

    def test_history_table_exists(self, tmp_path):
        conn = get_connection(str(tmp_path / 'fresh.db'))
        init_db(conn)
        names = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        assert 'maine_listing_history' in names

    def test_history_table_schema(self, tmp_path):
        conn = get_connection(str(tmp_path / 'fresh.db'))
        init_db(conn)
        cols = {r[1] for r in conn.execute(
            'PRAGMA table_info(maine_listing_history)'
        ).fetchall()}
        for expected in ('id', 'detail_url', 'snapshot_date', 'status', 'list_price'):
            assert expected in cols, f'missing column: {expected}'
        # Deliberately NOT in the table:
        assert 'days_on_market' not in cols, (
            'days_on_market must NOT be in history table — '
            'DOM ticks up daily and would spam history rows'
        )

    def test_indexes_created(self, tmp_path):
        conn = get_connection(str(tmp_path / 'fresh.db'))
        init_db(conn)
        idx = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        ).fetchall()}
        assert 'idx_maine_status' in idx
        assert 'idx_maine_last_seen' in idx
        assert 'idx_history_url' in idx
        assert 'idx_history_date' in idx


class TestMigrationOnExistingDB:
    """Simulate an old pre-migration DB and run init_db to migrate it."""

    def _build_legacy_db(self, path: str) -> sqlite3.Connection:
        conn = sqlite3.connect(path)
        # Legacy schema (pre-active-listings) — no status/list_date/etc.
        conn.executescript('''
            CREATE TABLE maine_transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                detail_url TEXT UNIQUE,
                close_date TEXT,
                sale_price INTEGER,
                discovered_at TEXT NOT NULL,
                scraped_at TEXT NOT NULL
            );
        ''')
        conn.execute('''
            INSERT INTO maine_transactions
            (detail_url, close_date, sale_price, discovered_at, scraped_at)
            VALUES ('https://mainelistings.com/listings/legacy-1',
                    '2024-06-01', 500000, '2024-06-02', '2024-06-02')
        ''')
        conn.commit()
        return conn

    def test_backfills_status_closed_on_existing_rows(self, tmp_path):
        db = str(tmp_path / 'legacy.db')
        legacy = self._build_legacy_db(db)
        legacy.close()

        conn = get_connection(db)
        init_db(conn)  # should migrate + backfill

        row = conn.execute(
            "SELECT status FROM maine_transactions WHERE detail_url LIKE '%legacy-1%'"
        ).fetchone()
        assert row[0] == 'Closed', (
            f'expected Closed, got {row[0]!r}; '
            'backfill must set status=Closed where close_date IS NOT NULL'
        )

    def test_migration_is_idempotent(self, tmp_path):
        db = str(tmp_path / 'legacy.db')
        legacy = self._build_legacy_db(db)
        legacy.close()

        # Run migration twice; should succeed both times without duplicate columns
        conn = get_connection(db)
        init_db(conn)
        conn.close()

        conn = get_connection(db)
        init_db(conn)  # second call should be a no-op

        cols = [r[1] for r in conn.execute(
            'PRAGMA table_info(maine_transactions)'
        ).fetchall()]
        # Each column should appear exactly once
        assert cols.count('status') == 1
        assert cols.count('list_date') == 1
