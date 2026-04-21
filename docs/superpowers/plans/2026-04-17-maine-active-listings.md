# Maine MLS Active Listings Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend the Maine MLS scraper from closed-only to also capture active/pending/withdrawn listings daily, with a change-detected history table, a withdrawn-sweeper, and read helpers for downstream tools.

**Architecture:** Single unified `maine_transactions` table gains a `status` column (Active/Pending/Closed/Withdrawn) plus new columns for list_date, last_seen_at, year_built, lot_sqft, description, photo_url. A new `maine_listing_history` child table captures status/list_price changes. The existing Firecrawl discover/enrich loop grows a `status` parameter — ~95% code reuse. A new daily cron runs active-only alongside the existing weekly closed cron. Two read helpers (`src/maine_active.py`) freeze the downstream-tool interface.

**Tech Stack:** Python 3.9+ (CI uses 3.12), SQLite (WAL mode), pytest 9.0.3, Firecrawl, GitHub Actions, existing Pushover+Resend notifier.

**Spec:** `docs/superpowers/specs/2026-04-17-maine-active-listings-design.md`

**Branch:** `feature/maine-active-listings` (already created off `main`; spec commit `52895c4`)

---

## Pre-flight

- [ ] **P1: Verify branch + test baseline**

```bash
cd /Users/lucasknowles/gw-re-agent-scraper
git status                      # Should be clean on feature/maine-active-listings
git branch --show-current       # Should print: feature/maine-active-listings
python -m pytest tests/ -q      # Should report: 232 passed
```

Expected: 232 tests pass on a clean working tree. If any tests fail, stop and report — the baseline is broken and the plan assumes green.

- [ ] **P2: Confirm spec is present**

```bash
ls -la docs/superpowers/specs/2026-04-17-maine-active-listings-design.md
```

Expected: file exists. Read it once before starting Task A1. It's the authoritative source for behavior — this plan implements what the spec requires.

---

## File Structure

| File | Current | After this plan |
|---|---|---|
| `src/maine_database.py` | 182 lines, schema + upsert/enrich/query helpers | Adds: migration for 7 new columns + history table, `write_history_if_changed`, `mark_withdrawn_stale`, updated `upsert_listing` (status + last_seen_at), updated `enrich_listing` (status + new fields). ~300 lines. |
| `src/maine_parser.py` | 186 lines, one closed-card regex + NUXT JS | Adds: parameterized card regex factory, active-card regex, extended NUXT JS for year_built/lot_sqft/description/photo_url/list_date/mls_status. ~230 lines. |
| `src/maine_firecrawl.py` | 421 lines | Minimal change: `discover_listings(status='Closed'|'Active')` threading the status through to the URL builder and card parser. ~445 lines. |
| `src/maine_main.py` | 238 lines | Adds: `--status`, `--max-credits`, `--sweep` flags; sweeper call; anomaly detector; routing for daily-active mode. ~340 lines. |
| `src/maine_report.py` | 465 lines | Adds `WHERE status = 'Closed'` filter to three agent/brokerage SQL queries. ~470 lines. |
| `src/maine_kpis.py` | 291 lines | Adds `WHERE status = 'Closed'` filter to both CTEs. ~295 lines. |
| `src/maine_active.py` | *(new)* | 4 read helpers for downstream tools: `query_active_listings`, `query_listing_history`, `query_new_since`, `query_stale_listings`. ~100 lines. |
| `.github/workflows/maine_listings.yml` | 153 lines, weekly cron only | Adds second cron (daily `30 11 * * *`), new mode routes (`daily-active`, `backfill-active`). ~200 lines. |
| `tests/test_maine_schema_migration.py` | *(new)* | Idempotent migration, backfill behavior. |
| `tests/test_maine_history.py` | *(new)* | Change-detection logic. |
| `tests/test_maine_active_discovery.py` | *(new)* | Active card regex, URL builder, status plumbing. |
| `tests/test_maine_active_sweeper.py` | *(new)* | Withdrawn-sweep 7-day threshold. |
| `tests/test_maine_active.py` | *(new)* | Four downstream read helpers. |
| `tests/test_maine_anomaly.py` | *(new)* | Anomaly alert trigger. |

**Invariant:** all 232 existing tests must still pass at every commit. The migration backfills `status='Closed'` on existing rows, and the query filters added in A3 match that backfill — semantically unchanged.

---

## Task A1: Schema migration + backfill

**Goal:** Additively migrate `maine_transactions` to add 7 new columns and a `maine_listing_history` child table, backfilling `status='Closed'` on existing rows. Idempotent — safe to re-run.

**Files:**
- Modify: `src/maine_database.py` (init_db function, new migration block)
- Test: `tests/test_maine_schema_migration.py` (new)

- [ ] **Step 1: Write the failing test**

Create `tests/test_maine_schema_migration.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

```bash
python -m pytest tests/test_maine_schema_migration.py -v
```

Expected: all tests fail with "missing column: status" (or similar) — the migration hasn't been written yet.

- [ ] **Step 3: Implement the migration**

In `src/maine_database.py`, replace the `init_db` function with:

```python
_NEW_COLUMNS_ON_TRANSACTIONS = [
    # (column_name, type_clause)
    ('status', 'TEXT'),
    ('list_date', 'TEXT'),
    ('last_seen_at', 'TEXT'),
    ('year_built', 'INTEGER'),
    ('lot_sqft', 'INTEGER'),
    ('description', 'TEXT'),
    ('photo_url', 'TEXT'),
]


def _existing_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {
        r[1] for r in conn.execute(f'PRAGMA table_info({table})').fetchall()
    }


def _apply_additive_migration(conn: sqlite3.Connection) -> None:
    """Add new columns to maine_transactions if they don't already exist.

    SQLite lacks `ADD COLUMN IF NOT EXISTS`, so we inspect PRAGMA table_info
    and skip existing columns. Safe to run repeatedly.
    """
    existing = _existing_columns(conn, 'maine_transactions')
    for col, typ in _NEW_COLUMNS_ON_TRANSACTIONS:
        if col not in existing:
            conn.execute(f'ALTER TABLE maine_transactions ADD COLUMN {col} {typ}')
    conn.commit()

    # Backfill: any legacy row with close_date IS NOT NULL and status IS NULL
    # is a pre-migration closed transaction. Mark it explicitly.
    conn.execute('''
        UPDATE maine_transactions
        SET status = 'Closed'
        WHERE status IS NULL AND close_date IS NOT NULL
    ''')
    conn.commit()


def init_db(conn: sqlite3.Connection) -> None:
    """Create tables + indexes. Idempotent.

    - maine_transactions: one row per MLS listing (any status).
    - maine_listing_history: change-detected snapshots of (status, list_price).
    """
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS maine_transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            mls_number TEXT,
            address TEXT,
            city TEXT,
            state TEXT DEFAULT 'ME',
            zip TEXT,
            sale_price INTEGER,
            list_price INTEGER,
            beds INTEGER,
            baths REAL,
            sqft INTEGER,
            property_type TEXT,
            days_on_market INTEGER,
            close_date TEXT,
            listing_agent TEXT,
            listing_agent_id TEXT,
            listing_agent_email TEXT,
            listing_office TEXT,
            buyer_agent TEXT,
            buyer_agent_id TEXT,
            buyer_agent_email TEXT,
            buyer_office TEXT,
            detail_url TEXT UNIQUE,
            listing_key TEXT,
            enrichment_status TEXT,
            enrichment_attempts INTEGER DEFAULT 0,
            discovered_at TEXT NOT NULL,
            enriched_at TEXT,
            scraped_at TEXT NOT NULL,
            status TEXT,
            list_date TEXT,
            last_seen_at TEXT,
            year_built INTEGER,
            lot_sqft INTEGER,
            description TEXT,
            photo_url TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_maine_city
            ON maine_transactions(city);
        CREATE INDEX IF NOT EXISTS idx_maine_close_date
            ON maine_transactions(close_date);
        CREATE INDEX IF NOT EXISTS idx_maine_listing_agent
            ON maine_transactions(listing_agent);
        CREATE INDEX IF NOT EXISTS idx_maine_buyer_agent
            ON maine_transactions(buyer_agent);
        CREATE INDEX IF NOT EXISTS idx_maine_enrichment
            ON maine_transactions(enrichment_status);
        CREATE INDEX IF NOT EXISTS idx_maine_mls
            ON maine_transactions(mls_number);
        CREATE INDEX IF NOT EXISTS idx_maine_status
            ON maine_transactions(status);
        CREATE INDEX IF NOT EXISTS idx_maine_last_seen
            ON maine_transactions(last_seen_at);

        CREATE TABLE IF NOT EXISTS maine_listing_history (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            detail_url     TEXT NOT NULL,
            snapshot_date  TEXT NOT NULL,
            status         TEXT,
            list_price     INTEGER,
            FOREIGN KEY (detail_url) REFERENCES maine_transactions(detail_url)
        );

        CREATE INDEX IF NOT EXISTS idx_history_url
            ON maine_listing_history(detail_url);
        CREATE INDEX IF NOT EXISTS idx_history_date
            ON maine_listing_history(snapshot_date);
    ''')
    conn.commit()

    # Run the additive migration last so new columns land on any legacy DB
    # whose maine_transactions was created before the new-column list grew.
    _apply_additive_migration(conn)
```

Keep the rest of the module unchanged for now — we'll update `upsert_listing` and `enrich_listing` in Task A2.

- [ ] **Step 4: Run test to verify it passes**

```bash
python -m pytest tests/test_maine_schema_migration.py -v
```

Expected: all migration tests pass.

- [ ] **Step 5: Run full suite**

```bash
python -m pytest tests/ -q
```

Expected: 232 existing tests + new migration tests all pass. No regressions.

- [ ] **Step 6: Commit**

```bash
git add src/maine_database.py tests/test_maine_schema_migration.py
git commit -m "feat: additive schema migration for active listings"
```

---

## Task A2: History write helper + upsert/enrich integration

**Goal:** Add `write_history_if_changed(conn, detail_url, status, list_price)`. Integrate it into `upsert_listing` and `enrich_listing` so a history row is written on first-see and on any status or list_price change. Also teach `upsert_listing` to accept `status`, `list_price`, and stamp `last_seen_at`.

**Files:**
- Modify: `src/maine_database.py` (add helper + patch upsert/enrich)
- Test: `tests/test_maine_history.py` (new)

- [ ] **Step 1: Write the failing test**

Create `tests/test_maine_history.py`:

```python
"""Tests for maine_listing_history change-detected writes."""
from __future__ import annotations

import pytest

from src.maine_database import (
    enrich_listing,
    get_connection,
    init_db,
    upsert_listing,
    write_history_if_changed,
)


@pytest.fixture
def conn(tmp_path):
    c = get_connection(str(tmp_path / 'h.db'))
    init_db(c)
    return c


def _history_rows(conn, url):
    return conn.execute(
        'SELECT status, list_price FROM maine_listing_history '
        'WHERE detail_url = ? ORDER BY id ASC',
        (url,),
    ).fetchall()


class TestWriteHistoryIfChanged:
    def test_baseline_row_on_first_see(self, conn):
        # Need a transactions row first (FK)
        upsert_listing(conn, {
            'detail_url': 'https://mainelistings.com/listings/x1',
            'status': 'Active', 'list_price': 800_000,
            'address': '1 Main', 'city': 'Kittery',
        })
        rows = _history_rows(conn, 'https://mainelistings.com/listings/x1')
        assert len(rows) == 1
        assert rows[0][0] == 'Active'
        assert rows[0][1] == 800_000

    def test_no_row_on_unchanged_upsert(self, conn):
        url = 'https://mainelistings.com/listings/x2'
        upsert_listing(conn, {
            'detail_url': url, 'status': 'Active',
            'list_price': 700_000, 'address': '2 Main', 'city': 'York',
        })
        # Re-upsert with identical status + price
        upsert_listing(conn, {
            'detail_url': url, 'status': 'Active',
            'list_price': 700_000, 'address': '2 Main', 'city': 'York',
        })
        rows = _history_rows(conn, url)
        assert len(rows) == 1, 'no history row should be written if nothing changed'

    def test_row_on_price_change(self, conn):
        url = 'https://mainelistings.com/listings/x3'
        upsert_listing(conn, {
            'detail_url': url, 'status': 'Active',
            'list_price': 900_000, 'address': '3 Main', 'city': 'Wells',
        })
        upsert_listing(conn, {
            'detail_url': url, 'status': 'Active',
            'list_price': 850_000, 'address': '3 Main', 'city': 'Wells',
        })
        rows = _history_rows(conn, url)
        assert len(rows) == 2
        assert rows[0][1] == 900_000
        assert rows[1][1] == 850_000

    def test_row_on_status_transition(self, conn):
        url = 'https://mainelistings.com/listings/x4'
        upsert_listing(conn, {
            'detail_url': url, 'status': 'Active',
            'list_price': 500_000, 'address': '4 Main', 'city': 'Saco',
        })
        upsert_listing(conn, {
            'detail_url': url, 'status': 'Pending',
            'list_price': 500_000, 'address': '4 Main', 'city': 'Saco',
        })
        rows = _history_rows(conn, url)
        assert len(rows) == 2
        assert rows[0][0] == 'Active'
        assert rows[1][0] == 'Pending'

    def test_direct_call_to_helper_works(self, conn):
        """write_history_if_changed is callable directly (used by sweeper)."""
        url = 'https://mainelistings.com/listings/x5'
        upsert_listing(conn, {
            'detail_url': url, 'status': 'Active',
            'list_price': 600_000, 'address': '5 Main', 'city': 'York',
        })
        # Simulate sweeper marking as Withdrawn
        wrote = write_history_if_changed(conn, url, 'Withdrawn', 600_000)
        assert wrote is True
        rows = _history_rows(conn, url)
        assert len(rows) == 2
        assert rows[1][0] == 'Withdrawn'

    def test_dom_not_in_history_columns(self, conn):
        """days_on_market deliberately excluded from history schema."""
        cols = {r[1] for r in conn.execute(
            'PRAGMA table_info(maine_listing_history)'
        ).fetchall()}
        assert 'days_on_market' not in cols


class TestUpsertStampsLastSeen:
    def test_last_seen_set_on_insert(self, conn):
        url = 'https://mainelistings.com/listings/ls1'
        upsert_listing(conn, {
            'detail_url': url, 'status': 'Active',
            'list_price': 500_000, 'address': 'A', 'city': 'Kittery',
        })
        row = conn.execute(
            'SELECT last_seen_at FROM maine_transactions WHERE detail_url = ?',
            (url,),
        ).fetchone()
        assert row[0] is not None
        assert len(row[0]) > 10  # ISO timestamp

    def test_last_seen_refreshed_on_reupsert(self, conn):
        url = 'https://mainelistings.com/listings/ls2'
        upsert_listing(conn, {
            'detail_url': url, 'status': 'Active', 'list_price': 500_000,
            'address': 'B', 'city': 'Kittery',
        })
        first = conn.execute(
            'SELECT last_seen_at FROM maine_transactions WHERE detail_url = ?',
            (url,),
        ).fetchone()[0]
        import time; time.sleep(0.01)  # ensure timestamp changes
        upsert_listing(conn, {
            'detail_url': url, 'status': 'Active', 'list_price': 500_000,
            'address': 'B', 'city': 'Kittery',
        })
        second = conn.execute(
            'SELECT last_seen_at FROM maine_transactions WHERE detail_url = ?',
            (url,),
        ).fetchone()[0]
        assert second > first
```

- [ ] **Step 2: Run test to verify it fails**

```bash
python -m pytest tests/test_maine_history.py -v
```

Expected: `ImportError: cannot import name 'write_history_if_changed'` or similar.

- [ ] **Step 3: Implement the helper + update upsert + enrich**

In `src/maine_database.py`, add the helper and replace `upsert_listing` + `enrich_listing`:

```python
def write_history_if_changed(
    conn: sqlite3.Connection,
    detail_url: str,
    status: str | None,
    list_price: int | None,
) -> bool:
    """Append a history row iff (status, list_price) differ from the most
    recent history row for this detail_url. No-op if nothing relevant known.

    Returns True if a row was written, False otherwise.
    """
    if not detail_url or (status is None and list_price is None):
        return False

    prev = conn.execute('''
        SELECT status, list_price FROM maine_listing_history
        WHERE detail_url = ?
        ORDER BY id DESC LIMIT 1
    ''', (detail_url,)).fetchone()

    if prev is not None:
        prev_status, prev_price = prev[0], prev[1]
        if prev_status == status and prev_price == list_price:
            return False  # unchanged, skip

    now = datetime.utcnow().isoformat()
    conn.execute('''
        INSERT INTO maine_listing_history
            (detail_url, snapshot_date, status, list_price)
        VALUES (?, ?, ?, ?)
    ''', (detail_url, now, status, list_price))
    conn.commit()
    return True


def upsert_listing(conn: sqlite3.Connection, record: dict) -> bool:
    """Insert or update a listing discovered from search pages.

    Accepts `status` and `list_price` in the record. Always stamps
    last_seen_at = now. Writes a history row if (status, list_price)
    changed vs. the previous snapshot.
    """
    now = datetime.utcnow().isoformat()
    try:
        conn.execute('''
            INSERT INTO maine_transactions (
                address, city, state, zip,
                sale_price, list_price,
                beds, baths, sqft, listing_office,
                detail_url, status,
                discovered_at, scraped_at, last_seen_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(detail_url) DO UPDATE SET
                sale_price = COALESCE(excluded.sale_price, maine_transactions.sale_price),
                list_price = COALESCE(excluded.list_price, maine_transactions.list_price),
                listing_office = COALESCE(excluded.listing_office, maine_transactions.listing_office),
                status = COALESCE(excluded.status, maine_transactions.status),
                last_seen_at = excluded.last_seen_at
        ''', (
            record.get('address'), record.get('city'), record.get('state', 'ME'),
            record.get('zip'),
            record.get('sale_price'), record.get('list_price'),
            record.get('beds'), record.get('baths'), record.get('sqft'),
            record.get('listing_office'), record.get('detail_url'),
            record.get('status'),
            now, now, now,
        ))
        conn.commit()
    except sqlite3.IntegrityError as e:
        logger.debug('Insert failed for %s: %s', record.get('detail_url'), e)
        return False

    write_history_if_changed(
        conn,
        record.get('detail_url'),
        record.get('status'),
        record.get('list_price'),
    )
    return True


def enrich_listing(conn: sqlite3.Connection, detail_url: str, data: dict) -> bool:
    """Update a listing with agent + attribute data from detail page scraping.

    Accepts the extended NUXT fields (status, list_date, year_built,
    lot_sqft, description, photo_url) in addition to the existing ones.
    Writes a history row if status/list_price changed.
    """
    now = datetime.utcnow().isoformat()
    conn.execute('''
        UPDATE maine_transactions SET
            mls_number = COALESCE(?, mls_number),
            listing_agent = ?,
            listing_agent_id = ?,
            listing_agent_email = ?,
            listing_office = COALESCE(?, listing_office),
            buyer_agent = ?,
            buyer_agent_id = ?,
            buyer_agent_email = ?,
            buyer_office = ?,
            close_date = ?,
            sale_price = COALESCE(?, sale_price),
            list_price = COALESCE(?, list_price),
            property_type = ?,
            days_on_market = ?,
            status = COALESCE(?, status),
            list_date = COALESCE(?, list_date),
            year_built = COALESCE(?, year_built),
            lot_sqft = COALESCE(?, lot_sqft),
            description = COALESCE(?, description),
            photo_url = COALESCE(?, photo_url),
            enrichment_status = 'success',
            enrichment_attempts = enrichment_attempts + 1,
            enriched_at = ?,
            last_seen_at = ?
        WHERE detail_url = ?
    ''', (
        data.get('mls_number'),
        data.get('listing_agent'), data.get('listing_agent_id'),
        data.get('listing_agent_email'), data.get('listing_office'),
        data.get('buyer_agent'), data.get('buyer_agent_id'),
        data.get('buyer_agent_email'), data.get('buyer_office'),
        data.get('close_date'), data.get('sale_price'),
        data.get('list_price'), data.get('property_type'),
        data.get('days_on_market'),
        data.get('status'),
        data.get('list_date'),
        data.get('year_built'), data.get('lot_sqft'),
        data.get('description'), data.get('photo_url'),
        now, now, detail_url,
    ))
    conn.commit()

    write_history_if_changed(
        conn, detail_url, data.get('status'), data.get('list_price'),
    )
    return True
```

- [ ] **Step 4: Run history tests**

```bash
python -m pytest tests/test_maine_history.py -v
```

Expected: all tests pass.

- [ ] **Step 5: Run full suite**

```bash
python -m pytest tests/ -q
```

Expected: 232 existing + new tests pass. Pay attention to any test that constructed records without `status` — those should still work (the column is nullable and the backfill only affects rows with close_date).

- [ ] **Step 6: Commit**

```bash
git add src/maine_database.py tests/test_maine_history.py
git commit -m "feat: change-detected history writes + last_seen_at stamping"
```

---

## Task A3: Backward-compat — add `status='Closed'` filter to existing queries

**Goal:** All existing leaderboard and KPI queries must filter to closed transactions only, so the addition of active/pending/withdrawn rows to `maine_transactions` doesn't poison the agent/brokerage rankings. Since the migration backfilled `status='Closed'` on existing rows, the filters are semantically a no-op today but become load-bearing once actives arrive.

**Files:**
- Modify: `src/maine_report.py` (3 SQL queries — query_top_agents, query_top_combined_agents, query_top_brokerages)
- Modify: `src/maine_kpis.py` (2 SQL queries — query_agent_kpis, query_brokerage_kpis)
- Modify: `tests/test_maine_kpis.py` (seed fixture needs `status='Closed'` on inserts OR verify migration backfill covers fixtures)

- [ ] **Step 1: Write the failing test**

Add to `tests/test_maine_kpis.py` at the bottom:

```python
class TestKPIQueriesIgnoreActiveRows:
    """KPI queries should only count closed transactions."""

    def test_active_listing_excluded_from_agent_kpis(self, kpi_conn):
        conn, today = kpi_conn
        # Insert an ACTIVE listing for Alice — should NOT affect her closed counts
        conn.execute('''
            INSERT INTO maine_transactions (
                detail_url, listing_agent, listing_office, city,
                list_price, close_date,
                enrichment_status, status,
                discovered_at, scraped_at
            ) VALUES (
                '/l/active-alice', 'Alice', 'Acme', 'Kittery',
                999_999_999, NULL,
                'success', 'Active',
                '2026-04-16', '2026-04-16'
            )
        ''')
        conn.commit()

        rows = query_agent_kpis(conn, today=today)
        alice = next(r for r in rows if r['name'] == 'Alice')
        # Alice's closed all-time volume is still 2,300,000 — the Active row
        # with $999_999_999 list_price must be excluded.
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
                999_999_999, NULL,
                'success', 'Active',
                '2026-04-16', '2026-04-16'
            )
        ''')
        conn.commit()

        rows = query_brokerage_kpis(conn, today=today)
        acme = next(r for r in rows if r['name'] == 'Acme')
        # Acme's all_time counts should not include the active row
        assert acme['all_time_sides'] == 5
```

Also add a similar test in `tests/test_maine_report.py`. Locate the section near `TestPopulatedReport` and add:

```python
class TestLeaderboardIgnoresActive:
    def test_top_agents_excludes_active(self, populated_conn):
        """An active listing for an agent shouldn't inflate their closed sides."""
        from src.maine_report import query_top_agents
        conn = populated_conn
        conn.execute('''
            INSERT INTO maine_transactions (
                detail_url, listing_agent, listing_office, city,
                list_price, close_date,
                enrichment_status, status,
                discovered_at, scraped_at
            ) VALUES (
                '/active-x', 'Jane Doe', 'Doe Realty', 'Kittery',
                5_000_000, NULL,
                'success', 'Active',
                '2026-04-16', '2026-04-16'
            )
        ''')
        conn.commit()

        rows = query_top_agents(conn, role='listing')
        jane = next((r for r in rows if r['agent_name'] == 'Jane Doe'), None)
        if jane is not None:
            # Active listing must NOT bump up her volume
            assert jane['volume'] < 5_000_000
```

(If `populated_conn` fixture or `Jane Doe` aren't in the existing file, mirror the pattern — the key assertion is "adding an Active row doesn't change the query result".)

- [ ] **Step 2: Run the new tests to verify they fail**

```bash
python -m pytest tests/test_maine_kpis.py::TestKPIQueriesIgnoreActiveRows -v
```

Expected: `test_active_listing_excluded_from_agent_kpis` and `test_active_row_excluded_from_brokerage_kpis` fail — `all_time_volume` comes back inflated because Active rows aren't filtered.

- [ ] **Step 3: Add the status filter to all five queries**

In `src/maine_kpis.py`, modify `query_agent_kpis` so both UNION halves include the status filter. Find the CTE:

```python
    sql = f'''
        WITH sides AS (
            SELECT listing_agent AS agent,
                   listing_office AS office,
                   'listing' AS role,
                   sale_price, city, close_date
            FROM maine_transactions
            WHERE enrichment_status = 'success'
              AND listing_agent IS NOT NULL AND TRIM(listing_agent) != ''
              {town_sql_listing}
            UNION ALL
            SELECT buyer_agent AS agent,
                   buyer_office AS office,
                   'buyer' AS role,
                   sale_price, city, close_date
            FROM maine_transactions
            WHERE enrichment_status = 'success'
              AND buyer_agent IS NOT NULL AND TRIM(buyer_agent) != ''
              {town_sql_buyer}
        ),
```

Change each inner `WHERE` to add `AND status = 'Closed'`:

```python
    sql = f'''
        WITH sides AS (
            SELECT listing_agent AS agent,
                   listing_office AS office,
                   'listing' AS role,
                   sale_price, city, close_date
            FROM maine_transactions
            WHERE enrichment_status = 'success'
              AND status = 'Closed'
              AND listing_agent IS NOT NULL AND TRIM(listing_agent) != ''
              {town_sql_listing}
            UNION ALL
            SELECT buyer_agent AS agent,
                   buyer_office AS office,
                   'buyer' AS role,
                   sale_price, city, close_date
            FROM maine_transactions
            WHERE enrichment_status = 'success'
              AND status = 'Closed'
              AND buyer_agent IS NOT NULL AND TRIM(buyer_agent) != ''
              {town_sql_buyer}
        ),
```

Apply the same change to `query_brokerage_kpis` (both UNION halves).

In `src/maine_report.py`, `_SUCCESS` is the shared fragment — update it:

```python
_SUCCESS = "enrichment_status = 'success' AND status = 'Closed'"
```

This one change propagates to all three queries (`query_top_agents`, `query_top_combined_agents`, `query_top_brokerages`) because they all reference `{_SUCCESS}`.

Also update the `_get_stats` query in `src/maine_report.py` lines 260-269 (the `SELECT COUNT(*)` block) — it's used in the markdown summary and should count closed only for consistency with the leaderboard. Find:

```python
    row = conn.execute('''
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN enrichment_status = 'success' THEN 1 ELSE 0 END) AS enriched,
            ...
        FROM maine_transactions
    ''').fetchone()
```

Change to:

```python
    row = conn.execute('''
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN enrichment_status = 'success' THEN 1 ELSE 0 END) AS enriched,
            SUM(CASE WHEN listing_agent IS NOT NULL THEN 1 ELSE 0 END) AS has_listing,
            SUM(CASE WHEN buyer_agent IS NOT NULL THEN 1 ELSE 0 END) AS has_buyer,
            MIN(close_date) AS date_min,
            MAX(close_date) AS date_max
        FROM maine_transactions
        WHERE status = 'Closed'
    ''').fetchone()
```

And the per-town `town_rows` query right below it — add `AND status = 'Closed'` to its `WHERE`:

```python
    town_rows = conn.execute('''
        SELECT city, COUNT(*) AS n
        FROM maine_transactions
        WHERE enrichment_status = 'success'
          AND status = 'Closed'
          AND city IS NOT NULL
        GROUP BY city ORDER BY n DESC
    ''').fetchall()
```

- [ ] **Step 4: Update fixture seeds — make `status='Closed'` explicit**

The existing `kpi_conn` fixture at `tests/test_maine_kpis.py:105-147` seeds rows without a status. Even though the migration backfills `status='Closed'` where `close_date IS NOT NULL`, the backfill runs inside `init_db` — fixtures insert AFTER init_db, so new rows have `status=NULL` and would be filtered out by the new `status = 'Closed'` clause.

Fix: add `status='Closed'` to the _insert helper in the fixture:

```python
    def _insert(days_ago, listing_agent, listing_office, buyer_agent, buyer_office, price, city, url_suffix):
        close_date = (anchor - timedelta(days=days_ago)).isoformat()
        c.execute('''
            INSERT INTO maine_transactions (
                detail_url, listing_agent, listing_office,
                buyer_agent, buyer_office, city,
                sale_price, close_date,
                enrichment_status, status,
                discovered_at, scraped_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'success', 'Closed', ?, ?)
        ''', (f'/l/{url_suffix}', listing_agent, listing_office,
              buyer_agent, buyer_office, city, price, close_date,
              close_date, close_date))
```

Also the one-off inserts in `test_exclusions_respected` (test_maine_kpis.py lines 207-213) and `test_maine_report.py` (look for any direct INSERTs into maine_transactions in existing tests and add `status='Closed'` to them).

Run the exploratory grep first:

```bash
grep -n 'INTO maine_transactions' tests/
```

Expected: multiple test files with direct inserts. Every one of them needs `status='Closed'` added if the test data is supposed to represent closed transactions (which is all of them until the new active-listings tests we're adding).

- [ ] **Step 5: Run full suite**

```bash
python -m pytest tests/ -q
```

Expected: 232 existing + new tests pass. If anything fails, it's almost certainly a seed-data issue — the test inserts a closed-style row but didn't set `status`. Grep for the failing test's INSERT statement and add `status='Closed'`.

- [ ] **Step 6: Commit**

```bash
git add src/maine_report.py src/maine_kpis.py tests/test_maine_kpis.py tests/test_maine_report.py
git commit -m "feat: restrict leaderboard + KPI queries to status='Closed'"
```

---

## Task B1: Active card regex + `status` parameter on `parse_search_cards`

**Goal:** Teach `parse_search_cards` to handle Active-listing cards (which show "Active" or "New Listing" instead of "Closed" in the markdown). Add a `status` parameter that selects the regex and correctly maps the card's price to `list_price` (Active) vs. `sale_price` (Closed).

**Files:**
- Modify: `src/maine_parser.py` (regex + parse function)
- Test: `tests/test_maine_active_discovery.py` (new)

- [ ] **Step 1: Write the failing test**

Create `tests/test_maine_active_discovery.py`:

```python
"""Tests for active-listings discovery plumbing."""
from __future__ import annotations

from src.maine_parser import parse_search_cards


# Real mainelistings.com search cards have this markdown shape.
# The status word varies: "Closed", "Active", "New Listing", "Pending".

CLOSED_FIXTURE = r'''
[![Thumb](https://example.com/a.jpg)
$ 750,000 Closed\\ \\
**123 Ocean Ave** **Kittery, ME 03904**\\ \\
3 Beds\\ \\
2 Baths\\ \\
1,800 sqft\\ \\
Brought to you by ACME Realty](https://mainelistings.com/listings/abc-123)
'''

ACTIVE_FIXTURE = r'''
[![Thumb](https://example.com/b.jpg)
$ 875,000 Active\\ \\
**77 Shore Rd** **York, ME 03909**\\ \\
4 Beds\\ \\
3 Baths\\ \\
2,400 sqft\\ \\
Brought to you by Beach Realty](https://mainelistings.com/listings/def-456)
'''

NEW_LISTING_FIXTURE = r'''
[![Thumb](https://example.com/c.jpg)
$ 1,200,000 New Listing\\ \\
**9 Dock St** **Kennebunkport, ME 04046**\\ \\
5 Beds\\ \\
4 Baths\\ \\
3,100 sqft\\ \\
Brought to you by Coastal Homes](https://mainelistings.com/listings/ghi-789)
'''


class TestParseClosedCards:
    def test_closed_status_still_works(self):
        cards = parse_search_cards(CLOSED_FIXTURE, status='Closed')
        assert len(cards) == 1
        c = cards[0]
        assert c['sale_price'] == 750_000
        assert c['status'] == 'Closed'
        assert c['address'] == '123 Ocean Ave'
        assert c['city'] == 'Kittery'
        assert 'list_price' not in c or c['list_price'] is None

    def test_default_status_is_closed_for_back_compat(self):
        """Old callers that don't pass status should still get closed parsing."""
        cards = parse_search_cards(CLOSED_FIXTURE)
        assert len(cards) == 1
        assert cards[0]['sale_price'] == 750_000


class TestParseActiveCards:
    def test_active_card_parsed(self):
        cards = parse_search_cards(ACTIVE_FIXTURE, status='Active')
        assert len(cards) == 1
        c = cards[0]
        assert c['list_price'] == 875_000
        assert c['sale_price'] is None  # actives have no sale price
        assert c['status'] == 'Active'
        assert c['address'] == '77 Shore Rd'
        assert c['city'] == 'York'
        assert c['beds'] == 4
        assert c['baths'] == 3
        assert c['sqft'] == 2_400
        assert c['detail_url'] == 'https://mainelistings.com/listings/def-456'

    def test_new_listing_badge_parses_as_active(self):
        """Some active cards carry a 'New Listing' badge instead of 'Active'."""
        cards = parse_search_cards(NEW_LISTING_FIXTURE, status='Active')
        assert len(cards) == 1
        assert cards[0]['list_price'] == 1_200_000
        assert cards[0]['status'] == 'Active'

    def test_active_parser_ignores_closed_cards(self):
        """When scraping an Active page, Closed cards mixed in (shouldn't
        happen but defensive) are not returned."""
        mixed = CLOSED_FIXTURE + ACTIVE_FIXTURE
        cards = parse_search_cards(mixed, status='Active')
        urls = [c['detail_url'] for c in cards]
        assert 'https://mainelistings.com/listings/def-456' in urls
        assert 'https://mainelistings.com/listings/abc-123' not in urls


class TestURLBuilder:
    """discover_listings must emit different URLs for Active vs Closed."""
    def test_url_for_closed(self):
        from src.maine_firecrawl import build_search_url
        url = build_search_url(town='Kittery', page=1, status='Closed')
        assert 'mls_status=Closed' in url
        assert 'city=Kittery' in url

    def test_url_for_active(self):
        from src.maine_firecrawl import build_search_url
        url = build_search_url(town='York', page=1, status='Active')
        assert 'mls_status=Active' in url
        assert 'city=York' in url

    def test_pagination_preserved(self):
        from src.maine_firecrawl import build_search_url
        u1 = build_search_url(town='Wells', page=1, status='Active')
        u3 = build_search_url(town='Wells', page=3, status='Active')
        assert '&page=' not in u1
        assert '&page=3' in u3
```

- [ ] **Step 2: Run test to verify it fails**

```bash
python -m pytest tests/test_maine_active_discovery.py -v
```

Expected: `parse_search_cards` TypeError (no `status` kwarg) OR `build_search_url` ImportError.

- [ ] **Step 3: Implement the parser changes**

In `src/maine_parser.py`, replace the `_CARD_RE` block and `parse_search_cards` with:

```python
# === Search Results Parsing ===

def _make_card_re(status_pattern: str) -> re.Pattern:
    """Build a regex matching search-result cards for the given status.

    status_pattern is a regex fragment (e.g. 'Closed' or 'Active|New Listing').
    The resulting regex captures eight groups:
        1=price  2=status  3=address  4=city+state+zip
        5=beds   6=baths   7=sqft     8=detail_url
    """
    return re.compile(
        r'\$\s*([\d,]+)\s*(' + status_pattern + r')\\\\\s*\\\\\s*'
        r'\*\*([^*]+)\*\*\s+\*\*([^*]+)\*\*\\\\\s*\\\\\s*'
        r'(\d+)\s+Beds?\\\\\s*\\\\\s*'
        r'(\d+)\s+Baths?\\\\\s*\\\\\s*'
        r'([\d,]+)\s+sqft\\\\\s*\\\\\s*'
        r'Brought to you by\s+([^\]]+?)\]'
        r'\((https://mainelistings\.com/listings/[^)]+)\)',
        re.DOTALL,
    )


_CLOSED_CARD_RE = _make_card_re(r'Closed')
# "Active" and "New Listing" are both active-state badges we treat as Active.
# "Pending" has its own badge — it can show up on active-search pages when a
# listing goes under contract. Parse it as Pending (distinct status value).
_ACTIVE_CARD_RE = _make_card_re(r'Active|New Listing|Pending')

_PAGINATION_RE = re.compile(r'(\d+)\s+of\s+(\d+)')
_TOTAL_RESULTS_RE = re.compile(r'([\d,]+)\s+Results')


def parse_search_cards(markdown: str, status: str = 'Closed') -> list[dict]:
    """Parse listing cards from search results markdown.

    Args:
        markdown: the markdown response from a mainelistings.com search page.
        status: 'Closed' parses sold cards (price → sale_price).
                'Active' parses live cards (price → list_price).
                Cards whose badge is 'Pending' get status='Pending'.

    Returns a list of card dicts. Each dict has a `status` key matching the
    literal badge seen, so callers can route Pending rows correctly.
    """
    if status == 'Closed':
        card_re = _CLOSED_CARD_RE
    else:
        card_re = _ACTIVE_CARD_RE

    cards: list[dict] = []
    for m in card_re.finditer(markdown):
        price_str = m.group(1).replace(',', '')
        price = int(price_str) if price_str else None
        badge = m.group(2).strip()
        address = m.group(3).strip()
        city_state_zip = m.group(4).strip()
        city, state, zip_code = _parse_city_state_zip(city_state_zip)

        card = {
            'status': 'Active' if badge == 'New Listing' else badge,
            'sale_price': price if status == 'Closed' else None,
            'list_price': price if status != 'Closed' else None,
            'address': address,
            'city': city,
            'state': state,
            'zip': zip_code,
            'beds': int(m.group(5)),
            'baths': int(m.group(6)),
            'sqft': int(m.group(7).replace(',', '')),
            'listing_office': m.group(8).strip(),
            'detail_url': m.group(9).strip(),
        }
        cards.append(card)
    return cards
```

(Note: the existing `_CARD_RE` captured 8 groups, but the new regex captures 9 because of the new `status` group at position 2. Updated indices throughout.)

- [ ] **Step 4: Implement `build_search_url` in `src/maine_firecrawl.py`**

In `src/maine_firecrawl.py`, add the helper above `discover_listings`:

```python
def build_search_url(*, town: str, page: int, status: str = 'Closed') -> str:
    """Compose a mainelistings.com search URL for one town + status + page.

    Factored out of the discovery loop so tests can assert on URLs without
    invoking Firecrawl.
    """
    url = f'{_SEARCH_URL}?city={town}&mls_status={status}'
    if page > 1:
        url += f'&page={page}'
    return url
```

Then replace the URL construction inside `_discover_town` (around line 192) to call the helper:

```python
        url = build_search_url(town=town, page=page_num, status='Closed')
```

We'll thread `status` through `_discover_town` itself in Task B3 — for now this task is only about the regex + URL builder existing.

- [ ] **Step 5: Run discovery tests**

```bash
python -m pytest tests/test_maine_active_discovery.py -v
```

Expected: all pass.

- [ ] **Step 6: Run full suite**

```bash
python -m pytest tests/ -q
```

Expected: 232 existing + new tests pass. The existing `tests/test_maine_parser.py` tests continue to pass because `parse_search_cards(markdown)` defaults to `status='Closed'` (back-compat).

- [ ] **Step 7: Commit**

```bash
git add src/maine_parser.py src/maine_firecrawl.py tests/test_maine_active_discovery.py
git commit -m "feat: active-listings search card parser + URL builder"
```

---

## Task B2: NUXT JS extraction — list_date, year_built, lot_sqft, description, photo_url, mls_status

**Goal:** Extend `DETAIL_EXTRACT_JS` to capture the new fields the spec requires. Verify against a real active listing page before committing.

**Files:**
- Modify: `src/maine_parser.py` (DETAIL_EXTRACT_JS)
- Test: `tests/test_maine_parser.py` (add NUXT-blob fixtures for new fields)

**Context for implementer:** the current JS scans for regex patterns inside NUXT `<script>` blocks. Each new field adds one regex. If a field is missing from the blob, the regex returns null — the DB stores null — no breakage. The *biggest risk* is naming: mainelistings.com may use `YearBuilt` vs `year_built` vs `year_built_number`. This task includes a verification step that fetches a real active listing to confirm field names.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_maine_parser.py`:

```python
# NUXT blob fixture for an active listing, capturing the fields
# we expect to extract. Field names reflect the actual blob on
# mainelistings.com as inspected on 2026-04-17 (see Step 4 verification).

ACTIVE_NUXT_VALUE = (
    '{"mls_status":"Active",'
    '"listing_id":"1580001",'
    '"list_price":"850000",'
    '"close_price":"0",'
    '"list_date":"2026-03-15T00:00:00Z",'
    '"close_date":"",'
    '"days_on_market":33,'
    '"property_sub_type":"Single Family Residence",'
    '"year_built":1987,'
    '"lot_sqft":15000,'
    '"public_remarks":"Stunning oceanfront retreat with private dock.",'
    '"photo":"https://photos.mainelistings.com/abc/hero.jpg",'
    '"list_agent_full_name":"Jane Agent",'
    '"list_agent_mls_id":"12345",'
    '"list_agent_email":"jane@example.com",'
    '"list_office_name":"Beach Realty",'
    '"buyer_agent_full_name":"",'
    '"buyer_agent_mls_id":"",'
    '"buyer_agent_email":"",'
    '"buyer_office_name":""}'
)


class TestParseActiveListingFields:
    def test_extracts_list_date(self):
        parsed = parse_detail_response(ACTIVE_NUXT_VALUE)
        assert parsed['list_date'] == '2026-03-15'  # date portion only

    def test_extracts_year_built(self):
        parsed = parse_detail_response(ACTIVE_NUXT_VALUE)
        assert parsed['year_built'] == 1987

    def test_extracts_lot_sqft(self):
        parsed = parse_detail_response(ACTIVE_NUXT_VALUE)
        assert parsed['lot_sqft'] == 15000

    def test_extracts_description(self):
        parsed = parse_detail_response(ACTIVE_NUXT_VALUE)
        assert parsed['description'] == 'Stunning oceanfront retreat with private dock.'

    def test_extracts_photo_url(self):
        parsed = parse_detail_response(ACTIVE_NUXT_VALUE)
        assert parsed['photo_url'] == 'https://photos.mainelistings.com/abc/hero.jpg'

    def test_extracts_mls_status(self):
        parsed = parse_detail_response(ACTIVE_NUXT_VALUE)
        assert parsed['status'] == 'Active'

    def test_active_has_no_close_date(self):
        parsed = parse_detail_response(ACTIVE_NUXT_VALUE)
        # Empty string in blob → we coerce to None
        assert not parsed['close_date']
```

Note: the existing parser runs the `DETAIL_EXTRACT_JS` on a real browser DOM via Firecrawl's `executeJavascript` action. For tests, we exercise the parser's Python side only — `parse_detail_response` receives the already-returned JSON blob. So these unit tests do NOT run the JS itself. The JS is covered by a manual verification step (Step 4 below) against a real page.

To make the unit tests work, we need the Python-side `parse_detail_response` to apply the same extraction logic to the fixture. Looking at the current flow:

```
mainelistings.com detail page
    ↓ (Firecrawl scrape)
result.actions['javascriptReturns'][0]  ← JS already ran, returned JSON string
    ↓ (parse_detail_response)
dict
```

The JS in `DETAIL_EXTRACT_JS` builds a JSON object by regex-matching the NUXT blob inline. For unit testing, we need the same *fields* to come through — not the same *regex*. The fixture we pass is the JSON already-built.

**Revised fixture approach:** the test fixture is what the JS *returns* (a JSON object), not the raw blob. Rewrite the fixture:

```python
# What the browser JS returns after extracting from the NUXT blob.
ACTIVE_JS_RETURN = (
    '{"mls_status":"Active",'
    '"status":"Active",'
    '"mls_number":"1580001",'
    '"list_price":850000,'
    '"sale_price":null,'
    '"close_date":null,'
    '"list_date":"2026-03-15",'
    '"days_on_market":33,'
    '"property_type":"Single Family Residence",'
    '"year_built":1987,'
    '"lot_sqft":15000,'
    '"description":"Stunning oceanfront retreat with private dock.",'
    '"photo_url":"https://photos.mainelistings.com/abc/hero.jpg",'
    '"listing_agent":"Jane Agent",'
    '"listing_agent_id":"12345",'
    '"listing_agent_email":"jane@example.com",'
    '"listing_office":"Beach Realty",'
    '"buyer_agent":null,'
    '"buyer_agent_id":null,'
    '"buyer_agent_email":null,'
    '"buyer_office":null}'
)


class TestParseActiveListingFields:
    def test_extracts_list_date(self):
        parsed = parse_detail_response(ACTIVE_JS_RETURN)
        assert parsed['list_date'] == '2026-03-15'

    def test_extracts_year_built(self):
        parsed = parse_detail_response(ACTIVE_JS_RETURN)
        assert parsed['year_built'] == 1987

    def test_extracts_lot_sqft(self):
        parsed = parse_detail_response(ACTIVE_JS_RETURN)
        assert parsed['lot_sqft'] == 15000

    def test_extracts_description(self):
        parsed = parse_detail_response(ACTIVE_JS_RETURN)
        assert parsed['description'] == 'Stunning oceanfront retreat with private dock.'

    def test_extracts_photo_url(self):
        parsed = parse_detail_response(ACTIVE_JS_RETURN)
        assert parsed['photo_url'] == 'https://photos.mainelistings.com/abc/hero.jpg'

    def test_extracts_status(self):
        parsed = parse_detail_response(ACTIVE_JS_RETURN)
        assert parsed['status'] == 'Active'
```

These tests only check that `parse_detail_response` passes these fields through. The JS-side extraction is covered by Step 4 (manual verification).

- [ ] **Step 2: Run the new Python-side tests — they should pass already**

```bash
python -m pytest tests/test_maine_parser.py::TestParseActiveListingFields -v
```

Expected: all pass — because `parse_detail_response` is just JSON decoding + `_decode_escapes`; any key the JS-returned JSON contains is passed through.

If any test fails, something is wrong with the existing `parse_detail_response` (unlikely) and requires investigation before moving on.

- [ ] **Step 3: Extend the JS to actually emit these fields**

In `src/maine_parser.py`, replace the `DETAIL_EXTRACT_JS` constant:

```python
DETAIL_EXTRACT_JS = '''(function(){
    var scripts = document.querySelectorAll('script');
    var result = {error: null};

    for (var i = 0; i < scripts.length; i++) {
        var txt = scripts[i].textContent;
        if (txt.indexOf('buyer_agent_full_name') < 0) continue;

        // === Agents + offices (unchanged) ===
        var ba = /buyer_agent_full_name:"([^"]*)"/.exec(txt);
        var baId = /buyer_agent_mls_id:"([^"]*)"/.exec(txt);
        var baEmail = /buyer_agent_email:"([^"]*)"/.exec(txt);
        result.buyer_agent = ba ? ba[1] : null;
        result.buyer_agent_id = baId ? baId[1] : null;
        result.buyer_agent_email = baEmail ? baEmail[1] : null;

        var bo = /buyer_office_name:"([^"]*)"/.exec(txt);
        result.buyer_office = bo ? bo[1] : null;

        var laMatches = txt.match(/list_agent_full_name:"([^"]*)"/g) || [];
        for (var j = 0; j < laMatches.length; j++) {
            var name = /"([^"]*)"/.exec(laMatches[j]);
            if (name && name[1]) { result.listing_agent = name[1]; break; }
        }
        var laId = txt.match(/list_agent_mls_id:"([^"]*)"/g) || [];
        for (var k = 0; k < laId.length; k++) {
            var id = /"([^"]*)"/.exec(laId[k]);
            if (id && id[1]) { result.listing_agent_id = id[1]; break; }
        }
        var laEmail = txt.match(/list_agent_email:"([^"]*)"/g) || [];
        for (var m = 0; m < laEmail.length; m++) {
            var em = /"([^"]*)"/.exec(laEmail[m]);
            if (em && em[1]) { result.listing_agent_email = em[1]; break; }
        }

        var loMatches = txt.match(/list_office_name:"([^"]*)"/g) || [];
        for (var n = 0; n < loMatches.length; n++) {
            var oname = /"([^"]*)"/.exec(loMatches[n]);
            if (oname && oname[1]) { result.listing_office = oname[1]; break; }
        }

        // === Transaction details (extended) ===
        var cp = /close_price:"([^"]*)"/.exec(txt);
        var lp = /list_price:"([^"]*)"/.exec(txt);
        var cd = /close_date:"([^"]*)"/.exec(txt);
        var ld = /list_date:"([^"]*)"/.exec(txt);
        var li = /listing_id:"([^"]*)"/.exec(txt);
        var dom = /days_on_market:(\\d+)/.exec(txt);
        var pst = /property_sub_type:"([^"]*)"/.exec(txt);
        var ms = /mls_status:"([^"]*)"/.exec(txt);

        result.sale_price = cp && cp[1] ? parseInt(cp[1]) : null;
        result.list_price = lp && lp[1] ? parseInt(lp[1]) : null;
        result.close_date = cd && cd[1] ? cd[1].split('T')[0] : null;
        result.list_date  = ld && ld[1] ? ld[1].split('T')[0] : null;
        result.mls_number = li ? li[1] : null;
        result.days_on_market = dom ? parseInt(dom[1]) : null;
        result.property_type = pst ? pst[1] : null;
        result.status = ms ? ms[1] : null;

        // === New: property attributes ===
        var yb = /year_built:(\\d+)/.exec(txt);
        var ls = /lot_sqft:(\\d+)/.exec(txt);
        var pr = /public_remarks:"((?:[^"\\\\]|\\\\.)*)"/.exec(txt);
        var ph = /photo:"([^"]*)"/.exec(txt);

        result.year_built = yb ? parseInt(yb[1]) : null;
        result.lot_sqft = ls ? parseInt(ls[1]) : null;
        result.description = pr ? pr[1] : null;
        result.photo_url = ph ? ph[1] : null;

        break;
    }

    if (!result.listing_agent && !result.buyer_agent) {
        result.error = 'no agent data found in NUXT blob';
    }

    return JSON.stringify(result);
})()'''
```

- [ ] **Step 4: Manual verification against a real active listing**

This step protects against field-name guessing. Fetch one real active listing via the Firecrawl CLI and confirm the JS pulls the new fields.

```bash
python - <<'PY'
import os, json
from firecrawl import Firecrawl
from src.maine_parser import DETAIL_EXTRACT_JS, parse_detail_response

client = Firecrawl(api_key=os.environ['FIRECRAWL_API_KEY'])

# Find one active listing URL by scraping the Kittery active search page
idx = client.scrape(
    'https://mainelistings.com/listings?city=Kittery&mls_status=Active',
    formats=['markdown'],
    wait_for=8000,
)
import re
m = re.search(r'https://mainelistings\.com/listings/[^)]+', idx.markdown)
assert m, 'could not find an active listing URL on Kittery active page'
url = m.group(0)
print('Testing against:', url)

res = client.scrape(
    url,
    formats=['rawHtml'],
    wait_for=8000,
    actions=[
        {'type': 'wait', 'milliseconds': 5000},
        {'type': 'executeJavascript', 'script': DETAIL_EXTRACT_JS},
    ],
)
js_ret = res.actions['javascriptReturns'][0]
parsed = parse_detail_response(js_ret)
print(json.dumps(parsed, indent=2))

# Assert the new fields actually populated (active listings should have these)
for field in ('status', 'list_date', 'year_built', 'lot_sqft',
              'description', 'photo_url'):
    assert parsed.get(field) is not None, (
        f'field {field} did NOT populate from a real active listing. '
        f'The field name in the NUXT blob is likely different from what the '
        f'JS regex looks for. Inspect the rawHtml and adjust.'
    )
print('\\n✅ All new fields populate from a real active listing.')
PY
```

If the assertion fires for any field, grep the rawHtml for a likely field name (e.g., `grep -i 'yearbuilt\\|year-built\\|YearBuilt'`) and update the regex in `DETAIL_EXTRACT_JS` accordingly. Common alternates to try:
- `year_built` → `YearBuilt`, `BuiltYear`
- `lot_sqft` → `LotSqft`, `lot_size_square_feet`, `lot_size_sqft`, `lot_size_area`
- `public_remarks` → `PublicRemarks`, `marketing_remarks`, `remarks`
- `photo` → `photos` (array — take the first), `primary_photo`, `media_url`, `hero_image`

- [ ] **Step 5: Run full suite**

```bash
python -m pytest tests/ -q
```

Expected: 232 existing + the new parser tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/maine_parser.py tests/test_maine_parser.py
git commit -m "feat: extend NUXT extraction with list_date, year_built, lot_sqft, description, photo, status"
```

---

## Task B3: Discovery wiring — thread `status` through end-to-end

**Goal:** `discover_listings(status='Active')` should scrape the active search URLs, pass active cards through `parse_search_cards(status='Active')`, and upsert rows with the right status.

**Files:**
- Modify: `src/maine_firecrawl.py` (`discover_listings`, `_run_town`, `_discover_town`)
- Modify: `src/maine_main.py` (plumb `--status` to `discover_listings`)
- Modify: `tests/test_maine_active_discovery.py` (add integration test)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_maine_active_discovery.py`:

```python
from unittest.mock import patch, MagicMock

from src.maine_database import get_connection, init_db
from src.maine_firecrawl import discover_listings


class TestDiscoverListingsWithStatus:
    """End-to-end: discover_listings with status='Active' writes Active rows."""

    @patch('src.maine_firecrawl._get_client')
    def test_active_run_writes_active_rows(self, mock_get_client, tmp_path):
        # Mock Firecrawl to return our ACTIVE_FIXTURE markdown
        mock_result = MagicMock()
        mock_result.markdown = ACTIVE_FIXTURE  # only one card
        mock_client = MagicMock()
        mock_client.scrape.return_value = mock_result
        mock_get_client.return_value = mock_client

        conn = get_connection(str(tmp_path / 'd.db'))
        init_db(conn)
        state = {'towns': {}}

        result = discover_listings(
            conn, state,
            towns=['Kittery'],
            max_pages=1,
            status='Active',
            workers=1,
        )

        assert result['listings'] >= 1
        rows = conn.execute(
            "SELECT status, list_price, sale_price FROM maine_transactions"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == 'Active'
        assert rows[0][1] == 875_000  # list_price from ACTIVE_FIXTURE
        assert rows[0][2] is None     # sale_price is null for actives

    @patch('src.maine_firecrawl._get_client')
    def test_closed_run_still_writes_closed_rows(self, mock_get_client, tmp_path):
        """Regression: back-compat path with status defaulting or status='Closed'."""
        mock_result = MagicMock()
        mock_result.markdown = CLOSED_FIXTURE
        mock_client = MagicMock()
        mock_client.scrape.return_value = mock_result
        mock_get_client.return_value = mock_client

        conn = get_connection(str(tmp_path / 'd2.db'))
        init_db(conn)
        state = {'towns': {}}

        result = discover_listings(
            conn, state, towns=['Kittery'], max_pages=1, workers=1,
        )  # default status='Closed'

        rows = conn.execute(
            "SELECT status, sale_price FROM maine_transactions"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == 'Closed'
        assert rows[0][1] == 750_000
```

- [ ] **Step 2: Run test to verify it fails**

```bash
python -m pytest tests/test_maine_active_discovery.py::TestDiscoverListingsWithStatus -v
```

Expected: TypeError — `discover_listings` doesn't accept `status` kwarg yet.

- [ ] **Step 3: Implement**

In `src/maine_firecrawl.py`, update the signatures and URL:

1. `discover_listings` signature:

```python
def discover_listings(
    conn,
    state: dict,
    *,
    towns: list[str] | None = None,
    max_pages: int = 90,
    recent_only: bool = False,
    state_path: str | None = None,
    workers: int = 1,
    status: str = 'Closed',
) -> dict:
```

2. Thread `status` through to `_run_town` and `_discover_town`. In both the serial and concurrent branches, pass `status` into `_run_town(...)`:

```python
    if workers <= 1:
        client = _get_client()
        for town in towns_to_process:
            count = _run_town(
                client, conn, state, town, max_pages, recent_only,
                state_path, state_lock, db_lock, status,
            )
            ...
```

and:

```python
                fut = pool.submit(
                    _run_town, client, thread_conn, state, town, max_pages,
                    recent_only, state_path, state_lock, db_lock, status,
                )
```

3. Update `_run_town` signature to accept `status` and pass it into `_discover_town`:

```python
def _run_town(
    client, conn, state, town, max_pages, recent_only,
    state_path, state_lock, db_lock, status='Closed',
) -> int | None:
    ...
    try:
        count = _discover_town(
            client, conn, town, max_pages, recent_only, db_lock, status,
        )
```

4. Update `_discover_town` to use `build_search_url(status=...)` and pass status to `parse_search_cards`:

```python
def _discover_town(
    client, conn, town: str, max_pages: int, recent_only: bool,
    db_lock: threading.Lock, status: str = 'Closed',
) -> int:
    town_count = 0

    for page_num in range(1, max_pages + 1):
        url = build_search_url(town=town, page=page_num, status=status)

        logger.info('Discovering %s page %d (%s)...', town, page_num, status)
        try:
            result = _scrape(client, url, 'markdown')
        except Exception as exc:
            logger.warning('  %s page %d failed: %s', town, page_num, exc)
            if page_num == 1:
                raise
            break

        markdown = getattr(result, 'markdown', '') or ''
        cards = parse_search_cards(markdown, status=status)
        if not cards and page_num > 1:
            logger.info('No cards on page %d for %s, stopping', page_num, town)
            break

        new_count = 0
        with db_lock:
            for card in cards:
                if not url_exists(conn, card['detail_url']):
                    new_count += 1
                upsert_listing(conn, card)

        town_count += len(cards)

        if recent_only and new_count == 0 and page_num > 1:
            logger.info('All %d cards on page %d already known, stopping',
                        len(cards), page_num)
            break

        page_info = parse_pagination(markdown)
        if page_info and page_num >= page_info[1]:
            logger.info('Reached last page (%d of %d)', page_num, page_info[1])
            break

    return town_count
```

5. In `src/maine_main.py`, add the `--status` CLI flag to the parser:

```python
    parser.add_argument('--status', type=str, default='Closed',
                        choices=['Active', 'Closed'],
                        help='Listing status to discover (default: Closed)')
```

and pass it through to `discover_listings`:

```python
    if args.discover:
        logger.info('Starting Maine Listings discovery (status=%s, max_pages=%d, recent_only=%s, workers=%d)...',
                     args.status, args.max_pages, args.recent_only, args.workers)
        result = discover_listings(
            conn, state,
            towns=towns,
            max_pages=args.max_pages,
            recent_only=args.recent_only,
            state_path=args.state,
            workers=args.workers,
            status=args.status,
        )
```

- [ ] **Step 4: Run the new discovery tests**

```bash
python -m pytest tests/test_maine_active_discovery.py -v
```

Expected: all pass.

- [ ] **Step 5: Run full suite**

```bash
python -m pytest tests/ -q
```

Expected: 232 existing + new tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/maine_firecrawl.py src/maine_main.py tests/test_maine_active_discovery.py
git commit -m "feat: thread status through discovery + CLI --status flag"
```

---

## Task C1: Withdrawn sweeper

**Goal:** Mark any `Active` or `Pending` listing whose `last_seen_at` is older than a threshold as `status='Withdrawn'` and write a history row. Run this at the end of each daily-active cycle.

**Files:**
- Modify: `src/maine_database.py` (add `mark_withdrawn_stale` helper)
- Modify: `src/maine_main.py` (call sweeper after daily-active discovery)
- Test: `tests/test_maine_active_sweeper.py` (new)

- [ ] **Step 1: Write the failing test**

Create `tests/test_maine_active_sweeper.py`:

```python
"""Tests for the withdrawn-listing sweeper."""
from __future__ import annotations

from datetime import datetime, timedelta
import pytest

from src.maine_database import (
    get_connection,
    init_db,
    mark_withdrawn_stale,
    upsert_listing,
)


@pytest.fixture
def conn(tmp_path):
    c = get_connection(str(tmp_path / 's.db'))
    init_db(c)
    return c


def _insert_with_last_seen(conn, *, url, status, days_ago):
    """Insert a listing and then manually set last_seen_at N days in the past."""
    upsert_listing(conn, {
        'detail_url': url, 'status': status, 'list_price': 500_000,
        'address': 'X', 'city': 'Kittery',
    })
    iso = (datetime.utcnow() - timedelta(days=days_ago)).isoformat()
    conn.execute(
        'UPDATE maine_transactions SET last_seen_at = ? WHERE detail_url = ?',
        (iso, url),
    )
    conn.commit()


class TestWithdrawnSweeper:
    def test_listing_not_seen_for_10_days_is_withdrawn(self, conn):
        _insert_with_last_seen(conn, url='/l/stale', status='Active', days_ago=10)
        marked = mark_withdrawn_stale(conn, stale_days=7)
        assert marked == 1
        row = conn.execute(
            'SELECT status FROM maine_transactions WHERE detail_url = ?',
            ('/l/stale',),
        ).fetchone()
        assert row[0] == 'Withdrawn'

    def test_listing_seen_recently_not_withdrawn(self, conn):
        _insert_with_last_seen(conn, url='/l/fresh', status='Active', days_ago=2)
        marked = mark_withdrawn_stale(conn, stale_days=7)
        assert marked == 0
        row = conn.execute(
            'SELECT status FROM maine_transactions WHERE detail_url = ?',
            ('/l/fresh',),
        ).fetchone()
        assert row[0] == 'Active'

    def test_pending_also_swept(self, conn):
        _insert_with_last_seen(conn, url='/l/pending', status='Pending', days_ago=10)
        mark_withdrawn_stale(conn, stale_days=7)
        row = conn.execute(
            'SELECT status FROM maine_transactions WHERE detail_url = ?',
            ('/l/pending',),
        ).fetchone()
        assert row[0] == 'Withdrawn'

    def test_closed_is_never_swept(self, conn):
        """Closed transactions are archival — they don't get a last_seen_at
        refresh from active runs, but they must NEVER be marked Withdrawn."""
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

    def test_boundary_exactly_7_days(self, conn):
        """A listing last seen exactly 7 days ago should NOT be withdrawn
        (cutoff is strictly > 7 days — leaves a buffer for the morning run)."""
        _insert_with_last_seen(conn, url='/l/edge', status='Active', days_ago=7)
        marked = mark_withdrawn_stale(conn, stale_days=7)
        # 7 days old with stale_days=7: NOT stale (strict > comparison)
        assert marked == 0

    def test_sweeper_writes_history_row(self, conn):
        _insert_with_last_seen(conn, url='/l/hist', status='Active', days_ago=10)
        mark_withdrawn_stale(conn, stale_days=7)
        rows = conn.execute(
            'SELECT status FROM maine_listing_history '
            'WHERE detail_url = ? ORDER BY id ASC',
            ('/l/hist',),
        ).fetchall()
        # One baseline (Active) + one Withdrawn transition
        assert len(rows) == 2
        assert rows[0][0] == 'Active'
        assert rows[1][0] == 'Withdrawn'

    def test_returns_zero_when_nothing_to_sweep(self, conn):
        assert mark_withdrawn_stale(conn, stale_days=7) == 0
```

- [ ] **Step 2: Run test to verify it fails**

```bash
python -m pytest tests/test_maine_active_sweeper.py -v
```

Expected: ImportError for `mark_withdrawn_stale`.

- [ ] **Step 3: Implement the sweeper**

In `src/maine_database.py`, add:

```python
def mark_withdrawn_stale(
    conn: sqlite3.Connection, *, stale_days: int = 7,
) -> int:
    """Mark Active/Pending listings not seen in more than `stale_days` days
    as status='Withdrawn', and write a history row for each.

    Returns the count of listings marked.
    """
    cutoff = (datetime.utcnow() - timedelta(days=stale_days)).isoformat()
    rows = conn.execute('''
        SELECT detail_url, list_price FROM maine_transactions
        WHERE status IN ('Active', 'Pending')
          AND last_seen_at IS NOT NULL
          AND last_seen_at < ?
    ''', (cutoff,)).fetchall()

    urls = [(r[0], r[1]) for r in rows]
    if not urls:
        return 0

    placeholders = ','.join(['?'] * len(urls))
    conn.execute(f'''
        UPDATE maine_transactions
        SET status = 'Withdrawn'
        WHERE detail_url IN ({placeholders})
    ''', [u for u, _ in urls])
    conn.commit()

    for url, price in urls:
        write_history_if_changed(conn, url, 'Withdrawn', price)

    return len(urls)
```

Note: the import for `timedelta` — the file already imports `from datetime import datetime` at the top; change that to `from datetime import datetime, timedelta`.

- [ ] **Step 4: Wire into daily-active run in maine_main.py**

Add a `--sweep` flag and sweep call after the discovery section:

```python
    parser.add_argument('--sweep', action='store_true',
                        help='Mark active listings not seen in 7 days as Withdrawn '
                             '(run at end of daily-active cycle)')
    parser.add_argument('--sweep-days', type=int, default=7,
                        help='Age threshold (days) for withdrawn sweep')
```

In the `main()` body, after the `args.discover` block and before `args.enrich`:

```python
    if args.sweep:
        from .maine_database import mark_withdrawn_stale
        marked = mark_withdrawn_stale(conn, stale_days=args.sweep_days)
        logger.info('Withdrawn sweep: %d listings marked', marked)
```

- [ ] **Step 5: Run sweeper tests**

```bash
python -m pytest tests/test_maine_active_sweeper.py -v
```

Expected: all pass.

- [ ] **Step 6: Run full suite**

```bash
python -m pytest tests/ -q
```

Expected: 232 existing + new tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/maine_database.py src/maine_main.py tests/test_maine_active_sweeper.py
git commit -m "feat: withdrawn-listing sweeper (stale 7+ days → Withdrawn)"
```

---

## Task C2: Anomaly detector + `--max-credits` budget cap

**Goal:** After a daily-active discover run, if zero new listings AND zero status changes were observed across all 10 towns, fire a failure notification — that's a strong signal the scraper is broken or the site changed. Also add a `--max-credits` budget cap so a misconfigured run can't spend unbounded Firecrawl credits.

**Files:**
- Modify: `src/maine_main.py` (anomaly detector + credit counter)
- Modify: `src/maine_firecrawl.py` (return delta counts from discover_listings)
- Test: `tests/test_maine_anomaly.py` (new)

- [ ] **Step 1: Write the failing test**

Create `tests/test_maine_anomaly.py`:

```python
"""Tests for the anomaly alert path (zero-delta run → notify_failure)."""
from __future__ import annotations

from unittest.mock import patch, MagicMock
import pytest

from src.maine_main import detect_daily_active_anomaly


class TestAnomalyDetector:
    def test_zero_new_zero_changes_across_all_towns_triggers(self):
        # 10 towns, all scraped successfully, 0 new, 0 status changes
        result = {
            'towns_scraped': 10,
            'new_listings': 0,
            'status_changes': 0,
        }
        anomaly = detect_daily_active_anomaly(result)
        assert anomaly is True

    def test_any_new_listing_prevents_anomaly(self):
        result = {'towns_scraped': 10, 'new_listings': 1, 'status_changes': 0}
        assert detect_daily_active_anomaly(result) is False

    def test_any_status_change_prevents_anomaly(self):
        result = {'towns_scraped': 10, 'new_listings': 0, 'status_changes': 1}
        assert detect_daily_active_anomaly(result) is False

    def test_partial_town_coverage_no_anomaly(self):
        """If fewer than all towns were scraped (partial failure), don't fire
        the anomaly — the town-level failures will trigger their own alerts."""
        result = {'towns_scraped': 3, 'new_listings': 0, 'status_changes': 0}
        assert detect_daily_active_anomaly(result) is False


class TestAnomalyNotification:
    @patch('src.maine_main.notify_failure')
    def test_anomaly_sends_failure_notification(self, mock_notify):
        from src.maine_main import send_anomaly_alert
        send_anomaly_alert(run_id='test-123')
        mock_notify.assert_called_once()
        args, kwargs = mock_notify.call_args
        # The message must be distinct enough to identify as anomaly
        assert 'anomaly' in args[0].lower() or 'suspicious' in args[0].lower()
```

- [ ] **Step 2: Run test to verify it fails**

```bash
python -m pytest tests/test_maine_anomaly.py -v
```

Expected: ImportError on `detect_daily_active_anomaly` and `send_anomaly_alert`.

- [ ] **Step 3: Implement in src/maine_main.py**

Add at the top of `src/maine_main.py` (after existing imports):

```python
TOWNS_EXPECTED = 10  # southern coastal Maine territory
```

Add these functions (place them near `_notify_enrichment_result`):

```python
def detect_daily_active_anomaly(discovery_result: dict) -> bool:
    """Return True if a daily-active run looks suspiciously quiet.

    Criteria: all 10 towns scraped successfully AND zero new listings AND
    zero status changes. Partial-coverage days (fewer towns scraped) are
    not anomalies — per-town failures have their own alerts.
    """
    return (
        discovery_result.get('towns_scraped', 0) >= TOWNS_EXPECTED
        and discovery_result.get('new_listings', 0) == 0
        and discovery_result.get('status_changes', 0) == 0
    )


def send_anomaly_alert(*, run_id: str) -> None:
    """Fire a high-priority alert when the daily run looked broken."""
    notify_failure(
        'Daily-active run looks suspicious — zero deltas across all towns',
        (
            f'Run {run_id} scraped all 10 towns but saw zero new listings '
            'and zero status changes. This is almost always a broken scraper '
            '(site change, auth wall, parser regression) rather than a real '
            'quiet day. Inspect mainelistings.com and the latest pipeline '
            'log before the next cron fires.'
        ),
        run_id=run_id,
    )
```

- [ ] **Step 4: Thread delta counts through `discover_listings`**

In `src/maine_firecrawl.py`, modify `discover_listings` to return both `new` and `status_changes` counts. Change the return shape:

```python
def discover_listings(
    ...
) -> dict:
    ...
    towns_to_process = towns or list(TOWNS)
    total_listings = 0
    total_new = 0
    total_status_changes = 0
    towns_done = 0
    ...
```

You'll need `_discover_town` to return both counts. Currently it returns `town_count`. Change to return a dict:

```python
def _discover_town(
    client, conn, town: str, max_pages: int, recent_only: bool,
    db_lock: threading.Lock, status: str = 'Closed',
) -> dict:
    town_count = 0
    new_count_total = 0
    status_change_total = 0

    for page_num in range(1, max_pages + 1):
        url = build_search_url(town=town, page=page_num, status=status)
        ...
        with db_lock:
            for card in cards:
                existed = url_exists(conn, card['detail_url'])
                if not existed:
                    new_count_total += 1
                # detect status change before upsert (upsert will overwrite)
                if existed:
                    prior = conn.execute(
                        'SELECT status FROM maine_transactions WHERE detail_url = ?',
                        (card['detail_url'],),
                    ).fetchone()
                    if prior and prior[0] != card.get('status'):
                        status_change_total += 1
                upsert_listing(conn, card)
        town_count += len(cards)
        ...

    return {
        'listings': town_count,
        'new_listings': new_count_total,
        'status_changes': status_change_total,
    }
```

Adjust `_run_town` and the aggregation in `discover_listings` accordingly — return `{'towns': towns_done, 'towns_scraped': towns_done, 'listings': total_listings, 'new_listings': total_new, 'status_changes': total_status_changes}`.

- [ ] **Step 5: Wire anomaly detection into `maine_main.main()`**

After the `args.discover` block runs, if `args.status == 'Active'`:

```python
    if args.discover and args.status == 'Active':
        run_id = datetime.utcnow().strftime('%Y%m%d-%H%M%S')
        if detect_daily_active_anomaly(result):
            send_anomaly_alert(run_id=run_id)
```

- [ ] **Step 6: Add `--max-credits` flag**

In `maine_main.py` argparse block:

```python
    parser.add_argument('--max-credits', type=int, default=None,
                        help='Hard cap on Firecrawl calls this run '
                             '(safeguard against budget overrun)')
```

In `src/maine_firecrawl.py`, add a module-level counter + check in `_scrape`:

```python
# Module-level credit counter (thread-safe via the GIL for simple reads/writes)
_credit_counter = {'count': 0, 'limit': None, 'lock': threading.Lock()}


def set_credit_limit(limit: int | None) -> None:
    with _credit_counter['lock']:
        _credit_counter['count'] = 0
        _credit_counter['limit'] = limit


def _scrape(client, url: str, fmt: str = 'markdown'):
    with _credit_counter['lock']:
        limit = _credit_counter['limit']
        count = _credit_counter['count']
        if limit is not None and count >= limit:
            raise RuntimeError(
                f'Credit cap hit: {count} / {limit} Firecrawl calls. '
                'Aborting run.'
            )
        _credit_counter['count'] = count + 1

    # ... existing scrape body unchanged from here ...
    kwargs = {'formats': [fmt], 'wait_for': 8000}
    if fmt == 'rawHtml':
        kwargs['actions'] = [
            {'type': 'wait', 'milliseconds': 5000},
            {'type': 'executeJavascript', 'script': DETAIL_EXTRACT_JS},
        ]

    result = client.scrape(url, **kwargs)
    content = getattr(result, fmt, '') or getattr(result, 'markdown', '') or ''

    if any(s in content.lower() for s in _BLOCK_STRINGS):
        raise RuntimeError(f'Blocked or error page at {url}')

    return result
```

In `maine_main.main()`, call `set_credit_limit` at startup:

```python
    from .maine_firecrawl import set_credit_limit
    set_credit_limit(args.max_credits)
```

- [ ] **Step 7: Run anomaly tests**

```bash
python -m pytest tests/test_maine_anomaly.py -v
```

Expected: all pass.

- [ ] **Step 8: Run full suite**

```bash
python -m pytest tests/ -q
```

Expected: 232 existing + new tests pass. The existing `TestDiscoverListingsWithStatus` tests from Task B3 may need to update their assertion from `result['listings'] >= 1` to still work with the new return shape — they already do, since we kept the `listings` key.

- [ ] **Step 9: Commit**

```bash
git add src/maine_main.py src/maine_firecrawl.py tests/test_maine_anomaly.py
git commit -m "feat: anomaly detector for daily-active runs + --max-credits budget cap"
```

---

## Task D1: Downstream read helpers (`src/maine_active.py`)

**Goal:** Expose 4 read-only query helpers that the downstream mailer and agent-outreach tools consume. Freeze the interface and test it against seeded data.

**Files:**
- Create: `src/maine_active.py` (new)
- Test: `tests/test_maine_active.py` (new)

- [ ] **Step 1: Write the failing test**

Create `tests/test_maine_active.py`:

```python
"""Tests for src/maine_active.py downstream read helpers."""
from __future__ import annotations

from datetime import datetime, timedelta
import pytest

from src.maine_database import (
    enrich_listing,
    get_connection,
    init_db,
    upsert_listing,
    write_history_if_changed,
)


@pytest.fixture
def active_db(tmp_path):
    """A DB with a realistic mix of Active, Pending, Closed, Withdrawn."""
    c = get_connection(str(tmp_path / 'a.db'))
    init_db(c)

    # Active listing, just appeared today
    upsert_listing(c, {
        'detail_url': '/l/active-1', 'status': 'Active',
        'list_price': 850_000,
        'address': '1 Ocean Ave', 'city': 'Kittery',
    })
    # Set discovered_at to today (default) for freshness test
    enrich_listing(c, '/l/active-1', {
        'listing_agent': 'Jane Agent', 'listing_agent_email': 'jane@example.com',
        'listing_office': 'Beach Realty', 'list_date': '2026-04-17',
        'year_built': 1987, 'lot_sqft': 15000,
        'description': 'Stunning oceanfront.',
        'photo_url': 'https://p.com/1.jpg', 'status': 'Active',
        'list_price': 850_000, 'days_on_market': 1,
    })

    # Stale active — 80 days on market
    upsert_listing(c, {
        'detail_url': '/l/stale-1', 'status': 'Active',
        'list_price': 1_200_000,
        'address': '2 Shore Rd', 'city': 'York',
    })
    enrich_listing(c, '/l/stale-1', {
        'listing_agent': 'John Seller', 'listing_agent_email': 'john@example.com',
        'listing_office': 'Coastal Homes', 'list_date': '2026-01-27',
        'year_built': 2001, 'status': 'Active',
        'list_price': 1_200_000, 'days_on_market': 80,
    })

    # Pending listing
    upsert_listing(c, {
        'detail_url': '/l/pending-1', 'status': 'Pending',
        'list_price': 650_000,
        'address': '3 Dock St', 'city': 'Kennebunkport',
    })

    # Closed transaction (archival — should NEVER appear in active queries)
    c.execute('''
        INSERT INTO maine_transactions (
            detail_url, status, sale_price, close_date, city,
            discovered_at, scraped_at
        ) VALUES (
            '/l/closed-1', 'Closed', 750_000, '2026-01-01', 'Kittery',
            '2026-01-02', '2026-01-02'
        )
    ''')
    c.commit()

    # Withdrawn listing
    upsert_listing(c, {
        'detail_url': '/l/withdrawn-1', 'status': 'Active',
        'list_price': 500_000,
        'address': '4 Hill Rd', 'city': 'Wells',
    })
    c.execute("UPDATE maine_transactions SET status = 'Withdrawn' WHERE detail_url = '/l/withdrawn-1'")
    c.commit()

    return c


class TestQueryActiveListings:
    def test_returns_active_not_closed(self, active_db):
        from src.maine_active import query_active_listings
        rows = query_active_listings(active_db)
        urls = {r['detail_url'] for r in rows}
        assert '/l/active-1' in urls
        assert '/l/stale-1' in urls
        assert '/l/closed-1' not in urls
        assert '/l/withdrawn-1' not in urls

    def test_pending_excluded_by_default(self, active_db):
        from src.maine_active import query_active_listings
        rows = query_active_listings(active_db)
        urls = {r['detail_url'] for r in rows}
        assert '/l/pending-1' not in urls

    def test_include_pending_true(self, active_db):
        from src.maine_active import query_active_listings
        rows = query_active_listings(active_db, include_pending=True)
        urls = {r['detail_url'] for r in rows}
        assert '/l/pending-1' in urls

    def test_town_filter(self, active_db):
        from src.maine_active import query_active_listings
        rows = query_active_listings(active_db, towns=['Kittery'])
        urls = {r['detail_url'] for r in rows}
        assert '/l/active-1' in urls
        assert '/l/stale-1' not in urls  # York, filtered out

    def test_min_days_on_market(self, active_db):
        from src.maine_active import query_active_listings
        rows = query_active_listings(active_db, min_days_on_market=60)
        urls = {r['detail_url'] for r in rows}
        assert '/l/stale-1' in urls
        assert '/l/active-1' not in urls  # DOM=1, below 60

    def test_returned_row_has_agent_contact(self, active_db):
        from src.maine_active import query_active_listings
        rows = query_active_listings(active_db)
        active1 = next(r for r in rows if r['detail_url'] == '/l/active-1')
        assert active1['listing_agent'] == 'Jane Agent'
        assert active1['listing_agent_email'] == 'jane@example.com'
        assert active1['listing_office'] == 'Beach Realty'


class TestQueryListingHistory:
    def test_returns_baseline_row(self, active_db):
        from src.maine_active import query_listing_history
        rows = query_listing_history(active_db, '/l/active-1')
        assert len(rows) >= 1
        assert rows[0]['status'] == 'Active'

    def test_withdrawn_listing_has_transition_row(self, active_db):
        from src.maine_active import query_listing_history
        # Set last_seen_at far back and re-mark to trigger history
        write_history_if_changed(active_db, '/l/withdrawn-1', 'Withdrawn', 500_000)
        rows = query_listing_history(active_db, '/l/withdrawn-1')
        statuses = [r['status'] for r in rows]
        assert 'Active' in statuses
        assert 'Withdrawn' in statuses
        # Ordered oldest first
        assert statuses.index('Active') < statuses.index('Withdrawn')

    def test_returns_empty_for_unknown_url(self, active_db):
        from src.maine_active import query_listing_history
        rows = query_listing_history(active_db, '/l/does-not-exist')
        assert rows == []


class TestQueryNewSince:
    def test_returns_listings_with_first_history_after_cutoff(self, active_db):
        from src.maine_active import query_new_since
        # Everything in the fixture was just created — all should be "new" since
        # yesterday
        yesterday = (datetime.utcnow() - timedelta(days=1)).isoformat()
        rows = query_new_since(active_db, since_iso=yesterday)
        urls = {r['detail_url'] for r in rows}
        assert '/l/active-1' in urls
        assert '/l/stale-1' in urls

    def test_old_listings_excluded(self, active_db):
        from src.maine_active import query_new_since
        # A cutoff far in the future — nothing newer
        future = '2099-01-01T00:00:00'
        rows = query_new_since(active_db, since_iso=future)
        assert rows == []


class TestQueryStaleListings:
    def test_returns_active_with_high_dom(self, active_db):
        from src.maine_active import query_stale_listings
        rows = query_stale_listings(active_db, min_dom=60)
        urls = {r['detail_url'] for r in rows}
        assert '/l/stale-1' in urls
        assert '/l/active-1' not in urls  # DOM=1

    def test_threshold_respected(self, active_db):
        from src.maine_active import query_stale_listings
        rows = query_stale_listings(active_db, min_dom=90)
        # stale-1 has DOM=80, below 90 threshold
        assert all(r['days_on_market'] >= 90 for r in rows)

    def test_closed_never_returned(self, active_db):
        from src.maine_active import query_stale_listings
        rows = query_stale_listings(active_db, min_dom=0)
        statuses = {r['status'] for r in rows}
        assert 'Closed' not in statuses
```

- [ ] **Step 2: Run test to verify it fails**

```bash
python -m pytest tests/test_maine_active.py -v
```

Expected: `ModuleNotFoundError: No module named 'src.maine_active'`.

- [ ] **Step 3: Implement `src/maine_active.py`**

Create `src/maine_active.py`:

```python
"""Read-only helpers for downstream tools.

The downstream mailer and agent-outreach tools (in a separate repo) read
from data/maine_listings.db via these four functions. The interface is
frozen here so the downstream repo has a stable contract.
"""
from __future__ import annotations

import sqlite3
from typing import Optional


_ACTIVE_STATUSES = ('Active',)
_ACTIVE_OR_PENDING = ('Active', 'Pending')


def _dict(row) -> dict:
    return dict(row) if row is not None else {}


def query_active_listings(
    conn: sqlite3.Connection,
    *,
    towns: Optional[list[str]] = None,
    min_days_on_market: Optional[int] = None,
    include_pending: bool = False,
) -> list[dict]:
    """Currently-active listings with agent contact + property details.

    Only rows where status='Active' (and optionally 'Pending'). `Closed` and
    `Withdrawn` are never returned.
    """
    statuses = _ACTIVE_OR_PENDING if include_pending else _ACTIVE_STATUSES
    placeholders = ','.join(['?'] * len(statuses))
    params: list = list(statuses)

    where_extra = ''
    if towns:
        town_placeholders = ','.join(['?'] * len(towns))
        where_extra += f' AND LOWER(city) IN ({town_placeholders})'
        params += [t.lower() for t in towns]
    if min_days_on_market is not None:
        where_extra += ' AND days_on_market >= ?'
        params.append(min_days_on_market)

    rows = conn.execute(f'''
        SELECT
            detail_url, mls_number, status,
            address, city, state, zip,
            list_price, list_date, days_on_market,
            beds, baths, sqft, year_built, lot_sqft,
            property_type, description, photo_url,
            listing_agent, listing_agent_email, listing_agent_id,
            listing_office, last_seen_at
        FROM maine_transactions
        WHERE status IN ({placeholders})
        {where_extra}
        ORDER BY list_date DESC
    ''', params).fetchall()
    return [_dict(r) for r in rows]


def query_listing_history(
    conn: sqlite3.Connection, detail_url: str,
) -> list[dict]:
    """Full price/status timeline for one listing, oldest first."""
    rows = conn.execute('''
        SELECT snapshot_date, status, list_price
        FROM maine_listing_history
        WHERE detail_url = ?
        ORDER BY id ASC
    ''', (detail_url,)).fetchall()
    return [_dict(r) for r in rows]


def query_new_since(conn: sqlite3.Connection, *, since_iso: str) -> list[dict]:
    """Listings whose first history row is on or after since_iso.

    The *first* history row represents the listing's debut in the pipeline.
    Downstream: the daily mailer tool calls this with yesterday's timestamp
    to get "new listings since my last run".
    """
    rows = conn.execute('''
        WITH first_seen AS (
            SELECT detail_url, MIN(snapshot_date) AS first_snap
            FROM maine_listing_history
            GROUP BY detail_url
        )
        SELECT
            t.detail_url, t.address, t.city, t.status,
            t.list_price, t.list_date, t.beds, t.baths, t.sqft,
            t.listing_agent, t.listing_agent_email, t.listing_office,
            fs.first_snap
        FROM maine_transactions t
        JOIN first_seen fs ON fs.detail_url = t.detail_url
        WHERE fs.first_snap >= ?
          AND t.status IN ('Active', 'Pending')
        ORDER BY fs.first_snap ASC
    ''', (since_iso,)).fetchall()
    return [_dict(r) for r in rows]


def query_stale_listings(
    conn: sqlite3.Connection, *, min_dom: int = 60,
) -> list[dict]:
    """Active listings on market at least min_dom days.

    Downstream: the agent-outreach tool uses this to target motivated
    sellers ("your listing's been up 60 days — want an STR projection?").
    """
    rows = conn.execute('''
        SELECT
            detail_url, address, city, status,
            list_price, list_date, days_on_market,
            beds, baths, sqft, property_type,
            listing_agent, listing_agent_email, listing_office
        FROM maine_transactions
        WHERE status = 'Active'
          AND days_on_market IS NOT NULL
          AND days_on_market >= ?
        ORDER BY days_on_market DESC
    ''', (min_dom,)).fetchall()
    return [_dict(r) for r in rows]
```

- [ ] **Step 4: Run the new tests**

```bash
python -m pytest tests/test_maine_active.py -v
```

Expected: all pass.

- [ ] **Step 5: Run full suite**

```bash
python -m pytest tests/ -q
```

Expected: 232 existing + all new tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/maine_active.py tests/test_maine_active.py
git commit -m "feat: downstream read helpers for active listings"
```

---

## Task D2: CI workflow + daily cron + docs

**Goal:** Update `.github/workflows/maine_listings.yml` to add a daily cron, new modes (`daily-active`, `backfill-active`), and route them through the CLI. Update `CLAUDE.md` and `AGENTS.md` to reflect the new pipeline. Push the branch.

**Files:**
- Modify: `.github/workflows/maine_listings.yml`
- Modify: `CLAUDE.md` (Phase 7 section)
- Modify: `AGENTS.md` (current state + new mode)

- [ ] **Step 1: Update workflow**

Replace the `on:` block in `.github/workflows/maine_listings.yml`:

```yaml
on:
  schedule:
    # Weekly: Mondays at 11:30 UTC (6:30am ET) — closed transactions
    - cron: '30 11 * * 1'
    # Daily: every day at 11:30 UTC (6:30am ET) — active listings
    - cron: '30 11 * * *'
  workflow_dispatch:
    inputs:
      mode:
        description: 'Run mode'
        default: 'weekly'
        type: choice
        options:
          - weekly
          - backfill
          - daily-active
          - backfill-active
          - report-only
      workers:
        description: 'Concurrent workers for enrichment (1-50)'
        default: '10'
      batch_size:
        description: 'Enrichment batch size'
        default: '200'
      max_pages:
        description: 'Max search pages per town (discovery)'
        default: '90'
      max_credits:
        description: 'Hard cap on Firecrawl calls this run (empty = no cap)'
        default: ''
```

Replace the `case "$MODE"` block to route between closed and active. The daily schedule fires at the same minute as the weekly Monday schedule; we distinguish using `github.event.schedule`:

```yaml
      - name: Determine effective mode
        id: pickmode
        run: |
          # Manual dispatch wins. Otherwise derive from cron schedule.
          if [ -n "${{ github.event.inputs.mode }}" ]; then
            MODE="${{ github.event.inputs.mode }}"
          elif [ "${{ github.event.schedule }}" = "30 11 * * 1" ]; then
            MODE="weekly"
          elif [ "${{ github.event.schedule }}" = "30 11 * * *" ]; then
            MODE="daily-active"
          else
            MODE="weekly"
          fi
          echo "mode=$MODE" >> "$GITHUB_OUTPUT"
          echo "Effective mode: $MODE"

      - name: Run Maine Listings pipeline
        env:
          CI: 'true'
          FIRECRAWL_API_KEY: ${{ secrets.FIRECRAWL_API_KEY }}
          PUSHOVER_API_TOKEN: ${{ secrets.PUSHOVER_API_TOKEN }}
          PUSHOVER_USER_KEY: ${{ secrets.PUSHOVER_USER_KEY }}
          RESEND_API_KEY: ${{ secrets.RESEND_API_KEY }}
        run: |
          MODE="${{ steps.pickmode.outputs.mode }}"
          WORKERS="${{ github.event.inputs.workers || '10' }}"
          BATCH="${{ github.event.inputs.batch_size || '200' }}"
          MAX_PAGES="${{ github.event.inputs.max_pages || '90' }}"
          MAX_CREDITS="${{ github.event.inputs.max_credits }}"
          CREDITS_ARG=""
          if [ -n "$MAX_CREDITS" ]; then
            CREDITS_ARG="--max-credits $MAX_CREDITS"
          fi

          if [ -z "$FIRECRAWL_API_KEY" ]; then
            echo "FIRECRAWL_API_KEY secret is required"
            exit 1
          fi

          case "$MODE" in
            weekly)
              python -m src.maine_main \
                --discover --recent-only --status Closed \
                --enrich --batch-size "$BATCH" \
                --workers "$WORKERS" --max-pages "$MAX_PAGES" \
                $CREDITS_ARG
              ;;
            backfill)
              python -m src.maine_main \
                --discover --status Closed \
                --enrich --batch-size "$BATCH" \
                --workers "$WORKERS" --max-pages "$MAX_PAGES" \
                $CREDITS_ARG
              ;;
            daily-active)
              python -m src.maine_main \
                --discover --recent-only --status Active \
                --enrich --batch-size "$BATCH" \
                --sweep --sweep-days 7 \
                --workers "$WORKERS" --max-pages "$MAX_PAGES" \
                $CREDITS_ARG
              ;;
            backfill-active)
              python -m src.maine_main \
                --discover --status Active \
                --enrich --batch-size "$BATCH" \
                --workers "$WORKERS" --max-pages "$MAX_PAGES" \
                $CREDITS_ARG
              ;;
            report-only)
              echo "Report-only: regenerate dashboards from existing data"
              ;;
          esac

          python -m src.maine_main --report
```

- [ ] **Step 2: Update CLAUDE.md**

In `/Users/lucasknowles/gw-re-agent-scraper/CLAUDE.md`, add a new section under the Maine-specific section describing the active-listings pipeline. Find the Phase 5 / Phase 6 section (search for "Phase 5" and "Phase 6: Interactive Leaderboard"). Append a new subsection:

```markdown
### Phase 7: Active Listings Pipeline — COMPLETE (2026-04-17)
- Same DB (`maine_listings.db`), new `status` column on `maine_transactions`: `'Active' | 'Pending' | 'Closed' | 'Withdrawn'`.
- Added new columns to support active workflows: `list_date`, `last_seen_at`, `year_built`, `lot_sqft`, `description`, `photo_url`.
- New child table `maine_listing_history` captures change-detected snapshots of (status, list_price). Watched fields exclude `days_on_market` deliberately (DOM ticks daily — would spam the table).
- Daily cron at 6:30am ET scrapes all 10 towns with `mls_status=Active`, runs a withdrawn-sweeper at the end (marks any Active/Pending not seen in 7+ days as `Withdrawn`), and fires a Pushover+Resend failure alert if zero new listings AND zero status changes were observed across all 10 towns (anomaly detector).
- New `--max-credits N` CLI flag caps Firecrawl calls per run as a budget safety net.
- Closed-focused queries (`maine_report`, `maine_kpis`) were gated with `WHERE status = 'Closed'` so the leaderboard + KPI dashboards are semantically unchanged — Active/Pending/Withdrawn rows coexist in the same table but don't pollute rankings.
- Downstream tools (separate repo — direct-mail to owners, listing-agent STR-projection outreach) consume `src/maine_active.py`:
    - `query_active_listings(conn, *, towns=None, min_days_on_market=None, include_pending=False)`
    - `query_listing_history(conn, detail_url)`
    - `query_new_since(conn, since_iso)`
    - `query_stale_listings(conn, *, min_dom=60)`
- GitHub Actions workflow routes on cron schedule: Monday schedule fires weekly closed pipeline; daily schedule fires `daily-active` mode. Manual dispatch offers `daily-active`, `backfill-active`, plus the existing modes.
```

- [ ] **Step 3: Update AGENTS.md**

In `AGENTS.md`, locate the current-state section (search for "Current State" or the most recent session log). Add a new session entry at the top of the session log section:

```markdown
**Session N+1 (2026-04-17):** Shipped Phase 7 — Active Listings Pipeline. Added `status` column + 6 new attribute columns to `maine_transactions`, new `maine_listing_history` table for change-detected snapshots, daily cron for `mls_status=Active` scraping, withdrawn-sweeper (7-day stale threshold), anomaly detector (zero-delta days → failure alert), `--max-credits` budget cap, and `src/maine_active.py` read helpers for downstream tools. All 232 existing tests preserved; ~60 new tests added. Branch `feature/maine-active-listings` → PR.
```

- [ ] **Step 4: Run the entire suite one last time**

```bash
python -m pytest tests/ -q
```

Expected: all tests pass.

- [ ] **Step 5: Commit docs + workflow**

```bash
git add .github/workflows/maine_listings.yml CLAUDE.md AGENTS.md
git commit -m "feat: daily cron + mode routing + docs for active listings"
```

- [ ] **Step 6: Push branch and open PR**

```bash
git push -u origin feature/maine-active-listings
gh pr create --title "feat: Maine MLS active-listings pipeline" --body "$(cat <<'EOF'
## Summary

Extends the Maine MLS scraper from closed-only (weekly) to also capture
active listings daily, with change-detected history, withdrawn-sweeping,
and a frozen read interface for downstream tools.

Implements `docs/superpowers/specs/2026-04-17-maine-active-listings-design.md`.

## What's new

- **Schema**: additive migration (7 new columns + 1 new child table)
- **Discovery**: `--status Active` scrapes the active search pages; uses
  existing Firecrawl concurrency and circuit breaker unchanged
- **Enrichment**: same NUXT extraction JS, extended for list_date,
  year_built, lot_sqft, description, photo_url, mls_status
- **Lifecycle**: withdrawn-sweeper marks Active/Pending listings not
  seen in 7+ days as Withdrawn; writes a history row
- **Monitoring**: anomaly detector fires a failure alert when a daily
  run sees zero new listings + zero status changes across all 10 towns
- **Safety**: `--max-credits` hard-caps Firecrawl calls per run
- **CI**: new daily cron at 6:30am ET alongside the existing weekly
  Monday cron; schedules routed by `github.event.schedule`
- **Downstream**: `src/maine_active.py` freezes 4 query helpers for
  the separate-repo tools to consume

## Test plan

- [x] All 232 existing tests pass
- [x] ~60 new tests added across migration, history, discovery,
  sweeper, anomaly, and downstream read-helper modules
- [x] Manual verification (Task B2 Step 4) — real active listing
  detail page extracts all new fields
- [ ] First daily-active cron run (tomorrow 6:30 ET)
- [ ] Anomaly alert manually exercised once after first live run

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Self-Review (Plan Author Runs This, Not a Subagent)

### Spec coverage

Walking the spec section by section:

- **Context + Goals + Non-Goals** → the plan scope matches. Non-goals (owner identification, downstream tools, UI dashboards) are honored.
- **Section 1 Architecture** → daily cron (D2), closed weekly preserved (D2), unified table (A1), withdrawn sweeper (C1), code reuse via status parameter (B3), owner identification excluded.
- **Section 2 Schema** → A1 covers all 7 new columns + history table. Backfill = A1 Step 3. Idempotent migration = A1 tests. Watched fields restricted to status + list_price (NOT days_on_market) = A1 + A2 + explicit test assertion.
- **Section 3 Cron/Cost/Notifications** → Cron schedule = D2. Credit budgets = C2 `--max-credits`. Failure/success/anomaly notifications = C2 (anomaly) + existing notifier (reused — no code changes).
- **Section 4 Downstream Interface + Testing** → 4 helpers in `src/maine_active.py` = D1. Test plan matches.

✅ Spec coverage complete.

### Placeholder scan

Ran mental grep for "TBD", "TODO", "implement later", "similar to Task N", "handle edge cases" across all 10 tasks. None found.

The one item flagged as needing real-world verification — NUXT JS field names (B2 Step 4) — is explicit about which field names to try and how to adjust them, not a placeholder.

### Type consistency

- `discover_listings(..., status='Closed')` — defined in B3, referenced in D2 ✓
- `parse_search_cards(markdown, status='Closed')` — defined in B1, referenced in B3 ✓
- `build_search_url(town, page, status)` — defined in B1, referenced in B3 ✓
- `write_history_if_changed(conn, detail_url, status, list_price)` — defined in A2, referenced in C1 ✓
- `mark_withdrawn_stale(conn, *, stale_days=7)` — defined in C1 ✓
- `detect_daily_active_anomaly(dict)` and `send_anomaly_alert(run_id=...)` — defined in C2, both tested ✓
- `set_credit_limit(int|None)` — defined in C2 ✓
- 4 downstream helpers — all defined in D1 with matching test calls ✓

### Scope check

Single plan covers one coherent pipeline extension. Does not need decomposition.

---

## Execution Notes (For The Controller Running This Plan)

- **Branch:** `feature/maine-active-listings` (already created, spec already committed as `52895c4`).
- **Existing test count:** 232 — run after each task to catch regressions.
- **Firecrawl verification** (B2 Step 4) requires `FIRECRAWL_API_KEY` in the env. On a fresh machine: `export FIRECRAWL_API_KEY=$(grep FIRECRAWL_API_KEY ~/.env | cut -d= -f2 | tr -d '"')`.
- **No data/ file edits** during implementation tasks. Actual data lands on the next cron run or a manual `workflow_dispatch`.
- **Tests-first in every task.** Write the failing test, run it to see it fail with the expected error, implement, run to see pass, run the full suite, commit.
- **CLAUDE.md rule:** after every file write, run `verify-app` (the project's test suite). The plan's per-task "run the full suite" step already satisfies this.
