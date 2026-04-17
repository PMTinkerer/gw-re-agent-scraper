# Maine MLS Leaderboard Redesign — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rebuild the interactive "All Agents" tab (renamed "Leaderboard") and the static Maine HTML dashboard around period-based KPIs (12mo / prior-12mo / 3yr / all-time), with a Biggest Movers banner, an Agent/Brokerage toggle, and a top-50-per-town slice.

**Architecture:** Add a focused data-layer module `src/maine_kpis.py` with one function per query and a pure-Python mover computation. Both consumers — the static dashboard (`src/maine_dashboard.py`) and the interactive index page (`src/index_page.py`) — call into `maine_kpis.py`. JS remains inline in `index_page.py` but the new leaderboard logic lives in a dedicated `_leaderboard_js()` helper.

**Tech Stack:** Python 3.9, SQLite (WAL), pytest, vanilla JS embedded in Python f-strings. No new external dependencies.

**Reference spec:** [`docs/superpowers/specs/2026-04-16-maine-leaderboard-redesign-design.md`](../specs/2026-04-16-maine-leaderboard-redesign-design.md)

**Working branch:** `feature/maine-listings-phase2` (PR #11 already open)

---

## File map

Files that will be **created**:
- `src/maine_kpis.py` — period queries + mover computation
- `tests/test_maine_kpis.py` — unit tests for the above

Files that will be **modified**:
- `src/maine_report.py` — extend `build_maine_search_index()` with period breakdowns
- `src/maine_dashboard.py` — rewrite leaderboard sections + add movers banner
- `src/index_page.py` — rewrite master table, add movers banner + filters + period selector
- `tests/test_maine_report.py` — update search index test for new fields
- `CLAUDE.md` — document new module and commands

Files that stay put: `src/maine_main.py`, `src/maine_firecrawl.py`, `src/maine_database.py`, `src/maine_parser.py`, `src/maine_state.py`, `src/maine_notifier.py`.

---

## Phase A — Data Layer (pure functions, TDD)

### Task A1: Scaffold `src/maine_kpis.py` with period constants

**Files:**
- Create: `src/maine_kpis.py`
- Test: (none in this task)

- [ ] **Step 1: Create the module with period constants and stubs**

```python
"""KPI queries for Maine MLS transactions.

Provides one row per agent (or brokerage) with metrics across four rolling
time windows: last-12mo, prior-12mo, last-3yr, all-time. Consumed by the
static dashboard (maine_dashboard.py) and interactive index tab
(index_page.py).

Period cutoffs are computed against a caller-supplied `today` ISO date string
(or date('now') if omitted). Tests pass a fixed today for determinism.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional

# Reuse existing exclusion constants from maine_report to avoid drift.
from .maine_report import _AGENT_EXCLUSIONS


PERIOD_12MO_DAYS = 365
PERIOD_3YR_DAYS = 365 * 3


@dataclass(frozen=True)
class PeriodCutoffs:
    """ISO date strings used as SQL parameters for each rolling window."""
    current_12mo_start: str  # close_date >= this
    prior_12mo_start: str    # close_date >= this AND < current_12mo_start
    three_year_start: str    # close_date >= this


def compute_cutoffs(today: Optional[str] = None) -> PeriodCutoffs:
    """Return ISO strings for the period boundaries relative to `today`.

    If `today` is None, uses date.today().
    """
    anchor = date.fromisoformat(today) if today else date.today()
    return PeriodCutoffs(
        current_12mo_start=(anchor - timedelta(days=PERIOD_12MO_DAYS)).isoformat(),
        prior_12mo_start=(anchor - timedelta(days=PERIOD_12MO_DAYS * 2)).isoformat(),
        three_year_start=(anchor - timedelta(days=PERIOD_3YR_DAYS)).isoformat(),
    )


def query_agent_kpis(
    conn: sqlite3.Connection,
    *,
    town: Optional[str] = None,
    limit: Optional[int] = None,
    today: Optional[str] = None,
) -> list[dict]:
    """One row per agent with all period metrics. Agents = both listing and
    buyer sides unioned together."""
    raise NotImplementedError  # Task A3


def query_brokerage_kpis(
    conn: sqlite3.Connection,
    *,
    town: Optional[str] = None,
    limit: Optional[int] = None,
    today: Optional[str] = None,
) -> list[dict]:
    """One row per brokerage (listing_office + buyer_office union, kept at
    branch level)."""
    raise NotImplementedError  # Task A4


def compute_rank_movers(
    rows: list[dict],
    *,
    current_field: str = 'current_12mo_sides',
    prior_field: str = 'prior_12mo_sides',
    min_sides: int = 5,
    top_n: int = 5,
) -> dict:
    """Given KPI rows, compute rank deltas and return risers/fallers.

    Returns {'risers': [...], 'fallers': [...], 'deltas': {name: int_or_None}}.
    A None delta means the entity was NEW (no prior-period activity).
    """
    raise NotImplementedError  # Task A2
```

- [ ] **Step 2: Verify the module imports cleanly**

Run: `python3 -c "from src.maine_kpis import compute_cutoffs, PERIOD_12MO_DAYS; print(compute_cutoffs('2026-04-16'))"`
Expected: `PeriodCutoffs(current_12mo_start='2025-04-16', prior_12mo_start='2024-04-16', three_year_start='2023-04-17')`

- [ ] **Step 3: Commit**

```bash
git add src/maine_kpis.py
git commit -m "feat: scaffold maine_kpis module with period constants"
```

---

### Task A2: `compute_rank_movers()` — pure Python, full TDD

**Files:**
- Modify: `src/maine_kpis.py` (replace the `compute_rank_movers` stub)
- Create: `tests/test_maine_kpis.py`

- [ ] **Step 1: Write failing tests**

Create `tests/test_maine_kpis.py`:

```python
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
```

- [ ] **Step 2: Run tests — verify they fail with NotImplementedError**

Run: `python3 -m pytest tests/test_maine_kpis.py::TestComputeRankMovers -v`
Expected: FAIL on the mover tests (NotImplementedError). The `TestComputeCutoffs` tests should PASS (that function is already implemented from Task A1).

- [ ] **Step 3: Implement `compute_rank_movers`**

In `src/maine_kpis.py`, replace the stub:

```python
def compute_rank_movers(
    rows: list[dict],
    *,
    current_field: str = 'current_12mo_sides',
    prior_field: str = 'prior_12mo_sides',
    min_sides: int = 5,
    top_n: int = 5,
) -> dict:
    """Given KPI rows, compute rank deltas and return risers/fallers.

    Rank is 1-based on the respective period's sides descending. Delta is
    prior_rank - current_rank (positive = moved up).

    NEW entities (prior sides = 0) get delta = None and are placed in risers.
    Entities with current_sides < min_sides are excluded from both lists.

    Returns {'risers': [...], 'fallers': [...]}.
    """
    if not rows:
        return {'risers': [], 'fallers': []}

    # Rank by current period (desc)
    current_sorted = sorted(rows, key=lambda r: r.get(current_field) or 0, reverse=True)
    current_rank = {r['name']: i + 1 for i, r in enumerate(current_sorted)}

    # Rank by prior period (desc), but only among entities with prior > 0
    prior_eligible = [r for r in rows if (r.get(prior_field) or 0) > 0]
    prior_sorted = sorted(prior_eligible, key=lambda r: r.get(prior_field) or 0, reverse=True)
    prior_rank = {r['name']: i + 1 for i, r in enumerate(prior_sorted)}

    enriched = []
    for r in rows:
        name = r['name']
        current_sides = r.get(current_field) or 0
        prior_sides = r.get(prior_field) or 0
        if current_sides < min_sides:
            continue
        if name in prior_rank:
            delta = prior_rank[name] - current_rank[name]
        else:
            delta = None  # NEW
        enriched.append({**r, 'delta': delta, 'current_rank': current_rank[name]})

    # NEW entities go to risers (sorted by current sides desc as tiebreaker)
    news = [e for e in enriched if e['delta'] is None]
    news.sort(key=lambda e: e.get(current_field) or 0, reverse=True)

    # Positive deltas = risers, negative = fallers
    positive = [e for e in enriched if e['delta'] is not None and e['delta'] > 0]
    negative = [e for e in enriched if e['delta'] is not None and e['delta'] < 0]
    positive.sort(key=lambda e: e['delta'], reverse=True)
    negative.sort(key=lambda e: e['delta'])  # most negative first

    risers = (news + positive)[:top_n]
    fallers = negative[:top_n]

    return {'risers': risers, 'fallers': fallers}
```

- [ ] **Step 4: Run tests — verify they pass**

Run: `python3 -m pytest tests/test_maine_kpis.py::TestComputeRankMovers -v`
Expected: all 6 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/maine_kpis.py tests/test_maine_kpis.py
git commit -m "feat: compute_rank_movers for Maine leaderboard"
```

---

### Task A3: `query_agent_kpis()` — period aggregation SQL

**Files:**
- Modify: `src/maine_kpis.py` (replace the `query_agent_kpis` stub)
- Modify: `tests/test_maine_kpis.py` (add fixture + tests)

- [ ] **Step 1: Add test fixture for populated DB**

Append to `tests/test_maine_kpis.py`:

```python
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
                enrichment_status, discovered_at, scraped_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'success', ?, ?)
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
```

- [ ] **Step 2: Write failing tests for `query_agent_kpis`**

Append to `tests/test_maine_kpis.py`:

```python
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
        # 3yr: 4 sides (days 30, 100, 200, 400) sum 2_100_000
        assert alice['three_yr_sides'] == 4
        assert alice['three_yr_volume'] == 2_100_000
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
        assert bob['buyer_sides'] == 4

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
                enrichment_status, discovered_at, scraped_at
            ) VALUES ('/l/99', 'NON-MREIS AGENT', 'X', 'Wells', 500000, '2025-06-01',
                'success', '2025-06-01', '2025-06-01')
        ''')
        conn.commit()
        rows = query_agent_kpis(conn, today=today)
        names = {r['name'] for r in rows}
        assert 'NON-MREIS AGENT' not in names
```

- [ ] **Step 3: Run tests — verify they fail**

Run: `python3 -m pytest tests/test_maine_kpis.py::TestQueryAgentKPIs -v`
Expected: all tests FAIL (NotImplementedError).

- [ ] **Step 4: Implement `query_agent_kpis`**

In `src/maine_kpis.py`, replace the stub:

```python
def query_agent_kpis(
    conn: sqlite3.Connection,
    *,
    town: Optional[str] = None,
    limit: Optional[int] = None,
    today: Optional[str] = None,
) -> list[dict]:
    """One row per unique agent name with all period metrics."""
    cutoffs = compute_cutoffs(today)
    exclusions = list(_AGENT_EXCLUSIONS)
    excl_placeholders = ','.join(['(?)'] * len(exclusions))

    town_sql_listing = ''
    town_sql_buyer = ''
    town_params: list = []
    if town:
        town_sql_listing = 'AND LOWER(city) = LOWER(?)'
        town_sql_buyer = 'AND LOWER(city) = LOWER(?)'
        town_params = [town, town]

    # Build a union of listing-side and buyer-side rows so each appearance
    # of an agent counts once. Then aggregate all period metrics in one pass.
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
        excluded(agent_lower) AS (VALUES {excl_placeholders})
        SELECT
            s.agent AS name,
            (
                SELECT office FROM sides s2
                WHERE s2.agent = s.agent AND s2.office IS NOT NULL
                GROUP BY office ORDER BY COUNT(*) DESC LIMIT 1
            ) AS office,
            SUM(CASE WHEN s.close_date >= ? THEN 1 ELSE 0 END) AS current_12mo_sides,
            SUM(CASE WHEN s.close_date >= ? THEN COALESCE(s.sale_price, 0) ELSE 0 END) AS current_12mo_volume,
            SUM(CASE WHEN s.close_date >= ? AND s.close_date < ? THEN 1 ELSE 0 END) AS prior_12mo_sides,
            SUM(CASE WHEN s.close_date >= ? AND s.close_date < ? THEN COALESCE(s.sale_price, 0) ELSE 0 END) AS prior_12mo_volume,
            SUM(CASE WHEN s.close_date >= ? THEN 1 ELSE 0 END) AS three_yr_sides,
            SUM(CASE WHEN s.close_date >= ? THEN COALESCE(s.sale_price, 0) ELSE 0 END) AS three_yr_volume,
            COUNT(*) AS all_time_sides,
            SUM(COALESCE(s.sale_price, 0)) AS all_time_volume,
            SUM(CASE WHEN s.role = 'listing' THEN 1 ELSE 0 END) AS listing_sides,
            SUM(CASE WHEN s.role = 'buyer' THEN 1 ELSE 0 END) AS buyer_sides,
            MAX(s.close_date) AS most_recent,
            (
                SELECT GROUP_CONCAT(city, ', ') FROM (
                    SELECT city, COUNT(*) AS cnt FROM sides s3
                    WHERE s3.agent = s.agent AND s3.city IS NOT NULL
                    GROUP BY city ORDER BY cnt DESC LIMIT 3
                )
            ) AS primary_towns
        FROM sides s
        WHERE LOWER(s.agent) NOT IN (SELECT agent_lower FROM excluded)
        GROUP BY s.agent
        ORDER BY current_12mo_volume DESC, all_time_volume DESC
    '''

    # town_params is [town, town] when town is set (one value per UNION half),
    # else empty. Exclusions fill the VALUES clause. Cutoffs feed the
    # CASE WHEN aggregations in column order.
    params = town_params + exclusions + [
        cutoffs.current_12mo_start,                                  # current sides
        cutoffs.current_12mo_start,                                  # current volume
        cutoffs.prior_12mo_start, cutoffs.current_12mo_start,        # prior sides window
        cutoffs.prior_12mo_start, cutoffs.current_12mo_start,        # prior volume window
        cutoffs.three_year_start,                                    # 3yr sides
        cutoffs.three_year_start,                                    # 3yr volume
    ]

    if limit is not None:
        sql += ' LIMIT ?'
        params.append(limit)

    rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]
```

- [ ] **Step 5: Run tests — verify they pass**

Run: `python3 -m pytest tests/test_maine_kpis.py::TestQueryAgentKPIs -v`
Expected: all tests PASS.

- [ ] **Step 6: Commit**

```bash
git add src/maine_kpis.py tests/test_maine_kpis.py
git commit -m "feat: query_agent_kpis with period rollups"
```

---

### Task A4: `query_brokerage_kpis()` — same shape at office level

**Files:**
- Modify: `src/maine_kpis.py` (replace the `query_brokerage_kpis` stub)
- Modify: `tests/test_maine_kpis.py` (add tests)

- [ ] **Step 1: Write failing tests**

Append to `tests/test_maine_kpis.py`:

```python
class TestQueryBrokerageKPIs:
    def test_aggregates_by_office(self, kpi_conn):
        conn, today = kpi_conn
        rows = query_brokerage_kpis(conn, today=today)
        offices = {r['name'] for r in rows}
        # Acme should appear (Alice's office, 5 listings)
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
        # Bob had 4 buyer sides at BBrok
        assert bbrok['buyer_sides'] == 4
        # Plus 1 listing side (Alice sold to Bob who was at BBrok → Bob's office)
        # Actually wait — BBrok appears as buyer_office on Alice's row 1,
        # and as buyer_office on Bob's own rows. So BBrok agents = {Bob} = 1.
        assert bbrok['agent_count'] == 1

    def test_top_agents_rollup(self, kpi_conn):
        conn, today = kpi_conn
        rows = query_brokerage_kpis(conn, today=today)
        acme = next(r for r in rows if r['name'] == 'Acme')
        assert 'Alice' in (acme['top_agents'] or '')
```

- [ ] **Step 2: Run tests — verify they fail**

Run: `python3 -m pytest tests/test_maine_kpis.py::TestQueryBrokerageKPIs -v`
Expected: FAIL (NotImplementedError).

- [ ] **Step 3: Implement `query_brokerage_kpis`**

In `src/maine_kpis.py`, replace the stub:

```python
def query_brokerage_kpis(
    conn: sqlite3.Connection,
    *,
    town: Optional[str] = None,
    limit: Optional[int] = None,
    today: Optional[str] = None,
) -> list[dict]:
    """One row per brokerage. Same period columns as query_agent_kpis plus:
    - agent_count (# distinct agent names at that brokerage)
    - top_agents (comma-separated top 3 agents by count at that brokerage)

    Brokerages are kept at branch level — no normalization.
    """
    cutoffs = compute_cutoffs(today)

    town_sql_listing = ''
    town_sql_buyer = ''
    town_params: list = []
    if town:
        town_sql_listing = 'AND LOWER(city) = LOWER(?)'
        town_sql_buyer = 'AND LOWER(city) = LOWER(?)'
        town_params = [town, town]

    sql = f'''
        WITH sides AS (
            SELECT listing_office AS office,
                   listing_agent AS agent,
                   'listing' AS role,
                   sale_price, city, close_date
            FROM maine_transactions
            WHERE enrichment_status = 'success'
              AND listing_office IS NOT NULL AND TRIM(listing_office) != ''
              {town_sql_listing}
            UNION ALL
            SELECT buyer_office AS office,
                   buyer_agent AS agent,
                   'buyer' AS role,
                   sale_price, city, close_date
            FROM maine_transactions
            WHERE enrichment_status = 'success'
              AND buyer_office IS NOT NULL AND TRIM(buyer_office) != ''
              {town_sql_buyer}
        )
        SELECT
            s.office AS name,
            COUNT(DISTINCT s.agent) AS agent_count,
            SUM(CASE WHEN s.close_date >= ? THEN 1 ELSE 0 END) AS current_12mo_sides,
            SUM(CASE WHEN s.close_date >= ? THEN COALESCE(s.sale_price, 0) ELSE 0 END) AS current_12mo_volume,
            SUM(CASE WHEN s.close_date >= ? AND s.close_date < ? THEN 1 ELSE 0 END) AS prior_12mo_sides,
            SUM(CASE WHEN s.close_date >= ? AND s.close_date < ? THEN COALESCE(s.sale_price, 0) ELSE 0 END) AS prior_12mo_volume,
            SUM(CASE WHEN s.close_date >= ? THEN 1 ELSE 0 END) AS three_yr_sides,
            SUM(CASE WHEN s.close_date >= ? THEN COALESCE(s.sale_price, 0) ELSE 0 END) AS three_yr_volume,
            COUNT(*) AS all_time_sides,
            SUM(COALESCE(s.sale_price, 0)) AS all_time_volume,
            SUM(CASE WHEN s.role = 'listing' THEN 1 ELSE 0 END) AS listing_sides,
            SUM(CASE WHEN s.role = 'buyer' THEN 1 ELSE 0 END) AS buyer_sides,
            MAX(s.close_date) AS most_recent,
            (
                SELECT GROUP_CONCAT(agent, ', ') FROM (
                    SELECT agent, COUNT(*) AS cnt FROM sides s2
                    WHERE s2.office = s.office AND s2.agent IS NOT NULL
                    GROUP BY agent ORDER BY cnt DESC LIMIT 3
                )
            ) AS top_agents,
            (
                SELECT GROUP_CONCAT(city, ', ') FROM (
                    SELECT city, COUNT(*) AS cnt FROM sides s3
                    WHERE s3.office = s.office AND s3.city IS NOT NULL
                    GROUP BY city ORDER BY cnt DESC LIMIT 3
                )
            ) AS primary_towns
        FROM sides s
        GROUP BY s.office
        ORDER BY current_12mo_volume DESC, all_time_volume DESC
    '''

    params = town_params + [
        cutoffs.current_12mo_start,
        cutoffs.current_12mo_start,
        cutoffs.prior_12mo_start, cutoffs.current_12mo_start,
        cutoffs.prior_12mo_start, cutoffs.current_12mo_start,
        cutoffs.three_year_start,
        cutoffs.three_year_start,
    ]

    if limit is not None:
        sql += ' LIMIT ?'
        params.append(limit)

    rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]
```

- [ ] **Step 4: Run tests — verify they pass**

Run: `python3 -m pytest tests/test_maine_kpis.py -v`
Expected: all tests in `test_maine_kpis.py` PASS. Full suite should still pass:
`python3 -m pytest tests/ -q` → 42+ passed.

- [ ] **Step 5: Commit**

```bash
git add src/maine_kpis.py tests/test_maine_kpis.py
git commit -m "feat: query_brokerage_kpis with branch-level aggregation"
```

---

### Task A5: Extend `build_maine_search_index()` with period breakdowns

**Files:**
- Modify: `src/maine_report.py`
- Modify: `tests/test_maine_report.py`

This enriches the agent search index with period metrics so the detail modal can show "Last 12mo: X sides" without issuing separate queries.

- [ ] **Step 1: Update the test to expect new fields**

In `tests/test_maine_report.py`, in the `TestPopulatedReport` class, add:

```python
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
```

- [ ] **Step 2: Run the test — verify it fails**

Run: `python3 -m pytest tests/test_maine_report.py::TestPopulatedReport::test_search_index_has_period_fields -v`
Expected: FAIL (missing fields).

- [ ] **Step 3: Rewrite `build_maine_search_index` to delegate to `query_agent_kpis`**

In `src/maine_report.py`, find `build_maine_search_index` and replace with:

```python
def build_maine_search_index(conn: sqlite3.Connection) -> list[dict]:
    """Build search records for index_page.py.

    Delegates to maine_kpis.query_agent_kpis so the master table, search
    results, and detail modal all share one source of truth.
    """
    if conn is None:
        return []
    # Import here to avoid import cycle at module load time.
    from .maine_kpis import query_agent_kpis
    rows = query_agent_kpis(conn)
    return [
        {
            'source': 'maine',
            'name': r['name'],
            'office': r['office'] or '',
            'total_sides': r['all_time_sides'],          # back-compat with existing consumers
            'listing_sides': r['listing_sides'],
            'buyer_sides': r['buyer_sides'],
            'volume': int(r['all_time_volume'] or 0),     # back-compat
            'most_recent': r['most_recent'] or '',
            'towns': (r['primary_towns'] or '').split(', ') if r['primary_towns'] else [],
            # New period breakdowns used by the detail modal:
            'current_12mo_volume': int(r['current_12mo_volume'] or 0),
            'current_12mo_sides': r['current_12mo_sides'],
            'prior_12mo_volume':  int(r['prior_12mo_volume'] or 0),
            'prior_12mo_sides':   r['prior_12mo_sides'],
            'three_yr_volume':    int(r['three_yr_volume'] or 0),
            'three_yr_sides':     r['three_yr_sides'],
            'all_time_volume':    int(r['all_time_volume'] or 0),
            'all_time_sides':     r['all_time_sides'],
        }
        for r in rows
    ]
```

- [ ] **Step 4: Run tests — verify they pass**

Run: `python3 -m pytest tests/test_maine_report.py tests/test_maine_kpis.py -v`
Expected: all tests PASS. Note the existing `test_search_index_returns_agents` test should still pass because we preserved the back-compat fields (`total_sides`, `volume`, `towns`).

- [ ] **Step 5: Commit**

```bash
git add src/maine_report.py tests/test_maine_report.py
git commit -m "feat: enrich maine search index with period breakdowns"
```

---

## Phase B — Static HTML Dashboard

### Task B1: Rewrite `maine_dashboard.py` to use KPI queries + movers banner

**Files:**
- Modify: `src/maine_dashboard.py` (substantial — most of the file changes)

- [ ] **Step 1: Read the existing file to understand current patterns**

Run: `cat src/maine_dashboard.py | head -100`
Familiarize with `_combined_section`, `_role_section`, `_brokerage_section`.

- [ ] **Step 2: Replace the three render helpers with a single KPI-based leaderboard renderer**

Replace the entire file content of `src/maine_dashboard.py` with:

```python
"""HTML dashboard for Maine Listings (MREIS MLS) leaderboards.

Produces data/maine_dashboard.html — a standalone page showing agent and
brokerage leaderboards powered by period-based KPIs, plus a "Biggest Movers"
banner comparing last-12mo vs prior-12mo ranking.
"""
from __future__ import annotations

import logging
import os
import sqlite3
from datetime import datetime
from html import escape

from .dashboard import _css, _sort_js
from .maine_kpis import (
    PERIOD_12MO_DAYS,
    compute_rank_movers,
    query_agent_kpis,
    query_brokerage_kpis,
)
from .maine_report import format_currency
from .state import TOWNS

logger = logging.getLogger(__name__)

_DEFAULT_DASHBOARD = os.path.join(
    os.path.dirname(__file__), '..', 'data', 'maine_dashboard.html',
)


def _e(text) -> str:
    if text is None:
        return 'N/A'
    return escape(str(text))


def _fmt_delta(delta) -> str:
    """Render a rank delta as ↑N / ↓N / NEW / —."""
    if delta is None:
        return '<span class="delta-new">NEW</span>'
    if delta > 0:
        return f'<span class="delta-up">&#9650;{delta}</span>'
    if delta < 0:
        return f'<span class="delta-down">&#9660;{abs(delta)}</span>'
    return '<span class="delta-flat">&mdash;</span>'


def _movers_banner(rows: list[dict], label: str) -> str:
    """Render the Biggest Movers banner for a given entity set."""
    movers = compute_rank_movers(rows, min_sides=5, top_n=5)
    # Hide when too thin.
    qualifying = len([r for r in rows if (r.get('current_12mo_sides') or 0) >= 5])
    if qualifying < 10:
        return ''

    def _card(m, direction):
        cur_vol = int(m.get('current_12mo_volume') or 0)
        prior_vol = int(m.get('prior_12mo_volume') or 0)
        pct = f'+{int((cur_vol - prior_vol) / prior_vol * 100)}%' if prior_vol > 0 else '&mdash;'
        return (
            f'<div class="mover-card {direction}">'
            f'<span class="mover-delta">{_fmt_delta(m.get("delta"))}</span>'
            f'<span class="mover-name">{_e(m.get("name"))}</span>'
            f'<span class="mover-office">{_e(m.get("office") or "")}</span>'
            f'<span class="mover-line">12mo: {format_currency(cur_vol)} '
            f'(vs {format_currency(prior_vol)})  {pct}</span>'
            f'</div>'
        )

    risers_html = ''.join(_card(m, 'up') for m in movers['risers'])
    fallers_html = ''.join(_card(m, 'down') for m in movers['fallers'])

    return f'''
    <section class="movers">
        <h2>&#128293; Biggest Movers &mdash; {escape(label)} (12mo vs prior 12mo)</h2>
        <div class="movers-grid">
            <div class="movers-col"><h3>&#9650; Risers</h3>{risers_html or '<p class="empty">No qualifying risers.</p>'}</div>
            <div class="movers-col"><h3>&#9660; Fallers</h3>{fallers_html or '<p class="empty">No qualifying fallers.</p>'}</div>
        </div>
    </section>
    '''


_AGENT_COLUMNS = [
    ('#',             'num',  None),
    ('Agent',         '',     'name'),
    ('Office',        '',     'office'),
    ('12mo Δ',        'num',  'delta'),
    ('12mo Vol',      'num',  'current_12mo_volume'),
    ('12mo Sides',    'num',  'current_12mo_sides'),
    ('3yr Vol',       'num',  'three_yr_volume'),
    ('All-Time Vol',  'num',  'all_time_volume'),
    ('All-Time Sides', 'num', 'all_time_sides'),
    ('L / B',         'num',  None),
    ('Avg (3yr)',     'num',  None),
    ('Most Recent',   'num',  'most_recent'),
]

_BROKERAGE_COLUMNS = [
    ('#',             'num',  None),
    ('Brokerage',     '',     'name'),
    ('Agents',        'num',  'agent_count'),
    ('12mo Δ',        'num',  'delta'),
    ('12mo Vol',      'num',  'current_12mo_volume'),
    ('12mo Sides',    'num',  'current_12mo_sides'),
    ('3yr Vol',       'num',  'three_yr_volume'),
    ('All-Time Vol',  'num',  'all_time_volume'),
    ('All-Time Sides', 'num', 'all_time_sides'),
    ('L / B',         'num',  None),
    ('Avg (3yr)',     'num',  None),
    ('Most Recent',   'num',  'most_recent'),
]


def _row_html(i: int, r: dict, deltas: dict, is_brokerage: bool) -> str:
    delta = deltas.get(r['name'])
    three_yr_sides = r.get('three_yr_sides') or 0
    three_yr_vol = r.get('three_yr_volume') or 0
    avg_3yr = three_yr_vol / three_yr_sides if three_yr_sides else 0
    lb = f'{r.get("listing_sides") or 0} : {r.get("buyer_sides") or 0}'
    cells = [
        f'<td class="num">{i}</td>',
        f'<td class="agent-name">{_e(r["name"])}</td>',
    ]
    if is_brokerage:
        cells.append(f'<td class="num">{r.get("agent_count") or 0}</td>')
    else:
        cells.append(f'<td>{_e(r.get("office") or "")}</td>')
    cells += [
        f'<td class="num">{_fmt_delta(delta)}</td>',
        f'<td class="num">{_e(format_currency(r.get("current_12mo_volume") or 0))}</td>',
        f'<td class="num">{r.get("current_12mo_sides") or 0}</td>',
        f'<td class="num">{_e(format_currency(r.get("three_yr_volume") or 0))}</td>',
        f'<td class="num">{_e(format_currency(r.get("all_time_volume") or 0))}</td>',
        f'<td class="num">{r.get("all_time_sides") or 0}</td>',
        f'<td class="num">{lb}</td>',
        f'<td class="num">{_e(format_currency(avg_3yr))}</td>',
        f'<td class="num">{_e(r.get("most_recent") or "")}</td>',
    ]
    return f'<tr>{"".join(cells)}</tr>'


def _leaderboard_table(rows: list[dict], is_brokerage: bool, title: str) -> str:
    movers = compute_rank_movers(rows, min_sides=5, top_n=5)
    deltas = {}
    for m in movers['risers'] + movers['fallers']:
        deltas[m['name']] = m.get('delta')

    columns = _BROKERAGE_COLUMNS if is_brokerage else _AGENT_COLUMNS
    header = ''.join(f'<th class="{c[1]}">{_e(c[0])}</th>' for c in columns)
    body = '\n'.join(_row_html(i, r, deltas, is_brokerage) for i, r in enumerate(rows, 1))

    return f'''
    <section class="section">
        <h2>{_e(title)}</h2>
        <div class="table-wrap"><table class="sortable">
            <thead><tr>{header}</tr></thead>
            <tbody>{body or '<tr><td class="empty" colspan="12">No data.</td></tr>'}</tbody>
        </table></div>
    </section>
    '''


def _stats(conn) -> dict:
    row = conn.execute('''
        SELECT COUNT(*) AS total,
               SUM(CASE WHEN enrichment_status='success' THEN 1 ELSE 0 END) AS enriched,
               MIN(close_date) AS date_min, MAX(close_date) AS date_max,
               SUM(COALESCE(sale_price,0)) AS volume
        FROM maine_transactions
    ''').fetchone()
    return dict(row)


def _movers_css() -> str:
    return '''
    .movers { margin: 24px 0; }
    .movers h2 { font-size: 1rem; letter-spacing: 0.02em; margin-bottom: 10px; color: var(--text-1); }
    .movers-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
    .movers-col h3 { font-size: 0.75rem; color: var(--text-2); margin-bottom: 8px; font-family: var(--mono); }
    .mover-card { padding: 10px 12px; background: var(--bg-elevated); border-radius: 8px; margin-bottom: 6px;
        display: grid; grid-template-columns: auto 1fr; gap: 4px 10px; font-size: 0.78rem; }
    .mover-card .mover-delta { grid-row: span 2; align-self: center; font-family: var(--mono); font-weight: 600; }
    .mover-card .mover-name { font-weight: 600; color: var(--text-1); }
    .mover-card .mover-office { font-size: 0.72rem; color: var(--text-2); }
    .mover-card .mover-line { grid-column: 1 / -1; font-size: 0.72rem; color: var(--text-3); font-family: var(--mono); }
    .delta-up { color: hsl(140, 60%, 60%); }
    .delta-down { color: hsl(0, 60%, 62%); }
    .delta-new { color: var(--accent); font-weight: 600; }
    .delta-flat { color: var(--text-3); }
    .empty { color: var(--text-3); font-style: italic; font-size: 0.8rem; }
    '''


def generate_maine_dashboard(
    conn: sqlite3.Connection,
    output_path: str | None = None,
) -> str:
    """Generate data/maine_dashboard.html."""
    output_path = output_path or _DEFAULT_DASHBOARD
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    agent_rows = query_agent_kpis(conn)
    brokerage_rows = query_brokerage_kpis(conn)
    stats = _stats(conn)
    generated_at = datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')

    sections = [
        _movers_banner(agent_rows, 'Agents'),
        _leaderboard_table(agent_rows[:50], False, 'Top 50 Agents — All Towns'),
        _movers_banner(brokerage_rows, 'Brokerages'),
        _leaderboard_table(brokerage_rows[:50], True, 'Top 50 Brokerages — All Towns'),
    ]

    for town in TOWNS:
        town_agents = query_agent_kpis(conn, town=town, limit=50)
        town_brokers = query_brokerage_kpis(conn, town=town, limit=50)
        if not town_agents and not town_brokers:
            continue
        sections.append(_leaderboard_table(town_agents, False, f'Top 50 Agents — {town}'))
        sections.append(_leaderboard_table(town_brokers, True, f'Top 50 Brokerages — {town}'))

    body = '\n'.join(s for s in sections if s)

    html = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Maine MLS Leaderboard</title>
    <style>{_css()}{_movers_css()}</style>
</head>
<body>
    <div class="wrap">
        <header class="header">
            <h1>Maine MLS Leaderboard</h1>
            <p class="sub">MaineListings.com (MREIS) &middot; 10 Towns &middot; {_e(generated_at)}</p>
        </header>
        <div class="stats">
            <div class="stat"><div class="label">Transactions</div><div class="value">{stats["enriched"]:,}</div></div>
            <div class="stat"><div class="label">Agents</div><div class="value">{len(agent_rows):,}</div></div>
            <div class="stat"><div class="label">Brokerages</div><div class="value">{len(brokerage_rows):,}</div></div>
            <div class="stat"><div class="label">Volume</div><div class="value">{_e(format_currency(stats["volume"] or 0))}</div></div>
        </div>
        <main>{body}</main>
        <footer class="footer">Generated {_e(generated_at)} &middot; MaineListings.com MREIS</footer>
    </div>
    <script>{_sort_js()}</script>
</body>
</html>'''

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)
    logger.info('Maine dashboard written to %s', output_path)
    return output_path
```

- [ ] **Step 3: Run tests — make sure nothing upstream broke**

Run: `python3 -m pytest tests/ -q`
Expected: all tests still pass (42+).

- [ ] **Step 4: Regenerate the dashboard and spot-check it visually**

Run: `python3 -m src.maine_main --report`
Expected output: `Maine dashboard written to ...`. Open `http://localhost:8766/maine.html` (after `cp data/maine_dashboard.html data/maine.html` to refresh the served copy) and verify:
- Top 50 agent table renders
- Rank delta column shows ↑/↓/NEW
- Movers banner appears with risers/fallers
- Per-town sections render

Run: `cp data/maine_dashboard.html data/maine.html`

- [ ] **Step 5: Commit**

```bash
git add src/maine_dashboard.py
git commit -m "feat: KPI-based Maine static dashboard with movers banner"
```

---

## Phase C — Interactive "Leaderboard" Tab in `index_page.py`

### Task C1: Embed agent + brokerage KPI JSON in index.html

**Files:**
- Modify: `src/index_page.py` (top-level orchestration + script tags)

- [ ] **Step 1: Add KPI queries to `generate_index_html`**

In `src/index_page.py`, find `generate_index_html` (near top of file) and update:

```python
def generate_index_html(
    redfin_conn=None,
    zillow_conn=None,
    maine_conn=None,
    output_path: str | None = None,
) -> str:
    """Generate index.html with tab navigation and agent search."""
    output_path = output_path or _DEFAULT_OUTPUT
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    redfin_index = build_agent_search_index(redfin_conn) if redfin_conn else []
    zillow_index = build_zillow_search_index(zillow_conn) if zillow_conn else []
    maine_index = build_maine_search_index(maine_conn) if maine_conn else []

    # New: KPI rollups for the Leaderboard tab.
    agent_kpis: list = []
    brokerage_kpis: list = []
    if maine_conn is not None:
        from .maine_kpis import query_agent_kpis, query_brokerage_kpis
        agent_kpis = query_agent_kpis(maine_conn)
        brokerage_kpis = query_brokerage_kpis(maine_conn)

    redfin_json = json.dumps(redfin_index, separators=(',', ':'))
    zillow_json = json.dumps(zillow_index, separators=(',', ':'))
    maine_json = json.dumps(maine_index, separators=(',', ':'))
    agent_json = json.dumps(agent_kpis, separators=(',', ':'), default=str)
    brokerage_json = json.dumps(brokerage_kpis, separators=(',', ':'), default=str)

    html = _build_html(redfin_json, zillow_json, maine_json, agent_json, brokerage_json)

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)
    logger.info(
        'Index page written to %s (%d Redfin, %d Zillow, %d Maine; '
        '%d agent KPIs, %d brokerage KPIs)',
        output_path, len(redfin_index), len(zillow_index), len(maine_index),
        len(agent_kpis), len(brokerage_kpis),
    )
    return output_path
```

- [ ] **Step 2: Update `_build_html` signature + embed new script tags**

In `_build_html`, update the signature and add the two new script tags near the existing ones:

```python
def _build_html(
    redfin_json: str,
    zillow_json: str,
    maine_json: str,
    agent_json: str,
    brokerage_json: str,
) -> str:
    return f'''<!DOCTYPE html>
...
    <script id="redfin-index" type="application/json">{redfin_json}</script>
    <script id="zillow-index" type="application/json">{zillow_json}</script>
    <script id="maine-index" type="application/json">{maine_json}</script>
    <script id="agent-kpis" type="application/json">{agent_json}</script>
    <script id="brokerage-kpis" type="application/json">{brokerage_json}</script>
    <script>{_search_js()}</script>
    <script>{_leaderboard_js()}</script>
</body>
</html>'''
```

(Keep the rest of the HTML intact; only the bottom script-tag area changes.)

- [ ] **Step 3: Add the empty `_leaderboard_js()` stub**

At the bottom of `src/index_page.py`, before the `def _search_js()` or after — add:

```python
def _leaderboard_js() -> str:
    """JS for the redesigned Leaderboard tab (Maine KPI-driven).

    Consumes #agent-kpis and #brokerage-kpis JSON, renders the master table,
    movers banner, and handles Agent/Brokerage toggle + Town filter +
    Period selector + in-table search.
    """
    return '/* Leaderboard JS — implemented in Task C2+ */'
```

- [ ] **Step 4: Regenerate + verify embed**

Run: `python3 -m src.maine_main --update-index`
Run: `grep -c 'id="agent-kpis"' data/index.html`
Expected: `1`.
Run: `grep -c 'id="brokerage-kpis"' data/index.html`
Expected: `1`.

- [ ] **Step 5: Commit**

```bash
git add src/index_page.py
git commit -m "feat: embed agent+brokerage KPIs into index.html"
```

---

### Task C2: Rebuild the Leaderboard table in JS (entity toggle, columns, sort)

**Files:**
- Modify: `src/index_page.py` (the master-table block in `_search_js()` and the new `_leaderboard_js()`)

This task reshapes the existing master-table JS. We move the table rendering from `_search_js` into the dedicated `_leaderboard_js`, swap the column set, and add the entity toggle.

- [ ] **Step 1: Delete the old master-table JS block in `_search_js()`**

In `src/index_page.py`, find the `// === MASTER TABLE ===` comment and delete that entire section (from `const masterBody = document.getElementById("master-body");` through the closing `});` that ends the tab-click listener — roughly 100 lines). Keep everything else (unified search, detail modal, tab switching) intact.

- [ ] **Step 2: Update the `<div id="master">` block in `_build_html`**

Find the master-tab div (contains `<table id="master-table">`) and replace it with the new Leaderboard layout:

```python
        <div id="master" class="master-tab">
            <div class="master-filters">
                <div class="entity-toggle">
                    <button class="pill active" data-entity="agent">Agents</button>
                    <button class="pill" data-entity="brokerage">Brokerages</button>
                </div>
                <select id="filter-town"><option value="">All Towns</option></select>
                <select id="filter-period">
                    <option value="current_12mo_volume" selected>12mo</option>
                    <option value="three_yr_volume">3yr</option>
                    <option value="all_time_volume">All-Time</option>
                </select>
                <input type="text" id="filter-name" placeholder="Filter by name or office...">
                <span id="master-count" class="master-count"></span>
            </div>
            <div id="movers-banner" class="movers-banner"></div>
            <div class="table-wrap"><table id="master-table">
                <thead><tr id="master-head"></tr></thead>
                <tbody id="master-body"></tbody>
            </table></div>
        </div>
```

- [ ] **Step 3: Add CSS for the new UI elements**

In `_css()`, find `.master-filters` and add/merge these rules near it:

```css
    .entity-toggle { display: inline-flex; background: var(--bg-elevated); border-radius: 20px; padding: 2px; margin-right: 12px; }
    .entity-toggle .pill { padding: 4px 12px; font-size: 0.75rem; font-family: var(--mono);
        background: transparent; border: none; color: var(--text-2); cursor: pointer; border-radius: 18px; }
    .entity-toggle .pill.active { background: var(--accent); color: #1a1a1a; font-weight: 600; }
    .movers-banner { margin: 12px 0; padding: 12px; background: var(--bg-surface); border-radius: 10px; }
    .movers-banner.hidden { display: none; }
    .movers-banner h3 { font-size: 0.78rem; margin-bottom: 8px; color: var(--text-2); letter-spacing: 0.04em; }
    .movers-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }
    .movers-col-title { font-size: 0.7rem; text-transform: uppercase; color: var(--text-3); margin-bottom: 6px; }
    .mover-card { padding: 8px 10px; background: var(--bg-elevated); border-radius: 6px; margin-bottom: 4px;
        display: grid; grid-template-columns: 40px 1fr; gap: 2px 8px; font-size: 0.75rem; cursor: pointer; }
    .mover-card:hover { background: var(--bg-hover); }
    .mover-card .mover-delta { grid-row: span 2; align-self: center; font-family: var(--mono); font-weight: 600; text-align: center; }
    .mover-card .mover-name { font-weight: 600; }
    .mover-card .mover-office { font-size: 0.7rem; color: var(--text-2); }
    .mover-card .mover-line { grid-column: 1 / -1; font-size: 0.68rem; color: var(--text-3); font-family: var(--mono); }
    .delta-up { color: hsl(140, 60%, 60%); }
    .delta-down { color: hsl(0, 60%, 62%); }
    .delta-new { color: var(--accent); }
    .delta-flat { color: var(--text-3); }
```

- [ ] **Step 4: Fill in `_leaderboard_js()` with the full implementation**

Replace the stub with:

```python
def _leaderboard_js() -> str:
    return r'''
    (function(){
        const agentRows = JSON.parse(document.getElementById("agent-kpis").textContent);
        const brokerageRows = JSON.parse(document.getElementById("brokerage-kpis").textContent);
        const head = document.getElementById("master-head");
        const body = document.getElementById("master-body");
        const count = document.getElementById("master-count");
        const filterTown = document.getElementById("filter-town");
        const filterPeriod = document.getElementById("filter-period");
        const filterName = document.getElementById("filter-name");
        const moversBanner = document.getElementById("movers-banner");

        let entity = "agent";  // "agent" or "brokerage"
        let sort = {col: "current_12mo_volume", asc: false};

        // Column defs per entity
        const COLS = {
            agent: [
                {key: null,                  label: "#",           num: true,  sortable: false},
                {key: "name",                label: "Agent",       num: false, sortable: true},
                {key: "office",              label: "Office",      num: false, sortable: true},
                {key: "_delta",              label: "12mo Δ",      num: true,  sortable: true},
                {key: "current_12mo_volume", label: "12mo Vol",    num: true,  sortable: true, fmt: "cur"},
                {key: "current_12mo_sides",  label: "12mo Sides",  num: true,  sortable: true},
                {key: "three_yr_volume",     label: "3yr Vol",     num: true,  sortable: true, fmt: "cur"},
                {key: "all_time_volume",     label: "All-Time Vol", num: true, sortable: true, fmt: "cur"},
                {key: "all_time_sides",      label: "All-Time",    num: true,  sortable: true},
                {key: "_lb",                 label: "L / B",       num: true,  sortable: false},
                {key: "_avg3",               label: "Avg (3yr)",   num: true,  sortable: true, fmt: "cur"},
                {key: "most_recent",         label: "Most Recent", num: true,  sortable: true},
            ],
            brokerage: [
                {key: null,                  label: "#",           num: true,  sortable: false},
                {key: "name",                label: "Brokerage",   num: false, sortable: true},
                {key: "agent_count",         label: "Agents",      num: true,  sortable: true},
                {key: "_delta",              label: "12mo Δ",      num: true,  sortable: true},
                {key: "current_12mo_volume", label: "12mo Vol",    num: true,  sortable: true, fmt: "cur"},
                {key: "current_12mo_sides",  label: "12mo Sides",  num: true,  sortable: true},
                {key: "three_yr_volume",     label: "3yr Vol",     num: true,  sortable: true, fmt: "cur"},
                {key: "all_time_volume",     label: "All-Time Vol", num: true, sortable: true, fmt: "cur"},
                {key: "all_time_sides",      label: "All-Time",    num: true,  sortable: true},
                {key: "_lb",                 label: "L / B",       num: true,  sortable: false},
                {key: "_avg3",               label: "Avg (3yr)",   num: true,  sortable: true, fmt: "cur"},
                {key: "most_recent",         label: "Most Recent", num: true,  sortable: true},
            ],
        };

        // Populate town filter from agent rows' primary_towns
        const towns = new Set();
        agentRows.forEach(r => {
            if (r.primary_towns) r.primary_towns.split(",").forEach(t => towns.add(t.trim()));
        });
        [...towns].filter(Boolean).sort().forEach(t => {
            const opt = document.createElement("option");
            opt.value = t; opt.textContent = t;
            filterTown.appendChild(opt);
        });

        function fmtCur(n) {
            n = Math.round(n || 0);
            if (n >= 1e6) return "$" + (n/1e6).toFixed(1) + "M";
            if (n >= 1e3) return "$" + Math.round(n/1e3) + "K";
            if (n === 0) return "—";
            return "$" + n.toLocaleString();
        }
        function esc(s) { const d = document.createElement("div"); d.textContent = s == null ? "" : s; return d.innerHTML; }

        function enrichRow(r) {
            const threeSides = r.three_yr_sides || 0;
            const threeVol = r.three_yr_volume || 0;
            return Object.assign({}, r, {
                _avg3: threeSides > 0 ? threeVol / threeSides : 0,
                _lb: (r.listing_sides || 0) + " : " + (r.buyer_sides || 0),
            });
        }

        function computeMovers(rows) {
            // Rank by current 12mo sides
            const currentSorted = [...rows].sort((a,b) => (b.current_12mo_sides||0) - (a.current_12mo_sides||0));
            const currentRank = {}; currentSorted.forEach((r,i) => currentRank[r.name] = i+1);

            const prior = rows.filter(r => (r.prior_12mo_sides||0) > 0);
            const priorSorted = [...prior].sort((a,b) => (b.prior_12mo_sides||0) - (a.prior_12mo_sides||0));
            const priorRank = {}; priorSorted.forEach((r,i) => priorRank[r.name] = i+1);

            const deltas = {};
            const risers = [], fallers = [], news = [];
            rows.forEach(r => {
                if ((r.current_12mo_sides||0) < 5) return;
                const pr = priorRank[r.name];
                if (pr == null) { deltas[r.name] = null; news.push(r); return; }
                const d = pr - currentRank[r.name];
                deltas[r.name] = d;
                if (d > 0) risers.push({...r, delta: d});
                else if (d < 0) fallers.push({...r, delta: d});
            });
            news.sort((a,b) => (b.current_12mo_volume||0) - (a.current_12mo_volume||0));
            risers.sort((a,b) => b.delta - a.delta);
            fallers.sort((a,b) => a.delta - b.delta);
            return {
                deltas,
                risers: [...news.map(r => ({...r, delta: null})), ...risers].slice(0, 5),
                fallers: fallers.slice(0, 5),
                qualifying: rows.filter(r => (r.current_12mo_sides||0) >= 5).length,
            };
        }

        function fmtDelta(d) {
            if (d == null) return '<span class="delta-new">NEW</span>';
            if (d > 0)   return '<span class="delta-up">▲' + d + '</span>';
            if (d < 0)   return '<span class="delta-down">▼' + Math.abs(d) + '</span>';
            return '<span class="delta-flat">—</span>';
        }

        function renderMovers(movers) {
            if (movers.qualifying < 10) {
                moversBanner.classList.add("hidden");
                return;
            }
            moversBanner.classList.remove("hidden");
            function card(m, dir) {
                const pct = (m.prior_12mo_volume > 0)
                    ? Math.round((m.current_12mo_volume - m.prior_12mo_volume) / m.prior_12mo_volume * 100)
                    : null;
                const pctStr = pct == null ? "" : (pct > 0 ? "+" + pct + "%" : pct + "%");
                return '<div class="mover-card" data-name="' + esc(m.name) + '">' +
                    '<span class="mover-delta">' + fmtDelta(m.delta) + '</span>' +
                    '<span class="mover-name">' + esc(m.name) + '</span>' +
                    '<span class="mover-office">' + esc(m.office || "") + '</span>' +
                    '<span class="mover-line">12mo: ' + fmtCur(m.current_12mo_volume) +
                    ' (vs ' + fmtCur(m.prior_12mo_volume) + ')  ' + pctStr + '</span>' +
                    '</div>';
            }
            moversBanner.innerHTML =
                '<h3>🔥 Biggest Movers — 12mo vs prior 12mo</h3>' +
                '<div class="movers-grid">' +
                '<div><div class="movers-col-title">▲ Risers</div>' +
                (movers.risers.map(m => card(m, "up")).join("") || '<p class="no-data">No qualifying risers.</p>') +
                '</div>' +
                '<div><div class="movers-col-title">▼ Fallers</div>' +
                (movers.fallers.map(m => card(m, "down")).join("") || '<p class="no-data">No qualifying fallers.</p>') +
                '</div></div>';
        }

        function renderHead() {
            const cols = COLS[entity];
            head.innerHTML = cols.map((c, idx) => {
                const cls = (c.num ? "num " : "") + (c.sortable ? "sortable " : "") + (sort.col === c.key ? "sort-active" : "");
                const arrow = sort.col === c.key ? (sort.asc ? "▲" : "▼") : (c.sortable ? "↕" : "");
                return '<th class="' + cls.trim() + '" data-col="' + (c.key || "") + '">' +
                    esc(c.label) + ' <span class="sort-arrow">' + arrow + '</span></th>';
            }).join("");
            head.querySelectorAll("th.sortable").forEach(th => {
                th.addEventListener("click", () => {
                    const k = th.dataset.col;
                    sort = {col: k, asc: sort.col === k ? !sort.asc : false};
                    render();
                });
            });
        }

        function render() {
            const raw = entity === "agent" ? agentRows : brokerageRows;
            let rows = raw.map(enrichRow);

            // Town filter
            const townVal = filterTown.value.toLowerCase();
            if (townVal) rows = rows.filter(r => (r.primary_towns||"").toLowerCase().includes(townVal));

            // Name/office filter
            const nameVal = filterName.value.toLowerCase().trim();
            if (nameVal) rows = rows.filter(r =>
                (r.name||"").toLowerCase().includes(nameVal) ||
                (r.office||"").toLowerCase().includes(nameVal));

            // Compute movers on filtered set
            const movers = computeMovers(rows);
            renderMovers(movers);

            // Attach delta to rows
            rows = rows.map(r => ({...r, _delta: movers.deltas[r.name]}));

            // Sort
            const col = sort.col;
            if (col) {
                rows.sort((a, b) => {
                    const va = a[col], vb = b[col];
                    if (va == null && vb == null) return 0;
                    if (va == null) return 1;
                    if (vb == null) return -1;
                    if (typeof va === "string") return sort.asc ? va.localeCompare(vb) : vb.localeCompare(va);
                    return sort.asc ? va - vb : vb - va;
                });
            }

            // Town filter triggers top-50 cap
            if (townVal) rows = rows.slice(0, 50);

            // Render body
            const cols = COLS[entity];
            body.innerHTML = rows.map((r, i) => {
                const cells = cols.map(c => {
                    let v;
                    if (c.key === null) v = i + 1;
                    else if (c.key === "_delta") v = fmtDelta(r._delta);
                    else if (c.key === "_lb") v = r._lb;
                    else if (c.key === "_avg3") v = fmtCur(r._avg3);
                    else if (c.fmt === "cur") v = fmtCur(r[c.key]);
                    else if (c.key === "office") v = esc(r.office || "");
                    else if (c.key === "name") v = esc(r.name || "");
                    else if (c.key === "most_recent") v = esc(r.most_recent || "");
                    else v = r[c.key] != null ? r[c.key].toLocaleString() : "—";
                    return '<td class="' + (c.num ? "num" : "") + '">' + v + '</td>';
                }).join("");
                return '<tr data-name="' + esc(r.name) + '">' + cells + '</tr>';
            }).join("");
            count.textContent = rows.length + (townVal ? " (top 50 of " + townVal + ")" : " of " + raw.length);

            // Row click → existing detail modal (reuses showDetail from search JS)
            body.querySelectorAll("tr").forEach(tr => {
                tr.addEventListener("click", () => {
                    if (typeof window.__showDetailByName === "function") {
                        window.__showDetailByName(tr.dataset.name);
                    }
                });
            });
        }

        // Wire up filters
        document.querySelectorAll(".entity-toggle .pill").forEach(btn => {
            btn.addEventListener("click", () => {
                document.querySelectorAll(".entity-toggle .pill").forEach(b => b.classList.remove("active"));
                btn.classList.add("active");
                entity = btn.dataset.entity;
                renderHead();
                render();
            });
        });
        filterTown.addEventListener("change", render);
        filterPeriod.addEventListener("change", () => {
            sort = {col: filterPeriod.value, asc: false};
            render();
        });
        let t; filterName.addEventListener("input", () => { clearTimeout(t); t = setTimeout(render, 180); });

        // Mover card clicks
        document.addEventListener("click", e => {
            const card = e.target.closest(".mover-card");
            if (!card) return;
            const name = card.dataset.name;
            if (typeof window.__showDetailByName === "function") window.__showDetailByName(name);
        });

        // Lazy render when tab first opens
        let rendered = false;
        document.querySelectorAll(".tab").forEach(btn => {
            btn.addEventListener("click", () => {
                if (btn.dataset.tab === "master" && !rendered) {
                    renderHead();
                    render();
                    rendered = true;
                }
            });
        });
        // If the master tab is the default, render now.
        if (document.querySelector('.tab.active').dataset.tab === "master") {
            renderHead();
            render();
            rendered = true;
        }
    })();
    '''
```

- [ ] **Step 5: Expose `showDetail` for the leaderboard JS to reuse**

The leaderboard JS calls `window.__showDetailByName(name)`. Wire it up in `_search_js`. Find `function showDetail(g) {` and immediately before it add:

```javascript
    window.__showDetailByName = function(name) {
        const ql = (name || "").toLowerCase();
        const matches = [];
        [redfin, zillow, maine].forEach((arr, idx) => {
            const src = ["redfin","zillow","maine"][idx];
            arr.forEach(a => {
                if ((a.name || "").toLowerCase() === ql) matches.push({...a, _src: src});
            });
        });
        if (!matches.length) return;
        const g = {name: matches[0].name, office: matches[0].office, sources: [], data: {}};
        matches.forEach(m => { g.sources.push(m._src); g.data[m._src] = m; if (m.office) g.office = m.office; });
        showDetail(g);
    };
```

- [ ] **Step 6: Regenerate and verify visually**

Run: `python3 -m src.maine_main --update-index`
Reload `http://localhost:8766/` in the browser. Verify:
- Leaderboard tab (formerly "All Agents") loads with a 12-column table
- Agents / Brokerages pill toggle swaps the data
- Town dropdown filters and caps to top 50
- Period dropdown changes the sort
- Name filter narrows rows live
- Clicking a row opens the detail modal
- Biggest Movers banner appears above the table when qualifying count ≥ 10

- [ ] **Step 7: Run full test suite — make sure nothing regressed**

Run: `python3 -m pytest tests/ -q`
Expected: all tests PASS.

- [ ] **Step 8: Commit**

```bash
git add src/index_page.py
git commit -m "feat: KPI-driven Leaderboard tab with movers banner"
```

---

### Task C3: Rename "All Agents" tab to "Leaderboard" + update archived pills

**Files:**
- Modify: `src/index_page.py`

- [ ] **Step 1: Update the tab button label**

In `src/index_page.py`, find the tab button and change the visible label:

```python
# Before:
<button class="tab" data-tab="master">All Agents</button>

# After:
<button class="tab" data-tab="master">Leaderboard</button>
```

Leave the Maine MLS tab's "active" state intact; the Leaderboard tab is second in order but it's the workhorse.

- [ ] **Step 2: Regenerate and verify**

Run: `python3 -m src.maine_main --update-index`
Reload the browser. Tab bar should read: `[Maine MLS]` `[Leaderboard]` `[Zillow archive]` `[Redfin archive]`.

- [ ] **Step 3: Commit**

```bash
git add src/index_page.py
git commit -m "chore: rename 'All Agents' tab to 'Leaderboard'"
```

---

### Task C4: Upgrade the detail modal to render Maine period breakdowns

**Files:**
- Modify: `src/index_page.py` (the `showDetail` function inside `_search_js`)

- [ ] **Step 1: Find and update the Maine section of showDetail**

In `_search_js`, find the block `const md = g.data.maine;` and replace with:

```javascript
        const md = g.data.maine;
        if (md) {
            html += '<div class="source-label">Maine MLS &mdash; Closed Transactions</div>';
            html += '<div class="stat-row">';
            html += stat("Last 12mo",   md.current_12mo_sides + " sides");
            html += stat("12mo Vol",    fmtCur(md.current_12mo_volume));
            html += stat("Prior 12mo",  md.prior_12mo_sides + " sides");
            html += stat("Prior Vol",   fmtCur(md.prior_12mo_volume));
            html += stat("3yr",         md.three_yr_sides + " sides");
            html += stat("3yr Vol",     fmtCur(md.three_yr_volume));
            html += stat("All-Time",    md.all_time_sides + " sides");
            html += stat("All-Time Vol", fmtCur(md.all_time_volume));
            html += stat("L / B",        (md.listing_sides||0) + " / " + (md.buyer_sides||0));
            html += stat("Most Recent", md.most_recent || "N/A");
            html += '</div>';
            if (md.towns && md.towns.length) {
                html += '<div class="office-line" style="margin-top:10px;">Towns: ' +
                    esc((md.towns || []).filter(Boolean).join(", ")) + '</div>';
            }
        }
```

- [ ] **Step 2: Regenerate and visually verify**

Run: `python3 -m src.maine_main --update-index`
Reload the browser. Click an agent from the search bar or the Leaderboard table. The modal should now show all four period splits.

- [ ] **Step 3: Commit**

```bash
git add src/index_page.py
git commit -m "feat: detail modal shows Maine period breakdowns"
```

---

## Phase D — Regression + Polish

### Task D1: Full suite + dashboard regen + PR update

- [ ] **Step 1: Run the complete test suite**

Run: `python3 -m pytest tests/ -v --tb=short 2>&1 | tail -20`
Expected: all tests PASS. Current count is 42 Maine-related tests + ~160 other tests = ~200+.

- [ ] **Step 2: Regenerate all artifacts**

Run: `python3 -m src.maine_main --report --update-index`
Run: `cp data/maine_dashboard.html data/maine.html`
Run: `cp data/dashboard.html data/redfin.html 2>/dev/null || true`
Run: `cp data/zillow_directory_dashboard.html data/zillow.html 2>/dev/null || true`

- [ ] **Step 3: Local visual QA checklist**

Open `http://localhost:8766/` and verify:
- [ ] Maine MLS tab loads and renders sections
- [ ] Leaderboard tab loads and shows Agents by default
- [ ] Agents/Brokerages toggle works
- [ ] Town dropdown filters + caps to top 50
- [ ] Period dropdown changes sort
- [ ] Name filter narrows rows live
- [ ] Movers banner renders (qualifying set ≥ 10)
- [ ] Column sort works in either direction
- [ ] Row click opens detail modal with period breakdowns
- [ ] Mover card click opens detail modal
- [ ] Archived pills on Zillow + Redfin tabs
- [ ] Unified search (top-right) still works

- [ ] **Step 4: Update CLAUDE.md with the new module**

In `CLAUDE.md`, in the "File Structure" section, add under `src/`:

```
│   ├── maine_kpis.py                 # Period-based KPI queries + rank movers
```

In "Verification Commands → Maine Listings Pipeline", add:

```bash
python -m src.maine_main --update-index      # Rebuild tabbed dashboard (Maine + Leaderboard tabs)
```

- [ ] **Step 5: Commit**

```bash
git add CLAUDE.md data/
git commit -m "data: regenerated dashboards after leaderboard redesign"
```

- [ ] **Step 6: Push and update PR description**

Run: `git push origin feature/maine-listings-phase2`
Run: `gh pr view 11 --json url | python3 -c "import sys,json; print(json.load(sys.stdin)['url'])"`

Manually update the PR description to mention the leaderboard redesign (or add a comment):

```bash
gh pr comment 11 --body "Added KPI-driven Leaderboard tab per spec at docs/superpowers/specs/2026-04-16-maine-leaderboard-redesign-design.md. New module src/maine_kpis.py holds period queries + mover computation. 48+ Maine tests pass."
```

---

## Done criteria

- [ ] All 12 tasks checked off
- [ ] Full test suite passing (~200+ tests including new `test_maine_kpis.py`)
- [ ] `src/maine_kpis.py` exists with `compute_cutoffs`, `compute_rank_movers`, `query_agent_kpis`, `query_brokerage_kpis`
- [ ] `data/maine_dashboard.html` renders with movers banner + KPI tables (visual check)
- [ ] `data/index.html` Leaderboard tab has Agents/Brokerages toggle + Town filter + Period selector + in-table search + movers banner (visual check)
- [ ] All spec sections implemented (see spec §Main table columns, §Biggest Movers banner, §Filters, §Detail modal)
- [ ] PR #11 updated with these commits on `feature/maine-listings-phase2`
