# Maine MLS Active Listings Pipeline — Design Spec

**Date:** 2026-04-17
**Status:** Approved (pending spec review)
**Branch:** TBD (feature branch off `main`)

## Context

The Maine MLS scraper currently captures closed transactions only (`mls_status=Closed`), running weekly. We need to also capture **active listings** so that two separate downstream tools (in a new repo) can read from the same SQLite DB:

1. A direct-mail pipeline that targets owners of newly-listed homes, asking if they've considered renting the home short-term.
2. A listing-agent outreach pipeline that assembles STR revenue projections for properties and sends them to listing agents for engagement, with the user as a manual approval layer.

The downstream tools are explicitly out of scope for this spec — this work extends this scraper and DB, and freezes a queryable interface the downstream repo will consume.

## Goals

1. Scrape active listings daily for all 10 territory towns (same as closed pipeline).
2. Store active listings in the same table as closed transactions using a `status` column, with a child history table capturing every price/status change over the listing's lifecycle.
3. Detect withdrawn/delisted properties automatically.
4. Notify on failures, summarize daily runs, and alert on anomalies (zero-delta days).
5. Expose stable read helpers for downstream tools so the interface is versioned and testable.

## Non-Goals

- **Owner identification.** MLS data does not include homeowner names. Tax-assessor lookup belongs to the downstream mailer tool, not this scraper.
- **Building the downstream tools.** They live in a separate repo.
- **UI changes to dashboards or leaderboards.** Existing Maine MLS dashboard, Leaderboard tab, and KPI queries remain closed-only and unchanged. Active listings are a raw-data surface for downstream consumption only.

---

## Section 1 — Architecture & Data Flow

```
┌──────────────────────────────────────────────────────────────┐
│ GitHub Actions cron                                          │
│   • Monday 6:30 ET  →  weekly closed-transactions pipeline   │
│   • Every day 6:30 ET → new daily active-listings pipeline   │
└──────────────────────────────────────────────────────────────┘
                        │
                        ▼
┌──────────────────────────────────────────────────────────────┐
│ src/maine_firecrawl.py (extended)                            │
│                                                              │
│  discover_listings(status='Closed'|'Active', ...)            │
│      └─ URL: mainelistings.com/listings?city=X               │
│              &mls_status={status}&page=N                     │
│                                                              │
│  enrich_listings(...) — unchanged, acts on whatever is       │
│      currently unenriched (listing_agent IS NULL, etc.)      │
└──────────────────────────────────────────────────────────────┘
                        │
                        ▼
┌──────────────────────────────────────────────────────────────┐
│ data/maine_listings.db                                       │
│                                                              │
│  maine_transactions                                          │
│    • existing columns (closed-focused)                       │
│    • NEW: status ('Active'|'Pending'|'Closed'|'Withdrawn')   │
│    • NEW: list_date, last_seen_at                            │
│    • NEW: year_built, lot_sqft, description, photo_url       │
│                                                              │
│  maine_listing_history  (NEW child table)                    │
│    • detail_url, snapshot_date                               │
│    • status, list_price, days_on_market                      │
│    • Row inserted only when any watched field changes        │
└──────────────────────────────────────────────────────────────┘
                        │
                        ▼
           Downstream tools (separate repo)
             read DB via read-only open
```

### Operational details

- **Daily active run** scrapes all 10 towns with `mls_status=Active`. Takes ~60-120 Firecrawl credits/day (discovery + enrichment of net-new rows in steady state).
- **Weekly closed run** stays exactly as today — that schedule is owned by the closed side.
- **Delisting detection** — each daily run stamps every listing found with `last_seen_at=now`. A sweeper at end-of-run marks any previously-Active listing not seen in 7+ days as `Withdrawn`.
- **Status transitions** (Active → Pending → Closed) are automatic: when the daily run sees a listing with a new status, that's a normal upsert + history row.
- **Code reuse** — existing `discover_listings()` and `enrich_listings()` grow a `status` parameter. Same NUXT extraction JS; the closed-only parser already reads the fields we need.

---

## Section 2 — Schema Changes

### New columns on `maine_transactions` (all nullable, additive migration)

```sql
ALTER TABLE maine_transactions ADD COLUMN status TEXT;
    -- 'Active' | 'Pending' | 'Closed' | 'Withdrawn'
    -- Backfill existing 16,024 rows to 'Closed' in the migration.

ALTER TABLE maine_transactions ADD COLUMN list_date TEXT;
    -- ISO date. Populated for all listings going forward.

ALTER TABLE maine_transactions ADD COLUMN last_seen_at TEXT;
    -- ISO timestamp of the most recent scrape that saw this listing
    -- in search results. Powers the Withdrawn sweeper.

ALTER TABLE maine_transactions ADD COLUMN year_built INTEGER;
ALTER TABLE maine_transactions ADD COLUMN lot_sqft INTEGER;
ALTER TABLE maine_transactions ADD COLUMN description TEXT;
ALTER TABLE maine_transactions ADD COLUMN photo_url TEXT;
    -- Hero photo (first URL in the NUXT photos array).

CREATE INDEX idx_maine_status     ON maine_transactions(status);
CREATE INDEX idx_maine_last_seen  ON maine_transactions(last_seen_at);
```

### New table `maine_listing_history`

```sql
CREATE TABLE maine_listing_history (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    detail_url     TEXT NOT NULL,
    snapshot_date  TEXT NOT NULL,
    status         TEXT,
    list_price     INTEGER,
    FOREIGN KEY (detail_url) REFERENCES maine_transactions(detail_url)
);

CREATE INDEX idx_history_url  ON maine_listing_history(detail_url);
CREATE INDEX idx_history_date ON maine_listing_history(snapshot_date);
```

### Watched fields for history writes

A history row is appended only when any of these changed vs. the most recent history row for that `detail_url`:

- `status`
- `list_price`

Plus one **baseline row** is inserted the first time a listing is ever seen by the pipeline, capturing its initial status and list price.

`days_on_market` is deliberately excluded from history — it ticks up by 1 every day a listing is active, which would create a history row daily for every active listing (~12K rows/month of pure noise). DOM at any point in time is trivially computable as `snapshot_date − list_date`. The current-state row in `maine_transactions` keeps the latest DOM for convenience.

Address/beds/sqft/year_built don't meaningfully change for a given listing — tracking them would just add noise. The current-state row captures any mutation if it occurs.

### Migration strategy

- Single migration runs idempotently on `init_db()`. Uses a `PRAGMA table_info` check to skip columns that already exist (SQLite has no `ADD COLUMN IF NOT EXISTS`).
- Backfill query sets `status='Closed'` wherever `close_date IS NOT NULL` and `status IS NULL`.
- No data loss risk — all existing queries (agent leaderboard, KPIs) continue to work because `status='Closed'` filters are added where the intent is closed-only.

### Query pattern updates

- Existing leaderboard / KPI queries get `WHERE status = 'Closed'` added. Semantically unchanged (all existing rows are backfilled to `'Closed'`).
- Downstream tools query `WHERE status IN ('Active', 'Pending')` against the same table.

---

## Section 3 — Cron, Cost, Notifications

### Cron schedule (GitHub Actions)

```yaml
# .github/workflows/maine_listings.yml
on:
  schedule:
    - cron: '30 11 * * 1'   # Monday 6:30am ET — weekly closed pipeline
    - cron: '30 11 * * *'   # Every day 6:30am ET — daily active pipeline
  workflow_dispatch:
    inputs:
      mode:
        type: choice
        options: [weekly-closed, daily-active, backfill-active, report-only]
```

A single workflow file handles both, routing on `mode`. On Monday both crons land at the same minute — the workflow runs twice (one closed, one active). They share the DB cleanly because WAL mode serializes writes and each run targets a disjoint `status` partition.

### Expected Firecrawl credits

| Run                        | Discovery | Enrichment | Total/run | Frequency | Monthly  |
|----------------------------|-----------|------------|-----------|-----------|----------|
| Weekly closed              | ~10       | ~50        | ~60       | 4/mo      | 240      |
| Daily active (steady)      | ~30       | ~20        | ~50       | 30/mo     | **1,500**|
| One-time active backfill   | ~100      | ~400       | ~500      | once      | 500      |

**Total steady-state: ~1,740/mo.** Fits comfortably in Firecrawl Standard (100K/mo); also fits Hobby (3K/mo) with ~40% headroom if downgraded.

### Notifications

Reuse `src/maine_notifier.py` (Pushover + Resend). New trigger cases:

- **Daily-active failure** — circuit breaker trips OR unexpected exception → `notify_failure()`.
- **Daily-active success summary** — `notify_success()` with counts: new actives, status changes, withdrawn this run. Silence-is-broken: if the email doesn't arrive, something's wrong.
- **Anomaly alert** — zero new listings AND zero status changes across all 10 towns → fire `notify_failure()` with "suspicious: no deltas detected". Strong signal of a site change or auth wall.

### DB backup

Existing `_backup_db()` runs before every mutating run. Daily cadence rotates through the "keep last 3" limit.

### Cost controls

- `--recent-only` flag on discovery stops paging after first all-known page (already implemented for closed).
- Enrichment `max_attempts=2` caps retries on flaky pages (already implemented).
- New `--max-credits N` flag hard-stops the run if Firecrawl calls exceed the budget. Belt-and-suspenders.

---

## Section 4 — Downstream Interface & Testing

### Read helpers for downstream tools

The downstream tools are in a separate repo. The DB file is the interface. Four read-only helpers get added to this repo so the interface is versioned and testable:

```python
# src/maine_active.py  (NEW, ~80 lines)

def query_active_listings(
    conn,
    *,
    towns: list[str] | None = None,
    min_days_on_market: int | None = None,
    include_pending: bool = False,
) -> list[dict]:
    """All currently-active listings with agent contact + property details."""

def query_listing_history(conn, detail_url: str) -> list[dict]:
    """Full price/status timeline for one listing, oldest first."""

def query_new_since(conn, since_iso: str) -> list[dict]:
    """Listings whose *first* history row is >= since_iso.
    Powers the daily mailer tool: "what appeared since yesterday?"
    """

def query_stale_listings(conn, *, min_dom: int = 60) -> list[dict]:
    """Active listings on market >= min_dom days.
    Powers the agent-outreach tool to target motivated sellers.
    """
```

### Contract guarantees for downstream tools

- `detail_url` is the stable primary key across runs — safe for the downstream tool to cache.
- `last_seen_at` is updated every run a listing is observed; downstream tools can cheaply check "still active this morning?"
- `maine_listing_history` is append-only — never rewritten. Safe to index/cache.
- Downstream tools open the DB read-only (`file:.../maine_listings.db?mode=ro`) so there's no write-conflict risk.

The downstream repo can either vendor `src/maine_active.py` via git submodule or copy the file. Out of scope here — but the interface is frozen so the downstream repo can develop against a stable surface.

### Testing plan

New tests under `tests/`:

- `tests/test_maine_schema_migration.py` — idempotent migration; backfill of `status='Closed'` on existing closed rows; no column duplication on re-run.
- `tests/test_maine_active_discovery.py` — URL builder emits `mls_status=Active`; search parser handles active cards (no sale_price); pagination unchanged.
- `tests/test_maine_history.py` — change detection: no history row on no-op, one row on status change, one on price change, two on both.
- `tests/test_maine_active_sweeper.py` — withdrawn-sweep marks listings not seen in 7 days; respects the 7-day boundary.
- `tests/test_maine_active.py` — the four `query_*` helpers, with seeded fixtures.
- `tests/test_maine_notifier.py` — anomaly-alert path (zero-delta run triggers failure notification).

All existing 232 tests continue to pass. The closed leaderboard queries get `WHERE status='Closed'` added; seed data in existing tests gets `status='Closed'` set on insert.

### Manual verification after landing

- `python -m src.maine_main --discover --status Active --towns kittery --max-pages 2` — spot-check ~30 rows.
- `python -m src.maine_main --enrich --batch-size 20` — confirm NUXT extraction works on actives.
- `sqlite3 data/maine_listings.db "SELECT status, COUNT(*) FROM maine_transactions GROUP BY status"` — verify distribution.

---

## Open Questions — None

All four sections approved in brainstorming (2026-04-17).
