# gw-re-agent-scraper

Identifies the top real estate agents and brokerages across 10 southern coastal Maine towns using publicly visible MLS data.

## What it does

1. **Collects closed MLS transactions** from MaineListings.com (the Maine MLS public consumer portal operated by the Maine Association of REALTORS). Both the listing agent AND the buyer's agent on every transaction, going back to 2011.
2. **Builds KPI rollups per agent and per brokerage** across four rolling windows: last-12 months, prior-12 months, last-3 years, all-time.
3. **Computes year-over-year leaderboard movement** — who moved up, who fell, who's NEW.
4. **Renders an interactive dashboard** with an Agent / Brokerage toggle, a Town filter, an in-table search, a Biggest Movers banner, and a sortable 12-column leaderboard.
5. **Exports a static HTML dashboard + markdown leaderboard** (`data/maine_dashboard.html`, `data/maine_leaderboard.md`) for sharing.
6. **Retains Redfin and Zillow pipelines** as archived sources for cross-reference, but Maine MLS is the primary source of truth.

## Live dashboard

**https://pmtinkerer.github.io/gw-re-agent-scraper/** — auto-deploys on push to `main`, and weekly via scheduled GitHub Actions.

## Current status (2026-04-17)

- **16,024 closed MLS transactions** enriched across 10 towns (99.97% enrichment success)
- **2,253 unique listing agents** + **2,634 unique buyer agents** = **3,165 distinct people in the search index**
- **444 unique brokerages** (kept at branch level — "Coldwell Banker Yorke Realty" ≠ "Coldwell Banker Realty")
- **15-year history** (2011-02 → 2026-04)
- **232 unit tests passing**
- **Weekly GitHub Actions workflow** (Monday 6:30am ET) keeps the data fresh at ~50-100 Firecrawl credits/week
- **Pushover + Resend alerting** for run failures
- Redfin pipeline archived (strictly a subset of MLS data — 4x/day cron disabled)
- Zillow pipeline archived (agent profiles kept for photo/bio/reviews reference — manual dispatch only)

## Setup

```bash
pip install -r requirements.txt
# no browser install required — Firecrawl handles rendering server-side

# Secrets (local dev)
export FIRECRAWL_API_KEY=fc-...    # required for scraping
# optional alerting (reads from ~/.env shared secrets file):
# PUSHOVER_API_TOKEN, PUSHOVER_USER_KEY, RESEND_API_KEY
```

## Local usage

```bash
# Full Phase 1 discovery across all 10 towns (one-time, ~600 credits)
python -m src.maine_main --discover --max-pages 90 --workers 3

# Full Phase 2 enrichment (one-time backfill, ~16K credits on Firecrawl Standard)
python -m src.maine_main --enrich --batch-size 16500 --workers 25

# Weekly incremental (discover new closings + enrich them, ~50-100 credits)
python -m src.maine_main --discover --recent-only --enrich --batch-size 200 --workers 10

# Regenerate markdown + HTML dashboard from existing DB
python -m src.maine_main --report

# Regenerate the tabbed index.html that wraps all three sources
python -m src.maine_main --update-index

# Run tests
python -m pytest tests/
```

## Dashboard tour

The site has four tabs:

1. **Maine MLS** (default) — standalone `maine_dashboard.html` with top-50 agents + brokerages, per-town breakdowns, and a Biggest Movers banner.
2. **Leaderboard** — the interactive workhorse. 12 columns per row (12mo Δ, 12mo Vol, 12mo Sides, 3yr Vol, All-Time Vol, All-Time Sides, L/B, Avg 3yr, Most Recent, Primary Towns). Agent/Brokerage toggle, town filter (caps to top 50), period selector, in-table search. Biggest Movers banner above the table.
3. **Zillow** (archive) — previous agent directory data; useful for bios/photos/reviews.
4. **Redfin** (archive) — previous transaction data; strictly a subset of MLS.

Global search bar (top-right) returns matches across all three sources with colored badges; clicking an agent opens a detail modal with every period split.

## Data sources

- **MaineListings.com (MREIS)** — Maine MLS public consumer portal, operated by the Maine Association of REALTORS. Data flows from here to Zillow, Realtor.com, Homes.com (not the other way around). Scraped via Firecrawl.
- Uses only publicly visible data. No MLS membership required. Runs weekly to keep strain on the source minimal.

## Towns covered

Kittery, York, Ogunquit, Wells, Kennebunk, Kennebunkport, Biddeford, Saco, Old Orchard Beach, Scarborough — all in Maine.

## Architecture highlights

- **Concurrent Firecrawl enrichment** — ThreadPoolExecutor with circuit breaker (aborts at 5 consecutive or 20 total failures). 25 workers completes ~16K listings in ~3 hours.
- **Thread-safe SQLite writes** — WAL mode + shared `db_lock` + retry on lock contention.
- **Period-based KPI queries** — a UNION of listing-side and buyer-side rows aggregated in one pass, with `CASE WHEN close_date >= ? THEN ... END` per period.
- **Pure-Python rank mover computation** — `compute_rank_movers()` in `src/maine_kpis.py` diffs current-12mo rank vs prior-12mo rank. NEW entities (no prior activity) are placed in risers with delta=None.
- **Escape-decoding at parse time** — the MaineListings NUXT blob embeds `\u002F` style escapes (Vue double-encodes); decoded in Python after regex extraction.
- **Automatic DB backup** before every mutating run (keeps last 3 timestamped copies).
- **Exclusion list** for known placeholders ("NON-MREIS AGENT", brokerage-as-agent names) applied at query time so they never rank.

## Supply chain security

All dependencies pinned to exact versions. GitHub Actions pinned to full SHAs. Dependabot + pip-audit enabled. See [SECURITY.md](SECURITY.md).

## Repository layout

```
src/                           # Python pipeline
  maine_main.py                # CLI orchestrator (discover / enrich / report / update-index)
  maine_firecrawl.py           # Concurrent Firecrawl scraping (search + detail pages)
  maine_parser.py              # Search card regex + NUXT blob extraction
  maine_database.py            # SQLite schema + upsert/enrich helpers
  maine_state.py               # Per-town discovery state tracking
  maine_kpis.py                # Period queries + rank movers (leaderboard data layer)
  maine_report.py              # Markdown leaderboard + unified agent search index
  maine_dashboard.py           # Standalone HTML dashboard
  maine_notifier.py            # Pushover + Resend alerts
  index_page.py                # Tabbed wrapper + interactive Leaderboard tab
  # Archived (kept for reference):
  scraper.py / main.py / report.py / dashboard.py   (Redfin)
  zillow_*.py                                       (Zillow)
tests/                         # 232 pytest tests
data/                          # SQLite DBs + generated dashboards (committed)
.github/workflows/
  maine_listings.yml           # Weekly cron + manual dispatch (primary)
  zillow_leaderboard.yml       # Manual dispatch only (archived)
  scrape_agents.yml            # Manual dispatch only (archived)
docs/superpowers/
  specs/                       # Design specs
  plans/                       # Implementation plans
```
