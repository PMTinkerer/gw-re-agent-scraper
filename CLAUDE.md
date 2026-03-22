# gw-re-agent-scraper — Project Context

## What This Project Does
Scrapes publicly visible sold property data from Redfin for 10 southern coastal Maine towns, then enriches each transaction with listing agent and brokerage data by visiting individual Redfin property pages via Playwright. The primary deliverable is `data/agent_leaderboard.md` — a ranked leaderboard of agents by listing volume. Runs on GitHub Actions free tier with resumable chunk-based processing.

## Current State (as of 2026-03-21)
- **2,371 transactions in SQLite** with full property data (address, price, MLS#, sold date, beds, baths, sqft)
- **10 transactions enriched with agent data** — Playwright enrichment pipeline built and tested; 2,361 URLs pending
- **97 unit tests passing**
- **Not yet pushed to GitHub**

## Service Territory (10 Towns)
Kittery, York, Ogunquit, Wells, Kennebunk, Kennebunkport, Biddeford, Saco, Old Orchard Beach, Scarborough — all in Maine.

## Architecture — Two-Phase Data Collection

### Phase 1: Redfin CSV (COMPLETE)
- Redfin's CSV endpoint provides property data but NO agent names (removed from CSV as of 2026)
- City-type towns (7 of 10): direct query via `region_type=6`
- Minorcivildivision towns (York, Ogunquit, Wells): query York County (`region_type=5`, `region_id=1309`) and filter by city
- Hardcoded region IDs in `src/scraper.py` `_REDFIN_REGIONS` dict
- Date format: Redfin uses "June-30-2025" — parsed by `_parse_redfin_date()`

### Phase 2: Playwright Agent Enrichment (BUILT — RUNNING)
- Visits each Redfin property URL (stored in `source_url` column) with stealth Playwright
- Extracts listing agent name and brokerage from the page via DOM selectors
- Two DOM structures: `.agent-card-wrapper` (Redfin-agent listings) and `.listing-agent-item` (non-Redfin agents)
- Tracks enrichment status per URL: `enrichment_status` column (NULL/success/no_agent/error) with retry up to 3 attempts
- Fresh browser context per page to avoid Redfin CDN session fingerprinting
- **Residential proxy** via `PROXY_URL` env var (IPRoyal) — GitHub Actions datacenter IPs get captcha'd
- **Resource blocking** — images, fonts, stylesheets, media blocked to save ~70-80% proxy bandwidth
- Fresh proxy session ID per page for IP rotation (replaces `session-{id}` in proxy URL)
- Rate limit: 10-20 seconds between page visits (lower causes CloudFront 403 blocks)
- Batch size: 80 URLs per run (~27 min, fits 45-min GitHub Actions timeout)
- React hydration wait: `wait_for_selector('.agent-card-wrapper, .listing-agent-item, .buyer-agent-item', timeout=8000)`
- CLI: `python -m src.main --enrich --batch-size 80`

## Key Decisions
- **Redfin CSV** for property data (reliable, structured, no browser needed)
- **Playwright** for agent data (Redfin pages require real browser, show agent info when visited)
- **Skipped RapidAPI/Realtor.com** — unofficial, poorly documented, not credible
- **SQLite** for storage — single file committed to repo
- **MLS number** as dedup key
- **rapidfuzz** for agent name fuzzy matching (>90% + same office)
- **Fresh browser context per page** — Redfin CloudFront blocks repeated requests from the same session; rotating context + user-agent + viewport avoids 403s
- **Residential proxy (IPRoyal)** — GitHub Actions datacenter IPs get immediately captcha'd by Redfin CloudFront; residential IPs work reliably. Set `PROXY_URL` secret in GitHub repo settings.
- **Resource blocking** — Playwright blocks images/fonts/stylesheets/media to reduce proxy bandwidth ~70-80%
- **DB-level enrichment tracking** (`enrichment_status` column) instead of state.py chunks — simpler for per-URL tracking
- **10-20 second delay** between enrichment page visits — 5-10s caused CDN blocks

## Verification Commands
```bash
# Regenerate leaderboard from existing data
python -m src.main --report-only

# Test a single Redfin CSV chunk locally
python -m src.main --mode initial --max-chunks 1 --towns "York, ME"

# Discover/verify Redfin region IDs
python -m src.main --discover-regions

# Run Playwright agent enrichment (80 URLs per batch)
python -m src.main --enrich --batch-size 80

# Run fuzzy agent merge
python -m src.main --merge-agents

# Reset all scraping progress
python -m src.main --reset-state

# Inspect scrape progress
python -m json.tool data/scrape_state.json

# Run unit tests
python -m pytest tests/

# Check database stats
sqlite3 data/agent_data.db "SELECT city, COUNT(*) FROM transactions GROUP BY city ORDER BY COUNT(*) DESC;"
sqlite3 data/agent_data.db "SELECT COUNT(*), COUNT(listing_agent) FROM transactions;"

# Check enrichment progress
sqlite3 data/agent_data.db "SELECT enrichment_status, COUNT(*) FROM transactions GROUP BY enrichment_status;"
```

## Known Constraints
- Redfin CSV max 350 rows per request — pagination returns overlapping data, so unique records plateau
- Minorcivildivision towns (York, Ogunquit, Wells) have lower transaction counts via county query
- Redfin blocks non-browser requests (403) — Playwright required for property pages
- **Redfin CloudFront captchas datacenter IPs** — GitHub Actions IPs are flagged; residential proxy (`PROXY_URL`) required
- **Redfin CloudFront blocks rapid sequential requests** — must use fresh browser context per page + 10-20s delay
- **Two DOM structures for agent data** — Redfin-agent listings use `.agent-card-wrapper`, non-Redfin agents use `.listing-agent-item`
- **React hydration timing** — agent cards take 3-8s to render after `domcontentloaded`; must use `wait_for_selector` not fixed timeout
- GitHub Actions job max 45-min timeout, 2000 min/month free tier
- Agent name normalization fuzzy threshold (90%) may need tuning after real data
- Some Redfin property URLs return CloudFront 403 intermittently — marked as `error` and retried (up to 3 attempts)

## City Normalization
Cape Neddick → York, Moody → Wells, Ocean Park → Old Orchard Beach, Cape Porpoise → Kennebunkport, Biddeford Pool → Biddeford, etc. See `src/scraper.py` CITY_NORMALIZATION dict.

## File Structure
```
gw-re-agent-scraper/
├── src/
│   ├── main.py        # CLI orchestrator (argparse, 10 flags incl --enrich, --batch-size)
│   ├── scraper.py     # Redfin CSV + Playwright agent enrichment
│   ├── database.py    # SQLite schema, upsert, normalization, rankings, enrichment tracking
│   ├── report.py      # Leaderboard markdown generator
│   └── state.py       # Chunk-based resumable state machine
├── tests/             # 97 unit tests (all passing)
├── data/
│   ├── agent_data.db       # 2,371 transactions (10 enriched with agent data)
│   ├── agent_leaderboard.md # Generated report
│   └── scrape_state.json   # Tracks scraping progress
├── .github/workflows/scrape_agents.yml
├── CLAUDE.md, AGENTS.md, PROJECT_PLAN.md, README.md
├── requirements.txt
└── .gitignore
```
