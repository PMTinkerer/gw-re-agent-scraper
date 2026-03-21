# gw-agent-scraper — Project Context

## What This Project Does
Scrapes publicly visible sold property data from Redfin for 10 southern coastal Maine towns, then enriches each transaction with listing agent and brokerage data by visiting individual Redfin property pages via Playwright. The primary deliverable is `data/agent_leaderboard.md` — a ranked leaderboard of agents by listing volume. Runs on GitHub Actions free tier with resumable chunk-based processing.

## Current State (as of 2026-03-21)
- **2,371 transactions in SQLite** with full property data (address, price, MLS#, sold date, beds, baths, sqft)
- **0 transactions have agent data** — Playwright enrichment script is the next step
- **62 unit tests passing**
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

### Phase 2: Playwright Agent Enrichment (TO BUILD)
- Visit each Redfin property URL (stored in `source_url` column) with stealth Playwright
- Extract listing agent name and brokerage from the page
- Update existing transaction records — only visit URLs where `listing_agent IS NULL`
- Stealth patterns: copy from `~/competitor-scraper/utils/stealth.py` (user-agent rotation, viewport randomization, webdriver masking)
- Rate limit: 5-10 seconds between page visits

## Key Decisions
- **Redfin CSV** for property data (reliable, structured, no browser needed)
- **Playwright** for agent data (Redfin pages require real browser, show agent info when visited)
- **Skipped RapidAPI/Realtor.com** — unofficial, poorly documented, not credible
- **SQLite** for storage — single file committed to repo
- **MLS number** as dedup key
- **rapidfuzz** for agent name fuzzy matching (>90% + same office)

## Verification Commands
```bash
# Regenerate leaderboard from existing data
python -m src.main --report-only

# Test a single Redfin CSV chunk locally
python -m src.main --mode initial --max-chunks 1 --towns "York, ME"

# Discover/verify Redfin region IDs
python -m src.main --discover-regions

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
```

## Known Constraints
- Redfin CSV max 350 rows per request — pagination returns overlapping data, so unique records plateau
- Minorcivildivision towns (York, Ogunquit, Wells) have lower transaction counts via county query
- Redfin blocks non-browser requests (403) — Playwright required for property pages
- GitHub Actions job max 45-min timeout, 2000 min/month free tier
- Agent name normalization fuzzy threshold (90%) may need tuning after real data

## City Normalization
Cape Neddick → York, Moody → Wells, Ocean Park → Old Orchard Beach, Cape Porpoise → Kennebunkport, Biddeford Pool → Biddeford, etc. See `src/scraper.py` CITY_NORMALIZATION dict.

## File Structure
```
gw-agent-scraper/
├── src/
│   ├── main.py        # CLI orchestrator (argparse, 8 flags)
│   ├── scraper.py     # Redfin CSV + Playwright enrichment (TO ADD)
│   ├── database.py    # SQLite schema, upsert, normalization, rankings
│   ├── report.py      # Leaderboard markdown generator
│   └── state.py       # Chunk-based resumable state machine
├── tests/             # 62 unit tests (all passing)
├── data/
│   ├── agent_data.db       # 2,371 transactions (no agent data yet)
│   ├── agent_leaderboard.md # Generated report (empty until agents populated)
│   └── scrape_state.json   # Tracks scraping progress
├── .github/workflows/scrape_agents.yml
├── CLAUDE.md, AGENTS.md, PROJECT_PLAN.md, README.md
├── requirements.txt
└── .gitignore
```
