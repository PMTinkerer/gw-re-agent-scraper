# gw-agent-scraper — Project Context

## What This Project Does
Scrapes publicly visible sold property data from Redfin (CSV endpoint) and Realtor.com (RapidAPI) for 10 southern coastal Maine towns to identify the dominant real estate listing agents. The primary deliverable is `data/agent_leaderboard.md` — a ranked leaderboard of agents by listing volume. Runs on GitHub Actions free tier with resumable chunk-based processing.

## Service Territory (10 Towns)
Kittery, York, Ogunquit, Wells, Kennebunk, Kennebunkport, Biddeford, Saco, Old Orchard Beach, Scarborough — all in Maine.

## Architecture Decisions
- **SQLite** for storage (`data/agent_data.db`) — no external DB dependency, committed to repo
- **MLS number** as dedup key across sources (INSERT OR REPLACE on UNIQUE constraint)
- **Chunk-based resumable scraping** for GitHub Actions free tier (45-min timeout, ~40 chunks total)
- **Redfin CSV endpoint** as primary source — pulls all 3 years at once per town via `sold_within_days=1095`
- **Realtor.com RapidAPI** as secondary source — 100 free req/month, per-town-per-year chunks
- **rapidfuzz** for agent name deduplication (>90% similarity + same office)
- **No pandas** — CSV via stdlib `csv.DictReader`, queries via raw SQL

## Data Flow
Redfin CSV / Realtor.com API → normalize agent names → SQLite → fuzzy merge agents → rebuild rankings → generate leaderboard markdown

## Verification Commands
```bash
# Regenerate leaderboard from existing data
python -m src.main --report-only

# Test a single Redfin chunk locally
python -m src.main --mode initial --max-chunks 1 --towns "York, ME"

# Discover Redfin region IDs
python -m src.main --discover-regions

# Run fuzzy agent merge
python -m src.main --merge-agents

# Reset all progress
python -m src.main --reset-state

# Inspect scrape progress
python -m json.tool data/scrape_state.json

# Run tests
python -m pytest tests/
```

## Environment Variables
- `RAPIDAPI_KEY` (optional) — for Realtor.com API. Scraper works without it (skips Realtor.com chunks).

## Known Constraints
- Redfin CSV returns max 350 rows per request — pagination via `page_number`
- Redfin `sold_within_days` parameter is undocumented — we use 1095 (3 years)
- Realtor.com RapidAPI free tier = 100 requests/month
- GitHub Actions job max 45-min timeout, 2000 min/month free tier
- Agent name normalization fuzzy threshold (90%) may need tuning after real data

## City Normalization
Cape Neddick → York, Moody → Wells, Ocean Park → Old Orchard Beach, Cape Porpoise → Kennebunkport, Biddeford Pool → Biddeford, etc. See `src/scraper.py` CITY_NORMALIZATION dict.
