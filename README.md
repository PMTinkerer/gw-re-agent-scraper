# gw-agent-scraper

Identifies the top real estate listing agents in southern coastal Maine using publicly visible sold property data from Redfin and Realtor.com.

## What it does

Scrapes 3 years of sold property data across 10 towns (Kittery to Scarborough), extracts listing agent and brokerage info, normalizes names, and generates a ranked leaderboard at `data/agent_leaderboard.md`.

## Setup

```bash
pip install -r requirements.txt
playwright install chromium
```

## Local usage

```bash
# Discover Redfin region IDs (required first time)
python -m src.main --discover-regions

# Run initial collection (3 chunks at a time)
python -m src.main --mode initial --max-chunks 3

# Test with a single town
python -m src.main --mode initial --max-chunks 1 --towns "York, ME"

# Only Redfin source
python -m src.main --source redfin --max-chunks 5

# Regenerate leaderboard from existing data
python -m src.main --report-only

# Run fuzzy agent name merge
python -m src.main --merge-agents

# Reset all progress
python -m src.main --reset-state
```

## GitHub Actions

The scraper runs automatically on GitHub Actions:
- **During initial collection:** 4x/day (every 6 hours)
- **After collection is complete:** Auto-detects and switches to 1x/day at midnight UTC

Manual trigger available via Actions tab → "Scrape RE Agent Data" → "Run workflow".

### Required secrets
- `RAPIDAPI_KEY` (optional) — for Realtor.com API. Without it, only Redfin is scraped.

## Reading the leaderboard

The output is `data/agent_leaderboard.md` with four sections:
1. **Top 30 Listing Agents** — ranked by total listing volume
2. **Top 15 Brokerages** — ranked by total volume across all agents
3. **Top 5 Agents per Town** — local leaders in each of the 10 towns
4. **Data Summary** — transaction counts, date ranges, source breakdown

## Data sources

- **Redfin** — CSV download endpoint with sold property data including agent names
- **Realtor.com** — RapidAPI wrapper (unofficial) for supplementary agent data

Uses only publicly visible data. No MLS access or real estate license required.

## Towns covered

Kittery, York, Ogunquit, Wells, Kennebunk, Kennebunkport, Biddeford, Saco, Old Orchard Beach, Scarborough — all in Maine.
