# gw-re-agent-scraper

Identifies the top real estate listing agents in southern coastal Maine using publicly visible sold property data from Redfin.

## What it does

1. **Collects sold property data** from Redfin's CSV endpoint (address, price, MLS#, sold date, beds, baths, sqft)
2. **Enriches with agent data** by visiting individual Redfin property pages via Playwright to extract listing agent name and brokerage
3. **Normalizes agent names** and fuzzy-merges near-duplicates
4. **Generates a ranked leaderboard** at `data/agent_leaderboard.md`

## Current status

- 2,371 transactions collected across 10 towns (March 2023–March 2026)
- Playwright agent enrichment pipeline running via GitHub Actions with residential proxy
- ~2,177 URLs pending enrichment (~27 automated runs to complete)
- 99 unit tests passing
- Live on GitHub with automated 4x/day enrichment

## Setup

```bash
pip install -r requirements.txt
playwright install chromium
```

## Local usage

```bash
# Discover Redfin region IDs (already cached in scrape_state.json)
python -m src.main --discover-regions

# Run Redfin CSV collection (3 chunks at a time)
python -m src.main --mode initial --max-chunks 3 --source redfin

# Enrich agent data from Redfin property pages (80 URLs per batch)
python -m src.main --enrich --batch-size 80

# Regenerate leaderboard from existing data
python -m src.main --report-only

# Run fuzzy agent name merge
python -m src.main --merge-agents

# Reset all progress
python -m src.main --reset-state

# Run tests
python -m pytest tests/
```

## GitHub Actions

The scraper runs automatically on GitHub Actions:
- **During initial collection:** 4x/day (every 6 hours)
- **After collection is complete:** Auto-detects and switches to 1x/day at midnight UTC
- **Agent enrichment** runs automatically after CSV scraping, processing 80 URLs per run

Manual trigger available via Actions tab → "Scrape RE Agent Data" → "Run workflow" (supports `enrich_batch_size` input).

## Reading the leaderboard

The output is `data/agent_leaderboard.md` with four sections:
1. **Top 30 Listing Agents** — ranked by total listing volume
2. **Top 15 Brokerages** — ranked by total volume across all agents
3. **Top 5 Agents per Town** — local leaders in each of the 10 towns
4. **Data Summary** — transaction counts, date ranges, source breakdown

## Data sources

- **Redfin CSV endpoint** — property data (address, price, beds, baths, MLS#, sold date)
- **Redfin property pages via Playwright** — listing agent name and brokerage (extracted from individual sold listing pages)

Uses only publicly visible data. No MLS access or real estate license required.

## Towns covered

Kittery, York, Ogunquit, Wells, Kennebunk, Kennebunkport, Biddeford, Saco, Old Orchard Beach, Scarborough — all in Maine.

## Architecture notes

- **Redfin CSV no longer includes agent columns** (as of 2026) — agent data must be extracted from individual property pages via Playwright
- **Two Redfin DOM structures** for agent data: `.agent-card-wrapper` (Redfin-agent listings) and `.listing-agent-item` (non-Redfin agents) — both handled automatically
- **Fresh browser context per page** with randomized user-agent/viewport — Redfin CloudFront blocks repeated requests from same session
- **10-20 second delay** between enrichment page visits — lower causes CDN 403 blocks
- Three towns (York, Ogunquit, Wells) are classified as "minorcivildivision" on Redfin, not "city" — CSV API queries use York County with city filtering
- Agent name normalization uses `rapidfuzz` (>90% similarity + same office = merge)
- Chunk-based resumable processing for GitHub Actions free tier (45-min timeout, 2000 min/month)
- Enrichment tracked per-URL via `enrichment_status` column (NULL → success/no_agent/error) with up to 3 retry attempts
