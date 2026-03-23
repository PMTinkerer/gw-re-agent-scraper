# gw-re-agent-scraper

Identifies the top real estate listing agents in southern coastal Maine using publicly visible sold property data from Redfin.

## What it does

1. **Collects sold property data** from Redfin's CSV endpoint (address, price, MLS#, sold date, beds, baths, sqft)
2. **Enriches with agent data** by visiting individual Redfin property pages via Playwright to extract listing agent name and brokerage
3. **Normalizes agent names** and fuzzy-merges near-duplicates
4. **Generates a ranked leaderboard** — HTML dashboard with trend badges + markdown report

## Live dashboard

**https://pmtinkerer.github.io/gw-re-agent-scraper/** — auto-updates after every CI run.

## Current status

- 2,311 SFH/Condo transactions across 10 towns (March 2023–March 2026)
- 383 transactions enriched with agent data, ~1,928 pending (~24 automated runs to complete)
- Property type filter active — only Single Family Residential + Condo/Co-op
- HTML dashboard with all-time rankings, 365-day rolling rankings with trend badges, brokerage rankings, per-town breakdowns
- 115 unit tests passing
- Public repo on GitHub with automated 4x/day enrichment via residential proxy

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

# Regenerate leaderboard + HTML dashboard from existing data
python -m src.main --report-only

# Purge non-residential records (land, multi-family, mobile)
python -m src.main --purge-non-residential

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
- **Dashboard auto-deployed** to GitHub Pages after every run

Manual trigger available via Actions tab → "Scrape RE Agent Data" → "Run workflow" (supports `enrich_batch_size` input).

## Reading the leaderboard

The HTML dashboard (`data/dashboard.html`, hosted on GitHub Pages) and markdown report (`data/agent_leaderboard.md`) contain:
1. **Top 30 Listing Agents — All-Time** — ranked by total listing volume
2. **Top 30 Listing Agents — Last 365 Days** — rolling rankings with trend badges showing who's heating up vs cooling off
3. **Top 15 Brokerages** — ranked by total volume across all agents
4. **Top 5 Agents per Town** — local leaders in each of the 10 towns

Agents where the name matches the brokerage (e.g., "Anchor Real Estate") are excluded from agent rankings but included in brokerage rankings.

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
