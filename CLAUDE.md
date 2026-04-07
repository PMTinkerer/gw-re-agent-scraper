# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Does
Scrapes publicly visible sold property data from Redfin for 10 southern coastal Maine towns, then enriches each transaction with listing agent and brokerage data by visiting individual Redfin property pages via Playwright. The primary deliverables are `data/dashboard.html` (HTML leaderboard with trend badges, hosted on GitHub Pages) and `data/agent_leaderboard.md` (markdown version). Runs on GitHub Actions free tier with resumable chunk-based processing.

## Current State (as of 2026-04-07)
- **Redfin pipeline**: 2,311 SFH/Condo transactions in SQLite, ~85% enriched with agent data via Playwright + IPRoyal proxy
- **Zillow pipeline (NEW)**: 740 unique agents (125 teams, 615 individuals) across all 10 towns, scraped via Firecrawl API (bypasses PerimeterX). Two-leaderboard dashboard: Brokerages + Agents
- **HTML dashboards** — Redfin at `data/dashboard.html`, Zillow at `data/zillow_directory_dashboard.html`, both auto-deployed to GitHub Pages
- **GitHub Pages live** at `https://pmtinkerer.github.io/gw-re-agent-scraper/`
- **128 unit tests passing**
- **Public repo on GitHub**

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
- **Resource blocking** — images, fonts, media blocked to save bandwidth (CSS loaded to avoid bot detection)
- Fresh proxy session ID per page for IP rotation (replaces `session-{id}` in proxy URL)
- Rate limit: 15-30 seconds between page visits (lower causes CloudFront 403/captcha blocks)
- Batch size: 40 URLs per run (~20 min, fits 45-min GitHub Actions timeout)
- React hydration wait: `wait_for_selector('.agent-card-wrapper, .listing-agent-item, .buyer-agent-item', timeout=8000)`
- CLI: `python -m src.main --enrich --batch-size 40`

### Phase 3: Zillow Directory via Firecrawl (BUILT — RUNNING)
- Firecrawl API scrapes Zillow agent directory pages, bypassing PerimeterX anti-bot (Playwright+proxy was blocked)
- Scrapes all 25 pages per town (~15 agents/page), capturing agents with ≥1 sale in each town
- Extracts: agent name, office/brokerage, team indicator, local sales count, 12-month sales, price range
- Classifies profiles as `team` (TEAM badge) or `individual` (no badge) using `**bold**` markers as name delimiter
- Two-leaderboard output: Brokerage leaderboard (agents grouped by office_name) + Agent leaderboard (individuals + teams)
- Office branches kept **separate** — "Coldwell Banker Yorke Realty" ≠ "Coldwell Banker Realty"
- No office name normalization applied (Zillow data is cleaner than Redfin; branches are competitors)
- Separate DB: `data/zillow_agent_data.db`, state: `data/zillow_scrape_state.json`
- ~250 Firecrawl credits per full run (~30 min), requires `FIRECRAWL_API_KEY` env var
- CLI: `python -m src.zillow_main --discover --use-firecrawl --max-pages 25 --directory-report`

### Future: Zillow Profile Enrichment (PLANNED — NOT YET BUILT)
- Scrape individual agent profile pages via Firecrawl to get:
  - **Buyer vs seller representation** from sold rows ("Represented: Buyer|Seller")
  - **Active listings count** from "For Sale (N)" section
  - **Total career sales** and **average price**
  - **Team member links** for team profiles
- ~740 credits for all agents (1 credit per profile page)
- Sold section is paginated (5 rows per page, click-to-paginate) — first page only via basic scrape, full pagination would require Firecrawl `instruct` mode
- Zillow caps directory pagination at 25 pages (~375 visible cards), even when "1,161 agents found" — agents beyond page 25 are inaccessible

## Key Decisions
- **Redfin CSV** for property data (reliable, structured, no browser needed)
- **Playwright** for agent data (Redfin pages require real browser, show agent info when visited)
- **Skipped RapidAPI/Realtor.com** — unofficial, poorly documented, not credible
- **SQLite** for storage — single file committed to repo
- **MLS number** as dedup key
- **rapidfuzz** for agent name fuzzy matching (>90% + same office)
- **Fresh browser context per page** — Redfin CloudFront blocks repeated requests from the same session; rotating context + user-agent + viewport avoids 403s
- **Residential proxy (IPRoyal)** — GitHub Actions datacenter IPs get immediately captcha'd by Redfin CloudFront; residential IPs work reliably. Set `PROXY_URL` secret in GitHub repo settings.
- **Resource blocking** — Playwright blocks images/fonts/media (not CSS — blocking CSS is a bot signal) to reduce proxy bandwidth
- **DB-level enrichment tracking** (`enrichment_status` column) instead of state.py chunks — simpler for per-URL tracking
- **15-30 second delay** between enrichment page visits — shorter intervals cause CDN blocks/captchas
- **SFH + Condo only** — `uipt=1,2` filter on Redfin CSV API excludes land, multi-family, mobile homes from scraping
- **Property type column** — `property_type` stored in DB; `--purge-non-residential` CLI flag deletes non-SFH/Condo records
- **Brokerage-as-agent exclusion** — two-layer filter: (1) `LOWER(listing_agent) != LOWER(listing_office)` auto-excludes exact matches, (2) `BROKERAGE_AS_AGENT` set in `database.py` explicitly excludes known brokerage-named agents (Anchor Real Estate, Anne Erwin Real Estate)
- **Office name normalization** — `OFFICE_NORMALIZATION` dict in `database.py` maps 15 variant office spellings to canonical names at upsert time (e.g., "KW Coastal..." → "Keller Williams Coastal...")
- **HTML dashboard** — `src/dashboard.py` generates `data/dashboard.html` with 6 sections: all-time agents, 365-day rolling agents, all-time brokerages, 365-day rolling brokerages (with trends + towns), and per-town breakdowns

## Verification Commands
```bash
# --- Redfin Pipeline ---
python -m src.main --report-only                          # Regenerate Redfin leaderboard
python -m src.main --enrich --batch-size 40               # Playwright agent enrichment
python -m src.main --merge-agents                         # Fuzzy agent merge

# --- Zillow Pipeline (Firecrawl) ---
python -m src.zillow_main --discover --use-firecrawl --max-pages 25 --directory-report  # Full scrape
python -m src.zillow_main --discover --use-firecrawl --max-pages 2 --towns "York, Kittery" --directory-report  # Quick test
python -m src.zillow_main --directory-report               # Regenerate from existing data

# --- Tests ---
python -m pytest tests/ -v                                 # All tests
python -m pytest tests/test_zillow_firecrawl.py -v         # Zillow Firecrawl tests only

# --- Database Checks ---
sqlite3 data/agent_data.db "SELECT city, COUNT(*) FROM transactions GROUP BY city ORDER BY COUNT(*) DESC;"
sqlite3 data/zillow_agent_data.db "SELECT profile_type, COUNT(*) FROM zillow_profiles GROUP BY profile_type;"
sqlite3 data/zillow_agent_data.db "SELECT town, COUNT(*) FROM zillow_profile_towns GROUP BY town ORDER BY COUNT(*) DESC;"
```

## Known Constraints
- Redfin CSV max 350 rows per request — pagination returns overlapping data, so unique records plateau
- Minorcivildivision towns (York, Ogunquit, Wells) have lower transaction counts via county query
- Redfin blocks non-browser requests (403) — Playwright required for property pages
- **Redfin CloudFront captchas datacenter IPs** — GitHub Actions IPs are flagged; residential proxy (`PROXY_URL`) required
- **Redfin CloudFront blocks rapid sequential requests** — must use fresh browser context per page + 15-30s delay
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
│   ├── main.py                  # Redfin CLI orchestrator
│   ├── scraper.py               # Redfin CSV + Playwright enrichment
│   ├── database.py              # SQLite schema, upsert, normalization (Redfin + Zillow)
│   ├── report.py                # Redfin leaderboard markdown generator
│   ├── dashboard.py             # Redfin HTML dashboard generator
│   ├── state.py                 # Redfin chunk-based state machine
│   ├── zillow_main.py           # Zillow CLI orchestrator
│   ├── zillow_firecrawl.py      # Firecrawl-based Zillow directory scraping
│   ├── zillow_directory_report.py # Zillow two-leaderboard report + dashboard
│   ├── zillow.py                # Playwright-based Zillow scraper (blocked, kept as fallback)
│   └── zillow_state.py          # Zillow discovery state machine
├── tests/                       # 128 unit tests
├── data/
│   ├── agent_data.db            # Redfin transactions (2,311 SFH/Condo)
│   ├── zillow_agent_data.db     # Zillow profiles (740 agents, 125 teams)
│   ├── dashboard.html           # Redfin HTML dashboard
│   ├── zillow_directory_dashboard.html  # Zillow HTML dashboard
│   └── zillow_agent_leaderboard.md      # Zillow markdown report
├── .github/workflows/
│   ├── scrape_agents.yml        # Redfin enrichment (4x/day)
│   └── zillow_leaderboard.yml   # Zillow Firecrawl discovery (manual dispatch)
├── CLAUDE.md, AGENTS.md, README.md
├── requirements.txt
└── .gitignore
```

## Supply Chain Security

This project follows the global supply chain security standard defined in `~/CLAUDE.md`. All dependencies must be pinned to exact versions, GitHub Actions must be SHA-pinned, and pip-audit must run in CI.
