# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Does
Scrapes publicly visible sold property data from Redfin for 10 southern coastal Maine towns, then enriches each transaction with listing agent and brokerage data by visiting individual Redfin property pages via Playwright. The primary deliverables are `data/dashboard.html` (HTML leaderboard with trend badges, hosted on GitHub Pages) and `data/agent_leaderboard.md` (markdown version). Runs on GitHub Actions free tier with resumable chunk-based processing.

## Current State (as of 2026-04-16)
- **Redfin pipeline**: 2,398 SFH/Condo transactions, ~85% enriched with agent data (listing agent only). County-query date chunking added for York/Wells/Ogunquit.
- **Zillow pipeline**: 740 directory profiles + 683/740 enriched with career stats, avg price, and 5 most recent sold transactions. Buyer/seller split visible.
- **Maine Listings pipeline (NEW — primary source going forward)**: 10,587 closed transactions discovered from mainelistings.com (MREIS MLS public portal). Captures BOTH listing agent AND buyer's agent on every transaction — data no other source provides. Phase 2 enrichment (detail page scraping) pending Firecrawl Standard plan upgrade.
- **Tabbed dashboard** at `data/index.html` with Redfin, Zillow, and "All Agents" tabs — sortable, filterable, with Local Sales / Career Total / Local % / Est. Volume columns
- **Agent search** across 834 Redfin + 685 Zillow agents with detail cards
- **GitHub Pages live** at `https://pmtinkerer.github.io/gw-re-agent-scraper/`
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

### Phase 4: Zillow Profile Enrichment (BUILT — 683/740 ENRICHED)
- Scrape individual agent profile pages via Firecrawl for career stats + recent sold data
- Extracts from `__NEXT_DATA__` + Apollo cache on profile pages: total career sales, 12-month count, 3-year avg price, price range, for-sale count, 5 most recent sold transactions (date, price, address, buyer/seller side, beds/baths)
- Cost: ~740 credits for full enrichment (1 per agent)
- Sold pagination: Zillow's sold table uses JS click pagination; React re-renders too fast for Firecrawl actions to capture. Only page 1 (5 most recent) reliably captured. Rest marked as "N older transactions not shown"
- CLI: `python -m src.zillow_main --enrich-profiles --enrich-batch 50`
- Data stored in `zillow_sold_transactions` table + enrichment columns on `zillow_profiles`

### Phase 5: Maine Listings (MREIS MLS) (BUILT — PHASE 1 COMPLETE, PHASE 2 PENDING UPGRADE)
- **This is the primary transaction source going forward.** Redfin misses buyer agents; Zillow paginates to only 5 sold rows. MaineListings.com (the Maine MLS public portal) provides BOTH listing and buyer agents on every closed transaction.
- Architecture: Two-phase scraping
  - **Phase 1 (discovery — DONE)**: Scrape `/listings?city={town}&mls_status=Closed&page={N}` search pages. Extract listing URL, price, address, beds/baths/sqft, listing office. 10,587 closed listings discovered across all 10 towns (~600 Firecrawl credits).
  - **Phase 2 (enrichment — PENDING)**: Visit each detail page, extract data from embedded NUXT JavaScript blob. Fields: MLS#, listing_agent + MLS ID + email, buyer_agent + MLS ID + email, listing_office, buyer_office, close_date, sale_price, list_price, property_type, days_on_market.
- Full enrichment cost: ~10,587 credits (needs Standard plan at $99/mo for one-time backfill)
- Weekly incremental (`--recent-only`): ~50-100 credits/week (fits Hobby plan)
- Key technical detail: Detail page has TWO `list_agent` objects in NUXT — first is `co_list_agent` (usually null), second has real data. Parser finds the one where `list_agent_email` is a quoted string, not minified `a`.
- Separate DB: `data/maine_listings.db`, state: `data/maine_scrape_state.json`
- Modules: `src/maine_database.py`, `src/maine_state.py`, `src/maine_parser.py`, `src/maine_firecrawl.py`, `src/maine_main.py`
- CLI: `python -m src.maine_main --discover --max-pages 90` then `python -m src.maine_main --enrich --batch-size 50`
- Weekly: `python -m src.maine_main --discover --recent-only --enrich --batch-size 50`

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
python -m src.zillow_main --enrich-profiles --enrich-batch 50                           # Profile enrichment
python -m src.zillow_main --directory-report                                            # Regenerate from existing data

# --- Maine Listings Pipeline (Firecrawl — PRIMARY GOING FORWARD) ---
python -m src.maine_main --discover --max-pages 90 --workers 3                          # Full discovery (all towns, 3 concurrent)
python -m src.maine_main --enrich --batch-size 200 --workers 25                         # Full enrichment (25 concurrent Firecrawl workers)
python -m src.maine_main --discover --recent-only --enrich --batch-size 200 --workers 10  # Weekly incremental
python -m src.maine_main --report                                                       # Generate md + HTML dashboard
python -m src.maine_main --update-index                                                 # Regenerate tabbed index.html with all 3 sources

# --- Tests ---
python -m pytest tests/ -v                                 # All tests
python -m pytest tests/test_zillow_firecrawl.py -v         # Zillow Firecrawl tests only

# --- Database Checks ---
sqlite3 data/agent_data.db "SELECT city, COUNT(*) FROM transactions GROUP BY city ORDER BY COUNT(*) DESC;"
sqlite3 data/zillow_agent_data.db "SELECT profile_type, COUNT(*) FROM zillow_profiles GROUP BY profile_type;"
sqlite3 data/maine_listings.db "SELECT city, COUNT(*) FROM maine_transactions GROUP BY city ORDER BY COUNT(*) DESC;"
sqlite3 data/maine_listings.db "SELECT COUNT(*) total, SUM(CASE WHEN enrichment_status='success' THEN 1 ELSE 0 END) enriched FROM maine_transactions;"
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
│   ├── main.py                       # Redfin CLI orchestrator
│   ├── scraper.py                    # Redfin CSV + Playwright enrichment
│   ├── database.py                   # SQLite schema (Redfin + Zillow)
│   ├── report.py                     # Redfin leaderboard markdown
│   ├── dashboard.py                  # Redfin HTML dashboard
│   ├── state.py                      # Redfin chunk-based state machine
│   ├── index_page.py                 # Tabbed index.html + agent search index
│   ├── zillow_main.py                # Zillow CLI orchestrator
│   ├── zillow_firecrawl.py           # Firecrawl Zillow directory scraping
│   ├── zillow_profile_scraper.py     # Zillow profile enrichment (career stats + sold rows)
│   ├── zillow_directory_report.py    # Zillow two-leaderboard report + dashboard
│   ├── zillow.py                     # Playwright Zillow scraper (blocked, fallback)
│   ├── zillow_state.py               # Zillow discovery state machine
│   ├── maine_main.py                 # Maine Listings CLI orchestrator
│   ├── maine_firecrawl.py            # Maine Listings search + detail page scraping (concurrent via ThreadPoolExecutor)
│   ├── maine_parser.py               # Search card regex + detail NUXT JS extraction
│   ├── maine_database.py             # Maine Listings SQLite schema
│   ├── maine_report.py               # Maine markdown leaderboard + agent search index
│   ├── maine_dashboard.py            # Maine HTML dashboard (combined/listing/buyer/brokerage tables)
│   └── maine_state.py                # Maine Listings state machine
├── tests/                            # 128+ unit tests
├── data/
│   ├── agent_data.db                 # Redfin transactions (2,398)
│   ├── zillow_agent_data.db          # Zillow profiles (740) + sold transactions (3,290)
│   ├── maine_listings.db             # Maine Listings transactions (10,587 discovered)
│   ├── index.html                    # Tabbed dashboard wrapper (Redfin | Zillow | Maine MLS | All Agents)
│   ├── dashboard.html                # Redfin HTML dashboard
│   ├── zillow_directory_dashboard.html  # Zillow HTML dashboard
│   ├── maine_dashboard.html          # Maine MLS HTML dashboard
│   └── *.md                          # Markdown leaderboards
├── .github/workflows/
│   ├── scrape_agents.yml             # Redfin enrichment (4x/day)
│   ├── zillow_leaderboard.yml        # Zillow Firecrawl (manual dispatch)
│   └── maine_listings.yml            # Maine MLS Firecrawl (weekly cron + manual)
├── CLAUDE.md, AGENTS.md, README.md
├── requirements.txt
└── .gitignore
```

## Supply Chain Security

This project follows the global supply chain security standard defined in `~/CLAUDE.md`. All dependencies must be pinned to exact versions, GitHub Actions must be SHA-pinned, and pip-audit must run in CI.
