# PROJECT_PLAN.md — gw-agent-scraper

## Objective
Identify the top real estate listing agents across 10 southern Maine towns using 3 years of publicly visible sold data.

## Phase Tracker
- [x] Phase 1: Project scaffold and database module
- [x] Phase 2: State manager and Redfin CSV integration
- [x] Phase 3: Redfin region ID discovery and data collection (2,371 transactions)
- [x] Phase 4: Report generator and CLI orchestrator
- [x] Phase 5: GitHub Actions workflow and project docs
- [ ] **Phase 6: Playwright agent enrichment** — visit each Redfin URL to extract listing agent + brokerage
- [ ] Phase 7: Agent name normalization tuning with real data
- [ ] Phase 8: Push to GitHub and first automated Actions run
- [ ] Phase 9: Leaderboard review and incremental mode
- [ ] Phase 10: Zillow fallback (only if needed)

## Current Priority: Phase 6 — Playwright Agent Enrichment

The Redfin CSV gives us property data but NOT agent names. Each Redfin property page DOES show the listing agent and brokerage when you visit the URL. We have 2,371 URLs in the database.

**What to build:**
- `enrich_agents_from_redfin(conn)` function in `src/scraper.py`
- Uses Playwright with stealth mode to visit each Redfin property URL
- Extracts listing agent name and brokerage from the page HTML/DOM
- Updates the existing transaction record in SQLite
- New CLI flag: `--enrich-agents`
- Resumable: only visits URLs where `listing_agent IS NULL`
- Rate limited: 5-10s delay between requests
- Copy stealth browser patterns from `~/competitor-scraper/utils/stealth.py`

**Estimated runtime:** 3-7 hours (2,371 pages), split across GitHub Actions runs

**First test:** Visit one old listing (March 2023) to confirm agent data is still visible

## Decision Log
| Date | Decision | Rationale |
|------|----------|-----------|
| 2026-03-21 | SQLite over Postgres | No external DB dependency, single file, committed to repo |
| 2026-03-21 | Redfin CSV for property data | Reliable structured data: address, price, MLS#, sold date, beds, baths, sqft |
| 2026-03-21 | Playwright for agent enrichment | Redfin CSV removed agent columns; individual property pages still show agent/brokerage data but require a real browser to render |
| 2026-03-21 | Skip RapidAPI / Realtor.com | Unofficial third-party API, poorly documented, not credible — Redfin pages are more reliable |
| 2026-03-21 | York County query for minorcivildivision towns | York, Ogunquit, Wells use region_type=40 which the CSV API doesn't support; querying county (type=5, id=1309) and filtering by city works |
| 2026-03-21 | rapidfuzz for name matching | C-accelerated, permissive license, >90% threshold + same office |
| 2026-03-21 | Chunk-based resumable processing | Fits GitHub Actions 45-min timeout and 2000 min/month free tier |

## Changelog
- 2026-03-21: Initial build complete. All Python modules, GitHub Actions workflow, unit tests, and documentation created.
- 2026-03-21: Collected 2,371 Redfin transactions. Discovered CSV lacks agent columns. Pivoted strategy to Playwright-based agent enrichment from individual property pages. Dropped RapidAPI approach.
