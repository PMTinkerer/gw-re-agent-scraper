# PROJECT_PLAN.md — gw-agent-scraper

## Objective
Identify the top real estate listing agents across 10 southern Maine towns using 3 years of publicly visible sold data.

## Phase Tracker
- [x] Phase 1: Project scaffold and database module
- [x] Phase 2: State manager and Redfin CSV integration
- [x] Phase 3: Redfin region ID discovery and data collection (2,371 transactions)
- [x] Phase 4: Report generator and CLI orchestrator
- [x] Phase 5: GitHub Actions workflow and project docs
- [x] **Phase 6: Playwright agent enrichment** — built, tested, 10 URLs enriched successfully
- [ ] Phase 7: Run full enrichment (~24 batches of 100 URLs)
- [ ] Phase 8: Agent name normalization tuning with real data
- [ ] Phase 9: Push to GitHub and first automated Actions run
- [ ] Phase 10: Leaderboard review and incremental mode

## Current Priority: Phase 7 — Run Full Enrichment

Phase 6 (Playwright enrichment pipeline) is complete and validated. 10/10 test URLs enriched with correct agent/brokerage data. 2,361 URLs remain.

**What was built:**
- `enrich_agents_from_redfin(conn, batch_size, headless)` in `src/scraper.py`
- `_extract_agent_data(page)` — handles two Redfin DOM structures (Redfin-agent vs non-Redfin agent listings)
- `_check_page_status(page)` — distinguishes captcha (stop batch) from CDN errors (retry)
- Fresh browser context per page with viewport/user-agent rotation
- `enrichment_status` + `enrichment_attempts` columns in DB for per-URL tracking
- CLI: `python -m src.main --enrich --batch-size 100`
- GitHub Actions step runs enrichment automatically after CSV scraping

**Estimated runtime:** ~24 runs × 100 URLs × ~20s/URL ≈ 33 min/run, fits 45-min Actions timeout

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
| 2026-03-21 | Fresh browser context per page | Redfin CloudFront blocks repeated requests from same browser session — rotating context avoids 403s |
| 2026-03-21 | 10-20s delay between enrichment visits | 5-10s caused CDN blocks; 10-20s is reliable |
| 2026-03-21 | DB-level enrichment tracking | Per-URL `enrichment_status` column instead of state.py chunks — simpler for individual URL tracking with retry support |
| 2026-03-21 | Dual DOM extraction strategy | Redfin uses `.agent-card-wrapper` for their agents, `.listing-agent-item` for external agents — both handled |

## Changelog
- 2026-03-21: Initial build complete. All Python modules, GitHub Actions workflow, unit tests, and documentation created.
- 2026-03-21: Collected 2,371 Redfin transactions. Discovered CSV lacks agent columns. Pivoted strategy to Playwright-based agent enrichment from individual property pages. Dropped RapidAPI approach.
- 2026-03-21: Built and validated Playwright agent enrichment pipeline. Handles two Redfin DOM structures, fresh browser context per page, CDN error detection and retry. 10/10 test URLs enriched correctly. 97 tests passing. GitHub Actions workflow updated with enrichment step.
