# PROJECT_PLAN.md — gw-re-agent-scraper

## Objective
Identify the top real estate listing agents across 10 southern Maine towns using 3 years of publicly visible sold data.

## Phase Tracker
- [x] Phase 1: Project scaffold and database module
- [x] Phase 2: State manager and Redfin CSV integration
- [x] Phase 3: Redfin region ID discovery and data collection (2,371 transactions)
- [x] Phase 4: Report generator and CLI orchestrator
- [x] Phase 5: GitHub Actions workflow and project docs
- [x] **Phase 6: Playwright agent enrichment** — built, tested, 10 URLs enriched successfully
- [x] Phase 7: Push to GitHub, configure residential proxy, automated enrichment running
- [x] Phase 8: HTML dashboard with trend badges, deployed to GitHub Pages
- [x] Phase 9: Property type filter (SFH + Condo only), purge non-residential records
- [ ] Phase 10: Enrichment completion (~24 batches remaining at 80 URLs/batch)
- [ ] Phase 11: Agent name normalization tuning with real data
- [ ] Phase 12: Leaderboard review and incremental mode

## Current Priority: Phase 10 — Enrichment Completion

Enrichment running in production via GitHub Actions with IPRoyal residential proxy. 660 URLs enriched, ~1,647 remaining. Dashboard live at https://pmtinkerer.github.io/gw-re-agent-scraper/

**What was built:**
- `enrich_agents_from_redfin(conn, batch_size, headless)` in `src/scraper.py`
- `_extract_agent_data(page)` — handles two Redfin DOM structures (Redfin-agent vs non-Redfin agent listings)
- `_check_page_status(page)` — distinguishes captcha (stop batch) from CDN errors (retry)
- Fresh browser context per page with viewport/user-agent rotation + residential proxy (IPRoyal)
- Resource blocking (images/fonts/stylesheets/media) to reduce proxy bandwidth ~70-80%
- `enrichment_status` + `enrichment_attempts` columns in DB for per-URL tracking
- CLI: `python -m src.main --enrich --batch-size 80`
- GitHub Actions step runs enrichment automatically 4x/day

**Estimated runtime:** ~21 runs × 80 URLs × ~20s/URL ≈ 27 min/run, fits 45-min Actions timeout

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
| 2026-03-22 | IPRoyal residential proxy | GitHub Actions datacenter IPs get captcha'd by Redfin CloudFront; residential IPs work reliably |
| 2026-03-22 | SFH + Condo only (`uipt=1,2`) | Land, multi-family, mobile home sales are irrelevant to residential agent leaderboard |
| 2026-03-22 | Brokerage-as-agent exclusion | Some brokerages (e.g., Anchor Real Estate) show brokerage name as listing agent; excluded from agent rankings via `LOWER(listing_agent) != LOWER(listing_office)` |
| 2026-03-22 | HTML dashboard on GitHub Pages | Auto-deployed after every CI run for zero-effort access to latest rankings |
| 2026-03-23 | Workflow concurrency control | Prevents parallel CI runs from creating merge conflicts on binary DB file |
| 2026-03-23 | Keep Redfin over Realtor.com | Realtor.com GraphQL API has agent data but only 1,066 results (vs 2,311) and months stale — not viable as primary source |

## Changelog
- 2026-03-21: Initial build complete. All Python modules, GitHub Actions workflow, unit tests, and documentation created.
- 2026-03-21: Collected 2,371 Redfin transactions. Discovered CSV lacks agent columns. Pivoted strategy to Playwright-based agent enrichment from individual property pages. Dropped RapidAPI approach.
- 2026-03-21: Built and validated Playwright agent enrichment pipeline. Handles two Redfin DOM structures, fresh browser context per page, CDN error detection and retry. 10/10 test URLs enriched correctly. 97 tests passing. GitHub Actions workflow updated with enrichment step.
- 2026-03-22: Pushed to GitHub. Configured residential proxy (IPRoyal). Added resource blocking. Built HTML dashboard with trend badges. Added property type filter (SFH + Condo only). Added brokerage-as-agent exclusion. Purged 1,636 non-residential records. Set up GitHub Pages.
- 2026-03-23: Fixed merge conflict from concurrent CI runs (added workflow concurrency control). Fixed Pages auto-deploy. Investigated Realtor.com GraphQL API — functional but far less comprehensive than Redfin; not viable as replacement. 115 tests passing.
