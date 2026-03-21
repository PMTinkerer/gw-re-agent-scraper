# PROJECT_PLAN.md — gw-agent-scraper

## Objective
Identify the top real estate listing agents across 10 southern Maine towns using 3 years of publicly visible sold data.

## Phase Tracker
- [x] Phase 1: Project scaffold and database module
- [x] Phase 2: State manager and Redfin CSV integration
- [x] Phase 3: Realtor.com RapidAPI integration
- [x] Phase 4: Report generator and CLI orchestrator
- [x] Phase 5: GitHub Actions automation and project docs
- [ ] Phase 6: First local test and region ID discovery
- [ ] Phase 7: Initial data collection (GitHub Actions)
- [ ] Phase 8: Agent name normalization tuning
- [ ] Phase 9: Leaderboard review and incremental mode
- [ ] Phase 10: Zillow fallback (only if needed)

## Decision Log
| Date | Decision | Rationale |
|------|----------|-----------|
| 2026-03-21 | SQLite over Postgres | No external DB dependency, single file, committed to repo, sufficient for this data volume |
| 2026-03-21 | Redfin as primary source | CSV endpoint returns structured data with agent names, no Playwright needed |
| 2026-03-21 | Realtor.com via RapidAPI only | 100 free req/month covers needs in ~1 month; avoids ToS-violating scraping |
| 2026-03-21 | Pull all 3 years at once per Redfin town | Reduces chunks from 30 to 10; filter by year in Python |
| 2026-03-21 | rapidfuzz for name matching | C-accelerated, permissive license, >90% threshold + same office |
| 2026-03-21 | No pandas dependency | csv.DictReader + raw SQL handles everything needed |
| 2026-03-21 | Chunk-based resumable processing | Fits GitHub Actions 45-min timeout and 2000 min/month free tier |

## Changelog
- 2026-03-21: Initial build complete. All Python modules, GitHub Actions workflow, unit tests, and documentation created.
