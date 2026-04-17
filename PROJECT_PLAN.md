# PROJECT_PLAN.md — gw-re-agent-scraper

## Objective
Identify the top real estate agents and brokerages across 10 southern Maine towns using MLS-authoritative data — both listing-side and buyer-side, with period-based KPIs and year-over-year movement tracking.

## Phase Tracker
- [x] Phase 1: Project scaffold and database module
- [x] Phase 2: State manager and Redfin CSV integration
- [x] Phase 3: Redfin region ID discovery and data collection
- [x] Phase 4: Report generator and CLI orchestrator
- [x] Phase 5: GitHub Actions workflow and project docs
- [x] Phase 6: Playwright agent enrichment
- [x] Phase 7: Residential proxy + automated enrichment
- [x] Phase 8: HTML dashboard + GitHub Pages
- [x] Phase 9: Property type filter
- [x] Phase 10: Office name normalization + brokerage-as-agent exclusion
- [x] Phase 11: Redfin enrichment completion (archived 2026-04-16)
- [x] Phase 12: Zillow parallel dataset — Firecrawl directory + profile enrichment (archived 2026-04-16)
- [x] Phase 13: Tabbed dashboard + unified agent search
- [x] Phase 14: Maine Listings (MREIS MLS) discovery — 16,029 closed listings across 10 towns
- [x] Phase 15: Maine Listings concurrent Phase 2 enrichment — 16,024 enriched (99.97% success)
- [x] Phase 16: Alerting (Pushover + Resend) + DB backup + weekly GH Actions cron
- [x] Phase 17: Redfin/Zillow retirement + Maine promoted to primary
- [x] Phase 18: Maine Leaderboard redesign — KPI rollups (12mo/prior-12mo/3yr/all-time), Biggest Movers banner, Agent/Brokerage toggle, period selector, in-table search, top-50-per-town

## Current Status
All primary objectives complete. The Maine MLS pipeline is the source of truth. Interactive Leaderboard + standalone HTML dashboard both in production. Weekly cron keeps data fresh.

**Live:** https://pmtinkerer.github.io/gw-re-agent-scraper/

## Decision Log (major decisions only)
| Date | Decision | Rationale |
|------|----------|-----------|
| 2026-03-21 | SQLite over Postgres | No external DB; single file; committed to repo |
| 2026-03-21 | Playwright for Redfin agent data | CSV dropped agent columns; property pages render them with browser |
| 2026-03-21 | Skip RapidAPI/Realtor.com | Unofficial; far less comprehensive than Redfin at the time |
| 2026-03-22 | IPRoyal residential proxy for Redfin | GH Actions datacenter IPs get captcha'd by CloudFront |
| 2026-03-22 | SFH + Condo only | Land/multi-family/mobile irrelevant to residential agent leaderboard |
| 2026-03-31 | Office name normalization + BROKERAGE_AS_AGENT exclusion | Variant spellings + placeholder names pollute rankings |
| 2026-04-07 | Firecrawl for Zillow | PerimeterX blocks Playwright + residential proxies; Firecrawl bypasses it |
| 2026-04-07 | Office branches stay separate | Competing entities within a chain; no cross-branch aggregation |
| 2026-04-15 | Maine Listings as primary source | Only source that captures both listing + buyer agent on every transaction |
| 2026-04-16 | Concurrent Firecrawl enrichment with ThreadPoolExecutor | Firecrawl Standard plan supports 50 concurrent; cuts 16K-listing backfill from ~18h → ~3h |
| 2026-04-16 | Pushover + Resend alerting | User explicit requirement for failure notifications |
| 2026-04-16 | DB backup before mutating runs | Protects against data loss if enrichment crashes mid-run |
| 2026-04-16 | Archive Redfin + Zillow pipelines | Maine MLS strictly dominates Redfin (15yr vs 3yr, both sides, no CSV cap); Zillow still useful for profile richness only |
| 2026-04-17 | Single enhanced table + Movers banner | UX: one dense sortable view beats multi-tab nav; Movers banner surfaces the momentum story at a glance |
| 2026-04-17 | Period KPI rollups in separate `maine_kpis.py` module | Keeps `maine_report.py` focused on markdown; shared by static dashboard + interactive tab |

## Changelog (most recent first)
- **2026-04-17:** Maine Leaderboard redesign shipped. 12-column KPI table + Biggest Movers banner. New `src/maine_kpis.py` module. 24 new tests (232 total). See `docs/superpowers/specs/2026-04-16-maine-leaderboard-redesign-design.md` and `docs/superpowers/plans/2026-04-16-maine-leaderboard-redesign.md`.
- **2026-04-16:** Maine Listings Phase 2 enrichment complete (16,024 / 16,029, 99.97%). Concurrent ThreadPoolExecutor refactor, circuit breaker, thread-safe SQLite writes. Pushover + Resend alerting. DB backup. Town canonicalization. Tabs reordered (Maine primary; Redfin + Zillow archived).
- **2026-04-15:** Maine Listings scraper built. Phase 1 discovery: 10,587 closed listings across 10 towns. Two-phase architecture (search page → detail page NUXT blob).
- **2026-04-10 to 2026-04-15:** Zillow profile enrichment. 683/740 agents. Page-1-only sold rows. Tabbed dashboard wrapping Redfin + Zillow.
- **2026-04-07:** Zillow Firecrawl pipeline built. 740 agents across 10 towns.
- **2026-04-06:** Zillow V1 scaffolded as parallel dataset (separate DB/state/reports, seller/buyer role-aware reporting).
- **2026-03-31:** 365-day rolling brokerage leaderboard + office normalization.
- **2026-03-22 to 2026-03-30:** Redfin enrichment + dashboard + proxy stabilization.
- **2026-03-21:** Initial build. Redfin CSV collection + Playwright enrichment scaffolding.
