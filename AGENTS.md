# AGENTS.md — gw-re-agent-scraper

## Current Status (2026-04-16)
**Phase: Maine Listings (MLS) Phase 2 Enrichment pending Firecrawl plan upgrade**

### Pipelines Summary
| Source | Status | Records |
|--------|--------|---------|
| Redfin (Playwright) | ✅ Running 4x/day | 2,398 txns, ~85% enriched (listing agent only) |
| Zillow Firecrawl Directory | ✅ Complete | 740 agents, 125 teams |
| Zillow Profile Enrichment | ✅ Complete | 683/740 agents enriched (career stats + 5 recent sold) |
| **Maine Listings Phase 1 (discovery)** | ✅ Complete | **10,587 closed listings across 10 towns** |
| **Maine Listings Phase 2 (enrichment)** | ⏳ Pending upgrade | 0 enriched so far — needs 10,587 credits |

### Maine Listings Pipeline (Primary source going forward)
MaineListings.com is the Maine MLS public portal. Captures BOTH listing agent AND buyer agent on every closed transaction — data no other source provides. 10+ years of historical data available.

- Phase 1 (search page discovery): Complete. 10,587 listings discovered from mainelistings.com across all 10 towns.
- Phase 2 (detail page enrichment): Extracts listing_agent, buyer_agent, offices, MLS#, close_date, sale_price from embedded NUXT JavaScript data. Currently serial with 6s delay (~18 hours total). Need concurrent refactor to run in ~1 hour with 25 workers.
- Cost to complete Phase 2: ~10,587 credits. User is upgrading Firecrawl to Standard plan ($99/mo, 100K credits, 50 concurrent) for one-time backfill, then downgrade to Hobby for weekly incremental (~50-100 credits/week).

### Firecrawl Credit Usage
- Hobby plan (3K/mo): ~890 used on Zillow enrichment + ~600 on Maine discovery = 1,490
- Standard plan needed for Maine Phase 2 backfill
- Weekly incremental post-backfill: fits Hobby tier easily

## Zillow Implementation Status
- Firecrawl-based directory scraping live and tested:
  - `src/zillow_firecrawl.py` — Firecrawl discovery + markdown parsing
  - `src/zillow_directory_report.py` — Two-leaderboard report + dashboard
  - `src/zillow_main.py` — CLI with `--use-firecrawl`, `--max-pages`, `--directory-report`
  - `tests/test_zillow_firecrawl.py` — 39 tests
  - `.github/workflows/zillow_leaderboard.yml` — GitHub Actions with Firecrawl support
- Separate Zillow database/state/output paths:
  - `data/zillow_agent_data.db`
  - `data/zillow_scrape_state.json`
  - `data/zillow_agent_leaderboard.md`
  - `data/zillow_buyer_leaderboard.md`
  - `data/zillow_dashboard.html`
  - `data/zillow_team_gap.md`
- Zillow pipeline modules implemented:
  - `src/zillow.py`
  - `src/zillow_main.py`
  - `src/zillow_state.py`
- Shared reporting/dashboard layer refactored to be explicit `source`-scoped and `role`-scoped so Zillow seller/buyer data can coexist with Redfin data.
- Zillow GitHub Actions workflow implemented at `.github/workflows/zillow_leaderboard.yml`.
- Fast proxy smoke diagnostics added:
  - `data/zillow_proxy_diagnostics.md`
  - smoke-only workflow mode for temp-branch validation
  - fail-fast exit code `2` when no Zillow probe returns `ok`

## What's In the Database
| Town | Transactions | Avg Price | Date Range |
|------|-------------|-----------|------------|
| Kennebunk | 351 | $771K | 2023-03 to 2026-03 |
| Scarborough | 340 | $770K | 2023-03 to 2026-03 |
| Old Orchard Beach | 337 | $523K | 2023-03 to 2026-03 |
| Kittery | 317 | $610K | 2023-03 to 2026-03 |
| Saco | 315 | $562K | 2023-03 to 2026-03 |
| Biddeford | 313 | $533K | 2023-03 to 2026-03 |
| Kennebunkport | 189 | $1.28M | 2023-03 to 2026-03 |
| York | 109 | $987K | 2023-03 to 2026-03 |
| Wells | 81 | $657K | 2023-04 to 2026-03 |
| Ogunquit | 19 | $810K | 2023-04 to 2026-01 |

## Key Discoveries
1. **Redfin CSV no longer includes agent columns** — confirmed for ALL MLS markets, not just Maine. This was the original spec's primary data source assumption. The CSV still provides excellent property data.
2. **Redfin property pages DO show agent/brokerage** — you must visit the individual URL to see them. Each of our 2,371 transactions has a `source_url` field with the Redfin link.
3. **RapidAPI (realtor-data1) is not credible** — unofficial, poorly documented third-party wrapper. Decided to skip it entirely.
4. **3 towns are "minorcivildivision" on Redfin** (York, Ogunquit, Wells) — the CSV API doesn't support this region type, so we query York County (region_type=5) and filter by city. This results in lower transaction counts for those towns.
5. **Redfin date format** is "June-30-2025" not ISO — parser added.
6. **Redfin pagination appears to return randomized/overlapping data** — unique records plateau regardless of page count. City-type towns cap at ~350 unique transactions.
7. **Two different Redfin DOM structures for agent data:**
   - Redfin-agent listings: `.agent-card-wrapper` → `.agent-card-title` "Seller's agent" + `.agent-basic-details--heading a` (name) + `.agent-basic-details--broker span` (office)
   - Non-Redfin agents: `.agent-info-section .listing-agent-item` → `.agent-basic-details--heading span` (name with "Listed by" prefix) + `.agent-basic-details--broker` (nested spans with dot separator)
8. **Redfin CloudFront blocks rapid sequential requests from same browser session** — must create fresh browser context per page (new user-agent, viewport, cookies). 5-10s delay caused 403s; 10-20s delay with context rotation works reliably.
9. **React hydration timing** — agent cards take 3-8s to render after `domcontentloaded`. Fixed 2-second `wait_for_timeout` failed; must use `wait_for_selector('.agent-card-wrapper, .agent-info-section', timeout=8000)` with fallback.
10. **Some Redfin URLs return intermittent CloudFront 403s** ("The request could not be satisfied") — these are transient CDN errors, not captchas. Marked as `error` and retried up to 3 times.
11. **Zillow public agent pages expose richer transaction detail than Redfin** — seller/buyer side (`Represented: Buyer|Seller`), sold rows, and profile-level sales stats are visible on public directory/profile pages, so a parallel Zillow dataset is viable if access can be stabilized.
12. **Current GitHub `PROXY_URL` is blocked by Zillow's PerimeterX** — as of April 7, 2026, both `requests` and Playwright probes return captcha HTTP `403` on Zillow professionals pages even when routed through the repo's configured proxy.
13. **Warmup strategy is not the root problem on Zillow** — making warmup non-fatal and retrying direct target navigation still resulted in captcha HTTP `403` on the actual directory pages.
14. **The first Zillow Actions failure was workflow plumbing, not the scraper itself** — initial temp-branch runs failed after scraping due to branch rebase/merge logic. Push-triggered smoke runs now skip commit/deploy steps and run as diagnostics only.
15. **Fast smoke-check workflow is now the preferred way to validate Zillow access** — it records the observed egress IP, runs `requests` + Playwright probes against a few Zillow URLs, writes `data/zillow_proxy_diagnostics.md`, and fails in ~1 minute if the proxy is blocked.
16. **Firecrawl may be worth a one-URL smoke test; Cloudflare Browser Rendering is less promising** — Firecrawl offers enhanced proxy/browser handling, while Cloudflare's Browser Rendering is documented as bot-identifiable. Local environment currently lacks `node`/`npm`/`npx`, so Firecrawl was not tested yet.

## Open Issues
- **~526 URLs still pending enrichment** — pipeline running, ~7 runs at 80/batch to complete
- York (99), Wells (73), Ogunquit (18) have lower counts due to county query limitations
- Some pages may have `no_agent` (listing removed, very old, etc.) — accept this as data gap
- Realtor.com GraphQL API investigated as alternative enrichment source — has agent data but far less comprehensive than Redfin (1,066 vs 2,311 records, data months stale). Not viable as replacement.
- PrimeMLS evaluated as data source — authoritative MLS but Cloudflare-protected, explicit anti-scraping ToS, sold data likely behind member login. Not viable without MLS membership.
- **Zillow blocked on current proxy** — latest smoke check on April 7, 2026 observed egress IP `96.191.2.240`; all Zillow professionals probes returned captcha HTTP `403`.
- **Zillow code is currently on temp branch only** — branch `zillow-actions-smoke-20260406` contains the Zillow pipeline/workflow/smoke diagnostics work; it has not been merged into `main`.
- **Need a different Zillow-capable proxy or compliant data source** — current blocker appears to be IP/session reputation, not DOM parsing or Playwright setup.

## Next Steps (Priority Order)
1. **[IMMEDIATE] Maine Listings Phase 2 enrichment with concurrent workers** — User is upgrading Firecrawl to Standard ($99/mo) for one month. Need to refactor `src/maine_firecrawl.py::enrich_listings` to use `ThreadPoolExecutor` with 20-25 concurrent workers + circuit breaker. Then enrich all 10,587 listings (~1 hour at 25 concurrent). See handoff prompt below.
2. **Build maine_report.py** — leaderboards for listing agents, buyer agents, brokerages using the MLS transaction data.
3. **Integrate Maine data into index.html** — add "Maine MLS" tab, update agent search index with Maine transaction detail.
4. **Schedule weekly incremental** — GitHub Actions workflow for `--discover --recent-only --enrich` (~50-100 credits/week, fits Hobby tier).
5. **Deprecate Redfin Playwright enrichment** — once Maine Listings covers all transactions with better data, stop Redfin enrichment to save proxy costs.

## Session Log
- 2026-03-21 (session 1): Initial build from spec. All modules, GitHub Actions workflow, unit tests, and docs created.
- 2026-03-21 (session 2): Discovered Redfin region IDs, found agent columns missing from CSV, adapted scraper for county queries and date format. Collected 2,371 transactions across 10 towns. Decided to skip RapidAPI (not credible) and use Playwright to enrich agent data from individual Redfin property pages. Committed code + data to local repo.
- 2026-03-21 (session 3): Built Playwright enrichment pipeline. Key learnings: (a) Redfin has two different DOM structures for agent data depending on whether the listing agent is a Redfin employee; (b) must create fresh browser context per page to avoid CloudFront 403 blocks; (c) React hydration requires `wait_for_selector` with 8s timeout, not fixed delay; (d) 10-20s delay between pages required (5-10s caused CDN blocks). Successfully enriched 10 test URLs with 100% accuracy. Added 35 new tests (97 total). Updated GitHub Actions workflow with enrichment step.
- 2026-03-22 (session 4): Pushed to GitHub. Configured residential proxy (IPRoyal) via PROXY_URL secret. Fixed proxy auth (split URL into server/username/password for Playwright). Added resource blocking to save ~70-80% proxy bandwidth.
- 2026-03-22 (session 5): Built HTML dashboard with 4 sections (all-time agents, 365-day rolling with trend badges, brokerages, per-town). Added property type filter (SFH + Condo only, `uipt=1,2`). Added brokerage-as-agent exclusion. Re-scraped to tag `property_type`, auto-purged 1,636 non-residential records. Set up GitHub Pages for auto-deployed dashboard.
- 2026-03-23 (session 6): Fixed merge conflict in dashboard from concurrent CI runs — added workflow concurrency control. Investigated Realtor.com GraphQL API as alternative enrichment source — functional but far less comprehensive than Redfin (1,066 vs 2,311 records, months stale). Decided to keep Redfin Playwright as primary enrichment. Fixed Pages auto-deploy by moving deployment into scraper workflow (GitHub bot pushes don't trigger separate workflows).
- 2026-03-30 (session 7): Diagnosed IPRoyal proxy outage (ERR_TUNNEL_CONNECTION_FAILED since Mar 28). User renewed subscription, enrichment resumed. Evaluated PrimeMLS as data source — authoritative MLS but Cloudflare-protected, explicit anti-scraping ToS, not viable. Fixed table column alignment with table-layout:fixed + colgroups.
- 2026-03-31 (session 8): Added 365-day rolling brokerage leaderboard with trend badges and operating towns. Split brokerage section into all-time + rolling (6 sections total). Added office name normalization — 15 variant spellings merged to canonical names (139 rows). Added Anne Erwin Real Estate to BROKERAGE_AS_AGENT exclusion list. 125 tests passing. Enrichment at 76% (1,762/2,311).
- 2026-04-06 (session 9): Reviewed project and planned Zillow V1 as a parallel dataset rather than a Redfin replacement. Implemented separate Zillow DB/state/artifacts, buyer/seller role-aware reporting, Zillow workflow, dashboard support, and Zillow test coverage. Local Zillow suites passed, but live local smoke tests showed Zillow serving PerimeterX captcha pages.
- 2026-04-07 (session 10): Hardened Zillow scraper with retries, warmup fallback, proxy/session telemetry, and GitHub Actions smoke-only mode. Ran multiple GitHub Actions validations on temp branch `zillow-actions-smoke-20260406`. Confirmed repo `PROXY_URL` secret is blocked by Zillow: latest smoke run observed egress IP `96.191.2.240` and both `requests` and Playwright got captcha HTTP `403` on Zillow professionals root, York, and Kittery. Added `data/zillow_proxy_diagnostics.md` and fail-fast smoke-check workflow to validate replacement proxies quickly.
- 2026-04-07 (session 11): Replaced blocked Playwright approach with Firecrawl API for Zillow. Smoke-tested Firecrawl against Zillow — bypassed PerimeterX completely. Built `zillow_firecrawl.py` (directory scraping + markdown parsing), `zillow_directory_report.py` (two-leaderboard report + dashboard), 39 new tests. Ran full 10-town scrape: 740 agents, 125 teams, ~250 credits. Fixed name/office parsing (bold markers as delimiter, strip rating bleed), added brokerage classification. Code review: fixed eval injection in workflow, added pip-audit, fixed or-chain bug, removed dead code. Buyer/seller split deferred to profile enrichment (next billing cycle). Key learnings: (a) Firecrawl SDK uses `client.scrape()` not `client.scrape_url()`, returns objects not dicts; (b) Zillow caps directory at 25 pages regardless of total count; (c) office branches must stay separate (competing entities within same chain); (d) brokerage profiles on Zillow have no structural difference from individual agents — classification by missing office_name works for unit tests but real data rarely has null office.
- 2026-04-07 (session 12): Built tabbed dashboard (`data/index.html`) wrapping Redfin + Zillow dashboards. Added agent search across 834 Redfin + 685 Zillow agents with detail cards. Added date-range chunking for county Redfin queries (York/Wells/Ogunquit went from 99/73/18 → 129/114/22 transactions, +87 total). Added Avg Price + Est. Volume columns to leaderboards.
- 2026-04-10 to 2026-04-15 (sessions 13-16): Zillow profile enrichment. Discovered Firecrawl `interact`/`browser` modes can't access Zillow (PerimeterX blocks all browser modes except `scrape()`). Sold row pagination inside profile pages not reliably capturable via Firecrawl actions (React re-renders too fast). Settled on page-1-only enrichment: 5 most recent sold transactions + career stats from NUXT/Apollo cache. Ran enrichment on 683/740 agents across ~2 days (~1,400 credits). Added enrichment data to dashboard: career sales, 12-mo, avg price, buyer/seller split from 5-row sample, recent transactions table with "N older transactions not shown" flag. Changed master leaderboard to rank by Local Sales (in our 10 towns) instead of career total; added Local % column (% of career in our territory).
- 2026-04-15 (session 17): Built Maine Listings (MREIS MLS) scraper. MaineListings.com is the Maine MLS public portal — shows BOTH listing agent AND buyer agent on every closed transaction via embedded NUXT JavaScript data. Built full pipeline: `maine_database.py`, `maine_state.py`, `maine_parser.py`, `maine_firecrawl.py`, `maine_main.py`. Tested on Kittery (5/5 success, both agents captured). Ran Phase 1 discovery: 10,587 closed listings across all 10 towns (~600 credits). Phase 2 enrichment deferred pending plan upgrade (10,587 credits needed, $99/mo Standard plan recommended for one-time backfill). Key technical insight: detail pages have TWO `list_agent` objects in NUXT — first is `co_list_agent` (usually null `a`), second has real data. Parser finds the one where `list_agent_email` is a quoted string.
