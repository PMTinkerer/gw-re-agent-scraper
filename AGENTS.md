# AGENTS.md — gw-agent-scraper

## Current Status
**Phase: Agent Data Enrichment (Playwright pipeline BUILT — running)**

Redfin property data collected: 2,371 transactions across all 10 towns (March 2023–March 2026). Property fields are complete (address, price, MLS#, sold date, beds, baths, sqft).

**Playwright enrichment pipeline built, tested, and validated.** 10 URLs enriched successfully (10/10 success rate on tested batch). 2,361 URLs pending. Estimated ~24 runs at 100 URLs/batch to complete all enrichment.

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

## Open Issues
- **2,361 URLs still pending enrichment** — pipeline works, just needs to run through full batch (~24 runs at 100/batch)
- York (109), Wells (81), Ogunquit (19) have lower counts due to county query limitations
- Not yet pushed to GitHub
- GitHub Actions workflow not yet tested in production
- Some pages may have `no_agent` (listing removed, very old, etc.) — accept this as data gap

## Next Steps (Priority Order)
1. **Run full enrichment** — `python -m src.main --enrich --batch-size 100` repeatedly, or via GitHub Actions (runs automatically)
2. **Review fuzzy agent merge results** after enrichment is mostly complete
3. **Push to GitHub and configure Actions** — first automated run
4. **Review leaderboard** output once agent data is populated across all towns

## Session Log
- 2026-03-21 (session 1): Initial build from spec. All modules, GitHub Actions workflow, unit tests, and docs created.
- 2026-03-21 (session 2): Discovered Redfin region IDs, found agent columns missing from CSV, adapted scraper for county queries and date format. Collected 2,371 transactions across 10 towns. Decided to skip RapidAPI (not credible) and use Playwright to enrich agent data from individual Redfin property pages. Committed code + data to local repo.
- 2026-03-21 (session 3): Built Playwright enrichment pipeline. Key learnings: (a) Redfin has two different DOM structures for agent data depending on whether the listing agent is a Redfin employee; (b) must create fresh browser context per page to avoid CloudFront 403 blocks; (c) React hydration requires `wait_for_selector` with 8s timeout, not fixed delay; (d) 10-20s delay between pages required (5-10s caused CDN blocks). Successfully enriched 10 test URLs with 100% accuracy. Added 35 new tests (97 total). Updated GitHub Actions workflow with enrichment step.
