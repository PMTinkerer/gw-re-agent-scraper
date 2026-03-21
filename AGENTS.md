# AGENTS.md — gw-agent-scraper

## Current Status
**Phase: Agent Data Enrichment (Playwright needed)**

Redfin property data collected: 2,371 transactions across all 10 towns (March 2023–March 2026). Property fields are complete (address, price, MLS#, sold date, beds, baths, sqft). Agent/brokerage fields are empty — the Redfin CSV endpoint no longer includes them.

**The next step is building a Playwright script to visit each Redfin property URL and extract the listing agent + brokerage from the page.**

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

## Key Discoveries (This Session)
1. **Redfin CSV no longer includes agent columns** — confirmed for ALL MLS markets, not just Maine. This was the original spec's primary data source assumption. The CSV still provides excellent property data.
2. **Redfin property pages DO show agent/brokerage** — you must visit the individual URL to see them. Each of our 2,371 transactions has a `source_url` field with the Redfin link.
3. **RapidAPI (realtor-data1) is not credible** — unofficial, poorly documented third-party wrapper. Decided to skip it entirely.
4. **3 towns are "minorcivildivision" on Redfin** (York, Ogunquit, Wells) — the CSV API doesn't support this region type, so we query York County (region_type=5) and filter by city. This results in lower transaction counts for those towns.
5. **Redfin date format** is "June-30-2025" not ISO — parser added.
6. **Redfin pagination appears to return randomized/overlapping data** — unique records plateau regardless of page count. City-type towns cap at ~350 unique transactions.

## Open Issues
- **Agent data is empty** — need Playwright enrichment script to visit 2,371 Redfin URLs
- York (109), Wells (81), Ogunquit (19) have lower counts due to county query limitations — Playwright enrichment may also find additional sold listings on these pages
- Not yet pushed to GitHub (no `gh` CLI installed)
- GitHub Actions workflow not yet tested in production

## Next Steps (Priority Order)
1. **Build Playwright enrichment script** — new function `enrich_agents_from_redfin(conn)` in `src/scraper.py`:
   - Iterate through transactions where `listing_agent IS NULL` and `source_url IS NOT NULL`
   - Visit each Redfin URL with stealth Playwright (reuse patterns from `~/competitor-scraper/utils/stealth.py`)
   - Extract listing agent name and brokerage from the page
   - Update the transaction record in SQLite
   - Add `--enrich-agents` CLI flag to `main.py`
   - Rate limit: 5-10 second delay between page visits
   - Resumable: skip transactions that already have agent data
   - Estimated runtime: 3-7 hours (2,371 pages × 5-10s each), split across multiple GitHub Actions runs
2. **Test on a single old listing (2023)** to verify agent data is still visible on pages that old
3. **Run full enrichment** locally or via GitHub Actions
4. **Run fuzzy agent merge + generate leaderboard** once agent data is populated
5. **Install `gh` CLI, create GitHub repo, push**
6. **Configure GitHub Actions** with scheduled runs

## Session Log
- 2026-03-21 (session 1): Initial build from spec. All modules, GitHub Actions workflow, unit tests, and docs created.
- 2026-03-21 (session 2): Discovered Redfin region IDs, found agent columns missing from CSV, adapted scraper for county queries and date format. Collected 2,371 transactions across 10 towns. Decided to skip RapidAPI (not credible) and use Playwright to enrich agent data from individual Redfin property pages. Committed code + data to local repo.
