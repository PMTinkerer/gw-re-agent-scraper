# AGENTS.md — gw-agent-scraper

## Current Status
Redfin property data collected (2,371 transactions across 10 towns). No agent data yet — Redfin CSV no longer includes agent columns. Realtor.com integration needed as primary agent data source.

## Last Completed Work
- Discovered Redfin region IDs for all 10 towns (hardcoded after web search)
- Found that 3 towns (York, Ogunquit, Wells) are "minorcivildivision" type on Redfin — CSV API doesn't support this, so we query York County and filter by city
- Discovered that Redfin CSV endpoint NO LONGER includes agent columns (LISTING AGENT, BUYER'S AGENT, etc.) for any MLS — this was the primary data source assumption in the original spec
- Collected 2,371 property transactions from Redfin with property data (address, price, MLS#, sold date, beds, baths, sqft) but zero agent names
- Fixed date parsing (Redfin uses "June-30-2025" format, not ISO)
- All 62 unit tests passing

## Open Issues
- **CRITICAL: No agent data from Redfin** — CSV endpoint removed agent columns. Realtor.com is now the only path to listing agent names
- York (109), Wells (81), Ogunquit (19) have lower transaction counts due to county-level pagination limitations
- Realtor.com RapidAPI response structure not yet validated — need RAPIDAPI_KEY to test
- City-type towns capped at ~350 transactions (Redfin CSV max per query, pagination appears to return overlapping data)

## Next Steps
1. **Set up Realtor.com RapidAPI** — sign up at rapidapi.com, get API key, test with one town to validate agent data is returned
2. If RapidAPI returns agent data: run all 30 Realtor.com chunks to enrich existing transactions
3. If RapidAPI does NOT return agent data: build Playwright scraper for Realtor.com sold pages
4. Push to GitHub and configure RAPIDAPI_KEY secret

## Session Log
- 2026-03-21 (session 1): Initial build from spec. All modules, GitHub Actions workflow, unit tests, and docs created.
- 2026-03-21 (session 2): Discovered Redfin region IDs, found agent columns missing from CSV, adapted scraper for county queries and date format. Collected 2,371 transactions across 10 towns. Committed code + data to local repo.
