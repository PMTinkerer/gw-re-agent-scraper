# AGENTS.md — gw-re-agent-scraper

## Current Status (2026-04-17)
**Phase: Maine MLS Phase 2 enrichment COMPLETE — leaderboard redesign shipped.**

### Pipelines Summary
| Source | Status | Records |
|--------|--------|---------|
| **Maine Listings (MREIS MLS)** | ✅ PRIMARY — weekly cron | **16,024 enriched** closed transactions (2011–2026); 2,253 listing agents + 2,634 buyer agents |
| Redfin (Playwright) | 🗄️ Archived | 2,398 transactions captured. Cron disabled; manual dispatch only. Strictly a subset of Maine MLS. |
| Zillow (Firecrawl directory + profile) | 🗄️ Archived | 740 agents, 683 enriched. Kept for profile richness (bios/photos/reviews). No cron. |

### Interactive Leaderboard (shipped 2026-04-17)
- **Leaderboard tab** at `data/index.html` → `/` on Pages
- 12 columns: `#`, Agent/Brokerage, Office/Agents, 12mo Δ, 12mo Vol, 12mo Sides, 3yr Vol, All-Time Vol, All-Time Sides, L/B, Avg 3yr, Most Recent, Primary Towns
- Agent / Brokerage toggle. Town filter (caps to top 50 when set). Period selector (12mo/3yr/All-time changes default sort). In-table name/office search.
- Biggest Movers banner: top 5 risers + top 5 fallers by 12mo rank vs prior-12mo rank. Auto-hides when < 10 qualifying entities (≥5 sides each).
- Row click + mover-card click both open detail modal with every period split.
- Data via `src/maine_kpis.py` (`query_agent_kpis`, `query_brokerage_kpis`, `compute_rank_movers`).

### Maine Listings Pipeline (Primary source)
- MaineListings.com is the Maine MLS public consumer portal (Maine Association of REALTORS). Every closed transaction shows both listing + buyer agent.
- Phase 1 (search page discovery): Complete. 16,029 listings across all 10 towns.
- Phase 2 (detail page enrichment): Complete. 16,024 enriched (5 Firecrawl 500 errors, 99.97% success). Concurrent with 25 Firecrawl workers, ~3h wall time.
- Weekly incremental (`--discover --recent-only --enrich`): ~50-100 credits/week, fits Hobby tier.
- Alerting: Pushover + Resend fire on circuit-breaker aborts and run summaries.
- DB backup before every mutating run (last 3 timestamped copies).

### Firecrawl Credit Usage
- One-time Phase 2 backfill: ~16K credits on Standard plan ($99/mo).
- Weekly incremental post-backfill: ~50-100 credits/week. Hobby plan (3K/mo) is sufficient long-term.

## Key Discoveries (Maine MLS)
1. **mainelistings.com is the official public MREIS portal** — operated by Maine Association of REALTORS. Data flows FROM MREIS TO Zillow/Realtor/Homes, not the reverse.
2. **Both agents visible on every closed transaction** — data no other scraped source provides (Redfin only shows listing agent, Zillow sold-rows paginate at 5).
3. **NUXT data blob has TWO `list_agent` objects** — first is `co_list_agent` (usually null), second has real data. Parser picks the one where `list_agent_email` is a quoted string.
4. **JSON-style escape sequences in string values** — NUXT double-encodes, so `"Better Homes\u002FMasiello"` arrives as literal `\u002F`. Decoded in Python after regex extraction.
5. **Town URL param requires human-readable spelling** — `?city=Old Orchard Beach` works; `?city=old_orchard_beach` silently returns zero results. Canonicalization layer in `maine_main._canonicalize_town`.
6. **Zillow numbers are inflated vs MLS truth** — cross-check on Troy Williams shows Zillow claims 1,680 local sales vs MLS reality of 592 sides over 15 years. Likely includes off-market or self-reported data. MLS is authoritative.
7. **Redfin CSV is capped at ~350 rows per town query** — undercounts every active agent. Strictly a subset of MLS.

## Key Discoveries (earlier sessions — Redfin + Zillow)
1. **Redfin CSV no longer includes agent columns** — confirmed for ALL MLS markets (2026).
2. **Redfin property pages DO show agent/brokerage** — must visit individual URL via Playwright. Two DOM structures: `.agent-card-wrapper` (Redfin-agent) and `.listing-agent-item` (non-Redfin).
3. **Redfin CloudFront blocks rapid sequential requests from the same browser session** — fresh browser context per page + residential proxy (IPRoyal) required.
4. **Zillow's PerimeterX blocks `interact()` and `browser()` modes** — only `Firecrawl.scrape()` bypasses it.
5. **Zillow sold-row pagination unreliable via Firecrawl actions** — React re-renders too fast. Only page 1 (5 most recent) reliably captured.
6. **Office branches must stay separate** — chain branches (RE/MAX, Sotheby's, Coldwell Banker) are competing entities within the same chain. No normalization across branches.

## Open Issues
- 5 Maine listings returned Firecrawl 500 errors during enrichment. Will auto-retry on next weekly run.
- 3 Maine listings have malformed `city` from search-card regex edge cases (e.g., "Kennebunkport, 04046"). Enrichment corrected most; 3 remain. Low-priority cleanup.
- "NON-MREIS AGENT" placeholder + brokerage-as-agent names filtered out at query time via `_AGENT_EXCLUSIONS` in `maine_report.py`. Add new pollutants to that set as they surface.

## Next Steps
1. Review and merge PR #11 to main. GitHub Pages auto-deploys on push.
2. Downgrade Firecrawl to Hobby tier ($99/mo → cheaper) after backfill lands on main. Weekly incremental fits 3K/mo easily.
3. If desired follow-ups: territorial map view, per-agent sparklines in detail modal, CSV export — all punted as out-of-scope per spec.

## Session Log (most recent first)
- **2026-04-17 (session 20):** Maine MLS Leaderboard redesign shipped. New `src/maine_kpis.py` module with period queries + rank movers. `src/maine_dashboard.py` and `src/index_page.py` rewritten around KPI rollups. Biggest Movers banner + Agent/Brokerage toggle + period selector + in-table search. 24 new tests (232 total passing). Docs refreshed — Maine is now the primary source across README, AGENTS, PROJECT_PLAN, CLAUDE.md.
- **2026-04-16 (session 19):** Full Phase 2 enrichment. 16,024/16,029 closed MLS transactions enriched (99.97% success, 5 Firecrawl 500 errors). Concurrent refactor to ThreadPoolExecutor + circuit breaker + thread-safe SQLite writes. Pushover + Resend alerting wired. DB backup before mutating runs. Town canonicalization fix (`old_orchard_beach` → `Old Orchard Beach`). Redfin 4x/day cron disabled; Zillow already manual-only. Tabs reordered: Maine MLS default, then Leaderboard, Zillow (archive), Redfin (archive). PR #11 opened.
- **2026-04-15 (session 18):** Built Maine Listings (MREIS MLS) scraper. Phase 1 discovery: 10,587 closed listings across all 10 towns (~600 credits). Phase 2 enrichment deferred pending plan upgrade. Key insight: detail pages have TWO `list_agent` objects in NUXT — parser picks the one with a quoted email.
- **2026-04-10 to 2026-04-15 (sessions 13-17):** Zillow profile enrichment. 683/740 agents. Page-1-only sold rows due to React re-render timing. Tabbed dashboard (`data/index.html`) wrapping Redfin + Zillow. Added date-range chunking for county Redfin queries. Local Sales + Local % columns in master leaderboard.
- **2026-04-07 (sessions 10-12):** Zillow PerimeterX blocking → Firecrawl smoke test → Firecrawl SDK pipeline built (`zillow_firecrawl.py`, `zillow_directory_report.py`). 740 agents, 125 teams in 10 towns (~250 credits). Fixed eval injection in workflow, added pip-audit.
- **2026-04-06 (session 9):** Planned Zillow V1 as parallel dataset. Separate DB/state/artifacts, buyer/seller role-aware reporting, Zillow workflow + smoke diagnostics.
- **2026-03-31 (session 8):** 365-day rolling brokerage leaderboard. Office name normalization (15 variants). Anne Erwin Real Estate added to BROKERAGE_AS_AGENT exclusion.
- **2026-03-30 (session 7):** IPRoyal proxy outage diagnosed + renewed. Evaluated PrimeMLS (not viable — ToS + membership).
- **2026-03-22 to 2026-03-23 (sessions 4-6):** Pushed to GitHub. Residential proxy + resource blocking. HTML dashboard with trend badges. Property type filter (SFH + Condo). Brokerage-as-agent exclusion. GitHub Pages auto-deploy.
- **2026-03-21 (sessions 1-3):** Initial build. Redfin CSV collection (2,371 transactions). Playwright agent enrichment pipeline (two DOM structures, fresh context per page, CDN error detection). 97 tests passing.
