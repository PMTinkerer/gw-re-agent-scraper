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

### Firecrawl Account Limits (verified live 2026-09-21)
Query these rather than trusting notes — both numbers below were previously
documented wrong, which cost a failed backfill run:

```bash
curl -s -H "Authorization: Bearer $FIRECRAWL_API_KEY" https://api.firecrawl.dev/v2/team/queue-status
curl -s -H "Authorization: Bearer $FIRECRAWL_API_KEY" https://api.firecrawl.dev/v2/team/credit-usage
```

- **`maxConcurrency` is 5.** Never pass `--workers` above 5. Higher values fail
  with `Request Timeout: ... timed out while waiting for a concurrency slot`,
  which trips the circuit breaker and aborts the batch. The `--workers 25` in
  older docs reflects a plan we are no longer on.
- **Credits are annual, not monthly:** 5,000 per billing period, currently
  2026-04-07 → 2027-04-07. This was previously documented as "Hobby plan
  (3K/mo)" — off by a factor of 12 in the wrong direction.
- Weekly incremental: ~50-150 credits/week. At ~28 weeks left in the period,
  routine operation is roughly 3,000-4,000 credits, so a large one-time
  backfill needs checking against `remainingCredits` first.

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
- **Backfill pending (run after PR merges).** ~565 Closed rows have no `close_date`/`buyer_agent` and ~924 rows are mislabelled `Withdrawn`. Both are now picked up automatically, but the backlog needs one pass: dispatch the workflow with mode `backfill-enrichment` (and `--reverify-withdrawn` for the Withdrawn set). Budget ~1,500 Firecrawl credits one-time; use `--max-credits` to spread it across runs if the monthly budget is tight.
- Recent months will keep filling in for several weekly runs after the sort fix lands — closings that were missed while the search was mis-sorted are recovered as the Closed search now surfaces them in close-date order.
- 5 Maine listings returned Firecrawl 500 errors during enrichment. Will auto-retry on next weekly run.
- 3 Maine listings have malformed `city` from search-card regex edge cases (e.g., "Kennebunkport, 04046"). Enrichment corrected most; 3 remain. Low-priority cleanup.
- "NON-MREIS AGENT" placeholder + brokerage-as-agent names filtered out at query time via `_AGENT_EXCLUSIONS` in `maine_report.py`. Add new pollutants to that set as they surface.

## Next Steps
1. Review and merge PR #11 to main. GitHub Pages auto-deploys on push.
2. Downgrade Firecrawl to Hobby tier ($99/mo → cheaper) after backfill lands on main. Weekly incremental fits 3K/mo easily.
3. If desired follow-ups: territorial map view, per-agent sparklines in detail modal, CSV export — all punted as out-of-scope per spec.

## Session Log (most recent first)
- **2026-09-19 (session 22):** Fixed silent under-capture of closed transactions. Reported symptom: agent Erin Lamarche's recent sales missing from the tool. Four defects found, all confirmed against live data: (1) **Closed search sorted by list date** — mainelistings.com defaults to `sort_by=on_market_date`, so `--recent-only` paged the oldest-listed end of a 208-page result set; July 2026 captured 15 closings vs 212 in July 2025 (~93% shortfall). `build_search_url` now sends `sort_by=close_date&sort_order=desc` for Closed. (2) **No re-enrichment on status change** — a row enriched while Active stayed `success` forever, so flipping to Closed never filled `close_date`/`buyer_agent` (565 rows). `upsert_listing` now resets enrichment on status change, and `get_unenriched` treats a Closed row with no close date as unenriched. (3) **Withdrawn sweeper guessed** — any Active row unseen 7 days was marked `Withdrawn` without verification, silently converting real sales into withdrawn listings (924 rows) and dropping them from every closed-side report. Replaced with `queue_stale_for_verification()`; `mark_withdrawn_stale()` is now a 30-day fallback that only acts on rows we tried and failed to verify. (4) **`--db` ignored when `--workers 1`** — `_enrich_serial` opened the default DB, so serial runs silently wrote the wrong database. Added `--reverify-withdrawn` one-time repair and a `backfill-enrichment` workflow mode. Active cron moved from daily to weekly. 19 new tests (278 passing). Verified end-to-end: both screenshot properties now resolve correctly — 454 Ocean Avenue (Withdrawn → Closed 2026-07-31, $1,245,000) and 4 Micelan Road (Closed 2026-06-26, $800,000), both with buyer agent Erin Lamarche / Portside Real Estate Group.
- **2026-04-17 (session 21):** Shipped Phase 7 — Active Listings Pipeline. Added `status` column + 6 new attribute columns to `maine_transactions`, new `maine_listing_history` table for change-detected snapshots, daily cron for `mls_status=Active` scraping, withdrawn-sweeper (7-day stale threshold), anomaly detector (zero-delta days → failure alert), `--max-credits` budget cap, and `src/maine_active.py` read helpers (4 functions) for downstream tools. Closed-side queries now filter on `status='Closed'` so actives/pending/withdrawn don't pollute leaderboards. 291 tests passing (232 baseline + 59 new). Branch `feature/maine-active-listings` → PR.
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

## Workspace Atlas

This repo is one of ~30 projects in Lucas Knowles's SCMaine workspace. The
workspace atlas — `~/atlas` locally, https://github.com/PMTinkerer/atlas —
is the source of truth for what exists, where features live, and the
cross-project rules. Consult it before building anything that might already
exist elsewhere.

- Cold start: `~/atlas/ATLAS.md`
- This project's card: `~/atlas/projects/gw-re-agent-scraper.md`
- "Where does X already live": `~/atlas/capabilities.md`
- External-service patterns: `~/atlas/integrations.md`

Rules that matter most in this repo:

- Pipeline scraping: playwright==1.58.0 + playwright-stealth, fresh context per page, resource blocking, 10-30s jittered delays.
- Pin every dependency exactly; SHA-pin GitHub Actions; no emojis anywhere; secrets never in code (~/.env or project .env).

After shipping a change here, update the project's card and its
`last_verified` date in the atlas (see `~/atlas/protocol/UPDATE.md`).
