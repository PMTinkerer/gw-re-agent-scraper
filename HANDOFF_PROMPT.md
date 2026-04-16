# Handoff Prompt for New Claude Session

---

Copy everything between the `=== BEGIN ===` and `=== END ===` markers below into your new Claude session. It has full context on what's been done and what needs to happen next.

---

## === BEGIN ===

I need you to continue work on my real estate agent scraper project at `/Users/lucasknowles/gw-re-agent-scraper`.

## What's Already Built (Full Context)

This project tracks real estate transactions and agents across 10 southern coastal Maine towns (Kittery, York, Ogunquit, Wells, Kennebunk, Kennebunkport, Biddeford, Saco, Old Orchard Beach, Scarborough). Read `CLAUDE.md` and `AGENTS.md` in the project root for complete background.

**Quick summary of pipelines:**
1. **Redfin** (Playwright): 2,398 transactions with listing agent names. Running 4x/day via GitHub Actions. Only captures listing agent, not buyer agent. Limited to ~350 records per town due to CSV API cap.
2. **Zillow** (Firecrawl): 740 agents with directory rankings. 683 enriched with career stats + 5 most recent sold transactions (buyer/seller split visible). Full pagination of sold history not possible due to React re-render timing.
3. **Maine Listings (MREIS MLS)** (Firecrawl): **PRIMARY SOURCE GOING FORWARD.** 10,587 closed listings discovered (Phase 1 done). This source uniquely provides BOTH listing agent AND buyer agent on every transaction directly from MLS data. Phase 2 enrichment (the part that extracts agent names from detail pages) has NOT run yet.

## Your Immediate Task

I just upgraded my Firecrawl plan to the **Standard tier** ($99/month, 100K credits/month, 50 concurrent requests). I want you to:

### Step 1: Refactor `src/maine_firecrawl.py::enrich_listings` for concurrent execution

**Current state:** The function is serial with a 6-second delay between requests. For 10,587 listings, that's ~18 hours.

**What to build:**
- Use `concurrent.futures.ThreadPoolExecutor` with a configurable number of workers (default 20, max 50)
- Add a `--workers N` flag to `src/maine_main.py`
- Drop the rate limit from 6s → 1-2s between request launches (Firecrawl's own rate limiting will handle pacing)
- Add a **circuit breaker**: if 5 consecutive requests fail, pause the pool for 30 seconds and alert via log. If 20 total failures in a batch, kill the run.
- Add progress logging: every 50 listings or every 60 seconds, log `[N/total] enriched, M failures, X credits used`
- Keep existing serial mode as fallback (`--workers 1` should still work)
- SQLite writes need to be thread-safe. The DB already uses WAL mode (from `maine_database.py::get_connection`) so concurrent readers + 1 writer is fine, but wrap UPDATE statements in try/except for any lock contention and retry with small backoff.

**Files you'll need to modify:**
- `src/maine_firecrawl.py` — refactor `enrich_listings()` function
- `src/maine_main.py` — add `--workers` CLI flag
- Consider whether to add tests — existing test suite is minimal for maine pipeline

### Step 2: Test on a small batch first

Before running the full 10,587:
```bash
# Test with 10 workers, 50 listings
python3 -m src.maine_main --enrich --batch-size 50 --workers 10
```

Verify:
- All 50 complete successfully (or document failures)
- Both `listing_agent` and `buyer_agent` populated
- No database corruption or lock errors
- Progress logs are readable

### Step 3: Run the full Phase 2 enrichment

Once small batch passes:
```bash
export FIRECRAWL_API_KEY=fc-4a9c5da66d8b4eacb4b6b453b3497941
nohup python3 -m src.maine_main --enrich --batch-size 11000 --workers 25 >> data/maine_enrichment_log.txt 2>&1 &
```

Monitor with:
```bash
tail -f data/maine_enrichment_log.txt
sqlite3 data/maine_listings.db "SELECT enrichment_status, COUNT(*) FROM maine_transactions GROUP BY enrichment_status;"
```

Expected: ~1 hour to complete all 10,587 at 25 concurrent workers. Cost: ~10,587 Firecrawl credits (well within Standard plan's 100K monthly budget).

### Step 4: After enrichment completes

1. **Build `src/maine_report.py`**: Generate leaderboards from the Maine data:
   - Listing agent leaderboard (who sells the most, by town, volume)
   - Buyer agent leaderboard (separate — this is unique to Maine source!)
   - Combined total sides leaderboard
   - Brokerage leaderboard (both listing office and buyer office aggregated)
   - Per-town breakdowns
   - Include dollar volume columns

2. **Integrate into `src/index_page.py`**: Add a "Maine MLS" tab to `data/index.html`. Include Maine data in the agent search index so searching an agent shows their Maine transactions alongside Zillow/Redfin data.

3. **Create weekly incremental workflow**: Add `.github/workflows/maine_listings.yml` that runs weekly:
   ```
   python3 -m src.maine_main --discover --recent-only --enrich --batch-size 200 --workers 10
   ```
   The `--recent-only` flag stops discovery when hitting known listings, so weekly runs only process new closings. Expected cost: ~50-100 credits/week (fits Hobby tier).

4. **Commit and push** after each meaningful chunk of work. User pushes to main, GitHub Pages auto-deploys at `https://pmtinkerer.github.io/gw-re-agent-scraper/`.

## Critical Technical Details to Know

### Maine Listings scraping specifics (from `src/maine_parser.py`)

The detail page extraction JS handles a subtle issue: the NUXT data has TWO `list_agent` objects. The first is `co_list_agent` (usually minified `a` = null), the second has real data. The parser iterates through all matches and picks the one with quoted string values. This is already implemented but understand it if debugging.

### Concurrent database writes

SQLite with WAL mode (which we use) supports concurrent readers + 1 writer. For 25 concurrent enrichment workers:
- Each worker reads one row (gets detail_url to process)
- Each worker makes a Firecrawl request (most of the time spent here)
- Each worker updates one row when done
- Lock contention is minimal because updates are fast and writes are serialized naturally by SQLite

If you see `database is locked` errors, wrap the UPDATE in a retry loop with `time.sleep(random.uniform(0.1, 0.5))`.

### Firecrawl SDK notes

- Already installed: `firecrawl-py==4.22.1`
- The SDK's `scrape()` method is the one that bypasses Zillow's PerimeterX. `interact()` and `browser()` get blocked.
- Required args for Maine detail pages: `formats=['rawHtml']`, `wait_for=8000`, `actions=[wait 5000ms, executeJavascript script]`
- The JS extract script is `DETAIL_EXTRACT_JS` in `src/maine_parser.py`

### What NOT to touch

- Don't modify the Redfin pipeline (it's running fine 4x/day)
- Don't modify the Zillow pipeline (enrichment is complete)
- Don't touch `.github/workflows/scrape_agents.yml` or `zillow_leaderboard.yml`
- Keep office branches SEPARATE (user requirement — e.g., "Coldwell Banker Yorke Realty" ≠ "Coldwell Banker Realty")
- The `FIRECRAWL_API_KEY` is already in `.env` (gitignored). The MCP config also has it. For scripts, export it explicitly.

## Environment

- Python 3.9, tests via `pytest`
- Working on `main` branch (the Zillow feature branch was merged earlier)
- GitHub Pages auto-deploys from `main`
- Public repo: `https://github.com/PMTinkerer/gw-re-agent-scraper`

## Verification Commands

```bash
# Check pipeline status
sqlite3 data/maine_listings.db "SELECT COUNT(*) total, SUM(CASE WHEN enrichment_status='success' THEN 1 ELSE 0 END) enriched FROM maine_transactions;"

# Sample the enriched data
sqlite3 data/maine_listings.db "SELECT listing_agent, buyer_agent, sale_price, close_date, city FROM maine_transactions WHERE enrichment_status='success' ORDER BY close_date DESC LIMIT 10;"

# Check credits
firecrawl --status | grep Credits

# Run tests
python3 -m pytest tests/ -v
```

## Approach

1. First, carefully read `src/maine_firecrawl.py`, `src/maine_main.py`, and `src/maine_database.py` to understand the existing structure
2. Propose your concurrent refactor approach before implementing (use plan mode if non-trivial)
3. Implement, test on small batch, then run the full backfill
4. After backfill completes, build the report module and dashboard integration
5. Commit incrementally with clear messages

Start by reading `CLAUDE.md` and `AGENTS.md`, then dive into the code.

## === END ===

---

## Notes for Lucas (you, the human)

**Before you paste the prompt above into a new Claude session, do these things:**

1. **Upgrade Firecrawl to Standard plan** at https://www.firecrawl.dev/pricing (if you haven't already)
2. **Verify the upgrade is active** by running `firecrawl --status` — should show ~100K credits, not 3K
3. **Make sure the current enrichment process is killed** if it's still running:
   ```bash
   ps aux | grep maine_main | grep -v grep
   # Kill any running process with: kill <PID>
   ```
4. **Pull the latest code** in the new session's first action:
   ```bash
   cd /Users/lucasknowles/gw-re-agent-scraper && git pull origin main
   ```

**Expected outcome after the handoff session:**
- All 10,587 Maine Listings enriched with both listing and buyer agent names
- New leaderboards showing listing sides + buyer sides + total volume
- Updated dashboard at `https://pmtinkerer.github.io/gw-re-agent-scraper/` with Maine MLS tab
- Weekly GitHub Actions workflow for incremental updates
- Total Firecrawl credits used: ~11K of 100K

**After the one-time backfill, you can downgrade back to Firecrawl Hobby tier** — weekly incremental only needs ~50-100 credits/week which fits comfortably in 3K/month.
