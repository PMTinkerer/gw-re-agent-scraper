# Maine Listings — Concurrent Enrichment Design

**Date:** 2026-04-16
**Owner:** Lucas Knowles
**Scope:** HANDOFF_PROMPT.md Steps 1–3 (refactor + smoke test + full backfill)
**Out of scope (separate plans later):** `maine_report.py`, `index_page.py` integration, weekly GitHub Actions workflow.

---

## 1. Goal

Refactor `src/maine_firecrawl.py::enrich_listings` so Phase 2 enrichment of the 10,587 discovered Maine Listings closed transactions finishes in ~1 hour instead of ~18 hours, using the upgraded Firecrawl Standard plan (100K credits / 50 concurrent requests). Listing **and** buyer agent names must end up populated on every successfully enriched row.

## 2. Design decisions (locked in brainstorming)

| Decision | Choice |
|---|---|
| Scope | Steps 1–3 of HANDOFF_PROMPT.md |
| DB concurrency model | Thread-local SQLite connection per worker; WAL serializes writes; retry on `database is locked` |
| Test coverage | Unit tests for circuit breaker + retry helper only; 50-listing smoke run is the integration test |
| Refactor structure | Internal `EnrichmentRunner` class inside `src/maine_firecrawl.py`; no new module |

## 3. Module changes (no new files)

### `src/maine_firecrawl.py`
- Add internal `EnrichmentRunner` class that owns: the `ThreadPoolExecutor`, circuit-breaker state, rate limiter, progress counters, DB path, and a `threading.local()` for per-thread connections.
- `enrich_listings(conn, *, batch_size, max_attempts, workers=20)` becomes a thin wrapper: fetches the pending list, constructs `EnrichmentRunner(db_path=<conn path>, workers=workers)`, calls `runner.run(pending)`, returns the result dict.
- Keep the existing `_scrape_page` logic intact; `EnrichmentRunner._process_one(row)` calls it.
- Return contract preserved and extended: `{'enriched': int, 'failed': int, 'total': int, 'aborted': bool}`. `aborted=True` only when the circuit breaker killed the run.

### `src/maine_database.py`
- Add one helper:
  ```
  execute_with_retry(conn, sql, params=(), *, max_retries=5)
  ```
  Wraps `conn.execute(sql, params); conn.commit()` in a loop that catches `sqlite3.OperationalError` whose message contains `"locked"`, sleeps `random.uniform(0.1, 0.5)` seconds, and retries up to `max_retries` times. After exhausting retries, re-raises.
- Update `enrich_listing()` and `mark_enrichment_failed()` to go through `execute_with_retry`. No schema changes.

### `src/maine_main.py`
- Add CLI flag `--workers N`, default `20`, validated `1 ≤ N ≤ 50`. Reject out-of-range with a clear error.
- Pass `workers=args.workers` through to `enrich_listings(...)`.
- Log the effective worker count at the start of enrichment.

## 4. `EnrichmentRunner` internals

### Fields
- `_db_path: str` — resolved from the caller's `conn` (via `conn.execute("PRAGMA database_list").fetchone()['file']`, or accepted as an explicit argument).
- `_workers: int`
- `_tls: threading.local` — stores the per-thread `sqlite3.Connection`.
- `_circuit: CircuitBreaker`
- `_limiter: MinGapLauncher` — constructed with `min_gap_seconds=1.5` by default; exposed to `enrich_listings()` as an optional keyword-only argument `min_gap_seconds` for tuning without a CLI flag.
- `_progress: ProgressCounter`

### Methods
- `run(pending: list[dict]) -> dict`
  1. If `not pending`: log and return zeros.
  2. Build `ThreadPoolExecutor(max_workers=self._workers)`.
  3. `futures = {executor.submit(self._process_one, row): row for row in pending}`.
  4. For each `future` in `as_completed(futures)`:
     - If `self._circuit.aborted`: cancel remaining futures, break.
     - Unpack `WorkerResult` → update progress counters, log if due.
  5. Close all thread-local connections (iterate `threading.enumerate()` on pool threads, or register `atexit`-style cleanup via a per-thread `weakref.finalize`).
  6. Return `{'enriched', 'failed', 'total', 'aborted'}`.
- `_process_one(row) -> WorkerResult`
  1. `self._limiter.acquire()` — enforce min-gap between request launches.
  2. `self._circuit.wait_if_paused()` — block if the pool is in a pause window.
  3. Make **one Firecrawl client per worker thread** (Firecrawl SDK is not documented as thread-safe; clients are cheap). Cache on `self._tls.client` on first use; reuse for the life of the thread.
  4. Scrape via the existing `_scrape_page` logic.
  5. Parse via `parse_detail_response`.
  6. On success: call `enrich_listing(self._get_conn(), url, data)`; `self._circuit.record_success()`; return `WorkerResult.success(...)`.
  7. On any exception: call `mark_enrichment_failed(self._get_conn(), url, str(exc)[:200])`; `self._circuit.record_failure()`; return `WorkerResult.failure(...)`.
- `_get_conn() -> sqlite3.Connection`
  - If `self._tls.conn` unset: open `get_connection(self._db_path)` (WAL pragma applied automatically) and cache.
  - Return `self._tls.conn`.

### `WorkerResult`
Simple `@dataclass(frozen=True)`:
```
status: Literal['enriched', 'failed', 'skipped']
url: str
error: str | None = None
```

## 5. Circuit breaker

Handoff requirement: **5 consecutive failures → pause pool for 30 s + log alert; 20 total failures → kill the run.**

### State
- `_consecutive: int = 0`
- `_total: int = 0`
- `_paused_until: float = 0.0`  (`time.monotonic()` epoch)
- `_aborted: bool = False`
- `_lock: threading.Lock`
- Constants: `CONSECUTIVE_THRESHOLD = 5`, `TOTAL_THRESHOLD = 20`, `PAUSE_SECONDS = 30`.

### Methods (all acquire `_lock` briefly)
- `wait_if_paused()` — while `now < _paused_until and not _aborted`, sleep `0.5 s` and re-check.
- `record_success()` — `_consecutive = 0`.
- `record_failure()` — `_consecutive += 1; _total += 1`.
  - If `_total >= TOTAL_THRESHOLD`: `_aborted = True`, log error `"circuit breaker aborted run after N total failures"`.
  - Elif `_consecutive >= CONSECUTIVE_THRESHOLD`: `_paused_until = time.monotonic() + PAUSE_SECONDS`, `_consecutive = 0`, log warning `"circuit breaker paused pool for 30 s after 5 consecutive failures"`.
- Property `aborted` → `_aborted`.

### Why this is correct
- Lock scope is tiny (updates to a handful of ints); no deadlock risk with `_lock` held while calling `time.monotonic()`.
- Workers observe abort via `aborted` property after each completion in the main loop; pending in-flight futures can finish naturally (we don't force-cancel in-flight work, only pending submissions).
- Resetting `_consecutive` after triggering a pause prevents re-triggering on the very next failure after resume.

## 6. Rate limiter (`MinGapLauncher`)

Handoff asks for **1–2 s between request launches**. Using **1.5 s default**, configurable via a runner argument.

### Implementation
```
class MinGapLauncher:
    def __init__(self, min_gap_seconds: float = 1.5):
        self._gap = min_gap_seconds
        self._last_launch = 0.0
        self._lock = threading.Lock()

    def acquire(self):
        with self._lock:
            now = time.monotonic()
            wait = self._gap - (now - self._last_launch)
            if wait > 0:
                time.sleep(wait)
                now = time.monotonic()
            self._last_launch = now
```

Holding the lock across `sleep` serializes launches fairly — in practice the pool naturally interleaves Firecrawl requests (which take multiple seconds each) so this doesn't collapse concurrency. Sleep-while-holding is intentional to keep the implementation tiny and correct under contention.

## 7. Progress reporting

No dedicated reporter thread. On each completion in the main loop (inside `as_completed`):

- `_progress.record(result)` increments counters under a small lock.
- Log a line when either:
  - `processed % 50 == 0`, **or**
  - `time.monotonic() - _last_log >= 60`.

Log format:
```
[1234/10587] enriched=1189, failed=45, remaining=9353 (3m22s since start)
```
No Firecrawl-credit counter (the SDK doesn't surface per-call cost in the response; users check credits manually via the handoff's `firecrawl --status` command).

## 8. Thread-local connection cleanup

- Workers only need their connection for the duration of the batch.
- After `as_completed` drains, the executor shuts down and its threads exit — Python closes the `threading.local()` per-thread storage and SQLite `Connection.__del__` closes the FD.
- Explicit cleanup not required for correctness, but we will register a `weakref.finalize(connection, connection.close)` when first creating the thread-local conn to guarantee close-on-GC even if the runner is used unusually.

## 9. Tests

New file: `tests/test_maine_enrichment_runner.py`

### `CircuitBreaker`
- `test_initial_state_not_paused_not_aborted`
- `test_record_success_resets_consecutive`
- `test_five_consecutive_failures_triggers_pause`
- `test_wait_if_paused_sleeps_until_deadline` (monkeypatch `time.monotonic` + `time.sleep`)
- `test_twenty_total_failures_aborts` (includes a mix of successes and failures to verify total counter is not reset by success)

### `execute_with_retry`
- `test_success_first_try_no_retry`
- `test_retries_on_database_locked_then_succeeds` (use a mock connection whose `execute` raises `OperationalError("database is locked")` twice, then succeeds)
- `test_gives_up_after_max_retries` (all attempts raise; assert final exception is raised)
- `test_non_locked_operational_error_propagates_immediately` (e.g. `OperationalError("no such table")` should NOT retry)

### What is intentionally **not** tested
- Full concurrent loop against a fake Firecrawl — we chose to rely on the 50-listing live smoke run for that (per brainstorming decision).
- Rate-limiter timing (wall-clock–dependent and implementation is trivial).
- Progress-logger formatting (cosmetic).

## 10. Small-batch smoke test (Step 2)

```
python3 -m src.maine_main --enrich --batch-size 50 --workers 10
```

Pass criteria:
1. Script exits 0.
2. `sqlite3 data/maine_listings.db "SELECT COUNT(*) FROM maine_transactions WHERE enrichment_status='success' AND enriched_at > '2026-04-16';"` ≥ 45 (allow up to 5 Firecrawl misses).
3. On a 10-row sample: `listing_agent IS NOT NULL AND buyer_agent IS NOT NULL` for at least 8/10.
4. No `database is locked` errors observed in the log (retries are allowed but the helper must resolve them).
5. Progress lines appear at `[50/50]` at minimum; earlier periodic lines preferred.

If the smoke test fails criteria 2–4, fix and re-run before the full backfill.

## 11. Full backfill (Step 3)

```
export FIRECRAWL_API_KEY=$(grep ^FIRECRAWL_API_KEY .env | cut -d= -f2)
nohup python3 -m src.maine_main --enrich --batch-size 11000 --workers 25 \
  >> data/maine_enrichment_log.txt 2>&1 &
echo "pid=$!"
```

Monitoring:
```
tail -f data/maine_enrichment_log.txt
sqlite3 data/maine_listings.db "SELECT enrichment_status, COUNT(*) FROM maine_transactions GROUP BY enrichment_status;"
```

Expected: ~60 min, ~11K credits, ≥ 95% success rate. Any remaining `error` rows will be retried automatically on the next run (the `enrichment_attempts < max_attempts` guard handles re-queueing; default `max_attempts=2`).

## 12. Rollback

No schema changes, no destructive operations. If the concurrent path misbehaves:
- Kill the background process (`kill <pid>`).
- Re-run with `--workers 1` to get serial behavior through the same code path (one thread, one connection, same rate limiter — effectively the old behavior).
- If a bad Firecrawl batch produced garbage writes, re-run with `--max-attempts 3` after manually resetting `enrichment_status` on affected rows via SQLite.

## 13. Risk log

| Risk | Mitigation |
|---|---|
| Firecrawl Standard plan rate limits tighter than 50 concurrent in practice | Default workers=20 (below plan max); circuit breaker catches sustained error bursts; can dial down `--workers` on rerun. |
| `database is locked` under 25 concurrent writers | Per-worker connection + WAL + retry helper with jittered backoff. |
| Thread-local connection leaks | `weakref.finalize(conn, conn.close)` on creation; executor shutdown terminates threads. |
| NUXT parser mis-match on detail pages from new MLS template | Existing parser handles the known double-`list_agent` blob; if a wave of `parse_failed` results appears, circuit breaker will not trigger (those are counted as failures, so it actually will — and that's fine: it stops us from burning credits). |
| Stdout-buffered progress logs don't appear under `nohup` | Python's `logging.StreamHandler` flushes per record, so the default config in `maine_main.py` is fine. If a wrapper shell ever stops flushing, run with `python3 -u`. |

## 14. Out-of-scope follow-ups

- `src/maine_report.py` — listing-agent, buyer-agent, combined-sides, brokerage leaderboards; per-town breakdowns; dollar volume columns.
- `src/index_page.py` — "Maine MLS" tab; include Maine rows in the agent search index.
- `.github/workflows/maine_listings.yml` — weekly incremental: `--discover --recent-only --enrich --batch-size 200 --workers 10`.

Each gets its own spec/plan cycle after the backfill data lands.
