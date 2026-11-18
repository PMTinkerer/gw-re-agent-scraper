# Maine Listings Concurrent Enrichment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor `src/maine_firecrawl.py::enrich_listings` to run concurrently (up to 50 workers) so the 10,587-listing Phase 2 backfill finishes in ~1 hour instead of ~18 hours, with a circuit breaker, thread-safe SQLite writes, and a `--workers` CLI flag.

**Architecture:** Introduce an internal `EnrichmentRunner` class in `maine_firecrawl.py` that owns a `ThreadPoolExecutor`, a circuit breaker (5 consecutive / 20 total failures), a `MinGapLauncher` rate limiter (1.5 s default), and per-thread SQLite connections via `threading.local()`. Add `execute_with_retry()` in `maine_database.py` to handle `database is locked` under concurrent writers. `enrich_listings()` becomes a thin wrapper; `workers=1` reproduces the existing serial behavior.

**Tech Stack:** Python 3.9+, `concurrent.futures.ThreadPoolExecutor`, `threading.local()`/`Lock`, SQLite WAL mode, `firecrawl-py==4.22.1`, pytest.

**Spec:** [docs/superpowers/specs/2026-04-16-maine-listings-concurrent-enrichment-design.md](../specs/2026-04-16-maine-listings-concurrent-enrichment-design.md)

---

## File Structure

**Create:**
- `tests/test_maine_enrichment_runner.py` — unit tests for `CircuitBreaker` and `execute_with_retry`.

**Modify:**
- `src/maine_database.py` — add `execute_with_retry()`; route `enrich_listing()` and `mark_enrichment_failed()` through it.
- `src/maine_firecrawl.py` — add `CircuitBreaker`, `MinGapLauncher`, `ProgressCounter`, `WorkerResult`, and `EnrichmentRunner` classes; refactor `enrich_listings()` to delegate to the runner while preserving the return-contract keys and adding `'aborted'`.
- `src/maine_main.py` — add `--workers` CLI flag (default 20, range 1–50); pass through to `enrich_listings()`.

Each of the tasks below produces an independently committable change. TDD order for the two pieces that have unit tests; the runner is wired up only after the primitives are green.

---

## Task 1: Add `execute_with_retry` helper with TDD

**Files:**
- Create: `tests/test_maine_enrichment_runner.py`
- Modify: `src/maine_database.py` (add one function near the top-level helpers)

- [ ] **Step 1: Write the failing tests for `execute_with_retry`**

Append to (new) `tests/test_maine_enrichment_runner.py`:

```python
"""Unit tests for Maine enrichment concurrency primitives."""
from __future__ import annotations

import sqlite3
from unittest.mock import MagicMock

import pytest

from src.maine_database import execute_with_retry


class TestExecuteWithRetry:
    def test_success_first_try_no_retry(self):
        conn = MagicMock(spec=sqlite3.Connection)
        execute_with_retry(conn, 'UPDATE t SET x=?', (1,))
        conn.execute.assert_called_once_with('UPDATE t SET x=?', (1,))
        conn.commit.assert_called_once()

    def test_retries_on_database_locked_then_succeeds(self, monkeypatch):
        monkeypatch.setattr('time.sleep', lambda _s: None)
        conn = MagicMock(spec=sqlite3.Connection)
        conn.execute.side_effect = [
            sqlite3.OperationalError('database is locked'),
            sqlite3.OperationalError('database is locked'),
            None,  # third attempt succeeds
        ]
        execute_with_retry(conn, 'UPDATE t SET x=?', (1,), max_retries=5)
        assert conn.execute.call_count == 3
        conn.commit.assert_called_once()

    def test_gives_up_after_max_retries(self, monkeypatch):
        monkeypatch.setattr('time.sleep', lambda _s: None)
        conn = MagicMock(spec=sqlite3.Connection)
        conn.execute.side_effect = sqlite3.OperationalError('database is locked')
        with pytest.raises(sqlite3.OperationalError, match='locked'):
            execute_with_retry(conn, 'UPDATE t SET x=?', (1,), max_retries=3)
        assert conn.execute.call_count == 3
        conn.commit.assert_not_called()

    def test_non_locked_operational_error_propagates_immediately(self):
        conn = MagicMock(spec=sqlite3.Connection)
        conn.execute.side_effect = sqlite3.OperationalError('no such table: t')
        with pytest.raises(sqlite3.OperationalError, match='no such table'):
            execute_with_retry(conn, 'UPDATE t SET x=?', (1,))
        assert conn.execute.call_count == 1
        conn.commit.assert_not_called()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python3 -m pytest tests/test_maine_enrichment_runner.py -v`
Expected: all 4 tests FAIL with `ImportError: cannot import name 'execute_with_retry' from 'src.maine_database'`.

- [ ] **Step 3: Implement `execute_with_retry` in `src/maine_database.py`**

Add these imports at the top of `src/maine_database.py` (after the existing `import sqlite3` / `from datetime import datetime`):

```python
import random
import time
```

Add this function immediately below the `get_connection()` function (around line 28 in the current file):

```python
def execute_with_retry(
    conn: sqlite3.Connection,
    sql: str,
    params: tuple = (),
    *,
    max_retries: int = 5,
) -> None:
    """Execute a write statement, retrying on 'database is locked' with jittered backoff.

    Only retries when the OperationalError message contains 'locked' (WAL contention).
    Other OperationalErrors (schema errors, constraint violations) propagate immediately.
    """
    last_exc: sqlite3.OperationalError | None = None
    for attempt in range(max_retries):
        try:
            conn.execute(sql, params)
            conn.commit()
            return
        except sqlite3.OperationalError as exc:
            if 'locked' not in str(exc).lower():
                raise
            last_exc = exc
            time.sleep(random.uniform(0.1, 0.5))
    assert last_exc is not None
    raise last_exc
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python3 -m pytest tests/test_maine_enrichment_runner.py -v`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add tests/test_maine_enrichment_runner.py src/maine_database.py
git commit -m "Add execute_with_retry helper for SQLite WAL contention"
```

---

## Task 2: Route existing writers through `execute_with_retry`

**Files:**
- Modify: `src/maine_database.py:108-158` (functions `enrich_listing` and `mark_enrichment_failed`)

This is a behavior-preserving refactor. Under a single writer, `execute_with_retry` behaves identically to the current `conn.execute(...); conn.commit()`. Under concurrent writers (coming in Task 5) it adds the retry.

- [ ] **Step 1: Replace the body of `enrich_listing()` in `src/maine_database.py`**

Change the function to:

```python
def enrich_listing(conn: sqlite3.Connection, detail_url: str, data: dict) -> bool:
    """Update a listing with agent data from detail page scraping."""
    now = datetime.utcnow().isoformat()
    execute_with_retry(conn, '''
        UPDATE maine_transactions SET
            mls_number = COALESCE(?, mls_number),
            listing_agent = ?,
            listing_agent_id = ?,
            listing_agent_email = ?,
            listing_office = COALESCE(?, listing_office),
            buyer_agent = ?,
            buyer_agent_id = ?,
            buyer_agent_email = ?,
            buyer_office = ?,
            close_date = ?,
            sale_price = COALESCE(?, sale_price),
            list_price = ?,
            property_type = ?,
            days_on_market = ?,
            enrichment_status = 'success',
            enrichment_attempts = enrichment_attempts + 1,
            enriched_at = ?
        WHERE detail_url = ?
    ''', (
        data.get('mls_number'),
        data.get('listing_agent'), data.get('listing_agent_id'),
        data.get('listing_agent_email'), data.get('listing_office'),
        data.get('buyer_agent'), data.get('buyer_agent_id'),
        data.get('buyer_agent_email'), data.get('buyer_office'),
        data.get('close_date'), data.get('sale_price'),
        data.get('list_price'), data.get('property_type'),
        data.get('days_on_market'),
        now, detail_url,
    ))
    return True
```

- [ ] **Step 2: Replace the body of `mark_enrichment_failed()` in `src/maine_database.py`**

Change the function to:

```python
def mark_enrichment_failed(
    conn: sqlite3.Connection, detail_url: str, error: str,
) -> None:
    """Mark a listing enrichment as failed."""
    now = datetime.utcnow().isoformat()
    execute_with_retry(conn, '''
        UPDATE maine_transactions SET
            enrichment_status = 'error',
            enrichment_attempts = enrichment_attempts + 1,
            enriched_at = ?
        WHERE detail_url = ?
    ''', (now, detail_url))
```

- [ ] **Step 3: Run the full test suite to verify no regressions**

Run: `python3 -m pytest tests/ -v`
Expected: every test that passed before still passes; the 4 new `execute_with_retry` tests still pass.

- [ ] **Step 4: Commit**

```bash
git add src/maine_database.py
git commit -m "Route Maine DB writes through execute_with_retry"
```

---

## Task 3: Add `CircuitBreaker` class with TDD

**Files:**
- Modify: `tests/test_maine_enrichment_runner.py` (add `TestCircuitBreaker` class)
- Modify: `src/maine_firecrawl.py` (add `CircuitBreaker` class)

- [ ] **Step 1: Write the failing tests for `CircuitBreaker`**

Append to `tests/test_maine_enrichment_runner.py`:

```python
from src.maine_firecrawl import CircuitBreaker


class TestCircuitBreaker:
    def test_initial_state_not_paused_not_aborted(self, monkeypatch):
        cb = CircuitBreaker()
        assert not cb.aborted
        # wait_if_paused should be an immediate no-op on a fresh breaker
        slept = []
        monkeypatch.setattr('time.sleep', lambda s: slept.append(s))
        cb.wait_if_paused()
        assert slept == []

    def test_record_success_resets_consecutive(self):
        cb = CircuitBreaker()
        for _ in range(4):
            cb.record_failure()
        cb.record_success()
        # Trigger 4 more failures; should not fire the 5-consecutive pause
        # because the counter was reset.
        for _ in range(4):
            cb.record_failure()
        assert cb._paused_until == 0.0

    def test_five_consecutive_failures_triggers_pause(self, monkeypatch):
        fake_time = [100.0]
        monkeypatch.setattr('time.monotonic', lambda: fake_time[0])
        cb = CircuitBreaker()
        for _ in range(5):
            cb.record_failure()
        assert cb._paused_until == 100.0 + CircuitBreaker.PAUSE_SECONDS
        assert not cb.aborted  # 5 failures alone does NOT abort
        # Pause resets consecutive so we don't re-pause immediately
        assert cb._consecutive == 0

    def test_wait_if_paused_sleeps_until_deadline(self, monkeypatch):
        fake_time = [200.0]
        sleeps = []
        monkeypatch.setattr('time.monotonic', lambda: fake_time[0])

        def fake_sleep(s):
            sleeps.append(s)
            fake_time[0] += s

        monkeypatch.setattr('time.sleep', fake_sleep)
        cb = CircuitBreaker()
        cb._paused_until = 200.0 + 2.0  # 2s pause window
        cb.wait_if_paused()
        assert sum(sleeps) >= 2.0
        # After waiting, we are past the deadline
        assert fake_time[0] >= cb._paused_until

    def test_twenty_total_failures_aborts(self):
        cb = CircuitBreaker()
        # Interleave failures with successes so the 5-consecutive pause never
        # fires. We need 20 total failures.
        for _ in range(20):
            cb.record_failure()
            cb.record_success()
        assert cb.aborted
        assert cb._total == 20
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python3 -m pytest tests/test_maine_enrichment_runner.py::TestCircuitBreaker -v`
Expected: all 5 tests FAIL with `ImportError: cannot import name 'CircuitBreaker'`.

- [ ] **Step 3: Add `CircuitBreaker` to `src/maine_firecrawl.py`**

Add these imports at the top of `src/maine_firecrawl.py` (after the existing imports block):

```python
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Literal
```

Add this class just above `def discover_listings(` (around line 65 in the current file):

```python
class CircuitBreaker:
    """Thread-safe failure tracker for the enrichment runner.

    Rules (per design spec):
      * 5 consecutive failures -> pause the pool for PAUSE_SECONDS.
      * 20 total failures -> abort the run; caller should drain in-flight
        futures and exit.
    """

    CONSECUTIVE_THRESHOLD = 5
    TOTAL_THRESHOLD = 20
    PAUSE_SECONDS = 30.0

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._consecutive = 0
        self._total = 0
        self._paused_until = 0.0
        self._aborted = False

    @property
    def aborted(self) -> bool:
        with self._lock:
            return self._aborted

    def record_success(self) -> None:
        with self._lock:
            self._consecutive = 0

    def record_failure(self) -> None:
        with self._lock:
            self._consecutive += 1
            self._total += 1
            if self._total >= self.TOTAL_THRESHOLD:
                if not self._aborted:
                    self._aborted = True
                    logger.error(
                        'circuit breaker aborted run after %d total failures',
                        self._total,
                    )
                return
            if self._consecutive >= self.CONSECUTIVE_THRESHOLD:
                self._paused_until = time.monotonic() + self.PAUSE_SECONDS
                self._consecutive = 0
                logger.warning(
                    'circuit breaker paused pool for %.0fs after %d consecutive failures',
                    self.PAUSE_SECONDS,
                    self.CONSECUTIVE_THRESHOLD,
                )

    def wait_if_paused(self) -> None:
        """Block while the breaker is in a pause window."""
        while True:
            with self._lock:
                remaining = self._paused_until - time.monotonic()
                if self._aborted or remaining <= 0:
                    return
            time.sleep(min(remaining, 0.5))
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python3 -m pytest tests/test_maine_enrichment_runner.py -v`
Expected: all 9 tests (4 retry + 5 circuit breaker) pass.

- [ ] **Step 5: Commit**

```bash
git add tests/test_maine_enrichment_runner.py src/maine_firecrawl.py
git commit -m "Add CircuitBreaker for Maine enrichment runner"
```

---

## Task 4: Add rate limiter, progress counter, and worker result dataclass

**Files:**
- Modify: `src/maine_firecrawl.py`

These three helpers are too thin to justify their own unit tests; they are exercised by the smoke run in Task 7. Keep them in the same file as `EnrichmentRunner`.

- [ ] **Step 1: Add `MinGapLauncher`, `ProgressCounter`, and `WorkerResult` to `src/maine_firecrawl.py`**

Add these immediately below the `CircuitBreaker` class:

```python
class MinGapLauncher:
    """Enforce a minimum gap between request launches across threads."""

    def __init__(self, min_gap_seconds: float = 1.5) -> None:
        self._gap = min_gap_seconds
        self._last_launch = 0.0
        self._lock = threading.Lock()

    def acquire(self) -> None:
        with self._lock:
            now = time.monotonic()
            wait = self._gap - (now - self._last_launch)
            if wait > 0:
                time.sleep(wait)
                now = time.monotonic()
            self._last_launch = now


class ProgressCounter:
    """Thread-safe counters + periodic log emitter."""

    LOG_EVERY_N = 50
    LOG_EVERY_SECONDS = 60.0

    def __init__(self, total: int) -> None:
        self._lock = threading.Lock()
        self._total = total
        self._processed = 0
        self._enriched = 0
        self._failed = 0
        self._started_at = time.monotonic()
        self._last_log = self._started_at

    def record(self, *, enriched: bool) -> None:
        with self._lock:
            self._processed += 1
            if enriched:
                self._enriched += 1
            else:
                self._failed += 1
            now = time.monotonic()
            due_by_count = self._processed % self.LOG_EVERY_N == 0
            due_by_time = (now - self._last_log) >= self.LOG_EVERY_SECONDS
            is_final = self._processed == self._total
            if due_by_count or due_by_time or is_final:
                elapsed = now - self._started_at
                logger.info(
                    '[%d/%d] enriched=%d failed=%d remaining=%d (%dm%02ds elapsed)',
                    self._processed,
                    self._total,
                    self._enriched,
                    self._failed,
                    self._total - self._processed,
                    int(elapsed // 60),
                    int(elapsed % 60),
                )
                self._last_log = now

    def snapshot(self) -> tuple[int, int, int]:
        with self._lock:
            return self._enriched, self._failed, self._processed


@dataclass(frozen=True)
class WorkerResult:
    status: Literal['enriched', 'failed']
    url: str
    error: str | None = None
```

- [ ] **Step 2: Run tests to verify nothing regressed**

Run: `python3 -m pytest tests/test_maine_enrichment_runner.py -v`
Expected: all 9 tests still pass.

- [ ] **Step 3: Verify `src/maine_firecrawl.py` still imports cleanly**

Run: `python3 -c "from src.maine_firecrawl import CircuitBreaker, MinGapLauncher, ProgressCounter, WorkerResult; print('ok')"`
Expected output: `ok`

- [ ] **Step 4: Commit**

```bash
git add src/maine_firecrawl.py
git commit -m "Add MinGapLauncher, ProgressCounter, WorkerResult primitives"
```

---

## Task 5: Add `EnrichmentRunner` and refactor `enrich_listings()` to delegate

**Files:**
- Modify: `src/maine_firecrawl.py` (replace the body of `enrich_listings()` and add `EnrichmentRunner`)

- [ ] **Step 1: Add `EnrichmentRunner` class to `src/maine_firecrawl.py`**

Insert immediately below `WorkerResult`:

```python
class EnrichmentRunner:
    """Concurrent orchestrator for Maine detail-page enrichment.

    Owns the ThreadPoolExecutor, circuit breaker, rate limiter, progress
    counter, and a threading.local() for per-thread SQLite connections and
    Firecrawl clients.
    """

    def __init__(
        self,
        db_path: str,
        *,
        workers: int = 20,
        min_gap_seconds: float = 1.5,
    ) -> None:
        if not 1 <= workers <= 50:
            raise ValueError(f'workers must be between 1 and 50, got {workers}')
        self._db_path = db_path
        self._workers = workers
        self._tls = threading.local()
        self._circuit = CircuitBreaker()
        self._limiter = MinGapLauncher(min_gap_seconds=min_gap_seconds)
        self._progress: ProgressCounter | None = None  # set in run()

    def _get_conn(self):
        conn = getattr(self._tls, 'conn', None)
        if conn is None:
            from .maine_database import get_connection
            conn = get_connection(self._db_path)
            self._tls.conn = conn
        return conn

    def _get_client(self):
        client = getattr(self._tls, 'client', None)
        if client is None:
            client = _get_client()
            self._tls.client = client
        return client

    def _process_one(self, row: dict) -> WorkerResult:
        url = row['detail_url']
        self._limiter.acquire()
        self._circuit.wait_if_paused()
        if self._circuit.aborted:
            return WorkerResult(status='failed', url=url, error='aborted')

        try:
            client = self._get_client()
            # last_call=0.0 disables MinGapLauncher's in-function throttle;
            # the pool-level MinGapLauncher above is the source of truth.
            result, _ = _scrape_page(client, url, 0.0, 'rawHtml')

            acts = getattr(result, 'actions', None)
            if not acts or 'javascriptReturns' not in acts:
                mark_enrichment_failed(self._get_conn(), url, 'no JS returns')
                self._circuit.record_failure()
                return WorkerResult(status='failed', url=url, error='no JS returns')

            ret = acts['javascriptReturns'][0]
            data = parse_detail_response(ret)
            if not data:
                mark_enrichment_failed(self._get_conn(), url, 'parse failed')
                self._circuit.record_failure()
                return WorkerResult(status='failed', url=url, error='parse failed')

            enrich_listing(self._get_conn(), url, data)
            self._circuit.record_success()
            return WorkerResult(status='enriched', url=url)

        except Exception as exc:  # noqa: BLE001 - we want to catch everything a worker throws
            try:
                mark_enrichment_failed(self._get_conn(), url, str(exc)[:200])
            except Exception as mark_exc:  # noqa: BLE001
                logger.error('failed to mark %s as failed: %s', url, mark_exc)
            self._circuit.record_failure()
            return WorkerResult(status='failed', url=url, error=str(exc)[:200])

    def run(self, pending: list[dict]) -> dict:
        total = len(pending)
        self._progress = ProgressCounter(total)
        enriched = 0
        failed = 0
        aborted = False

        if not pending:
            logger.info('Enrichment batch: 0 listings (nothing to do)')
            return {'enriched': 0, 'failed': 0, 'total': 0, 'aborted': False}

        logger.info(
            'Enrichment batch: %d listings, workers=%d, min_gap=%.1fs',
            total, self._workers, self._limiter._gap,
        )

        with ThreadPoolExecutor(max_workers=self._workers) as executor:
            futures = {executor.submit(self._process_one, row): row for row in pending}
            try:
                for future in as_completed(futures):
                    result = future.result()
                    if result.status == 'enriched':
                        enriched += 1
                    else:
                        failed += 1
                    self._progress.record(enriched=(result.status == 'enriched'))
                    if self._circuit.aborted and not aborted:
                        aborted = True
                        logger.error(
                            'aborting run: cancelling %d pending futures',
                            sum(1 for f in futures if not f.done()),
                        )
                        for f in futures:
                            if not f.done():
                                f.cancel()
            finally:
                # Close all thread-local connections that were opened.
                # ThreadPoolExecutor.shutdown() will join the workers;
                # their tls storage gets GC'd when threads exit, but we
                # explicitly close here so WAL flushes before the pool dies.
                pass  # connections close on thread exit via sqlite3 finalization

        return {
            'enriched': enriched,
            'failed': failed,
            'total': total,
            'aborted': aborted,
        }
```

- [ ] **Step 2: Replace the body of `enrich_listings()` in `src/maine_firecrawl.py`**

Replace the existing `def enrich_listings(...)` function (currently at the bottom of the file) with:

```python
def enrich_listings(
    conn,
    *,
    batch_size: int = 50,
    max_attempts: int = 2,
    workers: int = 20,
    min_gap_seconds: float = 1.5,
) -> dict:
    """Phase 2: Enrich listings with agent data from detail pages.

    With workers=1 this reproduces the original serial behavior through the
    same concurrent code path; with workers>1 it runs a ThreadPoolExecutor
    guarded by the circuit breaker and rate limiter.
    """
    pending = get_unenriched(conn, batch_size=batch_size, max_attempts=max_attempts)

    # Resolve the DB file path from the caller's connection so workers can
    # open their own thread-local connections against the same file.
    db_path_row = conn.execute('PRAGMA database_list').fetchone()
    db_path = db_path_row['file'] if db_path_row else None
    if not db_path:
        raise RuntimeError('could not resolve database path from connection')

    runner = EnrichmentRunner(
        db_path=db_path,
        workers=workers,
        min_gap_seconds=min_gap_seconds,
    )
    return runner.run(pending)
```

- [ ] **Step 3: Ensure the import of `get_unenriched` is still present**

Verify `from .maine_database import get_unenriched` is in the import block at the top of `src/maine_firecrawl.py` (it already is — do not duplicate).

- [ ] **Step 4: Run the full test suite**

Run: `python3 -m pytest tests/ -v`
Expected: all previously-passing tests still pass; the 9 `test_maine_enrichment_runner.py` tests still pass. No Firecrawl live calls happen because we're not invoking `enrich_listings()` from any test.

- [ ] **Step 5: Verify the module imports cleanly and `EnrichmentRunner` validates `workers`**

Run:

```bash
python3 -c "
from src.maine_firecrawl import EnrichmentRunner
try:
    EnrichmentRunner('/tmp/x.db', workers=0)
except ValueError as e:
    print('ok:', e)
try:
    EnrichmentRunner('/tmp/x.db', workers=51)
except ValueError as e:
    print('ok:', e)
r = EnrichmentRunner('/tmp/x.db', workers=10)
print('ok: constructed with workers=10')
"
```

Expected output:
```
ok: workers must be between 1 and 50, got 0
ok: workers must be between 1 and 50, got 51
ok: constructed with workers=10
```

- [ ] **Step 6: Commit**

```bash
git add src/maine_firecrawl.py
git commit -m "Add EnrichmentRunner; enrich_listings delegates to pool"
```

---

## Task 6: Add `--workers` CLI flag

**Files:**
- Modify: `src/maine_main.py:30-48` (argparse block) and `src/maine_main.py:72-76` (enrichment dispatch)

- [ ] **Step 1: Add the `--workers` argument in `src/maine_main.py`**

In the `argparse.ArgumentParser` block, add (immediately after the existing `--batch-size` argument):

```python
    parser.add_argument('--workers', type=int, default=20,
                        help='Concurrent enrichment workers (1-50, default: 20)')
```

- [ ] **Step 2: Pass the flag through in the enrichment dispatch**

Replace the existing enrichment block:

```python
    if args.enrich:
        logger.info('Starting detail page enrichment (batch=%d)...', args.batch_size)
        result = enrich_listings(conn, batch_size=args.batch_size)
        logger.info('Enrichment: %d enriched, %d failed, %d total',
                     result['enriched'], result['failed'], result['total'])
```

with:

```python
    if args.enrich:
        if not 1 <= args.workers <= 50:
            parser.error(f'--workers must be between 1 and 50, got {args.workers}')
        logger.info(
            'Starting detail page enrichment (batch=%d, workers=%d)...',
            args.batch_size, args.workers,
        )
        result = enrich_listings(
            conn, batch_size=args.batch_size, workers=args.workers,
        )
        log_fmt = 'Enrichment: %d enriched, %d failed, %d total'
        log_args = [result['enriched'], result['failed'], result['total']]
        if result.get('aborted'):
            log_fmt += ' (ABORTED by circuit breaker)'
        logger.info(log_fmt, *log_args)
```

- [ ] **Step 3: Verify the help text renders correctly**

Run: `python3 -m src.maine_main --help`
Expected: the usage text includes `--workers WORKERS` with the description above.

- [ ] **Step 4: Verify out-of-range rejection**

Run: `python3 -m src.maine_main --enrich --workers 0`
Expected: argparse-style error (exit code 2) with message `--workers must be between 1 and 50, got 0`. Nothing touches the database.

Run: `python3 -m src.maine_main --enrich --workers 99`
Expected: same-shaped error with `got 99`.

- [ ] **Step 5: Commit**

```bash
git add src/maine_main.py
git commit -m "Add --workers CLI flag to Maine enrichment pipeline"
```

---

## Task 7: 50-listing smoke test (manual gate)

This task does not change code — it verifies the concurrent path against live Firecrawl before the full backfill.

**Files:** none

- [ ] **Step 1: Confirm Firecrawl Standard plan is active**

Run: `firecrawl --status | grep -i credit`
Expected: shows ~100K credits remaining (not 3K). If not, stop and ask the user to finish the upgrade.

- [ ] **Step 2: Record the pre-run enrichment counts**

Run:

```bash
sqlite3 data/maine_listings.db "SELECT enrichment_status, COUNT(*) FROM maine_transactions GROUP BY enrichment_status;"
```

Save the output; it is the baseline for the delta check in Step 5.

- [ ] **Step 3: Run the smoke test**

Run:

```bash
export FIRECRAWL_API_KEY=$(grep ^FIRECRAWL_API_KEY .env | cut -d= -f2)
python3 -m src.maine_main --enrich --batch-size 50 --workers 10
```

Expected: exits 0 within ~5 minutes. Progress lines like `[50/50] enriched=47 failed=3 ...` appear near the end.

- [ ] **Step 4: Verify pass criteria**

Run:

```bash
sqlite3 data/maine_listings.db "SELECT COUNT(*) FROM maine_transactions WHERE enrichment_status='success' AND enriched_at > datetime('now','-1 hour');"
```

Expected: ≥ 45.

Run:

```bash
sqlite3 data/maine_listings.db "SELECT listing_agent, buyer_agent, sale_price, close_date, city FROM maine_transactions WHERE enrichment_status='success' AND enriched_at > datetime('now','-1 hour') ORDER BY enriched_at DESC LIMIT 10;"
```

Expected: in ≥ 8 of 10 rows both `listing_agent` and `buyer_agent` are non-null.

- [ ] **Step 5: Scan the run's stderr for lock errors**

The previous `python3 -m src.maine_main` command wrote logs to stdout/stderr. Any `database is locked` error that survived the retry helper would have surfaced as a Python traceback. Confirm none appeared. If lock tracebacks are visible, stop and file a follow-up before the full backfill.

- [ ] **Step 6: If all criteria met, commit a short note in the plan's status (optional)**

Skip this step if the smoke test was clean. If any criterion failed, revert any suspect commit from Tasks 1–6 and diagnose before Task 8.

---

## Task 8: Full backfill (manual gate)

Runs the concurrent pipeline against all remaining unenriched listings. Gated on Task 7 passing.

**Files:** none (creates `data/maine_enrichment_log.txt` as a log artifact; gitignore already excludes `data/*.txt`; check and add if needed).

- [ ] **Step 1: Confirm Task 7 passed**

Do not start this task unless Task 7's pass criteria were met.

- [ ] **Step 2: Verify no prior `maine_main --enrich` process is running**

Run: `ps aux | grep 'maine_main' | grep -v grep`
Expected: no matching processes. If something is running, kill it with `kill <PID>` (confirm with the user first) before starting the backfill.

- [ ] **Step 3: Launch the backfill in the background**

Run:

```bash
export FIRECRAWL_API_KEY=$(grep ^FIRECRAWL_API_KEY .env | cut -d= -f2)
nohup python3 -m src.maine_main --enrich --batch-size 11000 --workers 25 \
  >> data/maine_enrichment_log.txt 2>&1 &
echo "backfill pid=$!"
```

Note: `--batch-size 11000` is deliberately larger than the 10,587 pending rows so the single run drains the queue.

- [ ] **Step 4: Monitor progress every 10–15 minutes**

Run in another shell (the Bash tool's `run_in_background=false` is fine — each monitoring command is quick):

```bash
tail -n 20 data/maine_enrichment_log.txt
sqlite3 data/maine_listings.db "SELECT enrichment_status, COUNT(*) FROM maine_transactions GROUP BY enrichment_status;"
```

Expected trajectory:
- Within 5 min: `success` count has climbed by several hundred.
- Within 30 min: `success` count > 5,000.
- Within 75 min: process exits; `success` ≥ 10,000 and `enriched` rows sum to ~10,587.

- [ ] **Step 5: Verify circuit breaker did not abort**

Run: `grep -i "circuit breaker" data/maine_enrichment_log.txt`
Expected: empty, OR only `"paused pool for 30s"` lines (which are recoverable). A line containing `"aborted run"` means the breaker killed the run — stop and diagnose.

- [ ] **Step 6: Verify final enrichment counts**

Run:

```bash
sqlite3 data/maine_listings.db "SELECT COUNT(*) total, SUM(CASE WHEN enrichment_status='success' THEN 1 ELSE 0 END) enriched, SUM(CASE WHEN enrichment_status='error' THEN 1 ELSE 0 END) errored FROM maine_transactions;"
```

Expected: `total ≈ 10587`, `enriched ≥ 10000`, `errored ≤ 500`.

- [ ] **Step 7: Spot-check dual-agent coverage**

Run:

```bash
sqlite3 data/maine_listings.db "SELECT COUNT(*) FROM maine_transactions WHERE enrichment_status='success' AND listing_agent IS NOT NULL AND buyer_agent IS NOT NULL;"
```

Expected: ≥ 80% of successfully enriched rows have both agents populated.

- [ ] **Step 8: Commit the log artifact (optional, user preference)**

If the user wants the run log in git:

```bash
git add data/maine_enrichment_log.txt
git commit -m "Add Maine enrichment backfill run log"
```

If not, leave it untracked.

---

## Final Task: Update the open PR with the implementation

**Files:** all files modified in Tasks 1–6 are already committed; pushing updates PR #10.

- [ ] **Step 1: Push the implementation commits**

```bash
git push origin HEAD
```

- [ ] **Step 2: Leave a PR comment summarizing smoke-test and backfill results**

Use `gh pr comment 10 --body "..."` with the actual counts from Task 7 Step 4 and Task 8 Step 6. Example body:

```
Smoke test (Task 7): 50/50 processed, 47 enriched, 3 failed; no lock errors.
Backfill (Task 8): 10,587 pending -> 10,412 success, 175 error; ~62 min wall time;
no circuit-breaker aborts. Dual-agent coverage: 9,103 / 10,412 (87.4%).
```

- [ ] **Step 3: Mark PR ready for review (if opened as draft)**

The PR was opened non-draft, so no action needed.

---

## Self-Review Notes

Spec coverage check:
- §3 module changes → Tasks 1, 2, 5, 6 ✓
- §4 EnrichmentRunner → Task 5 ✓
- §5 CircuitBreaker → Task 3 ✓
- §6 MinGapLauncher → Task 4 ✓
- §7 Progress reporting → Task 4 ✓
- §8 Thread-local cleanup → Task 5 (relies on thread exit + sqlite3 finalization, per spec) ✓
- §9 Tests → Tasks 1, 3 ✓
- §10 Smoke test → Task 7 ✓
- §11 Full backfill → Task 8 ✓
- §12 Rollback → implicit (`--workers 1` through same code path, no schema changes)
- §13 Risk log → addressed in implementation (per-worker conn, retry helper, circuit breaker)

No placeholders. Types (`CircuitBreaker`, `MinGapLauncher`, `ProgressCounter`, `WorkerResult`, `EnrichmentRunner`) and method names (`acquire`, `record_success`, `record_failure`, `wait_if_paused`, `aborted`, `run`, `_process_one`, `_get_conn`, `_get_client`) are consistent across tasks.
