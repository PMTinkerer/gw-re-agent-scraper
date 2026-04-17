"""Firecrawl-based Maine Listings (MREIS MLS) transaction scraper.

Two-phase approach:
1. Discover: scrape search result pages to find closed listing URLs
2. Enrich: scrape detail pages to extract agent data from NUXT blob

Both phases support concurrent execution via ThreadPoolExecutor.
"""
from __future__ import annotations

import logging
import random
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from .maine_database import (
    _DEFAULT_DB,
    enrich_listing,
    get_connection,
    get_unenriched,
    mark_enrichment_failed,
    upsert_listing,
    url_exists,
)
from .maine_parser import (
    DETAIL_EXTRACT_JS,
    parse_detail_response,
    parse_pagination,
    parse_search_cards,
)
from .maine_state import mark_complete, mark_failed, mark_started, save_state
from .state import TOWNS
from .zillow_firecrawl import require_firecrawl_key

logger = logging.getLogger(__name__)

_SEARCH_URL = 'https://mainelistings.com/listings'
_BLOCK_STRINGS = ['unexpected occurred', 'access denied', 'captcha']
_DB_LOCK_RETRIES = 5


def _get_client():
    from firecrawl import Firecrawl
    return Firecrawl(api_key=require_firecrawl_key())


def _open_threadsafe_conn(db_path: str | None = None) -> sqlite3.Connection:
    """Open a SQLite connection safe for cross-thread use.

    WAL mode + check_same_thread=False + our db_lock gives concurrent readers
    plus serialized writers, which is what we want for the worker pool.
    """
    import os
    path = db_path or _DEFAULT_DB
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    conn = sqlite3.connect(path, check_same_thread=False, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    return conn


def _scrape(client, url: str, fmt: str = 'markdown'):
    """Scrape a page and return the result object. Raises on blocked pages."""
    kwargs = {'formats': [fmt], 'wait_for': 8000}
    if fmt == 'rawHtml':
        kwargs['actions'] = [
            {'type': 'wait', 'milliseconds': 5000},
            {'type': 'executeJavascript', 'script': DETAIL_EXTRACT_JS},
        ]

    result = client.scrape(url, **kwargs)
    content = getattr(result, fmt, '') or getattr(result, 'markdown', '') or ''

    if any(s in content.lower() for s in _BLOCK_STRINGS):
        raise RuntimeError(f'Blocked or error page at {url}')

    return result


def _db_write(conn: sqlite3.Connection, lock: threading.Lock, fn, *args, **kwargs):
    """Serialize SQLite writes and retry briefly on lock contention."""
    for attempt in range(_DB_LOCK_RETRIES):
        try:
            with lock:
                return fn(conn, *args, **kwargs)
        except sqlite3.OperationalError as exc:
            if 'locked' not in str(exc).lower() or attempt == _DB_LOCK_RETRIES - 1:
                raise
            time.sleep(random.uniform(0.1, 0.5))


# === Phase 1: Discovery ===

def discover_listings(
    conn,
    state: dict,
    *,
    towns: list[str] | None = None,
    max_pages: int = 90,
    recent_only: bool = False,
    state_path: str | None = None,
    workers: int = 1,
) -> dict:
    """Phase 1: Discover closed listings from search result pages.

    Towns are independent, so each worker processes one town end-to-end.
    """
    towns_to_process = towns or list(TOWNS)
    total_listings = 0
    towns_done = 0
    state_lock = threading.Lock()
    db_lock = threading.Lock()

    if workers <= 1:
        client = _get_client()
        for town in towns_to_process:
            count = _run_town(
                client, conn, state, town, max_pages, recent_only,
                state_path, state_lock, db_lock,
            )
            if count is not None:
                towns_done += 1
                total_listings += count
        return {'towns': towns_done, 'listings': total_listings}

    # Concurrent path needs a thread-safe connection; the caller's `conn`
    # was opened on the main thread and would raise on cross-thread use.
    thread_conn = _open_threadsafe_conn()
    try:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {}
            for town in towns_to_process:
                client = _get_client()
                fut = pool.submit(
                    _run_town, client, thread_conn, state, town, max_pages,
                    recent_only, state_path, state_lock, db_lock,
                )
                futures[fut] = town

            for fut in as_completed(futures):
                town = futures[fut]
                try:
                    count = fut.result()
                except Exception as exc:
                    logger.error('Town %s crashed: %s', town, exc)
                    continue
                if count is not None:
                    towns_done += 1
                    total_listings += count
    finally:
        thread_conn.close()

    return {'towns': towns_done, 'listings': total_listings}


def _run_town(
    client, conn, state, town, max_pages, recent_only,
    state_path, state_lock, db_lock,
) -> int | None:
    """Discover one town. Returns listing count or None on failure."""
    with state_lock:
        mark_started(state, town)
        save_state(state, state_path)

    try:
        count = _discover_town(
            client, conn, town, max_pages, recent_only, db_lock,
        )
        with state_lock:
            mark_complete(state, town, listings_found=count)
            save_state(state, state_path)
        logger.info('Town %s: %d listings discovered', town, count)
        return count
    except Exception as exc:
        with state_lock:
            mark_failed(state, town, str(exc))
            save_state(state, state_path)
        logger.error('Town %s failed: %s', town, exc)
        return None


def _discover_town(
    client, conn, town: str, max_pages: int, recent_only: bool,
    db_lock: threading.Lock,
) -> int:
    """Scrape search pages for one town."""
    town_count = 0

    for page_num in range(1, max_pages + 1):
        url = f'{_SEARCH_URL}?city={town}&mls_status=Closed'
        if page_num > 1:
            url += f'&page={page_num}'

        logger.info('Discovering %s page %d...', town, page_num)
        try:
            result = _scrape(client, url, 'markdown')
        except Exception as exc:
            logger.warning('  %s page %d failed: %s', town, page_num, exc)
            if page_num == 1:
                raise
            break

        markdown = getattr(result, 'markdown', '') or ''
        cards = parse_search_cards(markdown)
        if not cards and page_num > 1:
            logger.info('No cards on page %d for %s, stopping', page_num, town)
            break

        new_count = 0
        with db_lock:
            for card in cards:
                if not url_exists(conn, card['detail_url']):
                    new_count += 1
                upsert_listing(conn, card)

        town_count += len(cards)

        if recent_only and new_count == 0 and page_num > 1:
            logger.info('All %d cards on page %d already known, stopping',
                        len(cards), page_num)
            break

        page_info = parse_pagination(markdown)
        if page_info and page_num >= page_info[1]:
            logger.info('Reached last page (%d of %d)', page_num, page_info[1])
            break

    return town_count


# === Phase 2: Enrichment ===

class _CircuitBreaker:
    """Tracks consecutive and total failures across worker threads."""

    def __init__(self, consecutive_limit: int = 5, total_limit: int = 20):
        self.consecutive_limit = consecutive_limit
        self.total_limit = total_limit
        self._consecutive = 0
        self._total_failures = 0
        self._lock = threading.Lock()
        self._pause_until = 0.0

    def record_success(self) -> None:
        with self._lock:
            self._consecutive = 0

    def record_failure(self) -> str | None:
        """Record a failure. Returns action: 'abort', 'pause', or None."""
        with self._lock:
            self._consecutive += 1
            self._total_failures += 1
            if self._total_failures >= self.total_limit:
                return 'abort'
            if self._consecutive >= self.consecutive_limit:
                self._consecutive = 0
                self._pause_until = time.monotonic() + 30.0
                return 'pause'
        return None

    def wait_if_paused(self) -> None:
        with self._lock:
            wait = max(0.0, self._pause_until - time.monotonic())
        if wait > 0:
            time.sleep(wait)

    @property
    def total_failures(self) -> int:
        with self._lock:
            return self._total_failures


def enrich_listings(
    conn,
    *,
    batch_size: int = 50,
    max_attempts: int = 2,
    workers: int = 1,
    db_path: str | None = None,
) -> dict:
    """Phase 2: Enrich listings with agent data from detail pages.

    With workers > 1, uses a ThreadPoolExecutor. Each worker opens its own
    SQLite connection (WAL mode supports concurrent readers + 1 writer).
    """
    pending = get_unenriched(conn, batch_size=batch_size, max_attempts=max_attempts)
    total = len(pending)
    logger.info('Enrichment batch: %d listings (workers=%d)', total, workers)

    if workers <= 1:
        return _enrich_serial(pending)

    return _enrich_concurrent(pending, workers, db_path)


def _enrich_serial(pending: list[dict]) -> dict:
    """Serial enrichment using the caller's connection."""
    client = _get_client()
    conn = get_connection()
    db_lock = threading.Lock()
    breaker = _CircuitBreaker()
    counts = {'enriched': 0, 'failed': 0}

    start = time.monotonic()
    for i, row in enumerate(pending, 1):
        status = _enrich_one(client, conn, db_lock, row, breaker)
        if status == 'abort':
            logger.error('Circuit breaker tripped (aborting batch)')
            break
        counts['enriched' if status == 'ok' else 'failed'] += 1
        if i % 25 == 0:
            elapsed = time.monotonic() - start
            rate = i / elapsed if elapsed > 0 else 0
            logger.info('[%d/%d] %.1f/s, %d enriched, %d failed',
                        i, len(pending), rate,
                        counts['enriched'], counts['failed'])

    conn.close()
    return {'enriched': counts['enriched'], 'failed': counts['failed'], 'total': len(pending)}


def _enrich_concurrent(pending: list[dict], workers: int, db_path: str | None) -> dict:
    """Concurrent enrichment with a ThreadPoolExecutor."""
    client = _get_client()
    conn = _open_threadsafe_conn(db_path)
    db_lock = threading.Lock()
    breaker = _CircuitBreaker()
    counts = {'enriched': 0, 'failed': 0, 'aborted': False}
    progress_lock = threading.Lock()

    start = time.monotonic()
    last_log = start
    completed = 0

    def worker(row: dict) -> str:
        breaker.wait_if_paused()
        return _enrich_one(client, conn, db_lock, row, breaker)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(worker, row) for row in pending]

        for fut in as_completed(futures):
            try:
                status = fut.result()
            except Exception as exc:
                logger.error('Worker crashed: %s', exc)
                status = 'failed'

            with progress_lock:
                completed += 1
                if status == 'ok':
                    counts['enriched'] += 1
                else:
                    counts['failed'] += 1

                if status == 'abort':
                    counts['aborted'] = True
                    logger.error('Circuit breaker tripped (%d failures); stopping',
                                 breaker.total_failures)

                now = time.monotonic()
                if completed % 50 == 0 or now - last_log > 60:
                    elapsed = now - start
                    rate = completed / elapsed if elapsed > 0 else 0
                    remaining = (len(pending) - completed) / rate if rate > 0 else 0
                    logger.info(
                        '[%d/%d] %.1f/s, %d enriched, %d failed, ETA %.0fs',
                        completed, len(pending), rate,
                        counts['enriched'], counts['failed'], remaining,
                    )
                    last_log = now

            if counts['aborted']:
                for f in futures:
                    f.cancel()
                break

    conn.close()
    return {
        'enriched': counts['enriched'],
        'failed': counts['failed'],
        'total': len(pending),
        'aborted': counts['aborted'],
    }


def _enrich_one(
    client, conn, db_lock: threading.Lock, row: dict, breaker: _CircuitBreaker,
) -> str:
    """Enrich a single listing. Returns 'ok', 'failed', or 'abort'."""
    url = row['detail_url']
    try:
        result = _scrape(client, url, 'rawHtml')

        acts = getattr(result, 'actions', None)
        if not acts or 'javascriptReturns' not in acts:
            _db_write(conn, db_lock, mark_enrichment_failed, url, 'no JS returns')
            action = breaker.record_failure()
            return 'abort' if action == 'abort' else 'failed'

        data = parse_detail_response(acts['javascriptReturns'][0])
        if not data:
            _db_write(conn, db_lock, mark_enrichment_failed, url, 'parse failed')
            action = breaker.record_failure()
            return 'abort' if action == 'abort' else 'failed'

        _db_write(conn, db_lock, enrich_listing, url, data)
        breaker.record_success()
        return 'ok'

    except Exception as exc:
        try:
            _db_write(conn, db_lock, mark_enrichment_failed, url, str(exc)[:200])
        except Exception as inner:
            logger.error('  DB write failed for %s: %s', url, inner)
        logger.warning('  %s error: %s', url, str(exc)[:120])
        action = breaker.record_failure()
        return 'abort' if action == 'abort' else 'failed'
