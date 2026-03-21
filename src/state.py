"""Scrape state manager for resumable chunk-based processing.

Tracks which town+source+date_range combinations have been scraped,
failed, or are still pending. Enables GitHub Actions runs to pick up
where the last run left off.
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

_DEFAULT_STATE_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'scrape_state.json')

# All 10 target towns
TOWNS = [
    'Kittery', 'York', 'Ogunquit', 'Wells', 'Kennebunk',
    'Kennebunkport', 'Biddeford', 'Saco', 'Old Orchard Beach', 'Scarborough',
]

# Sources and their chunk granularity
# Redfin: one chunk per town (pulls all 3 years at once)
# Realtor: one chunk per town per year (API pagination)
SOURCES = ['redfin', 'realtor']
YEARS = [2023, 2024, 2025]

# Stale in-progress threshold (minutes)
_STALE_THRESHOLD = 60


def _town_slug(town: str) -> str:
    """Convert town name to a slug for chunk keys."""
    return town.lower().replace(' ', '_')


def _default_state() -> dict:
    """Return a fresh default state."""
    return {
        'mode': 'initial',
        'region_ids': {},
        'chunks': {},
        'rapidapi_calls_this_month': 0,
        'rapidapi_month': None,
        'last_run': None,
        'created_at': datetime.utcnow().isoformat(),
    }


def generate_all_chunks() -> list[str]:
    """Generate all chunk keys for initial collection.

    Redfin: one chunk per town (pulls all 3 years at once via sold_within_days=1095)
    Realtor: one chunk per town per year (API queries by date range)

    Returns ~40 chunk keys.
    """
    chunks = []
    for town in TOWNS:
        slug = _town_slug(town)
        # Redfin: single chunk per town
        chunks.append(f'redfin_{slug}')
        # Realtor: per-year per town
        for year in YEARS:
            chunks.append(f'realtor_{slug}_{year}')
    return chunks


def load_state(path: str | None = None) -> dict:
    """Load state from JSON file, creating defaults if missing."""
    path = path or _DEFAULT_STATE_PATH
    if not os.path.exists(path):
        state = _default_state()
        # Initialize all chunks as pending
        for key in generate_all_chunks():
            state['chunks'][key] = {'status': 'pending'}
        return state

    with open(path, 'r') as f:
        content = f.read().strip()
        if not content:
            state = _default_state()
            for key in generate_all_chunks():
                state['chunks'][key] = {'status': 'pending'}
            return state
        state = json.loads(content)

    # Ensure any new chunks exist (forward compatibility)
    for key in generate_all_chunks():
        if key not in state.get('chunks', {}):
            state.setdefault('chunks', {})[key] = {'status': 'pending'}

    return state


def save_state(state: dict, path: str | None = None) -> None:
    """Atomically save state to JSON file (write tmp then rename)."""
    path = path or _DEFAULT_STATE_PATH
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)

    state['last_run'] = datetime.utcnow().isoformat()

    dir_name = os.path.dirname(os.path.abspath(path))
    fd, tmp_path = tempfile.mkstemp(dir=dir_name, suffix='.json')
    try:
        with os.fdopen(fd, 'w') as f:
            json.dump(state, f, indent=2)
        os.replace(tmp_path, path)
    except Exception:
        # Clean up temp file on failure
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def get_next_chunks(state: dict, max_chunks: int = 3,
                    source_filter: str | None = None,
                    town_filter: str | None = None) -> list[str]:
    """Return the next chunk keys to process.

    Priority:
    1. Failed chunks (retry)
    2. Stale in-progress chunks (crashed runs, >60 min)
    3. Pending chunks

    Redfin chunks are prioritized over Realtor (primary source first).
    """
    failed = []
    stale = []
    pending = []
    now = datetime.utcnow()

    for key, info in state.get('chunks', {}).items():
        status = info.get('status', 'pending')

        # Apply filters
        if source_filter and not key.startswith(source_filter + '_'):
            continue
        if town_filter:
            town_slug = _town_slug(town_filter)
            # Check if the chunk key contains this town slug
            parts = key.split('_', 1)
            if len(parts) > 1 and town_slug not in parts[1]:
                continue

        if status == 'failed':
            failed.append(key)
        elif status == 'in_progress':
            started = info.get('started_at')
            if started:
                started_dt = datetime.fromisoformat(started)
                if (now - started_dt) > timedelta(minutes=_STALE_THRESHOLD):
                    stale.append(key)
            else:
                stale.append(key)
        elif status == 'pending':
            pending.append(key)

    # Sort: redfin before realtor within each priority group
    def _sort_key(k):
        return (0 if k.startswith('redfin_') else 1, k)

    candidates = (
        sorted(failed, key=_sort_key)
        + sorted(stale, key=_sort_key)
        + sorted(pending, key=_sort_key)
    )

    return candidates[:max_chunks]


def mark_started(state: dict, chunk_key: str) -> None:
    """Mark a chunk as in-progress."""
    state['chunks'][chunk_key] = {
        'status': 'in_progress',
        'started_at': datetime.utcnow().isoformat(),
    }


def mark_complete(state: dict, chunk_key: str, rows: int = 0) -> None:
    """Mark a chunk as complete with row count."""
    state['chunks'][chunk_key] = {
        'status': 'complete',
        'rows': rows,
        'completed_at': datetime.utcnow().isoformat(),
    }


def mark_failed(state: dict, chunk_key: str, error: str) -> None:
    """Mark a chunk as failed with error message."""
    prev = state['chunks'].get(chunk_key, {})
    retries = prev.get('retries', 0) + 1
    state['chunks'][chunk_key] = {
        'status': 'failed',
        'error': error,
        'retries': retries,
        'failed_at': datetime.utcnow().isoformat(),
    }


def is_initial_complete(state: dict) -> bool:
    """Check if all initial collection chunks are complete."""
    chunks = state.get('chunks', {})
    if not chunks:
        return False
    return all(
        info.get('status') == 'complete'
        for info in chunks.values()
    )


def parse_chunk_key(chunk_key: str) -> dict:
    """Parse a chunk key into its components.

    Examples:
        'redfin_york' -> {'source': 'redfin', 'town_slug': 'york', 'year': None}
        'realtor_old_orchard_beach_2024' -> {'source': 'realtor', 'town_slug': 'old_orchard_beach', 'year': 2024}
    """
    # Realtor chunks end with _YYYY
    for source in SOURCES:
        prefix = source + '_'
        if chunk_key.startswith(prefix):
            rest = chunk_key[len(prefix):]
            if source == 'realtor':
                # Last segment is the year
                parts = rest.rsplit('_', 1)
                if len(parts) == 2 and parts[1].isdigit():
                    return {
                        'source': source,
                        'town_slug': parts[0],
                        'year': int(parts[1]),
                    }
            return {
                'source': source,
                'town_slug': rest,
                'year': None,
            }

    return {'source': None, 'town_slug': None, 'year': None}


def slug_to_town(slug: str) -> str | None:
    """Convert a town slug back to the proper town name."""
    for town in TOWNS:
        if _town_slug(town) == slug:
            return town
    return None


def track_rapidapi_call(state: dict) -> bool:
    """Track a RapidAPI call. Returns False if budget exhausted (>=95 calls)."""
    current_month = datetime.utcnow().strftime('%Y-%m')
    if state.get('rapidapi_month') != current_month:
        state['rapidapi_month'] = current_month
        state['rapidapi_calls_this_month'] = 0

    if state['rapidapi_calls_this_month'] >= 95:
        logger.warning('RapidAPI budget exhausted for %s (%d calls)',
                       current_month, state['rapidapi_calls_this_month'])
        return False

    state['rapidapi_calls_this_month'] += 1
    return True
