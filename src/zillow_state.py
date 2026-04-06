"""State helpers for the Zillow scraping pipeline."""
from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime

from .state import TOWNS

_DEFAULT_STATE_PATH = os.path.join(
    os.path.dirname(__file__), '..', 'data', 'zillow_scrape_state.json',
)


def _town_slug(town: str) -> str:
    return town.lower().replace(' ', '_')


def _default_state() -> dict:
    return {
        'towns': {
            _town_slug(town): {'status': 'pending'}
            for town in TOWNS
        },
        'last_run': None,
        'created_at': datetime.utcnow().isoformat(),
    }


def load_state(path: str | None = None) -> dict:
    path = path or _DEFAULT_STATE_PATH
    if not os.path.exists(path):
        return _default_state()

    with open(path, 'r', encoding='utf-8') as f:
        content = f.read().strip()
        state = json.loads(content) if content else _default_state()

    default = _default_state()
    state.setdefault('towns', {})
    for slug, info in default['towns'].items():
        state['towns'].setdefault(slug, info)
    state.setdefault('created_at', default['created_at'])
    state.setdefault('last_run', None)
    return state


def save_state(state: dict, path: str | None = None) -> None:
    path = path or _DEFAULT_STATE_PATH
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    state['last_run'] = datetime.utcnow().isoformat()

    fd, tmp_path = tempfile.mkstemp(dir=os.path.dirname(os.path.abspath(path)), suffix='.json')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            json.dump(state, f, indent=2)
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def mark_started(state: dict, town: str) -> None:
    state['towns'][_town_slug(town)] = {
        'status': 'in_progress',
        'started_at': datetime.utcnow().isoformat(),
    }


def mark_complete(state: dict, town: str, profiles_found: int = 0) -> None:
    state['towns'][_town_slug(town)] = {
        'status': 'complete',
        'profiles_found': profiles_found,
        'completed_at': datetime.utcnow().isoformat(),
    }


def mark_failed(state: dict, town: str, error: str) -> None:
    slug = _town_slug(town)
    prev = state['towns'].get(slug, {})
    state['towns'][slug] = {
        'status': 'failed',
        'error': error,
        'retries': prev.get('retries', 0) + 1,
        'failed_at': datetime.utcnow().isoformat(),
    }


def reset_state(path: str | None = None) -> dict:
    state = _default_state()
    save_state(state, path=path)
    return state
