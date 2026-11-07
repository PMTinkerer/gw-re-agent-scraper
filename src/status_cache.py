"""Remember targeted status checks across runs (2026-11-07).

A listing that drops out of the complete Active search gets one paid status
check. Run 42316627167 showed why that must survive a failed run: after an
11-day outage it needed 340 such checks (166 of them listings that were already
Unverified and are re-checked every run), more than one run's time and unit
cap allow. Nothing was published, so every check was lost and the next run
would have started again from zero.

Each check is recorded in the persistent retry database and pushed with the
other reservation evidence every few checks. A listing that is still absent
reuses its result for up to a week instead of paying again. Reuse never makes a
listing eligible: an unresolved check still leaves it Unverified, and a listing
that comes back appears in the Active search, which is read fresh every run.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone

STATUS_REUSE = timedelta(days=7)
CHECKPOINT_EVERY = 20


def _valid(result):
    return result is None or (
        isinstance(result, dict) and isinstance(result.get("mls_number"), str)
    )


class StatusCache:
    """Wrap a status fetcher; persist each answer and reuse recent ones."""

    def __init__(self, path, fetch, checkpoint, *, now=None, reuse=STATUS_REUSE):
        self.path = path
        self.fetch = fetch
        self.checkpoint = checkpoint
        self.now = now or (lambda: datetime.now(timezone.utc))
        self.reuse = reuse
        self.fresh = 0
        self.reused = 0
        with sqlite3.connect(path, timeout=30) as conn:
            conn.execute("""CREATE TABLE IF NOT EXISTS status_checks (
                detail_url TEXT PRIMARY KEY, checked_at TEXT NOT NULL,
                result_json TEXT NOT NULL)""")

    def _recent(self, url):
        with sqlite3.connect(self.path, timeout=30) as conn:
            row = conn.execute(
                "SELECT checked_at, result_json FROM status_checks WHERE detail_url=?",
                (url,),
            ).fetchone()
        if row is None:
            return False, None
        try:
            age = self.now() - datetime.fromisoformat(row[0])
            result = json.loads(row[1])
        except (TypeError, ValueError):
            return False, None
        if age < timedelta(0) or age > self.reuse or not _valid(result):
            return False, None
        return True, result

    def __call__(self, url):
        found, result = self._recent(url)
        if found:
            self.reused += 1
            return result
        result = self.fetch(url)  # a failure is never recorded
        if not _valid(result):
            result = None
        with sqlite3.connect(self.path, timeout=30) as conn:
            conn.execute(
                """INSERT INTO status_checks(detail_url, checked_at, result_json)
                VALUES (?, ?, ?) ON CONFLICT(detail_url) DO UPDATE SET
                checked_at=excluded.checked_at, result_json=excluded.result_json""",
                (url, self.now().isoformat(), json.dumps(result, sort_keys=True)),
            )
        self.fresh += 1
        if self.fresh % CHECKPOINT_EVERY == 0:
            self.checkpoint()
            print(
                f"Status checks so far: {self.fresh} new, {self.reused} reused",
                flush=True,
            )
        return result
