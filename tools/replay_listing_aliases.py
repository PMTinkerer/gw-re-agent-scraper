"""Offline integration rehearsal; fixture identity proof is NOT live acceptance.

Replays captured summary bytes, uses stored rows as simulated alias detail
responses, and uses previously captured new-detail successes where available.
No transport session, credentials, or live callback is constructed. Publication
is restricted to a temporary copy. Source and checkpoint hashes must not change.
"""

import argparse
from collections import defaultdict, deque
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import sys
import tempfile
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.active_refresh import run_active_refresh  # noqa: E402
from src.maine_active import query_active_listings  # noqa: E402
from src.refresh_transport import RefreshTransport  # noqa: E402
from src.state import TOWNS  # noqa: E402


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def rehearse(summary_dir, source_db, checkpoint_state):
    protected = {p: digest(p) for p in (source_db, checkpoint_state)}
    records = sorted(
        (json.loads(p.read_text()) for p in summary_dir.glob("*.json")),
        key=lambda r: r["captured_at"],
    )
    assert records, "No captured summaries"
    queues = defaultdict(deque)
    for record in records:
        assert record["status_code"] == 200 and record["truncated"] is False
        assert (
            hashlib.sha256(record["markdown"].encode()).hexdigest()
            == record["content_sha256"]
        )
        queues[record["url"]].append(record)

    def summary(url, kind):
        assert kind == "summary" and queues[url], "Uncaptured request refused"
        return {"markdown": queues[url].popleft()["markdown"]}

    adapter = RefreshTransport.__new__(RefreshTransport)
    adapter._scrape = summary
    with sqlite3.connect(source_db.resolve().as_uri() + "?mode=ro", uri=True) as db:
        db.row_factory = sqlite3.Row
        source_rows = [dict(r) for r in db.execute("SELECT * FROM maine_transactions")]
        old_history = list(
            db.execute("SELECT * FROM maine_listing_history ORDER BY id")
        )
    by_id = defaultdict(list)
    for row in source_rows:
        by_id[row["detail_url"].rsplit("/", 1)[-1]].append(row)
    simulated_alias_calls = []
    missing_new_calls = []

    def fixture_detail(url):
        candidates = by_id[url.rsplit("/", 1)[-1]]
        if not candidates:
            missing_new_calls.append(url)
            return None
        assert len({(r["mls_number"], r["city"]) for r in candidates}) == 1
        row = min(candidates, key=lambda r: r["id"])
        simulated_alias_calls.append(url)
        return dict(row, detail_url=url, status="Active")

    started = time.monotonic()
    with tempfile.TemporaryDirectory(
        prefix=".alias-rehearsal-", dir=source_db.parent
    ) as d:
        temporary = Path(d)
        copied_db, state = temporary / "source.db", temporary / "retries.db"
        shutil.copy2(source_db, copied_db)
        shutil.copy2(checkpoint_state, state)
        manifest = run_active_refresh(
            db_path=copied_db,
            manifest_path=temporary / "manifest.json",
            state_path=state,
            towns=TOWNS,
            fetch_summary_range=adapter.summary_range,
            fetch_detail=fixture_detail,
            fetch_status=lambda url: None,
            now=lambda: datetime(2026, 9, 27, 17, tzinfo=timezone.utc),
            run_id="offline-alias-rehearsal-not-live-acceptance",
        )
        assert manifest["database_sha256"] == digest(copied_db)
        assert not any(queues.values()), "Not all captured requests consumed"
        with sqlite3.connect(copied_db) as db:
            db.row_factory = sqlite3.Row
            for old in source_rows:
                current = db.execute(
                    "SELECT id,detail_url,mls_number FROM maine_transactions WHERE id=?",
                    (old["id"],),
                ).fetchone()
                assert tuple(current) == (
                    old["id"],
                    old["detail_url"],
                    old["mls_number"],
                )
            current_history = list(
                db.execute("SELECT * FROM maine_listing_history ORDER BY id")
            )
            assert [tuple(r) for r in current_history[: len(old_history)]] == [
                tuple(r) for r in old_history
            ]
            active = query_active_listings(db, towns=TOWNS)
            ids = [r["mls_number"] for r in active]
            assert len(ids) == len(set(ids)), "Duplicate active MLS in frozen helper"
            assert sorted(ids) == manifest["active_mls_ids"]
        result = {
            "evidence_type": "offline_rehearsal_with_simulated_alias_identity_proof",
            "live_feed_accepted": False,
            "summary_responses": len(records),
            "towns": manifest["towns"],
            "active_rows": len(active),
            "unresolved_new_rows": len(manifest["unresolved"]),
            "simulated_alias_detail_calls": len(simulated_alias_calls),
            "uncached_new_detail_fixture_misses": len(missing_new_calls),
            "original_rows_preserved": len(source_rows),
            "original_history_rows_preserved": len(old_history),
            "seconds": round(time.monotonic() - started, 3),
        }
    assert all(digest(p) == expected for p, expected in protected.items())
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--summary-dir", type=Path, required=True)
    parser.add_argument("--source-db", type=Path, required=True)
    parser.add_argument("--checkpoint-state", type=Path, required=True)
    args = parser.parse_args()
    print(
        json.dumps(
            rehearse(args.summary_dir, args.source_db, args.checkpoint_state), indent=2
        )
    )
