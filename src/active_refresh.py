"""Fail-closed incremental Active refresh with injected, budgeted transport.

The original scraper and read helpers remain independent. No provider, notifier,
credential discovery, or scheduling is imported here. Callbacks must raise on
transport errors, blocks, and caps; None means a successfully read but ambiguous
detail/status page. Only a fully validated discovery can publish a snapshot.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
import re
import shutil
import sqlite3
import tempfile
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable
from urllib.parse import urlsplit

from .maine_database import enrich_listing, upsert_listing, write_history_if_changed
from .state import TOWNS


class RefreshIncomplete(RuntimeError):
    """The previous database and coverage manifest must remain authoritative."""


@dataclass(frozen=True)
class SummaryPage:
    town: str
    page: int
    total_pages: int
    total_results: int
    listings: list[dict]


_INACTIVE = {"Closed", "Sold", "Pending", "Withdrawn", "Cancelled", "Unverified"}
_STATUSES = _INACTIVE | {"Active"}


def _source_status(value):
    return {"Active Under Contract": "Pending", "Canceled": "Cancelled"}.get(
        value, value
    )


def _town(value: str) -> str:
    if not isinstance(value, str):
        raise RefreshIncomplete("Missing town coverage")
    canonical = " ".join(value.strip().lower().replace("_", " ").split())
    if canonical not in {town.lower() for town in TOWNS}:
        raise RefreshIncomplete(f"Unknown town: {value!r}")
    return canonical


def _timestamp(clock: Callable[[], datetime]) -> str:
    value = clock()
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Refresh clock must be timezone aware")
    return value.astimezone(timezone.utc).isoformat()


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _legacy_utc(value: str) -> str:
    """Existing schema timestamps were written with datetime.utcnow()."""
    try:
        timestamp = datetime.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise RefreshIncomplete("Missing historical status observation time") from exc
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return timestamp.astimezone(timezone.utc).isoformat()


def _valid_url(value: object) -> bool:
    if not isinstance(value, str) or value != value.strip():
        return False
    parts = urlsplit(value)
    return (
        parts.scheme == "https"
        and parts.netloc == "mainelistings.com"
        and parts.path.startswith("/listings/")
        and len(parts.path) > len("/listings/")
        and not parts.query
        and not parts.fragment
    )


class _PaginationInconsistent(RefreshIncomplete):
    """Valid source cards drifted within a town's paginated search."""


def _discover_town(town, callback, max_pages, completed):
    """Keep one attempt isolated until its entire town has complete coverage."""
    found = {}
    page_number, expected_pages, expected_count = 1, None, None
    while True:
        page = callback(town, page_number)
        if not isinstance(page, SummaryPage) or _town(page.town) != town:
            raise RefreshIncomplete("Missing requested town coverage")
        if (
            type(page.page) is not int
            or page.page != page_number
            or type(page.total_pages) is not int
            or not 1 <= page.total_pages <= max_pages
            or type(page.total_results) is not int
            or page.total_results < 0
            or not isinstance(page.listings, list)
        ):
            raise RefreshIncomplete("Invalid or capped pagination")
        if expected_pages is None:
            expected_pages, expected_count = page.total_pages, page.total_results
        if not page.listings and not (
            page_number == 1 and expected_count == 0 and expected_pages == 1
        ):
            raise RefreshIncomplete("Missing discovery page results")
        duplicate = False
        for listing in page.listings:
            url = listing.get("detail_url") if isinstance(listing, dict) else None
            if (
                not _valid_url(url)
                or url in completed
                or (listing.get("city") is not None and _town(listing["city"]) != town)
                or listing.get("status") not in {"Active", "Pending"}
            ):
                raise RefreshIncomplete(
                    "Duplicate across towns, invalid, or out-of-town discovery card"
                )
            duplicate = duplicate or url in found
            # Query provenance is not a property fact. Never fill city from it.
            found[url] = dict(listing, discovery_town=town)
        # Validate the whole page before classifying any drift as retryable.
        if duplicate:
            raise _PaginationInconsistent("Duplicate discovery card within town")
        if (page.total_pages, page.total_results) != (expected_pages, expected_count):
            raise _PaginationInconsistent(
                "Pagination or result count changed during discovery"
            )
        if page_number == expected_pages:
            break
        page_number += 1
    if len(found) != expected_count:
        raise _PaginationInconsistent("Incomplete discovery result count")
    return found


def _discover(towns, callback, max_pages):
    found = {}
    for town in towns:
        for attempt in range(2):
            try:
                town_found = _discover_town(town, callback, max_pages, found)
                break
            except _PaginationInconsistent:
                if attempt == 1:
                    raise
        found.update(town_found)
    return found


def _contract_ready(row):
    """Mirror the downstream listing field contract without importing its app.

    Missing/invalid enrichment is not a successful detail result. Nullable
    numeric fields stay nullable; optional blank contact/photo text is allowed
    because the consumer normalizes it to None.
    """
    for field in ("mls_number", "address", "city", "list_date"):
        if not isinstance(row.get(field), str) or not row[field].strip():
            return False
    if not _valid_url(row.get("detail_url")):
        return False
    for field, minimum in (("list_price", 1), ("beds", 0)):
        value = row.get(field)
        if value is not None and (type(value) is not int or value < minimum):
            return False
    baths = row.get("baths")
    if baths is not None and (
        type(baths) not in (int, float) or not math.isfinite(baths) or baths < 0
    ):
        return False
    property_type = row.get("property_type")
    if property_type is not None and (
        not isinstance(property_type, str) or not property_type.strip()
    ):
        return False
    for field in ("photo_url", "listing_agent_email", "listing_office"):
        value = row.get(field)
        if value is None:
            continue
        if not isinstance(value, str):
            return False
        if not value.strip():
            continue
        if field == "photo_url":
            try:
                parsed = urlsplit(value.strip())
            except ValueError:
                return False
            if parsed.scheme not in ("http", "https") or not parsed.netloc:
                return False
        if field == "listing_agent_email" and not re.fullmatch(
            r"[^@\s]+@[^@\s]+\.[^@\s]+", value.strip()
        ):
            return False
    return True


def _merged_detail(summary, detail, *, cached):
    merged = dict(summary, **detail)
    # Current summary facts take precedence, but absent facts may be supplied
    # by validated detail evidence. Blank/malformed supplied facts stay invalid.
    for field in ("address", "city", "beds", "baths", "detail_url"):
        if summary.get(field) is not None:
            merged[field] = summary[field]
    merged["list_price"] = summary.get("list_price")
    merged["status"] = summary["status"]
    if not cached and summary["status"] == "Active":
        merged["status"] = _source_status(detail.get("status") or "Active")
    return merged


def _retry_detail(state, url, callback, timestamp, summary, identities, validate=None):
    """Persist the attempt before calling, and cache success across failed runs."""
    with sqlite3.connect(state, timeout=30) as conn:
        conn.execute("BEGIN IMMEDIATE")
        record = conn.execute(
            "SELECT attempts, detail_json FROM new_listing_retries WHERE detail_url=?",
            (url,),
        ).fetchone()
        if record and record[1]:
            detail = json.loads(record[1])
            if validate:
                validate(detail)
            _identity(detail, summary, identities)
            merged = _merged_detail(summary, detail, cached=True)
            if _contract_ready(merged):
                return merged
            # Older cached partial data must not poison every future feed.
            conn.execute(
                "UPDATE new_listing_retries SET detail_json=NULL WHERE detail_url=?",
                (url,),
            )
        if record and record[0] >= 2:
            return None
        conn.execute(
            """INSERT INTO new_listing_retries(detail_url, attempts, last_attempt_at)
            VALUES (?, 1, ?) ON CONFLICT(detail_url) DO UPDATE SET
            attempts=attempts+1, last_attempt_at=excluded.last_attempt_at""",
            (url, timestamp),
        )
    detail = callback(url)
    if validate:
        validate(detail)
    if (
        not isinstance(detail, dict)
        or not isinstance(detail.get("mls_number"), str)
        or not detail["mls_number"].strip()
    ):
        return None
    _identity(detail, summary, identities)
    merged = _merged_detail(summary, detail, cached=False)
    if not _contract_ready(merged):
        return None
    with sqlite3.connect(state, timeout=30) as conn:
        conn.execute(
            "UPDATE new_listing_retries SET detail_json=? WHERE detail_url=?",
            (json.dumps(detail), url),
        )
    return merged


def _observe(conn, run_id, row, status, reason, timestamp, source):
    conn.execute(
        """INSERT INTO active_refresh_observations
        (run_id, detail_url, mls_number, city, status, reason, observed_at, source)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            run_id,
            row["detail_url"],
            row.get("mls_number"),
            row["city"],
            status,
            reason,
            timestamp,
            source,
        ),
    )
    conn.commit()


def _identity(detail, row, identities):
    expected_town = row.get("city") or row.get("discovery_town")
    if detail.get("city") is not None and _town(detail["city"]) != _town(expected_town):
        raise RefreshIncomplete("Detail town identity change")
    if (
        detail.get("detail_url") is not None
        and detail["detail_url"] != row["detail_url"]
    ):
        raise RefreshIncomplete("Detail URL identity change")
    identity = detail.get("mls_number")
    if identity is None:
        return
    if (
        not isinstance(identity, str)
        or not identity
        or identity != identity.strip()
        or (row.get("mls_number") and identity != row["mls_number"])
        or (identity in identities and row["detail_url"] not in identities[identity])
    ):
        raise RefreshIncomplete("MLS identity collision or change")


def _stage(db_path, stage_path):
    # A publication is a standalone DB. Uncheckpointed external writes would be
    # lost by replacing it, so refuse them instead of changing the source DB.
    wal = Path(str(db_path) + "-wal")
    if wal.exists() and wal.stat().st_size:
        raise RefreshIncomplete("Source database has uncheckpointed writes")
    shutil.copy2(db_path, stage_path)
    conn = sqlite3.connect(stage_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=DELETE")
    conn.execute("""CREATE TABLE IF NOT EXISTS active_refresh_observations (
        id INTEGER PRIMARY KEY, run_id TEXT NOT NULL, detail_url TEXT NOT NULL,
        mls_number TEXT, city TEXT NOT NULL, status TEXT NOT NULL,
        reason TEXT NOT NULL, observed_at TEXT NOT NULL, source TEXT NOT NULL)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS active_refresh_unresolved (
        id INTEGER PRIMARY KEY, run_id TEXT NOT NULL, detail_url TEXT NOT NULL,
        discovery_town TEXT NOT NULL, reason TEXT NOT NULL,
        observed_at TEXT NOT NULL)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS active_refresh_aliases (
        alias_url TEXT PRIMARY KEY, canonical_url TEXT NOT NULL,
        mls_number TEXT NOT NULL, city TEXT NOT NULL,
        verified_at TEXT NOT NULL, run_id TEXT NOT NULL)""")
    conn.commit()
    return conn


def _stable_listing_key(url):
    """Only the human-readable slug may differ; never fuzzy-match addresses."""
    if not _valid_url(url):
        return None
    match = re.fullmatch(
        r"(/listings/ME/[A-Za-z_-]+/[0-9]{5}/[0-9]+)/[a-zA-Z0-9-]+/([0-9]+)",
        urlsplit(url).path,
    )
    return match.groups() if match else None


def _alias_resolution(conn, known, identities, discovered):
    stable = {}
    for url in known:
        key = _stable_listing_key(url)
        if key:
            stable.setdefault(key, []).append(url)

    def group(key):
        urls = stable.get(key, [])
        if not urls:
            raise RefreshIncomplete("Missing historical source listing identity")
        canonical = min(urls, key=lambda url: known[url]["id"])
        row = known[canonical]
        if (
            not row.get("mls_number")
            or identities.get(row["mls_number"]) != set(urls)
            or any(
                known[url].get("mls_number") != row["mls_number"]
                or _town(known[url]["city"]) != _town(row["city"])
                for url in urls
            )
        ):
            raise RefreshIncomplete("Ambiguous historical MLS alias identity")
        return canonical, urls

    aliases = {}
    for record in conn.execute("SELECT * FROM active_refresh_aliases"):
        alias, canonical = record["alias_url"], record["canonical_url"]
        row = known.get(canonical)
        key = _stable_listing_key(alias)
        if (
            not key
            or alias == canonical
            or row is None
            or key != _stable_listing_key(canonical)
            or group(key)[0] != canonical
            or record["mls_number"] != row.get("mls_number")
            or _town(record["city"]) != _town(row["city"])
        ):
            raise RefreshIncomplete("Invalid or conflicting verified listing alias")
        aliases[alias] = canonical
    seen = set()
    candidates = {}
    groups = {}
    for url in discovered:
        key = _stable_listing_key(url)
        if key:
            if key in seen:
                raise RefreshIncomplete(
                    "Multiple current URLs share a source listing identity"
                )
            seen.add(key)
            if key in stable and (len(stable[key]) > 1 or url not in known):
                canonical, matches = group(key)
                groups[canonical] = matches
                if (url not in aliases and url != canonical) or any(
                    item != canonical and aliases.get(item) != canonical
                    for item in matches
                ):
                    candidates[url] = canonical
                elif url != canonical:
                    aliases[url] = canonical
    return aliases, candidates, groups


def _verify_alias_detail(detail, row, url, summary):
    if (
        not isinstance(detail, dict)
        or detail.get("mls_number") != row["mls_number"]
        or not detail.get("city")
        or _town(detail["city"]) != _town(row["city"])
        or _town(row["city"]) != summary["discovery_town"]
        or (detail.get("detail_url") is not None and detail["detail_url"] != url)
    ):
        raise RefreshIncomplete("Unverified listing alias identity")


def _publish(db_path, manifest_path, stage_path, manifest, original_hash, temporary):
    from src.accepted_feed import publish_accepted

    if _sha256(db_path) != original_hash:
        raise RefreshIncomplete("Source database changed during refresh")
    wal = Path(str(db_path) + "-wal")
    if wal.exists() and wal.stat().st_size:
        raise RefreshIncomplete("Source database acquired uncheckpointed writes")
    manifest_stage = temporary / "manifest.json"
    manifest_stage.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    backup = temporary / "previous.db"
    shutil.copy2(db_path, backup)
    manifest_backup = temporary / "previous-manifest.json"
    if manifest_path.exists():
        shutil.copy2(manifest_path, manifest_backup)
    manifest_replaced = False
    os.replace(stage_path, db_path)
    try:
        os.replace(manifest_stage, manifest_path)
        manifest_replaced = True
        publish_accepted(
            db_path.resolve(), manifest_path.read_bytes(), db_path.parent / "accepted"
        )
    except BaseException:
        os.replace(backup, db_path)
        if manifest_replaced:
            if manifest_backup.exists():
                os.replace(manifest_backup, manifest_path)
            else:
                manifest_path.unlink()
        raise


def run_active_refresh(
    *,
    db_path: str | Path,
    manifest_path: str | Path,
    state_path: str | Path,
    towns: list[str],
    fetch_summary: Callable[[str, int], SummaryPage] | None = None,
    fetch_summary_range: Callable | None = None,
    fetch_detail: Callable[[str], dict | None],
    fetch_status: Callable[[str], dict | None],
    now: Callable[[], datetime] | None = None,
    max_pages: int = 90,
    run_id: str | None = None,
) -> dict:
    """Publish only a complete, checksum-bound Active coverage snapshot.

    The retry state must persist even when this function raises. Callbacks own
    pre-request budget reservations and must never return None on outages.
    """
    if type(max_pages) is not int or max_pages < 1:
        raise ValueError("max_pages must be a positive integer")
    if (fetch_summary is None) == (fetch_summary_range is None):
        raise ValueError("Select exactly one discovery mechanism")
    canonical_towns = sorted({_town(town) for town in towns})
    if not canonical_towns:
        raise RefreshIncomplete("At least one requested town is required")
    clock = now or (lambda: datetime.now(timezone.utc))
    run_id = run_id or uuid.uuid4().hex
    started_at = _timestamp(clock)
    db_path, manifest_path, state_path = map(Path, (db_path, manifest_path, state_path))
    if len({path.resolve() for path in (db_path, manifest_path, state_path)}) != 3:
        raise ValueError(
            "Database, manifest, and persistent retry state must be distinct"
        )
    if db_path.parent.resolve() != manifest_path.parent.resolve():
        raise ValueError("Database and manifest must share a publication directory")
    state_path.parent.mkdir(parents=True, exist_ok=True)
    with open(str(db_path) + ".refresh.lock", "a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        original_hash = _sha256(db_path)
        with sqlite3.connect(state_path, timeout=30) as state:
            state.execute("""CREATE TABLE IF NOT EXISTS new_listing_retries (
                detail_url TEXT PRIMARY KEY, attempts INTEGER NOT NULL,
                last_attempt_at TEXT NOT NULL, detail_json TEXT)""")
        range_result = None
        if fetch_summary_range is not None:
            from .price_discovery import discover_price_ranges

            # Only a complete, matching prior publication may supply hints.
            # Even valid hints are queried afresh; they never establish coverage.
            hints = None
            try:
                previous = json.loads(manifest_path.read_text())
                if (
                    isinstance(previous, dict)
                    and previous.get("complete") is True
                    and type(previous.get("schema_version")) is int
                    and previous["schema_version"] == 1
                    and previous.get("database_sha256") == original_hash
                ):
                    hints = previous.get("price_range_hints")
            except (OSError, ValueError):
                pass
            range_result = discover_price_ranges(
                canonical_towns, fetch_summary_range, hints=hints
            )
            discovered = range_result.listings
        else:
            discovered = _discover(canonical_towns, fetch_summary, max_pages)
        with tempfile.TemporaryDirectory(
            prefix=".active-refresh-", dir=db_path.parent
        ) as directory:
            temporary = Path(directory)
            stage_path = temporary / "snapshot.db"
            conn = _stage(db_path, stage_path)
            try:
                known = {
                    row["detail_url"]: dict(row)
                    for row in conn.execute("SELECT * FROM maine_transactions")
                }
                identities = {}
                for url, row in known.items():
                    identity = row.get("mls_number")
                    if identity:
                        identities.setdefault(identity, set()).add(url)
                aliases, candidates, alias_groups = _alias_resolution(
                    conn, known, identities, discovered
                )
                observed_urls = set()
                current_identities = {}
                unresolved = []
                for url, summary in discovered.items():
                    canonical = aliases.get(url, candidates.get(url, url))
                    if canonical in known:
                        row = known[canonical]
                        alias_detail = None
                        if url in candidates:
                            alias_identities = dict(identities)
                            alias_identities[row["mls_number"]] = identities[
                                row["mls_number"]
                            ] | {url}
                            detail = _retry_detail(
                                state_path,
                                url,
                                fetch_detail,
                                started_at,
                                dict(summary, mls_number=row["mls_number"]),
                                alias_identities,
                                validate=lambda detail: _verify_alias_detail(
                                    detail, row, url, summary
                                ),
                            )
                            if detail is None:
                                raise RefreshIncomplete(
                                    "Listing alias details unresolved or retry exhausted"
                                )
                            alias_detail = detail
                            for alias in set(alias_groups.get(canonical, [])) | {url}:
                                if alias == canonical or alias in aliases:
                                    continue
                                conn.execute(
                                    """INSERT INTO active_refresh_aliases
                                    (alias_url, canonical_url, mls_number, city, verified_at, run_id)
                                    VALUES (?, ?, ?, ?, ?, ?)""",
                                    (
                                        alias,
                                        canonical,
                                        row["mls_number"],
                                        row["city"],
                                        started_at,
                                        run_id,
                                    ),
                                )
                                aliases[alias] = canonical
                        for duplicate in alias_groups.get(canonical, []):
                            observed_urls.add(duplicate)
                        _identity(dict(summary, detail_url=canonical), row, identities)
                        if _town(row["city"]) != summary["discovery_town"]:
                            raise RefreshIncomplete("Known identity changed town")
                        status = (
                            alias_detail["status"]
                            if alias_detail
                            else summary["status"]
                        )
                        if status not in _STATUSES:
                            raise RefreshIncomplete("Invalid detail status")
                        if alias_detail:
                            conn.execute(
                                "UPDATE maine_transactions SET address=? WHERE detail_url=?",
                                (alias_detail["address"], canonical),
                            )
                        price = summary.get("list_price", row["list_price"])
                        if not _contract_ready(
                            dict(row, status=status, list_price=price)
                        ):
                            status = "Unverified"
                        conn.execute(
                            """UPDATE maine_transactions SET status=?, list_price=?,
                            last_seen_at=? WHERE detail_url=?""",
                            (status, price, started_at, canonical),
                        )
                        conn.commit()
                        write_history_if_changed(conn, canonical, status, price)
                    else:
                        detail = _retry_detail(
                            state_path,
                            url,
                            fetch_detail,
                            started_at,
                            summary,
                            identities,
                        )
                        if detail is None:
                            evidence = dict(
                                detail_url=url,
                                discovery_town=summary["discovery_town"],
                                reason="new_listing_details_unresolved_or_retry_exhausted",
                                observed_at=started_at,
                            )
                            unresolved.append(evidence)
                            conn.execute(
                                """INSERT INTO active_refresh_unresolved
                                (run_id, detail_url, discovery_town, reason, observed_at)
                                VALUES (?, ?, ?, ?, ?)""",
                                (
                                    run_id,
                                    url,
                                    evidence["discovery_town"],
                                    evidence["reason"],
                                    started_at,
                                ),
                            )
                            continue
                        _identity(detail, summary, identities)
                        status = _source_status(
                            detail.get("status") or summary["status"]
                        )
                        if status not in _STATUSES:
                            raise RefreshIncomplete("Invalid detail status")
                        upsert_listing(conn, detail)
                        enrich_listing(conn, url, dict(detail, status=status))
                        identities.setdefault(detail["mls_number"], set()).add(url)
                        row = detail
                    observed_urls.add(canonical)
                    identity = row.get("mls_number")
                    if identity:
                        if (
                            identity in current_identities
                            and current_identities[identity] != url
                        ):
                            raise RefreshIncomplete(
                                "Multiple current URLs share an MLS identity"
                            )
                        current_identities[identity] = url
                    _observe(
                        conn,
                        run_id,
                        row,
                        status,
                        (
                            "active_listing_contract_incomplete"
                            if status == "Unverified"
                            else "observed_in_complete_active_search"
                        ),
                        started_at,
                        "active_summary",
                    )
                # The independent legacy scraper may reactivate or reinsert a
                # verified secondary URL. Quarantine it even when absent from
                # today's discovery so the frozen reader cannot expose it.
                for alias in aliases:
                    row = known.get(alias)
                    if row is None or _town(row["city"]) not in canonical_towns:
                        continue
                    conn.execute(
                        "UPDATE maine_transactions SET status='Unverified' WHERE detail_url=?",
                        (alias,),
                    )
                    conn.commit()
                    write_history_if_changed(
                        conn, alias, "Unverified", row["list_price"]
                    )
                    _observe(
                        conn,
                        run_id,
                        row,
                        "Unverified",
                        "verified_source_alias_superseded",
                        started_at,
                        "verified_alias",
                    )
                for url, row in known.items():
                    # Ignore unrelated historical towns; malformed legacy towns
                    # cannot accidentally be assigned to requested coverage.
                    city = " ".join((row.get("city") or "").strip().lower().split())
                    if (
                        city not in canonical_towns
                        or url in aliases
                        or url in observed_urls
                        or row["status"] not in {"Active", "Unverified"}
                    ):
                        continue
                    detail = fetch_status(url)
                    if isinstance(detail, dict):
                        _identity(detail, row, identities)
                    status = detail.get("status") if isinstance(detail, dict) else None
                    status = _source_status(status)
                    if status not in _INACTIVE:
                        status = "Unverified"
                    reason = (
                        "absent_from_complete_active_search_status_confirmed"
                        if status != "Unverified"
                        else "absent_from_complete_active_search_status_unresolved"
                    )
                    conn.execute(
                        "UPDATE maine_transactions SET status=? WHERE detail_url=?",
                        (status, url),
                    )
                    conn.commit()
                    write_history_if_changed(conn, url, status, row["list_price"])
                    _observe(
                        conn, run_id, row, status, reason, started_at, "targeted_status"
                    )
                active_ids, inactive = [], []
                for record in conn.execute(
                    "SELECT * FROM maine_transactions ORDER BY mls_number"
                ):
                    row = dict(record)
                    if row["detail_url"] in aliases:
                        continue
                    city = " ".join((row.get("city") or "").strip().lower().split())
                    if city not in canonical_towns or not row.get("mls_number"):
                        continue
                    if row["status"] == "Active" and row["detail_url"] in observed_urls:
                        active_ids.append(str(row["mls_number"]))
                    elif _source_status(row["status"]) in _INACTIVE:
                        evidence = conn.execute(
                            """SELECT reason, observed_at FROM active_refresh_observations
                            WHERE detail_url=? ORDER BY id DESC LIMIT 1""",
                            (row["detail_url"],),
                        ).fetchone()
                        inactive.append(
                            dict(
                                mls_number=str(row["mls_number"]),
                                city=city,
                                status=_source_status(row["status"]),
                                reason=(
                                    evidence["reason"]
                                    if evidence
                                    else "previously_recorded_inactive_status"
                                ),
                                observed_at=(
                                    evidence["observed_at"]
                                    if evidence
                                    else _legacy_utc(row["scraped_at"])
                                ),
                                detail_url=row["detail_url"],
                            )
                        )
                grouped = {}
                for record in inactive:
                    if record["mls_number"] not in active_ids:
                        grouped.setdefault(record["mls_number"], []).append(record)
                inactive = []
                for identity in sorted(grouped):
                    records = grouped[identity]
                    latest = max(
                        records,
                        key=lambda item: (
                            datetime.fromisoformat(item["observed_at"]),
                            item["detail_url"],
                        ),
                    )
                    if len({item["status"] for item in records}) > 1:
                        latest = dict(
                            latest,
                            status="Unverified",
                            reason="conflicting_exact_mls_alias_statuses",
                        )
                    inactive.append(latest)
                conn.commit()
                if conn.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                    raise RefreshIncomplete("Staged database integrity check failed")
            finally:
                conn.close()
            manifest = dict(
                schema_version=1,
                run_id=run_id,
                started_at=started_at,
                completed_at=_timestamp(clock),
                complete=True,
                database_sha256=_sha256(stage_path),
                towns=canonical_towns,
                active_mls_ids=sorted(active_ids),
                inactive=inactive,
                unresolved=unresolved,
            )
            if range_result is not None:
                manifest["price_range_hints"] = range_result.hints
                manifest["discovery_requests"] = range_result.requests
            _publish(
                db_path, manifest_path, stage_path, manifest, original_hash, temporary
            )
            return manifest
