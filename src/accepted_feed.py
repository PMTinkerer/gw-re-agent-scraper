"""Append-only accepted snapshots; the final atomic pointer is the commit point."""

import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile


def bootstrap_accepted(
    database: Path,
    manifest_bytes: bytes,
    destination: Path,
    *,
    database_sha256: str,
    manifest_sha256: str,
) -> dict:
    """Publish recovered acceptance only when both independent historical pins match."""
    if (
        hashlib.sha256(Path(database).read_bytes()).hexdigest() != database_sha256
        or hashlib.sha256(manifest_bytes).hexdigest() != manifest_sha256
    ):
        raise ValueError("Recovered acceptance differs from pinned evidence")
    return publish_accepted(database, manifest_bytes, destination)


def publish_accepted(database: Path, manifest_bytes: bytes, destination: Path) -> dict:
    """Preserve exact accepted bytes. Caller must have completed discovery validation."""
    database, destination = Path(database), Path(destination)
    if database.is_symlink() or any(
        path.is_symlink()
        for path in (destination, destination / "bundles", destination / "current.json")
    ):
        raise ValueError("Accepted publication symlinks are forbidden")
    manifest = json.loads(manifest_bytes)
    db_bytes = database.read_bytes()
    db_hash = hashlib.sha256(db_bytes).hexdigest()
    if (
        manifest.get("complete") is not True
        or manifest.get("database_sha256") != db_hash
    ):
        raise ValueError("Accepted feed requires complete matching evidence")
    if not manifest.get("run_id") or not manifest.get("towns"):
        raise ValueError("Accepted feed requires run and town evidence")
    with sqlite3.connect(":memory:") as conn:
        conn.deserialize(db_bytes)
        if conn.execute("PRAGMA integrity_check").fetchall() != [("ok",)]:
            raise ValueError("Accepted database integrity failure")
    manifest_hash = hashlib.sha256(manifest_bytes).hexdigest()
    pointer = dict(
        schema_version=1, manifest_sha256=manifest_hash, database_sha256=db_hash
    )
    bundles = destination / "bundles"
    bundles.mkdir(parents=True, exist_ok=True)
    bundle = bundles / manifest_hash
    if any(
        path.is_symlink()
        for path in (
            bundle,
            bundle / "maine_listings.db",
            bundle / "active_refresh.json",
        )
    ):
        raise ValueError("Accepted publication symlinks are forbidden")
    if bundle.exists():
        if (bundle / "maine_listings.db").read_bytes() != db_bytes or (
            bundle / "active_refresh.json"
        ).read_bytes() != manifest_bytes:
            raise ValueError("Immutable accepted bundle collision")
    else:
        with tempfile.TemporaryDirectory(prefix=".stage-", dir=bundles) as temporary:
            stage = Path(temporary) / "bundle"
            stage.mkdir()
            for name, content in [
                ("maine_listings.db", db_bytes),
                ("active_refresh.json", manifest_bytes),
            ]:
                with (stage / name).open("xb") as stream:
                    stream.write(content)
                    stream.flush()
                    os.fsync(stream.fileno())
            os.rename(stage, bundle)
    with tempfile.NamedTemporaryFile(
        prefix=".current-", dir=destination, delete=False
    ) as stream:
        staged_pointer = Path(stream.name)
        stream.write((json.dumps(pointer, sort_keys=True) + "\n").encode())
        stream.flush()
        os.fsync(stream.fileno())
    try:
        os.replace(staged_pointer, destination / "current.json")
    finally:
        staged_pointer.unlink(missing_ok=True)
    return pointer
