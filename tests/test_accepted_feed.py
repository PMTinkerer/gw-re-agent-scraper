import hashlib
import json

import pytest

from tests.test_active_refresh import execute, card

pytest_plugins = ["tests.test_active_refresh"]


def test_refresh_publishes_immutable_bundle_isolated_from_legacy(setup):
    execute(setup)
    root = setup["db_path"].parent / "accepted"
    assert (root / "current.json").exists()
    pointer = json.loads((root / "current.json").read_text())
    bundle = root / "bundles" / pointer["manifest_sha256"]
    before = (bundle / "maine_listings.db").read_bytes()
    setup["db_path"].write_bytes(b"legacy weekly mutation")
    assert (bundle / "maine_listings.db").read_bytes() == before
    assert hashlib.sha256(before).hexdigest() == pointer["database_sha256"]


def test_next_acceptance_advances_pointer_failed_publication_retains_old(
    setup, monkeypatch
):
    execute(setup)
    pointer = setup["db_path"].parent / "accepted/current.json"
    assert pointer.exists()
    old = pointer.read_bytes()
    execute(setup, [card("old")])
    assert pointer.read_bytes() != old
    accepted = pointer.read_bytes()
    legacy_before = setup["db_path"].read_bytes(), setup["manifest_path"].read_bytes()
    import src.accepted_feed as publication

    original = publication.os.replace

    def fail_pointer(source, destination):
        if str(destination) == str(pointer):
            raise OSError("injected pointer failure")
        return original(source, destination)

    monkeypatch.setattr(publication.os, "replace", fail_pointer)
    with pytest.raises(OSError, match="pointer failure"):
        execute(setup)
    assert pointer.read_bytes() == accepted
    assert (
        setup["db_path"].read_bytes(),
        setup["manifest_path"].read_bytes(),
    ) == legacy_before


def test_bootstrap_preserves_exact_evidence_and_rejects_wrong_pins(setup, tmp_path):
    execute(setup)
    import src.accepted_feed as publication

    bootstrap = getattr(publication, "bootstrap_accepted", None)
    assert callable(bootstrap)
    db = setup["db_path"]
    manifest = setup["manifest_path"].read_bytes()
    db_pin = hashlib.sha256(db.read_bytes()).hexdigest()
    manifest_pin = hashlib.sha256(manifest).hexdigest()
    destination = tmp_path / "bootstrap"
    bootstrap(
        db, manifest, destination, database_sha256=db_pin, manifest_sha256=manifest_pin
    )
    assert (
        destination / "bundles" / manifest_pin / "active_refresh.json"
    ).read_bytes() == manifest
    before = (destination / "current.json").read_bytes()
    with pytest.raises(ValueError, match="pinned"):
        bootstrap(
            db,
            manifest + b" ",
            destination,
            database_sha256=db_pin,
            manifest_sha256=manifest_pin,
        )
    assert (destination / "current.json").read_bytes() == before


def test_publisher_refuses_symlink_destination_and_preserves_old_pointer(
    setup, tmp_path
):
    execute(setup)
    import src.accepted_feed as publication

    original = setup["db_path"].parent / "accepted"
    before = (original / "current.json").read_bytes()
    link = tmp_path / "linked-accepted"
    link.symlink_to(original, target_is_directory=True)
    with pytest.raises(ValueError, match="symlink"):
        publication.publish_accepted(
            setup["db_path"], setup["manifest_path"].read_bytes(), link
        )
    assert (original / "current.json").read_bytes() == before
