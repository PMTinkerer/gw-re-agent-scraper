"""Verified address-slug corrections retain the original listing and history."""

import sqlite3

import pytest

from src.active_refresh import RefreshIncomplete, SummaryPage, run_active_refresh
from src.maine_database import get_connection, init_db, upsert_listing, enrich_listing

OLD = "https://mainelistings.com/listings/ME/Biddeford/04005/10376/1-willow-ridge-ridge/782016957"
NEW = OLD.replace("ridge-ridge", "ridge-road")


@pytest.fixture
def source(tmp_path):
    config = dict(
        db_path=tmp_path / "source.db",
        manifest_path=tmp_path / "manifest.json",
        state_path=tmp_path / "retry.db",
        towns=["Biddeford"],
    )
    conn = get_connection(str(config["db_path"]))
    init_db(conn)
    upsert_listing(conn, card(OLD))
    enrich_listing(
        conn,
        OLD,
        dict(
            mls_number="1672938",
            status="Active",
            list_date="2026-09-01",
            listing_agent_email="old@example.test",
        ),
    )
    conn.close()
    return config


def card(url=NEW):
    return dict(
        detail_url=url,
        city="Biddeford",
        address="1 Willow Ridge Road",
        status="Active",
        list_price=1099000,
    )


def detail(url):
    return dict(
        detail_url=url,
        city="Biddeford",
        mls_number="1672938",
        list_date="2026-09-01",
        status="Active",
    )


def run(source, cards=None, fetch_detail=detail, fetch_status=None):
    cards = [card()] if cards is None else cards
    return run_active_refresh(
        **source,
        fetch_summary=lambda town, page: SummaryPage(town, page, 1, len(cards), cards),
        fetch_detail=fetch_detail,
        fetch_status=fetch_status
        or (lambda url: pytest.fail("Unexpected absence check"))
    )


def test_verified_slug_change_preserves_row_and_reuses_alias(source):
    calls = []

    def fetch(url):
        calls.append(url)
        return detail(url)

    with sqlite3.connect(source["db_path"]) as conn:
        before = conn.execute(
            "SELECT id, detail_url, listing_agent_email FROM maine_transactions"
        ).fetchall()
        history = conn.execute("SELECT * FROM maine_listing_history").fetchall()
    for _ in range(3):
        assert run(source, fetch_detail=fetch)["active_mls_ids"] == ["1672938"]
    assert calls == [NEW]
    with sqlite3.connect(source["db_path"]) as conn:
        assert (
            conn.execute(
                "SELECT id, detail_url, listing_agent_email FROM maine_transactions"
            ).fetchall()
            == before
        )
        assert (
            conn.execute("SELECT * FROM maine_listing_history").fetchall()[
                : len(history)
            ]
            == history
        )
        assert conn.execute(
            "SELECT alias_url, canonical_url, mls_number FROM active_refresh_aliases"
        ).fetchall() == [(NEW, OLD, "1672938")]
    assert run(
        source, cards=[card(OLD)], fetch_detail=lambda _: pytest.fail("Known URL")
    )["active_mls_ids"] == ["1672938"]


@pytest.mark.parametrize(
    "changes",
    [{"mls_number": "other"}, {"city": "York"}, {"city": None}, {"detail_url": OLD}],
)
def test_alias_requires_explicit_matching_identity(source, changes):
    original = source["db_path"].read_bytes()
    with pytest.raises(RefreshIncomplete):
        run(source, fetch_detail=lambda url: dict(detail(url), **changes))
    assert source["db_path"].read_bytes() == original


def test_two_current_slugs_for_same_listing_fail_before_detail(source):
    with pytest.raises(RefreshIncomplete):
        run(
            source,
            cards=[card(OLD), card(NEW)],
            fetch_detail=lambda _: pytest.fail("Duplicate must fail first"),
        )


def test_different_source_id_does_not_alias_same_mls(source):
    with pytest.raises(RefreshIncomplete, match="MLS identity"):
        run(source, cards=[card(NEW.replace("782016957", "656591652"))])


def test_alias_absence_keeps_original_status_history(source):
    run(source)
    calls = []

    def status(url):
        calls.append(url)
        return dict(mls_number="1672938", city="Biddeford", status="Sold")

    result = run(source, cards=[], fetch_status=status)
    assert calls == [OLD]
    assert result["active_mls_ids"] == []
    assert result["inactive"][0]["detail_url"] == OLD


def test_invalid_persisted_alias_fails_closed(source):
    run(source)
    with sqlite3.connect(source["db_path"]) as conn:
        conn.execute("UPDATE active_refresh_aliases SET mls_number='changed'")
    with pytest.raises(RefreshIncomplete):
        run(source)


@pytest.mark.parametrize(
    "observed", [NEW, OLD, OLD.replace("ridge-ridge", "ridge-lane")]
)
def test_verified_existing_duplicate_group_has_one_active_row(source, observed):
    duplicate = OLD.replace("ridge-ridge", "ridge-lane")
    conn = get_connection(str(source["db_path"]))
    upsert_listing(conn, card(duplicate))
    enrich_listing(
        conn,
        duplicate,
        dict(
            mls_number="1672938",
            city="Biddeford",
            status="Active",
            list_date="2026-09-01",
        ),
    )
    conn.close()
    calls = []

    def fetch(url):
        calls.append(url)
        return detail(url)

    for _ in range(2):
        assert run(source, cards=[card(observed)], fetch_detail=fetch)[
            "active_mls_ids"
        ] == ["1672938"]
    assert calls == [observed]
    with sqlite3.connect(source["db_path"]) as conn:
        assert conn.execute(
            "SELECT detail_url,status FROM maine_transactions ORDER BY id"
        ).fetchall() == [(OLD, "Active"), (duplicate, "Unverified")]
        assert (
            conn.execute(
                "SELECT reason FROM active_refresh_observations WHERE detail_url=? ORDER BY id DESC LIMIT 1",
                (duplicate,),
            ).fetchone()[0]
            == "verified_source_alias_superseded"
        )
    result = run(
        source,
        cards=[],
        fetch_status=lambda url: (
            dict(status="Sold")
            if url == OLD
            else pytest.fail("Superseded URL status request")
        ),
    )
    assert result["inactive"][0]["status"] == "Sold"


def test_alias_detail_confirmed_pending_holds_out_from_active_feed(source):
    result = run(source, fetch_detail=lambda url: dict(detail(url), status="Pending"))
    assert result["active_mls_ids"] == []
    assert result["inactive"][0]["status"] == "Pending"


@pytest.mark.parametrize(
    "duplicate_fields", [{"mls_number": "other"}, {"city": "York"}]
)
def test_conflicting_historical_group_never_merged(source, duplicate_fields):
    duplicate = OLD.replace("ridge-ridge", "ridge-lane")
    conn = get_connection(str(source["db_path"]))
    init_db(conn)
    upsert_listing(conn, card(duplicate))
    enrich_listing(
        conn,
        duplicate,
        dict(
            mls_number="1672938",
            city="Biddeford",
            status="Active",
            list_date="2026-09-01",
        )
        | duplicate_fields,
    )
    if "city" in duplicate_fields:
        conn.execute(
            "UPDATE maine_transactions SET city=? WHERE detail_url=?",
            (duplicate_fields["city"], duplicate),
        )
        conn.commit()
    conn.close()
    original = source["db_path"].read_bytes()
    with pytest.raises(RefreshIncomplete):
        run(
            source,
            fetch_detail=lambda _: pytest.fail(
                "Conflicting group must fail before transport"
            ),
        )
    assert source["db_path"].read_bytes() == original


def test_alias_staged_rollback_reuses_bounded_verified_detail(source):
    other = OLD.replace("782016957", "123456789")
    conn = get_connection(str(source["db_path"]))
    upsert_listing(conn, card(other))
    enrich_listing(
        conn, other, dict(mls_number="other", status="Active", list_date="2026-09-01")
    )
    conn.close()
    original = source["db_path"].read_bytes()
    calls = []

    def fetch(url):
        calls.append(url)
        return detail(url)

    def unavailable(url):
        raise RefreshIncomplete("Status transport unavailable")

    with pytest.raises(RefreshIncomplete, match="Status transport unavailable"):
        run(source, fetch_detail=fetch, fetch_status=unavailable)
    assert source["db_path"].read_bytes() == original
    assert not source["manifest_path"].exists()
    result = run(source, fetch_detail=fetch, fetch_status=lambda _: dict(status="Sold"))
    assert result["active_mls_ids"] == ["1672938"]
    assert calls == [NEW]


@pytest.mark.parametrize(
    "url",
    [
        NEW.replace("04005", "04006"),
        NEW.replace("10376", "10377"),
        NEW + "/",
        NEW.replace("782016957", "x782016957"),
    ],
)
def test_changed_source_prefix_or_malformed_id_cannot_alias(source, url):
    original = source["db_path"].read_bytes()
    with pytest.raises(RefreshIncomplete, match="MLS identity"):
        run(source, cards=[card(url)])
    assert source["db_path"].read_bytes() == original


def test_legacy_reactivated_alias_is_demoted_even_when_no_longer_discovered(source):
    run(source)
    conn = get_connection(str(source["db_path"]))
    upsert_listing(conn, card(NEW))
    enrich_listing(
        conn, NEW, dict(mls_number="1672938", status="Active", list_date="2026-09-01")
    )
    conn.close()
    result = run(
        source,
        cards=[],
        fetch_status=lambda url: (
            dict(status="Sold")
            if url == OLD
            else pytest.fail("No secondary status request")
        ),
    )
    assert result["active_mls_ids"] == []
    assert result["inactive"][0]["status"] == "Sold"
    with sqlite3.connect(source["db_path"]) as conn:
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM maine_transactions WHERE status='Active'"
            ).fetchone()[0]
            == 0
        )
        assert (
            conn.execute(
                "SELECT status FROM maine_transactions WHERE detail_url=?", (NEW,)
            ).fetchone()[0]
            == "Unverified"
        )
        assert (
            conn.execute(
                "SELECT reason FROM active_refresh_observations WHERE detail_url=? ORDER BY id DESC LIMIT 1",
                (NEW,),
            ).fetchone()[0]
            == "verified_source_alias_superseded"
        )


def test_alias_missing_mls_exhausts_two_attempts_without_publishing(source):
    original = source["db_path"].read_bytes()
    calls = []

    def missing_mls(url):
        calls.append(url)
        return dict(detail(url), mls_number=None)

    for attempt in range(3):
        with pytest.raises(RefreshIncomplete, match="alias"):
            run(source, fetch_detail=missing_mls)
        assert source["db_path"].read_bytes() == original
        assert not source["manifest_path"].exists()
        with sqlite3.connect(source["state_path"]) as conn:
            assert conn.execute(
                "SELECT attempts, detail_json FROM new_listing_retries WHERE detail_url=?",
                (NEW,),
            ).fetchone() == (min(attempt + 1, 2), None)
    assert calls == [NEW, NEW]


def test_cached_alias_identity_is_revalidated_before_use(source):
    import json

    # An unsuccessful live proof creates the bounded retry record but cannot
    # publish an alias. Simulate a stale/corrupted cache from another attempt.
    original = source["db_path"].read_bytes()
    with pytest.raises(RefreshIncomplete):
        run(source, fetch_detail=lambda url: dict(detail(url), mls_number=None))
    with sqlite3.connect(source["state_path"]) as conn:
        conn.execute(
            "UPDATE new_listing_retries SET detail_json=? WHERE detail_url=?",
            (json.dumps(dict(detail(NEW), mls_number="different")), NEW),
        )
    with pytest.raises(RefreshIncomplete, match="alias"):
        run(
            source,
            fetch_detail=lambda _: pytest.fail(
                "Invalid cache must not trigger another request"
            ),
        )
    assert source["db_path"].read_bytes() == original
    assert not source["manifest_path"].exists()
    with sqlite3.connect(source["state_path"]) as conn:
        assert (
            conn.execute(
                "SELECT attempts FROM new_listing_retries WHERE detail_url=?", (NEW,)
            ).fetchone()[0]
            == 1
        )
