"""Offline authoritative Active discovery and bounded enrichment behavior."""

import hashlib
import json
import sqlite3
from datetime import datetime, timezone

import pytest

from src.active_refresh import RefreshIncomplete, SummaryPage, run_active_refresh
from src.maine_database import get_connection, init_db, upsert_listing, enrich_listing


def card(key, town="York", **fields):
    return dict(
        detail_url=f"https://mainelistings.com/listings/{key}",
        city=town,
        status="Active",
        address=f"{key} Main",
        list_price=500000,
        **fields,
    )


@pytest.fixture
def setup(tmp_path):
    db = tmp_path / "listings.db"
    conn = get_connection(str(db))
    init_db(conn)
    for key, status in [("old", "Active"), ("gone", "Active"), ("legacy", "Closed")]:
        row = card(key)
        row["status"] = status
        upsert_listing(conn, row)
        enrich_listing(
            conn,
            row["detail_url"],
            dict(
                mls_number=key,
                status=status,
                list_date="2026-09-01",
                listing_agent_email="agent@example.test",
                photo_url="https://photo.test/old.jpg",
            ),
        )
    conn.close()
    return dict(
        db_path=db,
        manifest_path=tmp_path / "active_refresh.json",
        state_path=tmp_path / "refresh_state.db",
        towns=["York"],
    )


def execute(
    setup, cards=None, fetch_detail=None, fetch_status=None, fetch_summary=None
):
    cards = [card("old"), card("gone")] if cards is None else cards
    return run_active_refresh(
        **setup,
        fetch_summary=fetch_summary
        or (lambda town, page: SummaryPage(town, page, 1, len(cards), cards)),
        fetch_detail=fetch_detail
        or (
            lambda url: dict(
                mls_number=url.rsplit("/", 1)[1],
                status="Active",
                list_date="2026-09-01",
            )
        ),
        fetch_status=fetch_status or (lambda url: dict(status="Closed")),
    )


def rows(setup):
    with sqlite3.connect(setup["db_path"]) as conn:
        conn.row_factory = sqlite3.Row
        return {
            r["mls_number"]: dict(r)
            for r in conn.execute("SELECT * FROM maine_transactions")
        }


def test_repeated_mornings_only_new_urls_fetch_details_and_manifest_matches_database(
    setup,
):
    calls = []

    def detail(url):
        calls.append(url)
        return dict(mls_number="new", status="Active", list_date="2026-09-01")

    for _ in range(3):
        manifest = execute(
            setup, [card("old"), card("gone"), card("new")], fetch_detail=detail
        )
    assert calls == [card("new")["detail_url"]]
    assert manifest["active_mls_ids"] == ["gone", "new", "old"]
    assert manifest["schema_version"] == 1 and manifest["complete"] is True
    assert manifest["towns"] == ["york"]
    assert (
        manifest["database_sha256"]
        == hashlib.sha256(setup["db_path"].read_bytes()).hexdigest()
    )
    assert json.loads(setup["manifest_path"].read_text()) == manifest


def test_absent_status_preserves_contact_photo_and_records_reason(setup):
    manifest = execute(setup, [card("old")])
    gone = rows(setup)["gone"]
    assert gone["status"] == "Closed"
    assert gone["listing_agent_email"] == "agent@example.test"
    assert gone["photo_url"] == "https://photo.test/old.jpg"
    inactive = {row["mls_number"]: row for row in manifest["inactive"]}
    assert inactive["gone"]["status"] == "Closed"
    assert inactive["gone"]["reason"] and inactive["gone"]["observed_at"]
    assert inactive["legacy"]["status"] == "Closed"
    with sqlite3.connect(setup["db_path"]) as conn:
        assert (
            conn.execute(
                "SELECT status FROM maine_listing_history WHERE detail_url=? ORDER BY id DESC LIMIT 1",
                (card("gone")["detail_url"],),
            ).fetchone()[0]
            == "Closed"
        )
        assert (
            conn.execute("SELECT COUNT(*) FROM active_refresh_observations").fetchone()[
                0
            ]
            >= 2
        )


@pytest.mark.parametrize("status", [None, {}, {"status": "Mystery"}])
def test_ambiguous_absence_is_unverified_never_sold(setup, status):
    result = execute(setup, [card("old")], fetch_status=lambda _: status)
    assert rows(setup)["gone"]["status"] == "Unverified"
    assert (
        next(row for row in result["inactive"] if row["mls_number"] == "gone")["status"]
        == "Unverified"
    )


def test_empty_complete_run_and_reappearance_keep_identity_history(setup):
    original = rows(setup)["old"]
    manifest = execute(setup, [])
    assert manifest["active_mls_ids"] == []
    called = []
    execute(setup, [card("old")], fetch_detail=lambda url: called.append(url))
    assert called == []
    assert rows(setup)["old"]["id"] == original["id"]
    assert rows(setup)["old"]["status"] == "Active"


def test_old_unenriched_rows_never_backfilled(setup):
    conn = get_connection(str(setup["db_path"]))
    upsert_listing(conn, card("unresolved"))
    conn.close()
    calls = []
    execute(
        setup,
        [card("old"), card("gone"), card("unresolved")],
        fetch_detail=lambda url: calls.append(url),
    )
    assert calls == []


def test_new_invalid_detail_retries_twice_across_mornings_then_stops(setup):
    calls = []
    for _ in range(4):
        result = execute(
            setup,
            [card("old"), card("gone"), card("new")],
            fetch_detail=lambda url: calls.append(url),
        )
    assert len(calls) == 2
    assert "new" not in result["active_mls_ids"]
    assert "new" not in rows(setup)


def test_detail_timeout_attempt_persists_even_when_run_fails(setup):
    calls = []

    def fail(url):
        calls.append(url)
        raise TimeoutError("provider unavailable")

    original = setup["db_path"].read_bytes()
    for _ in range(2):
        with pytest.raises(TimeoutError):
            execute(setup, [card("new")], fetch_detail=fail)
        assert setup["db_path"].read_bytes() == original
        assert not setup["manifest_path"].exists()
    execute(setup, [card("new")], fetch_detail=fail)
    assert len(calls) == 2


@pytest.mark.parametrize(
    "failure",
    ["partial", "duplicate", "wrongtown", "pagecount", "timeout", "captcha", "cap"],
)
def test_discovery_failure_leaves_database_and_manifest_unchanged(setup, failure):
    execute(setup)
    before = setup["db_path"].read_bytes(), setup["manifest_path"].read_bytes()

    def summary(town, page):
        if failure in ("timeout", "captcha", "cap"):
            raise RuntimeError(failure)
        if failure == "partial":
            return SummaryPage(town, page, 1, 2, [card("old")])
        if failure == "duplicate":
            return SummaryPage(town, page, 1, 2, [card("old"), card("old")])
        if failure == "wrongtown":
            return SummaryPage("Wells", page, 1, 1, [card("old")])
        return SummaryPage(
            town, page, 2 if page == 1 else 3, 2, [card("old" if page == 1 else "gone")]
        )

    with pytest.raises((RefreshIncomplete, RuntimeError)):
        execute(setup, fetch_summary=summary)
    assert (
        setup["db_path"].read_bytes(),
        setup["manifest_path"].read_bytes(),
    ) == before


def test_pages_continue_after_known_urls_and_cover_every_town(setup):
    setup["towns"] = ["York", "Wells"]
    calls = []

    def summary(town, page):
        calls.append((town, page))
        if town == "wells":
            return SummaryPage(town, page, 1, 0, [])
        return SummaryPage(town, page, 2, 2, [card("old" if page == 1 else "gone")])

    result = execute(setup, fetch_summary=summary)
    assert set(calls) == {("york", 1), ("york", 2), ("wells", 1)}
    assert result["towns"] == ["wells", "york"]


@pytest.mark.parametrize("budget_exhausted", [False, True])
def test_failed_discovery_retry_preserves_published_bytes(setup, budget_exhausted):
    execute(setup)
    before = setup["db_path"].read_bytes(), setup["manifest_path"].read_bytes()
    calls, details = [], []

    def summary(town, page):
        calls.append((town, page))
        if budget_exhausted and len(calls) == 2:
            raise RuntimeError("budget exhausted before retry request")
        return SummaryPage(town, page, 1, 2, [card("new"), card("new")])

    with pytest.raises((RefreshIncomplete, RuntimeError)):
        execute(setup, fetch_summary=summary, fetch_detail=details.append)
    assert calls == [("york", 1), ("york", 1)]
    assert details == []
    assert (
        setup["db_path"].read_bytes(),
        setup["manifest_path"].read_bytes(),
    ) == before
    with sqlite3.connect(setup["state_path"]) as conn:
        assert (
            conn.execute("SELECT COUNT(*) FROM new_listing_retries").fetchone()[0] == 0
        )


def test_successful_discovery_retry_enriches_only_final_new_home_once(setup):
    before = rows(setup)
    calls, details = [], []

    def summary(town, page):
        calls.append(page)
        if len(calls) == 1:
            return SummaryPage(town, page, 2, 3, [card("stale"), card("old")])
        if len(calls) == 2:
            return SummaryPage(town, page, 2, 3, [card("old")])
        return SummaryPage(
            town,
            page,
            2,
            3,
            [card("old"), card("gone")] if page == 1 else [card("new")],
        )

    def detail(url):
        details.append(url)
        return dict(mls_number="new", status="Active", list_date="2026-09-01")

    result = execute(setup, fetch_summary=summary, fetch_detail=detail)
    assert calls == [1, 2, 1, 2]
    assert details == [card("new")["detail_url"]]
    assert result["active_mls_ids"] == ["gone", "new", "old"]
    assert (
        result["database_sha256"]
        == hashlib.sha256(setup["db_path"].read_bytes()).hexdigest()
    )
    assert json.loads(setup["manifest_path"].read_text()) == result
    after = rows(setup)
    assert "stale" not in after
    for key in ("old", "gone"):
        for field in (
            "id",
            "mls_number",
            "list_date",
            "listing_agent_email",
            "photo_url",
        ):
            assert after[key][field] == before[key][field]
    execute(setup, [card("old"), card("gone"), card("new")], fetch_detail=detail)
    assert details == [card("new")["detail_url"]]


def test_discovery_retry_cannot_bypass_persistent_budget_reservations(setup, tmp_path):
    from src.refresh_budget import BudgetExceeded, BudgetLedger

    execute(setup)
    before = setup["db_path"].read_bytes(), setup["manifest_path"].read_bytes()
    budget_path = tmp_path / "budget.db"
    ledger = BudgetLedger(budget_path, daily_limit=15, read_balance=lambda: 1000)
    prior_id = ledger.reserve("prior-run", "summary", "prior/1")
    with sqlite3.connect(budget_path) as conn:
        prior = conn.execute(
            "SELECT * FROM reservations WHERE reservation_id=?", (prior_id,)
        ).fetchone()
    requested, fetched = [], []

    def summary(town, page):
        requested.append(page)
        ledger.reserve("retry-run", "summary", f"{town}/{page}")
        fetched.append(page)
        return SummaryPage(town, page, 2, 2, [card("old")])

    with pytest.raises(BudgetExceeded):
        execute(setup, fetch_summary=summary)
    assert requested == [1, 2, 1] and fetched == [1, 2]
    assert (
        setup["db_path"].read_bytes(),
        setup["manifest_path"].read_bytes(),
    ) == before
    with sqlite3.connect(budget_path) as conn:
        assert (
            conn.execute(
                "SELECT * FROM reservations WHERE reservation_id=?", (prior_id,)
            ).fetchone()
            == prior
        )
        assert conn.execute(
            "SELECT COUNT(*), SUM(reserved_units) FROM reservations"
        ).fetchone() == (3, 15)


def test_mls_collision_aborts_without_publication(setup):
    original = setup["db_path"].read_bytes()
    with pytest.raises(RefreshIncomplete, match="identity"):
        execute(
            setup,
            [card("old"), card("new")],
            fetch_detail=lambda _: dict(mls_number="old", status="Active"),
        )
    assert setup["db_path"].read_bytes() == original


def test_absent_status_identity_change_rejected(setup):
    with pytest.raises(RefreshIncomplete, match="identity"):
        execute(
            setup,
            [card("old")],
            fetch_status=lambda _: dict(mls_number="unexpected", status="Closed"),
        )


def test_status_transport_failure_aborts_all_updates(setup):
    before = setup["db_path"].read_bytes()

    def unavailable(url):
        raise TimeoutError("offline")

    with pytest.raises(TimeoutError):
        execute(setup, [card("old")], fetch_status=unavailable)
    assert setup["db_path"].read_bytes() == before


def test_publication_manifest_replace_failure_rolls_back_database(setup, monkeypatch):
    execute(setup)
    before = setup["db_path"].read_bytes(), setup["manifest_path"].read_bytes()
    import src.active_refresh as refresh

    real_replace = refresh.os.replace

    def fail_manifest(source, destination):
        if str(destination) == str(setup["manifest_path"]):
            raise OSError("disk problem")
        return real_replace(source, destination)

    monkeypatch.setattr(refresh.os, "replace", fail_manifest)
    with pytest.raises(OSError):
        execute(setup, [])
    assert (
        setup["db_path"].read_bytes(),
        setup["manifest_path"].read_bytes(),
    ) == before


def test_historical_inactive_observation_has_explicit_utc_timestamp(setup):
    result = execute(setup)
    legacy = next(row for row in result["inactive"] if row["mls_number"] == "legacy")
    timestamp = datetime.fromisoformat(legacy["observed_at"])
    assert timestamp.tzinfo is timezone.utc
    assert timestamp <= datetime.fromisoformat(result["completed_at"])


@pytest.mark.parametrize(
    "status", ["Closed", "Sold", "Pending", "Withdrawn", "Cancelled"]
)
def test_each_explicit_inactive_status_is_retained(setup, status):
    result = execute(setup, [card("old")], fetch_status=lambda _: dict(status=status))
    assert (
        next(row for row in result["inactive"] if row["mls_number"] == "gone")["status"]
        == status
    )


def test_successful_new_detail_cached_when_later_status_check_aborts(setup):
    calls = []

    def detail(url):
        calls.append(url)
        return dict(mls_number="new", status="Active", list_date="2026-09-01")

    def failed_status(url):
        raise TimeoutError("provider offline")

    with pytest.raises(TimeoutError):
        execute(
            setup,
            [card("old"), card("new")],
            fetch_detail=detail,
            fetch_status=failed_status,
        )
    result = execute(setup, [card("old"), card("new")], fetch_detail=detail)
    assert calls == [card("new")["detail_url"]]
    assert "new" in result["active_mls_ids"]


def test_provider_detail_with_different_url_is_rejected(setup):
    with pytest.raises(RefreshIncomplete, match="identity"):
        execute(
            setup,
            [card("new")],
            fetch_detail=lambda _: dict(
                mls_number="new", detail_url=card("different")["detail_url"]
            ),
        )


def test_page_ceiling_aborts_before_enrichment(setup):
    calls = []
    with pytest.raises(RefreshIncomplete):
        execute(
            setup,
            fetch_summary=lambda town, page: SummaryPage(
                town, page, 91, 91, [card("new")]
            ),
            fetch_detail=lambda url: calls.append(url),
        )
    assert calls == []


def test_external_database_change_is_not_overwritten(setup):
    def summary(town, page):
        with sqlite3.connect(setup["db_path"]) as conn:
            conn.execute(
                "UPDATE maine_transactions SET address='External change' WHERE mls_number='old'"
            )
        conn.close()
        return SummaryPage(town, page, 1, 2, [card("old"), card("gone")])

    with pytest.raises(RefreshIncomplete, match="changed"):
        execute(setup, fetch_summary=summary)
    assert rows(setup)["old"]["address"] == "External change"
    assert not setup["manifest_path"].exists()


def test_new_listing_preserves_display_city_spelling(setup):
    execute(setup, [card("new")])
    assert rows(setup)["new"]["city"] == "York"


def add_alias(setup, key, identity, status):
    conn = get_connection(str(setup["db_path"]))
    data = card(key)
    data["status"] = status
    upsert_listing(conn, data)
    enrich_listing(
        conn,
        data["detail_url"],
        dict(mls_number=identity, status=status, list_date="2026-09-01"),
    )
    conn.close()


def test_one_current_exact_url_resolves_legacy_alias_without_merging_rows(setup):
    add_alias(setup, "old-alias", "old", "Active")
    result = execute(setup)
    assert result["active_mls_ids"].count("old") == 1
    assert all(row["mls_number"] != "old" for row in result["inactive"])
    with sqlite3.connect(setup["db_path"]) as conn:
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM maine_transactions WHERE mls_number='old'"
            ).fetchone()[0]
            == 2
        )


def test_two_current_urls_sharing_identity_fail_closed(setup):
    add_alias(setup, "old-alias", "old", "Active")
    with pytest.raises(RefreshIncomplete, match="identity"):
        execute(setup, [card("old"), card("old-alias"), card("gone")])


def test_historical_aliases_deduplicate_inactive_evidence(setup):
    add_alias(setup, "legacy-alias", "legacy", "Closed")
    result = execute(setup)
    inactive = [row for row in result["inactive"] if row["mls_number"] == "legacy"]
    assert len(inactive) == 1 and inactive[0]["status"] == "Closed"


def test_conflicting_historical_aliases_are_unverified(setup):
    add_alias(setup, "legacy-alias", "legacy", "Withdrawn")
    result = execute(setup)
    inactive = [row for row in result["inactive"] if row["mls_number"] == "legacy"]
    assert len(inactive) == 1 and inactive[0]["status"] == "Unverified"
    assert inactive[0]["reason"] == "conflicting_exact_mls_alias_statuses"


def test_under_contract_maps_to_pending_evidence(setup):
    add_alias(setup, "contract", "contract", "Active Under Contract")
    result = execute(setup)
    assert (
        next(row for row in result["inactive"] if row["mls_number"] == "contract")[
            "status"
        ]
        == "Pending"
    )


def test_targeted_under_contract_response_maps_to_pending(setup):
    execute(
        setup,
        [card("old")],
        fetch_status=lambda _: dict(status="Active Under Contract"),
    )
    assert rows(setup)["gone"]["status"] == "Pending"


def test_partial_detail_is_not_cached_or_published_and_retry_is_bounded(setup):
    calls = []

    def partial(url):
        calls.append(url)
        return dict(mls_number="new", status="Active")

    for _ in range(3):
        result = execute(
            setup, [card("old"), card("gone"), card("new")], fetch_detail=partial
        )
        assert result["active_mls_ids"] == ["gone", "old"]
        assert "new" not in rows(setup)
    assert len(calls) == 2
    with sqlite3.connect(setup["state_path"]) as conn:
        assert conn.execute(
            "SELECT attempts, detail_json FROM new_listing_retries"
        ).fetchone() == (2, None)


@pytest.mark.parametrize(
    "field,value",
    [
        ("list_date", ""),
        ("property_type", ""),
        ("photo_url", "not-a-url"),
        ("listing_agent_email", "bad"),
        ("listing_office", 12),
    ],
)
def test_malformed_detail_contract_never_cached(setup, field, value):
    detail = dict(mls_number="new", status="Active", list_date="2026-09-01")
    detail[field] = value
    result = execute(setup, [card("new")], fetch_detail=lambda _: detail)
    assert "new" not in result["active_mls_ids"]
    with sqlite3.connect(setup["state_path"]) as conn:
        assert (
            conn.execute("SELECT detail_json FROM new_listing_retries").fetchone()[0]
            is None
        )


@pytest.mark.parametrize(
    "field,value",
    [
        ("list_price", 0),
        ("list_price", -1),
        ("beds", -1),
        ("beds", 1.5),
        ("beds", True),
        ("baths", float("inf")),
        ("baths", -0.5),
        ("address", " "),
    ],
)
def test_malformed_merged_summary_contract_never_cached(setup, field, value):
    listing = card("new")
    listing[field] = value
    result = execute(setup, [listing])
    assert "new" not in result["active_mls_ids"]


def test_known_unusable_row_is_unverified_without_detail_fetch(setup):
    with sqlite3.connect(setup["db_path"]) as conn:
        conn.execute(
            "UPDATE maine_transactions SET list_date=NULL WHERE mls_number='old'"
        )
    conn.close()
    calls = []
    result = execute(setup, fetch_detail=lambda url: calls.append(url))
    assert result["active_mls_ids"] == ["gone"]
    old = next(row for row in result["inactive"] if row["mls_number"] == "old")
    assert old["status"] == "Unverified"
    assert old["reason"] == "active_listing_contract_incomplete"
    assert calls == []
    assert rows(setup)["old"]["listing_agent_email"] == "agent@example.test"


@pytest.mark.parametrize(
    "cached_status,current_status", [("Active", "Pending"), ("Pending", "Active")]
)
def test_cached_detail_cannot_override_current_summary_status_or_price(
    setup, cached_status, current_status
):
    calls = []

    def detail(url):
        calls.append(url)
        return dict(
            mls_number="new",
            status=cached_status,
            list_date="2026-09-01",
            list_price=900000,
        )

    def unavailable(url):
        raise TimeoutError("offline")

    with pytest.raises(TimeoutError):
        execute(
            setup,
            [card("old"), card("new")],
            fetch_detail=detail,
            fetch_status=unavailable,
        )
    current = card("new")
    current.update(status=current_status, list_price=450000)
    execute(setup, [card("old"), current], fetch_detail=detail)
    assert len(calls) == 1
    assert rows(setup)["new"]["status"] == current_status
    assert rows(setup)["new"]["list_price"] == 450000


def test_fresh_detail_cannot_make_pending_summary_active(setup):
    current = card("new")
    current["status"] = "Pending"
    execute(setup, [current])
    assert rows(setup)["new"]["status"] == "Pending"


def test_fresh_detail_can_restrict_active_summary_to_pending(setup):
    execute(
        setup,
        [card("new")],
        fetch_detail=lambda _: dict(
            mls_number="new", status="Pending", list_date="2026-09-01"
        ),
    )
    assert rows(setup)["new"]["status"] == "Pending"


def test_original_detail_city_mismatch_cannot_be_hidden_by_summary_merge(setup):
    with pytest.raises(RefreshIncomplete, match="identity"):
        execute(
            setup,
            [card("new")],
            fetch_detail=lambda _: dict(
                mls_number="new", city="Wells", list_date="2026-09-01"
            ),
        )


def test_partial_detail_can_recover_on_its_last_allowed_attempt(setup):
    execute(setup, [card("new")], fetch_detail=lambda _: dict(mls_number="new"))
    result = execute(setup, [card("new")])
    assert result["active_mls_ids"] == ["new"]
    with sqlite3.connect(setup["state_path"]) as conn:
        attempts, cached = conn.execute(
            "SELECT attempts, detail_json FROM new_listing_retries"
        ).fetchone()
        assert attempts == 2 and json.loads(cached)["list_date"] == "2026-09-01"
