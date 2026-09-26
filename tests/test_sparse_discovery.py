"""Synthetic missing-fact reproductions, not captured failed provider output."""

import sqlite3

import pytest

from src.active_refresh import RefreshIncomplete, SummaryPage, _discover
from src.incremental_cards import parse_active_cards
from tests.test_active_refresh import card, execute, rows, setup as _setup

setup = _setup


def test_addressless_public_card_counts_without_invented_facts():
    # Synthetic representation of the source-visible Old Orchard Beach land card.
    text = (
        "[$1,300,000Active\\\\\n\\\\\nBrought to you by The Boulos Company]"
        "(https://mainelistings.com/listings/759917817)"
    )
    cards = parse_active_cards(text)
    assert len(cards) == 1
    assert cards[0]["address"] is None and cards[0]["city"] is None
    found = _discover(
        ["old orchard beach"],
        lambda town, page: SummaryPage(town, page, 1, 1, cards),
        90,
    )
    result = next(iter(found.values()))
    assert result["discovery_town"] == "old orchard beach"
    assert result["city"] is None


def sparse(key):
    return dict(card(key), address=None, city=None)


def test_sparse_new_listing_is_audited_not_published_and_retry_is_bounded(setup):
    calls = []

    def detail(url):
        calls.append(url)
        return dict(mls_number="new", property_type="Land", list_date="2026-09-01")

    for _ in range(3):
        result = execute(
            setup, [card("old"), card("gone"), sparse("new")], fetch_detail=detail
        )
        assert result["active_mls_ids"] == ["gone", "old"]
        assert result["complete"] is True
        assert result["unresolved"][0]["detail_url"] == card("new")["detail_url"]
        assert result["unresolved"][0]["discovery_town"] == "york"
    assert len(calls) == 2
    assert "new" not in rows(setup)
    with sqlite3.connect(setup["db_path"]) as conn:
        assert (
            conn.execute("SELECT COUNT(*) FROM active_refresh_unresolved").fetchone()[0]
            == 3
        )


def test_known_sparse_listing_preserves_facts_and_never_checks_absence(setup):
    original = rows(setup)["old"]

    def unexpected(url):
        pytest.fail("Known, present URL must not incur another detail/status request")

    result = execute(
        setup,
        [sparse("old"), card("gone")],
        fetch_detail=unexpected,
        fetch_status=unexpected,
    )
    assert result["active_mls_ids"] == ["gone", "old"]
    for field in ("address", "city", "photo_url", "listing_agent_email"):
        assert rows(setup)["old"][field] == original[field]


def test_actual_detail_facts_can_fill_absent_summary(setup):
    result = execute(
        setup,
        [card("old"), card("gone"), sparse("new")],
        fetch_detail=lambda _: dict(
            mls_number="new",
            city="York",
            address="12 Real Road",
            list_date="2026-09-01",
        ),
    )
    assert "new" in result["active_mls_ids"]
    assert rows(setup)["new"]["address"] == "12 Real Road"
    assert rows(setup)["new"]["city"] == "York"


def test_sparse_card_does_not_weaken_detail_town_identity(setup):
    with pytest.raises(RefreshIncomplete, match="town"):
        execute(
            setup,
            [sparse("new")],
            fetch_detail=lambda _: dict(
                mls_number="new",
                city="Wells",
                address="12 Real Road",
                list_date="2026-09-01",
            ),
        )


def test_sparse_duplicate_still_fails_complete_coverage():
    with pytest.raises(RefreshIncomplete, match="Duplicate"):
        _discover(
            ["york"],
            lambda town, page: SummaryPage(town, page, 1, 2, [sparse("new")] * 2),
            90,
        )


def test_unresolved_can_resolve_next_run_without_losing_old_audit(setup):
    cards = [card("old"), card("gone"), sparse("new")]
    execute(setup, cards, fetch_detail=lambda _: None)
    result = execute(
        setup,
        cards,
        fetch_detail=lambda _: dict(
            mls_number="new",
            city="York",
            address="12 Real Road",
            list_date="2026-09-01",
        ),
    )
    assert result["unresolved"] == []
    assert "new" in result["active_mls_ids"]
    with sqlite3.connect(setup["db_path"]) as conn:
        assert (
            conn.execute("SELECT COUNT(*) FROM active_refresh_unresolved").fetchone()[0]
            == 1
        )


def test_missing_city_does_not_hide_known_out_of_town_identity(setup):
    with pytest.raises(RefreshIncomplete, match="town"):
        execute(dict(setup, towns=["Wells"]), [sparse("old")])
