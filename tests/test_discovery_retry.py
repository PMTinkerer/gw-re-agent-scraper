"""A town may restart once, but incomplete attempts never contribute coverage."""

import pytest

from src.active_refresh import RefreshIncomplete, SummaryPage, _discover


def card(key, town="York", **changes):
    return dict(
        dict(
            detail_url=f"https://mainelistings.com/listings/{key}",
            city=town,
            status="Active",
        ),
        **changes,
    )


def scripted(pages):
    calls = []
    responses = iter(pages)

    def fetch(town, page):
        calls.append((town, page))
        result = next(responses)
        if isinstance(result, Exception):
            raise result
        return result

    return fetch, calls


@pytest.mark.parametrize("failure", ["duplicate", "total", "pages", "count"])
def test_inconsistent_town_restarts_once_with_fresh_accumulator(failure):
    bad = SummaryPage("york", 2, 2, 2, [card("stale")])
    if failure == "total":
        bad = SummaryPage("york", 2, 2, 3, [card("other")])
    elif failure == "pages":
        bad = SummaryPage("york", 2, 3, 2, [card("other")])
    elif failure == "count":
        bad = SummaryPage("york", 2, 2, 2, [card("other"), card("extra")])
    fetch, calls = scripted(
        [
            SummaryPage("york", 1, 2, 2, [card("stale")]),
            bad,
            SummaryPage("york", 1, 2, 2, [card("fresh")]),
            SummaryPage("york", 2, 2, 2, [card("last")]),
        ]
    )
    assert set(_discover(["york"], fetch, 90)) == {
        card("fresh")["detail_url"],
        card("last")["detail_url"],
    }
    assert calls == [("york", 1), ("york", 2)] * 2


def test_completed_towns_are_retained_without_refetching():
    fetch, calls = scripted(
        [
            SummaryPage("wells", 1, 1, 1, [card("known", "Wells")]),
            SummaryPage("york", 1, 1, 2, [card("stale"), card("stale")]),
            SummaryPage("york", 1, 1, 1, [card("fresh")]),
        ]
    )
    assert set(_discover(["wells", "york"], fetch, 90)) == {
        card("known")["detail_url"],
        card("fresh")["detail_url"],
    }
    assert calls == [("wells", 1), ("york", 1), ("york", 1)]


@pytest.mark.parametrize("failure", ["duplicate", "total", "pages", "count"])
def test_second_inconsistent_attempt_is_terminal(failure):
    first = SummaryPage("york", 1, 2, 2, [card("a")])
    second = SummaryPage("york", 2, 2, 2, [card("a")])
    if failure == "total":
        second = SummaryPage("york", 2, 2, 3, [card("b")])
    elif failure == "pages":
        second = SummaryPage("york", 2, 3, 2, [card("b")])
    elif failure == "count":
        second = SummaryPage("york", 2, 2, 2, [card("b"), card("c")])
    fetch, calls = scripted([first, second, first, second])
    with pytest.raises(RefreshIncomplete):
        _discover(["york"], fetch, 90)
    assert calls == [("york", 1), ("york", 2)] * 2


@pytest.mark.parametrize(
    "changes",
    [
        {"detail_url": "https://example.test/listings/a"},
        {"city": "Wells"},
        {"city": ""},
        {"status": "Closed"},
    ],
)
@pytest.mark.parametrize("inconsistency", ["duplicate", "total", "pages"])
def test_inconsistency_does_not_hide_invalid_card_later_on_page(changes, inconsistency):
    first = SummaryPage("york", 1, 2, 2, [card("a")])
    second = SummaryPage(
        "york",
        2,
        3 if inconsistency == "pages" else 2,
        3 if inconsistency == "total" else 2,
        [card("a"), card("bad", **changes)],
    )
    fetch, calls = scripted([first, second])
    with pytest.raises(RefreshIncomplete):
        _discover(["york"], fetch, 90)
    assert calls == [("york", 1), ("york", 2)]


@pytest.mark.parametrize(
    "failure",
    [
        None,
        SummaryPage("wells", 1, 1, 0, []),
        SummaryPage("york", 2, 1, 0, []),
        SummaryPage("york", 1, True, 0, []),
        SummaryPage("york", 1, 91, 0, []),
        SummaryPage("york", 1, 1, -1, []),
        SummaryPage("york", 1, 1, True, []),
        SummaryPage("york", 1, 1, 1, ()),
        SummaryPage("york", 1, 1, 1, []),
        SummaryPage("york", 1, 1, 1, [None]),
        TimeoutError("provider unavailable"),
        RefreshIncomplete("blocked"),
        RuntimeError("budget exhausted"),
    ],
)
@pytest.mark.parametrize("during_retry", [False, True])
def test_fatal_failure_never_retries(failure, during_retry):
    pages = (
        [SummaryPage("york", 1, 1, 2, [card("a"), card("a")])] if during_retry else []
    )
    fetch, calls = scripted([*pages, failure])
    with pytest.raises((RefreshIncomplete, TimeoutError, RuntimeError)):
        _discover(["york"], fetch, 90)
    assert calls == [("york", 1)] * (2 if during_retry else 1)


@pytest.mark.parametrize("during_retry", [False, True])
def test_cross_town_duplicate_is_fatal_even_after_within_town_duplicate(during_retry):
    pages = [SummaryPage("wells", 1, 1, 1, [card("shared", "Wells")])]
    if during_retry:
        pages.append(SummaryPage("york", 1, 1, 2, [card("a"), card("a")]))
    pages.append(SummaryPage("york", 1, 1, 3, [card("a"), card("a"), card("shared")]))
    fetch, calls = scripted(pages)
    with pytest.raises(RefreshIncomplete):
        _discover(["wells", "york"], fetch, 90)
    assert calls == [("wells", 1)] + [("york", 1)] * (2 if during_retry else 1)


def test_missing_page_on_changed_totals_is_fatal():
    fetch, calls = scripted(
        [
            SummaryPage("york", 1, 2, 2, [card("a")]),
            SummaryPage("york", 2, 3, 3, []),
        ]
    )
    with pytest.raises(RefreshIncomplete):
        _discover(["york"], fetch, 90)
    assert calls == [("york", 1), ("york", 2)]
