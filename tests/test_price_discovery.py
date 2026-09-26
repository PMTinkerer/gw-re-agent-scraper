"""Offline, deterministic first-page discovery contract tests."""

import importlib.util
import json
import random
from dataclasses import replace
from pathlib import Path

import pytest

from src.active_refresh import RefreshIncomplete


def test_price_discovery_api_exists():
    assert importlib.util.find_spec("src.price_discovery") is not None


def card(identifier, price, town="Wells", **fields):
    return dict(
        detail_url=f"https://mainelistings.com/listings/{identifier}",
        list_price=price,
        city=town,
        status="Active",
        **fields,
    )


class Source:
    """Simulate range filtering of known cards, never a provider acceptance."""

    def __init__(self, rows, *, shuffle=False):
        self.rows = rows
        self.calls = []
        self.shuffle = shuffle

    def __call__(self, town, minimum, maximum):
        from src.price_discovery import RangePage

        self.calls.append((town, minimum, maximum))
        rows = [
            dict(row)
            for row in self.rows
            if (minimum is None or row["list_price"] >= minimum)
            and (maximum is None or row["list_price"] <= maximum)
        ]
        if self.shuffle:
            random.Random(len(self.calls)).shuffle(rows)
        rows.sort(key=lambda row: row["list_price"], reverse=True)
        return RangePage(
            town,
            minimum,
            maximum,
            1,
            max(1, (len(rows) + 23) // 24),
            len(rows),
            rows[:24],
        )


def discover(source, **kwargs):
    from src.price_discovery import discover_price_ranges

    return discover_price_ranges(["Wells"], source, **kwargs)


@pytest.mark.parametrize("count", [0, 1, 24, 25])
def test_complete_counts_and_final_fresh_probe(count):
    source = Source([card(i, 100 + i) for i in range(count)])
    result = discover(source)
    assert len(result.listings) == count
    assert result.requests == {"wells": len(source.calls)}
    assert source.calls[0] == source.calls[-1] == ("wells", None, None)
    assert len(source.calls) == (2 if count <= 24 else 4)
    assert all(row["discovery_town"] == "wells" for row in result.listings.values())


def test_sparse_cards_do_not_invent_city_or_address():
    row = card(1, 1)
    del row["city"]
    result = discover(Source([row]))
    saved = result.listings[row["detail_url"]]
    assert "city" not in saved and "address" not in saved


def test_captured_wells_prices_cold_and_warm_simulation():
    evidence = json.loads(
        (
            Path(__file__).parents[1]
            / "docs/evidence/wells-price-bands-2026-09-26.json"
        ).read_text()
    )
    rows = [
        card(identifier, price)
        for band in evidence["bands"]
        for identifier, price in band["rows"]
    ]
    cold_source = Source(rows, shuffle=True)
    cold = discover(cold_source)
    warm_source = Source(rows, shuffle=True)
    warm = discover(warm_source, hints=cold.hints)
    assert (
        set(cold.listings) == set(warm.listings) == {row["detail_url"] for row in rows}
    )
    assert len(cold.listings) == 200
    assert "https://mainelistings.com/listings/760702704" in cold.listings
    assert warm.requests["wells"] < cold.requests["wells"] <= 90
    assert cold.hints["schema_version"] == 1
    bands = cold.hints["towns"]["wells"]
    assert bands[0]["minimum"] is None and bands[-1]["maximum"] is None
    assert all(
        left["maximum"] + 1 == right["minimum"] for left, right in zip(bands, bands[1:])
    )


def test_shuffled_equal_price_group_isolated_without_pagination():
    rows = [card(i, 500) for i in range(24)] + [card(100, 1), card(101, 1000)]
    source = Source(rows, shuffle=True)
    assert len(discover(source).listings) == 26
    assert ("wells", 500, 500) in source.calls or ("wells", None, 500) in source.calls


@pytest.mark.parametrize(
    "field,value",
    [
        ("page", True),
        ("page", 2),
        ("total_pages", True),
        ("total_pages", 0),
        ("total_pages", 2),
        ("total_results", True),
        ("total_results", -1),
        ("total_results", "1"),
        ("listings", {}),
        ("town", "York"),
        ("minimum", True),
        ("minimum", 1),
        ("maximum", False),
        ("maximum", 100),
    ],
)
def test_invalid_page_contract(field, value):
    source = Source([card(1, 100)])
    with pytest.raises(RefreshIncomplete):
        discover(lambda *args: replace(source(*args), **{field: value}))


@pytest.mark.parametrize(
    "field,value",
    [
        ("list_price", True),
        ("list_price", 0),
        ("list_price", -1),
        ("list_price", 1.5),
        ("list_price", "100"),
        ("list_price", None),
        ("detail_url", "https://example.com/listings/1"),
        ("detail_url", None),
        ("city", "York"),
        ("city", "Atlantis"),
        ("city", False),
        ("status", "Closed"),
        ("status", None),
        ("status", []),
        ("status", {}),
    ],
)
def test_invalid_card_contract(field, value):
    source = Source([card(1, 100)])

    def fetch(*args):
        page = source(*args)
        page.listings[0][field] = value
        return page

    with pytest.raises(RefreshIncomplete):
        discover(fetch)


def test_duplicate_page_cards_rejected():
    with pytest.raises(RefreshIncomplete, match="Duplicate"):
        discover(Source([card(1, 100), card(1, 100)]))


def test_same_price_overflow_terminates_in_two_splits():
    source = Source([card(i, 100) for i in range(25)])
    with pytest.raises(RefreshIncomplete, match="price.*overflow"):
        discover(source)
    assert ("wells", 100, 100) in source.calls
    assert len(source.calls) <= 5


@pytest.mark.parametrize("cap", [1, 2, 3])
def test_per_town_request_cap_includes_final_probe(cap):
    source = Source([card(i, 100 + i) for i in range(25)])
    with pytest.raises(RefreshIncomplete, match="request cap"):
        discover(source, max_requests=cap)
    assert len(source.calls) == cap


def test_transport_exception_propagates_without_retry():
    calls = []
    error = RuntimeError("budget exhausted")

    def fetch(*args):
        calls.append(args)
        raise error

    with pytest.raises(RuntimeError) as caught:
        discover(fetch)
    assert caught.value is error and len(calls) == 1


def hint(bands):
    return {"schema_version": 1, "towns": {"wells": bands}}


@pytest.mark.parametrize(
    "bad",
    [
        None,
        {},
        [],
        {"schema_version": True, "towns": {}},
        {"schema_version": 2, "towns": {}},
        hint([]),
        hint([{"minimum": 0, "maximum": None}]),
        hint([{"minimum": None, "maximum": 100}]),
        hint([{"minimum": None, "maximum": True}, {"minimum": 2, "maximum": None}]),
        hint([{"minimum": None, "maximum": 100}, {"minimum": 100, "maximum": None}]),
        hint([{"minimum": None, "maximum": 100}, {"minimum": 102, "maximum": None}]),
        hint([{"minimum": None, "maximum": -1}, {"minimum": 0, "maximum": None}]),
        hint(
            [
                {"minimum": None, "maximum": 100},
                {"minimum": 101, "maximum": 50},
                {"minimum": 51, "maximum": None},
            ]
        ),
    ],
)
def test_corrupt_hints_discarded_for_cold_discovery(bad):
    rows = [card(i, i + 1) for i in range(30)]
    cold = Source(rows)
    discover(cold)
    source = Source(rows)
    result = discover(source, hints=bad)
    assert len(result.listings) == 30 and source.calls == cold.calls


def test_valid_hints_expand_oversized_ranges_and_keep_open_tails():
    hints = hint([{"minimum": None, "maximum": 100}, {"minimum": 101, "maximum": None}])
    source = Source([card(i, i + 1) for i in range(40)] + [card(100, 10**12)])
    result = discover(source, hints=hints)
    assert len(result.listings) == 41
    assert source.calls[1] == ("wells", None, 100)
    assert ("wells", 101, None) in source.calls
    assert len(result.hints["towns"]["wells"]) > 2


@pytest.mark.parametrize("cap", [True, 0, -1, 91, 1.5, None])
def test_invalid_request_cap_rejected_before_callback(cap):
    source = Source([])
    with pytest.raises((ValueError, RefreshIncomplete)):
        discover(source, max_requests=cap)
    assert source.calls == []


def test_final_count_drift_fails():
    source = Source([card(1, 100)])

    def fetch(*args):
        page = source(*args)
        return (
            replace(page, total_results=2, listings=page.listings + [card(2, 100)])
            if len(source.calls) == 2
            else page
        )

    with pytest.raises(RefreshIncomplete, match="count"):
        discover(fetch)


@pytest.mark.parametrize("change", ["lost", "price", "status"])
def test_parent_probe_must_survive_with_same_facts(change):
    source = Source([card(i, 100 + i) for i in range(25)])

    def fetch(town, minimum, maximum):
        page = source(town, minimum, maximum)
        if minimum is not None or maximum is not None:
            for row in page.listings:
                if row["detail_url"].endswith("/24"):
                    if change == "lost":
                        row["detail_url"] += "new"
                    else:
                        row[{"price": "list_price", "status": "status"}[change]] = (
                            125 if change == "price" else "Pending"
                        )
        return page

    with pytest.raises(RefreshIncomplete, match="probe"):
        discover(fetch)


def test_child_counts_must_reconcile_parent():
    source = Source([card(i, 100 + i) for i in range(25)])

    def fetch(town, minimum, maximum):
        page = source(town, minimum, maximum)
        if maximum is not None:
            return replace(
                page, total_results=page.total_results - 1, listings=page.listings[1:]
            )
        return page

    with pytest.raises(RefreshIncomplete, match="count"):
        discover(fetch)


def test_duplicate_across_towns_fails():
    from src.price_discovery import discover_price_ranges

    def fetch(town, minimum, maximum):
        return Source([card(1, 100, town)])(town, minimum, maximum)

    with pytest.raises(RefreshIncomplete, match="Duplicate"):
        discover_price_ranges(["Wells", "York"], fetch)


def test_all_cards_validated_before_duplicate_or_count_drift():
    source = Source([card(1, 100), card(1, 100), card(3, 100)])

    def fetch(*args):
        page = source(*args)
        page.listings[-1]["status"] = "Closed"
        return page

    with pytest.raises(RefreshIncomplete, match="Invalid.*card"):
        discover(fetch)


def test_duplicate_across_disjoint_leaves_is_rejected():
    rows = [card(i, i + 1) for i in range(25)]
    rows[-1]["detail_url"] = rows[0]["detail_url"]
    with pytest.raises(RefreshIncomplete, match="Duplicate"):
        discover(Source(rows))


@pytest.mark.parametrize("change", ["lost", "price", "status"])
def test_final_probe_is_independently_reconciled(change):
    source = Source([card(1, 100)])

    def fetch(*args):
        page = source(*args)
        if len(source.calls) == 2:
            page.listings[0][
                {"lost": "detail_url", "price": "list_price", "status": "status"}[
                    change
                ]
            ] = {
                "lost": "https://mainelistings.com/listings/new",
                "price": 101,
                "status": "Pending",
            }[
                change
            ]
        return page

    with pytest.raises(RefreshIncomplete, match="probe"):
        discover(fetch)


def test_corruption_in_unqueried_town_discards_entire_hint():
    cached = hint(
        [{"minimum": None, "maximum": 100}, {"minimum": 101, "maximum": None}]
    )
    cached["towns"]["york"] = [{"minimum": None, "maximum": 100}]
    source = Source([card(1, 10)])
    result = discover(source, hints=cached)
    assert result.requests == {"wells": 2}


def test_hint_length_limited_to_ninety():
    cached = hint(
        [{"minimum": None, "maximum": 0}]
        + [{"minimum": i, "maximum": i} for i in range(1, 90)]
        + [{"minimum": 90, "maximum": None}]
    )
    source = Source([])
    assert discover(source, hints=cached).requests == {"wells": 2}


def test_valid_ninety_leaf_hint_cannot_exceed_request_cap():
    cached = hint(
        [{"minimum": None, "maximum": 0}]
        + [{"minimum": i, "maximum": i} for i in range(1, 89)]
        + [{"minimum": 89, "maximum": None}]
    )
    source = Source([])
    with pytest.raises(RefreshIncomplete, match="request cap"):
        discover(source, hints=cached)
    assert len(source.calls) == 90


def test_small_towns_have_separate_request_budgets():
    from src.price_discovery import discover_price_ranges

    source = Source([])
    result = discover_price_ranges(["Wells", "York"], source, max_requests=2)
    assert result.requests == {"wells": 2, "york": 2}


@pytest.mark.parametrize("towns", [[], ["Wells", "wells"], ["Atlantis"], [False]])
def test_invalid_requested_towns_rejected_before_transport(towns):
    from src.price_discovery import discover_price_ranges

    source = Source([])
    with pytest.raises(RefreshIncomplete):
        discover_price_ranges(towns, source)
    assert not source.calls


def test_non_card_and_out_of_range_card_rejected():
    source = Source([card(i, i + 1) for i in range(25)])

    def fetch(*args):
        page = source(*args)
        if page.maximum is not None:
            page.listings[0]["list_price"] = page.maximum + 1
        return page

    with pytest.raises(RefreshIncomplete, match="Invalid.*card"):
        discover(fetch)
    with pytest.raises(RefreshIncomplete, match="Invalid.*card"):
        discover(lambda *args: replace(source(*args), listings=[None]))
