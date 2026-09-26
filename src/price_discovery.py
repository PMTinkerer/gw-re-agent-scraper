"""Pure, bounded discovery through disjoint first-page price ranges.

Callbacks own transport and persistent budget accounting. This module never
retries, paginates, or publishes partial results. Hints contain only boundaries;
every observation and the final unbounded count are fetched anew.
"""

from dataclasses import dataclass

from .active_refresh import RefreshIncomplete, _town, _valid_url


@dataclass(frozen=True)
class RangePage:
    town: str
    minimum: int | None
    maximum: int | None
    page: int
    total_pages: int
    total_results: int
    listings: list[dict]


@dataclass(frozen=True)
class DiscoveryResult:
    listings: dict[str, dict]
    hints: dict
    requests: dict[str, int]


def _bound(value):
    return value is None or (type(value) is int and value >= 0)


def _hints(value):
    """A malformed cache is disposable; never accept partial cached domains."""
    if (
        not isinstance(value, dict)
        or type(value.get("schema_version")) is not int
        or value["schema_version"] != 1
        or not isinstance(value.get("towns"), dict)
    ):
        return {}
    output = {}
    try:
        for town, bands in value["towns"].items():
            if (
                _town(town) != town
                or not isinstance(bands, list)
                or not 1 <= len(bands) <= 90
            ):
                return {}
            previous = None
            checked = []
            for index, band in enumerate(bands):
                if not isinstance(band, dict) or set(band) != {"minimum", "maximum"}:
                    return {}
                low, high = band["minimum"], band["maximum"]
                if not _bound(low) or not _bound(high):
                    return {}
                if index == 0:
                    if low is not None:
                        return {}
                elif low is None or previous is None or low != previous + 1:
                    return {}
                if (high is None) != (index == len(bands) - 1):
                    return {}
                if low is not None and high is not None and low > high:
                    return {}
                checked.append((low, high))
                previous = high
            output[town] = checked
    except RefreshIncomplete:
        return {}
    return output


def _read(fetch, town, low, high, requests, cap):
    if requests[town] >= cap:
        raise RefreshIncomplete(f"Price discovery request cap reached for {town}")
    requests[town] += 1
    page = fetch(town, low, high)
    if (
        not isinstance(page, RangePage)
        or _town(page.town) != town
        or not _bound(page.minimum)
        or not _bound(page.maximum)
        or (page.minimum, page.maximum) != (low, high)
        or type(page.page) is not int
        or page.page != 1
        or type(page.total_results) is not int
        or page.total_results < 0
        or type(page.total_pages) is not int
        or page.total_pages != max(1, (page.total_results + 23) // 24)
        or not isinstance(page.listings, list)
    ):
        raise RefreshIncomplete("Invalid price-range page metadata")
    seen = set()
    duplicate = False
    for row in page.listings:
        url = row.get("detail_url") if isinstance(row, dict) else None
        price = row.get("list_price") if isinstance(row, dict) else None
        try:
            valid = (
                _valid_url(url)
                and type(price) is int
                and price > 0
                and (low is None or price >= low)
                and (high is None or price <= high)
                and isinstance(row.get("status"), str)
                and row["status"] in {"Active", "Pending"}
                and (row.get("city") is None or _town(row["city"]) == town)
            )
        except (ValueError, RefreshIncomplete):
            valid = False
        if not valid:
            raise RefreshIncomplete("Invalid price-range discovery card")
        duplicate = duplicate or url in seen
        seen.add(url)
    # Malformed later cards must not be hidden behind count/duplicate drift.
    if duplicate:
        raise RefreshIncomplete("Duplicate price-range discovery card")
    if len(page.listings) != min(24, page.total_results):
        raise RefreshIncomplete("Incomplete price-range card count")
    return page


def _split(page):
    low, high = page.minimum, page.maximum
    floor = 0 if low is None else low
    if high is not None and floor == high:
        raise RefreshIncomplete("Exact-price bucket overflow")
    prices = sorted({row["list_price"] for row in page.listings})
    interior = [price for price in prices if high is None or price < high]
    # With all observed cards at the upper edge, isolate that exact price.
    pivot = interior[len(interior) // 2] if interior else prices[0] - 1
    if pivot < floor or (high is not None and pivot >= high):
        raise RefreshIncomplete("Exact-price bucket overflow")
    return (low, pivot), (pivot + 1, high)


def discover_price_ranges(towns, fetch_range, *, hints=None, max_requests=90):
    """Return only reconciled listings and fresh complete-domain leaf hints."""
    if type(max_requests) is not int or not 1 <= max_requests <= 90:
        raise ValueError("max_requests must be an integer from 1 to 90")
    canonical = [_town(town) for town in towns]
    if not canonical or len(set(canonical)) != len(canonical):
        raise RefreshIncomplete("Missing or duplicate town coverage")
    cached = _hints(hints)
    found, fresh, requests = {}, {}, dict.fromkeys(canonical, 0)
    for town in canonical:
        probes, leaves, town_found = [], [], {}

        def read(low, high):
            page = _read(fetch_range, town, low, high, requests, max_requests)
            probes.extend(
                (row["detail_url"], row["list_price"], row["status"])
                for row in page.listings
            )
            return page

        def collect(page):
            if page.total_results > 24:
                left, right = _split(page)
                left_count = collect(read(*left))
                right_count = collect(read(*right))
                if left_count + right_count != page.total_results:
                    raise RefreshIncomplete("Child price-range count changed")
            else:
                for row in page.listings:
                    url = row["detail_url"]
                    if url in town_found or url in found:
                        raise RefreshIncomplete(
                            "Duplicate listing across leaves or towns"
                        )
                    town_found[url] = dict(row, discovery_town=town)
                leaves.append({"minimum": page.minimum, "maximum": page.maximum})
            return page.total_results

        root = read(None, None)
        if town in cached:
            count = sum(collect(read(*band)) for band in cached[town])
            if count != root.total_results:
                raise RefreshIncomplete("Hint price-range count changed")
        else:
            collect(root)
        final = read(None, None)
        if (
            final.total_results != root.total_results
            or len(town_found) != root.total_results
        ):
            raise RefreshIncomplete("Final price-range count changed")
        for url, price, status in probes:
            row = town_found.get(url)
            if row is None or (row["list_price"], row["status"]) != (price, status):
                raise RefreshIncomplete("Price-range probe lost or changed")
        found.update(town_found)
        fresh[town] = sorted(
            leaves, key=lambda band: -1 if band["minimum"] is None else band["minimum"]
        )
    return DiscoveryResult(found, {"schema_version": 1, "towns": fresh}, requests)
