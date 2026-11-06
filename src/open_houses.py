"""Upcoming open houses for higher-priced homes, for Lucas's own alert email.

The public search filter ``open_house_end_time_after=now`` lists every listing
with an open house that has not ended yet. One search per town (plus extra
pages only when the source says there are more) finds them, instead of
re-reading every listing page.

The exact shape of that results page has not been seen from this codebase yet
(the cloud cannot reach mainelistings.com). So the in-page script returns two
independent kinds of evidence and keeps neither guess on its own:

* each listing card's link and visible text (price, address, any "Open House"
  line the card shows), and
* any object in the page state whose keys mention open houses, together with
  the identifying fields of the listing that holds it.

Nothing here touches the accepted feed, the listings database or any outreach
gate. A failed town is reported as failed, never as "no open houses".
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

EASTERN = ZoneInfo("America/New_York")
SEARCH_URL = "https://mainelistings.com/listings"
DEFAULT_MIN_PRICE = 1_000_000
PAGE_SIZE = 24
MAX_PAGES_PER_TOWN = 4
OUTPUT_PATH = "data/open_houses.json"
SCHEMA_VERSION = 1

OPEN_HOUSE_JS = r"""(() => {
  const out = {results_text: null, pages_text: null, cards: [], records: [], fields: []};
  const body = document.body ? document.body.innerText : '';
  const count = body.match(/([\d,]+)\s+Results?\b/);
  out.results_text = count ? count[1] : null;
  const pages = body.match(/(?:Page\s+)?(\d+)\s+of\s+(\d+)/);
  out.pages_text = pages ? [pages[1], pages[2]] : null;
  const seen = {};
  for (const a of document.querySelectorAll('a[href*="/listings/ME/"]')) {
    const href = a.href.split('?')[0].split('#')[0];
    let node = a, text = '';
    for (let i = 0; i < 8 && node; i++, node = node.parentElement) {
      text = (node.innerText || '').trim();
      if (text.indexOf('$') >= 0) break;
    }
    if (text.length > 900) text = text.slice(0, 900);
    if (!(href in seen) || text.length > seen[href].length) seen[href] = text;
  }
  for (const href of Object.keys(seen)) out.cards.push({url: href, text: seen[href]});
  const OH = /open_?house/i;
  const LISTING = /^(id|listing_id|listing_key|list_price|unparsed_address|street_address|city|postal_code|mls_status|property_type|slug|url)$/;
  function prims(obj, filter) {
    const r = {};
    for (const k of Object.keys(obj)) {
      const v = obj[k];
      if (filter && !filter.test(k)) continue;
      if (v === null || ['string', 'number', 'boolean'].includes(typeof v)) {
        r[k] = typeof v === 'string' ? v.slice(0, 300) : v;
      }
    }
    return r;
  }
  let visited = 0;
  const stack = [];
  function walk(node, depth) {
    if (!node || typeof node !== 'object' || depth > 14 || ++visited > 300000) return;
    if (out.records.length >= 200) return;
    if (!Array.isArray(node)) {
      const keys = Object.keys(node);
      const ohKeys = keys.filter(k => OH.test(k));
      if (ohKeys.length) {
        let listing = null;
        for (let i = stack.length - 1; i >= 0; i--) {
          if ('list_price' in stack[i] || 'listing_id' in stack[i]) { listing = stack[i]; break; }
        }
        if (!listing && ('list_price' in node || 'listing_id' in node)) listing = node;
        const nested = [];
        for (const k of ohKeys) {
          const v = node[k];
          const items = Array.isArray(v) ? v : (v && typeof v === 'object' ? [v] : []);
          for (const item of items.slice(0, 10)) {
            if (item && typeof item === 'object') nested.push(prims(item));
          }
        }
        out.records.push({own: prims(node, OH), nested: nested,
                          listing: listing ? prims(listing, LISTING) : null});
      }
      stack.push(node);
      for (const k of keys) walk(node[k], depth + 1);
      stack.pop();
    } else {
      for (const item of node) walk(item, depth + 1);
    }
  }
  try { walk(window.__NUXT__, 0); } catch (e) { out.walk_error = String(e).slice(0, 200); }
  const texts = Array.from(document.scripts).map(s => s.textContent).join('\n');
  const re = /(open_?house[A-Za-z0-9_]*)"?\s*:\s*("[^"]{0,120}"|[-\d.]+|true|false|null)/gi;
  let m;
  while ((m = re.exec(texts)) !== null && out.fields.length < 60) out.fields.push([m[1], m[2]]);
  return JSON.stringify(out);
})()"""


class OpenHousePageError(ValueError):
    """The results page did not carry enough evidence to trust its listings."""


def build_search_url(town, page, min_price):
    query = {
        "city": town,
        "open_house_end_time_after": "now",
        "min_list_price": min_price,
        "sort_by": "list_price",
        "sort_order": "desc",
        "page": page,
    }
    return SEARCH_URL + "?" + urlencode(query)


def listing_town(url):
    match = re.search(r"/listings/ME/([^/]+)/", url or "")
    return match.group(1).replace("-", " ") if match else None


def listing_source_id(url):
    match = re.search(r"/(\d+)/?$", url or "")
    return match.group(1) if match else None


_PRICE = re.compile(r"\$\s*([0-9]{1,3}(?:,[0-9]{3})+|[0-9]{4,})")
_OPEN_LINE = re.compile(r"(?im)^[^\n]*open\s*house[^\n]*$")
_ADDRESS = re.compile(r"(?m)^\s*(\d[^\n$]{2,80}?)\s*$")


def _parse_card(card):
    text = card.get("text") or ""
    price = _PRICE.search(text)
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    office = re.search(r"Brought to you by\s+([^\n]+)", text)
    address = None
    for index, line in enumerate(lines):
        if re.search(r",\s*ME\s+\d{5}", line) and index:
            address = lines[index - 1]
            break
    town = (listing_town(card["url"]) or "").lower()
    for index, line in enumerate(lines):
        # Layout from 2026-11-06: one "street, City" line above the office.
        street, comma, city = line.rpartition(",")
        if (
            address is None
            and comma
            and street.strip()
            and town
            and city.strip().lower() == town
            and index + 1 < len(lines)
            and lines[index + 1].startswith("Brought to you by")
        ):
            address = street.strip()
    return {
        "detail_url": card["url"],
        "listing_id": listing_source_id(card["url"]),
        "list_price": int(price.group(1).replace(",", "")) if price else None,
        "address": address,
        "listing_office": office.group(1).strip()[:120] if office else None,
        "card_open_house_text": [
            m.group().strip()[:200] for m in _OPEN_LINE.finditer(text)
        ][:4],
    }


def _parse_time(value):
    """ISO-like source time to an aware ISO string; naive times are Eastern."""
    if not isinstance(value, str) or not re.match(r"\d{4}-\d{2}-\d{2}[T ]\d", value):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=EASTERN)
    return parsed.astimezone(timezone.utc).isoformat()


def _window(fields):
    start = end = None
    for key, value in fields.items():
        lowered = key.lower()
        if start is None and "start" in lowered:
            start = _parse_time(value)
        elif end is None and "end" in lowered:
            end = _parse_time(value)
    if start is None and end is None:
        return None
    kind = next(
        (v for k, v in fields.items() if "type" in k.lower() and isinstance(v, str)),
        None,
    )
    return {"start": start, "end": end, "type": kind}


def _windows_by_listing(records, cards):
    by_id = {}
    only = cards[0]["listing_id"] if len(cards) == 1 else None
    for record in records:
        listing = record.get("listing") or {}
        ids = {
            str(v)
            for k, v in listing.items()
            if k in ("id", "listing_id", "listing_key")
        }
        target = next((c["listing_id"] for c in cards if c["listing_id"] in ids), only)
        if target is None:
            continue
        for fields in [record.get("own") or {}, *(record.get("nested") or [])]:
            window = _window(fields)
            if window and window not in by_id.setdefault(target, []):
                by_id[target].append(window)
    return by_id


def parse_open_house_page(result):
    """Return (result_count, total_pages, listings) from the in-page script."""
    try:
        data = json.loads(result) if isinstance(result, str) else result
    except ValueError as exc:
        raise OpenHousePageError("Open house script returned invalid JSON") from exc
    if not isinstance(data, dict) or not isinstance(data.get("cards"), list):
        raise OpenHousePageError("Open house script returned no card list")
    count_text = data.get("results_text")
    if not isinstance(count_text, str) or not re.fullmatch(r"[\d,]+", count_text):
        raise OpenHousePageError("Missing result count on open house page")
    count = int(count_text.replace(",", ""))
    # Pages follow from the count; "N of M" text on a card (photo counters)
    # is kept only as diagnostic evidence, never trusted for paging.
    total_pages = max(1, (count + PAGE_SIZE - 1) // PAGE_SIZE)
    cards = [
        _parse_card(c)
        for c in data["cards"]
        if isinstance(c, dict) and listing_source_id(c.get("url"))
    ]
    if count and not cards:
        raise OpenHousePageError("Results reported but no listing cards found")
    windows = _windows_by_listing(data.get("records") or [], cards)
    for card in cards:
        card["windows"] = windows.get(card["listing_id"], [])
    return count, total_pages, cards


def scan_town(fetch_page, town, min_price):
    """Every page of one town's open house results. Raises on any doubt."""
    listings, page, total = {}, 1, 1
    while page <= total:
        count, total, cards = parse_open_house_page(
            fetch_page(build_search_url(town, page, min_price))
        )
        if total > MAX_PAGES_PER_TOWN:
            raise OpenHousePageError("More open house pages than the safety cap")
        for card in cards:
            listings.setdefault(card["detail_url"], card)
        page += 1
    if len(listings) < count:
        raise OpenHousePageError("Fewer open house listings than the source count")
    return list(listings.values())


def _future(window, now):
    end = window.get("end") or window.get("start")
    return end is None or datetime.fromisoformat(end) >= now


def run_open_house_pass(
    fetch_page,
    towns,
    *,
    run_id,
    now=None,
    min_price=DEFAULT_MIN_PRICE,
    output_path=OUTPUT_PATH,
    stop_errors=(),
):
    """Scan each town and write the open house file. Never raises for a town.

    ``stop_errors`` (such as an exhausted budget) end the pass early; the towns
    not reached are recorded as not scanned.
    """
    now = now or datetime.now(timezone.utc)
    town_status, found, seen = {}, [], set()
    stopped = None
    for town in towns:
        if stopped:
            town_status[town] = {"status": "not_scanned", "error": stopped}
            continue
        try:
            listings = scan_town(fetch_page, town, min_price)
        except stop_errors as exc:
            stopped = type(exc).__name__
            town_status[town] = {"status": "not_scanned", "error": stopped}
            continue
        except Exception as exc:  # a broken town must not hide the others
            town_status[town] = {"status": "failed", "error": type(exc).__name__}
            continue
        kept = []
        for listing in listings:
            if listing["list_price"] is not None and listing["list_price"] < min_price:
                continue
            listing["windows"] = [w for w in listing["windows"] if _future(w, now)]
            kept.append(listing)
            # The link names the home's own town; one home is listed once even
            # if the source returns it for more than one town search.
            listing["city"] = listing_town(listing["detail_url"]) or town
            if listing["detail_url"] not in seen:
                seen.add(listing["detail_url"])
                found.append(listing)
        town_status[town] = {"status": "complete", "count": len(kept)}
    found.sort(key=lambda r: (-(r["list_price"] or 0), r["detail_url"]))
    document = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": now.isoformat(),
        "run_id": run_id,
        "min_list_price": min_price,
        "source": "mainelistings.com open_house_end_time_after=now",
        "towns": town_status,
        "open_houses": found,
    }
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)
    return document
