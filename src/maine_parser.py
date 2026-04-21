"""Parsing helpers for Maine Listings (MREIS MLS) data.

Extracts listing data from search result pages (markdown) and
detail pages (rawHtml via executeJavascript).
"""
from __future__ import annotations

import re

# === Search Results Parsing ===

def _make_card_re(status_pattern: str) -> re.Pattern:
    """Build a regex matching search-result cards for the given status.

    status_pattern is a regex fragment (e.g. 'Closed' or 'Active|New Listing').
    The resulting regex captures nine groups:
        1=price  2=status  3=address  4=city+state+zip
        5=beds   6=baths   7=sqft     8=listing_office  9=detail_url
    """
    return re.compile(
        r'\$\s*([\d,]+)\s*(' + status_pattern + r')\\\\\s*\\\\\s*'
        r'\*\*([^*]+)\*\*\s+\*\*([^*]+)\*\*\\\\\s*\\\\\s*'
        r'(\d+)\s+Beds?\\\\\s*\\\\\s*'
        r'(\d+)\s+Baths?\\\\\s*\\\\\s*'
        r'([\d,]+)\s+sqft\\\\\s*\\\\\s*'
        r'Brought to you by\s+([^\]]+?)\]'
        r'\((https://mainelistings\.com/listings/[^)]+)\)',
        re.DOTALL,
    )


_CLOSED_CARD_RE = _make_card_re(r'Closed')
# "Active" and "New Listing" are both active-state badges. "Pending" can
# show up on active-search pages when a listing goes under contract.
_ACTIVE_CARD_RE = _make_card_re(r'Active|New Listing|Pending')

_PAGINATION_RE = re.compile(r'(\d+)\s+of\s+(\d+)')
_TOTAL_RESULTS_RE = re.compile(r'([\d,]+)\s+Results')


def parse_search_cards(markdown: str, status: str = 'Closed') -> list[dict]:
    """Parse listing cards from search results markdown.

    Args:
        markdown: markdown response from a mainelistings.com search page.
        status: 'Closed' parses sold cards (price → sale_price).
                'Active' parses live cards (price → list_price).
                Cards badged 'Pending' get status='Pending'.
    """
    card_re = _CLOSED_CARD_RE if status == 'Closed' else _ACTIVE_CARD_RE

    cards: list[dict] = []
    for m in card_re.finditer(markdown):
        price_str = m.group(1).replace(',', '')
        price = int(price_str) if price_str else None
        badge = m.group(2).strip()
        address = m.group(3).strip()
        city_state_zip = m.group(4).strip()
        city, state, zip_code = _parse_city_state_zip(city_state_zip)

        cards.append({
            'status': 'Active' if badge == 'New Listing' else badge,
            'sale_price': price if status == 'Closed' else None,
            'list_price': price if status != 'Closed' else None,
            'address': address,
            'city': city,
            'state': state,
            'zip': zip_code,
            'beds': int(m.group(5)),
            'baths': int(m.group(6)),
            'sqft': int(m.group(7).replace(',', '')),
            'listing_office': m.group(8).strip(),
            'detail_url': m.group(9).strip(),
        })
    return cards


def _parse_city_state_zip(text: str) -> tuple[str, str, str]:
    """Parse 'Kittery, ME 03904' into (city, state, zip)."""
    match = re.match(r'(.+?),\s*([A-Z]{2})\s+(\d{5})', text)
    if match:
        return match.group(1).strip(), match.group(2), match.group(3)
    return text, 'ME', ''


def parse_pagination(markdown: str) -> tuple[int, int] | None:
    """Extract current page and total pages from '1 of 90'."""
    match = _PAGINATION_RE.search(markdown)
    if match:
        return int(match.group(1)), int(match.group(2))
    return None


def parse_total_results(markdown: str) -> int | None:
    """Extract total result count from 'N,NNN Results'."""
    match = _TOTAL_RESULTS_RE.search(markdown)
    if match:
        return int(match.group(1).replace(',', ''))
    return None


# === Detail Page Extraction ===

# JavaScript to extract agent/listing data from the NUXT blob
# in mainelistings.com detail pages. The page has TWO list_agent
# objects — the first is co_list_agent (usually null/minified 'a'),
# the second has actual data. We find the one with a quoted email.
DETAIL_EXTRACT_JS = '''(function(){
    var scripts = document.querySelectorAll('script');
    var result = {error: null};

    // Photo URL lives in og:image meta, not in the NUXT blob
    var og = document.querySelector('meta[property="og:image"]');
    result.photo_url = og ? og.getAttribute('content') : null;

    // Pick the first match whose quoted value is non-empty — the NUXT blob
    // double-declares several fields (the first is a minified 'a' placeholder).
    function pickQuoted(txt, field) {
        var re = new RegExp(field + ':"([^"]*)"', 'g');
        var m;
        while ((m = re.exec(txt)) !== null) {
            if (m[1]) return m[1];
        }
        return null;
    }
    // Same idea for numeric fields — NUXT uses both bare (e.g. `year_built:2026`)
    // and quoted (e.g. `lot_size_square_feet:"94525.2"`) forms, depending on field.
    // The minified 'a' placeholders don't satisfy the digit pattern so they're skipped.
    function pickNumeric(txt, field) {
        var re = new RegExp(field + ':"?(-?\\\\d+(?:\\\\.\\\\d+)?)"?', 'g');
        var m;
        while ((m = re.exec(txt)) !== null) {
            var v = parseFloat(m[1]);
            if (!isNaN(v)) return v;
        }
        return null;
    }

    for (var i = 0; i < scripts.length; i++) {
        var txt = scripts[i].textContent;
        if (txt.indexOf('buyer_agent_full_name') < 0) continue;

        // Agents + offices
        result.listing_agent       = pickQuoted(txt, 'list_agent_full_name');
        result.listing_agent_id    = pickQuoted(txt, 'list_agent_mls_id');
        result.listing_agent_email = pickQuoted(txt, 'list_agent_email');
        result.listing_office      = pickQuoted(txt, 'list_office_name');
        result.buyer_agent         = pickQuoted(txt, 'buyer_agent_full_name');
        result.buyer_agent_id      = pickQuoted(txt, 'buyer_agent_mls_id');
        result.buyer_agent_email   = pickQuoted(txt, 'buyer_agent_email');
        result.buyer_office        = pickQuoted(txt, 'buyer_office_name');

        // Transaction details
        var cp = pickQuoted(txt, 'close_price');
        var lp = pickQuoted(txt, 'list_price');
        var cd = pickQuoted(txt, 'close_date');
        var ld = pickQuoted(txt, 'listing_contract_date');
        result.sale_price  = cp ? parseInt(cp) : null;
        result.list_price  = lp ? parseInt(lp) : null;
        result.close_date  = cd ? cd.split('T')[0] : null;
        result.list_date   = ld ? ld.split('T')[0] : null;
        result.mls_number  = pickQuoted(txt, 'listing_id');
        result.property_type = pickQuoted(txt, 'property_sub_type');
        result.status      = pickQuoted(txt, 'mls_status');

        var dom = pickNumeric(txt, 'days_on_market');
        result.days_on_market = dom !== null ? Math.round(dom) : null;

        // Property attributes (for active-listings downstream tools)
        var yb = pickNumeric(txt, 'year_built');
        var lsqft = pickNumeric(txt, 'lot_size_square_feet');
        result.year_built = yb !== null ? Math.round(yb) : null;
        result.lot_sqft   = lsqft !== null ? Math.round(lsqft) : null;

        var pr = /public_remarks:"((?:[^"\\\\]|\\\\.)*)"/.exec(txt);
        result.description = pr ? pr[1] : null;

        break;
    }

    if (!result.listing_agent && !result.buyer_agent) {
        result.error = 'no agent data found in NUXT blob';
    }

    return JSON.stringify(result);
})()'''


_UNICODE_ESCAPE_RE = re.compile(r'\\u([0-9a-fA-F]{4})')


def _decode_escapes(value):
    """Decode JSON-style \\uXXXX escapes left over from the NUXT blob regex.

    The detail-page NUXT data embeds strings with escape sequences like
    "\\u002F" (forward slash) because Vue double-encodes the payload. Our
    JS regex captures the raw bytes, so we have to decode here.
    """
    if not isinstance(value, str):
        return value
    return _UNICODE_ESCAPE_RE.sub(
        lambda m: chr(int(m.group(1), 16)),
        value,
    )


def parse_detail_response(js_return: dict | str) -> dict | None:
    """Parse the JS extraction result from a detail page."""
    import json
    val = js_return.get('value', js_return) if isinstance(js_return, dict) else js_return
    try:
        data = json.loads(val) if isinstance(val, str) else val
    except (json.JSONDecodeError, TypeError):
        return None

    if not isinstance(data, dict) or data.get('error'):
        return None

    return {k: _decode_escapes(v) for k, v in data.items()}
