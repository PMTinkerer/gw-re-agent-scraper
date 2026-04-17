"""Parsing helpers for Maine Listings (MREIS MLS) data.

Extracts listing data from search result pages (markdown) and
detail pages (rawHtml via executeJavascript).
"""
from __future__ import annotations

import re

# === Search Results Parsing ===

_CARD_RE = re.compile(
    r'\$\s*([\d,]+)\s*Closed\\\\\s*\\\\\s*'
    r'\*\*([^*]+)\*\*\s+\*\*([^*]+)\*\*\\\\\s*\\\\\s*'
    r'(\d+)\s+Beds?\\\\\s*\\\\\s*'
    r'(\d+)\s+Baths?\\\\\s*\\\\\s*'
    r'([\d,]+)\s+sqft\\\\\s*\\\\\s*'
    r'Brought to you by\s+([^\]]+?)\]'
    r'\((https://mainelistings\.com/listings/[^)]+)\)',
    re.DOTALL,
)

_PAGINATION_RE = re.compile(r'(\d+)\s+of\s+(\d+)')
_TOTAL_RESULTS_RE = re.compile(r'([\d,]+)\s+Results')


def parse_search_cards(markdown: str) -> list[dict]:
    """Parse listing cards from search results markdown."""
    cards = []
    for m in _CARD_RE.finditer(markdown):
        price_str = m.group(1).replace(',', '')
        address = m.group(2).strip()
        city_state_zip = m.group(3).strip()

        city, state, zip_code = _parse_city_state_zip(city_state_zip)

        cards.append({
            'sale_price': int(price_str) if price_str else None,
            'address': address,
            'city': city,
            'state': state,
            'zip': zip_code,
            'beds': int(m.group(4)),
            'baths': int(m.group(5)),
            'sqft': int(m.group(6).replace(',', '')),
            'listing_office': m.group(7).strip(),
            'detail_url': m.group(8).strip(),
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

    for (var i = 0; i < scripts.length; i++) {
        var txt = scripts[i].textContent;
        if (txt.indexOf('buyer_agent_full_name') < 0) continue;

        // Buyer agent
        var ba = /buyer_agent_full_name:"([^"]*)"/.exec(txt);
        var baId = /buyer_agent_mls_id:"([^"]*)"/.exec(txt);
        var baEmail = /buyer_agent_email:"([^"]*)"/.exec(txt);
        result.buyer_agent = ba ? ba[1] : null;
        result.buyer_agent_id = baId ? baId[1] : null;
        result.buyer_agent_email = baEmail ? baEmail[1] : null;

        // Buyer office
        var bo = /buyer_office_name:"([^"]*)"/.exec(txt);
        result.buyer_office = bo ? bo[1] : null;

        // Listing agent — find the one with a real email (not minified 'a')
        var laMatches = txt.match(/list_agent_full_name:"([^"]*)"/g) || [];
        for (var j = 0; j < laMatches.length; j++) {
            var name = /"([^"]*)"/.exec(laMatches[j]);
            if (name && name[1]) {
                result.listing_agent = name[1];
                break;
            }
        }
        var laId = txt.match(/list_agent_mls_id:"([^"]*)"/g) || [];
        for (var k = 0; k < laId.length; k++) {
            var id = /"([^"]*)"/.exec(laId[k]);
            if (id && id[1]) { result.listing_agent_id = id[1]; break; }
        }
        var laEmail = txt.match(/list_agent_email:"([^"]*)"/g) || [];
        for (var m = 0; m < laEmail.length; m++) {
            var em = /"([^"]*)"/.exec(laEmail[m]);
            if (em && em[1]) { result.listing_agent_email = em[1]; break; }
        }

        // Listing office
        var loMatches = txt.match(/list_office_name:"([^"]*)"/g) || [];
        for (var n = 0; n < loMatches.length; n++) {
            var oname = /"([^"]*)"/.exec(loMatches[n]);
            if (oname && oname[1]) { result.listing_office = oname[1]; break; }
        }

        // Transaction details
        var cp = /close_price:"([^"]*)"/.exec(txt);
        var lp = /list_price:"([^"]*)"/.exec(txt);
        var cd = /close_date:"([^"]*)"/.exec(txt);
        var li = /listing_id:"([^"]*)"/.exec(txt);
        var dom = /days_on_market:(\\d+)/.exec(txt);
        var pst = /property_sub_type:"([^"]*)"/.exec(txt);

        result.sale_price = cp ? parseInt(cp[1]) : null;
        result.list_price = lp ? parseInt(lp[1]) : null;
        result.close_date = cd ? cd[1].split('T')[0] : null;
        result.mls_number = li ? li[1] : null;
        result.days_on_market = dom ? parseInt(dom[1]) : null;
        result.property_type = pst ? pst[1] : null;

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
