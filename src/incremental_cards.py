"""Complete Active-card discovery without inventing omitted property facts.

The legacy parser requires beds, baths and sqft. Public MLS result pages also
contain land/multifamily cards that omit some or all of these fields. They must
still count toward complete coverage; property eligibility is decided downstream.
This parser is confined to the incremental lane, leaving the weekly lane intact.
"""

import re

from .maine_parser import _parse_city_state_zip

_BREAK = r"\\\\\s*\\\\\s*"
# Detailed cards have a price/status line followed by the source's double break.
# Compact map cards and repeated photo links do not have this structure.
_CARD_START = re.compile(r"\$[^\n\\]*" + _BREAK)
_CARD = re.compile(
    r"\$\s*((?:[0-9]{1,3}(?:,[0-9]{3})+|[0-9]+))\s*"
    r"(Active Under Contract|Active|New Listing|Pending)"
    + _BREAK
    + r"(?:\*\*([^*]+)\*\*\s+\*\*([^*]+)\*\*"
    + _BREAK
    + r")?"
    + r"(?:(\d+)\s+[Bb]eds?"
    + _BREAK
    + r")?"
    + r"(?:(\d+)\s+Baths?"
    + _BREAK
    + r")?"
    + r"(?:([\d,]+)\s+sqft"
    + _BREAK
    + r")?"
    + r"Brought to you by\s+([^\]]+?)\]"
    + r"\((https://mainelistings\.com/listings/[^)]+)\)",
    re.DOTALL,
)


# Layout seen from 2026-11-06 (run 42276461389): photo, status, an optional
# "Open House: ..." line, then price. Facts share one line joined by " · " and
# are left out entirely when the source has none; the bold line is
# "street, City" with the ZIP only in the listing link.
_NEW_START = re.compile(r"\[!\[[^\]]*\]\([^)\s]*\)" + _BREAK)
_NEW_CARD = re.compile(
    r"\[!\[[^\]]*\]\([^)\s]*\)"
    + _BREAK
    + r"(Active Under Contract|Active|New Listing|Pending)"
    + _BREAK
    + r"(?:Open House:[^\\\n]*"
    + _BREAK
    + r")?"
    + r"\$\s*((?:[0-9]{1,3}(?:,[0-9]{3})+|[0-9]+))"
    + r"(?:"
    + _BREAK
    + r"([^\\\n*]+?)[ \t]*(?=\\))?"
    + r"(?:(?:"
    + _BREAK
    + r"|[ \t]+)\*\*([^*]+)\*\*)?"
    + r"\s*"
    + _BREAK
    + r"Brought to you by\s+([^\]]+?)\]"
    + r"\((https://mainelistings\.com/listings/[^)]+)\)",
    re.DOTALL,
)
_FACT = re.compile(
    r"(\d+) [Bb]eds?|(\d+) [Bb]aths?|((?:[0-9]{1,3}(?:,[0-9]{3})+|[0-9]+)) sqft"
)
_LINK_ZIP = re.compile(r"^https://mainelistings\.com/listings/(ME)/[^/]+/(\d{5})/")


# Same normalization as detail/status evidence in active_refresh: a home under
# contract is Pending, never available for outreach.
_STATUS = {"New Listing": "Active", "Active Under Contract": "Pending"}


class ActiveCardParseError(ValueError):
    """A source card was present but could not be parsed completely."""


def _facts(text):
    """Beds, baths, sqft from "3 Beds · 5 Baths · 3,490 sqft"; strict."""
    found = [None, None, None]
    if text is None:
        return found
    for part in text.split(" \u00b7 "):
        fact = _FACT.fullmatch(part.strip())
        if fact is None:
            raise ActiveCardParseError("Unparsed Active result card facts")
        slot = next(i for i in range(3) if fact[i + 1] is not None)
        if found[slot] is not None:
            raise ActiveCardParseError("Repeated Active result card fact")
        found[slot] = int(fact[slot + 1].replace(",", ""))
    return found


def _new_card(match):
    beds, baths, sqft = _facts(match[3])
    address = city = state = zip_code = None
    if match[4] is not None:
        street, comma, town = match[4].strip().rpartition(",")
        if not comma or not street.strip() or not town.strip():
            raise ActiveCardParseError("Unparsed Active result card address")
        address, city = street.strip(), town.strip()
        link = _LINK_ZIP.match(match[6].strip())
        state, zip_code = (link[1], link[2]) if link else (None, None)
    return {
        "status": _STATUS.get(match[1], match[1]),
        "sale_price": None,
        "list_price": int(match[2].replace(",", "")),
        "address": address,
        "city": city,
        "state": state,
        "zip": zip_code,
        "beds": beds,
        "baths": baths,
        "sqft": sqft,
        "listing_office": match[5].strip(),
        "detail_url": match[6].strip(),
    }


def _check_footers(markdown, matches):
    # An invalid/missing price must not hide a card that still has its footer.
    previous_end = 0
    for match in matches:
        if "Brought to you by" in markdown[previous_end : match.start()]:
            raise ActiveCardParseError("Unparsed Active result card footer")
        previous_end = match.end()
    if "Brought to you by" in markdown[previous_end:]:
        raise ActiveCardParseError("Unparsed Active result card footer")


def _parse_new_layout(markdown, starts):
    matches = []
    for index, start in enumerate(starts):
        end = starts[index + 1].start() if index + 1 < len(starts) else len(markdown)
        match = _NEW_CARD.match(markdown, start.start(), end)
        if match is None:
            raise ActiveCardParseError("Malformed or unparsed Active result card")
        matches.append(match)
    _check_footers(markdown, matches)
    return [_new_card(match) for match in matches]


def parse_active_cards(markdown):
    new_starts = list(_NEW_START.finditer(markdown))
    if new_starts:
        return _parse_new_layout(markdown, new_starts)
    starts = list(_CARD_START.finditer(markdown))
    matches = []
    for index, start in enumerate(starts):
        end = starts[index + 1].start() if index + 1 < len(starts) else len(markdown)
        match = _CARD.match(markdown, start.start(), end)
        if match is None:
            raise ActiveCardParseError("Malformed or unparsed Active result card")
        matches.append(match)
    _check_footers(markdown, matches)
    cards = []
    for match in matches:
        city, state, zip_code = (
            _parse_city_state_zip(match[4].strip())
            if match[4] is not None
            else (None, None, None)
        )
        cards.append(
            {
                "status": _STATUS.get(match[2], match[2]),
                "sale_price": None,
                "list_price": int(match[1].replace(",", "")),
                "address": match[3].strip() if match[3] is not None else None,
                "city": city,
                "state": state,
                "zip": zip_code,
                "beds": int(match[5]) if match[5] is not None else None,
                "baths": int(match[6]) if match[6] is not None else None,
                "sqft": (
                    int(match[7].replace(",", "")) if match[7] is not None else None
                ),
                "listing_office": match[8].strip(),
                "detail_url": match[9].strip(),
            }
        )
    return cards
