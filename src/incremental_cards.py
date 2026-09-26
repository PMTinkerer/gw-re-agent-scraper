"""Complete Active-card discovery without inventing omitted property facts.

The legacy parser requires beds, baths and sqft. Public MLS result pages also
contain land/multifamily cards that omit some or all of these fields. They must
still count toward complete coverage; property eligibility is decided downstream.
This parser is confined to the incremental lane, leaving the weekly lane intact.
"""

import re

from .maine_parser import _parse_city_state_zip

_BREAK = r"\\\\\s*\\\\\s*"
_CARD = re.compile(
    r"\$\s*([\d,]+)\s*(Active|New Listing|Pending)"
    + _BREAK
    + r"\*\*([^*]+)\*\*\s+\*\*([^*]+)\*\*"
    + _BREAK
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


def parse_active_cards(markdown):
    cards = []
    for match in _CARD.finditer(markdown):
        city, state, zip_code = _parse_city_state_zip(match[4].strip())
        cards.append(
            {
                "status": "Active" if match[2] == "New Listing" else match[2],
                "sale_price": None,
                "list_price": int(match[1].replace(",", "")),
                "address": match[3].strip(),
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
