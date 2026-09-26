"""Incremental-only supplement for facts visibly displayed on the detail page.

The legacy extractor is embedded unchanged. Address-hidden listings stay hidden;
never recover an address from private application state or similar-listing cards.
Verified public markup: one h1 with the address, immediately followed by the
displayed town/state/ZIP. Unknown structures remain unresolved, not guessed.
"""

from .maine_parser import DETAIL_EXTRACT_JS

INCREMENTAL_DETAIL_JS = (
    "(function(){ const result = JSON.parse(" + DETAIL_EXTRACT_JS + r""");
    const headings = document.querySelectorAll('h1');
    if (headings.length === 1) {
        const heading = headings[0];
        const address = heading.textContent.trim();
        const location = heading.nextElementSibling;
        const match = location && /^(.+),\s*([A-Z]{2})\s+(\d{5})$/.exec(location.textContent.trim());
        if (address && match) {
            result.address = address;
            result.city = match[1].trim();
            result.state = match[2];
            result.zip = match[3];
        }
    }
    return JSON.stringify(result);
})()"""
)
