"""Firecrawl-based Maine Listings (MREIS MLS) transaction scraper.

Two-phase approach:
1. Discover: scrape search result pages to find closed listing URLs
2. Enrich: scrape detail pages to extract agent data from NUXT blob
"""
from __future__ import annotations

import json
import logging
import time

from .maine_database import (
    enrich_listing,
    get_unenriched,
    mark_enrichment_failed,
    upsert_listing,
    url_exists,
)
from .maine_parser import (
    DETAIL_EXTRACT_JS,
    parse_detail_response,
    parse_pagination,
    parse_search_cards,
)
from .maine_state import mark_complete, mark_failed, mark_started, save_state
from .state import TOWNS
from .zillow_firecrawl import require_firecrawl_key

logger = logging.getLogger(__name__)

_MIN_DELAY = 6.0
_SEARCH_URL = 'https://mainelistings.com/listings'
_BLOCK_STRINGS = ['unexpected occurred', 'access denied', 'captcha']


def _get_client():
    from firecrawl import Firecrawl
    return Firecrawl(api_key=require_firecrawl_key())


def _scrape_page(client, url: str, last_call: float, fmt: str = 'markdown'):
    """Scrape a page with rate limiting. Returns (content, last_call)."""
    elapsed = time.monotonic() - last_call
    if elapsed < _MIN_DELAY:
        time.sleep(_MIN_DELAY - elapsed)

    now = time.monotonic()
    kwargs = {'formats': [fmt], 'wait_for': 8000}
    if fmt == 'rawHtml':
        kwargs['actions'] = [
            {'type': 'wait', 'milliseconds': 5000},
            {'type': 'executeJavascript', 'script': DETAIL_EXTRACT_JS},
        ]

    result = client.scrape(url, **kwargs)
    content = getattr(result, fmt, '') or getattr(result, 'markdown', '') or ''

    if any(s in content.lower() for s in _BLOCK_STRINGS):
        raise RuntimeError(f'Blocked or error page at {url}')

    return result, now


def discover_listings(
    conn,
    state: dict,
    *,
    towns: list[str] | None = None,
    max_pages: int = 90,
    recent_only: bool = False,
    state_path: str | None = None,
) -> dict:
    """Phase 1: Discover closed listings from search result pages."""
    client = _get_client()
    towns_to_process = towns or list(TOWNS)
    total_listings = 0
    towns_done = 0
    last_call = 0.0

    for town in towns_to_process:
        mark_started(state, town)
        save_state(state, state_path)
        town_count = 0

        try:
            town_count, last_call = _discover_town(
                client, conn, town, max_pages, recent_only, last_call,
            )
            mark_complete(state, town, listings_found=town_count)
            towns_done += 1
            total_listings += town_count
            logger.info('Town %s: %d listings discovered', town, town_count)
        except Exception as exc:
            mark_failed(state, town, str(exc))
            logger.error('Town %s failed: %s', town, exc)

        save_state(state, state_path)

    return {'towns': towns_done, 'listings': total_listings}


def _discover_town(
    client, conn, town: str, max_pages: int, recent_only: bool,
    last_call: float,
) -> tuple[int, float]:
    """Scrape search pages for one town."""
    town_count = 0

    for page_num in range(1, max_pages + 1):
        url = f'{_SEARCH_URL}?city={town}&mls_status=Closed'
        if page_num > 1:
            url += f'&page={page_num}'

        logger.info('Discovering %s page %d...', town, page_num)
        result, last_call = _scrape_page(client, url, last_call, 'markdown')
        markdown = getattr(result, 'markdown', '') or ''

        cards = parse_search_cards(markdown)
        if not cards and page_num > 1:
            logger.info('No cards on page %d for %s, stopping', page_num, town)
            break

        new_count = 0
        for card in cards:
            if not url_exists(conn, card['detail_url']):
                new_count += 1
            upsert_listing(conn, card)

        town_count += len(cards)

        if recent_only and new_count == 0 and page_num > 1:
            logger.info('All %d cards on page %d already known, stopping', len(cards), page_num)
            break

        page_info = parse_pagination(markdown)
        if page_info and page_num >= page_info[1]:
            logger.info('Reached last page (%d of %d)', page_num, page_info[1])
            break

    return town_count, last_call


def enrich_listings(
    conn,
    *,
    batch_size: int = 50,
    max_attempts: int = 2,
) -> dict:
    """Phase 2: Enrich listings with agent data from detail pages."""
    client = _get_client()
    pending = get_unenriched(conn, batch_size=batch_size, max_attempts=max_attempts)
    logger.info('Enrichment batch: %d listings', len(pending))

    enriched = 0
    failed = 0
    last_call = 0.0

    for i, row in enumerate(pending):
        url = row['detail_url']
        logger.info('[%d/%d] Enriching %s...', i + 1, len(pending), url)

        try:
            result, last_call = _scrape_page(client, url, last_call, 'rawHtml')

            acts = getattr(result, 'actions', None)
            if not acts or 'javascriptReturns' not in acts:
                mark_enrichment_failed(conn, url, 'no JS returns')
                failed += 1
                continue

            ret = acts['javascriptReturns'][0]
            data = parse_detail_response(ret)
            if not data:
                mark_enrichment_failed(conn, url, 'parse failed')
                failed += 1
                continue

            enrich_listing(conn, url, data)
            enriched += 1
            logger.info(
                '  %s listed by %s, sold by %s ($%s, %s)',
                row['city'],
                data.get('listing_agent', '?'),
                data.get('buyer_agent', '?'),
                f"{data.get('sale_price', 0):,}" if data.get('sale_price') else '?',
                data.get('close_date', '?'),
            )

        except Exception as exc:
            mark_enrichment_failed(conn, url, str(exc)[:200])
            failed += 1
            logger.error('  Error: %s', exc)

    return {'enriched': enriched, 'failed': failed, 'total': len(pending)}
