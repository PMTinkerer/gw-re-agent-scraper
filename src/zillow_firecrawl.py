"""Firecrawl-based Zillow directory discovery.

Replaces Playwright-based directory scraping with Firecrawl API calls.
Firecrawl bypasses Zillow's PerimeterX anti-bot protection by handling
proxy/IP rotation at the infrastructure level.
"""
from __future__ import annotations

import logging
import os
import re
import time

from .database import record_zillow_directory_profile
from .state import TOWNS
from .zillow import (
    _PAGE_INFO_RE,
    _extract_profile_card_candidates,
    _parse_int,
    _town_directory_url,
    ZillowAccessError,
)
from .zillow_state import mark_complete, mark_failed, mark_started, save_state

logger = logging.getLogger(__name__)

_MIN_DELAY_SECONDS = 6.0
_SCRAPE_WAIT_MS = 5000

_ZILLOW_BLOCK_STRINGS = [
    'captcha', 'px-captcha', 'perimeterx', 'verify you are human',
    'press and hold', 'access denied', '403 forbidden',
]

_PROFILE_LINK_RE = re.compile(
    r'\[([\s\S]*?)\]\((https://www\.zillow\.com/profile/[^)]+)\)',
)
_IMAGE_MD_RE = re.compile(r'!\[[^\]]*\]\([^)]*\)')
_BOLD_RE = re.compile(r'\*\*(.+?)\*\*')
_DIGIT_WORD_RE = re.compile(r'(\d)((?:team\s+)?sales?\b)')


def require_firecrawl_key() -> str:
    """Return FIRECRAWL_API_KEY from environment, raising if missing."""
    key = os.environ.get('FIRECRAWL_API_KEY', '').strip()
    if not key:
        raise RuntimeError(
            'FIRECRAWL_API_KEY environment variable is not set.\n\n'
            'Setup:\n'
            '1. Sign up at https://www.firecrawl.dev\n'
            '2. Copy your API key from the dashboard\n'
            '3. Add to .env: FIRECRAWL_API_KEY=fc-YOUR-KEY\n'
        )
    return key


def _get_firecrawl_client():
    """Lazy-import and return a Firecrawl client instance."""
    from firecrawl import Firecrawl  # noqa: WPS433
    return Firecrawl(api_key=require_firecrawl_key())


def _classify_markdown_response(markdown: str) -> str:
    """Return 'ok' or 'blocked' based on markdown content."""
    lower = markdown.lower()
    for indicator in _ZILLOW_BLOCK_STRINGS:
        if indicator in lower:
            return 'blocked'
    if '/profile/' not in markdown:
        return 'blocked'
    return 'ok'


_RATING_RE = re.compile(r'\d\.\d\(\d+\)')
_CARD_START_RE = re.compile(r'(?:TEAM\s+)?\d\.\d\(\d+\)')
_STAT_BOUNDARY_RE = re.compile(
    r'\$[\dKMB,.]|(?:\d[\d,]*\s*(?:team\s+)?(?:sales?|sale)\b)|No (?:recent|sales)',
    re.IGNORECASE,
)


def _strip_card_noise(raw: str) -> str:
    """Strip image markdown, backslash escapes, and leading nav noise."""
    text = _IMAGE_MD_RE.sub('', raw)
    text = text.replace('\\', ' ')
    bold_match = _BOLD_RE.search(text)
    card_start = _CARD_START_RE.search(text)
    if card_start:
        trim_to = card_start.start()
        if bold_match and bold_match.start() < trim_to:
            trim_to = bold_match.start()
        text = text[trim_to:]
    return text


def _clean_card_text(raw: str) -> str:
    """Clean a raw markdown card into a space-joined text for stat extraction."""
    text = _strip_card_noise(raw)
    text = _BOLD_RE.sub(r'\1', text)
    text = ' '.join(text.split())
    text = _DIGIT_WORD_RE.sub(r'\1 \2', text)
    return text.strip()


def _extract_name_office_and_type(raw: str) -> tuple[str | None, str | None, str]:
    """Extract name, office, and entity type from raw card text.

    Uses **bold** markers as the authoritative name delimiter.
    Text between the bold name and the first stat boundary is the office.
    No office text → brokerage. TEAM prefix → team. Otherwise → individual.
    """
    text = _strip_card_noise(raw)
    is_team = bool(re.search(r'\bTEAM\b', text[:50] if len(text) > 50 else text, re.IGNORECASE))

    bold_match = _BOLD_RE.search(text)
    if not bold_match:
        return None, None, 'individual'

    name = bold_match.group(1).strip()
    after_bold = ' '.join(text[bold_match.end():].split())
    after_bold = re.sub(r'^\d\.\d\(\d+\)\s*', '', after_bold)

    stat_hit = _STAT_BOUNDARY_RE.search(after_bold)
    office = after_bold[:stat_hit.start()].strip() if stat_hit else after_bold.strip()

    if not office:
        if is_team:
            return name, None, 'team'
        return name, None, 'brokerage'
    if is_team:
        return name, office, 'team'
    return name, office, 'individual'


def parse_agent_cards_from_markdown(
    markdown: str, town: str,
) -> list[dict]:
    """Parse agent directory cards from Firecrawl markdown output.

    Returns a list of candidate dicts compatible with the existing
    database.record_zillow_directory_profile() interface, enriched
    with profile_name, office_name, and corrected profile_type.
    """
    raw_links = []
    enrichment: dict[str, tuple] = {}
    for match in _PROFILE_LINK_RE.finditer(markdown):
        raw_text = match.group(1)
        href = match.group(2)
        cleaned = _clean_card_text(raw_text)
        if cleaned:
            raw_links.append({'href': href, 'text': cleaned})
            name, office, etype = _extract_name_office_and_type(raw_text)
            enrichment[href] = (name, office, etype)

    candidates = _extract_profile_card_candidates(raw_links, town)

    for candidate in candidates:
        url = candidate['profile_url']
        if url in enrichment:
            name, office, etype = enrichment[url]
            candidate['profile_name'] = name
            candidate['office_name'] = office
            candidate['profile_type'] = etype

    return candidates


def parse_page_info_from_markdown(markdown: str) -> tuple[int, int] | None:
    """Extract 'Page X of Y' pagination from markdown."""
    match = _PAGE_INFO_RE.search(markdown)
    if match:
        return int(match.group(1)), int(match.group(2))
    return None


def _scrape_directory_page(client, url: str, last_call: float) -> tuple[str, float]:
    """Scrape a single directory page via Firecrawl with rate limiting.

    Returns (markdown_content, updated_last_call_timestamp).
    """
    elapsed = time.monotonic() - last_call
    if elapsed < _MIN_DELAY_SECONDS:
        sleep_time = _MIN_DELAY_SECONDS - elapsed
        logger.debug('Rate limiting: sleeping %.1fs', sleep_time)
        time.sleep(sleep_time)

    now = time.monotonic()
    result = client.scrape(
        url,
        formats=['markdown'],
        wait_for=_SCRAPE_WAIT_MS,
        location={'country': 'US', 'languages': ['en']},
    )
    markdown = getattr(result, 'markdown', '') or ''
    if not markdown:
        raise ZillowAccessError('empty', 'Empty response from Firecrawl')

    status = _classify_markdown_response(markdown)
    if status == 'blocked':
        raise ZillowAccessError('blocked', f'Zillow blocked Firecrawl request to {url}')

    return markdown, now


def discover_zillow_profiles_firecrawl(
    conn,
    state: dict,
    *,
    towns: list[str] | None = None,
    max_pages: int = 5,
    state_path: str | None = None,
) -> dict:
    """Discover Zillow agent profiles via Firecrawl directory scraping.

    Iterates through directory pages for each town, extracts agent
    cards, and stores them in the database.

    Returns dict with 'towns_processed' and 'profiles_found' keys.
    """
    client = _get_firecrawl_client()
    towns_to_process = towns or list(TOWNS)
    total_profiles = 0
    towns_processed = 0
    last_call = 0.0

    for town in towns_to_process:
        mark_started(state, town)
        save_state(state, state_path)
        town_profiles = 0

        try:
            town_profiles, last_call = _discover_town(
                client, conn, town, max_pages, last_call,
            )
            mark_complete(state, town, profiles_found=town_profiles)
            towns_processed += 1
            total_profiles += town_profiles
            logger.info(
                'Town %s complete: %d profiles discovered', town, town_profiles,
            )
        except ZillowAccessError as exc:
            mark_failed(state, town, str(exc))
            logger.error('Town %s failed: %s', town, exc)

        save_state(state, state_path)

    return {'towns_processed': towns_processed, 'profiles_found': total_profiles}


def _discover_town(
    client,
    conn,
    town: str,
    max_pages: int,
    last_call: float,
) -> tuple[int, float]:
    """Scrape directory pages for a single town. Returns (profiles_found, last_call)."""
    base_url = _town_directory_url(town)
    town_profiles = 0

    for page_num in range(1, max_pages + 1):
        url = base_url if page_num == 1 else f'{base_url}?page={page_num}'
        logger.info('Scraping %s page %d...', town, page_num)

        markdown, last_call = _scrape_directory_page(client, url, last_call)
        candidates = parse_agent_cards_from_markdown(markdown, town)

        for card in candidates:
            record_zillow_directory_profile(
                conn,
                town=town,
                profile_url=card['profile_url'],
                profile_type=card['profile_type'],
                local_sales_count=card['local_sales_count'],
                raw_card_text=card.get('raw_card_text'),
                profile_name=card.get('profile_name'),
                office_name=card.get('office_name'),
                sales_last_12_months=card.get('sales_last_12_months'),
            )
            town_profiles += 1

        page_info = parse_page_info_from_markdown(markdown)
        if page_info and page_num >= page_info[1]:
            logger.info('Reached last page (%d of %d) for %s', page_num, page_info[1], town)
            break
        if not candidates and page_num > 1:
            logger.info('No candidates on page %d for %s, stopping', page_num, town)
            break

    return town_profiles, last_call
