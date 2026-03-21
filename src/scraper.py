"""Data collection scrapers for Redfin and Realtor.com.

Redfin: CSV download endpoint (primary source)
Realtor.com: RapidAPI wrapper (secondary source)
"""
from __future__ import annotations

import csv
import io
import json
import logging
import os
import random
import re
import time
from datetime import datetime, timedelta

import requests

from .database import upsert_transaction
from .state import (
    TOWNS, slug_to_town, track_rapidapi_call,
)

logger = logging.getLogger(__name__)

# --- Shared Utilities ---

# User agents copied from ~/competitor-scraper/utils/stealth.py
_USER_AGENTS = [
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
]

# City normalization — adapted from ~/competitor-scraper/utils/exporter.py
CITY_NORMALIZATION = {
    'nubble peninsula': 'York',
    'long sands beach': 'York',
    'short sands beach': 'York',
    'york beach': 'York',
    'york harbor': 'York',
    'cape neddick': 'York',
    'moody': 'Wells',
    'moody beach': 'Wells',
    'ocean park': 'Old Orchard Beach',
    'ocean park meadows': 'Old Orchard Beach',
    'oob': 'Old Orchard Beach',
    'goose rocks beach': 'Kennebunkport',
    'goose rocks': 'Kennebunkport',
    'cape porpoise': 'Kennebunkport',
    'kennebunk beach': 'Kennebunk',
    'kennebunk port': 'Kennebunkport',
    'biddeford pool': 'Biddeford',
}

# Set of valid target towns (lowercased for comparison)
_VALID_TOWNS = {t.lower() for t in TOWNS}


def random_delay(min_s: float, max_s: float) -> None:
    """Sleep for a random duration between min_s and max_s seconds."""
    duration = random.uniform(min_s, max_s)
    logger.debug('Waiting %.1fs', duration)
    time.sleep(duration)


def _normalize_city(city: str | None) -> str | None:
    """Normalize city name to a canonical target town, or return as-is."""
    if not city:
        return None
    lookup = city.strip().lower()
    normalized = CITY_NORMALIZATION.get(lookup, city.strip())
    return normalized


def _is_target_town(city: str | None) -> bool:
    """Check if a city is one of our 10 target towns."""
    if not city:
        return False
    normalized = _normalize_city(city)
    return normalized.lower() in _VALID_TOWNS if normalized else False


def _get_session() -> requests.Session:
    """Create a requests session with random user-agent."""
    session = requests.Session()
    session.headers.update({
        'User-Agent': random.choice(_USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
    })
    return session


def _request_with_retry(session: requests.Session, url: str,
                        max_retries: int = 3, method: str = 'GET',
                        **kwargs) -> requests.Response | None:
    """Make an HTTP request with exponential backoff on failure."""
    backoff = 30
    for attempt in range(max_retries + 1):
        try:
            logger.info('Request [%d/%d]: %s %s', attempt + 1, max_retries + 1, method, url)
            resp = session.request(method, url, timeout=60, **kwargs)
            logger.info('Response: %d (%d bytes)', resp.status_code, len(resp.content))

            if resp.status_code == 200:
                return resp
            elif resp.status_code in (429, 403):
                if attempt < max_retries:
                    logger.warning('Got %d, backing off %ds', resp.status_code, backoff)
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                else:
                    logger.error('Got %d after %d retries, giving up', resp.status_code, max_retries)
                    return None
            else:
                logger.error('Unexpected status %d', resp.status_code)
                return None
        except requests.RequestException as e:
            if attempt < max_retries:
                logger.warning('Request error: %s, retrying in %ds', e, backoff)
                time.sleep(backoff)
                backoff *= 2
            else:
                logger.error('Request failed after %d retries: %s', max_retries, e)
                return None
    return None


def _parse_redfin_date(date_str: str) -> str | None:
    """Parse Redfin date format ('June-30-2025') to ISO ('2025-06-30')."""
    if not date_str:
        return None
    try:
        dt = datetime.strptime(date_str, '%B-%d-%Y')
        return dt.strftime('%Y-%m-%d')
    except ValueError:
        # Try ISO format as fallback
        if len(date_str) == 10 and date_str[4] == '-':
            return date_str
        logger.debug('Could not parse date: %s', date_str)
        return None


# --- Redfin ---

# Hardcoded Redfin region data discovered from redfin.com URLs (March 2026)
# Some Maine towns are "city" (region_type=6), others are "minorcivildivision" (region_type=40)
_REDFIN_REGIONS: dict[str, dict] = {
    'kittery':           {'id': 23512, 'type': 6},    # /city/23512/ME/Kittery
    'york':              {'id': 956,   'type': 40},   # /minorcivildivision/956/ME/York
    'ogunquit':          {'id': 958,   'type': 40},   # /minorcivildivision/958/ME/Ogunquit
    'wells':             {'id': 947,   'type': 40},   # /minorcivildivision/947/ME/Wells
    'kennebunk':         {'id': 23470, 'type': 6},    # /city/23470/ME/Kennebunk
    'kennebunkport':     {'id': 23484, 'type': 6},    # /city/23484/ME/Kennebunkport
    'biddeford':         {'id': 1283,  'type': 6},    # /city/1283/ME/Biddeford
    'saco':              {'id': 16583, 'type': 6},    # /city/16583/ME/Saco
    'old_orchard_beach': {'id': 35638, 'type': 6},    # /city/35638/ME/Old-Orchard-Beach
    'scarborough':       {'id': 25626, 'type': 6},    # /city/25626/ME/Scarborough
}


def discover_redfin_region_id(town: str, state_dict: dict | None = None) -> int | None:
    """Look up Redfin region_id for a town from hardcoded data.

    Caches result in state_dict['region_ids'] if provided.
    """
    from .state import _town_slug
    slug = _town_slug(town)

    # Check cache first
    if state_dict:
        cached = state_dict.get('region_ids', {}).get(slug)
        if cached:
            return cached

    region = _REDFIN_REGIONS.get(slug)
    if region:
        region_id = region['id']
        logger.info('Found region_id for %s: %d (type=%d)', town, region_id, region['type'])
        if state_dict is not None:
            state_dict.setdefault('region_ids', {})[slug] = region_id
        return region_id

    logger.error('No hardcoded region_id for %s (slug=%s)', town, slug)
    return None


def scrape_redfin(town: str, region_id: int, conn, state_dict: dict | None = None) -> int:
    """Scrape Redfin CSV endpoint for sold properties in a town (last 3 years).

    NOTE: The Redfin CSV does NOT include agent columns for any MLS as of 2026.
    This collects property data (address, price, sold date, MLS#, etc.) which
    can later be enriched with agent data from Realtor.com.

    For minorcivildivision towns (York, Ogunquit, Wells), we query York County
    (region_type=5, region_id=1309) and filter by city name.

    Returns the number of rows inserted/updated.
    """
    from .state import _town_slug
    session = _get_session()
    session.headers['Referer'] = 'https://www.redfin.com/'

    total_rows = 0
    page = 0
    cutoff_date = (datetime.utcnow() - timedelta(days=1095)).strftime('%Y-%m-%d')

    # Look up region info — minorcivildivision towns use York County instead
    slug = _town_slug(town)
    region_info = _REDFIN_REGIONS.get(slug, {})
    if region_info.get('type') == 40:
        # Minorcivildivision — CSV API doesn't support this type
        # Use York County and filter by city name
        actual_region_id = 1309
        actual_region_type = 5
        logger.info('Using York County (1309/type=5) for %s (minorcivildivision)', town)
    else:
        actual_region_id = region_id
        actual_region_type = region_info.get('type', 6)

    while True:
        url = (
            f'https://www.redfin.com/stingray/api/gis-csv'
            f'?al=1&num_homes=350&ord=redfin-recommended-asc'
            f'&page_number={page}&region_id={actual_region_id}&region_type={actual_region_type}'
            f'&sold_within_days=1095&status=9'
            f'&uipt=1,2,3,4,5,6,7,8&v=8'
        )

        random_delay(5, 10)
        resp = _request_with_retry(session, url)

        if resp is None:
            raise RuntimeError(f'Redfin request failed for {town} (page {page})')

        text = resp.text
        if not text.strip():
            logger.warning('Empty CSV response for %s page %d', town, page)
            break

        reader = csv.DictReader(io.StringIO(text))
        page_rows = 0
        csv_row_count = 0

        for row in reader:
            csv_row_count += 1

            # Extract and normalize city
            city = _normalize_city(row.get('CITY'))

            # Filter: only our target towns (important for county-level queries)
            # For county queries, keep ALL target towns (not just the requested one)
            # since the data is already being downloaded anyway
            if not _is_target_town(city):
                continue

            # Parse and filter sold date (Redfin uses "June-30-2025" format)
            raw_date = row.get('SOLD DATE', '').strip()
            sale_date = _parse_redfin_date(raw_date)
            if sale_date and sale_date < cutoff_date:
                continue

            mls = row.get('MLS#', '').strip()
            if not mls:
                continue

            # Note: Redfin CSV no longer includes agent columns (as of 2026)
            # Agent data will be enriched from Realtor.com
            record = {
                'mls_number': mls,
                'address': row.get('ADDRESS', '').strip() or None,
                'city': city,
                'state': row.get('STATE OR PROVINCE', 'ME').strip(),
                'zip': row.get('ZIP OR POSTAL CODE', '').strip() or None,
                'sale_price': row.get('PRICE'),
                'list_price': None,
                'beds': row.get('BEDS'),
                'baths': row.get('BATHS'),
                'sqft': row.get('SQUARE FEET'),
                'year_built': row.get('YEAR BUILT'),
                'days_on_market': row.get('DAYS ON MARKET'),
                'sale_date': sale_date or None,
                'listing_agent': row.get('LISTING AGENT', '').strip() or None,
                'buyer_agent': row.get("BUYER'S AGENT", '').strip() or None,
                'listing_office': row.get('LISTING BROKER', '').strip() or None,
                'buyer_office': row.get("BUYER'S BROKER", '').strip() or None,
                'source_url': row.get('URL (SEE https://www.redfin.com/buy-a-home/comparative-market-analysis FOR INFO ON PRICING)', '').strip() or None,
                'data_source': 'redfin',
                'scraped_at': datetime.utcnow().isoformat(),
            }

            if upsert_transaction(conn, record):
                page_rows += 1

        conn.commit()
        total_rows += page_rows
        logger.info('Redfin %s page %d: %d rows inserted (%d CSV rows total)', town, page, page_rows, csv_row_count)

        # Paginate based on total CSV rows returned, not just inserted rows
        if csv_row_count < 350:
            break

        page += 1
        # Safety: don't paginate forever
        # County queries need more pages (York County has 10K+ transactions)
        max_pages = 50 if actual_region_type == 5 else 20
        if page > max_pages:
            logger.warning('Redfin pagination limit (%d) reached for %s', max_pages, town)
            break

    logger.info('Redfin %s total: %d rows', town, total_rows)
    return total_rows


# --- Realtor.com RapidAPI ---

_RAPIDAPI_HOST = 'realtor-data1.p.rapidapi.com'


def scrape_realtor(town: str, year: int, conn, state_dict: dict | None = None) -> int:
    """Scrape Realtor.com via RapidAPI for sold properties in a town for a specific year.

    Returns the number of rows inserted/updated.
    """
    api_key = os.environ.get('RAPIDAPI_KEY')
    if not api_key:
        logger.warning('RAPIDAPI_KEY not set — skipping Realtor.com for %s %d', town, year)
        return -1  # Signal that this was skipped, not failed

    # Budget check
    if state_dict and not track_rapidapi_call(state_dict):
        logger.warning('RapidAPI budget exhausted — skipping %s %d', town, year)
        return -1

    session = _get_session()
    session.headers.update({
        'X-RapidAPI-Key': api_key,
        'X-RapidAPI-Host': _RAPIDAPI_HOST,
    })

    # Date range for the target year
    date_start = f'{year}-01-01'
    date_end = f'{year}-12-31'

    total_rows = 0
    offset = 0
    limit = 200

    while True:
        url = f'https://{_RAPIDAPI_HOST}/property/sold'
        params = {
            'city': town,
            'state_code': 'ME',
            'offset': str(offset),
            'limit': str(limit),
            'sort': 'sold_date',
        }

        random_delay(10, 15)
        resp = _request_with_retry(session, url, params=params)

        if resp is None:
            raise RuntimeError(f'Realtor.com API failed for {town} {year} (offset {offset})')

        try:
            data = resp.json()
        except json.JSONDecodeError:
            logger.error('Invalid JSON from Realtor.com API for %s %d', town, year)
            raise RuntimeError(f'Realtor.com returned invalid JSON for {town} {year}')

        properties = data.get('data', data.get('results', []))
        if not properties:
            # Try alternate response structure
            if isinstance(data, list):
                properties = data
            else:
                logger.info('No more results for %s %d at offset %d', town, year, offset)
                break

        page_rows = 0

        for prop in properties:
            # Extract sale date and filter by year
            sale_date = (
                prop.get('last_sold_date')
                or prop.get('sold_date')
                or prop.get('list_date')
            )
            if sale_date:
                # Normalize date format
                sale_date = sale_date[:10]  # Take YYYY-MM-DD portion
                if not (date_start <= sale_date <= date_end):
                    continue

            # Extract MLS number
            mls = (
                prop.get('mls_id')
                or prop.get('mls', {}).get('id')
                if isinstance(prop.get('mls'), dict) else prop.get('mls')
            )
            if not mls:
                # Use property_id as fallback identifier
                mls = prop.get('property_id')
            if not mls:
                continue

            # Extract address
            location = prop.get('location', {})
            address_obj = location.get('address', {})
            if isinstance(address_obj, dict):
                address = address_obj.get('line', '')
                city = _normalize_city(address_obj.get('city', town))
                zip_code = address_obj.get('postal_code')
            else:
                address = prop.get('address', '')
                city = _normalize_city(prop.get('city', town))
                zip_code = prop.get('postal_code') or prop.get('zip')

            # Extract price
            price = (
                prop.get('last_sold_price')
                or prop.get('sold_price')
                or prop.get('list_price')
                or prop.get('price')
            )

            # Extract agents
            listing_agent = None
            buyer_agent = None
            listing_office = None

            # Agent data can be nested in various ways
            agents = prop.get('agents', [])
            if agents:
                for agent in agents:
                    role = agent.get('type', '').lower()
                    name = agent.get('name') or agent.get('full_name')
                    office = agent.get('office', {})
                    office_name = office.get('name') if isinstance(office, dict) else None
                    if 'list' in role or 'sell' in role:
                        listing_agent = name
                        listing_office = office_name
                    elif 'buy' in role:
                        buyer_agent = name

            # Fallback: check top-level agent fields
            if not listing_agent:
                listing_agent = prop.get('listing_agent') or prop.get('agent_name')
            if not listing_office:
                listing_office = prop.get('listing_office') or prop.get('office_name')

            record = {
                'mls_number': f'realtor_{mls}',  # Prefix to avoid collision with Redfin MLS#s
                'address': address or None,
                'city': city,
                'state': 'ME',
                'zip': zip_code,
                'sale_price': price,
                'list_price': prop.get('list_price'),
                'beds': prop.get('beds') or prop.get('description', {}).get('beds'),
                'baths': prop.get('baths') or prop.get('description', {}).get('baths'),
                'sqft': prop.get('sqft') or prop.get('description', {}).get('sqft'),
                'year_built': prop.get('year_built') or prop.get('description', {}).get('year_built'),
                'days_on_market': prop.get('days_on_market'),
                'sale_date': sale_date,
                'listing_agent': listing_agent,
                'buyer_agent': buyer_agent,
                'listing_office': listing_office,
                'buyer_office': None,
                'source_url': prop.get('permalink') or prop.get('url'),
                'data_source': 'realtor',
                'scraped_at': datetime.utcnow().isoformat(),
            }

            if upsert_transaction(conn, record):
                page_rows += 1

        conn.commit()
        total_rows += page_rows
        logger.info('Realtor.com %s %d offset %d: %d rows', town, year, offset, page_rows)

        # If we got fewer than limit, no more pages
        if len(properties) < limit:
            break

        offset += limit

        # Track additional API calls for pagination
        if state_dict and not track_rapidapi_call(state_dict):
            logger.warning('RapidAPI budget exhausted mid-pagination for %s %d', town, year)
            break

    logger.info('Realtor.com %s %d total: %d rows', town, year, total_rows)
    return total_rows


# ---------------------------------------------------------------------------
# Phase 2: Playwright Agent Enrichment
# ---------------------------------------------------------------------------

# Desktop viewports for stealth randomization (from competitor-scraper)
_VIEWPORTS = [
    {'width': 1920, 'height': 1080},
    {'width': 1536, 'height': 864},
    {'width': 1440, 'height': 900},
    {'width': 1366, 'height': 768},
    {'width': 1280, 'height': 720},
]

# Regex patterns for text-based agent extraction
_LISTED_BY_RE = re.compile(
    r'Listed\s+by\s+(.+?)(?:\s*[•·]\s*(.+?))?(?:\n|$)',
    re.IGNORECASE,
)
_BOUGHT_WITH_RE = re.compile(
    r'Bought\s+with\s+(.+?)(?:\s*[•·]\s*(.+?))?(?:\n|$)',
    re.IGNORECASE,
)
_COURTESY_RE = re.compile(
    r'Listing\s+(?:provided|courtesy)\s+(?:by|of)\s+(.+?)(?:\n|$)',
    re.IGNORECASE,
)


def _launch_stealth_browser(playwright, headless: bool = True):
    """Launch a Chromium browser with stealth configuration.

    Returns (browser, context, page) — caller must close browser when done.
    """
    launch_args = {
        'headless': headless,
        'args': [
            '--disable-blink-features=AutomationControlled',
            '--no-first-run',
            '--no-default-browser-check',
        ],
    }
    if headless:
        launch_args['args'].append('--disable-gpu')

    browser = playwright.chromium.launch(**launch_args)

    viewport = random.choice(_VIEWPORTS)
    user_agent = random.choice(_USER_AGENTS)

    context = browser.new_context(
        user_agent=user_agent,
        viewport=viewport,
        locale='en-US',
        timezone_id='America/New_York',
    )

    page = context.new_page()

    # Hide navigator.webdriver property
    page.add_init_script(
        'Object.defineProperty(navigator, "webdriver", {get: () => undefined});'
    )

    # Apply playwright-stealth patches if available
    try:
        from playwright_stealth import Stealth
        stealth = Stealth()
        stealth.apply_stealth_sync(page)
        logger.debug('playwright-stealth patches applied')
    except ImportError:
        logger.warning('playwright-stealth not installed — basic stealth only')

    return browser, context, page


def _check_page_status(page) -> str:
    """Check page status: 'ok', 'captcha' (stop batch), or 'error' (skip URL).

    Returns:
        'ok': Normal page, proceed with extraction.
        'captcha': Captcha/rate-limit — stop entire batch.
        'error': CDN error or transient failure — mark URL as error, continue.
    """
    try:
        text = (page.text_content('body') or '').lower()
        # Hard blocks — stop the batch
        captcha_indicators = [
            'verify you are a human',
            'please verify',
            'captcha',
            'are you a robot',
        ]
        if any(ind in text for ind in captcha_indicators):
            return 'captcha'
        # Soft blocks — transient/CDN errors
        error_indicators = [
            'the request could not be satisfied',  # CloudFront 403
            'access denied',
            'request blocked',
        ]
        if any(ind in text for ind in error_indicators):
            return 'error'
        return 'ok'
    except Exception:
        return 'error'


def _extract_agent_data(page) -> dict:
    """Extract listing agent and brokerage from a Redfin property page.

    Tries multiple strategies in order of reliability:
    1. Redfin agent card DOM selectors (.agent-card-wrapper structure)
    2. Text pattern matching on page content ("Bought with", "Seller's agent")
    3. JSON-LD structured data (rarely has agent info, but worth checking)

    Returns dict with keys: listing_agent, listing_office, buyer_agent, buyer_office
    (any may be None).
    """
    result = {
        'listing_agent': None,
        'listing_office': None,
        'buyer_agent': None,
        'buyer_office': None,
    }

    # Strategy 1: Redfin agent card DOM structure
    # Seller's agent: .agent-card-wrapper containing .agent-card-title "Seller's agent"
    #   Name: .agent-basic-details--heading a (or img[alt] in .agent-photo)
    #   Office: .agent-basic-details--broker span
    try:
        agent_data = page.evaluate("""() => {
            const result = {};

            // Helper: extract broker name from .agent-basic-details--broker
            function getBroker(container) {
                const brokerEl = container.querySelector('.agent-basic-details--broker');
                if (!brokerEl) return null;
                // The broker text is inside nested spans with a dot separator
                // Structure: <span><span> <span class="font-dot">•</span> BrokerName </span></span>
                const text = brokerEl.textContent?.trim()
                    ?.replace(/^[•·\\s]+/, '')  // strip leading dot/spaces
                    ?.replace(/[•·\\s]+$/, '')  // strip trailing dot/spaces
                    ?.trim();
                return text || null;
            }

            // Structure A: Redfin-agent listings (.agent-card-wrapper)
            const cards = document.querySelectorAll('.agent-card-wrapper');
            for (const card of cards) {
                const title = card.querySelector('.agent-card-title')?.textContent?.trim();
                if (!title) continue;

                const nameEl = card.querySelector('.agent-basic-details--heading a');
                const name = nameEl?.textContent?.trim()
                    || card.querySelector('.agent-photo img')?.alt?.trim();
                const office = getBroker(card);

                if (title.toLowerCase().includes('seller')) {
                    result.listing_agent = name || null;
                    result.listing_office = office || null;
                } else if (title.toLowerCase().includes('buyer')) {
                    result.buyer_agent = name || null;
                    result.buyer_office = office || null;
                }
            }

            // Structure B: Non-Redfin agent listings (.agent-info-section .agent-item)
            if (!result.listing_agent) {
                const listingItem = document.querySelector('.listing-agent-item .agent-basic-details--heading');
                if (listingItem) {
                    // Name is in a child <span> (after "Listed by" text)
                    const nameSpan = listingItem.querySelector('span');
                    result.listing_agent = nameSpan?.textContent?.trim() || null;
                    // Broker from sibling
                    const agentInfoItem = listingItem.closest('.agent-info-item') || listingItem.closest('.agent-item');
                    if (agentInfoItem) {
                        result.listing_office = getBroker(agentInfoItem);
                    }
                }
            }
            if (!result.buyer_agent) {
                const buyerItem = document.querySelector('.buyer-agent-item .agent-basic-details--heading');
                if (buyerItem) {
                    const nameSpan = buyerItem.querySelector('span');
                    result.buyer_agent = nameSpan?.textContent?.trim() || null;
                    const agentInfoItem = buyerItem.closest('.agent-info-item') || buyerItem.closest('.agent-item');
                    if (agentInfoItem) {
                        result.buyer_office = getBroker(agentInfoItem);
                    }
                }
            }

            return result;
        }""")
        if agent_data:
            for key in result:
                if agent_data.get(key):
                    result[key] = agent_data[key]

        if result['listing_agent'] or result['listing_office']:
            logger.debug('Strategy 1 (agent cards) found: %s', result)
            return result
    except Exception as e:
        logger.debug('Strategy 1 (agent cards) failed: %s', e)

    # Strategy 2: Text pattern matching
    # "Bought with Agent Name  • Office Name" (buyer agent in .agent-info-container)
    # "Seller's agentName Office" (sometimes concatenated without clear delimiters)
    try:
        body_text = page.text_content('body') or ''

        m = _BOUGHT_WITH_RE.search(body_text)
        if m:
            result['buyer_agent'] = m.group(1).strip()
            if m.group(2):
                result['buyer_office'] = m.group(2).strip()

        m = _LISTED_BY_RE.search(body_text)
        if m:
            result['listing_agent'] = m.group(1).strip()
            if m.group(2):
                result['listing_office'] = m.group(2).strip()

        if not result['listing_office']:
            m = _COURTESY_RE.search(body_text)
            if m:
                result['listing_office'] = m.group(1).strip()

        if any(result.values()):
            logger.debug('Strategy 2 (text patterns) found: %s', result)
            return result
    except Exception as e:
        logger.debug('Strategy 2 (text patterns) failed: %s', e)

    # Strategy 3: JSON-LD structured data (Redfin rarely includes agent info here)
    try:
        json_ld_text = page.evaluate("""() => {
            const scripts = document.querySelectorAll('script[type="application/ld+json"]');
            for (const s of scripts) {
                try {
                    const data = JSON.parse(s.textContent);
                    const types = Array.isArray(data['@type']) ? data['@type'] : [data['@type']];
                    if (types.includes('RealEstateListing') || types.includes('Product')) {
                        return s.textContent;
                    }
                } catch {}
            }
            return null;
        }""")
        if json_ld_text:
            import json as _json
            data = _json.loads(json_ld_text)
            if 'agent' in data:
                agent = data['agent']
                result['listing_agent'] = agent.get('name')
            if 'broker' in data:
                broker = data['broker']
                result['listing_office'] = broker.get('name') if isinstance(broker, dict) else str(broker)
            if any(result.values()):
                logger.debug('Strategy 3 (JSON-LD) found: %s', result)
    except Exception as e:
        logger.debug('Strategy 3 (JSON-LD) failed: %s', e)

    return result


def enrich_agents_from_redfin(
    conn,
    batch_size: int = 200,
    headless: bool = True,
) -> dict:
    """Visit Redfin property pages to extract agent data.

    Returns dict with keys: enriched, no_agent, errors, total_attempted.
    """
    from .database import get_enrichment_queue, set_enrichment_status

    queue = get_enrichment_queue(conn, batch_size)
    if not queue:
        logger.info('No URLs pending enrichment.')
        return {'enriched': 0, 'no_agent': 0, 'errors': 0, 'total_attempted': 0}

    logger.info('Enrichment batch: %d URLs to process', len(queue))

    from playwright.sync_api import sync_playwright

    enriched = 0
    no_agent = 0
    errors = 0
    consecutive_errors = 0

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=headless,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--no-first-run',
                '--no-default-browser-check',
            ] + (['--disable-gpu'] if headless else []),
        )

        try:
            for i, row in enumerate(queue):
                mls = row['mls_number']
                url = row['source_url']
                logger.info('[%d/%d] Enriching MLS %s: %s', i + 1, len(queue), mls, url)

                # Fresh context per page to avoid CDN session fingerprinting
                viewport = random.choice(_VIEWPORTS)
                user_agent = random.choice(_USER_AGENTS)
                context = browser.new_context(
                    user_agent=user_agent,
                    viewport=viewport,
                    locale='en-US',
                    timezone_id='America/New_York',
                )
                page = context.new_page()
                page.add_init_script(
                    'Object.defineProperty(navigator, "webdriver", {get: () => undefined});'
                )
                try:
                    from playwright_stealth import Stealth
                    Stealth().apply_stealth_sync(page)
                except ImportError:
                    pass

                try:
                    page.goto(url, wait_until='domcontentloaded', timeout=30000)
                    # Wait for agent info to render (React hydration)
                    # Two possible structures: .agent-card-wrapper (Redfin) or .agent-info-section (non-Redfin)
                    try:
                        page.wait_for_selector('.agent-card-wrapper, .agent-info-section', timeout=8000)
                    except Exception:
                        page.wait_for_timeout(3000)

                    page_status = _check_page_status(page)
                    if page_status == 'captcha':
                        logger.warning('Captcha detected. Stopping batch.')
                        set_enrichment_status(conn, mls, 'error')
                        errors += 1
                        context.close()
                        break
                    if page_status == 'error':
                        logger.warning('  CDN/access error — marking for retry.')
                        set_enrichment_status(conn, mls, 'error')
                        errors += 1
                        consecutive_errors += 1
                        context.close()
                        if consecutive_errors >= 3:
                            logger.warning('3 consecutive errors — stopping batch early.')
                            break
                        if i < len(queue) - 1:
                            random_delay(10, 20)
                        continue

                    agent_data = _extract_agent_data(page)

                    if agent_data.get('listing_agent') or agent_data.get('listing_office'):
                        set_enrichment_status(conn, mls, 'success', agent_data)
                        enriched += 1
                        consecutive_errors = 0
                        logger.info('  Found: agent=%s, office=%s',
                                    agent_data.get('listing_agent'), agent_data.get('listing_office'))
                    else:
                        set_enrichment_status(conn, mls, 'no_agent')
                        no_agent += 1
                        consecutive_errors = 0
                        logger.info('  No agent data found on page.')

                except Exception as e:
                    logger.error('  Error enriching MLS %s: %s', mls, e)
                    set_enrichment_status(conn, mls, 'error')
                    errors += 1
                    consecutive_errors += 1

                    if consecutive_errors >= 3:
                        logger.warning('3 consecutive errors — stopping batch early.')
                        context.close()
                        break

                finally:
                    context.close()

                # Rate limit between pages (skip delay after last URL)
                if i < len(queue) - 1:
                    random_delay(10, 20)

        finally:
            browser.close()

    total = enriched + no_agent + errors
    logger.info('Enrichment complete: %d enriched, %d no_agent, %d errors (of %d attempted)',
                enriched, no_agent, errors, total)
    return {'enriched': enriched, 'no_agent': no_agent, 'errors': errors, 'total_attempted': total}
