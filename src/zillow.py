"""Zillow discovery, scraping, and output helpers."""
from __future__ import annotations

import logging
import os
import random
import re
from datetime import datetime, timedelta
from urllib.parse import urlparse

from .dashboard import generate_scoped_dashboard
from .database import (
    build_observation_id,
    build_transaction_match_key,
    get_pending_zillow_profiles,
    get_team_gap_rows,
    log_team_only_sale,
    mark_zillow_profile_status,
    normalize_address,
    record_zillow_directory_profile,
    record_zillow_team_member,
    resolve_team_only_sales,
    sha256_text,
    upsert_zillow_transaction,
)
from .report import generate_leaderboard
from .scraper import (
    _STEALTH_INIT_SCRIPT,
    _USER_AGENTS,
    _VIEWPORTS,
    _simulate_human,
    random_delay,
)
from .state import TOWNS

logger = logging.getLogger(__name__)

_DEFAULT_SELLER_REPORT = os.path.join(os.path.dirname(__file__), '..', 'data', 'zillow_agent_leaderboard.md')
_DEFAULT_BUYER_REPORT = os.path.join(os.path.dirname(__file__), '..', 'data', 'zillow_buyer_leaderboard.md')
_DEFAULT_TEAM_GAP_REPORT = os.path.join(os.path.dirname(__file__), '..', 'data', 'zillow_team_gap.md')
_DEFAULT_DASHBOARD = os.path.join(os.path.dirname(__file__), '..', 'data', 'zillow_dashboard.html')
_ZILLOW_PROFESSIONALS_HOME_URL = 'https://www.zillow.com/professionals/real-estate-agent-reviews/'

_DIRECTORY_LOCAL_SALES_RE = re.compile(
    r'(?P<count>[\d,]+)\s+(?:team\s+)?sales?\s+in\s+(?P<town>[A-Za-z ]+)',
    re.IGNORECASE,
)
_SALES_LAST_12_RE = re.compile(
    r'(?P<count>[\d,]+)\s+(?:team\s+)?sales?\s+last 12 months',
    re.IGNORECASE,
)
_SOLD_ROW_RE = re.compile(
    r'^(?P<address>.+?)\s+Sold date:\s*(?P<sale_date>\d{1,2}/\d{1,2}/\d{4})'
    r'\s+Closing price:\s*\$(?P<sale_price>[\d,]+)\s+Represented:\s*(?P<represented>Buyer|Seller)',
    re.IGNORECASE,
)
_ADDRESS_CITY_RE = re.compile(
    r'^(?P<street>.+?)\s+(?P<city>[^,]+),\s*(?P<state>[A-Z]{2}),?\s*(?P<zip>\d{5})$',
)
_PAGE_INFO_RE = re.compile(r'Page\s+(\d+)\s+of\s+(\d+)', re.IGNORECASE)
_PROFILE_STAT_RE = {
    'sales_last_12_months': re.compile(r'([\d,]+)\s+Sales last 12 months', re.IGNORECASE),
    'total_sales': re.compile(r'([\d,]+)\s+Total sales', re.IGNORECASE),
    'average_price': re.compile(r'\$([\d,]+)\s+Average price', re.IGNORECASE),
    'price_range': re.compile(r'(\$[\dKMB,.~-]+\s*-\s*\$?[\dKMB,.]+)\s+Price range', re.IGNORECASE),
}
_ZILLOW_BLOCK_INDICATORS = {
    'captcha': [
        'captcha',
        'px-captcha',
        'captcha.px-cloud.net',
        'perimeterx',
        'verify you are human',
        'before we continue',
        'press and hold',
        'press & hold',
        'security check',
    ],
    'blocked': [
        '/captcha/',
        'access to this page has been denied',
        'request unsuccessful',
        '403 forbidden',
        'temporarily unavailable',
        'access denied',
    ],
}


class ZillowAccessError(RuntimeError):
    """Raised when Zillow blocks or fails a page load after retries."""

    def __init__(self, status: str, message: str, *, status_code: int | None = None):
        super().__init__(message)
        self.status = status
        self.status_code = status_code


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw in (None, ''):
        return default
    try:
        return max(0.0, float(raw))
    except ValueError:
        logger.warning('Invalid float for %s=%r. Using default %.2f', name, raw, default)
        return default


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw in (None, ''):
        return default
    try:
        return max(0, int(raw))
    except ValueError:
        logger.warning('Invalid int for %s=%r. Using default %d', name, raw, default)
        return default


def _pause_from_env(min_env: str, max_env: str, default_min: float, default_max: float) -> None:
    delay_min = _env_float(min_env, default_min)
    delay_max = _env_float(max_env, default_max)
    if delay_max <= 0:
        return
    if delay_max < delay_min:
        delay_min, delay_max = delay_max, delay_min
    random_delay(delay_min, delay_max)


def _town_slug(town: str) -> str:
    return town.lower().replace(' ', '-')


def _parse_int(value: str | int | None) -> int | None:
    if value in (None, ''):
        return None
    return int(str(value).replace(',', '').strip())


def _parse_us_date(date_str: str | None) -> str | None:
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str.strip(), '%m/%d/%Y').strftime('%Y-%m-%d')
    except ValueError:
        return None


def _target_town_set(towns: list[str] | None = None) -> set[str]:
    return {town.lower() for town in (towns or TOWNS)}


def _town_directory_url(town: str) -> str:
    return f'https://www.zillow.com/professionals/real-estate-agent-reviews/{_town_slug(town)}-me/'


def _extract_profile_card_candidates(raw_links: list[dict], town: str) -> list[dict]:
    """Parse directory card text into profile candidates."""
    candidates = []
    seen: set[str] = set()
    for item in raw_links:
        href = item.get('href') or ''
        text = ' '.join((item.get('text') or '').split())
        if '/profile/' not in href or not text or href in seen:
            continue
        local_match = _DIRECTORY_LOCAL_SALES_RE.search(text)
        if not local_match:
            continue
        if local_match.group('town').strip().lower() != town.lower():
            continue
        local_sales = _parse_int(local_match.group('count')) or 0
        if local_sales <= 0:
            continue
        sales_12_match = _SALES_LAST_12_RE.search(text)
        candidates.append({
            'profile_url': href,
            'profile_type': 'team' if text.upper().startswith('TEAM ') or ' team sales ' in text.lower() else 'individual',
            'local_sales_count': local_sales,
            'sales_last_12_months': _parse_int(sales_12_match.group('count')) if sales_12_match else None,
            'raw_card_text': text,
        })
        seen.add(href)
    return candidates


def _parse_sold_row(item: dict) -> dict | None:
    """Parse a Zillow sold-row anchor into a structured row."""
    href = item.get('href') or ''
    text = ' '.join((item.get('text') or '').split())
    match = _SOLD_ROW_RE.search(text)
    if not match:
        return None

    address_text = match.group('address').strip()
    address_parts = _split_address_city(address_text)
    if not address_parts:
        return None

    represented = match.group('represented').strip().lower()
    sale_date = _parse_us_date(match.group('sale_date'))
    sale_price = _parse_int(match.group('sale_price'))
    street, city, state, zip_code = address_parts
    normalized_address = normalize_address(street, city, state, zip_code)
    normalized_address_hash = sha256_text(normalized_address)
    transaction_match_key = build_transaction_match_key(
        normalized_address_hash,
        sale_date,
        sale_price,
    )

    return {
        'source_url': href,
        'address': street,
        'city': city,
        'state': state,
        'zip': zip_code,
        'sale_date': sale_date,
        'sale_price': sale_price,
        'represented_side': represented,
        'normalized_address': normalized_address,
        'normalized_address_hash': normalized_address_hash,
        'transaction_match_key': transaction_match_key,
        'raw_text': text,
    }


def _parse_page_info(section_text: str) -> tuple[int, int] | None:
    match = _PAGE_INFO_RE.search(section_text or '')
    if not match:
        return None
    return int(match.group(1)), int(match.group(2))


def _split_address_city(address_text: str) -> tuple[str, str, str, str] | None:
    match = re.match(r'^(?P<prefix>.+),\s*(?P<state>[A-Z]{2}),?\s*(?P<zip>\d{5})$', address_text)
    if not match:
        return None

    prefix = match.group('prefix').strip()
    state = match.group('state').strip()
    zip_code = match.group('zip').strip()
    prefix_upper = prefix.upper()
    for town in sorted(TOWNS, key=len, reverse=True):
        town_upper = town.upper()
        suffix = f' {town_upper}'
        if prefix_upper.endswith(suffix):
            street = prefix[:-len(suffix)].strip()
            return street, town, state, zip_code
        if prefix_upper == town_upper:
            return '', town, state, zip_code

    fallback = _ADDRESS_CITY_RE.match(address_text)
    if not fallback:
        return None
    return (
        fallback.group('street').strip(),
        fallback.group('city').strip(),
        fallback.group('state').strip(),
        fallback.group('zip').strip(),
    )


def _classify_zillow_document(
    text: str | None,
    *,
    page_url: str | None = None,
    title: str | None = None,
    status_code: int | None = None,
) -> dict:
    """Classify a Zillow response as ok, captcha, or blocked."""
    haystack = ' '.join(
        part for part in [
            (text or '').lower(),
            (page_url or '').lower(),
            (title or '').lower(),
        ] if part
    )
    if any(indicator in haystack for indicator in _ZILLOW_BLOCK_INDICATORS['captcha']):
        return {
            'status': 'captcha',
            'reason': 'captcha_indicators',
            'status_code': status_code,
        }
    if any(indicator in haystack for indicator in _ZILLOW_BLOCK_INDICATORS['blocked']):
        return {
            'status': 'blocked',
            'reason': 'blocked_indicators',
            'status_code': status_code,
        }
    if status_code in (403, 429):
        return {
            'status': 'blocked',
            'reason': f'http_{status_code}',
            'status_code': status_code,
        }
    if status_code and status_code >= 400:
        return {
            'status': 'blocked',
            'reason': f'http_{status_code}',
            'status_code': status_code,
        }
    return {
        'status': 'ok',
        'reason': 'ok',
        'status_code': status_code,
    }


def _check_zillow_page_status(page, response=None) -> dict:
    """Inspect the current document and classify the result."""
    try:
        document = page.content()
    except Exception:
        document = ''
    try:
        title = page.title()
    except Exception:
        title = ''
    try:
        page_url = page.url
    except Exception:
        page_url = ''
    status_code = getattr(response, 'status', None)
    return _classify_zillow_document(
        document,
        page_url=page_url,
        title=title,
        status_code=status_code,
    )


def _guess_office_name(body_text: str, profile_name: str | None) -> str | None:
    if not body_text or not profile_name:
        return None
    lines = [line.strip() for line in body_text.splitlines() if line.strip()]
    try:
        idx = lines.index(profile_name)
    except ValueError:
        return None
    for line in lines[idx + 1:idx + 8]:
        lowered = line.lower()
        if any(token in lowered for token in (
            'reviews', 'contact', 'recent sales', 'report a problem',
            'sales last 12 months', 'total sales', 'price range',
            'average price', 'get to know', 'visit ', 'specialties',
        )):
            continue
        return line
    return None


def _extract_profile_metadata(page) -> dict:
    """Extract high-level Zillow profile metadata from the page."""
    try:
        profile_name = (page.locator('h1').first.text_content() or '').strip() or None
    except Exception:
        profile_name = None
    body_text = page.text_content('body') or ''
    office_name = _guess_office_name(body_text, profile_name)

    metadata = {
        'profile_name': profile_name,
        'office_name': office_name,
    }
    for key, pattern in _PROFILE_STAT_RE.items():
        match = pattern.search(body_text)
        metadata[key] = _parse_int(match.group(1)) if match and key != 'price_range' else (
            match.group(1).strip() if match else None
        )
    return metadata


def _section_text(page, heading_pattern: str) -> str:
    text = page.evaluate("""(pattern) => {
        const regex = new RegExp(pattern, 'i');
        const headings = Array.from(document.querySelectorAll('h1,h2,h3'));
        const heading = headings.find((el) => regex.test((el.innerText || el.textContent || '').trim()));
        if (!heading) return '';
        const parts = [];
        let node = heading.nextElementSibling;
        while (node && !/^H[123]$/.test(node.tagName)) {
            const text = (node.innerText || node.textContent || '').trim();
            if (text) parts.push(text);
            node = node.nextElementSibling;
        }
        if (!parts.length) {
            const container = heading.closest('section, article, [data-testid], div');
            if (container) {
                const text = (container.innerText || container.textContent || '').trim();
                if (text) parts.push(text);
            }
        }
        return parts.join('\\n');
    }""", heading_pattern)
    return ' '.join((text or '').split())


def _section_links(page, heading_pattern: str) -> list[dict]:
    return page.evaluate("""(pattern) => {
        const regex = new RegExp(pattern, 'i');
        const headings = Array.from(document.querySelectorAll('h1,h2,h3'));
        const heading = headings.find((el) => regex.test((el.innerText || el.textContent || '').trim()));
        if (!heading) return [];
        const links = [];
        let node = heading.nextElementSibling;
        while (node && !/^H[123]$/.test(node.tagName)) {
            if (node.matches && node.matches('a[href]')) {
                links.push(node);
            }
            if (node.querySelectorAll) {
                links.push(...Array.from(node.querySelectorAll('a[href]')));
            }
            node = node.nextElementSibling;
        }
        if (!links.length) {
            const container = heading.closest('section, article, [data-testid], div');
            if (container && container.querySelectorAll) {
                links.push(...Array.from(container.querySelectorAll('a[href]')));
            }
        }
        const seen = new Set();
        return links
            .map((link) => ({
                href: link.href || '',
                text: (link.innerText || link.textContent || '').trim(),
            }))
            .filter((item) => item.href && !seen.has(item.href) && seen.add(item.href));
    }""", heading_pattern)


def _extract_cardish_links(page, href_substring: str) -> list[dict]:
    """Extract anchors plus nearby card text from loosely structured Zillow pages."""
    return page.evaluate("""(hrefSubstring) => {
        const anchors = Array.from(document.querySelectorAll(`a[href*="${hrefSubstring}"]`));
        const best = new Map();

        function normalize(text) {
            return (text || '').replace(/\\s+/g, ' ').trim();
        }

        function pickContainer(anchor) {
            let node = anchor;
            let bestNode = anchor;
            for (let depth = 0; node && depth < 6; depth += 1) {
                const text = normalize(node.innerText || node.textContent || '');
                const profileLinks = node.querySelectorAll ? node.querySelectorAll(`a[href*="${hrefSubstring}"]`).length : 0;
                if (text && text.length <= 1200 && profileLinks <= 4) {
                    bestNode = node;
                }
                node = node.parentElement;
            }
            return bestNode;
        }

        for (const anchor of anchors) {
            const href = anchor.href || '';
            if (!href) continue;
            const container = pickContainer(anchor);
            const text = normalize((container.innerText || container.textContent || '')) || normalize(anchor.innerText || anchor.textContent || '');
            if (!text) continue;
            const existing = best.get(href);
            if (!existing || text.length > existing.text.length) {
                best.set(href, {href, text});
            }
        }

        return Array.from(best.values());
    }""", href_substring)


def _extract_directory_card_links(page) -> list[dict]:
    """Return directory card candidates with enough surrounding text for parsing."""
    return _extract_cardish_links(page, '/profile/')


def _extract_sold_row_links(page) -> list[dict]:
    """Return sold-row anchors with surrounding row text."""
    return _extract_cardish_links(page, '/homedetails/')


def _has_profile_links(page) -> bool:
    try:
        return bool(_extract_directory_card_links(page))
    except Exception:
        return False


def _profile_has_identity(page) -> bool:
    try:
        return bool(page.evaluate("""() => {
            const heading = document.querySelector('h1');
            return !!((heading && (heading.innerText || heading.textContent || '').trim()));
        }"""))
    except Exception:
        return False


def _click_section_page(page, heading_pattern: str, target_page: int) -> bool:
    return bool(page.evaluate("""(payload) => {
        const regex = new RegExp(payload.pattern, 'i');
        const headings = Array.from(document.querySelectorAll('h1,h2,h3'));
        const heading = headings.find((el) => regex.test((el.innerText || el.textContent || '').trim()));
        if (!heading) return false;
        const candidates = [];
        let node = heading.nextElementSibling;
        while (node && !/^H[123]$/.test(node.tagName)) {
            if (node.matches && node.matches('a[href],button')) {
                candidates.push(node);
            }
            if (node.querySelectorAll) {
                candidates.push(...Array.from(node.querySelectorAll('a[href],button')));
            }
            node = node.nextElementSibling;
        }
        const target = String(payload.targetPage);
        const match = candidates.find((el) => (el.innerText || el.textContent || '').trim() === target);
        if (!match) return false;
        match.click();
        return true;
    }""", {'pattern': heading_pattern, 'targetPage': target_page}))


def _extract_team_member_links(page, current_profile_url: str) -> list[dict]:
    links = _section_links(page, r'^Meet\b|^Team members?\b')
    members = []
    seen: set[str] = set()
    for item in links:
        href = item.get('href') or ''
        if '/profile/' not in href or href == current_profile_url or href in seen:
            continue
        members.append({
            'profile_url': href,
            'member_name': ' '.join((item.get('text') or '').split()) or None,
        })
        seen.add(href)
    return members


def _extract_sold_rows_from_profile(
    page,
    *,
    target_towns: set[str],
    date_cutoff: str,
    max_pages: int = 40,
) -> list[dict]:
    """Extract sold rows from a Zillow profile, paginating through the sold section."""
    sold_rows: list[dict] = []
    seen_match_keys: set[tuple[str | None, str]] = set()

    for _ in range(max_pages):
        links = _extract_sold_row_links(page)
        for item in links:
            if '/homedetails/' not in (item.get('href') or ''):
                continue
            parsed = _parse_sold_row(item)
            if not parsed:
                continue
            if parsed['sale_date'] and parsed['sale_date'] < date_cutoff:
                continue
            if parsed['city'].strip().lower() not in target_towns:
                continue
            key = (parsed['transaction_match_key'], parsed['represented_side'])
            if key in seen_match_keys:
                continue
            sold_rows.append(parsed)
            seen_match_keys.add(key)

        section_text = _section_text(page, r'^Sold\b')
        page_info = _parse_page_info(section_text)
        if not page_info:
            break
        current_page, total_pages = page_info
        if current_page >= total_pages or current_page >= max_pages:
            break
        if not _click_section_page(page, r'^Sold\b', current_page + 1):
            break
        page.wait_for_timeout(random.randint(1200, 2200))
        _simulate_human(page)
        _pause_from_env(
            'ZILLOW_PAGINATION_DELAY_MIN',
            'ZILLOW_PAGINATION_DELAY_MAX',
            1.0,
            2.0,
        )

    return sold_rows


def _configure_context(context, route_handler) -> None:
    context.route('**/*', route_handler)
    context.add_init_script(_STEALTH_INIT_SCRIPT)


def _build_proxy_base() -> dict | None:
    proxy_url = os.environ.get('PROXY_URL')
    if not proxy_url:
        return None
    parsed = urlparse(proxy_url)
    return {
        'server': f'{parsed.scheme}://{parsed.hostname}:{parsed.port}',
        'username': parsed.username or '',
        'password': parsed.password or '',
    }


def _rotated_proxy(proxy_base: dict | None) -> dict | None:
    if not proxy_base:
        return None
    password = proxy_base['password']
    fresh_session = f'session-{random.randint(100000, 999999)}'
    rotated_password = re.sub(r'session-[^_&]+', fresh_session, password) if password else password
    return {**proxy_base, 'password': rotated_password}


def _launch_browser(pw, headless: bool = True):
    return pw.chromium.launch(
        headless=headless,
        args=[
            '--disable-blink-features=AutomationControlled',
            '--no-first-run',
            '--no-default-browser-check',
            '--disable-dev-shm-usage',
        ] + (['--disable-gpu'] if headless else []),
    )


def _new_page(browser, proxy_base: dict | None, route_handler):
    context = browser.new_context(
        user_agent=random.choice(_USER_AGENTS),
        viewport=random.choice(_VIEWPORTS),
        locale='en-US',
        timezone_id='America/New_York',
        color_scheme='light',
        reduced_motion='no-preference',
        extra_http_headers={
            'Accept-Language': 'en-US,en;q=0.9',
            'DNT': '1',
            'Upgrade-Insecure-Requests': '1',
        },
        **(dict(proxy=_rotated_proxy(proxy_base)) if proxy_base else {}),
    )
    _configure_context(context, route_handler)
    page = context.new_page()
    try:
        from playwright_stealth import Stealth
        Stealth().apply_stealth_sync(page)
    except ImportError:
        pass
    return context, page


def _close_context(context) -> None:
    if context is None:
        return
    try:
        context.close()
    except Exception:
        pass


def _format_status_message(status_info: dict, phase: str) -> str:
    status = status_info.get('status', 'failed')
    reason = status_info.get('reason', 'unknown')
    status_code = status_info.get('status_code')
    code_text = f' status={status_code}' if status_code is not None else ''
    return f'{phase} {status} ({reason}){code_text}'


def _warm_context_session(page, *, label: str, warmup_url: str | None = None) -> None:
    logger.debug('Pre-warming Zillow session for %s', label)
    warmup_target = warmup_url or _ZILLOW_PROFESSIONALS_HOME_URL
    warm_response = page.goto(warmup_target, wait_until='domcontentloaded', timeout=30000)
    page.wait_for_timeout(random.randint(2000, 4000))
    _simulate_human(page)
    status_info = _check_zillow_page_status(page, response=warm_response)
    if status_info['status'] != 'ok':
        raise ZillowAccessError(
            status_info['status'],
            _format_status_message(status_info, f'{label} warmup'),
            status_code=status_info.get('status_code'),
        )
    _pause_from_env('ZILLOW_WARMUP_DELAY_MIN', 'ZILLOW_WARMUP_DELAY_MAX', 2.0, 4.0)


def _page_has_ready_content(page, page_kind: str) -> bool:
    if page_kind == 'directory':
        return _has_profile_links(page)
    if page_kind == 'profile':
        return _profile_has_identity(page)
    return True


def _load_zillow_page(
    browser,
    proxy_base: dict | None,
    route_handler,
    target_url: str,
    *,
    page_kind: str,
    max_attempts: int,
):
    """Load a Zillow page with same-session warmup, retries, and fresh proxy rotation."""
    last_status = {
        'status': 'failed',
        'reason': 'not_attempted',
        'status_code': None,
    }
    for attempt in range(1, max_attempts + 1):
        context = page = None
        try:
            logger.info('Zillow %s attempt %d/%d: %s', page_kind, attempt, max_attempts, target_url)
            context, page = _new_page(browser, proxy_base, route_handler)
            try:
                _warm_context_session(
                    page,
                    label=page_kind,
                    warmup_url=_ZILLOW_PROFESSIONALS_HOME_URL,
                )
            except ZillowAccessError as warmup_exc:
                logger.warning(
                    'Zillow %s warmup failed (%s). Retrying target URL without warmup on a fresh context.',
                    page_kind,
                    warmup_exc,
                )
                _close_context(context)
                context, page = _new_page(browser, proxy_base, route_handler)

            response = page.goto(target_url, wait_until='domcontentloaded', timeout=45000)
            page.wait_for_timeout(random.randint(2500, 4500))
            _simulate_human(page)

            last_status = _check_zillow_page_status(page, response=response)
            if last_status['status'] != 'ok':
                raise ZillowAccessError(
                    last_status['status'],
                    _format_status_message(last_status, page_kind),
                    status_code=last_status.get('status_code'),
                )

            if not _page_has_ready_content(page, page_kind):
                page.wait_for_timeout(random.randint(2500, 4500))
                _simulate_human(page)
                last_status = _check_zillow_page_status(page)
                if last_status['status'] != 'ok':
                    raise ZillowAccessError(
                        last_status['status'],
                        _format_status_message(last_status, page_kind),
                        status_code=last_status.get('status_code'),
                    )
                if not _page_has_ready_content(page, page_kind):
                    raise ZillowAccessError('failed', f'{page_kind} content did not render after warmup')

            return context, page
        except ZillowAccessError:
            _close_context(context)
            context = None
            if attempt >= max_attempts:
                raise
            _pause_from_env('ZILLOW_BLOCK_BACKOFF_MIN', 'ZILLOW_BLOCK_BACKOFF_MAX', 20.0, 40.0)
        except Exception as exc:
            _close_context(context)
            context = None
            if attempt >= max_attempts:
                raise ZillowAccessError('failed', f'{page_kind} load failed: {exc}') from exc
            _pause_from_env('ZILLOW_RETRY_BACKOFF_MIN', 'ZILLOW_RETRY_BACKOFF_MAX', 8.0, 15.0)

    raise ZillowAccessError(
        last_status.get('status', 'failed'),
        _format_status_message(last_status, page_kind),
        status_code=last_status.get('status_code'),
    )


def discover_zillow_profiles(
    conn,
    state: dict,
    *,
    towns: list[str] | None = None,
    headless: bool = True,
    state_path: str | None = None,
) -> dict:
    """Discover Zillow individual and team profiles from town directory pages."""
    from playwright.sync_api import sync_playwright
    from .zillow_state import mark_complete, mark_failed, mark_started, save_state

    proxy_base = _build_proxy_base()
    blocked_resource_types = {'image', 'media'}
    directory_attempts = max(1, _env_int('ZILLOW_DIRECTORY_ATTEMPTS', 3))

    def _block_heavy(route):
        if route.request.resource_type in blocked_resource_types:
            route.abort()
        else:
            route.fallback()

    towns_to_process = towns or TOWNS
    profiles_found = 0
    processed_towns = 0

    if not proxy_base:
        logger.warning('No PROXY_URL set for Zillow discovery. Zillow frequently blocks direct datacenter IPs.')
    _pause_from_env('ZILLOW_STARTUP_DELAY_MIN', 'ZILLOW_STARTUP_DELAY_MAX', 15.0, 30.0)

    with sync_playwright() as pw:
        browser = _launch_browser(pw, headless=headless)
        try:
            for town in towns_to_process:
                directory_url = _town_directory_url(town)
                logger.info('Discovering Zillow profiles for %s: %s', town, directory_url)
                mark_started(state, town)
                save_state(state, state_path)

                context = page = None
                town_profiles = 0
                try:
                    context, page = _load_zillow_page(
                        browser,
                        proxy_base,
                        _block_heavy,
                        directory_url,
                        page_kind='directory',
                        max_attempts=directory_attempts,
                    )

                    visited_pages: set[int] = set()
                    for _ in range(50):
                        raw_links = _extract_directory_card_links(page)
                        candidates = _extract_profile_card_candidates(raw_links, town)
                        for candidate in candidates:
                            record_zillow_directory_profile(
                                conn,
                                town,
                                candidate['profile_url'],
                                candidate['profile_type'],
                                candidate['local_sales_count'],
                                raw_card_text=candidate['raw_card_text'],
                            )
                        town_profiles += len(candidates)
                        section_text = _section_text(page, r'^Real estate agents in ')
                        page_info = _parse_page_info(section_text)
                        if not page_info:
                            break
                        current_page, total_pages = page_info
                        if current_page in visited_pages:
                            break
                        visited_pages.add(current_page)
                        if current_page >= total_pages:
                            break
                        if not _click_section_page(page, r'^Real estate agents in ', current_page + 1):
                            break
                        page.wait_for_timeout(random.randint(1200, 2200))
                        _simulate_human(page)
                        _pause_from_env(
                            'ZILLOW_PAGINATION_DELAY_MIN',
                            'ZILLOW_PAGINATION_DELAY_MAX',
                            1.0,
                            2.0,
                        )

                    profiles_found += town_profiles
                    processed_towns += 1
                    mark_complete(state, town, profiles_found=town_profiles)
                except Exception as exc:
                    mark_failed(state, town, str(exc))
                    logger.error('Zillow discovery failed for %s: %s', town, exc)
                finally:
                    save_state(state, state_path)
                    _close_context(context)
                    _pause_from_env(
                        'ZILLOW_DIRECTORY_DELAY_MIN',
                        'ZILLOW_DIRECTORY_DELAY_MAX',
                        6.0,
                        10.0,
                    )
        finally:
            browser.close()

    return {
        'towns_processed': processed_towns,
        'profiles_found': profiles_found,
    }


def scrape_zillow_profiles(
    conn,
    *,
    batch_size: int = 20,
    headless: bool = True,
    towns: list[str] | None = None,
) -> dict:
    """Scrape pending Zillow profiles, logging team-only rows and storing individual observations."""
    from playwright.sync_api import sync_playwright

    pending = get_pending_zillow_profiles(conn, batch_size=batch_size)
    if not pending:
        logger.info('No Zillow profiles pending scrape.')
        return {'processed': 0, 'individual_rows': 0, 'team_rows': 0, 'blocked': 0}

    proxy_base = _build_proxy_base()
    blocked_resource_types = {'image', 'media'}
    target_towns = _target_town_set(towns)
    date_cutoff = (datetime.utcnow() - timedelta(days=1095)).strftime('%Y-%m-%d')
    profile_attempts = max(1, _env_int('ZILLOW_PROFILE_ATTEMPTS', 3))

    def _block_heavy(route):
        if route.request.resource_type in blocked_resource_types:
            route.abort()
        else:
            route.fallback()

    processed = 0
    individual_rows = 0
    team_rows = 0
    hard_blocks = 0
    captchas = 0
    navigation_failures = 0

    if not proxy_base:
        logger.warning('No PROXY_URL set for Zillow profile scraping. Zillow frequently blocks direct datacenter IPs.')
    _pause_from_env('ZILLOW_STARTUP_DELAY_MIN', 'ZILLOW_STARTUP_DELAY_MAX', 15.0, 30.0)

    with sync_playwright() as pw:
        browser = _launch_browser(pw, headless=headless)
        try:
            for profile in pending:
                profile_url = profile['profile_url']
                profile_type = profile['profile_type']
                logger.info('Scraping Zillow %s profile: %s', profile_type, profile_url)

                context = page = None
                try:
                    context, page = _load_zillow_page(
                        browser,
                        proxy_base,
                        _block_heavy,
                        profile_url,
                        page_kind='profile',
                        max_attempts=profile_attempts,
                    )

                    metadata = _extract_profile_metadata(page)
                    sold_rows = _extract_sold_rows_from_profile(
                        page,
                        target_towns=target_towns,
                        date_cutoff=date_cutoff,
                    )

                    if profile_type == 'team':
                        for member in _extract_team_member_links(page, profile_url):
                            record_zillow_team_member(
                                conn,
                                profile_url,
                                member['profile_url'],
                                member_name=member['member_name'],
                            )
                        for row in sold_rows:
                            log_team_only_sale(conn, {
                                'team_profile_url': profile_url,
                                'team_name': metadata.get('profile_name'),
                                'property_url': row['source_url'],
                                'represented_side': row['represented_side'],
                                'sale_date': row['sale_date'],
                                'sale_price': row['sale_price'],
                                'normalized_address': row['normalized_address'],
                                'normalized_address_hash': row['normalized_address_hash'],
                                'transaction_match_key': row['transaction_match_key'],
                                'local_town': row['city'],
                            })
                        team_rows += len(sold_rows)
                    else:
                        profile_name = metadata.get('profile_name')
                        office_name = metadata.get('office_name')
                        for row in sold_rows:
                            represented_side = row['represented_side']
                            normalized_address_hash = row['normalized_address_hash']
                            observation_id = build_observation_id(
                                profile_url,
                                represented_side,
                                normalized_address_hash,
                                row['sale_date'],
                                row['sale_price'],
                            )
                            record = {
                                'observation_id': observation_id,
                                'address': row['address'],
                                'city': row['city'],
                                'state': row['state'],
                                'zip': row['zip'],
                                'sale_price': row['sale_price'],
                                'sale_date': row['sale_date'],
                                'source_url': row['source_url'],
                                'data_source': 'zillow',
                                'represented_side': represented_side,
                                'agent_profile_url': profile_url,
                                'profile_type': 'individual',
                                'normalized_address': row['normalized_address'],
                                'normalized_address_hash': normalized_address_hash,
                                'transaction_match_key': row['transaction_match_key'],
                                'local_directory_town': row['city'],
                                'attribution_confidence': 'profile_individual',
                            }
                            if represented_side == 'seller':
                                record['listing_agent'] = profile_name
                                record['listing_office'] = office_name
                            else:
                                record['buyer_agent'] = profile_name
                                record['buyer_office'] = office_name
                            if upsert_zillow_transaction(conn, record):
                                resolve_team_only_sales(conn, row['transaction_match_key'], represented_side)
                                individual_rows += 1

                    mark_zillow_profile_status(conn, profile_url, 'success', metadata=metadata)
                    processed += 1
                    hard_blocks = 0
                    captchas = 0
                    navigation_failures = 0
                except ZillowAccessError as exc:
                    logger.error('Failed scraping Zillow profile %s: %s', profile_url, exc)
                    mark_zillow_profile_status(conn, profile_url, exc.status, error=str(exc))
                    if exc.status == 'captcha':
                        captchas += 1
                    elif exc.status == 'blocked':
                        hard_blocks += 1
                    else:
                        navigation_failures += 1
                except Exception as exc:
                    message = str(exc)
                    logger.error('Failed scraping Zillow profile %s: %s', profile_url, message)
                    mark_zillow_profile_status(conn, profile_url, 'failed', error=message)
                    navigation_failures += 1
                finally:
                    _close_context(context)

                if captchas >= 2 or hard_blocks >= 4 or navigation_failures >= 6:
                    logger.warning('Zillow circuit breaker triggered. Stopping batch early.')
                    break

                if hard_blocks > 0 or captchas > 0:
                    _pause_from_env('ZILLOW_BLOCK_BACKOFF_MIN', 'ZILLOW_BLOCK_BACKOFF_MAX', 20.0, 40.0)
                else:
                    _pause_from_env(
                        'ZILLOW_PROFILE_DELAY_MIN',
                        'ZILLOW_PROFILE_DELAY_MAX',
                        3.0,
                        5.0,
                    )
        finally:
            browser.close()

    return {
        'processed': processed,
        'individual_rows': individual_rows,
        'team_rows': team_rows,
        'blocked': hard_blocks + captchas,
    }


def generate_team_gap_report(conn, output_path: str | None = None) -> str:
    """Generate a markdown report of unresolved team-only rows."""
    output_path = output_path or _DEFAULT_TEAM_GAP_REPORT
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    rows = get_team_gap_rows(conn)
    grouped: dict[tuple[str, str], int] = {}
    for row in rows:
        grouped[(row['team_name'] or 'Unknown Team', row['local_town'] or 'Unknown')] = (
            grouped.get((row['team_name'] or 'Unknown Team', row['local_town'] or 'Unknown'), 0) + 1
        )

    lines = [
        '# Zillow Team-Only Gap Report',
        '',
        f'_Generated: {datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")}_',
        '',
        f'- **Unresolved team-only sales:** {len(rows):,}',
        '',
        '## Summary by Team / Town',
        '',
    ]

    if grouped:
        lines.append('| Team | Town | Unresolved Sales |')
        lines.append('|------|------|------------------|')
        for (team_name, town), count in sorted(grouped.items()):
            lines.append(f'| {team_name} | {town} | {count} |')
    else:
        lines.append('_No unresolved team-only sales found._')

    lines.extend([
        '',
        '## Recent Unresolved Sales',
        '',
    ])
    if rows:
        lines.append('| Team | Town | Side | Sale Date | Price | Property |')
        lines.append('|------|------|------|-----------|-------|----------|')
        for row in rows[:100]:
            lines.append(
                f'| {row["team_name"] or "Unknown"} | {row["local_town"] or "Unknown"} | '
                f'{row["represented_side"].title()} | {row["sale_date"] or "N/A"} | '
                f'${row["sale_price"]:,} | {row["property_url"] or "N/A"} |'
            )
    else:
        lines.append('_No unresolved sales to review._')

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))

    logger.info('Zillow team-gap report written to %s', output_path)
    return output_path


def generate_zillow_outputs(conn) -> dict:
    """Generate the public seller-side Zillow outputs and internal artifacts."""
    seller_report = generate_leaderboard(
        conn,
        _DEFAULT_SELLER_REPORT,
        source='zillow',
        role='seller',
        title='# Zillow Seller-Side Agent Leaderboard -- Southern Coastal Maine',
    )
    buyer_report = generate_leaderboard(
        conn,
        _DEFAULT_BUYER_REPORT,
        source='zillow',
        role='buyer',
        title='# Zillow Buyer-Side Agent Leaderboard -- Southern Coastal Maine',
    )
    dashboard = generate_scoped_dashboard(
        conn,
        output_path=_DEFAULT_DASHBOARD,
        source='zillow',
        role='seller',
        heading='Zillow Seller-Side Leaderboard',
        subtitle='Southern Coastal Maine',
        source_label='Zillow',
        description='Zillow seller-side represented sales leaderboard across 10 southern coastal Maine towns.',
    )
    team_gap = generate_team_gap_report(conn, _DEFAULT_TEAM_GAP_REPORT)
    return {
        'seller_report': seller_report,
        'buyer_report': buyer_report,
        'dashboard': dashboard,
        'team_gap_report': team_gap,
    }
