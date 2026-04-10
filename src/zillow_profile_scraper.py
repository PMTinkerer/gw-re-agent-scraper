"""Zillow profile enrichment via Firecrawl.

Scrapes individual agent profile pages to extract career stats,
recent sold transactions, and active listing counts. Uses rawHtml
format with executeJavascript to extract structured data from
Zillow's __NEXT_DATA__ and Apollo cache.
"""
from __future__ import annotations

import json
import logging
import re
import time
from datetime import datetime

from .database import get_zillow_connection, init_zillow_db
from .zillow_firecrawl import require_firecrawl_key, _MIN_DELAY_SECONDS

logger = logging.getLogger(__name__)

_EXTRACT_JS = '''(function(){
    var nd = document.getElementById('__NEXT_DATA__');
    if (!nd) return JSON.stringify({error: 'no NEXT_DATA'});
    var data = JSON.parse(nd.textContent);
    var pp = data.props.pageProps;
    var zm = /X1-Z[a-zA-Z0-9_-]+/.exec(nd.textContent);

    var stats = pp.agentSalesStats || {};
    var du = pp.displayUser || {};
    var pi = pp.professionalInformation || {};

    // Get page 1 sold rows from Apollo cache
    var soldRows = [];
    var scripts = document.querySelectorAll('script');
    for (var i = 0; i < scripts.length; i++) {
        var txt = scripts[i].textContent;
        var idx = txt.indexOf('"totalShownSales"');
        if (idx >= 0) {
            var salesIdx = txt.indexOf('"shownSales":[', idx);
            if (salesIdx >= 0) {
                var depth = 0;
                var arrStart = txt.indexOf('[', salesIdx);
                var pos = arrStart;
                for (; pos < txt.length; pos++) {
                    if (txt[pos] === '[') depth++;
                    if (txt[pos] === ']') { depth--; if (depth === 0) break; }
                }
                try { soldRows = JSON.parse(txt.substring(arrStart, pos + 1)); } catch(e) {}
            }
            break;
        }
    }

    return JSON.stringify({
        agent: du.name || null,
        office: du.businessName || null,
        screenName: du.screenName || null,
        encodedZuid: zm ? zm[0] : null,
        stats: stats,
        forSaleCount: pp.forSaleListings ? pp.forSaleListings.length : 0,
        specialties: pi.specialties || null,
        soldRows: soldRows,
    });
})()'''


def _parse_price(price_str: str | None) -> int | None:
    """Parse '$585,000' to 585000."""
    if not price_str:
        return None
    cleaned = re.sub(r'[^\d]', '', price_str)
    return int(cleaned) if cleaned else None


def _parse_sold_row(row: dict) -> dict:
    """Normalize a sold row from the Apollo cache."""
    card = row.get('saleCardData', {}) or {}
    rep_list = card.get('representedList', [])
    represented = rep_list[0] if rep_list else ''
    sold_duration = card.get('soldDuration', '')

    beds = None
    baths = None
    for attr in (row.get('attributes') or []):
        if attr.get('label') == 'bd':
            beds = attr.get('value')
        elif attr.get('label') == 'ba':
            baths = attr.get('value')

    return {
        'zpid': row.get('zpid'),
        'address': row.get('fullAddressText', ''),
        'city_state': row.get('cityState', ''),
        'sold_date': sold_duration,
        'closing_price': _parse_price(row.get('closingPrice')),
        'represented': represented,
        'beds': beds,
        'baths': baths,
    }


def _store_enrichment(conn, profile_url: str, data: dict) -> int:
    """Store profile enrichment data in the database."""
    now = datetime.utcnow().isoformat()
    stats = data.get('stats', {})

    conn.execute('''
        UPDATE zillow_profiles SET
            total_sales = COALESCE(?, total_sales),
            sales_last_12_months = COALESCE(?, sales_last_12_months),
            average_price = COALESCE(?, average_price),
            for_sale_count = ?,
            total_sold_zillow = ?,
            avg_price_3yr = ?,
            price_range_min = ?,
            price_range_max = ?,
            screen_name = COALESCE(?, screen_name),
            enrichment_status = 'success',
            scrape_status = 'success',
            scrape_attempts = scrape_attempts + 1,
            last_scraped_at = ?
        WHERE profile_url = ?
    ''', (
        stats.get('countAllTime'),
        stats.get('countLastYear'),
        stats.get('averageValueThreeYear'),
        data.get('forSaleCount'),
        stats.get('countAllTime'),
        stats.get('averageValueThreeYear'),
        stats.get('priceRangeThreeYearMin'),
        stats.get('priceRangeThreeYearMax'),
        data.get('screenName'),
        now,
        profile_url,
    ))

    sold_rows = data.get('soldRows', [])
    inserted = 0
    for row in sold_rows:
        parsed = _parse_sold_row(row)
        if not parsed['zpid']:
            continue
        try:
            conn.execute('''
                INSERT INTO zillow_sold_transactions (
                    profile_url, zpid, address, city_state,
                    sold_date, closing_price, represented,
                    beds, baths, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(profile_url, zpid) DO UPDATE SET
                    sold_date = excluded.sold_date,
                    closing_price = excluded.closing_price,
                    represented = excluded.represented
            ''', (
                profile_url, parsed['zpid'], parsed['address'],
                parsed['city_state'], parsed['sold_date'],
                parsed['closing_price'], parsed['represented'],
                parsed['beds'], parsed['baths'], now,
            ))
            inserted += 1
        except Exception as e:
            logger.debug('Sold row insert failed for %s: %s', parsed['zpid'], e)

    conn.execute('''
        UPDATE zillow_profiles SET sold_rows_scraped = ? WHERE profile_url = ?
    ''', (inserted, profile_url))
    conn.commit()
    return inserted


def _mark_failed(conn, profile_url: str, error: str) -> None:
    now = datetime.utcnow().isoformat()
    conn.execute('''
        UPDATE zillow_profiles SET
            enrichment_status = 'error',
            scrape_attempts = scrape_attempts + 1,
            last_error = ?,
            last_scraped_at = ?
        WHERE profile_url = ?
    ''', (error, now, profile_url))
    conn.commit()


def enrich_zillow_profiles(
    conn,
    *,
    batch_size: int = 50,
    max_attempts: int = 2,
) -> dict:
    """Enrich Zillow profiles with career stats and recent sold data."""
    from firecrawl import Firecrawl
    client = Firecrawl(api_key=require_firecrawl_key())

    pending = conn.execute('''
        SELECT profile_url FROM zillow_profiles
        WHERE (enrichment_status IS NULL OR enrichment_status = 'error')
          AND scrape_attempts < ?
        ORDER BY
            CASE profile_type WHEN 'team' THEN 0 ELSE 1 END,
            profile_url
        LIMIT ?
    ''', (max_attempts, batch_size)).fetchall()

    logger.info('Profile enrichment batch: %d profiles', len(pending))
    enriched = 0
    failed = 0
    last_call = 0.0

    for i, row in enumerate(pending):
        url = row['profile_url']
        logger.info('[%d/%d] Enriching %s...', i + 1, len(pending), url)

        elapsed = time.monotonic() - last_call
        if elapsed < _MIN_DELAY_SECONDS:
            time.sleep(_MIN_DELAY_SECONDS - elapsed)

        try:
            last_call = time.monotonic()
            result = client.scrape(
                url,
                formats=['rawHtml'],
                wait_for=5000,
                actions=[
                    {'type': 'wait', 'milliseconds': 5000},
                    {'type': 'executeJavascript', 'script': _EXTRACT_JS},
                ],
            )

            acts = getattr(result, 'actions', None)
            if not acts or 'javascriptReturns' not in acts:
                _mark_failed(conn, url, 'no JS returns')
                failed += 1
                continue

            ret = acts['javascriptReturns'][0]
            val = ret.get('value', ret) if isinstance(ret, dict) else ret
            data = json.loads(val)

            if 'error' in data:
                _mark_failed(conn, url, data['error'])
                failed += 1
                continue

            sold_count = _store_enrichment(conn, url, data)
            stats = data.get('stats', {})
            total = stats.get('countAllTime', 0)
            enriched += 1
            logger.info(
                '  %s: %d total sales, %d sold rows scraped, %d for sale%s',
                data.get('agent', '?'), total, sold_count,
                data.get('forSaleCount', 0),
                f' (has {total - sold_count} more not scraped)' if total > sold_count else '',
            )

        except Exception as e:
            _mark_failed(conn, url, str(e)[:200])
            failed += 1
            logger.error('  Error enriching %s: %s', url, e)

    return {'enriched': enriched, 'failed': failed, 'total': len(pending)}
