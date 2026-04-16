"""HTML dashboard for Maine Listings (MREIS MLS) leaderboards.

Produces data/maine_dashboard.html — a standalone page showing combined,
listing-side, buyer-side, and brokerage leaderboards. Designed to be
iframed into the main tabbed index page.
"""
from __future__ import annotations

import logging
import os
import sqlite3
from datetime import datetime
from html import escape

from .dashboard import _css, _sort_js
from .maine_report import (
    format_currency,
    query_top_agents,
    query_top_brokerages,
    query_top_combined_agents,
)
from .state import TOWNS

logger = logging.getLogger(__name__)

_DEFAULT_DASHBOARD = os.path.join(
    os.path.dirname(__file__), '..', 'data', 'maine_dashboard.html',
)


def _e(text) -> str:
    if text is None:
        return 'N/A'
    return escape(str(text))


def _get_dashboard_stats(conn: sqlite3.Connection) -> dict:
    row = conn.execute('''
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN enrichment_status = 'success' THEN 1 ELSE 0 END) AS enriched,
            SUM(CASE WHEN listing_agent IS NOT NULL THEN 1 ELSE 0 END) AS has_listing,
            SUM(CASE WHEN buyer_agent IS NOT NULL THEN 1 ELSE 0 END) AS has_buyer,
            SUM(COALESCE(sale_price, 0)) AS volume,
            MIN(close_date) AS date_min,
            MAX(close_date) AS date_max
        FROM maine_transactions
    ''').fetchone()

    unique_agents = conn.execute('''
        SELECT COUNT(DISTINCT agent) FROM (
            SELECT listing_agent AS agent FROM maine_transactions
            WHERE listing_agent IS NOT NULL AND enrichment_status = 'success'
            UNION
            SELECT buyer_agent FROM maine_transactions
            WHERE buyer_agent IS NOT NULL AND enrichment_status = 'success'
        )
    ''').fetchone()[0]

    unique_brokerages = conn.execute('''
        SELECT COUNT(DISTINCT office) FROM (
            SELECT listing_office AS office FROM maine_transactions
            WHERE listing_office IS NOT NULL AND enrichment_status = 'success'
            UNION
            SELECT buyer_office FROM maine_transactions
            WHERE buyer_office IS NOT NULL AND enrichment_status = 'success'
        )
    ''').fetchone()[0]

    return {
        'total': row['total'] or 0,
        'enriched': row['enriched'] or 0,
        'has_listing': row['has_listing'] or 0,
        'has_buyer': row['has_buyer'] or 0,
        'volume': row['volume'] or 0,
        'date_min': row['date_min'] or 'N/A',
        'date_max': row['date_max'] or 'N/A',
        'unique_agents': unique_agents,
        'unique_brokerages': unique_brokerages,
    }


def _combined_section(agents: list[dict], title: str) -> str:
    if not agents:
        return f'<section class="section"><h2>{_e(title)}</h2><p class="empty">No data.</p></section>'

    rows = []
    for i, a in enumerate(agents, 1):
        cls = ' class="rank-1"' if i == 1 else ''
        rows.append(f'''<tr{cls}>
            <td class="num">{i}</td>
            <td class="agent-name">{_e(a["agent_name"])}</td>
            <td>{_e(a.get("office"))}</td>
            <td class="num">{a["total_sides"]}</td>
            <td class="num">{a["listing_sides"]}</td>
            <td class="num">{a["buyer_sides"]}</td>
            <td class="num">{_e(format_currency(a["volume"]))}</td>
            <td class="num">{_e(format_currency(a["avg_price"]))}</td>
            <td>{_e(a.get("towns"))}</td>
            <td class="num">{_e(a.get("most_recent"))}</td>
        </tr>''')

    return f'''<section class="section">
        <h2>{_e(title)}</h2>
        <div class="table-wrap"><table class="sortable">
            <thead><tr>
                <th class="num">#</th>
                <th>Agent</th>
                <th>Office</th>
                <th class="num">Total</th>
                <th class="num">List</th>
                <th class="num">Buy</th>
                <th class="num">Volume</th>
                <th class="num">Avg</th>
                <th>Primary Towns</th>
                <th class="num">Most Recent</th>
            </tr></thead>
            <tbody>{''.join(rows)}</tbody>
        </table></div>
    </section>'''


def _role_section(agents: list[dict], title: str) -> str:
    if not agents:
        return f'<section class="section"><h2>{_e(title)}</h2><p class="empty">No data.</p></section>'

    rows = []
    for i, a in enumerate(agents, 1):
        cls = ' class="rank-1"' if i == 1 else ''
        rows.append(f'''<tr{cls}>
            <td class="num">{i}</td>
            <td class="agent-name">{_e(a["agent_name"])}</td>
            <td>{_e(a.get("office"))}</td>
            <td class="num">{a["sides"]}</td>
            <td class="num">{_e(format_currency(a["volume"]))}</td>
            <td class="num">{_e(format_currency(a["avg_price"]))}</td>
            <td class="num">{a["high_value"]}</td>
            <td>{_e(a.get("towns"))}</td>
            <td class="num">{_e(a.get("most_recent"))}</td>
        </tr>''')

    return f'''<section class="section">
        <h2>{_e(title)}</h2>
        <div class="table-wrap"><table class="sortable">
            <thead><tr>
                <th class="num">#</th>
                <th>Agent</th>
                <th>Office</th>
                <th class="num">Sides</th>
                <th class="num">Volume</th>
                <th class="num">Avg</th>
                <th class="num">$500K+</th>
                <th>Primary Towns</th>
                <th class="num">Most Recent</th>
            </tr></thead>
            <tbody>{''.join(rows)}</tbody>
        </table></div>
    </section>'''


def _brokerage_section(brokerages: list[dict], title: str) -> str:
    if not brokerages:
        return f'<section class="section"><h2>{_e(title)}</h2><p class="empty">No data.</p></section>'

    rows = []
    for i, b in enumerate(brokerages, 1):
        cls = ' class="rank-1"' if i == 1 else ''
        rows.append(f'''<tr{cls}>
            <td class="num">{i}</td>
            <td class="agent-name">{_e(b["brokerage"])}</td>
            <td class="num">{b["sides"]}</td>
            <td class="num">{b["agent_count"]}</td>
            <td class="num">{_e(format_currency(b["volume"]))}</td>
            <td class="num">{_e(format_currency(b["avg_price"]))}</td>
            <td>{_e(b.get("top_agents"))}</td>
        </tr>''')

    return f'''<section class="section">
        <h2>{_e(title)}</h2>
        <p class="sub">Office branches kept separate (branches compete with each other).</p>
        <div class="table-wrap"><table class="sortable">
            <thead><tr>
                <th class="num">#</th>
                <th>Brokerage</th>
                <th class="num">Sides</th>
                <th class="num">Agents</th>
                <th class="num">Volume</th>
                <th class="num">Avg</th>
                <th>Top Agents</th>
            </tr></thead>
            <tbody>{''.join(rows)}</tbody>
        </table></div>
    </section>'''


def generate_maine_dashboard(
    conn: sqlite3.Connection,
    output_path: str | None = None,
) -> str:
    """Generate data/maine_dashboard.html."""
    output_path = output_path or _DEFAULT_DASHBOARD
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    stats = _get_dashboard_stats(conn)
    generated_at = datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')

    sections = [
        _combined_section(
            query_top_combined_agents(conn, limit=50),
            'Top Agents by Total Sides (Listing + Buyer)',
        ),
        _role_section(
            query_top_agents(conn, role='listing', limit=30),
            'Top Listing-Side Agents',
        ),
        _role_section(
            query_top_agents(conn, role='buyer', limit=30),
            'Top Buyer-Side Agents',
        ),
        _brokerage_section(
            query_top_brokerages(conn, limit=25),
            'Top Brokerages by Total Sides',
        ),
    ]

    for town in TOWNS:
        combined = query_top_combined_agents(conn, limit=10, town=town)
        brokers = query_top_brokerages(conn, limit=5, town=town)
        if not combined and not brokers:
            continue
        sections.append(_combined_section(combined, f'Top Agents — {town}'))
        sections.append(_brokerage_section(brokers, f'Top Brokerages — {town}'))

    body = '\n'.join(sections)

    html = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Maine MLS Leaderboard &mdash; Southern Coastal Maine</title>
    <style>{_css()}</style>
</head>
<body>
    <div class="wrap">
        <header class="header">
            <h1>Maine MLS Leaderboard</h1>
            <p class="sub">MaineListings.com (MREIS) &middot; 10 Towns &middot; {_e(generated_at)}</p>
        </header>
        <div class="stats">
            <div class="stat"><div class="label">Closed Transactions</div><div class="value">{stats["enriched"]:,}</div></div>
            <div class="stat"><div class="label">Agents</div><div class="value">{stats["unique_agents"]:,}</div></div>
            <div class="stat"><div class="label">Brokerages</div><div class="value">{stats["unique_brokerages"]:,}</div></div>
            <div class="stat"><div class="label">Volume</div><div class="value">{_e(format_currency(stats["volume"]))}</div></div>
        </div>
        <p class="sub" style="margin: 24px 0 8px; font-size: 0.75rem; color: var(--text-3);">
            Unique to MLS data: every transaction shows both sides, so
            <strong>buyer-side performance</strong> is visible alongside listings.
            Date range: {_e(stats["date_min"])} to {_e(stats["date_max"])}.
        </p>
        <main>{body}</main>
        <footer class="footer">
            Generated {_e(generated_at)} &middot; Data source: MaineListings.com (MREIS MLS) &middot; gw-re-agent-scraper
        </footer>
    </div>
<script>{_sort_js()}</script>
</body>
</html>'''

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)
    logger.info('Maine dashboard written to %s', output_path)
    return output_path
