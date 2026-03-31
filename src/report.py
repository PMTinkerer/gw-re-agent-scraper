"""Leaderboard report generator.

Produces data/agent_leaderboard.md with ranked tables of top listing agents,
brokerages, per-town breakdowns, and data summary.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime

from .database import BROKERAGE_AS_AGENT

logger = logging.getLogger(__name__)

_DEFAULT_OUTPUT = os.path.join(os.path.dirname(__file__), '..', 'data', 'agent_leaderboard.md')


def format_currency(amount: int | float | None) -> str:
    """Format a dollar amount for display.

    >= 1M  -> "$1.2M"
    >= 1K  -> "$339K"
    < 1K   -> "$123"
    """
    if amount is None or amount == 0:
        return '$0'
    amount = int(amount)
    if amount >= 1_000_000:
        return f'${amount / 1_000_000:.1f}M'
    elif amount >= 1_000:
        return f'${amount / 1_000:.0f}K'
    else:
        return f'${amount:,}'


def format_currency_full(amount: int | float | None) -> str:
    """Format a dollar amount with full comma-separated display."""
    if amount is None or amount == 0:
        return '$0'
    return f'${int(amount):,}'


def generate_leaderboard(conn, output_path: str | None = None) -> str:
    """Generate the agent leaderboard markdown report.

    Returns the output file path.
    """
    output_path = output_path or _DEFAULT_OUTPUT
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    now = datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')

    # Data summary stats
    stats = _get_report_stats(conn)

    lines = []
    lines.append('# Real Estate Agent Leaderboard -- Southern Coastal Maine')
    lines.append(f'_Generated: {now} | Data: {stats["date_min"]} to {stats["date_max"]} | '
                 f'Sources: {", ".join(stats["sources"])}_')
    lines.append(f'_Total sales analyzed: {stats["total"]:,} | '
                 f'Unique listing agents: {stats["unique_agents"]:,}_')
    lines.append('')

    # Section 1: Top 30 Listing Agents
    lines.append('## Top 30 Listing Agents by Total Volume')
    lines.append('')
    agents = _query_top_agents(conn, limit=30)
    if agents:
        lines.append('| Rank | Agent | Office | Listing Sides | Total Volume | Avg Price | High-Value (>=500K) | Primary Towns | Most Recent |')
        lines.append('|------|-------|--------|--------------|-------------|-----------|-------------------|--------------|-------------|')
        for i, a in enumerate(agents, 1):
            lines.append(
                f'| {i} | {a["agent_name"]} | {a["office"] or "N/A"} | '
                f'{a["sides"]} | {format_currency(a["volume"])} | '
                f'{format_currency(a["avg_price"])} | {a["high_value"]} | '
                f'{a["towns"]} | {a["most_recent"] or "N/A"} |'
            )
    else:
        lines.append('_No data available._')
    lines.append('')

    # Section 2: Top 15 Brokerages
    lines.append('## Top 15 Brokerages by Total Volume')
    lines.append('')
    brokerages = _query_top_brokerages(conn, limit=15)
    if brokerages:
        lines.append('| Rank | Brokerage | Listing Sides | Total Volume | Avg Price | Top Agents |')
        lines.append('|------|-----------|--------------|-------------|-----------|------------|')
        for i, b in enumerate(brokerages, 1):
            lines.append(
                f'| {i} | {b["office"]} | {b["sides"]} | '
                f'{format_currency(b["volume"])} | {format_currency(b["avg_price"])} | '
                f'{b["top_agents"]} |'
            )
    else:
        lines.append('_No data available._')
    lines.append('')

    # Section 3: Top 5 per town
    lines.append('## Top 5 Listing Agents by Town')
    lines.append('')
    from .state import TOWNS
    for town in TOWNS:
        town_agents = _query_top_agents_by_town(conn, town, limit=5)
        lines.append(f'### {town}')
        lines.append('')
        if town_agents:
            lines.append('| Rank | Agent | Office | Listing Sides | Total Volume |')
            lines.append('|------|-------|--------|--------------|-------------|')
            for i, a in enumerate(town_agents, 1):
                lines.append(
                    f'| {i} | {a["agent_name"]} | {a["office"] or "N/A"} | '
                    f'{a["sides"]} | {format_currency(a["volume"])} |'
                )
        else:
            lines.append('_No data available for this town._')
        lines.append('')

    # Section 4: Data Summary
    lines.append('## Data Summary')
    lines.append('')
    lines.append(f'- **Total transactions:** {stats["total"]:,}')
    lines.append(f'- **With listing agent:** {stats["with_agent"]:,}')
    lines.append(f'- **Date range:** {stats["date_min"]} to {stats["date_max"]}')
    lines.append(f'- **Sources:**')
    for source, count in stats['source_breakdown'].items():
        lines.append(f'  - {source.title()}: {count:,} properties')
    lines.append(f'- **Per-town breakdown:**')
    for town, count in stats['town_breakdown'].items():
        flag = ' (thin data)' if count < 50 else ''
        lines.append(f'  - {town}: {count:,} sales{flag}')
    lines.append(f'- **Report generated:** {now}')
    lines.append('')

    content = '\n'.join(lines)

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(content)

    logger.info('Leaderboard written to %s (%d agents, %d transactions)',
                output_path, len(agents) if agents else 0, stats['total'])
    return output_path


# --- Query Helpers ---

def _get_report_stats(conn) -> dict:
    total = conn.execute('SELECT COUNT(*) FROM transactions').fetchone()[0]
    with_agent = conn.execute(
        'SELECT COUNT(*) FROM transactions WHERE listing_agent IS NOT NULL'
    ).fetchone()[0]
    unique_agents = conn.execute(
        'SELECT COUNT(DISTINCT listing_agent) FROM transactions WHERE listing_agent IS NOT NULL'
    ).fetchone()[0]

    date_range = conn.execute(
        'SELECT MIN(sale_date), MAX(sale_date) FROM transactions WHERE sale_date IS NOT NULL'
    ).fetchone()

    sources = conn.execute(
        'SELECT data_source, COUNT(*) FROM transactions GROUP BY data_source'
    ).fetchall()

    towns = conn.execute('''
        SELECT city, COUNT(*) FROM transactions
        WHERE city IS NOT NULL
        GROUP BY city ORDER BY city
    ''').fetchall()

    return {
        'total': total,
        'with_agent': with_agent,
        'unique_agents': unique_agents,
        'date_min': date_range[0] if date_range else 'N/A',
        'date_max': date_range[1] if date_range else 'N/A',
        'sources': [r[0] for r in sources] if sources else ['None'],
        'source_breakdown': {r[0]: r[1] for r in sources},
        'town_breakdown': {r[0]: r[1] for r in towns},
    }


def _query_top_agents(conn, limit: int = 30, since_date: str | None = None) -> list[dict]:
    date_filter = 'AND sale_date >= ?' if since_date else ''
    # Build exclusion for known brokerage-as-agent names
    brokerage_placeholders = ', '.join('?' for _ in BROKERAGE_AS_AGENT)
    brokerage_exclusion = f'AND LOWER(listing_agent) NOT IN ({brokerage_placeholders})' if BROKERAGE_AS_AGENT else ''
    params = list(BROKERAGE_AS_AGENT)
    if since_date:
        params.append(since_date)
    params.append(limit)
    rows = conn.execute(f'''
        SELECT
            listing_agent,
            (
                SELECT listing_office FROM transactions t2
                WHERE t2.listing_agent = t.listing_agent
                    AND t2.listing_office IS NOT NULL
                GROUP BY listing_office ORDER BY COUNT(*) DESC LIMIT 1
            ) as primary_office,
            COUNT(*) as sides,
            SUM(COALESCE(sale_price, list_price, 0)) as volume,
            AVG(COALESCE(sale_price, list_price, 0)) as avg_price,
            SUM(CASE WHEN COALESCE(sale_price, list_price, 0) >= 500000 THEN 1 ELSE 0 END) as high_value,
            (
                SELECT GROUP_CONCAT(city, ', ') FROM (
                    SELECT city, COUNT(*) as cnt FROM transactions t3
                    WHERE t3.listing_agent = t.listing_agent AND t3.city IS NOT NULL
                    GROUP BY city ORDER BY cnt DESC LIMIT 3
                )
            ) as primary_towns,
            MAX(sale_date) as most_recent
        FROM transactions t
        WHERE listing_agent IS NOT NULL
          AND (listing_office IS NULL OR LOWER(listing_agent) != LOWER(listing_office))
          {brokerage_exclusion}
          {date_filter}
        GROUP BY listing_agent
        ORDER BY volume DESC
        LIMIT ?
    ''', params).fetchall()

    return [
        {
            'agent_name': r['listing_agent'],
            'office': r['primary_office'],
            'sides': r['sides'],
            'volume': r['volume'],
            'avg_price': int(r['avg_price']) if r['avg_price'] else 0,
            'high_value': r['high_value'],
            'towns': r['primary_towns'] or 'N/A',
            'most_recent': r['most_recent'],
        }
        for r in rows
    ]


def _query_top_brokerages(conn, limit: int = 20, since_date: str | None = None) -> list[dict]:
    date_filter = 'AND sale_date >= ?' if since_date else ''
    params = (since_date, limit) if since_date else (limit,)
    rows = conn.execute(f'''
        SELECT
            listing_office,
            COUNT(*) as sides,
            SUM(COALESCE(sale_price, list_price, 0)) as volume,
            AVG(COALESCE(sale_price, list_price, 0)) as avg_price,
            (
                SELECT GROUP_CONCAT(agent, ', ') FROM (
                    SELECT listing_agent as agent, COUNT(*) as cnt
                    FROM transactions t2
                    WHERE t2.listing_office = t.listing_office
                        AND t2.listing_agent IS NOT NULL
                    GROUP BY listing_agent ORDER BY cnt DESC LIMIT 3
                )
            ) as top_agents,
            (
                SELECT GROUP_CONCAT(city, ', ') FROM (
                    SELECT city, COUNT(*) as cnt FROM transactions t3
                    WHERE t3.listing_office = t.listing_office AND t3.city IS NOT NULL
                    GROUP BY city ORDER BY cnt DESC LIMIT 3
                )
            ) as primary_towns
        FROM transactions t
        WHERE listing_office IS NOT NULL {date_filter}
        GROUP BY listing_office
        ORDER BY volume DESC
        LIMIT ?
    ''', params).fetchall()

    return [
        {
            'office': r['listing_office'],
            'sides': r['sides'],
            'volume': r['volume'],
            'avg_price': int(r['avg_price']) if r['avg_price'] else 0,
            'top_agents': r['top_agents'] or 'N/A',
            'towns': r['primary_towns'] or 'N/A',
        }
        for r in rows
    ]


def _query_top_agents_by_town(conn, town: str, limit: int = 5) -> list[dict]:
    brokerage_placeholders = ', '.join('?' for _ in BROKERAGE_AS_AGENT)
    brokerage_exclusion = f'AND LOWER(listing_agent) NOT IN ({brokerage_placeholders})' if BROKERAGE_AS_AGENT else ''
    params = list(BROKERAGE_AS_AGENT) + [town, limit]
    rows = conn.execute(f'''
        SELECT
            listing_agent,
            (
                SELECT listing_office FROM transactions t2
                WHERE t2.listing_agent = t.listing_agent
                    AND t2.listing_office IS NOT NULL
                GROUP BY listing_office ORDER BY COUNT(*) DESC LIMIT 1
            ) as primary_office,
            COUNT(*) as sides,
            SUM(COALESCE(sale_price, list_price, 0)) as volume
        FROM transactions t
        WHERE listing_agent IS NOT NULL
          AND (listing_office IS NULL OR LOWER(listing_agent) != LOWER(listing_office))
          {brokerage_exclusion}
          AND LOWER(city) = LOWER(?)
        GROUP BY listing_agent
        ORDER BY volume DESC
        LIMIT ?
    ''', params).fetchall()

    return [
        {
            'agent_name': r['listing_agent'],
            'office': r['primary_office'],
            'sides': r['sides'],
            'volume': r['volume'],
        }
        for r in rows
    ]


# --- Public API (used by dashboard.py) ---

def query_top_agents(conn, limit: int = 30, since_date: str | None = None) -> list[dict]:
    return _query_top_agents(conn, limit, since_date=since_date)

def query_top_brokerages(conn, limit: int = 20, since_date: str | None = None) -> list[dict]:
    return _query_top_brokerages(conn, limit, since_date=since_date)

def query_top_agents_by_town(conn, town: str, limit: int = 5) -> list[dict]:
    return _query_top_agents_by_town(conn, town, limit)

def get_report_stats(conn) -> dict:
    return _get_report_stats(conn)
