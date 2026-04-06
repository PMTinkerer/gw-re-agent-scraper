"""Leaderboard report generator.

Produces markdown leaderboards with source- and role-scoped ranking tables.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime

from .database import BROKERAGE_AS_AGENT

logger = logging.getLogger(__name__)

_DEFAULT_OUTPUT = os.path.join(os.path.dirname(__file__), '..', 'data', 'agent_leaderboard.md')


def format_currency(amount: int | float | None) -> str:
    """Format a dollar amount for display."""
    if amount is None or amount == 0:
        return '$0'
    amount = int(amount)
    if amount >= 1_000_000:
        return f'${amount / 1_000_000:.1f}M'
    if amount >= 1_000:
        return f'${amount / 1_000:.0f}K'
    return f'${amount:,}'


def format_currency_full(amount: int | float | None) -> str:
    """Format a dollar amount with full comma-separated display."""
    if amount is None or amount == 0:
        return '$0'
    return f'${int(amount):,}'


def _role_meta(role: str) -> dict[str, str]:
    role = role.lower()
    if role == 'seller':
        return {
            'agent_col': 'listing_agent',
            'office_col': 'listing_office',
            'label': 'Listing',
            'public_label': 'Seller-side',
        }
    if role == 'buyer':
        return {
            'agent_col': 'buyer_agent',
            'office_col': 'buyer_office',
            'label': 'Buyer',
            'public_label': 'Buyer-side',
        }
    raise ValueError(f'Unknown role: {role}')


def _base_filters(
    source: str | None = None,
    since_date: str | None = None,
    town: str | None = None,
) -> tuple[str, list]:
    conditions = ['1=1']
    params: list = []
    if source:
        conditions.append('data_source = ?')
        params.append(source)
    if since_date:
        conditions.append('sale_date >= ?')
        params.append(since_date)
    if town:
        conditions.append('LOWER(city) = LOWER(?)')
        params.append(town)
    return ' AND '.join(conditions), params


def generate_leaderboard(
    conn,
    output_path: str | None = None,
    *,
    source: str | None = None,
    role: str = 'seller',
    title: str | None = None,
) -> str:
    """Generate a markdown leaderboard report and return its output path."""
    output_path = output_path or _DEFAULT_OUTPUT
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    role_info = _role_meta(role)
    now = datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')
    stats = _get_report_stats(conn, source=source, role=role)
    title = title or '# Real Estate Agent Leaderboard -- Southern Coastal Maine'

    source_desc = source.title() if source else ', '.join(stats['sources'])
    lines = [
        title,
        (
            f'_Generated: {now} | Data: {stats["date_min"]} to {stats["date_max"]} | '
            f'Source: {source_desc} | Role: {role_info["public_label"]}_'
        ),
        f'_Total sales analyzed: {stats["total"]:,} | Unique agents: {stats["unique_agents"]:,}_',
        '',
    ]

    side_label = role_info['label']

    lines.append(f'## Top 30 {side_label} Agents by Total Volume')
    lines.append('')
    agents = _query_top_agents(conn, limit=30, source=source, role=role)
    if agents:
        lines.append('| Rank | Agent | Office | Sides | Total Volume | Avg Price | High-Value (>=500K) | Primary Towns | Most Recent |')
        lines.append('|------|-------|--------|------|-------------|-----------|-------------------|--------------|-------------|')
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

    brokerage_heading = 'Top 15 Brokerages by Total Volume'
    if role != 'seller':
        brokerage_heading = f'Top 15 {side_label} Brokerages by Total Volume'
    lines.append(f'## {brokerage_heading}')
    lines.append('')
    brokerages = _query_top_brokerages(conn, limit=15, source=source, role=role)
    if brokerages:
        lines.append('| Rank | Brokerage | Sides | Total Volume | Avg Price | Top Agents |')
        lines.append('|------|-----------|------|-------------|-----------|------------|')
        for i, b in enumerate(brokerages, 1):
            lines.append(
                f'| {i} | {b["office"]} | {b["sides"]} | '
                f'{format_currency(b["volume"])} | {format_currency(b["avg_price"])} | '
                f'{b["top_agents"]} |'
            )
    else:
        lines.append('_No data available._')
    lines.append('')

    lines.append(f'## Top 5 {side_label} Agents by Town')
    lines.append('')
    from .state import TOWNS
    for town in TOWNS:
        town_agents = _query_top_agents_by_town(conn, town, limit=5, source=source, role=role)
        lines.append(f'### {town}')
        lines.append('')
        if town_agents:
            lines.append('| Rank | Agent | Office | Sides | Total Volume |')
            lines.append('|------|-------|--------|------|-------------|')
            for i, a in enumerate(town_agents, 1):
                lines.append(
                    f'| {i} | {a["agent_name"]} | {a["office"] or "N/A"} | '
                    f'{a["sides"]} | {format_currency(a["volume"])} |'
                )
        else:
            lines.append('_No data available for this town._')
        lines.append('')

    lines.append('## Data Summary')
    lines.append('')
    lines.append(f'- **Total transactions:** {stats["total"]:,}')
    lines.append(f'- **With {role_info["label"].lower()} agent:** {stats["with_agent"]:,}')
    lines.append(f'- **Date range:** {stats["date_min"]} to {stats["date_max"]}')
    lines.append('- **Sources:**')
    for source_name, count in stats['source_breakdown'].items():
        lines.append(f'  - {source_name.title()}: {count:,} observations')
    lines.append('- **Per-town breakdown:**')
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


def _get_report_stats(conn, *, source: str | None = None, role: str = 'seller') -> dict:
    role_info = _role_meta(role)
    agent_col = role_info['agent_col']
    where_sql, params = _base_filters(source=source)

    total = conn.execute(
        f'SELECT COUNT(*) FROM transactions WHERE {where_sql}',
        params,
    ).fetchone()[0]
    with_agent = conn.execute(
        f'SELECT COUNT(*) FROM transactions WHERE {where_sql} AND {agent_col} IS NOT NULL',
        params,
    ).fetchone()[0]
    unique_agents = conn.execute(
        f'SELECT COUNT(DISTINCT {agent_col}) FROM transactions WHERE {where_sql} AND {agent_col} IS NOT NULL',
        params,
    ).fetchone()[0]
    date_range = conn.execute(
        f'SELECT MIN(sale_date), MAX(sale_date) FROM transactions WHERE {where_sql} AND sale_date IS NOT NULL',
        params,
    ).fetchone()
    sources = conn.execute(
        f'SELECT data_source, COUNT(*) FROM transactions WHERE {where_sql} GROUP BY data_source',
        params,
    ).fetchall()
    towns = conn.execute(f'''
        SELECT city, COUNT(*) FROM transactions
        WHERE {where_sql} AND city IS NOT NULL
        GROUP BY city ORDER BY city
    ''', params).fetchall()

    return {
        'total': total,
        'with_agent': with_agent,
        'unique_agents': unique_agents,
        'date_min': date_range[0] if date_range and date_range[0] else 'N/A',
        'date_max': date_range[1] if date_range and date_range[1] else 'N/A',
        'sources': [r[0] for r in sources] if sources else ['None'],
        'source_breakdown': {r[0]: r[1] for r in sources},
        'town_breakdown': {r[0]: r[1] for r in towns},
    }


def _query_top_agents(
    conn,
    limit: int = 30,
    since_date: str | None = None,
    *,
    source: str | None = None,
    role: str = 'seller',
) -> list[dict]:
    role_info = _role_meta(role)
    agent_col = role_info['agent_col']
    office_col = role_info['office_col']
    brokerage_placeholders = ', '.join('?' for _ in BROKERAGE_AS_AGENT)
    brokerage_exclusion = ''
    if BROKERAGE_AS_AGENT:
        brokerage_exclusion = f'AND LOWER(t.{agent_col}) NOT IN ({brokerage_placeholders})'

    where_sql, params = _base_filters(source=source, since_date=since_date)
    params = params + list(BROKERAGE_AS_AGENT) + [limit]
    rows = conn.execute(f'''
        WITH filtered AS (
            SELECT * FROM transactions WHERE {where_sql}
        )
        SELECT
            t.{agent_col} as agent_name,
            (
                SELECT t2.{office_col} FROM filtered t2
                WHERE t2.{agent_col} = t.{agent_col}
                    AND t2.{office_col} IS NOT NULL
                GROUP BY t2.{office_col}
                ORDER BY COUNT(*) DESC LIMIT 1
            ) as primary_office,
            COUNT(*) as sides,
            SUM(COALESCE(t.sale_price, t.list_price, 0)) as volume,
            AVG(COALESCE(t.sale_price, t.list_price, 0)) as avg_price,
            SUM(CASE WHEN COALESCE(t.sale_price, t.list_price, 0) >= 500000 THEN 1 ELSE 0 END) as high_value,
            (
                SELECT GROUP_CONCAT(city, ', ') FROM (
                    SELECT city, COUNT(*) as cnt FROM filtered t3
                    WHERE t3.{agent_col} = t.{agent_col} AND t3.city IS NOT NULL
                    GROUP BY city ORDER BY cnt DESC LIMIT 3
                )
            ) as primary_towns,
            MAX(t.sale_date) as most_recent
        FROM filtered t
        WHERE t.{agent_col} IS NOT NULL
          AND (t.{office_col} IS NULL OR LOWER(t.{agent_col}) != LOWER(t.{office_col}))
          {brokerage_exclusion}
        GROUP BY t.{agent_col}
        ORDER BY volume DESC
        LIMIT ?
    ''', params).fetchall()

    return [
        {
            'agent_name': r['agent_name'],
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


def _query_top_brokerages(
    conn,
    limit: int = 20,
    since_date: str | None = None,
    *,
    source: str | None = None,
    role: str = 'seller',
) -> list[dict]:
    role_info = _role_meta(role)
    agent_col = role_info['agent_col']
    office_col = role_info['office_col']
    where_sql, params = _base_filters(source=source, since_date=since_date)
    rows = conn.execute(f'''
        WITH filtered AS (
            SELECT * FROM transactions WHERE {where_sql}
        )
        SELECT
            t.{office_col} as office_name,
            COUNT(*) as sides,
            SUM(COALESCE(t.sale_price, t.list_price, 0)) as volume,
            AVG(COALESCE(t.sale_price, t.list_price, 0)) as avg_price,
            (
                SELECT GROUP_CONCAT(agent_name, ', ') FROM (
                    SELECT t2.{agent_col} as agent_name, COUNT(*) as cnt
                    FROM filtered t2
                    WHERE t2.{office_col} = t.{office_col}
                        AND t2.{agent_col} IS NOT NULL
                    GROUP BY t2.{agent_col}
                    ORDER BY cnt DESC LIMIT 3
                )
            ) as top_agents,
            (
                SELECT GROUP_CONCAT(city, ', ') FROM (
                    SELECT city, COUNT(*) as cnt FROM filtered t3
                    WHERE t3.{office_col} = t.{office_col} AND t3.city IS NOT NULL
                    GROUP BY city ORDER BY cnt DESC LIMIT 3
                )
            ) as primary_towns
        FROM filtered t
        WHERE t.{office_col} IS NOT NULL
        GROUP BY t.{office_col}
        ORDER BY volume DESC
        LIMIT ?
    ''', params + [limit]).fetchall()

    return [
        {
            'office': r['office_name'],
            'sides': r['sides'],
            'volume': r['volume'],
            'avg_price': int(r['avg_price']) if r['avg_price'] else 0,
            'top_agents': r['top_agents'] or 'N/A',
            'towns': r['primary_towns'] or 'N/A',
        }
        for r in rows
    ]


def _query_top_agents_by_town(
    conn,
    town: str,
    limit: int = 5,
    *,
    source: str | None = None,
    role: str = 'seller',
) -> list[dict]:
    role_info = _role_meta(role)
    agent_col = role_info['agent_col']
    office_col = role_info['office_col']
    brokerage_placeholders = ', '.join('?' for _ in BROKERAGE_AS_AGENT)
    brokerage_exclusion = ''
    if BROKERAGE_AS_AGENT:
        brokerage_exclusion = f'AND LOWER(t.{agent_col}) NOT IN ({brokerage_placeholders})'

    where_sql, params = _base_filters(source=source, town=town)
    rows = conn.execute(f'''
        WITH filtered AS (
            SELECT * FROM transactions WHERE {where_sql}
        )
        SELECT
            t.{agent_col} as agent_name,
            (
                SELECT t2.{office_col} FROM filtered t2
                WHERE t2.{agent_col} = t.{agent_col}
                    AND t2.{office_col} IS NOT NULL
                GROUP BY t2.{office_col}
                ORDER BY COUNT(*) DESC LIMIT 1
            ) as primary_office,
            COUNT(*) as sides,
            SUM(COALESCE(t.sale_price, t.list_price, 0)) as volume,
            AVG(COALESCE(t.sale_price, t.list_price, 0)) as avg_price
        FROM filtered t
        WHERE t.{agent_col} IS NOT NULL
          AND (t.{office_col} IS NULL OR LOWER(t.{agent_col}) != LOWER(t.{office_col}))
          {brokerage_exclusion}
        GROUP BY t.{agent_col}
        ORDER BY volume DESC
        LIMIT ?
    ''', params + list(BROKERAGE_AS_AGENT) + [limit]).fetchall()

    return [
        {
            'agent_name': r['agent_name'],
            'office': r['primary_office'],
            'sides': r['sides'],
            'volume': r['volume'],
            'avg_price': int(r['avg_price']) if r['avg_price'] else 0,
        }
        for r in rows
    ]


def query_top_agents(
    conn,
    limit: int = 30,
    since_date: str | None = None,
    *,
    source: str | None = None,
    role: str = 'seller',
) -> list[dict]:
    return _query_top_agents(conn, limit, since_date=since_date, source=source, role=role)


def query_top_brokerages(
    conn,
    limit: int = 20,
    since_date: str | None = None,
    *,
    source: str | None = None,
    role: str = 'seller',
) -> list[dict]:
    return _query_top_brokerages(conn, limit, since_date=since_date, source=source, role=role)


def query_top_agents_by_town(
    conn,
    town: str,
    limit: int = 5,
    *,
    source: str | None = None,
    role: str = 'seller',
) -> list[dict]:
    return _query_top_agents_by_town(conn, town, limit, source=source, role=role)


def get_report_stats(conn, *, source: str | None = None, role: str = 'seller') -> dict:
    return _get_report_stats(conn, source=source, role=role)
