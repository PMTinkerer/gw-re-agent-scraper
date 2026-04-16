"""Maine Listings (MREIS MLS) leaderboard report generator.

Unlike Redfin, Maine Listings captures both listing AND buyer agents on every
closed transaction. This module produces four leaderboards:

  1. Listing-side agents
  2. Buyer-side agents
  3. Combined (total sides) agents
  4. Brokerages (listing + buyer office rollups, kept as separate branches)

Plus per-town breakdowns for each.
"""
from __future__ import annotations

import logging
import os
import sqlite3
from datetime import datetime

from .state import TOWNS

logger = logging.getLogger(__name__)

_DEFAULT_OUTPUT = os.path.join(
    os.path.dirname(__file__), '..', 'data', 'maine_leaderboard.md',
)

_SUCCESS = "enrichment_status = 'success'"


def format_currency(amount: int | float | None) -> str:
    if amount is None or amount == 0:
        return '$0'
    amount = int(amount)
    if amount >= 1_000_000:
        return f'${amount / 1_000_000:.1f}M'
    if amount >= 1_000:
        return f'${amount / 1_000:.0f}K'
    return f'${amount:,}'


def _role_cols(role: str) -> tuple[str, str]:
    if role == 'listing':
        return 'listing_agent', 'listing_office'
    if role == 'buyer':
        return 'buyer_agent', 'buyer_office'
    raise ValueError(f'Unknown role: {role}')


def query_top_agents(
    conn: sqlite3.Connection,
    *,
    role: str,
    limit: int = 30,
    town: str | None = None,
) -> list[dict]:
    """Top agents for a role (listing or buyer)."""
    agent_col, office_col = _role_cols(role)
    params: list = []
    town_sql = ''
    if town:
        town_sql = 'AND LOWER(city) = LOWER(?)'
        params.append(town)
    params.append(limit)

    rows = conn.execute(f'''
        SELECT
            t.{agent_col} AS agent_name,
            (
                SELECT t2.{office_col}
                FROM maine_transactions t2
                WHERE t2.{agent_col} = t.{agent_col}
                  AND t2.{office_col} IS NOT NULL
                  AND {_SUCCESS}
                GROUP BY t2.{office_col}
                ORDER BY COUNT(*) DESC LIMIT 1
            ) AS office,
            COUNT(*) AS sides,
            SUM(COALESCE(t.sale_price, t.list_price, 0)) AS volume,
            AVG(COALESCE(t.sale_price, t.list_price, 0)) AS avg_price,
            SUM(CASE WHEN COALESCE(t.sale_price, t.list_price, 0) >= 500000
                     THEN 1 ELSE 0 END) AS high_value,
            MAX(t.close_date) AS most_recent,
            (
                SELECT GROUP_CONCAT(city, ', ') FROM (
                    SELECT city, COUNT(*) AS cnt
                    FROM maine_transactions t3
                    WHERE t3.{agent_col} = t.{agent_col}
                      AND t3.city IS NOT NULL
                      AND {_SUCCESS}
                    GROUP BY city ORDER BY cnt DESC LIMIT 3
                )
            ) AS towns
        FROM maine_transactions t
        WHERE t.{agent_col} IS NOT NULL
          AND TRIM(t.{agent_col}) != ''
          AND {_SUCCESS}
          {town_sql}
        GROUP BY t.{agent_col}
        ORDER BY sides DESC, volume DESC
        LIMIT ?
    ''', params).fetchall()

    return [dict(r) for r in rows]


def query_top_combined_agents(
    conn: sqlite3.Connection,
    *,
    limit: int = 30,
    town: str | None = None,
) -> list[dict]:
    """Agents ranked by total sides (listing + buyer combined).

    An agent gets credit for each side they represented. If John Smith was
    the listing agent on 10 sales and buyer agent on 5, he has 15 total sides.
    """
    town_params: list = []
    town_sql = ''
    if town:
        town_sql = 'AND LOWER(city) = LOWER(?)'
        town_params = [town]

    params = town_params + town_params + [limit]

    rows = conn.execute(f'''
        WITH sides AS (
            SELECT listing_agent AS agent, listing_office AS office,
                   sale_price, city, close_date, 'listing' AS role
            FROM maine_transactions
            WHERE listing_agent IS NOT NULL
              AND TRIM(listing_agent) != ''
              AND {_SUCCESS}
              {town_sql}
            UNION ALL
            SELECT buyer_agent AS agent, buyer_office AS office,
                   sale_price, city, close_date, 'buyer' AS role
            FROM maine_transactions
            WHERE buyer_agent IS NOT NULL
              AND TRIM(buyer_agent) != ''
              AND {_SUCCESS}
              {town_sql}
        )
        SELECT
            agent AS agent_name,
            (SELECT office FROM sides s2
             WHERE s2.agent = s.agent AND s2.office IS NOT NULL
             GROUP BY office ORDER BY COUNT(*) DESC LIMIT 1) AS office,
            COUNT(*) AS total_sides,
            SUM(CASE WHEN role = 'listing' THEN 1 ELSE 0 END) AS listing_sides,
            SUM(CASE WHEN role = 'buyer' THEN 1 ELSE 0 END) AS buyer_sides,
            SUM(COALESCE(sale_price, 0)) AS volume,
            AVG(COALESCE(sale_price, 0)) AS avg_price,
            MAX(close_date) AS most_recent,
            (
                SELECT GROUP_CONCAT(city, ', ') FROM (
                    SELECT city, COUNT(*) AS cnt
                    FROM sides s3 WHERE s3.agent = s.agent
                      AND s3.city IS NOT NULL
                    GROUP BY city ORDER BY cnt DESC LIMIT 3
                )
            ) AS towns
        FROM sides s
        GROUP BY agent
        ORDER BY total_sides DESC, volume DESC
        LIMIT ?
    ''', params).fetchall()

    return [dict(r) for r in rows]


def query_top_brokerages(
    conn: sqlite3.Connection,
    *,
    limit: int = 20,
    town: str | None = None,
) -> list[dict]:
    """Top brokerages by combined sides (listing_office + buyer_office).

    Office branches are kept separate — "Coldwell Banker Yorke Realty" is a
    distinct competitor from "Coldwell Banker Realty".
    """
    town_params: list = []
    town_sql = ''
    if town:
        town_sql = 'AND LOWER(city) = LOWER(?)'
        town_params = [town]

    params = town_params + town_params + [limit]

    rows = conn.execute(f'''
        WITH sides AS (
            SELECT listing_office AS office, listing_agent AS agent,
                   sale_price, city
            FROM maine_transactions
            WHERE listing_office IS NOT NULL
              AND TRIM(listing_office) != ''
              AND {_SUCCESS}
              {town_sql}
            UNION ALL
            SELECT buyer_office AS office, buyer_agent AS agent,
                   sale_price, city
            FROM maine_transactions
            WHERE buyer_office IS NOT NULL
              AND TRIM(buyer_office) != ''
              AND {_SUCCESS}
              {town_sql}
        )
        SELECT
            office AS brokerage,
            COUNT(*) AS sides,
            COUNT(DISTINCT agent) AS agent_count,
            SUM(COALESCE(sale_price, 0)) AS volume,
            AVG(COALESCE(sale_price, 0)) AS avg_price,
            (
                SELECT GROUP_CONCAT(agent, ', ') FROM (
                    SELECT agent, COUNT(*) AS cnt
                    FROM sides s2
                    WHERE s2.office = s.office AND s2.agent IS NOT NULL
                    GROUP BY agent ORDER BY cnt DESC LIMIT 3
                )
            ) AS top_agents
        FROM sides s
        GROUP BY office
        ORDER BY sides DESC, volume DESC
        LIMIT ?
    ''', params).fetchall()

    return [dict(r) for r in rows]


def _get_stats(conn: sqlite3.Connection) -> dict:
    row = conn.execute('''
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN enrichment_status = 'success' THEN 1 ELSE 0 END) AS enriched,
            SUM(CASE WHEN listing_agent IS NOT NULL THEN 1 ELSE 0 END) AS has_listing,
            SUM(CASE WHEN buyer_agent IS NOT NULL THEN 1 ELSE 0 END) AS has_buyer,
            MIN(close_date) AS date_min,
            MAX(close_date) AS date_max
        FROM maine_transactions
    ''').fetchone()

    town_rows = conn.execute('''
        SELECT city, COUNT(*) AS n
        FROM maine_transactions
        WHERE enrichment_status = 'success' AND city IS NOT NULL
        GROUP BY city ORDER BY n DESC
    ''').fetchall()

    return {
        'total': row['total'] or 0,
        'enriched': row['enriched'] or 0,
        'has_listing': row['has_listing'] or 0,
        'has_buyer': row['has_buyer'] or 0,
        'date_min': row['date_min'] or 'N/A',
        'date_max': row['date_max'] or 'N/A',
        'towns': {r['city']: r['n'] for r in town_rows},
    }


def generate_leaderboard(
    conn: sqlite3.Connection,
    output_path: str | None = None,
) -> str:
    """Generate the Maine Listings markdown leaderboard."""
    output_path = output_path or _DEFAULT_OUTPUT
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    now = datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')
    stats = _get_stats(conn)

    lines = [
        '# Maine MLS Agent Leaderboard -- Southern Coastal Maine',
        (
            f'_Generated: {now} | Source: MaineListings.com (MREIS MLS) | '
            f'Date range: {stats["date_min"]} to {stats["date_max"]}_'
        ),
        (
            f'_Enriched: {stats["enriched"]:,} / {stats["total"]:,} closed '
            f'transactions | Listing agent: {stats["has_listing"]:,} | '
            f'Buyer agent: {stats["has_buyer"]:,}_'
        ),
        '',
        ('> Unique to MLS data: every transaction includes both listing '
         'and buyer agent, so buyer-side performance is visible.'),
        '',
    ]

    _append_combined_section(conn, lines)
    _append_role_section(conn, lines, 'listing', 'Top 30 Listing-Side Agents')
    _append_role_section(conn, lines, 'buyer', 'Top 30 Buyer-Side Agents')
    _append_brokerage_section(conn, lines)
    _append_per_town_section(conn, lines)
    _append_summary(lines, stats)

    content = '\n'.join(lines)
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(content)
    logger.info('Maine leaderboard written to %s', output_path)
    return output_path


def _append_combined_section(conn: sqlite3.Connection, lines: list[str]) -> None:
    lines.append('## Top 30 Agents by Total Sides (Listing + Buyer)')
    lines.append('')
    agents = query_top_combined_agents(conn, limit=30)
    if not agents:
        lines.append('_No enriched data yet._')
        lines.append('')
        return
    lines.append('| Rank | Agent | Office | Total | List | Buy | Volume | Avg | Primary Towns | Most Recent |')
    lines.append('|------|-------|--------|-------|------|-----|--------|-----|---------------|-------------|')
    for i, a in enumerate(agents, 1):
        lines.append(
            f'| {i} | {a["agent_name"]} | {a["office"] or "N/A"} | '
            f'{a["total_sides"]} | {a["listing_sides"]} | {a["buyer_sides"]} | '
            f'{format_currency(a["volume"])} | {format_currency(a["avg_price"])} | '
            f'{a["towns"] or "N/A"} | {a["most_recent"] or "N/A"} |'
        )
    lines.append('')


def _append_role_section(
    conn: sqlite3.Connection, lines: list[str], role: str, heading: str,
) -> None:
    lines.append(f'## {heading} by Sides')
    lines.append('')
    agents = query_top_agents(conn, role=role, limit=30)
    if not agents:
        lines.append('_No enriched data yet._')
        lines.append('')
        return
    lines.append('| Rank | Agent | Office | Sides | Volume | Avg | $500K+ | Primary Towns | Most Recent |')
    lines.append('|------|-------|--------|-------|--------|-----|--------|---------------|-------------|')
    for i, a in enumerate(agents, 1):
        lines.append(
            f'| {i} | {a["agent_name"]} | {a["office"] or "N/A"} | '
            f'{a["sides"]} | {format_currency(a["volume"])} | '
            f'{format_currency(a["avg_price"])} | {a["high_value"]} | '
            f'{a["towns"] or "N/A"} | {a["most_recent"] or "N/A"} |'
        )
    lines.append('')


def _append_brokerage_section(conn: sqlite3.Connection, lines: list[str]) -> None:
    lines.append('## Top 20 Brokerages by Total Sides')
    lines.append('')
    lines.append('_Office branches kept separate (branches compete with each other)._')
    lines.append('')
    brokerages = query_top_brokerages(conn, limit=20)
    if not brokerages:
        lines.append('_No enriched data yet._')
        lines.append('')
        return
    lines.append('| Rank | Brokerage | Sides | Agents | Volume | Avg | Top Agents |')
    lines.append('|------|-----------|-------|--------|--------|-----|------------|')
    for i, b in enumerate(brokerages, 1):
        lines.append(
            f'| {i} | {b["brokerage"]} | {b["sides"]} | {b["agent_count"]} | '
            f'{format_currency(b["volume"])} | {format_currency(b["avg_price"])} | '
            f'{b["top_agents"] or "N/A"} |'
        )
    lines.append('')


def _append_per_town_section(conn: sqlite3.Connection, lines: list[str]) -> None:
    lines.append('## Top 5 Agents by Town (Combined Sides)')
    lines.append('')
    for town in TOWNS:
        town_agents = query_top_combined_agents(conn, limit=5, town=town)
        lines.append(f'### {town}')
        lines.append('')
        if not town_agents:
            lines.append('_No data for this town._')
            lines.append('')
            continue
        lines.append('| Rank | Agent | Office | Total | List | Buy | Volume |')
        lines.append('|------|-------|--------|-------|------|-----|--------|')
        for i, a in enumerate(town_agents, 1):
            lines.append(
                f'| {i} | {a["agent_name"]} | {a["office"] or "N/A"} | '
                f'{a["total_sides"]} | {a["listing_sides"]} | {a["buyer_sides"]} | '
                f'{format_currency(a["volume"])} |'
            )
        lines.append('')


def _append_summary(lines: list[str], stats: dict) -> None:
    lines.append('## Data Summary')
    lines.append('')
    lines.append(f'- **Total closed transactions discovered:** {stats["total"]:,}')
    lines.append(f'- **Successfully enriched:** {stats["enriched"]:,}')
    lines.append(f'- **With listing agent:** {stats["has_listing"]:,}')
    lines.append(f'- **With buyer agent:** {stats["has_buyer"]:,}')
    lines.append(f'- **Date range:** {stats["date_min"]} to {stats["date_max"]}')
    lines.append('- **Per-town breakdown:**')
    for town, count in stats['towns'].items():
        flag = ' (thin data)' if count < 50 else ''
        lines.append(f'  - {town}: {count:,} sales{flag}')
    lines.append('')


def build_maine_search_index(conn: sqlite3.Connection) -> list[dict]:
    """Build search records for integration into index_page.py.

    Returns one record per agent (across both listing + buyer roles)
    with enough metadata for the client-side agent search UI.
    """
    if conn is None:
        return []

    rows = conn.execute(f'''
        WITH sides AS (
            SELECT listing_agent AS agent, listing_office AS office,
                   sale_price, city, close_date, 'listing' AS role
            FROM maine_transactions
            WHERE listing_agent IS NOT NULL
              AND TRIM(listing_agent) != ''
              AND {_SUCCESS}
            UNION ALL
            SELECT buyer_agent AS agent, buyer_office AS office,
                   sale_price, city, close_date, 'buyer' AS role
            FROM maine_transactions
            WHERE buyer_agent IS NOT NULL
              AND TRIM(buyer_agent) != ''
              AND {_SUCCESS}
        )
        SELECT
            agent AS name,
            (SELECT office FROM sides s2
             WHERE s2.agent = s.agent AND s2.office IS NOT NULL
             GROUP BY office ORDER BY COUNT(*) DESC LIMIT 1) AS office,
            COUNT(*) AS total_sides,
            SUM(CASE WHEN role = 'listing' THEN 1 ELSE 0 END) AS listing_sides,
            SUM(CASE WHEN role = 'buyer' THEN 1 ELSE 0 END) AS buyer_sides,
            SUM(COALESCE(sale_price, 0)) AS volume,
            MAX(close_date) AS most_recent,
            GROUP_CONCAT(DISTINCT city) AS towns
        FROM sides s
        GROUP BY agent
        ORDER BY total_sides DESC
    ''').fetchall()

    return [
        {
            'source': 'maine',
            'name': r['name'],
            'office': r['office'] or '',
            'total_sides': r['total_sides'],
            'listing_sides': r['listing_sides'],
            'buyer_sides': r['buyer_sides'],
            'volume': int(r['volume'] or 0),
            'most_recent': r['most_recent'] or '',
            'towns': (r['towns'] or '').split(',') if r['towns'] else [],
        }
        for r in rows
    ]
