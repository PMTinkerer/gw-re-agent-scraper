"""Directory-only leaderboard and dashboard for Zillow data.

Generates reports from zillow_profiles + zillow_profile_towns tables
without requiring individual transaction data. Used with the Firecrawl
directory-scraping approach (Approach A).
"""
from __future__ import annotations

import logging
import os
import sqlite3
from datetime import datetime
from html import escape

from .dashboard import _css, _sort_js
from .state import TOWNS

logger = logging.getLogger(__name__)

_DEFAULT_LEADERBOARD = os.path.join(
    os.path.dirname(__file__), '..', 'data', 'zillow_agent_leaderboard.md',
)
_DEFAULT_DASHBOARD = os.path.join(
    os.path.dirname(__file__), '..', 'data', 'zillow_directory_dashboard.html',
)


def query_directory_top_agents(
    conn: sqlite3.Connection,
    *,
    limit: int = 30,
    town: str | None = None,
) -> list[dict]:
    """Top agents ranked by total local sales count across towns."""
    params: list = []
    where = ''
    if town:
        where = 'WHERE pt.town = ?'
        params.append(town)

    params.append(limit)
    rows = conn.execute(f'''
        SELECT
            p.profile_url,
            p.profile_name,
            p.office_name,
            p.profile_type,
            p.sales_last_12_months,
            p.price_range,
            SUM(pt.local_sales_count) AS total_local_sales,
            GROUP_CONCAT(DISTINCT pt.town) AS towns
        FROM zillow_profiles p
        JOIN zillow_profile_towns pt ON p.profile_url = pt.profile_url
        {where}
        GROUP BY p.profile_url
        ORDER BY total_local_sales DESC
        LIMIT ?
    ''', params).fetchall()

    return [dict(r) for r in rows]


def query_directory_top_brokerages(
    conn: sqlite3.Connection,
    *,
    limit: int = 15,
    town: str | None = None,
) -> list[dict]:
    """Top brokerages ranked by aggregate local sales of their agents."""
    params: list = []
    where = ''
    if town:
        where = 'WHERE pt.town = ?'
        params.append(town)

    params.append(limit)
    rows = conn.execute(f'''
        SELECT
            p.office_name,
            COUNT(DISTINCT p.profile_url) AS agent_count,
            SUM(pt.local_sales_count) AS total_local_sales,
            SUM(COALESCE(p.sales_last_12_months, 0)) AS total_12mo_sales
        FROM zillow_profiles p
        JOIN zillow_profile_towns pt ON p.profile_url = pt.profile_url
        {where}
        AND p.office_name IS NOT NULL
        GROUP BY p.office_name
        ORDER BY total_local_sales DESC
        LIMIT ?
    ''', params).fetchall()

    return [dict(r) for r in rows]


def get_directory_stats(conn: sqlite3.Connection) -> dict:
    """Summary statistics from directory data."""
    total = conn.execute('SELECT COUNT(*) FROM zillow_profiles').fetchone()[0]
    teams = conn.execute(
        "SELECT COUNT(*) FROM zillow_profiles WHERE profile_type = 'team'",
    ).fetchone()[0]
    towns_with_data = conn.execute(
        'SELECT COUNT(DISTINCT town) FROM zillow_profile_towns',
    ).fetchone()[0]
    return {
        'total_agents': total,
        'teams': teams,
        'individuals': total - teams,
        'towns_with_data': towns_with_data,
    }


def generate_directory_leaderboard(
    conn: sqlite3.Connection,
    output_path: str | None = None,
) -> str:
    """Generate markdown leaderboard from directory data."""
    output_path = output_path or _DEFAULT_LEADERBOARD
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    stats = get_directory_stats(conn)
    lines = [
        '# Zillow Agent Leaderboard — Southern Coastal Maine',
        f'*Directory data: {stats["total_agents"]} agents '
        f'({stats["teams"]} teams, {stats["individuals"]} individuals) '
        f'across {stats["towns_with_data"]} towns*\n',
    ]

    lines.append('## Top 30 Agents — All Towns\n')
    lines.append('| # | Agent | Office | Type | Local Sales | 12-Mo Sales | Towns |')
    lines.append('|---|-------|--------|------|-------------|-------------|-------|')
    for i, a in enumerate(query_directory_top_agents(conn, limit=30), 1):
        lines.append(
            f'| {i} | {a["profile_name"] or "N/A"} '
            f'| {a["office_name"] or "N/A"} '
            f'| {a["profile_type"]} '
            f'| {a["total_local_sales"]:,} '
            f'| {a["sales_last_12_months"] or "N/A"} '
            f'| {a["towns"] or "N/A"} |'
        )

    lines.append('\n## Top 15 Brokerages\n')
    lines.append('| # | Brokerage | Agents | Local Sales | 12-Mo Sales |')
    lines.append('|---|-----------|--------|-------------|-------------|')
    for i, b in enumerate(query_directory_top_brokerages(conn, limit=15), 1):
        lines.append(
            f'| {i} | {b["office_name"]} '
            f'| {b["agent_count"]} '
            f'| {b["total_local_sales"]:,} '
            f'| {b["total_12mo_sales"]:,} |'
        )

    for town in TOWNS:
        agents = query_directory_top_agents(conn, limit=10, town=town)
        if not agents:
            continue
        lines.append(f'\n## Top Agents — {town}\n')
        lines.append('| # | Agent | Office | Local Sales | 12-Mo |')
        lines.append('|---|-------|--------|-------------|-------|')
        for i, a in enumerate(agents, 1):
            lines.append(
                f'| {i} | {a["profile_name"] or "N/A"} '
                f'| {a["office_name"] or "N/A"} '
                f'| {a["total_local_sales"]:,} '
                f'| {a["sales_last_12_months"] or "N/A"} |'
            )

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines) + '\n')
    logger.info('Directory leaderboard written to %s', output_path)
    return output_path


def _e(text) -> str:
    if text is None:
        return 'N/A'
    return escape(str(text))


def generate_directory_dashboard(
    conn: sqlite3.Connection,
    output_path: str | None = None,
) -> str:
    """Generate HTML dashboard from directory data."""
    output_path = output_path or _DEFAULT_DASHBOARD
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    stats = get_directory_stats(conn)
    generated_at = datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')
    top_agents = query_directory_top_agents(conn, limit=30)
    top_brokerages = query_directory_top_brokerages(conn, limit=15)

    sections = [_build_agents_section(top_agents, 'Top Agents — All Towns')]
    sections.append(_build_brokerages_section(top_brokerages))

    for town in TOWNS:
        agents = query_directory_top_agents(conn, limit=10, town=town)
        if agents:
            sections.append(_build_agents_section(agents, f'Top Agents — {town}'))

    body = '\n'.join(sections)
    html = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Zillow Agent Leaderboard &mdash; Southern Coastal Maine</title>
    <style>{_css()}</style>
</head>
<body>
    <div class="wrap">
        <header class="header">
            <h1>Zillow Agent Leaderboard</h1>
            <p class="sub">Southern Coastal Maine &middot; 10 Towns &middot; {_e(generated_at)}</p>
        </header>
        <div class="stats">
            <div class="stat"><div class="label">Agents Tracked</div><div class="value">{stats["total_agents"]:,}</div></div>
            <div class="stat"><div class="label">Teams</div><div class="value">{stats["teams"]:,}</div></div>
            <div class="stat"><div class="label">Towns</div><div class="value">{stats["towns_with_data"]}</div></div>
        </div>
        <main>{body}</main>
        <footer class="footer">
            Generated {_e(generated_at)} &middot; Data source: Zillow Directory &middot; gw-re-agent-scraper
        </footer>
    </div>
<script>{_sort_js()}</script>
</body>
</html>'''

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)
    logger.info('Directory dashboard written to %s', output_path)
    return output_path


def _build_agents_section(agents: list[dict], title: str) -> str:
    rows = ''
    for i, a in enumerate(agents, 1):
        cls = ' class="rank-1"' if i == 1 else ''
        rows += f'''<tr{cls}>
            <td class="num">{i}</td>
            <td class="agent-name">{_e(a.get("profile_name"))}</td>
            <td class="office">{_e(a.get("office_name"))}</td>
            <td class="num">{_e(a.get("profile_type"))}</td>
            <td class="num">{a["total_local_sales"]:,}</td>
            <td class="num">{a.get("sales_last_12_months") or "N/A"}</td>
            <td class="towns">{_e(a.get("towns"))}</td>
        </tr>'''

    return f'''<section class="section">
        <h2>{_e(title)}</h2>
        <div class="table-wrap"><table>
            <colgroup>
                <col style="width:5%"><col style="width:20%"><col style="width:22%">
                <col style="width:8%"><col style="width:12%"><col style="width:10%"><col style="width:23%">
            </colgroup>
            <thead><tr>
                <th class="num">#</th><th>Agent</th><th>Office</th>
                <th class="num">Type</th><th class="num">Local Sales</th>
                <th class="num">12-Mo</th><th>Towns</th>
            </tr></thead>
            <tbody>{rows}</tbody>
        </table></div>
    </section>'''


def _build_brokerages_section(brokerages: list[dict]) -> str:
    rows = ''
    for i, b in enumerate(brokerages, 1):
        cls = ' class="rank-1"' if i == 1 else ''
        rows += f'''<tr{cls}>
            <td class="num">{i}</td>
            <td class="office">{_e(b.get("office_name"))}</td>
            <td class="num">{b["agent_count"]}</td>
            <td class="num">{b["total_local_sales"]:,}</td>
            <td class="num">{b["total_12mo_sales"]:,}</td>
        </tr>'''

    return f'''<section class="section">
        <h2>Top Brokerages</h2>
        <div class="table-wrap"><table>
            <colgroup>
                <col style="width:5%"><col style="width:35%">
                <col style="width:15%"><col style="width:20%"><col style="width:20%">
            </colgroup>
            <thead><tr>
                <th class="num">#</th><th>Brokerage</th>
                <th class="num">Agents</th><th class="num">Local Sales</th>
                <th class="num">12-Mo Sales</th>
            </tr></thead>
            <tbody>{rows}</tbody>
        </table></div>
    </section>'''
