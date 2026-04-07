"""Directory-only leaderboard and dashboard for Zillow data.

Generates two leaderboards — Brokerages and Agents — from
zillow_profiles + zillow_profile_towns tables without requiring
individual transaction data.
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
    """Top agents (individuals + teams, excluding brokerages)."""
    params: list = []
    town_filter = ''
    if town:
        town_filter = 'AND pt.town = ?'
        params.append(town)

    params.append(limit)
    rows = conn.execute(f'''
        SELECT
            p.profile_url, p.profile_name, p.office_name,
            p.profile_type, p.sales_last_12_months, p.price_range,
            SUM(pt.local_sales_count) AS total_local_sales,
            GROUP_CONCAT(DISTINCT pt.town) AS towns
        FROM zillow_profiles p
        JOIN zillow_profile_towns pt ON p.profile_url = pt.profile_url
        WHERE p.profile_type != 'brokerage'
        {town_filter}
        GROUP BY p.profile_url
        ORDER BY total_local_sales DESC
        LIMIT ?
    ''', params).fetchall()

    return [dict(r) for r in rows]


def query_directory_brokerage_leaderboard(
    conn: sqlite3.Connection,
    *,
    limit: int = 20,
    town: str | None = None,
) -> list[dict]:
    """Top brokerages combining agent rollup + direct brokerage profiles."""
    town_filter = ''
    town_params: list = []
    if town:
        town_filter = 'AND pt.town = ?'
        town_params = [town]

    agent_rows = conn.execute(f'''
        SELECT
            p.office_name AS brokerage,
            COUNT(DISTINCT p.profile_url) AS agent_count,
            SUM(pt.local_sales_count) AS agent_sales,
            SUM(COALESCE(p.sales_last_12_months, 0)) AS agent_12mo_sales
        FROM zillow_profiles p
        JOIN zillow_profile_towns pt ON p.profile_url = pt.profile_url
        WHERE p.profile_type IN ('individual', 'team')
          AND p.office_name IS NOT NULL
          {town_filter}
        GROUP BY p.office_name
    ''', town_params).fetchall()

    direct_rows = conn.execute(f'''
        SELECT
            p.profile_name AS brokerage,
            SUM(pt.local_sales_count) AS direct_sales,
            MAX(p.sales_last_12_months) AS sales_12mo
        FROM zillow_profiles p
        JOIN zillow_profile_towns pt ON p.profile_url = pt.profile_url
        WHERE p.profile_type = 'brokerage'
          {town_filter}
        GROUP BY p.profile_name
    ''', town_params).fetchall()

    return _merge_brokerage_data(agent_rows, direct_rows, limit)


def _merge_brokerage_data(agent_rows, direct_rows, limit: int) -> list[dict]:
    """Merge agent rollup and direct brokerage data."""
    brokerages: dict[str, dict] = {}
    for r in agent_rows:
        name = r['brokerage']
        brokerages[name] = {
            'brokerage': name,
            'agent_count': r['agent_count'],
            'agent_sales': r['agent_sales'],
            'agent_12mo_sales': r['agent_12mo_sales'],
            'direct_sales': None,
            'sales_12mo': None,
        }
    for r in direct_rows:
        name = r['brokerage']
        if name in brokerages:
            brokerages[name]['direct_sales'] = r['direct_sales']
            brokerages[name]['sales_12mo'] = r['sales_12mo']
        else:
            brokerages[name] = {
                'brokerage': name,
                'agent_count': 0,
                'agent_sales': 0,
                'agent_12mo_sales': 0,
                'direct_sales': r['direct_sales'],
                'sales_12mo': r['sales_12mo'],
            }

    for b in brokerages.values():
        b['total_sales'] = b['direct_sales'] if b['direct_sales'] is not None else (b['agent_sales'] or 0)
        if b['sales_12mo'] is None:
            b['sales_12mo'] = b.get('agent_12mo_sales') or 0

    ranked = sorted(brokerages.values(), key=lambda x: x['total_sales'], reverse=True)
    return ranked[:limit]


def get_directory_stats(conn: sqlite3.Connection) -> dict:
    """Summary statistics from directory data."""
    total = conn.execute('SELECT COUNT(*) FROM zillow_profiles').fetchone()[0]
    teams = conn.execute(
        "SELECT COUNT(*) FROM zillow_profiles WHERE profile_type = 'team'",
    ).fetchone()[0]
    brokerages = conn.execute(
        "SELECT COUNT(*) FROM zillow_profiles WHERE profile_type = 'brokerage'",
    ).fetchone()[0]
    individuals = conn.execute(
        "SELECT COUNT(*) FROM zillow_profiles WHERE profile_type = 'individual'",
    ).fetchone()[0]
    towns_with_data = conn.execute(
        'SELECT COUNT(DISTINCT town) FROM zillow_profile_towns',
    ).fetchone()[0]
    return {
        'total_profiles': total,
        'agents': individuals + teams,
        'teams': teams,
        'individuals': individuals,
        'brokerages': brokerages,
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
        '# Zillow Leaderboard — Southern Coastal Maine',
        f'*{stats["agents"]} agents ({stats["teams"]} teams, '
        f'{stats["individuals"]} individuals), '
        f'{stats["brokerages"]} brokerages, '
        f'{stats["towns_with_data"]} towns*\n',
    ]

    _append_brokerage_section(conn, lines)
    _append_agent_section(conn, lines)
    _append_town_sections(conn, lines)

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines) + '\n')
    logger.info('Directory leaderboard written to %s', output_path)
    return output_path


def _append_brokerage_section(conn, lines: list[str]) -> None:
    lines.append('## Top 20 Brokerages\n')
    lines.append('| # | Brokerage | Agents | Total Sales | 12-Mo Sales |')
    lines.append('|---|-----------|--------|-------------|-------------|')
    for i, b in enumerate(query_directory_brokerage_leaderboard(conn), 1):
        lines.append(
            f'| {i} | {b["brokerage"]} '
            f'| {b["agent_count"]} '
            f'| {b["total_sales"]:,} '
            f'| {b["sales_12mo"] or "N/A"} |'
        )


def _append_agent_section(conn, lines: list[str]) -> None:
    lines.append('\n## Top 30 Agents\n')
    lines.append('| # | Agent | Office | Type | Local Sales | 12-Mo | Towns |')
    lines.append('|---|-------|--------|------|-------------|-------|-------|')
    for i, a in enumerate(query_directory_top_agents(conn, limit=30), 1):
        lines.append(
            f'| {i} | {a["profile_name"] or "N/A"} '
            f'| {a["office_name"] or "N/A"} '
            f'| {a["profile_type"]} '
            f'| {a["total_local_sales"]:,} '
            f'| {a["sales_last_12_months"] or "N/A"} '
            f'| {a["towns"] or "N/A"} |'
        )


def _append_town_sections(conn, lines: list[str]) -> None:
    for town in TOWNS:
        agents = query_directory_top_agents(conn, limit=10, town=town)
        broks = query_directory_brokerage_leaderboard(conn, limit=5, town=town)
        if not agents and not broks:
            continue
        lines.append(f'\n## {town}\n')
        if broks:
            lines.append('### Top Brokerages\n')
            lines.append('| # | Brokerage | Agents | Sales |')
            lines.append('|---|-----------|--------|-------|')
            for i, b in enumerate(broks, 1):
                lines.append(
                    f'| {i} | {b["brokerage"]} '
                    f'| {b["agent_count"]} '
                    f'| {b["total_sales"]:,} |'
                )
        if agents:
            lines.append('\n### Top Agents\n')
            lines.append('| # | Agent | Office | Sales | 12-Mo |')
            lines.append('|---|-------|--------|-------|-------|')
            for i, a in enumerate(agents, 1):
                lines.append(
                    f'| {i} | {a["profile_name"] or "N/A"} '
                    f'| {a["office_name"] or "N/A"} '
                    f'| {a["total_local_sales"]:,} '
                    f'| {a["sales_last_12_months"] or "N/A"} |'
                )


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
    top_brokerages = query_directory_brokerage_leaderboard(conn, limit=20)
    top_agents = query_directory_top_agents(conn, limit=30)

    sections = [_build_brokerages_section(top_brokerages)]
    sections.append(_build_agents_section(top_agents, 'Top Agents — All Towns'))

    for town in TOWNS:
        broks = query_directory_brokerage_leaderboard(conn, limit=10, town=town)
        agents = query_directory_top_agents(conn, limit=10, town=town)
        if broks:
            sections.append(_build_brokerages_section(broks, f'Top Brokerages — {town}'))
        if agents:
            sections.append(_build_agents_section(agents, f'Top Agents — {town}'))

    body = '\n'.join(sections)
    html = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Zillow Leaderboard &mdash; Southern Coastal Maine</title>
    <style>{_css()}</style>
</head>
<body>
    <div class="wrap">
        <header class="header">
            <h1>Zillow Leaderboard</h1>
            <p class="sub">Southern Coastal Maine &middot; 10 Towns &middot; {_e(generated_at)}</p>
        </header>
        <div class="stats">
            <div class="stat"><div class="label">Brokerages</div><div class="value">{stats["brokerages"]:,}</div></div>
            <div class="stat"><div class="label">Agents</div><div class="value">{stats["agents"]:,}</div></div>
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
        ptype = a.get('profile_type', '')
        type_badge = 'TEAM' if ptype == 'team' else ''
        rows += f'''<tr{cls}>
            <td class="num">{i}</td>
            <td class="agent-name">{_e(a.get("profile_name"))}</td>
            <td class="office">{_e(a.get("office_name"))}</td>
            <td class="num">{type_badge}</td>
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


def _build_brokerages_section(brokerages: list[dict], title: str = 'Top Brokerages') -> str:
    rows = ''
    for i, b in enumerate(brokerages, 1):
        cls = ' class="rank-1"' if i == 1 else ''
        rows += f'''<tr{cls}>
            <td class="num">{i}</td>
            <td class="office">{_e(b.get("brokerage"))}</td>
            <td class="num">{b["agent_count"]}</td>
            <td class="num">{b["total_sales"]:,}</td>
            <td class="num">{b.get("sales_12mo") or "N/A"}</td>
        </tr>'''

    return f'''<section class="section">
        <h2>{_e(title)}</h2>
        <div class="table-wrap"><table>
            <colgroup>
                <col style="width:5%"><col style="width:35%">
                <col style="width:15%"><col style="width:20%"><col style="width:20%">
            </colgroup>
            <thead><tr>
                <th class="num">#</th><th>Brokerage</th>
                <th class="num">Agents</th><th class="num">Total Sales</th>
                <th class="num">12-Mo Sales</th>
            </tr></thead>
            <tbody>{rows}</tbody>
        </table></div>
    </section>'''
