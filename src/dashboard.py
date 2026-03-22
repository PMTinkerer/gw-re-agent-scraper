"""HTML dashboard generator for agent leaderboard.

Produces data/dashboard.html — a self-contained static HTML file with
embedded CSS, showing ranked agent/brokerage tables with trend indicators.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from html import escape

from .report import (
    format_currency,
    query_top_agents,
    query_top_brokerages,
    query_top_agents_by_town,
    get_report_stats,
)
from .state import TOWNS

logger = logging.getLogger(__name__)

_DEFAULT_OUTPUT = os.path.join(os.path.dirname(__file__), '..', 'data', 'dashboard.html')


def generate_dashboard(conn, output_path: str | None = None) -> str:
    """Generate the HTML dashboard. Returns the output file path."""
    output_path = output_path or _DEFAULT_OUTPUT
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    now = datetime.utcnow()
    since_date = (now - timedelta(days=365)).strftime('%Y-%m-%d')
    generated_at = now.strftime('%Y-%m-%d %H:%M UTC')

    stats = get_report_stats(conn)
    all_time_agents = query_top_agents(conn, limit=30)
    rolling_agents = query_top_agents(conn, limit=30, since_date=since_date)
    trends = _compute_trend_indicators(all_time_agents, rolling_agents)
    brokerages = query_top_brokerages(conn, limit=15)

    town_agents = {}
    for town in TOWNS:
        town_agents[town] = query_top_agents_by_town(conn, town, limit=5)

    html = _build_html(
        stats=stats,
        all_time_agents=all_time_agents,
        rolling_agents=rolling_agents,
        trends=trends,
        brokerages=brokerages,
        town_agents=town_agents,
        generated_at=generated_at,
    )

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)

    logger.info('Dashboard written to %s', output_path)
    return output_path


def _compute_trend_indicators(
    all_time_agents: list[dict],
    rolling_agents: list[dict],
) -> dict[str, dict]:
    """Compute rank change and volume delta for each agent.

    Compares 365-day rank vs all-time rank. Positive rank_change = heating up.
    """
    all_time_rank = {a['agent_name']: i + 1 for i, a in enumerate(all_time_agents)}

    trends = {}
    for i, agent in enumerate(rolling_agents):
        name = agent['agent_name']
        rolling_rank = i + 1
        if name in all_time_rank:
            rank_change = all_time_rank[name] - rolling_rank
            trends[name] = {
                'rank_change': rank_change,
                'rolling_volume': agent['volume'],
                'is_new': False,
            }
        else:
            trends[name] = {
                'rank_change': 0,
                'rolling_volume': agent['volume'],
                'is_new': True,
            }
    return trends


def _render_trend_badge(trend: dict) -> str:
    """Render an HTML trend badge."""
    vol = format_currency(trend['rolling_volume'])
    if trend['is_new']:
        return '<span class="badge badge-new">NEW</span>'
    change = trend['rank_change']
    if change > 0:
        return f'<span class="badge badge-up">&#9650;{change} ({vol})</span>'
    elif change < 0:
        return f'<span class="badge badge-down">&#9660;{abs(change)} ({vol})</span>'
    else:
        return f'<span class="badge badge-flat">&mdash; ({vol})</span>'


def _e(text) -> str:
    """Escape HTML, handling None."""
    if text is None:
        return 'N/A'
    return escape(str(text))


def _format_date_short(date_str: str | None) -> str:
    """Format 'YYYY-MM-DD' as 'Mar 2023'. Falls back to raw string."""
    if not date_str or date_str == 'N/A':
        return 'N/A'
    try:
        dt = datetime.strptime(str(date_str)[:10], '%Y-%m-%d')
        return dt.strftime('%b %Y')
    except (ValueError, TypeError):
        return escape(str(date_str))


def _build_html(
    stats: dict,
    all_time_agents: list[dict],
    rolling_agents: list[dict],
    trends: dict,
    brokerages: list[dict],
    town_agents: dict[str, list[dict]],
    generated_at: str,
) -> str:
    """Assemble the complete HTML document."""
    sections = []

    # --- Section 1: Top Agents All-Time ---
    rows_html = ''
    if all_time_agents:
        for i, a in enumerate(all_time_agents, 1):
            row_cls = ' class="rank-1"' if i == 1 else ''
            rows_html += f'''<tr{row_cls}>
                <td class="num">{i}</td>
                <td class="agent-name">{_e(a['agent_name'])}</td>
                <td>{_e(a['office'])}</td>
                <td class="num">{a['sides']}</td>
                <td class="num vol">{format_currency(a['volume'])}</td>
                <td class="num">{format_currency(a['avg_price'])}</td>
                <td class="towns">{_e(a['towns'])}</td>
            </tr>'''
    else:
        rows_html = '<tr><td colspan="7" class="no-data">No data available yet.</td></tr>'

    sections.append(f'''<section class="section">
        <h2>Top Agents &mdash; All-Time</h2>
        <div class="table-wrap">
        <table>
            <thead><tr>
                <th class="num">#</th>
                <th>Agent</th>
                <th>Office</th>
                <th class="num">Sides</th>
                <th class="num">Volume</th>
                <th class="num">Avg Price</th>
                <th>Towns</th>
            </tr></thead>
            <tbody>{rows_html}</tbody>
        </table>
        </div>
    </section>''')

    # --- Section 2: Top Agents 365-Day Rolling ---
    rows_html = ''
    if rolling_agents:
        for i, a in enumerate(rolling_agents, 1):
            name = a['agent_name']
            trend = trends.get(name, {'rank_change': 0, 'rolling_volume': 0, 'is_new': False})
            badge = _render_trend_badge(trend)
            row_cls = ' class="rank-1"' if i == 1 else ''
            rows_html += f'''<tr{row_cls}>
                <td class="num">{i}</td>
                <td class="agent-name">{_e(name)}</td>
                <td>{_e(a['office'])}</td>
                <td class="num">{a['sides']}</td>
                <td class="num vol">{format_currency(a['volume'])}</td>
                <td style="text-align:center">{badge}</td>
            </tr>'''
    else:
        rows_html = '<tr><td colspan="6" class="no-data">No data available yet.</td></tr>'

    sections.append(f'''<section class="section">
        <h2>Top Agents &mdash; Last 365 Days</h2>
        <div class="table-wrap">
        <table>
            <thead><tr>
                <th class="num">#</th>
                <th>Agent</th>
                <th>Office</th>
                <th class="num">Sides</th>
                <th class="num">Volume</th>
                <th style="text-align:center">Trend</th>
            </tr></thead>
            <tbody>{rows_html}</tbody>
        </table>
        </div>
    </section>''')

    # --- Section 3: Top Brokerages ---
    rows_html = ''
    if brokerages:
        for i, b in enumerate(brokerages, 1):
            row_cls = ' class="rank-1"' if i == 1 else ''
            rows_html += f'''<tr{row_cls}>
                <td class="num">{i}</td>
                <td class="agent-name">{_e(b['office'])}</td>
                <td class="num">{b['sides']}</td>
                <td class="num vol">{format_currency(b['volume'])}</td>
                <td class="num">{format_currency(b['avg_price'])}</td>
                <td>{_e(b['top_agents'])}</td>
            </tr>'''
    else:
        rows_html = '<tr><td colspan="6" class="no-data">No data available yet.</td></tr>'

    sections.append(f'''<section class="section">
        <h2>Top Brokerages</h2>
        <div class="table-wrap">
        <table>
            <thead><tr>
                <th class="num">#</th>
                <th>Brokerage</th>
                <th class="num">Sides</th>
                <th class="num">Volume</th>
                <th class="num">Avg Price</th>
                <th>Top Agents</th>
            </tr></thead>
            <tbody>{rows_html}</tbody>
        </table>
        </div>
    </section>''')

    # --- Section 4: Top Agents by Town ---
    town_sections = ''
    for town in TOWNS:
        agents = town_agents.get(town, [])
        rows_html = ''
        if agents:
            for i, a in enumerate(agents, 1):
                rows_html += f'''<tr>
                    <td class="num">{i}</td>
                    <td class="agent-name">{_e(a['agent_name'])}</td>
                    <td>{_e(a['office'])}</td>
                    <td class="num">{a['sides']}</td>
                    <td class="num vol">{format_currency(a['volume'])}</td>
                </tr>'''
        else:
            rows_html = '<tr><td colspan="5" class="no-data">No data available.</td></tr>'

        town_sections += f'''<div class="town-group">
            <h3>{_e(town)}</h3>
            <div class="table-wrap">
            <table>
                <thead><tr>
                    <th class="num">#</th>
                    <th>Agent</th>
                    <th>Office</th>
                    <th class="num">Sides</th>
                    <th class="num">Volume</th>
                </tr></thead>
                <tbody>{rows_html}</tbody>
            </table>
            </div>
        </div>'''

    sections.append(f'''<section class="section">
        <h2>Top Agents by Town</h2>
        {town_sections}
    </section>''')

    # --- Assemble ---
    body = '\n'.join(sections)

    return f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta name="description" content="Ranked leaderboard of real estate listing agents and brokerages across 10 southern coastal Maine towns.">
    <title>Agent Leaderboard &mdash; Southern Coastal Maine</title>
    <style>{_css()}</style>
</head>
<body>
    <div class="wrap">
        <header class="header">
            <h1>Real Estate Agent Leaderboard</h1>
            <p class="sub">Southern Coastal Maine &middot; 10 Towns &middot; {_e(generated_at)}</p>
        </header>
        <div class="stats">
            <div class="stat">
                <div class="label">Total Sales</div>
                <div class="value">{stats['total']:,}</div>
            </div>
            <div class="stat">
                <div class="label">Agents Tracked</div>
                <div class="value">{stats['unique_agents']:,}</div>
            </div>
            <div class="stat">
                <div class="label">Date Range</div>
                <div class="value">{_format_date_short(stats['date_min'])} &mdash; {_format_date_short(stats['date_max'])}</div>
            </div>
        </div>
        <main>
            {body}
        </main>
        <footer class="footer">
            Generated {_e(generated_at)} &middot; Data source: Redfin &middot; gw-re-agent-scraper
        </footer>
    </div>
</body>
</html>'''


def _css() -> str:
    """Return the embedded CSS stylesheet."""
    return """
    @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

    :root {
        --bg-base: hsl(220, 15%, 8%);
        --bg-surface: hsl(220, 13%, 11%);
        --bg-elevated: hsl(220, 12%, 14%);
        --bg-hover: hsl(220, 11%, 17%);
        --text-1: rgba(255,255,255,0.90);
        --text-2: rgba(255,255,255,0.55);
        --text-3: rgba(255,255,255,0.32);
        --accent: hsl(42, 78%, 60%);
        --accent-dim: hsla(42, 78%, 60%, 0.80);
        --trend-up: hsl(152, 55%, 45%);
        --trend-down: hsl(0, 60%, 52%);
        --trend-flat: rgba(255,255,255,0.30);
        --trend-new: hsl(200, 75%, 52%);
        --shadow: hsla(220, 40%, 4%, 0.55);
        --rule: rgba(255,255,255,0.06);
        --radius: 10px;
        --font: 'DM Sans', sans-serif;
        --mono: 'JetBrains Mono', monospace;
    }

    html { scroll-behavior: smooth; }

    body {
        font-family: var(--font);
        background: var(--bg-base);
        color: var(--text-1);
        line-height: 1.5;
        min-height: 100dvh;
        -webkit-font-smoothing: antialiased;
        -moz-osx-font-smoothing: grayscale;
    }

    body::before {
        content: '';
        position: fixed;
        inset: 0;
        background: radial-gradient(ellipse at 8% -5%, hsla(42,70%,55%,0.045) 0%, transparent 55%);
        pointer-events: none;
        z-index: 0;
    }

    body::after {
        content: '';
        position: fixed;
        inset: 0;
        opacity: 0.018;
        pointer-events: none;
        z-index: 1;
        background-image: url("data:image/svg+xml,%3Csvg viewBox='0 0 256 256' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.85' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23n)'/%3E%3C/svg%3E");
        background-repeat: repeat;
        background-size: 256px 256px;
    }

    .wrap {
        max-width: 1280px;
        margin: 0 auto;
        padding: 0 32px;
        position: relative;
        z-index: 2;
    }

    /* --- HEADER --- */
    .header {
        padding: 56px 0 20px;
    }
    .header h1 {
        font-size: 2.1rem;
        font-weight: 700;
        letter-spacing: -0.03em;
        line-height: 1.15;
        color: var(--text-1);
        text-wrap: balance;
    }
    .header .sub {
        font-size: 0.78rem;
        color: var(--text-3);
        margin-top: 6px;
        font-family: var(--mono);
        letter-spacing: 0.01em;
    }

    /* --- STAT CARDS --- */
    .stats {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
        gap: 16px;
        margin: 28px 0 48px;
    }
    .stat {
        background: var(--bg-surface);
        border-radius: var(--radius);
        padding: 22px 26px;
        transition: transform 180ms ease-out, box-shadow 180ms ease-out;
        animation: fadeUp 450ms ease-out both;
    }
    .stat:nth-child(1) { animation-delay: 0ms; }
    .stat:nth-child(2) { animation-delay: 70ms; }
    .stat:nth-child(3) { animation-delay: 140ms; }
    .stat:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 12px var(--shadow);
    }
    .stat .label {
        font-family: var(--mono);
        font-size: 0.62rem;
        font-weight: 500;
        letter-spacing: 0.1em;
        text-transform: uppercase;
        color: var(--text-3);
        margin-bottom: 8px;
    }
    .stat .value {
        font-size: 1.85rem;
        font-weight: 700;
        color: var(--accent);
        letter-spacing: -0.02em;
        line-height: 1.2;
        white-space: nowrap;
    }

    /* --- SECTIONS --- */
    .section {
        background: var(--bg-surface);
        border-radius: var(--radius);
        padding: 28px 32px 24px;
        margin-bottom: 24px;
        animation: fadeUp 450ms ease-out both;
    }
    .section:nth-of-type(1) { animation-delay: 200ms; }
    .section:nth-of-type(2) { animation-delay: 280ms; }
    .section:nth-of-type(3) { animation-delay: 360ms; }
    .section:nth-of-type(4) { animation-delay: 440ms; }

    .section h2 {
        font-size: 1.25rem;
        font-weight: 600;
        letter-spacing: -0.01em;
        color: var(--text-1);
        padding-bottom: 16px;
        border-bottom: 1px solid var(--rule);
        margin-bottom: 4px;
    }

    /* --- TOWN SUB-SECTIONS --- */
    .town-group h3 {
        font-size: 0.95rem;
        font-weight: 600;
        color: var(--text-2);
        margin: 24px 0 4px;
        padding-left: 2px;
    }

    /* --- TABLES --- */
    .table-wrap { overflow-x: auto; }

    table {
        width: 100%;
        border-collapse: collapse;
        font-size: 0.82rem;
    }
    thead th {
        font-family: var(--mono);
        font-size: 0.6rem;
        font-weight: 500;
        letter-spacing: 0.1em;
        text-transform: uppercase;
        color: var(--text-3);
        text-align: left;
        padding: 14px 12px 10px;
        border-bottom: 1px solid var(--rule);
        position: sticky;
        top: 0;
        background: var(--bg-surface);
    }
    thead th.num { text-align: right; }

    tbody td {
        padding: 10px 12px;
        color: var(--text-2);
        border: none;
        transition: background 120ms ease-out, color 120ms ease-out;
        vertical-align: middle;
    }
    tbody tr:nth-child(odd) td { background: transparent; }
    tbody tr:nth-child(even) td { background: var(--bg-elevated); }
    tbody tr:hover td { background: var(--bg-hover); }

    td.num {
        text-align: right;
        font-family: var(--mono);
        font-variant-numeric: tabular-nums;
        font-size: 0.8rem;
    }
    td.vol {
        color: var(--accent-dim);
        font-weight: 500;
    }
    td.agent-name {
        color: var(--text-1);
        font-weight: 500;
    }
    tr.rank-1 td.agent-name {
        color: var(--accent);
        font-weight: 600;
    }
    tr.rank-1 td:first-child {
        border-left: 2px solid var(--accent);
    }

    td.towns {
        font-size: 0.75rem;
        color: var(--text-3);
        max-width: 180px;
    }

    .no-data {
        text-align: center;
        color: var(--text-3);
        font-style: italic;
        padding: 32px 12px;
    }

    /* --- TREND BADGES --- */
    .badge {
        display: inline-block;
        font-family: var(--mono);
        font-size: 0.68rem;
        font-weight: 500;
        padding: 2px 8px;
        border-radius: 4px;
        white-space: nowrap;
    }
    .badge-up {
        color: var(--trend-up);
        background: hsla(152, 55%, 45%, 0.12);
    }
    .badge-down {
        color: var(--trend-down);
        background: hsla(0, 60%, 52%, 0.12);
    }
    .badge-flat {
        color: var(--trend-flat);
        background: rgba(255,255,255,0.05);
    }
    .badge-new {
        color: var(--trend-new);
        background: hsla(200, 75%, 52%, 0.12);
    }

    /* --- FOOTER --- */
    .footer {
        text-align: center;
        padding: 40px 0 48px;
        font-size: 0.7rem;
        color: var(--text-3);
        font-family: var(--mono);
    }

    /* --- ANIMATION --- */
    @keyframes fadeUp {
        from { opacity: 0; transform: translateY(10px); }
        to { opacity: 1; transform: translateY(0); }
    }

    /* --- RESPONSIVE --- */
    @media (max-width: 768px) {
        .wrap { padding: 0 16px; }
        .header { padding: 32px 0 16px; }
        .header h1 { font-size: 1.5rem; }
        .stat .value { font-size: 1.4rem; }
        .section { padding: 20px 16px 16px; }
        table { font-size: 0.75rem; }
        thead th { font-size: 0.55rem; }
    }
    """
