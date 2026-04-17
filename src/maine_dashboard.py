"""HTML dashboard for Maine Listings (MREIS MLS) leaderboards.

Produces data/maine_dashboard.html — a standalone page showing agent and
brokerage leaderboards powered by period-based KPIs, plus a "Biggest Movers"
banner comparing last-12mo vs prior-12mo ranking.
"""
from __future__ import annotations

import logging
import os
import sqlite3
from datetime import datetime
from html import escape

from .dashboard import _css, _sort_js
from .maine_kpis import (
    PERIOD_12MO_DAYS,
    compute_rank_movers,
    query_agent_kpis,
    query_brokerage_kpis,
)
from .maine_report import format_currency
from .state import TOWNS

logger = logging.getLogger(__name__)

_DEFAULT_DASHBOARD = os.path.join(
    os.path.dirname(__file__), '..', 'data', 'maine_dashboard.html',
)


def _e(text) -> str:
    if text is None:
        return 'N/A'
    return escape(str(text))


def _fmt_delta(delta) -> str:
    """Render a rank delta as ↑N / ↓N / NEW / —."""
    if delta is None:
        return '<span class="delta-new">NEW</span>'
    if delta > 0:
        return f'<span class="delta-up">&#9650;{delta}</span>'
    if delta < 0:
        return f'<span class="delta-down">&#9660;{abs(delta)}</span>'
    return '<span class="delta-flat">&mdash;</span>'


def _movers_banner(rows: list[dict], label: str) -> str:
    """Render the Biggest Movers banner for a given entity set."""
    movers = compute_rank_movers(rows, min_sides=5, top_n=5)
    # Hide when too thin.
    qualifying = len([r for r in rows if (r.get('current_12mo_sides') or 0) >= 5])
    if qualifying < 10:
        return ''

    def _card(m, direction):
        cur_vol = int(m.get('current_12mo_volume') or 0)
        prior_vol = int(m.get('prior_12mo_volume') or 0)
        pct = f'+{int((cur_vol - prior_vol) / prior_vol * 100)}%' if prior_vol > 0 else '&mdash;'
        return (
            f'<div class="mover-card {direction}">'
            f'<span class="mover-delta">{_fmt_delta(m.get("delta"))}</span>'
            f'<span class="mover-name">{_e(m.get("name"))}</span>'
            f'<span class="mover-office">{_e(m.get("office") or "")}</span>'
            f'<span class="mover-line">12mo: {format_currency(cur_vol)} '
            f'(vs {format_currency(prior_vol)})  {pct}</span>'
            f'</div>'
        )

    risers_html = ''.join(_card(m, 'up') for m in movers['risers'])
    fallers_html = ''.join(_card(m, 'down') for m in movers['fallers'])

    return f'''
    <section class="movers">
        <h2>&#128293; Biggest Movers &mdash; {escape(label)} (12mo vs prior 12mo)</h2>
        <div class="movers-grid">
            <div class="movers-col"><h3>&#9650; Risers</h3>{risers_html or '<p class="empty">No qualifying risers.</p>'}</div>
            <div class="movers-col"><h3>&#9660; Fallers</h3>{fallers_html or '<p class="empty">No qualifying fallers.</p>'}</div>
        </div>
    </section>
    '''


_AGENT_COLUMNS = [
    ('#',             'num',  None),
    ('Agent',         '',     'name'),
    ('Office',        '',     'office'),
    ('12mo Δ',        'num',  'delta'),
    ('12mo Vol',      'num',  'current_12mo_volume'),
    ('12mo Sides',    'num',  'current_12mo_sides'),
    ('3yr Vol',       'num',  'three_yr_volume'),
    ('All-Time Vol',  'num',  'all_time_volume'),
    ('All-Time Sides', 'num', 'all_time_sides'),
    ('L / B',         'num',  None),
    ('Avg (3yr)',     'num',  None),
    ('Most Recent',   'num',  'most_recent'),
]

_BROKERAGE_COLUMNS = [
    ('#',             'num',  None),
    ('Brokerage',     '',     'name'),
    ('Agents',        'num',  'agent_count'),
    ('12mo Δ',        'num',  'delta'),
    ('12mo Vol',      'num',  'current_12mo_volume'),
    ('12mo Sides',    'num',  'current_12mo_sides'),
    ('3yr Vol',       'num',  'three_yr_volume'),
    ('All-Time Vol',  'num',  'all_time_volume'),
    ('All-Time Sides', 'num', 'all_time_sides'),
    ('L / B',         'num',  None),
    ('Avg (3yr)',     'num',  None),
    ('Most Recent',   'num',  'most_recent'),
]


def _row_html(i: int, r: dict, deltas: dict, is_brokerage: bool) -> str:
    delta = deltas.get(r['name'])
    three_yr_sides = r.get('three_yr_sides') or 0
    three_yr_vol = r.get('three_yr_volume') or 0
    avg_3yr = three_yr_vol / three_yr_sides if three_yr_sides else 0
    lb = f'{r.get("listing_sides") or 0} : {r.get("buyer_sides") or 0}'
    cells = [
        f'<td class="num">{i}</td>',
        f'<td class="agent-name">{_e(r["name"])}</td>',
    ]
    if is_brokerage:
        cells.append(f'<td class="num">{r.get("agent_count") or 0}</td>')
    else:
        cells.append(f'<td>{_e(r.get("office") or "")}</td>')
    cells += [
        f'<td class="num">{_fmt_delta(delta)}</td>',
        f'<td class="num">{_e(format_currency(r.get("current_12mo_volume") or 0))}</td>',
        f'<td class="num">{r.get("current_12mo_sides") or 0}</td>',
        f'<td class="num">{_e(format_currency(r.get("three_yr_volume") or 0))}</td>',
        f'<td class="num">{_e(format_currency(r.get("all_time_volume") or 0))}</td>',
        f'<td class="num">{r.get("all_time_sides") or 0}</td>',
        f'<td class="num">{lb}</td>',
        f'<td class="num">{_e(format_currency(avg_3yr))}</td>',
        f'<td class="num">{_e(r.get("most_recent") or "")}</td>',
    ]
    return f'<tr>{"".join(cells)}</tr>'


def _leaderboard_table(rows: list[dict], is_brokerage: bool, title: str) -> str:
    movers = compute_rank_movers(rows, min_sides=5, top_n=5)
    deltas = {}
    for m in movers['risers'] + movers['fallers']:
        deltas[m['name']] = m.get('delta')

    columns = _BROKERAGE_COLUMNS if is_brokerage else _AGENT_COLUMNS
    header = ''.join(f'<th class="{c[1]}">{_e(c[0])}</th>' for c in columns)
    body = '\n'.join(_row_html(i, r, deltas, is_brokerage) for i, r in enumerate(rows, 1))

    return f'''
    <section class="section">
        <h2>{_e(title)}</h2>
        <div class="table-wrap"><table class="sortable">
            <thead><tr>{header}</tr></thead>
            <tbody>{body or '<tr><td class="empty" colspan="12">No data.</td></tr>'}</tbody>
        </table></div>
    </section>
    '''


def _stats(conn) -> dict:
    row = conn.execute('''
        SELECT COUNT(*) AS total,
               SUM(CASE WHEN enrichment_status='success' THEN 1 ELSE 0 END) AS enriched,
               MIN(close_date) AS date_min, MAX(close_date) AS date_max,
               SUM(COALESCE(sale_price,0)) AS volume
        FROM maine_transactions
    ''').fetchone()
    return dict(row)


def _movers_css() -> str:
    return '''
    .movers { margin: 24px 0; }
    .movers h2 { font-size: 1rem; letter-spacing: 0.02em; margin-bottom: 10px; color: var(--text-1); }
    .movers-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
    .movers-col h3 { font-size: 0.75rem; color: var(--text-2); margin-bottom: 8px; font-family: var(--mono); }
    .mover-card { padding: 10px 12px; background: var(--bg-elevated); border-radius: 8px; margin-bottom: 6px;
        display: grid; grid-template-columns: auto 1fr; gap: 4px 10px; font-size: 0.78rem; }
    .mover-card .mover-delta { grid-row: span 2; align-self: center; font-family: var(--mono); font-weight: 600; }
    .mover-card .mover-name { font-weight: 600; color: var(--text-1); }
    .mover-card .mover-office { font-size: 0.72rem; color: var(--text-2); }
    .mover-card .mover-line { grid-column: 1 / -1; font-size: 0.72rem; color: var(--text-3); font-family: var(--mono); }
    .delta-up { color: hsl(140, 60%, 60%); }
    .delta-down { color: hsl(0, 60%, 62%); }
    .delta-new { color: var(--accent); font-weight: 600; }
    .delta-flat { color: var(--text-3); }
    .empty { color: var(--text-3); font-style: italic; font-size: 0.8rem; }
    '''


def generate_maine_dashboard(
    conn: sqlite3.Connection,
    output_path: str | None = None,
) -> str:
    """Generate data/maine_dashboard.html."""
    output_path = output_path or _DEFAULT_DASHBOARD
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    agent_rows = query_agent_kpis(conn)
    brokerage_rows = query_brokerage_kpis(conn)
    stats = _stats(conn)
    generated_at = datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')

    sections = [
        _movers_banner(agent_rows, 'Agents'),
        _leaderboard_table(agent_rows[:50], False, 'Top 50 Agents — All Towns'),
        _movers_banner(brokerage_rows, 'Brokerages'),
        _leaderboard_table(brokerage_rows[:50], True, 'Top 50 Brokerages — All Towns'),
    ]

    for town in TOWNS:
        town_agents = query_agent_kpis(conn, town=town, limit=50)
        town_brokers = query_brokerage_kpis(conn, town=town, limit=50)
        if not town_agents and not town_brokers:
            continue
        sections.append(_leaderboard_table(town_agents, False, f'Top 50 Agents — {town}'))
        sections.append(_leaderboard_table(town_brokers, True, f'Top 50 Brokerages — {town}'))

    body = '\n'.join(s for s in sections if s)

    html = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Maine MLS Leaderboard</title>
    <style>{_css()}{_movers_css()}</style>
</head>
<body>
    <div class="wrap">
        <header class="header">
            <h1>Maine MLS Leaderboard</h1>
            <p class="sub">MaineListings.com (MREIS) &middot; 10 Towns &middot; {_e(generated_at)}</p>
        </header>
        <div class="stats">
            <div class="stat"><div class="label">Transactions</div><div class="value">{stats["enriched"]:,}</div></div>
            <div class="stat"><div class="label">Agents</div><div class="value">{len(agent_rows):,}</div></div>
            <div class="stat"><div class="label">Brokerages</div><div class="value">{len(brokerage_rows):,}</div></div>
            <div class="stat"><div class="label">Volume</div><div class="value">{_e(format_currency(stats["volume"] or 0))}</div></div>
        </div>
        <main>{body}</main>
        <footer class="footer">Generated {_e(generated_at)} &middot; MaineListings.com MREIS</footer>
    </div>
    <script>{_sort_js()}</script>
</body>
</html>'''

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)
    logger.info('Maine dashboard written to %s', output_path)
    return output_path
