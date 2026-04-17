"""Generate the tabbed index.html with embedded agent search.

Produces data/index.html — a wrapper page with tab navigation between
Redfin and Zillow dashboards, plus a client-side agent search that
queries embedded JSON data from both sources.
"""
from __future__ import annotations

import json
import logging
import os

from .maine_report import build_maine_search_index
from .report import build_agent_search_index
from .zillow_directory_report import build_zillow_search_index

logger = logging.getLogger(__name__)

_DEFAULT_OUTPUT = os.path.join(
    os.path.dirname(__file__), '..', 'data', 'index.html',
)


def generate_index_html(
    redfin_conn=None,
    zillow_conn=None,
    maine_conn=None,
    output_path: str | None = None,
) -> str:
    """Generate index.html with tab navigation and agent search."""
    output_path = output_path or _DEFAULT_OUTPUT
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    redfin_index = build_agent_search_index(redfin_conn) if redfin_conn else []
    zillow_index = build_zillow_search_index(zillow_conn) if zillow_conn else []
    maine_index = build_maine_search_index(maine_conn) if maine_conn else []

    # KPI rollups for the Leaderboard tab.
    agent_kpis: list = []
    brokerage_kpis: list = []
    if maine_conn is not None:
        from .maine_kpis import query_agent_kpis, query_brokerage_kpis
        agent_kpis = query_agent_kpis(maine_conn)
        brokerage_kpis = query_brokerage_kpis(maine_conn)

    redfin_json = json.dumps(redfin_index, separators=(',', ':'))
    zillow_json = json.dumps(zillow_index, separators=(',', ':'))
    maine_json = json.dumps(maine_index, separators=(',', ':'))
    agent_json = json.dumps(agent_kpis, separators=(',', ':'), default=str)
    brokerage_json = json.dumps(brokerage_kpis, separators=(',', ':'), default=str)

    html = _build_html(redfin_json, zillow_json, maine_json, agent_json, brokerage_json)

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)
    logger.info(
        'Index page written to %s (%d Redfin, %d Zillow, %d Maine; '
        '%d agent KPIs, %d brokerage KPIs)',
        output_path, len(redfin_index), len(zillow_index), len(maine_index),
        len(agent_kpis), len(brokerage_kpis),
    )
    return output_path


def _fmt_currency(amount: int | float) -> str:
    """JS-compatible currency formatter (used in template)."""
    if amount >= 1_000_000:
        return f'${amount / 1_000_000:.1f}M'
    if amount >= 1_000:
        return f'${amount / 1_000:.0f}K'
    return f'${amount:,}'


def _build_html(
    redfin_json: str,
    zillow_json: str,
    maine_json: str,
    agent_json: str,
    brokerage_json: str,
) -> str:
    return f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>RE Agent Leaderboard &mdash; Southern Coastal Maine</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
    <style>{_css()}</style>
</head>
<body>
    <nav class="tab-bar">
        <div class="logo">Southern Coastal <span>Maine</span></div>
        <button class="tab active" data-tab="maine">Maine MLS</button>
        <button class="tab" data-tab="master">All Agents</button>
        <button class="tab" data-tab="zillow">Zillow <span class="archived-pill">archive</span></button>
        <button class="tab" data-tab="redfin">Redfin <span class="archived-pill">archive</span></button>
        <div class="search-wrap">
            <input type="text" id="agent-search" placeholder="Search any agent or office..." autocomplete="off">
            <div id="search-results" class="search-results hidden"></div>
        </div>
    </nav>
    <div class="tab-content">
        <iframe id="maine" class="active" src="maine.html"></iframe>
        <iframe id="zillow" src="zillow.html"></iframe>
        <iframe id="redfin" src="redfin.html"></iframe>
        <div id="master" class="master-tab">
            <div class="master-filters">
                <select id="filter-town"><option value="">All Towns</option></select>
                <select id="filter-type"><option value="">All Types</option><option value="individual">Individual</option><option value="team">Team</option></select>
                <input type="text" id="filter-name" placeholder="Filter by name or office...">
                <span id="master-count" class="master-count"></span>
            </div>
            <div class="table-wrap"><table id="master-table">
                <thead><tr>
                    <th class="num">#</th>
                    <th>Agent</th>
                    <th>Office</th>
                    <th class="num">Total Sides</th>
                    <th class="num">Listing</th>
                    <th class="num">Buyer</th>
                    <th class="num">12-Mo</th>
                    <th class="num">Avg Price</th>
                    <th class="num">Total Volume</th>
                    <th class="num">Most Recent</th>
                    <th>Primary Towns</th>
                </tr></thead>
                <tbody id="master-body"></tbody>
            </table></div>
        </div>
    </div>
    <div id="agent-detail" class="agent-detail hidden"></div>
    <script id="redfin-index" type="application/json">{redfin_json}</script>
    <script id="zillow-index" type="application/json">{zillow_json}</script>
    <script id="maine-index" type="application/json">{maine_json}</script>
    <script id="agent-kpis" type="application/json">{agent_json}</script>
    <script id="brokerage-kpis" type="application/json">{brokerage_json}</script>
    <script>{_search_js()}</script>
    <script>{_leaderboard_js()}</script>
</body>
</html>'''


def _css() -> str:
    return '''
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
        --radius: 10px;
        --font: 'DM Sans', sans-serif;
        --mono: 'JetBrains Mono', monospace;
    }
    html, body { height: 100%; }
    body {
        font-family: var(--font);
        background: var(--bg-base);
        color: var(--text-1);
        display: flex;
        flex-direction: column;
    }
    .tab-bar {
        display: flex;
        align-items: center;
        gap: 4px;
        padding: 10px 24px;
        background: var(--bg-surface);
        border-bottom: 1px solid rgba(255,255,255,0.06);
        flex-shrink: 0;
        position: relative;
        z-index: 100;
    }
    .tab-bar .logo {
        font-size: 1.1rem;
        font-weight: 700;
        letter-spacing: -0.02em;
        color: var(--text-1);
        margin-right: 20px;
    }
    .tab-bar .logo span { color: var(--accent); }
    .tab {
        padding: 8px 18px;
        border-radius: var(--radius);
        font-size: 0.8rem;
        font-weight: 500;
        font-family: var(--mono);
        cursor: pointer;
        border: none;
        background: transparent;
        color: var(--text-3);
        transition: all 0.15s ease;
    }
    .tab:hover { color: var(--text-2); background: var(--bg-elevated); }
    .tab.active { color: var(--text-1); background: var(--bg-elevated); box-shadow: inset 0 -2px 0 var(--accent); }
    .search-wrap {
        margin-left: auto;
        position: relative;
    }
    #agent-search {
        width: 280px;
        padding: 8px 14px;
        border-radius: var(--radius);
        border: 1px solid rgba(255,255,255,0.08);
        background: var(--bg-elevated);
        color: var(--text-1);
        font-family: var(--font);
        font-size: 0.82rem;
        outline: none;
        transition: border-color 0.15s;
    }
    #agent-search:focus { border-color: var(--accent); }
    #agent-search::placeholder { color: var(--text-3); }
    .search-results {
        position: absolute;
        top: 100%;
        right: 0;
        width: 360px;
        max-height: 320px;
        overflow-y: auto;
        background: var(--bg-surface);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: var(--radius);
        margin-top: 4px;
        box-shadow: 0 8px 32px hsla(220,40%,4%,0.6);
    }
    .search-results.hidden { display: none; }
    .sr-item {
        padding: 10px 14px;
        cursor: pointer;
        border-bottom: 1px solid rgba(255,255,255,0.04);
        transition: background 0.1s;
    }
    .sr-item:hover { background: var(--bg-hover); }
    .sr-item:last-child { border-bottom: none; }
    .sr-name { font-weight: 600; font-size: 0.85rem; }
    .sr-office { font-size: 0.75rem; color: var(--text-2); }
    .sr-badge {
        display: inline-block;
        font-size: 0.65rem;
        font-family: var(--mono);
        padding: 1px 6px;
        border-radius: 4px;
        margin-left: 6px;
        vertical-align: middle;
    }
    .sr-badge.redfin { background: hsla(0,60%,50%,0.2); color: hsl(0,70%,65%); }
    .sr-badge.zillow { background: hsla(210,60%,50%,0.2); color: hsl(210,70%,65%); }
    .sr-badge.maine { background: hsla(140,60%,45%,0.2); color: hsl(140,70%,65%); }
    .archived-pill {
        display: inline-block;
        margin-left: 6px;
        padding: 1px 6px;
        border-radius: 4px;
        font-size: 0.55rem;
        font-family: var(--mono);
        background: rgba(255,255,255,0.06);
        color: var(--text-3);
        vertical-align: middle;
        letter-spacing: 0.04em;
        text-transform: uppercase;
    }
    .tab-content { flex: 1; position: relative; }
    .tab-content iframe {
        position: absolute; inset: 0; width: 100%; height: 100%;
        border: none; display: none;
    }
    .tab-content iframe.active { display: block; }
    .agent-detail {
        position: fixed; inset: 0;
        background: rgba(0,0,0,0.7);
        z-index: 200;
        display: flex;
        align-items: center;
        justify-content: center;
        backdrop-filter: blur(4px);
    }
    .agent-detail.hidden { display: none; }
    .detail-card {
        background: var(--bg-surface);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 14px;
        padding: 32px;
        max-width: 720px;
        width: 90vw;
        max-height: 85vh;
        overflow-y: auto;
        box-shadow: 0 16px 64px hsla(220,40%,4%,0.8);
    }
    .detail-card h2 { font-size: 1.3rem; font-weight: 700; margin-bottom: 4px; }
    .detail-card .office-line { font-size: 0.85rem; color: var(--text-2); margin-bottom: 20px; }
    .detail-card .source-label {
        font-size: 0.7rem; font-family: var(--mono); font-weight: 600;
        letter-spacing: 0.05em; text-transform: uppercase;
        color: var(--accent); margin: 18px 0 8px;
    }
    .detail-card table {
        width: 100%; border-collapse: collapse;
        font-size: 0.78rem; font-family: var(--mono);
    }
    .detail-card th {
        text-align: left; padding: 6px 10px;
        color: var(--text-3); font-weight: 500;
        border-bottom: 1px solid rgba(255,255,255,0.06);
    }
    .detail-card td {
        padding: 6px 10px;
        border-bottom: 1px solid rgba(255,255,255,0.03);
    }
    .detail-card td.num { text-align: right; }
    .detail-card .stat-row {
        display: flex; gap: 20px; flex-wrap: wrap; margin: 8px 0 12px;
    }
    .detail-card .stat-item {
        background: var(--bg-elevated); border-radius: 8px;
        padding: 10px 16px; min-width: 120px;
    }
    .detail-card .stat-item .label { font-size: 0.65rem; color: var(--text-3); font-family: var(--mono); }
    .detail-card .stat-item .val { font-size: 1.1rem; font-weight: 700; margin-top: 2px; }
    .close-btn {
        float: right; background: none; border: none; color: var(--text-3);
        font-size: 1.5rem; cursor: pointer; line-height: 1;
    }
    .close-btn:hover { color: var(--text-1); }
    .no-data { color: var(--text-3); font-size: 0.8rem; font-style: italic; }
    .master-tab {
        display: none; position: absolute; inset: 0;
        overflow-y: auto; padding: 20px 32px;
        background: var(--bg-base);
    }
    .master-tab.active { display: block; }
    .master-filters {
        display: flex; gap: 10px; align-items: center;
        margin-bottom: 16px; flex-wrap: wrap;
    }
    .master-filters select, .master-filters input {
        padding: 8px 12px; border-radius: var(--radius);
        border: 1px solid rgba(255,255,255,0.08);
        background: var(--bg-elevated); color: var(--text-1);
        font-family: var(--font); font-size: 0.82rem;
    }
    .master-filters select { min-width: 140px; }
    .master-filters input { min-width: 220px; }
    .master-count {
        font-size: 0.75rem; color: var(--text-3);
        font-family: var(--mono); margin-left: auto;
    }
    #master-table { width: 100%; border-collapse: collapse; font-size: 0.78rem; }
    #master-table th {
        position: sticky; top: 0; background: var(--bg-surface);
        padding: 8px 10px; text-align: left; color: var(--text-2);
        font-weight: 500; font-family: var(--mono); font-size: 0.72rem;
        border-bottom: 1px solid rgba(255,255,255,0.08); cursor: pointer;
        user-select: none;
    }
    #master-table th:hover { color: var(--text-1); }
    #master-table td {
        padding: 6px 10px; border-bottom: 1px solid rgba(255,255,255,0.03);
        font-family: var(--mono);
    }
    #master-table td.num { text-align: right; }
    #master-table tr:hover { background: var(--bg-hover); cursor: pointer; }
    #master-table .sort-arrow { margin-left: 4px; font-size: 0.65rem; color: var(--text-3); }
    #master-table th.sort-active .sort-arrow { color: var(--accent); }
    '''


def _leaderboard_js() -> str:
    """JS for the redesigned Leaderboard tab (Maine KPI-driven).

    Consumes #agent-kpis and #brokerage-kpis JSON, renders the master table,
    movers banner, and handles Agent/Brokerage toggle + Town filter +
    Period selector + in-table search.
    """
    return '/* Leaderboard JS — implemented in Task C2+ */'


def _search_js() -> str:
    return '''
    const redfin = JSON.parse(document.getElementById("redfin-index").textContent);
    const zillow = JSON.parse(document.getElementById("zillow-index").textContent);
    const maine = JSON.parse(document.getElementById("maine-index").textContent);
    const input = document.getElementById("agent-search");
    const results = document.getElementById("search-results");
    const detail = document.getElementById("agent-detail");

    // Tab switching
    document.querySelectorAll(".tab").forEach(btn => {
        btn.addEventListener("click", () => {
            document.querySelectorAll(".tab").forEach(b => b.classList.remove("active"));
            document.querySelectorAll(".tab-content iframe").forEach(f => f.classList.remove("active"));
            btn.classList.add("active");
            document.getElementById(btn.dataset.tab).classList.add("active");
        });
    });

    // Debounced search
    let timer;
    input.addEventListener("input", () => {
        clearTimeout(timer);
        timer = setTimeout(() => search(input.value.trim()), 200);
    });
    input.addEventListener("focus", () => { if (input.value.trim().length >= 2) search(input.value.trim()); });
    document.addEventListener("click", e => {
        if (!e.target.closest(".search-wrap")) results.classList.add("hidden");
    });

    function search(q) {
        if (q.length < 2) { results.classList.add("hidden"); return; }
        const ql = q.toLowerCase();
        const matches = [];

        redfin.forEach(a => {
            if ((a.name && a.name.toLowerCase().includes(ql)) || (a.office && a.office.toLowerCase().includes(ql)))
                matches.push({...a, _src: "redfin"});
        });
        zillow.forEach(a => {
            if ((a.name && a.name.toLowerCase().includes(ql)) || (a.office && a.office.toLowerCase().includes(ql)))
                matches.push({...a, _src: "zillow"});
        });
        maine.forEach(a => {
            if ((a.name && a.name.toLowerCase().includes(ql)) || (a.office && a.office.toLowerCase().includes(ql)))
                matches.push({...a, _src: "maine"});
        });

        if (!matches.length) {
            results.innerHTML = '<div class="sr-item"><span class="sr-name no-data">No results</span></div>';
            results.classList.remove("hidden");
            return;
        }

        // Dedupe display: group by name, show source badges
        const grouped = {};
        matches.forEach(m => {
            const key = (m.name || "").toLowerCase();
            if (!grouped[key]) grouped[key] = {name: m.name, office: m.office, sources: [], data: {}};
            grouped[key].sources.push(m._src);
            grouped[key].data[m._src] = m;
            if (m.office) grouped[key].office = m.office;
        });

        let html = "";
        Object.values(grouped).slice(0, 15).forEach(g => {
            const badges = [...new Set(g.sources)].map(s =>
                '<span class="sr-badge ' + s + '">' + s + '</span>'
            ).join("");
            html += '<div class="sr-item" data-key="' + esc(g.name) + '">' +
                '<div class="sr-name">' + esc(g.name) + badges + '</div>' +
                '<div class="sr-office">' + esc(g.office || "") + '</div></div>';
        });
        results.innerHTML = html;
        results.classList.remove("hidden");

        results.querySelectorAll(".sr-item").forEach(el => {
            el.addEventListener("click", () => {
                const key = el.dataset.key.toLowerCase();
                const g = grouped[key];
                if (g) showDetail(g);
            });
        });
    }

    function showDetail(g) {
        results.classList.add("hidden");
        let html = '<div class="detail-card"><button class="close-btn">&times;</button>';
        html += '<h2>' + esc(g.name) + '</h2>';
        html += '<div class="office-line">' + esc(g.office || "N/A") + '</div>';

        const rd = g.data.redfin;
        if (rd) {
            html += '<div class="source-label">Redfin &mdash; Transaction Data</div>';
            html += '<div class="stat-row">';
            html += stat("Total Sides", rd.total_sides);
            html += stat("Total Volume", fmtCur(rd.total_volume));
            html += stat("Avg Price", fmtCur(rd.avg_price));
            html += stat("365-Day Sides", rd.rolling_sides);
            html += stat("365-Day Volume", fmtCur(rd.rolling_volume));
            html += stat("Most Recent", rd.most_recent || "N/A");
            html += '</div>';
            if (rd.towns && Object.keys(rd.towns).length) {
                html += '<table><thead><tr><th>Town</th><th class="num">Sides</th><th class="num">Volume</th></tr></thead><tbody>';
                Object.entries(rd.towns).sort((a,b) => b[1].volume - a[1].volume).forEach(([t,d]) => {
                    html += '<tr><td>' + esc(t) + '</td><td class="num">' + d.sides + '</td><td class="num">' + fmtCur(d.volume) + '</td></tr>';
                });
                html += '</tbody></table>';
            }
        }

        const zd = g.data.zillow;
        if (zd) {
            html += '<div class="source-label">Zillow &mdash; Agent Profile</div>';
            html += '<div class="stat-row">';
            html += stat("Career Sales", zd.career_sales || zd.total_local_sales);
            html += stat("12-Mo Sales", zd.sales_12mo || "N/A");
            html += stat("Avg Price (3yr)", zd.avg_price ? fmtCur(zd.avg_price) : "N/A");
            html += stat("For Sale", zd.for_sale != null ? zd.for_sale : "N/A");
            html += stat("Type", zd.type || "N/A");
            html += '</div>';

            // Buyer/Seller breakdown from sold rows
            if (zd.sold_rows && zd.sold_rows.length) {
                const buyer = zd.sold_rows.filter(r => r.side === "Buyer").length;
                const seller = zd.sold_rows.filter(r => r.side === "Seller").length;
                const both = zd.sold_rows.filter(r => r.side === "Buyer and Seller").length;
                if (buyer + seller + both > 0) {
                    html += '<div class="stat-row">';
                    html += stat("Seller Sides", seller);
                    html += stat("Buyer Sides", buyer);
                    if (both) html += stat("Both Sides", both);
                    html += '</div>';
                }
            }

            // Per-town breakdown
            if (zd.towns && Object.keys(zd.towns).length) {
                html += '<table><thead><tr><th>Town</th><th class="num">Local Sales</th></tr></thead><tbody>';
                Object.entries(zd.towns).sort((a,b) => b[1] - a[1]).forEach(([t,c]) => {
                    html += '<tr><td>' + esc(t) + '</td><td class="num">' + c + '</td></tr>';
                });
                html += '</tbody></table>';
            }

            // Recent transactions
            if (zd.sold_rows && zd.sold_rows.length) {
                html += '<div class="source-label" style="margin-top:16px;">Recent Transactions</div>';
                html += '<table><thead><tr><th>When</th><th class="num">Price</th><th>Side</th><th>Location</th></tr></thead><tbody>';
                zd.sold_rows.forEach(r => {
                    html += '<tr><td>' + esc(r.date || "") + '</td><td class="num">' + fmtCur(r.price) +
                        '</td><td>' + esc(r.side || "") + '</td><td>' + esc(r.city || "") + '</td></tr>';
                });
                html += '</tbody></table>';
                if (zd.career_sales && zd.career_sales > zd.sold_rows.length) {
                    html += '<p class="no-data" style="margin-top:6px;">' +
                        (zd.career_sales - zd.sold_rows.length) + ' older transactions not shown</p>';
                }
            }

            if (zd.profile_url) {
                html += '<p style="margin-top:10px;font-size:0.75rem;"><a href="' + esc(zd.profile_url) +
                    '" target="_blank" style="color:var(--accent);">View on Zillow &rarr;</a></p>';
            }
        }

        const md = g.data.maine;
        if (md) {
            html += '<div class="source-label">Maine MLS &mdash; Closed Transactions</div>';
            html += '<div class="stat-row">';
            html += stat("Total Sides", md.total_sides);
            html += stat("Listing Sides", md.listing_sides);
            html += stat("Buyer Sides", md.buyer_sides);
            html += stat("Total Volume", fmtCur(md.volume));
            html += stat("Most Recent", md.most_recent || "N/A");
            html += '</div>';
            if (md.towns && md.towns.length) {
                html += '<div class="office-line" style="margin-top:10px;">Towns: ' +
                    esc(md.towns.join(", ")) + '</div>';
            }
        }

        if (!rd && !zd && !md) html += '<p class="no-data">No data found for this agent.</p>';
        html += '</div>';
        detail.innerHTML = html;
        detail.classList.remove("hidden");

        detail.querySelector(".close-btn").addEventListener("click", () => detail.classList.add("hidden"));
        detail.addEventListener("click", e => { if (e.target === detail) detail.classList.add("hidden"); });
    }

    function stat(label, val) {
        return '<div class="stat-item"><div class="label">' + label + '</div><div class="val">' + val + '</div></div>';
    }
    function fmtCur(n) {
        if (n == null || n === 0) return "$0";
        if (n >= 1e6) return "$" + (n/1e6).toFixed(1) + "M";
        if (n >= 1e3) return "$" + Math.round(n/1e3) + "K";
        return "$" + n.toLocaleString();
    }
    function esc(s) { const d = document.createElement("div"); d.textContent = s; return d.innerHTML; }

    // === MASTER TABLE ===
    const masterBody = document.getElementById("master-body");
    const filterTown = document.getElementById("filter-town");
    const filterType = document.getElementById("filter-type");
    const filterName = document.getElementById("filter-name");
    const masterCount = document.getElementById("master-count");

    // Build master dataset from Maine MLS (authoritative source of truth),
    // optionally enriched with Zillow data on exact name match (case-insensitive)
    // for team badge and 12-month recency indicator.
    const zillowByName = {};
    zillow.forEach(a => {
        if (a.name) zillowByName[a.name.toLowerCase().trim()] = a;
    });
    const masterData = maine.map(a => {
        const key = (a.name || "").toLowerCase().trim();
        const z = zillowByName[key];
        const totalSides = a.total_sides || 0;
        const volume = a.volume || 0;
        const avg = totalSides > 0 ? volume / totalSides : 0;
        return {
            name: a.name || "",
            office: a.office || "",
            type: z ? (z.type || "") : "",
            totalSides: totalSides,
            listing: a.listing_sides || 0,
            buyer: a.buyer_sides || 0,
            mo12: z ? (z.sales_12mo || 0) : 0,
            avg: avg,
            volume: volume,
            mostRecent: a.most_recent || "",
            towns: (a.towns || []).join(", "),
            townList: a.towns || [],
            _maine: a,
            _zillow: z,
        };
    });

    // Populate town filter
    const allTowns = new Set();
    masterData.forEach(a => a.townList.forEach(t => allTowns.add(t)));
    [...allTowns].sort().forEach(t => {
        const opt = document.createElement("option");
        opt.value = t; opt.textContent = t;
        filterTown.appendChild(opt);
    });

    function renderMaster() {
        const townVal = filterTown.value.toLowerCase();
        const typeVal = filterType.value.toLowerCase();
        const nameVal = filterName.value.toLowerCase().trim();

        let filtered = masterData.filter(a => {
            if (townVal && !a.townList.some(t => t.toLowerCase() === townVal)) return false;
            if (typeVal && a.type.toLowerCase() !== typeVal) return false;
            if (nameVal && !a.name.toLowerCase().includes(nameVal) && !a.office.toLowerCase().includes(nameVal)) return false;
            return true;
        });

        // Apply current sort
        if (masterSort.col >= 0) {
            filtered.sort((a, b) => {
                const va = masterSortVal(a, masterSort.col);
                const vb = masterSortVal(b, masterSort.col);
                if (typeof va === "string" && typeof vb === "string")
                    return masterSort.asc ? va.localeCompare(vb) : vb.localeCompare(va);
                return masterSort.asc ? va - vb : vb - va;
            });
        }

        let html = "";
        filtered.forEach((a, i) => {
            const teamBadge = a.type === "team" ? ' <span class="sr-badge zillow">team</span>' : "";
            html += '<tr data-idx="' + i + '">' +
                '<td class="num">' + (i+1) + '</td>' +
                '<td>' + esc(a.name) + teamBadge + '</td>' +
                '<td>' + esc(a.office) + '</td>' +
                '<td class="num">' + a.totalSides.toLocaleString() + '</td>' +
                '<td class="num">' + a.listing.toLocaleString() + '</td>' +
                '<td class="num">' + a.buyer.toLocaleString() + '</td>' +
                '<td class="num">' + (a.mo12 ? a.mo12 : "&mdash;") + '</td>' +
                '<td class="num">' + fmtCur(a.avg) + '</td>' +
                '<td class="num">' + fmtCur(a.volume) + '</td>' +
                '<td class="num">' + esc(a.mostRecent || "") + '</td>' +
                '<td>' + esc(a.towns) + '</td>' +
                '</tr>';
        });
        masterBody.innerHTML = html;
        masterCount.textContent = filtered.length + " of " + masterData.length + " agents";

        // Click row to show detail
        masterBody.querySelectorAll("tr").forEach(tr => {
            tr.addEventListener("click", () => {
                const idx = parseInt(tr.dataset.idx);
                const a = filtered[idx];
                if (!a) return;
                const sources = ["maine"];
                const data = {maine: a._maine};
                if (a._zillow) { sources.push("zillow"); data.zillow = a._zillow; }
                showDetail({name: a.name, office: a.office, sources: sources, data: data});
            });
        });
    }

    const masterSort = {col: 3, asc: false}; // Default: total sides desc
    function masterSortVal(a, col) {
        switch(col) {
            case 1: return a.name.toLowerCase();
            case 2: return a.office.toLowerCase();
            case 3: return a.totalSides || 0;
            case 4: return a.listing || 0;
            case 5: return a.buyer || 0;
            case 6: return a.mo12 || 0;
            case 7: return a.avg || 0;
            case 8: return a.volume || 0;
            case 9: return a.mostRecent || "";
            case 10: return a.towns.toLowerCase();
            default: return 0;
        }
    }

    // Sort headers
    document.querySelectorAll("#master-table thead th").forEach((th, idx) => {
        const arrow = document.createElement("span");
        arrow.className = "sort-arrow";
        arrow.textContent = idx === 3 ? "\u25BC" : "\u2195";
        th.appendChild(arrow);
        if (idx === 3) th.classList.add("sort-active");

        th.addEventListener("click", () => {
            const asc = masterSort.col === idx ? !masterSort.asc : false;
            masterSort.col = idx; masterSort.asc = asc;
            document.querySelectorAll("#master-table thead th").forEach(h => {
                h.classList.remove("sort-active");
                h.querySelector(".sort-arrow").textContent = "\u2195";
            });
            th.classList.add("sort-active");
            arrow.textContent = asc ? "\u25B2" : "\u25BC";
            renderMaster();
        });
    });

    filterTown.addEventListener("change", renderMaster);
    filterType.addEventListener("change", renderMaster);
    let nameTimer;
    filterName.addEventListener("input", () => { clearTimeout(nameTimer); nameTimer = setTimeout(renderMaster, 200); });

    // Initial render when tab is first shown
    let masterRendered = false;
    document.querySelectorAll(".tab").forEach(btn => {
        btn.addEventListener("click", () => {
            if (btn.dataset.tab === "master" && !masterRendered) {
                renderMaster();
                masterRendered = true;
            }
        });
    });
    '''
