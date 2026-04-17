# Maine MLS Leaderboard Redesign — Design Spec

**Date:** 2026-04-16
**Author:** Lucas Knowles (via brainstorm w/ Claude)
**Status:** Approved design, pending implementation plan

## Problem

The current "All Agents" tab ranks agents by a single period and mixes
Maine/Zillow data awkwardly. We have 16,024 enriched Maine MLS transactions
(2011–2026) but no way to answer the questions the user actually cares about:

- Who dominates all-time vs. right now?
- Who's rising fastest? Who's fading?
- Who owns which town?
- Same questions, but for brokerages.

## Intent (what the user wants to understand)

Market intelligence for southern-coastal-Maine real-estate activity. Specific
use cases: vendor selection, competitive positioning, recruiting
conversations, negotiation leverage. The user is an STR operator with peripheral
exposure to the agent market, not a realtor — so they need the "shape" of the
market fast, not deep transactional detail.

## Architecture

### Information architecture

Rename the existing **"All Agents"** tab to **"Leaderboard"** and make it the
interactive workhorse. The existing standalone Maine MLS tab (static HTML
dashboard) stays for export/sharing.

Inside the Leaderboard tab:

```
┌─────────────────────────────────────────────────────────────┐
│ [Agents] [Brokerages]  Town: [All ▾]  Period: [12mo ▾]  🔍 │
├─────────────────────────────────────────────────────────────┤
│ 🔥 Biggest Movers (12mo vs prior-12mo)        [collapse ▾] │
│  ▲ Top 5 risers        │       ▼ Top 5 fallers             │
├─────────────────────────────────────────────────────────────┤
│ Main leaderboard table (top 50 when town filtered)          │
└─────────────────────────────────────────────────────────────┘
```

### Approach

Approach 2 from brainstorm: **single enhanced table + Movers banner**. Not
multi-page subviews and not a map-heavy dashboard. All key info in one scroll,
movement emphasis via a top banner.

### Entities

Both **Agents** and **Brokerages** get the same treatment. A toggle at the
top swaps the entity. The banner, table, and detail modal all respect the
toggle. Columns are nearly identical — one differs (see below).

Brokerages stay at branch level (e.g., "Coldwell Banker Yorke Realty" ≠
"Coldwell Banker Realty") — consistent with prior user direction.

### Time periods

Rolling windows anchored to `date('now')`:

- **12-month** (aka current): `close_date >= date('now', '-1 year')`
- **Prior 12-month**: `close_date >= date('now', '-2 years') AND close_date < date('now', '-1 year')`
- **3-year**: `close_date >= date('now', '-3 years')`
- **All-time**: no cutoff (back to 2011-02-23)

## Main table columns

Ordered by scan priority (left to right):

| # | Column        | Source/Formula                              | Notes                               |
|---|---------------|---------------------------------------------|-------------------------------------|
| 1 | `#`           | position in sorted view                     |                                     |
| 2 | Agent/Brokerage | name                                      | clickable                           |
| 3 | Office / Agents | agents: `listing_office` (most common). brokerages: `agent_count` (# distinct agents at that brokerage) | column label swaps with toggle |
| 4 | **12mo Δ**    | rank delta (current 12mo rank − prior 12mo rank) | `↑7` / `↓3` / `NEW` / `—`      |
| 5 | 12mo Vol      | `SUM(sale_price)` last 365d                 | current velocity                    |
| 6 | 12mo Sides    | `COUNT(*)` last 365d                        | activity intensity                  |
| 7 | 3yr Vol       | `SUM(sale_price)` last 3 years              | sustained performance               |
| 8 | All-Time Vol  | `SUM(sale_price)` 2011→now                  | career scale                        |
| 9 | All-Time Sides | `COUNT(*)` 2011→now                         | career activity                     |
| 10 | L / B        | `listing_sides : buyer_sides` (all-time)    | rendered as single cell "47 : 12"   |
| 11 | Avg Price    | `3yr vol ÷ 3yr sides`                        | 3yr window (not all-time — avoids 2011 price distortion) |
| 12 | Most Recent  | `MAX(close_date)`                           | freshness                           |

### Defaults

- **Default sort:** `12mo Vol` descending.
- **Tiebreaker:** `All-Time Vol` descending.
- Click any column header to re-sort. Sort state persists within session only.

### Top-50 rule

When `Town = All`, show all qualifying agents/brokerages (no cap).
When a specific town is selected, cap at top 50 of that town.

### Formatting

Volumes: `$1.2M` / `$500K` / `$250`. No raw numbers.
Dates: `YYYY-MM-DD`.
Rank deltas: color-coded — green ↑, red ↓, neutral NEW, gray —.

## Biggest Movers banner

**Location:** between filters and table.
**Collapsible** via chevron.
**Respects** active Agent/Brokerage toggle and Town filter.

### Qualification rules

- Entity must have **≥5 sides in the last 12 months** (noise filter)
- If < 10 qualifying entities exist for the current view, banner auto-hides
  silently (prevents meaningless "top 5 of 6" scenarios)

### What each card shows

```
↑14  Jane Doe          Keller W.
     12mo: $42M (vs $12M)  +250%
```

- Rank delta (or `NEW` if no prior-period activity)
- Name + office
- 12mo volume (vs prior 12mo) + % volume change

### Card count

5 risers, 5 fallers. If fewer than 5 qualify on either side, just show what
qualifies (don't pad).

### Interaction

Card click → opens same detail modal as a table-row click.

## Filters and controls

| Control            | Behavior                                                          |
|--------------------|-------------------------------------------------------------------|
| Agent/Brokerage toggle | Binary pill. Instant swap of banner + table.                  |
| Town dropdown      | "All Towns" default + 10 towns. Re-scopes banner + table. Caps table to top 50 when a town is selected. |
| Period selector    | `12mo` / `3yr` / `All-time`. Changes (a) the default sort column, (b) the movers banner comparison window, and (c) the rank-delta column header (always reads `{period} Δ`). Other columns stay fixed. For `All-time`, there is no prior period — movers banner hides and Δ shows `—` for everyone. |
| In-table search    | Live filter on name OR office substring (case-insensitive). Filters rows; does not open modal. |

Global nav search (existing) continues to work as a cross-source unified search
with a modal result card. The two search features coexist; they solve
different problems.

## Detail modal

Reuses the existing detail-card DOM with an augmented layout for Maine data.
Shown for both row clicks and banner-card clicks.

Content (for a Maine agent):

```
Jane Doe  [team badge if zillow-matched]
Keller Williams Coastal — Kennebunk

Last 12mo:   52 sides  |  $42M volume  |  avg $808K  |  ↑14 from prior 12mo
Prior 12mo:  38 sides  |  $12M volume  |  avg $316K
3-year:      128 sides |  $71M volume
All-time:    215 sides |  $98M volume  |  since 2014-03-14

Split:       L 94  /  B 121 (buyer-heavy)
Primary towns: Kennebunk, Kittery, York
Most recent:   2026-04-12

[Zillow overlay if matched: team, 12-mo count, reviews]
```

For brokerages, the "Split" row is replaced by `Agents: N` and an inline list
of top agents within that brokerage. Otherwise identical.

## Data layer

### New module responsibilities (high-level only — plan phase will enumerate)

New or expanded functions in `src/maine_report.py`:

- `query_agent_kpis(conn, *, town=None, limit=None)`: returns one row per
  agent with all period metrics (12mo vol/sides, 3yr vol/sides, all-time
  vol/sides, listing_sides, buyer_sides, avg_price_3yr, most_recent,
  primary_office, primary_towns).
- `query_brokerage_kpis(conn, *, town=None, limit=None)`: same shape,
  aggregated at listing_office+buyer_office union, with `agent_count` and
  `top_agents` rolled up.
- `compute_rank_movers(kpi_rows, *, current_field, prior_field, min_sides=5)`:
  pure Python helper that takes a list of KPI rows and returns rank deltas
  and the riser/faller lists.
- Period constants live at module top (e.g., `PERIOD_12MO_CUTOFF`) so queries
  and movers stay in sync.

### Consumers

- `src/maine_dashboard.py`: static HTML dashboard — will render the new
  leaderboards and movers banner server-side using the helpers above.
- `src/index_page.py`: interactive tab — embeds agent + brokerage KPI
  datasets as JSON script tags, renders via client-side JS. Sort/search/filter
  all happen in-browser.

### Exclusions

Reuse the existing `_AGENT_EXCLUSIONS` frozenset from `maine_report.py` (NON-MREIS
placeholder, brokerage-as-agent names). Extend if new pollutants surface.

## Error handling

- Empty period → `—` in the cell. Never render `$0` where the metric doesn't
  exist (that's misleading).
- Missing prior-period data → `NEW` tag in rank delta cell.
- DB unavailable → the existing graceful fallback (empty leaderboard with a
  "no data yet" note) applies.

## Testing

- Unit tests in `tests/test_maine_report.py` (extend): `query_agent_kpis`,
  `query_brokerage_kpis`, `compute_rank_movers` — verify period windowing,
  rank delta math, NEW tagging, qualification threshold.
- Regression: all existing 33 tests must still pass.

## What this replaces

- The current "master table" JS in `src/index_page.py` (currently uses Maine
  data but with a fixed small column set and no movement tracking) gets
  rewritten.
- The existing Maine dashboard's agent/brokerage sections (`src/maine_dashboard.py`)
  will be rebuilt to use the new KPI queries and add the movers banner.

## Out of scope (explicit non-goals)

- Map view showing town dominance (punted; potential future tab)
- Sparklines / trend charts per agent (punted; could be a detail-modal enhancement later)
- Export-to-CSV button (not requested)
- Reconciliation between Maine and Zillow "career sides" numbers (they don't
  match and we're going with Maine as authoritative — Zillow team badge +
  12mo count only)

## Open questions

None — design confirmed section-by-section during brainstorm.
