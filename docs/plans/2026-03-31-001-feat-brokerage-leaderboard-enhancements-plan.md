---
title: "feat: Enhance brokerage leaderboard with 365-day rolling, trends, and towns"
type: feat
status: active
date: 2026-03-31
---

# Enhance Brokerage Leaderboard

## Overview

Three changes to the brokerage section of the dashboard:

1. **Increase brokerage display limit** from 15 to 20 so Anchor Real Estate (currently #22 with 13 sides) and similar mid-tier brokerages appear. The brokerage-as-agent exclusion in agent queries is already working correctly — Anchor is excluded from agent rankings but included in brokerage rankings. The visibility issue is purely a display limit.

2. **Add a 365-day rolling brokerage leaderboard** with trend badges, mirroring the existing agent rolling leaderboard pattern.

3. **Add operating towns** to both brokerage tables (all-time and rolling), showing which towns each brokerage is active in.

## Problem Frame

The user has repeatedly flagged Anchor Real Estate as missing from the brokerage leaderboard. Investigation shows the exclusion logic is correct — Anchor IS in the brokerage query results at position #22, but the dashboard only displays top 15. Additionally, the brokerage section lacks the 365-day rolling view and trend indicators that the agent section already has, and has no geographic context for where brokerages operate.

## Requirements Trace

- R1. Anchor Real Estate (and similar brokerage-as-agent entries) must appear in brokerage rankings
- R2. Brokerage leaderboard shows all-time AND 365-day rolling rankings (two tables)
- R3. Rolling brokerage table includes trend badges (same pattern as agent trends)
- R4. Both brokerage tables include a "Towns" column showing top operating towns
- R5. Agent rankings continue to exclude brokerage-as-agent entries (no regression)

## Scope Boundaries

- No changes to agent leaderboard sections
- No changes to enrichment pipeline or scraper logic
- No changes to database schema
- CSS-only additions for new table columns; no design overhaul

## Key Technical Decisions

- **Increase brokerage limit to 20:** Anchor is at #22 currently but will rise as more data is enriched. Limit of 20 ensures reasonable coverage. Both all-time and rolling use limit=20.
- **Reuse `_compute_trend_indicators` pattern:** The existing function is agent-specific (keyed on `agent_name`). Rather than generalizing it, create a parallel `_compute_brokerage_trends` that keys on `office` — simpler, no risk of breaking agent trends.
- **Add towns subquery to `_query_top_brokerages`:** Mirror the `primary_towns` pattern from `_query_top_agents` — GROUP_CONCAT of top 3 cities by transaction count.
- **`since_date` parameter for brokerages:** Add the same `since_date` optional parameter that `_query_top_agents` already supports.

## Implementation Units

- [ ] **Unit 1: Add `since_date` and `towns` to brokerage query**

  **Goal:** Enable date-filtered brokerage queries and include operating towns in results.

  **Requirements:** R2, R4

  **Dependencies:** None

  **Files:**
  - Modify: `src/report.py` — `_query_top_brokerages()` and `query_top_brokerages()`
  - Test: `tests/test_report.py`

  **Approach:**
  - Add `since_date: str | None = None` parameter to `_query_top_brokerages()`, same pattern as `_query_top_agents()`
  - Add `primary_towns` subquery: `SELECT GROUP_CONCAT(city, ', ') FROM (SELECT city, COUNT(*) as cnt FROM transactions t2 WHERE t2.listing_office = t.listing_office AND t2.city IS NOT NULL GROUP BY city ORDER BY cnt DESC LIMIT 3)`
  - Update `query_top_brokerages()` wrapper to pass through `since_date`
  - Add `towns` key to returned dict

  **Patterns to follow:**
  - `_query_top_agents()` lines 181-226 — same `date_filter` and `primary_towns` pattern

  **Test scenarios:**
  - Brokerage query with `since_date` returns fewer or equal results vs no filter
  - Brokerage query without `since_date` returns same results as before (backward compat)
  - Returned dicts include `towns` key with comma-separated city names
  - Anchor Real Estate appears in results (not filtered out)

  **Verification:**
  - All existing tests pass
  - New tests pass for date filter and towns field

- [ ] **Unit 2: Add brokerage trend computation to dashboard**

  **Goal:** Compute rank-change trends for brokerages, paralleling the agent trend system.

  **Requirements:** R3

  **Dependencies:** Unit 1

  **Files:**
  - Modify: `src/dashboard.py` — add `_compute_brokerage_trends()`, update `generate_dashboard()`
  - Test: `tests/test_dashboard.py`

  **Approach:**
  - Add `_compute_brokerage_trends(all_time, rolling)` function keyed on `office` name instead of `agent_name`
  - Same logic: `rank_change = all_time_rank - rolling_rank`, `is_new` for entries not in all-time top
  - In `generate_dashboard()`: query rolling brokerages with `since_date`, compute trends
  - Reuse existing `_render_trend_badge()` for badge HTML

  **Patterns to follow:**
  - `_compute_trend_indicators()` in dashboard.py lines 63-90

  **Test scenarios:**
  - Brokerage that improved rank shows positive `rank_change`
  - Brokerage that declined shows negative `rank_change`
  - New brokerage (in rolling but not all-time) shows `is_new: True`
  - Unchanged rank shows `rank_change: 0`

  **Verification:**
  - Trend computation tests pass
  - `generate_dashboard()` produces HTML with brokerage trend badges

- [ ] **Unit 3: Update dashboard HTML — two brokerage sections with towns and trends**

  **Goal:** Render all-time and rolling brokerage tables with towns column and trend badges.

  **Requirements:** R1, R2, R3, R4

  **Dependencies:** Units 1, 2

  **Files:**
  - Modify: `src/dashboard.py` — `_build_html()`
  - Test: `tests/test_dashboard.py`

  **Approach:**
  - Split current single brokerage section into two: "Top Brokerages — All-Time" and "Top Brokerages — Last 365 Days"
  - All-time table columns: #, Brokerage, Sides, Volume, Avg Price, Towns (add towns, keep existing columns)
  - Rolling table columns: #, Brokerage, Sides, Volume, Towns, Trend (replace Avg Price with Trend badge)
  - Update `colgroup` percentages to accommodate the new Towns column
  - Both tables use `limit=20`
  - Add `towns` class to towns `<td>` cells (reuses existing `.towns` CSS)
  - Update `_build_html()` signature to accept `rolling_brokerages` and `brokerage_trends`
  - Add animation delays for the new section (increment by 80ms)

  **Patterns to follow:**
  - Agent rolling section (Section 2) in `_build_html()` — same table structure with trend badge column
  - Town column rendering from agent all-time section

  **Test scenarios:**
  - Generated HTML contains both "Top Brokerages — All-Time" and "Top Brokerages — Last 365 Days"
  - Brokerage tables include Towns column data
  - Rolling brokerage table includes trend badges
  - Empty database produces valid HTML with "No data" messages for both sections

  **Verification:**
  - Dashboard renders correctly in browser with 5 sections (was 4)
  - Anchor Real Estate visible in brokerage tables (with limit=20)
  - Towns column shows geographic distribution
  - Trend badges colored correctly

- [ ] **Unit 4: Update tests and docs**

  **Goal:** Ensure full test coverage and documentation accuracy.

  **Requirements:** R5

  **Dependencies:** Units 1-3

  **Files:**
  - Modify: `tests/test_report.py`
  - Modify: `tests/test_dashboard.py`
  - Modify: `CLAUDE.md`

  **Approach:**
  - Add test: brokerage `since_date` filter works
  - Add test: brokerage results include `towns` key
  - Add test: brokerage trend computation (reuse pattern from `TestComputeTrendIndicators`)
  - Add test: HTML output contains both brokerage sections
  - Verify existing `TestBrokerageAsAgentExclusion` still passes (R5 regression check)
  - Update CLAUDE.md to document 5-section dashboard layout

  **Test scenarios:**
  - `test_brokerage_since_date_filter` — fewer results when date-filtered
  - `test_brokerage_results_include_towns` — towns field present and non-empty
  - `test_brokerage_trend_computation` — rank changes computed correctly
  - `test_dashboard_has_two_brokerage_sections` — both section headings in HTML
  - Existing exclusion tests unchanged and passing

  **Verification:**
  - `python -m pytest tests/` — all tests pass
  - `python3 -m src.main --report-only` — generates dashboard with 5 sections
  - Visual check in browser confirms layout

## System-Wide Impact

- **Dashboard section count:** 4 → 5 (new rolling brokerage section)
- **Animation delays:** Need to increment for the additional section
- **CI commit size:** Dashboard HTML will be slightly larger due to extra table
- **No API or schema changes**

## Risks & Dependencies

- **Merge conflicts with CI:** Dashboard HTML is regenerated by CI every 6 hours. Changes should be committed and pushed before the next CI run to avoid conflicts.
- **Column width tuning:** The Towns column addition may need width adjustments after visual review.

## Sources & References

- Existing pattern: agent rolling leaderboard + trends in `src/dashboard.py`
- Existing pattern: `since_date` parameter in `_query_top_agents()`
- Current data: Anchor Real Estate at brokerage rank #22 (13 sides, $12.8M)
