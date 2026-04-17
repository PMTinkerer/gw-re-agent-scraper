"""KPI queries for Maine MLS transactions.

Provides one row per agent (or brokerage) with metrics across four rolling
time windows: last-12mo, prior-12mo, last-3yr, all-time. Consumed by the
static dashboard (maine_dashboard.py) and interactive index tab
(index_page.py).

Period cutoffs are computed against a caller-supplied `today` ISO date string
(or date('now') if omitted). Tests pass a fixed today for determinism.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional

# Reuse existing exclusion constants from maine_report to avoid drift.
from .maine_report import _AGENT_EXCLUSIONS


PERIOD_12MO_DAYS = 365
PERIOD_3YR_DAYS = 365 * 3


@dataclass(frozen=True)
class PeriodCutoffs:
    """ISO date strings used as SQL parameters for each rolling window."""
    current_12mo_start: str  # close_date >= this
    prior_12mo_start: str    # close_date >= this AND < current_12mo_start
    three_year_start: str    # close_date >= this


def compute_cutoffs(today: Optional[str] = None) -> PeriodCutoffs:
    """Return ISO strings for the period boundaries relative to `today`.

    If `today` is None, uses date.today().
    """
    anchor = date.fromisoformat(today) if today else date.today()
    return PeriodCutoffs(
        current_12mo_start=(anchor - timedelta(days=PERIOD_12MO_DAYS)).isoformat(),
        prior_12mo_start=(anchor - timedelta(days=PERIOD_12MO_DAYS * 2)).isoformat(),
        three_year_start=(anchor - timedelta(days=PERIOD_3YR_DAYS)).isoformat(),
    )


def query_agent_kpis(
    conn: sqlite3.Connection,
    *,
    town: Optional[str] = None,
    limit: Optional[int] = None,
    today: Optional[str] = None,
) -> list[dict]:
    """One row per unique agent name with all period metrics."""
    cutoffs = compute_cutoffs(today)
    exclusions = list(_AGENT_EXCLUSIONS)
    excl_placeholders = ','.join(['(?)'] * len(exclusions))

    town_sql_listing = ''
    town_sql_buyer = ''
    town_params: list = []
    if town:
        town_sql_listing = 'AND LOWER(city) = LOWER(?)'
        town_sql_buyer = 'AND LOWER(city) = LOWER(?)'
        town_params = [town, town]

    # Build a union of listing-side and buyer-side rows so each appearance
    # of an agent counts once. Then aggregate all period metrics in one pass.
    sql = f'''
        WITH sides AS (
            SELECT listing_agent AS agent,
                   listing_office AS office,
                   'listing' AS role,
                   sale_price, city, close_date
            FROM maine_transactions
            WHERE enrichment_status = 'success'
              AND listing_agent IS NOT NULL AND TRIM(listing_agent) != ''
              {town_sql_listing}
            UNION ALL
            SELECT buyer_agent AS agent,
                   buyer_office AS office,
                   'buyer' AS role,
                   sale_price, city, close_date
            FROM maine_transactions
            WHERE enrichment_status = 'success'
              AND buyer_agent IS NOT NULL AND TRIM(buyer_agent) != ''
              {town_sql_buyer}
        ),
        excluded(agent_lower) AS (VALUES {excl_placeholders})
        SELECT
            s.agent AS name,
            (
                SELECT office FROM sides s2
                WHERE s2.agent = s.agent AND s2.office IS NOT NULL
                GROUP BY office ORDER BY COUNT(*) DESC LIMIT 1
            ) AS office,
            SUM(CASE WHEN s.close_date >= ? THEN 1 ELSE 0 END) AS current_12mo_sides,
            SUM(CASE WHEN s.close_date >= ? THEN COALESCE(s.sale_price, 0) ELSE 0 END) AS current_12mo_volume,
            SUM(CASE WHEN s.close_date >= ? AND s.close_date < ? THEN 1 ELSE 0 END) AS prior_12mo_sides,
            SUM(CASE WHEN s.close_date >= ? AND s.close_date < ? THEN COALESCE(s.sale_price, 0) ELSE 0 END) AS prior_12mo_volume,
            SUM(CASE WHEN s.close_date >= ? THEN 1 ELSE 0 END) AS three_yr_sides,
            SUM(CASE WHEN s.close_date >= ? THEN COALESCE(s.sale_price, 0) ELSE 0 END) AS three_yr_volume,
            COUNT(*) AS all_time_sides,
            SUM(COALESCE(s.sale_price, 0)) AS all_time_volume,
            SUM(CASE WHEN s.role = 'listing' THEN 1 ELSE 0 END) AS listing_sides,
            SUM(CASE WHEN s.role = 'buyer' THEN 1 ELSE 0 END) AS buyer_sides,
            MAX(s.close_date) AS most_recent,
            (
                SELECT GROUP_CONCAT(city, ', ') FROM (
                    SELECT city, COUNT(*) AS cnt FROM sides s3
                    WHERE s3.agent = s.agent AND s3.city IS NOT NULL
                    GROUP BY city ORDER BY cnt DESC LIMIT 3
                )
            ) AS primary_towns
        FROM sides s
        WHERE LOWER(s.agent) NOT IN (SELECT agent_lower FROM excluded)
        GROUP BY s.agent
        ORDER BY current_12mo_volume DESC, all_time_volume DESC
    '''

    # town_params is [town, town] when town is set (one value per UNION half),
    # else empty. Exclusions fill the VALUES clause. Cutoffs feed the
    # CASE WHEN aggregations in column order.
    params = town_params + exclusions + [
        cutoffs.current_12mo_start,                                  # current sides
        cutoffs.current_12mo_start,                                  # current volume
        cutoffs.prior_12mo_start, cutoffs.current_12mo_start,        # prior sides window
        cutoffs.prior_12mo_start, cutoffs.current_12mo_start,        # prior volume window
        cutoffs.three_year_start,                                    # 3yr sides
        cutoffs.three_year_start,                                    # 3yr volume
    ]

    if limit is not None:
        sql += ' LIMIT ?'
        params.append(limit)

    rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def query_brokerage_kpis(
    conn: sqlite3.Connection,
    *,
    town: Optional[str] = None,
    limit: Optional[int] = None,
    today: Optional[str] = None,
) -> list[dict]:
    """One row per brokerage. Same period columns as query_agent_kpis plus:
    - agent_count (# distinct agent names at that brokerage)
    - top_agents (comma-separated top 3 agents by count at that brokerage)

    Brokerages are kept at branch level — no normalization.
    """
    cutoffs = compute_cutoffs(today)

    town_sql_listing = ''
    town_sql_buyer = ''
    town_params: list = []
    if town:
        town_sql_listing = 'AND LOWER(city) = LOWER(?)'
        town_sql_buyer = 'AND LOWER(city) = LOWER(?)'
        town_params = [town, town]

    sql = f'''
        WITH sides AS (
            SELECT listing_office AS office,
                   listing_agent AS agent,
                   'listing' AS role,
                   sale_price, city, close_date
            FROM maine_transactions
            WHERE enrichment_status = 'success'
              AND listing_office IS NOT NULL AND TRIM(listing_office) != ''
              {town_sql_listing}
            UNION ALL
            SELECT buyer_office AS office,
                   buyer_agent AS agent,
                   'buyer' AS role,
                   sale_price, city, close_date
            FROM maine_transactions
            WHERE enrichment_status = 'success'
              AND buyer_office IS NOT NULL AND TRIM(buyer_office) != ''
              {town_sql_buyer}
        )
        SELECT
            s.office AS name,
            COUNT(DISTINCT s.agent) AS agent_count,
            SUM(CASE WHEN s.close_date >= ? THEN 1 ELSE 0 END) AS current_12mo_sides,
            SUM(CASE WHEN s.close_date >= ? THEN COALESCE(s.sale_price, 0) ELSE 0 END) AS current_12mo_volume,
            SUM(CASE WHEN s.close_date >= ? AND s.close_date < ? THEN 1 ELSE 0 END) AS prior_12mo_sides,
            SUM(CASE WHEN s.close_date >= ? AND s.close_date < ? THEN COALESCE(s.sale_price, 0) ELSE 0 END) AS prior_12mo_volume,
            SUM(CASE WHEN s.close_date >= ? THEN 1 ELSE 0 END) AS three_yr_sides,
            SUM(CASE WHEN s.close_date >= ? THEN COALESCE(s.sale_price, 0) ELSE 0 END) AS three_yr_volume,
            COUNT(*) AS all_time_sides,
            SUM(COALESCE(s.sale_price, 0)) AS all_time_volume,
            SUM(CASE WHEN s.role = 'listing' THEN 1 ELSE 0 END) AS listing_sides,
            SUM(CASE WHEN s.role = 'buyer' THEN 1 ELSE 0 END) AS buyer_sides,
            MAX(s.close_date) AS most_recent,
            (
                SELECT GROUP_CONCAT(agent, ', ') FROM (
                    SELECT agent, COUNT(*) AS cnt FROM sides s2
                    WHERE s2.office = s.office AND s2.agent IS NOT NULL
                    GROUP BY agent ORDER BY cnt DESC LIMIT 3
                )
            ) AS top_agents,
            (
                SELECT GROUP_CONCAT(city, ', ') FROM (
                    SELECT city, COUNT(*) AS cnt FROM sides s3
                    WHERE s3.office = s.office AND s3.city IS NOT NULL
                    GROUP BY city ORDER BY cnt DESC LIMIT 3
                )
            ) AS primary_towns
        FROM sides s
        GROUP BY s.office
        ORDER BY current_12mo_volume DESC, all_time_volume DESC
    '''

    params = town_params + [
        cutoffs.current_12mo_start,
        cutoffs.current_12mo_start,
        cutoffs.prior_12mo_start, cutoffs.current_12mo_start,
        cutoffs.prior_12mo_start, cutoffs.current_12mo_start,
        cutoffs.three_year_start,
        cutoffs.three_year_start,
    ]

    if limit is not None:
        sql += ' LIMIT ?'
        params.append(limit)

    rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def compute_rank_movers(
    rows: list[dict],
    *,
    current_field: str = 'current_12mo_sides',
    prior_field: str = 'prior_12mo_sides',
    min_sides: int = 5,
    top_n: int = 5,
) -> dict:
    """Given KPI rows, compute rank deltas and return risers/fallers.

    Rank is 1-based on the respective period's sides descending. Delta is
    prior_rank - current_rank (positive = moved up).

    NEW entities (prior sides = 0) get delta = None and are placed in risers.
    Entities with current_sides < min_sides are excluded from both lists.

    Returns {'risers': [...], 'fallers': [...]}.
    """
    if not rows:
        return {'risers': [], 'fallers': []}

    # Rank by current period (desc)
    current_sorted = sorted(rows, key=lambda r: r.get(current_field) or 0, reverse=True)
    current_rank = {r['name']: i + 1 for i, r in enumerate(current_sorted)}

    # Rank by prior period (desc), but only among entities with prior > 0
    prior_eligible = [r for r in rows if (r.get(prior_field) or 0) > 0]
    prior_sorted = sorted(prior_eligible, key=lambda r: r.get(prior_field) or 0, reverse=True)
    prior_rank = {r['name']: i + 1 for i, r in enumerate(prior_sorted)}

    enriched = []
    for r in rows:
        name = r['name']
        current_sides = r.get(current_field) or 0
        prior_sides = r.get(prior_field) or 0
        if current_sides < min_sides:
            continue
        if name in prior_rank:
            delta = prior_rank[name] - current_rank[name]
        else:
            delta = None  # NEW
        enriched.append({**r, 'delta': delta, 'current_rank': current_rank[name]})

    # NEW entities go to risers (sorted by current sides desc as tiebreaker)
    news = [e for e in enriched if e['delta'] is None]
    news.sort(key=lambda e: e.get(current_field) or 0, reverse=True)

    # Positive deltas = risers, negative = fallers
    positive = [e for e in enriched if e['delta'] is not None and e['delta'] > 0]
    negative = [e for e in enriched if e['delta'] is not None and e['delta'] < 0]
    positive.sort(key=lambda e: e['delta'], reverse=True)
    negative.sort(key=lambda e: e['delta'])  # most negative first

    risers = (news + positive)[:top_n]
    fallers = negative[:top_n]

    return {'risers': risers, 'fallers': fallers}
