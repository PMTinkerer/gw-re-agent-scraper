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
    """One row per agent with all period metrics. Agents = both listing and
    buyer sides unioned together."""
    raise NotImplementedError  # Task A3


def query_brokerage_kpis(
    conn: sqlite3.Connection,
    *,
    town: Optional[str] = None,
    limit: Optional[int] = None,
    today: Optional[str] = None,
) -> list[dict]:
    """One row per brokerage (listing_office + buyer_office union, kept at
    branch level)."""
    raise NotImplementedError  # Task A4


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
