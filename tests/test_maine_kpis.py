"""Tests for src/maine_kpis.py — KPI queries and mover computation."""
from __future__ import annotations

import pytest
from datetime import date, timedelta

from src.maine_database import get_connection, init_db
from src.maine_kpis import (
    PERIOD_12MO_DAYS,
    compute_cutoffs,
    compute_rank_movers,
    query_agent_kpis,
    query_brokerage_kpis,
)


# === Period cutoffs ===

class TestComputeCutoffs:
    def test_anchored_today_ignores_system_clock(self):
        c = compute_cutoffs('2026-04-16')
        assert c.current_12mo_start == '2025-04-16'
        assert c.prior_12mo_start == '2024-04-16'
        # 3 years × 365 days = 1095 days → 2023-04-17
        assert c.three_year_start == '2023-04-17'

    def test_none_today_uses_system(self):
        c = compute_cutoffs(None)
        # Just verify it returns something shaped correctly.
        assert len(c.current_12mo_start) == 10  # ISO date


# === Rank movers ===

class TestComputeRankMovers:
    def _row(self, name: str, current: int, prior: int) -> dict:
        return {
            'name': name,
            'current_12mo_sides': current,
            'prior_12mo_sides': prior,
        }

    def test_basic_riser(self):
        rows = [
            self._row('Alice', 50, 10),   # jumped up
            self._row('Bob', 40, 20),
            self._row('Charlie', 30, 30),
            self._row('Dan', 20, 40),     # dropped
            self._row('Eve', 10, 50),
        ]
        movers = compute_rank_movers(rows)
        # Alice: current rank 1, prior rank 3 (after Eve=50, Bob=40 prior descending)
        # prior desc: Eve(50), Bob(40), Dan(40)? Let's just check it ran
        riser_names = [r['name'] for r in movers['risers']]
        assert 'Alice' in riser_names

    def test_min_sides_threshold_excludes_low_volume(self):
        rows = [
            self._row('Alice', 3, 0),   # below threshold, excluded
            self._row('Bob', 20, 5),
            self._row('Charlie', 30, 10),
        ]
        movers = compute_rank_movers(rows, min_sides=5)
        all_names = [r['name'] for r in movers['risers'] + movers['fallers']]
        assert 'Alice' not in all_names

    def test_new_entity_no_prior_period(self):
        rows = [
            self._row('Alice', 50, 0),
            self._row('Bob', 30, 30),
            self._row('Charlie', 20, 40),
        ]
        movers = compute_rank_movers(rows)
        # Alice is NEW (prior=0)
        alice = next(r for r in movers['risers'] if r['name'] == 'Alice')
        assert alice['delta'] is None  # None = NEW

    def test_deltas_are_signed(self):
        rows = [
            self._row('Alice', 100, 10),
            self._row('Bob', 10, 100),
        ]
        movers = compute_rank_movers(rows)
        alice = next(r for r in movers['risers'] if r['name'] == 'Alice')
        bob = next(r for r in movers['fallers'] if r['name'] == 'Bob')
        # Alice moved up (current rank 1, prior rank 2) → delta = +1
        assert alice['delta'] == 1
        # Bob moved down (current rank 2, prior rank 1) → delta = -1
        assert bob['delta'] == -1

    def test_empty_input_returns_empty_lists(self):
        movers = compute_rank_movers([])
        assert movers['risers'] == []
        assert movers['fallers'] == []

    def test_top_n_caps_each_side(self):
        rows = [self._row(f'A{i}', 100 - i, i * 10) for i in range(20)]
        movers = compute_rank_movers(rows, top_n=3)
        assert len(movers['risers']) <= 3
        assert len(movers['fallers']) <= 3
