"""Durable reservations are conservative allowances, never reported charges."""

from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import multiprocessing
from datetime import datetime, timedelta, timezone
import sqlite3

import pytest

from src.refresh_budget import BudgetExceeded, BudgetLedger


def test_daily_cap_persists_after_restart_and_failed_requests(tmp_path):
    path = tmp_path / "budget.db"
    options = dict(daily_limit=10, read_balance=lambda: 1000)
    BudgetLedger(path, **options).reserve("run1", "summary", "york/1")
    BudgetLedger(path, **options).reserve("run2", "summary", "york/1")
    with pytest.raises(BudgetExceeded):
        BudgetLedger(path, **options).reserve("run3", "summary", "york/1")
    with sqlite3.connect(path) as conn:
        assert (
            conn.execute("SELECT SUM(reserved_units) FROM reservations").fetchone()[0]
            == 10
        )


def test_rolling_limit_survives_utc_day_boundary(tmp_path):
    clock = [datetime(2026, 9, 26, 23, 59, tzinfo=timezone.utc)]
    ledger = BudgetLedger(
        tmp_path / "b.db",
        daily_limit=5,
        rolling_limit=10,
        read_balance=lambda: 1000,
        now=lambda: clock[0],
    )
    ledger.reserve("1", "summary", "1")
    clock[0] += timedelta(minutes=2)
    ledger.reserve("2", "summary", "1")
    clock[0] += timedelta(days=1)
    with pytest.raises(BudgetExceeded):
        ledger.reserve("3", "summary", "1")
    clock[0] += timedelta(days=31)
    ledger.reserve("4", "summary", "1")


@pytest.mark.parametrize("balance", [None, 0, 4, "500", True, float("nan")])
def test_unknown_or_low_balance_fails_closed(tmp_path, balance):
    ledger = BudgetLedger(tmp_path / "b.db", read_balance=lambda: balance)
    with pytest.raises(BudgetExceeded):
        ledger.reserve("r", "summary", "1")


def test_balance_exception_fails_closed(tmp_path):
    def unavailable():
        raise TimeoutError("offline")

    ledger = BudgetLedger(tmp_path / "b.db", read_balance=unavailable)
    with pytest.raises(BudgetExceeded):
        ledger.reserve("r", "summary", "1")


def test_concurrent_reservations_cannot_overrun_cap(tmp_path):
    path = tmp_path / "b.db"
    BudgetLedger(path, daily_limit=20, read_balance=lambda: 1000)

    def reserve(index):
        try:
            BudgetLedger(path, daily_limit=20, read_balance=lambda: 1000).reserve(
                str(index), "summary", "1"
            )
            return True
        except BudgetExceeded:
            return False

    with ThreadPoolExecutor(max_workers=8) as pool:
        assert sum(pool.map(reserve, range(20))) == 4


def test_bad_units_and_naive_clock_rejected(tmp_path):
    with pytest.raises(ValueError):
        BudgetLedger(tmp_path / "b.db", daily_limit=0, read_balance=lambda: 100)
    ledger = BudgetLedger(tmp_path / "b.db", read_balance=lambda: 100)
    with pytest.raises(ValueError):
        ledger.reserve("r", "summary", "1", units=0)
    ledger = BudgetLedger(
        tmp_path / "b.db", read_balance=lambda: 100, now=lambda: datetime(2026, 9, 1)
    )
    with pytest.raises(ValueError):
        ledger.reserve("r", "summary", "1")


def _reserve_in_process(path):
    try:
        BudgetLedger(path, daily_limit=20, read_balance=lambda: 1000).reserve(
            "r", "summary", "1"
        )
        return True
    except BudgetExceeded:
        return False


def test_independent_processes_share_same_allowance(tmp_path):
    path = tmp_path / "b.db"
    with ProcessPoolExecutor(
        max_workers=4, mp_context=multiprocessing.get_context("spawn")
    ) as pool:
        assert sum(pool.map(_reserve_in_process, [path] * 12)) == 4


def test_basic_request_cannot_reserve_less_than_safety_floor(tmp_path):
    ledger = BudgetLedger(tmp_path / "b.db", read_balance=lambda: 1000)
    with pytest.raises(ValueError):
        ledger.reserve("r", "summary", "1", units=1)
