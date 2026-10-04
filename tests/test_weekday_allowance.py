"""Weekday refresh caps approved by Lucas on 2026-10-04 (2,000/run and day, 80,000/30 days)."""

import json
from datetime import datetime, timedelta, timezone

import pytest

from src.refresh_allowance import normal_allowance, reserve_run

START = datetime(2026, 10, 4, 10, 15, tzinfo=timezone.utc)


def ledger(path, rows=()):
    path.write_text(json.dumps({"schema_version": 1, "reservations": list(rows)}))


def legacy_history():
    """35,000 units reserved by approved September tests, still inside the window."""
    base = datetime(2026, 9, 28, 12, tzinfo=timezone.utc)
    sizes = [5000] * 6 + [4000, 500, 500]
    return [
        {
            "run_id": f"legacy-{index}",
            "reserved_at": (base + timedelta(minutes=index)).isoformat(),
            "reserved_units": units,
        }
        for index, units in enumerate(sizes)
    ]


def test_policy_is_dated_and_legacy_rules_are_unchanged():
    effective = datetime(2026, 10, 4, tzinfo=timezone.utc)
    assert normal_allowance(effective) == (2000, 2000, 80000)
    assert normal_allowance(START) == (2000, 2000, 80000)
    assert normal_allowance(effective - timedelta(seconds=1)) == (500, 500, 10000)


def test_weekday_run_reserves_2000_once_per_day(tmp_path):
    path = tmp_path / "allowance.json"
    ledger(path)
    assert reserve_run(path, "monday", now=START) == 2000
    with pytest.raises(RuntimeError, match="allowance"):
        reserve_run(path, "monday-again", now=START + timedelta(hours=1))
    assert reserve_run(path, "tuesday", now=START + timedelta(days=1)) == 2000


def test_legacy_500_unit_history_remains_valid(tmp_path):
    path = tmp_path / "allowance.json"
    ledger(path, legacy_history())
    before = json.loads(path.read_text())["reservations"]
    assert reserve_run(path, "first-weekday", now=START) == 2000
    rows = json.loads(path.read_text())["reservations"]
    assert rows[:-1] == before


def test_rolling_cap_covers_legacy_history_plus_daily_runs(tmp_path):
    path = tmp_path / "allowance.json"
    ledger(path, legacy_history())
    for day in range(22):  # 35,000 + 22 * 2,000 = 79,000
        reserve_run(path, f"day-{day}", now=START + timedelta(days=day))
    before = path.read_bytes()
    with pytest.raises(RuntimeError, match="allowance"):
        reserve_run(path, "over-cap", now=START + timedelta(days=22))
    assert path.read_bytes() == before


def test_incremental_workflow_runs_weekdays_only_and_stays_gated():
    from pathlib import Path

    workflow = Path(".github/workflows/incremental_active.yml").read_text()
    assert "- cron: '15 10 * * 1-5'" in workflow
    assert "'15 10 * * *'" not in workflow
    assert "vars.INCREMENTAL_ACTIVE_ENABLED == 'true'" in workflow
