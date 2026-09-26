import json
from datetime import datetime, timedelta, timezone

import pytest

from src.refresh_allowance import reserve_run


def test_remote_run_allowance_survives_failed_run_and_rerun(tmp_path):
    path = tmp_path / "allowance.json"
    path.write_text(json.dumps({"schema_version": 1, "reservations": []}))
    now = datetime(2026, 9, 26, tzinfo=timezone.utc)
    reserve_run(path, "run-1", now=now)
    with pytest.raises(RuntimeError, match="allowance"):
        reserve_run(path, "run-2", now=now)
    with pytest.raises(RuntimeError):
        reserve_run(path, "run-1", now=now + timedelta(days=1))
    assert len(json.loads(path.read_text())["reservations"]) == 1


def test_rolling_allowance_cannot_reset_by_next_day(tmp_path):
    path = tmp_path / "allowance.json"
    path.write_text(json.dumps({"schema_version": 1, "reservations": []}))
    now = datetime(2026, 9, 1, tzinfo=timezone.utc)
    for day in range(20):
        reserve_run(path, f"run-{day}", now=now + timedelta(days=day))
    with pytest.raises(RuntimeError, match="allowance"):
        reserve_run(path, "extra", now=now + timedelta(days=20))


def test_missing_or_corrupt_ledger_never_resets(tmp_path):
    path = tmp_path / "missing.json"
    with pytest.raises((OSError, RuntimeError)):
        reserve_run(path, "one")
    path.write_text("{}")
    with pytest.raises(RuntimeError):
        reserve_run(path, "one")
