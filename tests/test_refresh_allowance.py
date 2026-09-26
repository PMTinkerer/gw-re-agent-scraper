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


def approved_ledger(path):
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "reservations": [],
                "one_time_approvals": [
                    {
                        "approval_id": "extra-canary",
                        "utc_date": "2026-09-26",
                        "additional_units": 500,
                    }
                ],
            }
        )
    )


def test_explicit_extra_run_is_single_use_and_preserves_prior_reservation(tmp_path):
    path = tmp_path / "allowance.json"
    approved_ledger(path)
    now = datetime(2026, 9, 26, tzinfo=timezone.utc)
    reserve_run(path, "first", now=now)
    first = json.loads(path.read_text())["reservations"][0]
    with pytest.raises(RuntimeError, match="allowance"):
        reserve_run(path, "unapproved", now=now)
    reserve_run(path, "second", now=now, approval_id="extra-canary")
    rows = json.loads(path.read_text())["reservations"]
    assert rows[0] == first
    assert rows[1]["approval_id"] == "extra-canary"
    assert sum(row["reserved_units"] for row in rows) == 1000
    with pytest.raises(RuntimeError):
        reserve_run(path, "third", now=now, approval_id="extra-canary")
    with pytest.raises(RuntimeError):
        reserve_run(path, "third", now=now)
    reserve_run(path, "tomorrow", now=now + timedelta(days=1))


@pytest.mark.parametrize(
    "day, approval", [(25, "extra-canary"), (27, "extra-canary"), (26, "unknown")]
)
def test_exception_must_match_approved_utc_day_and_id(tmp_path, day, approval):
    path = tmp_path / "allowance.json"
    approved_ledger(path)
    before = path.read_bytes()
    with pytest.raises(RuntimeError):
        reserve_run(
            path,
            "run",
            now=datetime(2026, 9, day, tzinfo=timezone.utc),
            approval_id=approval,
        )
    assert path.read_bytes() == before


def test_exception_cannot_exceed_rolling_cap(tmp_path):
    path = tmp_path / "allowance.json"
    approved_ledger(path)
    start = datetime(2026, 9, 7, tzinfo=timezone.utc)
    for day in range(20):
        reserve_run(path, str(day), now=start + timedelta(days=day))
    before = path.read_bytes()
    with pytest.raises(RuntimeError, match="allowance"):
        reserve_run(
            path, "extra", now=start + timedelta(days=19), approval_id="extra-canary"
        )
    assert path.read_bytes() == before
