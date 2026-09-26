import json
from datetime import datetime, timedelta, timezone

import pytest

from src.refresh_allowance import reserve_run


def price_retry_ledger(path, prior=5000):
    data = {
        "schema_version": 1,
        "reservations": [
            {
                "run_id": "prior",
                "reserved_units": prior,
                "reserved_at": "2026-09-26T00:00:00+00:00",
            }
        ],
        "one_time_approvals": [
            {
                "approval_id": "price-retry",
                "utc_date": "2026-09-26",
                "additional_units": 5000,
            }
        ],
    }
    path.write_text(json.dumps(data))
    return data


def test_price_retry_is_bounded_single_use_and_preserves_history(tmp_path):
    path = tmp_path / "allowance.json"
    before = price_retry_ledger(path)
    now = datetime(2026, 9, 26, 19, tzinfo=timezone.utc)
    assert (
        reserve_run(
            path, "price", now=now, finalization=True, approval_id="price-retry"
        )
        == 5000
    )
    after = json.loads(path.read_text())
    assert after["reservations"][:-1] == before["reservations"]
    assert after["one_time_approvals"] == before["one_time_approvals"]
    assert after["reservations"][-1]["approval_id"] == "price-retry"
    with pytest.raises(RuntimeError):
        reserve_run(
            path, "again", now=now, finalization=True, approval_id="price-retry"
        )
    with pytest.raises(RuntimeError):
        reserve_run(path, "normal", now=now, finalization=True)
    with pytest.raises(RuntimeError):
        reserve_run(path, "tomorrow", now=now + timedelta(days=1))
    assert json.loads(path.read_text()) == after


def test_price_retry_never_reserves_more_than_5000_per_run(tmp_path):
    path = tmp_path / "allowance.json"
    price_retry_ledger(path, prior=500)
    assert (
        reserve_run(
            path,
            "price",
            now=datetime(2026, 9, 26, 19, tzinfo=timezone.utc),
            finalization=True,
            approval_id="price-retry",
        )
        == 5000
    )


@pytest.mark.parametrize(
    "mode", ["wrong_day", "wrong_id", "wrong_units", "no_finalization", "rolling"]
)
def test_price_retry_invalid_authority_or_capacity_does_not_mutate(tmp_path, mode):
    path = tmp_path / "allowance.json"
    data = price_retry_ledger(path)
    now = datetime(2026, 9, 26, 19, tzinfo=timezone.utc)
    if mode == "wrong_units":
        data["one_time_approvals"][0]["additional_units"] = 10000
    if mode == "rolling":
        data["reservations"].append(
            {
                "run_id": "yesterday",
                "reserved_units": 5000,
                "reserved_at": "2026-09-25T00:00:00+00:00",
            }
        )
    path.write_text(json.dumps(data))
    before = path.read_bytes()
    with pytest.raises(RuntimeError):
        reserve_run(
            path,
            "price",
            now=now + timedelta(days=mode == "wrong_day"),
            finalization=mode != "no_finalization",
            approval_id="unknown" if mode == "wrong_id" else "price-retry",
        )
    assert path.read_bytes() == before


def test_finalization_uses_remaining_5000_without_erasing_history(tmp_path):
    path = tmp_path / "allowance.json"
    approved_ledger(path)
    now = datetime(2026, 9, 26, tzinfo=timezone.utc)
    reserve_run(path, "first", now=now)
    reserve_run(path, "second", now=now, approval_id="extra-canary")
    previous = json.loads(path.read_text())["reservations"]
    assert reserve_run(path, "finalize", now=now, finalization=True) == 4000
    rows = json.loads(path.read_text())["reservations"]
    assert rows[:2] == previous
    assert rows[-1]["finalization"] is True
    assert sum(row["reserved_units"] for row in rows) == 5000
    with pytest.raises(RuntimeError, match="allowance"):
        reserve_run(path, "more", now=now, finalization=True)
    assert reserve_run(path, "tomorrow", now=now + timedelta(days=1)) == 500


def test_finalization_respects_rolling_limit(tmp_path):
    path = tmp_path / "allowance.json"
    approved_ledger(path)
    now = datetime(2026, 9, 26, tzinfo=timezone.utc)
    for day in range(16):
        reserve_run(path, str(day), now=now - timedelta(days=16 - day))
    assert reserve_run(path, "finalize", now=now, finalization=True) == 2000
    with pytest.raises(RuntimeError, match="allowance"):
        reserve_run(path, "more", now=now + timedelta(days=1), finalization=True)


def test_finalization_cannot_stack_old_exception(tmp_path):
    path = tmp_path / "allowance.json"
    approved_ledger(path)
    with pytest.raises(RuntimeError, match="combine"):
        reserve_run(
            path,
            "both",
            now=datetime(2026, 9, 26, tzinfo=timezone.utc),
            finalization=True,
            approval_id="extra-canary",
        )


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
