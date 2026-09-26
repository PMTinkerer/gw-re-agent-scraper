"""Whole-run allowances committed remotely before ephemeral runners spend."""

from __future__ import annotations

import fcntl
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

RUN_UNITS = 500
DAILY_UNITS = 500
ROLLING_UNITS = 10000
FINALIZATION_DAILY_UNITS = 5000


def reserve_run(path, run_id, *, now=None, approval_id="", finalization=False):
    if finalization and approval_id:
        raise RuntimeError("Cannot combine finalization and one-time allowance")
    path = Path(path)
    now = now or datetime.now(timezone.utc)
    if not run_id or now.tzinfo is None:
        raise ValueError("Run ID and timezone-aware clock required")
    now = now.astimezone(timezone.utc)
    with open(str(path) + ".lock", "a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        data = json.loads(path.read_text())
        if data.get("schema_version") != 1 or not isinstance(
            data.get("reservations"), list
        ):
            raise RuntimeError("Invalid durable allowance ledger")
        additional_units = 0
        if approval_id:
            approvals = data.get("one_time_approvals", [])
            if not isinstance(approvals, list):
                raise RuntimeError("Invalid one-time allowance approvals")
            matches = [
                row
                for row in approvals
                if isinstance(row, dict) and row.get("approval_id") == approval_id
            ]
            if (
                len(matches) != 1
                or matches[0].get("utc_date") != now.date().isoformat()
                or type(matches[0].get("additional_units")) is not int
                or matches[0]["additional_units"] != RUN_UNITS
            ):
                raise RuntimeError("No matching dated one-time allowance approval")
            additional_units = RUN_UNITS
        daily = rolling = 0
        for row in data["reservations"]:
            if row["run_id"] == run_id:
                raise RuntimeError("Run allowance is single use")
            if approval_id and row.get("approval_id") == approval_id:
                raise RuntimeError("One-time allowance approval already consumed")
            timestamp = datetime.fromisoformat(row["reserved_at"])
            units = row["reserved_units"]
            if timestamp.tzinfo is None or type(units) is not int or units < RUN_UNITS:
                raise RuntimeError("Invalid historical allowance")
            if timestamp > now:
                raise RuntimeError("Future allowance timestamp")
            if timestamp.date() == now.date():
                daily += units
            if timestamp > now - timedelta(days=30):
                rolling += units
        daily_limit = (
            FINALIZATION_DAILY_UNITS if finalization else DAILY_UNITS + additional_units
        )
        # Manual finalization reserves only capacity still available today.
        # Failed reservations remain counted; old rows are never rewritten.
        run_units = (
            min(daily_limit - daily, ROLLING_UNITS - rolling)
            if finalization else RUN_UNITS
        )
        if (
            run_units < RUN_UNITS or daily + run_units > daily_limit
            or rolling + run_units > ROLLING_UNITS
        ):
            raise RuntimeError("Durable run allowance exhausted")
        data["reservations"].append(
            {
                "run_id": run_id,
                "reserved_at": now.isoformat(),
                "reserved_units": run_units,
                **({"finalization": True} if finalization else {}),
                **({"approval_id": approval_id} if approval_id else {}),
            }
        )
        temporary = path.with_suffix(".pending")
        with temporary.open("w") as out:
            json.dump(data, out, indent=2, sort_keys=True)
            out.flush()
            os.fsync(out.fileno())
        os.replace(temporary, path)
        return run_units
