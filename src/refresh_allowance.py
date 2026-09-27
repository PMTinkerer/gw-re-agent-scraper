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
ALIAS_VERIFICATION_CEILINGS = {
    f"2026-09-27-alias-verification-{attempt}": 25000 + 5000 * attempt
    for attempt in range(1, 6)
}


def reserve_run(path, run_id, *, now=None, approval_id="", finalization=False):
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
        explicit_ceilings = {}
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
                or matches[0]["additional_units"]
                != (FINALIZATION_DAILY_UNITS if finalization else RUN_UNITS)
            ):
                raise RuntimeError(
                    "Cannot combine finalization without matching dated 5000-unit approval"
                    if finalization
                    else "No matching dated one-time allowance approval"
                )
            additional_units = matches[0]["additional_units"]
            ceiling_fields = ("daily_ceiling_units", "rolling_ceiling_units")
            alias_ceiling = ALIAS_VERIFICATION_CEILINGS.get(approval_id)
            if (
                approval_id.startswith("2026-09-27-alias-verification-")
                and alias_ceiling is None
            ):
                raise RuntimeError("Unknown alias verification approval")
            if alias_ceiling is not None or any(
                field in matches[0] for field in ceiling_fields
            ):
                # A separately recorded single-use extension, not a global cap
                # increase. Larger extensions are bound to exact dated approvals;
                # all other explicit exceptions retain 15,000.
                ceiling = ROLLING_UNITS + FINALIZATION_DAILY_UNITS
                if (
                    approval_id == "2026-09-26-price-partition-test"
                    and matches[0].get("utc_date") == "2026-09-26"
                ):
                    ceiling += FINALIZATION_DAILY_UNITS
                elif (
                    approval_id == "2026-09-27-parser-performance-test"
                    and matches[0].get("utc_date") == "2026-09-27"
                ):
                    ceiling += 2 * FINALIZATION_DAILY_UNITS
                elif alias_ceiling is not None:
                    if matches[0].get("utc_date") != "2026-09-27":
                        raise RuntimeError("Invalid alias verification approval date")
                    ceiling = alias_ceiling
                if not finalization or any(
                    type(matches[0].get(field)) is not int
                    or matches[0][field] != ceiling
                    for field in ceiling_fields
                ):
                    raise RuntimeError("Invalid explicit test ceilings")
                explicit_ceilings = {
                    field: matches[0][field] for field in ceiling_fields
                }
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
            FINALIZATION_DAILY_UNITS if finalization else DAILY_UNITS
        ) + additional_units
        daily_limit = explicit_ceilings.get("daily_ceiling_units", daily_limit)
        rolling_limit = explicit_ceilings.get("rolling_ceiling_units", ROLLING_UNITS)
        # Manual finalization reserves only capacity still available today.
        # Failed reservations remain counted; old rows are never rewritten.
        run_units = (
            min(FINALIZATION_DAILY_UNITS, daily_limit - daily, rolling_limit - rolling)
            if finalization
            else RUN_UNITS
        )
        if (
            run_units < RUN_UNITS
            or daily + run_units > daily_limit
            or rolling + run_units > rolling_limit
        ):
            raise RuntimeError("Durable run allowance exhausted")
        data["reservations"].append(
            {
                "run_id": run_id,
                "reserved_at": now.isoformat(),
                "reserved_units": run_units,
                **({"finalization": True} if finalization else {}),
                **({"approval_id": approval_id} if approval_id else {}),
                **explicit_ceilings,
            }
        )
        temporary = path.with_suffix(".pending")
        with temporary.open("w") as out:
            json.dump(data, out, indent=2, sort_keys=True)
            out.flush()
            os.fsync(out.fileno())
        os.replace(temporary, path)
        return run_units
