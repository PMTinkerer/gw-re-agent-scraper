"""Persistent conservative request allowances; these are not provider charges."""

from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable


class BudgetExceeded(RuntimeError):
    """No paid request may follow an unavailable or exhausted allowance."""


class BudgetLedger:
    """Reserve before each request, atomically across processes and restarts.

    Reservations are never refunded, including requests that fail or runs that
    do not publish. Provider balance is independently required for every reserve.
    """

    def __init__(
        self,
        path: str | Path,
        *,
        read_balance: Callable[[], int | None],
        daily_limit: int = 500,
        rolling_limit: int = 10000,
        units_per_request: int = 5,
        now: Callable[[], datetime] | None = None,
    ):
        for value in (daily_limit, rolling_limit, units_per_request):
            if type(value) is not int or value <= 0:
                raise ValueError("Budget limits must be positive integers")
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.read_balance = read_balance
        self.daily_limit = daily_limit
        self.rolling_limit = rolling_limit
        self.units_per_request = units_per_request
        self.now = now or (lambda: datetime.now(timezone.utc))
        with sqlite3.connect(self.path, timeout=30) as conn:
            conn.execute("""CREATE TABLE IF NOT EXISTS reservations (
                reservation_id TEXT PRIMARY KEY, run_id TEXT NOT NULL,
                request_kind TEXT NOT NULL, request_key TEXT NOT NULL,
                reserved_at REAL NOT NULL, reserved_units INTEGER NOT NULL
                    CHECK(reserved_units > 0))""")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS reservation_time ON reservations(reserved_at)"
            )

    def reserve(
        self,
        run_id: str,
        request_kind: str,
        request_key: str,
        *,
        units: int | None = None,
    ) -> str:
        units = self.units_per_request if units is None else units
        if type(units) is not int or units < self.units_per_request:
            raise ValueError(
                "Reservation units cannot be below the request safety floor"
            )
        now = self.now()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("Budget clock must be timezone aware")
        now = now.astimezone(timezone.utc)
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
        rolling_start = (now - timedelta(days=30)).timestamp()
        with sqlite3.connect(self.path, timeout=30) as conn:
            conn.execute("BEGIN IMMEDIATE")
            daily, rolling = conn.execute(
                """SELECT
                COALESCE(SUM(CASE WHEN reserved_at >= ? THEN reserved_units ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN reserved_at >= ? THEN reserved_units ELSE 0 END), 0)
                FROM reservations""",
                (day_start, rolling_start),
            ).fetchone()
            if daily + units > self.daily_limit or rolling + units > self.rolling_limit:
                raise BudgetExceeded("Conservative reserved-credit allowance exhausted")
            try:
                balance = self.read_balance()
            except Exception as exc:
                raise BudgetExceeded("Provider credit balance unavailable") from exc
            if type(balance) is not int or balance < units:
                raise BudgetExceeded("Provider credit balance unknown or too low")
            reservation_id = uuid.uuid4().hex
            conn.execute(
                "INSERT INTO reservations VALUES (?, ?, ?, ?, ?, ?)",
                (
                    reservation_id,
                    run_id,
                    request_kind,
                    request_key,
                    now.timestamp(),
                    units,
                ),
            )
        return reservation_id
