"""Explicit no-email daily lane. Never imports legacy notification/worker code."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path

from .active_refresh import run_active_refresh
from .refresh_allowance import (
    reserve_run,
    RUN_UNITS,
    DAILY_UNITS,
    FINALIZATION_DAILY_UNITS,
    ALIAS_VERIFICATION_CEILINGS,
)
from .refresh_budget import BudgetExceeded, BudgetLedger
from .refresh_transport import RefreshTransport, verify_billing_policy
from .state import TOWNS

EXPECTED_ORIGIN = "https://github.com/PMTinkerer/gw-re-agent-scraper.git"


def _git(*args):
    return subprocess.run(
        ["git", *args], check=True, capture_output=True, text=True
    ).stdout.strip()


def _checkpoint(paths, run_id):
    """Push reservation/retry evidence before requesting any billable work.

    A rejected push never runs a scraper. Never merge/overwrite remote data here.
    The existing shared workflow concurrency group prevents sibling pipeline races.
    """
    _git("add", "--", *paths)
    if _git("diff", "--cached", "--name-only"):
        _git("commit", "-m", "data: reserve incremental refresh " + run_id)
        _git("push", "origin", "HEAD:main")
    remote = _git("ls-remote", "origin", "refs/heads/main").split()
    if not remote or remote[0] != _git("rev-parse", "HEAD"):
        raise RuntimeError("Remote reservation checkpoint not verified")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--billing-proof", required=True)
    parser.add_argument("--github-publish", action="store_true")
    parser.add_argument("--one-time-approval", default="")
    parser.add_argument(
        "--finalization",
        action="store_true",
        help="Operator-approved temporary 5,000-unit manual testing ceiling",
    )
    args = parser.parse_args(argv)
    if (args.one_time_approval or args.finalization) and os.environ.get(
        "GITHUB_EVENT_NAME"
    ) != "workflow_dispatch":
        raise RuntimeError("Expanded allowance requires manual dispatch")
    # No standalone environment variables may bypass the publication boundary.
    if not args.github_publish or os.environ.get("GITHUB_ACTIONS") != "true":
        raise RuntimeError("Use the gated GitHub daily lane; no ad-hoc paid runs")
    if os.environ.get("GITHUB_REF") != "refs/heads/main":
        raise RuntimeError("Incremental publication requires main")
    if _git("remote", "get-url", "origin") not in (
        EXPECTED_ORIGIN,
        EXPECTED_ORIGIN.removesuffix(".git"),
    ):
        raise RuntimeError("Unexpected scraper repository")
    if _git("status", "--porcelain", "--untracked-files=no"):
        raise RuntimeError("Clean tracked checkout required before refresh")
    if _git("diff", "--cached", "--name-only"):
        raise RuntimeError("Unrelated staged files present")
    run_id = os.environ["GITHUB_RUN_ID"] + "-" + os.environ["GITHUB_RUN_ATTEMPT"]
    key = os.environ.get("FIRECRAWL_API_KEY", "")
    verify_billing_policy(args.billing_proof, key)
    allowance_path = "data/active_refresh_allowances.json"
    state_path = "data/active_refresh_retries.db"
    budget_path = "data/active_refresh_usage.db"
    # Fail before reserving today's allowance if the configured account is unavailable.
    transport = RefreshTransport(
        api_key=key,
        policy_path=args.billing_proof,
        reserve=lambda *_: None,
        diagnostics_path=".firecrawl/active-refresh-diagnostics",
    )
    daily_limit = FINALIZATION_DAILY_UNITS if args.finalization else DAILY_UNITS
    if transport.balance() < (
        FINALIZATION_DAILY_UNITS if args.finalization else RUN_UNITS
    ):
        raise BudgetExceeded(
            "Insufficient existing credits for conservative run allowance"
        )
    options = {"approval_id": args.one_time_approval}
    if args.finalization:
        options["finalization"] = True
    run_units = reserve_run(allowance_path, run_id, **options)
    # The dated single-use approval was validated above before widening request
    # headroom. The whole-run ceiling remains the returned (at most 5000) units.
    if args.finalization and args.one_time_approval:
        daily_limit += FINALIZATION_DAILY_UNITS
    request_limits = {"daily_limit": daily_limit}
    alias_ceiling = ALIAS_VERIFICATION_CEILINGS.get(args.one_time_approval)
    if alias_ceiling is not None:
        request_limits.update(daily_limit=alias_ceiling, rolling_limit=alias_ceiling)
    _checkpoint([allowance_path], run_id)
    ledger = BudgetLedger(
        budget_path,
        read_balance=transport.balance,
        **request_limits,
    )
    requested = 0

    def before_request(kind, url):
        nonlocal requested
        if requested + 5 > run_units:
            raise BudgetExceeded("Whole-run conservative allowance exhausted")
        ledger.reserve(run_id, kind, url)
        requested += 5
        # The producer records a new-detail attempt BEFORE entering this callback.
        # Persist that attempt remotely before payment, even if the runner is killed.
        if kind == "new_detail":
            _checkpoint([state_path, budget_path], run_id)

    transport.reserve = before_request
    try:
        result = run_active_refresh(
            db_path="data/maine_listings.db",
            manifest_path="data/active_refresh.json",
            state_path=state_path,
            towns=TOWNS,
            run_id=run_id,
            fetch_summary_range=transport.summary_range,
            fetch_detail=transport.detail,
            fetch_status=transport.status,
        )
    finally:
        # No broad git add data/. Publication is absent on an incomplete run.
        paths = [p for p in (state_path, budget_path) if Path(p).exists()]
        if paths:
            _checkpoint(paths, run_id)
    pointer_path = Path("data/accepted/current.json")
    pointer = json.loads(pointer_path.read_text())
    bundle = "data/accepted/bundles/" + pointer["manifest_sha256"]
    _checkpoint(
        [
            "data/maine_listings.db",
            "data/active_refresh.json",
            str(pointer_path),
            bundle + "/maine_listings.db",
            bundle + "/active_refresh.json",
        ],
        run_id,
    )
    print(
        f"Published complete discovery {result['run_id']}: {len(result['active_mls_ids'])} active MLS IDs; "
        f"{len(result.get('unresolved', []))} unresolved new listings; "
        f"{requested} reserved units (not billed credits). No notifications."
    )


if __name__ == "__main__":
    main()
