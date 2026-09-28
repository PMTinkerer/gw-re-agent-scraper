import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import pytest

from src import incremental_main as cli
from tests.test_refresh_transport import proof


def configure(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "data").mkdir()
    (tmp_path / "data/active_refresh_allowances.json").write_text(
        json.dumps({"schema_version": 1, "reservations": []})
    )
    for key, value in {
        "GITHUB_ACTIONS": "true",
        "GITHUB_REF": "refs/heads/main",
        "GITHUB_RUN_ID": "42",
        "GITHUB_RUN_ATTEMPT": "1",
        "FIRECRAWL_API_KEY": "fixture-key",
    }.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(
        cli,
        "_git",
        lambda *a: cli.EXPECTED_ORIGIN if a[:2] == ("remote", "get-url") else "",
    )
    events = []

    class FakeTransport:
        def __init__(self, **kwargs):
            self.reserve = kwargs["reserve"]

        def balance(self):
            return 1000

        def summary(self, *args):
            self.reserve("summary", "https://mainelistings.com/listings")
            events.append("paid")

        summary_range = detail = status = summary

    monkeypatch.setattr(cli, "RefreshTransport", FakeTransport)
    return proof(tmp_path), events


def test_remote_reservation_failure_means_zero_scrapes(tmp_path, monkeypatch):
    policy, events = configure(tmp_path, monkeypatch)

    def reject(*args):
        raise RuntimeError("push rejected")

    monkeypatch.setattr(cli, "_checkpoint", reject)
    with pytest.raises(RuntimeError, match="push rejected"):
        cli.main(["--billing-proof", str(policy), "--github-publish"])
    assert events == []


def test_incomplete_run_keeps_reservation_and_never_publishes_source(
    tmp_path, monkeypatch
):
    policy, events = configure(tmp_path, monkeypatch)
    checkpoints = []
    monkeypatch.setattr(
        cli, "_checkpoint", lambda paths, run: checkpoints.append(paths)
    )

    def fail(**kwargs):
        assert "fetch_summary" not in kwargs
        kwargs["fetch_summary_range"]("york", None, None)
        raise RuntimeError("incomplete")

    monkeypatch.setattr(cli, "run_active_refresh", fail)
    with pytest.raises(RuntimeError, match="incomplete"):
        cli.main(["--billing-proof", str(policy), "--github-publish"])
    assert events == ["paid"]
    assert checkpoints[0] == ["data/active_refresh_allowances.json"]
    assert all("data/maine_listings.db" not in paths for paths in checkpoints)
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "2")
    with pytest.raises(RuntimeError, match="allowance"):
        cli.main(["--billing-proof", str(policy), "--github-publish"])
    assert events == ["paid"]


def test_direct_local_cli_cannot_spend(tmp_path, monkeypatch):
    policy, events = configure(tmp_path, monkeypatch)
    monkeypatch.delenv("GITHUB_ACTIONS")
    with pytest.raises(RuntimeError, match="no ad-hoc"):
        cli.main(["--billing-proof", str(policy), "--github-publish"])
    assert events == []


@pytest.mark.parametrize(
    "origin", [cli.EXPECTED_ORIGIN, cli.EXPECTED_ORIGIN.removesuffix(".git")]
)
def test_github_checkout_origin_reaches_checkpoint_without_spending(
    tmp_path, monkeypatch, origin
):
    policy, events = configure(tmp_path, monkeypatch)
    monkeypatch.setattr(
        cli, "_git", lambda *a: origin if a[:2] == ("remote", "get-url") else ""
    )

    def stop(*args):
        raise RuntimeError("checkpoint reached")

    monkeypatch.setattr(cli, "_checkpoint", stop)
    with pytest.raises(RuntimeError, match="checkpoint reached"):
        cli.main(["--billing-proof", str(policy), "--github-publish"])
    assert events == []


@pytest.mark.parametrize(
    "origin",
    [
        "https://github.com/other/gw-re-agent-scraper",
        cli.EXPECTED_ORIGIN + "/other",
        "http://github.com/PMTinkerer/gw-re-agent-scraper",
    ],
)
def test_other_origin_stops_before_reservation(tmp_path, monkeypatch, origin):
    policy, events = configure(tmp_path, monkeypatch)
    monkeypatch.setattr(
        cli, "_git", lambda *a: origin if a[:2] == ("remote", "get-url") else ""
    )
    with pytest.raises(RuntimeError, match="Unexpected scraper repository"):
        cli.main(["--billing-proof", str(policy), "--github-publish"])
    assert events == []
    assert (
        json.loads((tmp_path / "data/active_refresh_allowances.json").read_text())[
            "reservations"
        ]
        == []
    )


def test_daily_workflow_is_gated_and_has_no_notification_credentials():
    root = Path(__file__).resolve().parents[1]
    workflow = (root / ".github/workflows/incremental_active.yml").read_text()
    assert "vars.INCREMENTAL_ACTIVE_ENABLED == 'true'" in workflow
    assert "group: maine-listings" in workflow
    for prohibited in ("RESEND", "PUSHOVER", "GRAPH", "deploy-pages", "maine_main"):
        assert prohibited not in workflow


def test_exception_requires_manual_dispatch(tmp_path, monkeypatch):
    policy, events = configure(tmp_path, monkeypatch)
    monkeypatch.setenv("GITHUB_EVENT_NAME", "schedule")
    with pytest.raises(RuntimeError, match="manual dispatch"):
        cli.main(
            [
                "--billing-proof",
                str(policy),
                "--github-publish",
                "--one-time-approval",
                "extra-canary",
            ]
        )
    assert events == []


def test_finalization_requires_manual_dispatch(tmp_path, monkeypatch):
    policy, events = configure(tmp_path, monkeypatch)
    monkeypatch.setenv("GITHUB_EVENT_NAME", "schedule")
    with pytest.raises(RuntimeError, match="manual dispatch"):
        cli.main(["--billing-proof", str(policy), "--github-publish", "--finalization"])
    assert events == []


def test_finalization_wires_run_and_request_budgets(tmp_path, monkeypatch):
    policy, events = configure(tmp_path, monkeypatch)
    monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")
    transport = cli.RefreshTransport
    monkeypatch.setattr(transport, "balance", lambda self: 10000)
    checkpoints = []
    monkeypatch.setattr(
        cli, "_checkpoint", lambda paths, run: checkpoints.append(paths)
    )

    def run(**kwargs):
        for _ in range(1000):
            kwargs["fetch_summary_range"]("york", None, None)
        with pytest.raises(cli.BudgetExceeded, match="Whole-run"):
            kwargs["fetch_summary_range"]("york", None, None)
        Path("data/accepted").mkdir(exist_ok=True)
        Path("data/accepted/current.json").write_text(
            json.dumps({"manifest_sha256": "a" * 64})
        )
        return {"run_id": "42-1", "active_mls_ids": []}

    monkeypatch.setattr(cli, "run_active_refresh", run)
    cli.main(["--billing-proof", str(policy), "--github-publish", "--finalization"])
    assert len(events) == 1000
    rows = json.loads(Path("data/active_refresh_allowances.json").read_text())[
        "reservations"
    ]
    assert rows[-1]["reserved_units"] == 5000
    assert checkpoints[0] == ["data/active_refresh_allowances.json"]


def test_manual_exception_reaches_durable_reservation(tmp_path, monkeypatch):
    policy, events = configure(tmp_path, monkeypatch)
    monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")

    def stop(path, run_id, **kwargs):
        assert kwargs == {"approval_id": "extra-canary"}
        raise RuntimeError("approved reservation reached")

    monkeypatch.setattr(cli, "reserve_run", stop)
    with pytest.raises(RuntimeError, match="approved reservation reached"):
        cli.main(
            [
                "--billing-proof",
                str(policy),
                "--github-publish",
                "--one-time-approval",
                "extra-canary",
            ]
        )
    assert events == []


def test_price_retry_wires_existing_usage_and_1000_request_ceiling(
    tmp_path, monkeypatch
):
    from tests.test_refresh_allowance import price_retry_ledger

    policy, events = configure(tmp_path, monkeypatch)
    monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")
    monkeypatch.setattr(cli.RefreshTransport, "balance", lambda self: 20000)
    path = Path("data/active_refresh_allowances.json")
    data = price_retry_ledger(path)
    now = datetime.now(timezone.utc)
    data["reservations"][0]["reserved_at"] = now.isoformat()
    data["one_time_approvals"][0]["utc_date"] = now.date().isoformat()
    path.write_text(json.dumps(data))
    ledger = cli.BudgetLedger(
        "data/active_refresh_usage.db", read_balance=lambda: 20000
    )
    ledger.reserve("prior", "summary", "prior", units=100)
    checkpoints = []
    monkeypatch.setattr(
        cli, "_checkpoint", lambda paths, run: checkpoints.append(paths)
    )

    def run(**kwargs):
        for _ in range(1000):
            kwargs["fetch_summary_range"]("york", None, None)
        with pytest.raises(cli.BudgetExceeded, match="Whole-run"):
            kwargs["fetch_summary_range"]("york", None, None)
        Path("data/accepted").mkdir(exist_ok=True)
        Path("data/accepted/current.json").write_text(
            json.dumps({"manifest_sha256": "a" * 64})
        )
        return {"run_id": "42-1", "active_mls_ids": []}

    monkeypatch.setattr(cli, "run_active_refresh", run)
    cli.main(
        [
            "--billing-proof",
            str(policy),
            "--github-publish",
            "--finalization",
            "--one-time-approval",
            "price-retry",
        ]
    )
    assert len(events) == 1000
    rows = json.loads(path.read_text())["reservations"]
    assert rows[0] == data["reservations"][0]
    assert rows[-1]["reserved_units"] == 5000
    assert checkpoints[0] == ["data/active_refresh_allowances.json"]


@pytest.mark.parametrize("attempt", range(1, 6))
def test_alias_batch_wires_existing_usage_and_1000_request_ceiling(
    tmp_path, monkeypatch, attempt
):
    from tests.test_refresh_allowance import alias_verification_ledger

    policy, events = configure(tmp_path, monkeypatch)
    monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")
    monkeypatch.setattr(cli.RefreshTransport, "balance", lambda self: 100000)
    path = Path("data/active_refresh_allowances.json")
    data = alias_verification_ledger(path)
    prior = 25000 + 5000 * (attempt - 1)
    data["reservations"][0]["reserved_units"] = prior
    path.write_text(json.dumps(data))
    now = datetime(2026, 9, 27, 18, tzinfo=timezone.utc)
    reserve = cli.reserve_run
    monkeypatch.setattr(
        cli, "reserve_run", lambda *args, **kwargs: reserve(*args, now=now, **kwargs)
    )
    ledger_class = cli.BudgetLedger
    monkeypatch.setattr(
        cli,
        "BudgetLedger",
        lambda *args, **kwargs: ledger_class(*args, now=lambda: now, **kwargs),
    )
    ledger = cli.BudgetLedger(
        "data/active_refresh_usage.db",
        read_balance=lambda: 100000,
        daily_limit=prior,
        rolling_limit=prior,
    )
    ledger.reserve("prior", "summary", "prior", units=prior)
    checkpoints = []
    monkeypatch.setattr(
        cli, "_checkpoint", lambda paths, run: checkpoints.append(paths)
    )

    def run(**kwargs):
        for _ in range(1000):
            kwargs["fetch_summary_range"]("york", None, None)
        with pytest.raises(cli.BudgetExceeded, match="Whole-run"):
            kwargs["fetch_summary_range"]("york", None, None)
        Path("data/accepted").mkdir(exist_ok=True)
        Path("data/accepted/current.json").write_text(
            json.dumps({"manifest_sha256": "a" * 64})
        )
        return {"run_id": "42-1", "active_mls_ids": []}

    monkeypatch.setattr(cli, "run_active_refresh", run)
    cli.main(
        [
            "--billing-proof",
            str(policy),
            "--github-publish",
            "--finalization",
            "--one-time-approval",
            f"2026-09-27-alias-verification-{attempt}",
        ]
    )
    assert len(events) == 1000
    assert json.loads(path.read_text())["reservations"][0] == data["reservations"][0]
    assert checkpoints[0] == ["data/active_refresh_allowances.json"]


@pytest.mark.parametrize("mode", ["six", "wrong_ceiling", "wrong_date", "wrong_mode"])
def test_alias_batch_invalid_authority_stops_before_scrapes(
    tmp_path, monkeypatch, mode
):
    from tests.test_refresh_allowance import alias_verification_ledger

    policy, events = configure(tmp_path, monkeypatch)
    monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")
    monkeypatch.setattr(cli.RefreshTransport, "balance", lambda self: 100000)
    path = Path("data/active_refresh_allowances.json")
    data = alias_verification_ledger(path)
    approval = data["one_time_approvals"][0]
    if mode == "six":
        approval["approval_id"] = "2026-09-27-alias-verification-6"
    if mode == "wrong_ceiling":
        approval["rolling_ceiling_units"] = 50000
    if mode == "wrong_date":
        approval["utc_date"] = "2026-09-28"
    path.write_text(json.dumps(data))
    before = path.read_bytes()
    reserve = cli.reserve_run
    monkeypatch.setattr(
        cli,
        "reserve_run",
        lambda *args, **kwargs: reserve(
            *args, now=datetime(2026, 9, 27, 18, tzinfo=timezone.utc), **kwargs
        ),
    )
    checkpoints = []
    monkeypatch.setattr(cli, "_checkpoint", lambda *args: checkpoints.append(args))
    with pytest.raises(RuntimeError):
        cli.main(
            [
                "--billing-proof",
                str(policy),
                "--github-publish",
                "--one-time-approval",
                approval["approval_id"],
            ]
            + ([] if mode == "wrong_mode" else ["--finalization"])
        )
    assert events == []
    assert checkpoints == []
    assert path.read_bytes() == before
    assert not Path("data/active_refresh_usage.db").exists()


def test_checkpoint_verifies_actual_local_remote_and_rejects_divergence(
    tmp_path, monkeypatch
):
    """Exercise git, not a checkpoint mock; the remote is an offline bare fixture."""

    def git(cwd, *args):
        return subprocess.run(
            ["git", "-C", str(cwd), *args], check=True, capture_output=True, text=True
        ).stdout.strip()

    remote = tmp_path / "remote.git"
    remote.mkdir()
    git(remote, "init", "--bare")
    checkout = tmp_path / "runner"
    checkout.mkdir()
    git(checkout, "init", "-b", "main")
    git(checkout, "config", "user.name", "Fixture")
    git(checkout, "config", "user.email", "fixture@example.test")
    git(checkout, "remote", "add", "origin", str(remote))
    monkeypatch.chdir(checkout)
    (checkout / "allowance.json").write_text('{"reserved":500}')
    cli._checkpoint(["allowance.json"], "run-1")
    assert git(remote, "show", "main:allowance.json") == '{"reserved":500}'
    prior = git(checkout, "rev-parse", "HEAD")
    (checkout / "allowance.json").write_text('{"reserved":1000}')
    cli._checkpoint(["allowance.json"], "run-2")
    # Move the local fixture branch back, without touching working files, then
    # commit a different reservation. Remote must reject rather than be replaced.
    git(checkout, "update-ref", "refs/heads/main", prior)
    (checkout / "allowance.json").write_text('{"reserved":1500}')
    with pytest.raises(subprocess.CalledProcessError):
        cli._checkpoint(["allowance.json"], "run-diverged")
    assert git(remote, "show", "main:allowance.json") == '{"reserved":1000}'
