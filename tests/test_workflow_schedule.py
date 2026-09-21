from pathlib import Path

import yaml


WORKFLOW = Path(__file__).parents[1] / ".github/workflows/maine_listings.yml"


def _workflow():
    return yaml.safe_load(WORKFLOW.read_text())


def test_only_the_weekly_schedule_runs():
    """The daily active cron was retired in favour of a single weekly run.

    It previously fired on Mondays alongside the weekly run and raced it for
    the generated-data commit; weekly discovery is sufficient for this data
    and keeps Firecrawl credit use inside budget.
    """
    schedules = [s["cron"] for s in _workflow()[True]["schedule"]]

    assert schedules == ["30 11 * * 1"]


def test_no_daily_cron_remains():
    workflow = WORKFLOW.read_text()

    assert "cron: '30 11 * * *'" not in workflow
    assert "cron: '30 12 * * *'" not in workflow


def test_mode_selection_uses_env_indirection():
    """Workflow inputs must not be interpolated straight into the run block."""
    workflow = WORKFLOW.read_text()

    assert "INPUT_MODE: ${{ github.event.inputs.mode }}" in workflow
    assert (
        'if [ -n "$INPUT_MODE" ]; then\n'
        '            MODE="$INPUT_MODE"'
    ) in workflow


def test_scheduled_runs_default_to_weekly_mode():
    """With no dispatch input, a cron run must fall through to weekly."""
    workflow = WORKFLOW.read_text()

    assert (
        '          else\n'
        '            MODE="weekly"\n'
        '          fi'
    ) in workflow


def test_repair_modes_exist_but_are_never_scheduled():
    """The one-time repair modes must be dispatch-only — a re-run costs credits."""
    workflow = WORKFLOW.read_text()
    inputs = _workflow()[True]["workflow_dispatch"]["inputs"]

    assert "reverify-withdrawn" in inputs["mode"]["options"]
    assert "backfill-enrichment" in inputs["mode"]["options"]
    assert "--reverify-withdrawn" in workflow
    # Nothing may map a cron schedule onto a repair mode.
    assert 'MODE="reverify-withdrawn"' not in workflow
    assert 'MODE="backfill-enrichment"' not in workflow


def test_weekly_mode_covers_closed_active_and_verification():
    """The single weekly run must do the work the daily cron used to."""
    workflow = WORKFLOW.read_text()
    weekly = workflow.split('weekly)', 1)[1].split(';;', 1)[0]

    assert '--status Closed' in weekly
    assert '--status Active' in weekly
    assert '--sweep' in weekly
    assert '--enrich' in weekly
