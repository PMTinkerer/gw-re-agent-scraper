from pathlib import Path


WORKFLOW = Path(__file__).parents[1] / ".github/workflows/maine_listings.yml"


def test_weekly_and_daily_schedules_do_not_overlap_on_monday():
    workflow = WORKFLOW.read_text()

    assert "cron: '30 11 * * 1'" in workflow
    assert "cron: '30 12 * * *'" in workflow
    assert "cron: '30 11 * * *'" not in workflow
    assert (
        'elif [ "${{ github.event.schedule }}" = "30 11 * * 1" ]; then\n'
        '            MODE="weekly"'
    ) in workflow
    assert (
        'elif [ "${{ github.event.schedule }}" = "30 12 * * *" ]; then\n'
        '            MODE="daily-active"'
    ) in workflow
