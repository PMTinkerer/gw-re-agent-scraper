"""Targeted status checks survive a run that does not publish (2026-11-07)."""

from datetime import datetime, timedelta, timezone

import pytest

from src.status_cache import CHECKPOINT_EVERY, StatusCache

NOW = datetime(2026, 11, 9, 10, 30, tzinfo=timezone.utc)
URL = (
    "https://mainelistings.com/listings/ME/Wells/04090/10376/1-a-road-wells-me-04090/1"
)


class Clock:
    def __init__(self):
        self.value = NOW

    def __call__(self):
        return self.value


def cache(tmp_path, fetch, clock=None, checkpoints=None):
    checkpoints = [] if checkpoints is None else checkpoints
    return StatusCache(
        tmp_path / "retries.db",
        fetch,
        lambda: checkpoints.append(1),
        now=clock or Clock(),
    )


def test_a_recent_answer_is_reused_instead_of_paid_again(tmp_path):
    calls = []

    def fetch(url):
        calls.append(url)
        return {"mls_number": "1690001", "status": "Pending"}

    clock = Clock()
    assert cache(tmp_path, fetch, clock)(URL)["status"] == "Pending"
    clock.value = NOW + timedelta(days=6)
    second = cache(tmp_path, fetch, clock)
    assert second(URL) == {"mls_number": "1690001", "status": "Pending"}
    assert calls == [URL] and second.reused == 1 and second.fresh == 0


def test_unresolved_answers_are_reused_too_and_expire_after_a_week(tmp_path):
    calls = []

    def fetch(url):
        calls.append(url)
        return None

    clock = Clock()
    assert cache(tmp_path, fetch, clock)(URL) is None
    clock.value = NOW + timedelta(days=3)
    assert cache(tmp_path, fetch, clock)(URL) is None
    assert len(calls) == 1
    clock.value = NOW + timedelta(days=8)
    assert cache(tmp_path, fetch, clock)(URL) is None
    assert len(calls) == 2


def test_a_failed_check_is_never_recorded(tmp_path):
    def broken(url):
        raise TimeoutError

    with pytest.raises(TimeoutError):
        cache(tmp_path, broken)(URL)
    calls = []
    cache(tmp_path, lambda url: calls.append(url))(URL)
    assert calls == [URL]


def test_malformed_answers_are_stored_as_unresolved(tmp_path):
    first = cache(tmp_path, lambda url: {"status": "Closed"})
    assert first(URL) is None
    assert cache(tmp_path, lambda url: pytest.fail("refetched"))(URL) is None


def test_progress_is_pushed_every_few_new_checks(tmp_path):
    pushed = []
    checks = cache(tmp_path, lambda url: {"mls_number": url[-3:]}, checkpoints=pushed)
    for index in range(CHECKPOINT_EVERY * 2 + 3):
        checks(f"{URL}{index:03d}")
    assert len(pushed) == 2


def test_a_clock_moved_backwards_does_not_reuse(tmp_path):
    clock = Clock()
    calls = []

    def fetch(url):
        calls.append(url)
        return None

    cache(tmp_path, fetch, clock)(URL)
    clock.value = NOW - timedelta(hours=1)
    cache(tmp_path, fetch, clock)(URL)
    assert len(calls) == 2
