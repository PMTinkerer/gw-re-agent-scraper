"""Bounded retries for transient Firecrawl gateway failures (Lucas, 2026-10-04)."""

import pytest
import requests

from src import refresh_transport
from src.refresh_transport import RefreshTransport
from tests.test_refresh_transport import Response, Session, proof


class Flaky(Session):
    """Return each scripted failure in order, then the normal document."""

    def __init__(self, failures):
        super().__init__()
        self.failures = list(failures)

    def post(self, *args, **kwargs):
        if self.failures:
            self.posts.append(kwargs)
            failure = self.failures.pop(0)
            if isinstance(failure, Exception):
                raise failure
            return Failed(failure)
        return super().post(*args, **kwargs)


class Failed(Response):
    def __init__(self, status):
        super().__init__({})
        self.status_code = status

    def raise_for_status(self):
        error = requests.HTTPError(f"{self.status_code} Server Error")
        error.response = self
        raise error


def transport(tmp_path, session, reservations):
    return RefreshTransport(
        api_key="fixture-key",
        policy_path=proof(tmp_path),
        reserve=lambda kind, url: reservations.append(kind),
        session=session,
    )


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    waits = []
    monkeypatch.setattr(refresh_transport.time, "sleep", waits.append)
    return waits


@pytest.mark.parametrize(
    "failures",
    [[502], [503, 504], [requests.ConnectionError("reset")], [requests.Timeout()]],
    ids=str,
)
def test_transient_failures_retry_and_reserve_every_attempt(
    tmp_path, failures, no_sleep
):
    reservations = []
    session = Flaky(failures)
    page = transport(tmp_path, session, reservations).summary("york", 1)
    assert page.total_results == 0
    assert len(session.posts) == len(failures) + 1
    assert reservations == ["summary"] * (len(failures) + 1)
    assert no_sleep == [5, 15][: len(failures)]


def test_three_transient_failures_still_stop_the_run(tmp_path, no_sleep):
    reservations = []
    session = Flaky([502, 502, 502])
    with pytest.raises(requests.HTTPError, match="502"):
        transport(tmp_path, session, reservations).summary("york", 1)
    assert len(session.posts) == 3 and reservations == ["summary"] * 3


@pytest.mark.parametrize("status", [400, 401, 402, 403, 404, 500])
def test_non_transient_errors_never_retry(tmp_path, status, no_sleep):
    reservations = []
    session = Flaky([status])
    with pytest.raises(requests.HTTPError):
        transport(tmp_path, session, reservations).summary("york", 1)
    assert len(session.posts) == 1 and no_sleep == []


def test_budget_exhaustion_stops_before_a_retry_request(tmp_path, no_sleep):
    session = Flaky([502])
    calls = []

    def reserve(kind, url):
        calls.append(kind)
        if len(calls) > 1:
            raise RuntimeError("Whole-run conservative allowance exhausted")

    with pytest.raises(RuntimeError, match="allowance"):
        RefreshTransport(
            api_key="fixture-key",
            policy_path=proof(tmp_path),
            reserve=reserve,
            session=session,
        ).summary("york", 1)
    assert len(session.posts) == 1
