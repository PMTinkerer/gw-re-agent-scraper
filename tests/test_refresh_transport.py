import hashlib
import json
from datetime import datetime, timezone

import pytest
import requests

from src.refresh_transport import (
    BillingPolicyError,
    RefreshTransport,
    verify_billing_policy,
)


def test_environment_auth_cannot_replace_verified_bearer(tmp_path, monkeypatch):
    monkeypatch.setattr(
        requests.sessions, "get_netrc_auth", lambda url: ("fixture", "password")
    )
    transport = RefreshTransport(
        api_key="fixture-key", policy_path=proof(tmp_path), reserve=lambda *a: None
    )
    prepared = transport.session.prepare_request(
        requests.Request(
            "GET",
            "https://api.firecrawl.dev/v2/team/credit-usage",
            headers={"Authorization": "Bearer fixture-key"},
        )
    )
    assert prepared.headers["Authorization"] == "Bearer fixture-key"


class Response:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self.payload


class Session:
    def __init__(self, document=None):
        self.posts = []
        self.document = document or {
            "markdown": "0 Results",
            "metadata": {"statusCode": 200},
        }

    def get(self, *args, **kwargs):
        return Response({"success": True, "data": {"remainingCredits": 1000}})

    def post(self, *args, **kwargs):
        self.posts.append(kwargs)
        return Response({"success": True, "data": self.document})


def proof(tmp_path, **overrides):
    path = tmp_path / "billing.json"
    path.write_text(
        json.dumps(
            {
                "api_key_sha256": hashlib.sha256(b"fixture-key").hexdigest(),
                "pay_as_you_go_enabled": False,
                "verified_at": datetime.now(timezone.utc).isoformat(),
                "evidence": "Operator readback of billing settings",
                **overrides,
            }
        )
    )
    return path


@pytest.mark.parametrize(
    "changes",
    [
        {"pay_as_you_go_enabled": True},
        {"api_key_sha256": "wrong"},
        {"verified_at": "2000-01-01T00:00:00+00:00"},
    ],
)
def test_billing_gate(changes, tmp_path):
    with pytest.raises(BillingPolicyError):
        verify_billing_policy(proof(tmp_path, **changes), "fixture-key")


def test_explicitly_approved_five_dollar_policy(tmp_path):
    verify_billing_policy(
        proof(
            tmp_path, pay_as_you_go_enabled=True, monthly_limit_usd=5, currency="USD"
        ),
        "fixture-key",
    )


@pytest.mark.parametrize(
    "limit", [None, "5", True, -1, 5.01, 10, float("inf"), float("nan")]
)
def test_enabled_policy_rejects_unapproved_or_unknown_limit(tmp_path, limit):
    with pytest.raises(BillingPolicyError):
        verify_billing_policy(
            proof(
                tmp_path,
                pay_as_you_go_enabled=True,
                monthly_limit_usd=limit,
                currency="USD",
            ),
            "fixture-key",
        )


@pytest.mark.parametrize("currency", [None, "EUR", ""])
def test_enabled_policy_requires_usd(tmp_path, currency):
    with pytest.raises(BillingPolicyError):
        verify_billing_policy(
            proof(
                tmp_path,
                pay_as_you_go_enabled=True,
                monthly_limit_usd=5,
                currency=currency,
            ),
            "fixture-key",
        )


def test_no_paid_call_before_reservation(tmp_path):
    calls = []
    session = Session()

    def reject(*args):
        calls.append("reserved")
        raise RuntimeError("cap reached")

    transport = RefreshTransport(
        api_key="fixture-key",
        policy_path=proof(tmp_path),
        reserve=reject,
        session=session,
    )
    with pytest.raises(RuntimeError, match="cap reached"):
        transport.summary("york", 1)
    assert calls == ["reserved"] and session.posts == []


def test_basic_bounded_options_and_no_retries(tmp_path):
    session = Session()
    transport = RefreshTransport(
        api_key="fixture-key",
        policy_path=proof(tmp_path),
        reserve=lambda *a: None,
        session=session,
    )
    page = transport.summary("york", 1)
    assert page.total_results == 0 and page.total_pages == 1
    body = session.posts[0]["json"]
    assert body["proxy"] == "basic" and body["parsers"] == []
    assert body["formats"] == ["markdown"] and body["maxAge"] == 0
    assert session.posts[0]["allow_redirects"] is False
    assert len(session.posts) == 1


@pytest.mark.parametrize(
    "document",
    [
        {"markdown": "captcha", "metadata": {"statusCode": 200}},
        {"markdown": "0 Results", "metadata": {"statusCode": 403}},
        {"markdown": "0 Results"},
        {"markdown": "Loading...", "metadata": {"statusCode": 200}},
    ],
)
def test_invalid_response_never_becomes_empty_snapshot(tmp_path, document):
    session = Session(document)
    transport = RefreshTransport(
        api_key="fixture-key",
        policy_path=proof(tmp_path),
        reserve=lambda *a: None,
        session=session,
    )
    with pytest.raises(RuntimeError):
        transport.summary("york", 1)
    assert len(session.posts) == 1
