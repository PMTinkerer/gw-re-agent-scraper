import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

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


def test_saved_live_cards_with_missing_facts_keep_complete_coverage(tmp_path):
    from src.active_refresh import _discover

    pages = json.loads(
        (
            Path(__file__).parent / "fixtures/biddeford-active-cards-2026-09-26.json"
        ).read_text()
    )
    session = Session()
    transport = RefreshTransport(
        api_key="fixture-key",
        policy_path=proof(tmp_path),
        reserve=lambda *a: None,
        session=session,
    )

    def fetch(town, page):
        saved = pages[page - 1]
        session.document = {
            "markdown": f"{saved['total_results']} Results\n{page} of 5\n"
            + saved["markdown"],
            "metadata": {"statusCode": 200},
        }
        result = transport.summary(town, page)
        assert len(result.listings) == saved["card_count"]
        return result

    found = _discover(["biddeford"], fetch, 90)
    assert len(found) == 118
    south = next(row for row in found.values() if row["address"] == "177 South Street")
    assert south["beds"] is None and south["baths"] == 2 and south["sqft"] == 2402
    birdie = next(row for row in found.values() if row["address"] == "10 Birdie Lane")
    assert birdie["beds"] is None and birdie["baths"] is None and birdie["sqft"] is None
    woodland = next(
        row for row in found.values() if row["address"] == "2 Woodland Drive"
    )
    assert woodland["beds"] == 3 and woodland["baths"] == 1


@pytest.mark.parametrize(
    "old,new",
    [
        ("2 Baths", "unknown Baths"),
        ("Brought to you by", "missing card terminator"),
        ("Biddeford, ME 04005", "York, ME 03909"),
    ],
)
def test_malformed_saved_card_cannot_be_hidden_by_next_card(old, new):
    from src.active_refresh import RefreshIncomplete, SummaryPage, _discover
    from src.incremental_cards import parse_active_cards

    pages = json.loads(
        (
            Path(__file__).parent / "fixtures/biddeford-active-cards-2026-09-26.json"
        ).read_text()
    )
    pages[0]["markdown"] = pages[0]["markdown"].replace(old, new, 1)

    def fetch(town, page):
        saved = pages[page - 1]
        return SummaryPage(town, page, 5, 118, parse_active_cards(saved["markdown"]))

    with pytest.raises(RefreshIncomplete):
        _discover(["biddeford"], fetch, 90)


def test_second_live_scan_duplicate_never_becomes_complete_coverage():
    """Replay the exact card substitution observed in canary 36257185586.

    Page five replaced 365 Main Street with 350 Main Street, already on page
    four, while the published count stayed 118. Never deduplicate and accept.
    Raw provider exports are retained locally; this uses public card excerpts.
    """
    from src.active_refresh import RefreshIncomplete, SummaryPage, _discover
    from src.incremental_cards import _CARD, parse_active_cards

    pages = json.loads(
        (
            Path(__file__).parent / "fixtures/biddeford-active-cards-2026-09-26.json"
        ).read_text()
    )
    duplicate = next(
        m.group()
        for m in _CARD.finditer(pages[3]["markdown"])
        if m[3].strip() == "350 Main Street"
    )
    missing = next(
        m.group()
        for m in _CARD.finditer(pages[4]["markdown"])
        if m[3].strip() == "365 Main Street"
    )
    pages[4]["markdown"] = pages[4]["markdown"].replace(missing, duplicate, 1)
    cards = [parse_active_cards(page["markdown"]) for page in pages]
    assert [len(rows) for rows in cards] == [24, 24, 24, 24, 22]
    assert len({row["detail_url"] for rows in cards for row in rows}) == 117
    with pytest.raises(RefreshIncomplete, match="Duplicate"):
        _discover(
            ["biddeford"],
            lambda town, page: SummaryPage(town, page, 5, 118, cards[page - 1]),
            90,
        )
