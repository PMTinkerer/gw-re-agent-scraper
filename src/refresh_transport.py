"""No-email, basic-only Firecrawl adapter for the incremental refresh lane."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlencode, urlsplit

import requests

from .active_refresh import RefreshIncomplete, SummaryPage
from .incremental_cards import parse_active_cards
from .maine_parser import (
    DETAIL_EXTRACT_JS,
    parse_detail_response,
    parse_pagination,
    parse_total_results,
)

API = "https://api.firecrawl.dev/v2"
STATUS_JS = r"""(() => {
 const texts = Array.from(document.scripts).map(s => s.textContent).join('\n');
 function field(name) {
   const matches = [...texts.matchAll(new RegExp(name + ':"([^"\\n]+)"', 'g'))];
   return matches.length ? matches[0][1] : null;
 }
 return JSON.stringify({mls_number: field('listing_id'), status: field('mls_status')});
})()"""


class BillingPolicyError(RuntimeError):
    pass


def verify_billing_policy(path, api_key, *, now=None):
    """Explicit dated operator evidence; never infer billing policy from balance."""
    try:
        data = json.loads(Path(path).read_text())
        verified = datetime.fromisoformat(data["verified_at"])
        age = (now or datetime.now(timezone.utc)) - verified
        enabled = data.get("pay_as_you_go_enabled")
        limit = data.get("monthly_limit_usd")
        billing_approved = enabled is False or (
            enabled is True
            and data.get("currency") == "USD"
            and type(limit) in (int, float)
            and 0 <= limit <= 5
        )
        if (
            not billing_approved
            or data.get("api_key_sha256")
            != hashlib.sha256(api_key.encode()).hexdigest()
            or not isinstance(data.get("evidence"), str)
            or not data["evidence"].strip()
            or not timedelta(0) <= age <= timedelta(days=30)
        ):
            raise ValueError("unverified policy")
    except (OSError, KeyError, TypeError, ValueError) as exc:
        raise BillingPolicyError(
            "Current matching-account billing proof within the approved USD 5/month limit is required"
        ) from exc


class RefreshTransport:
    def __init__(self, *, api_key, policy_path, reserve, session=None):
        if not api_key:
            raise BillingPolicyError("Explicit Firecrawl credential required")
        verify_billing_policy(policy_path, api_key)
        self.key, self.policy_path, self.reserve = api_key, policy_path, reserve
        # requests defaults to zero retries; no SDK or implicit credential loading.
        self.session = session or requests.Session()
        # Never let ~/.netrc or proxy environment change the verified identity.
        self.session.trust_env = False
        if getattr(self.session, "auth", None) is not None:
            raise BillingPolicyError(
                "Session authentication must not override the selected key"
            )

    def balance(self):
        response = self.session.get(
            API + "/team/credit-usage",
            headers={"Authorization": "Bearer " + self.key},
            timeout=(10, 20),
            allow_redirects=False,
        )
        response.raise_for_status()
        result = response.json()
        value = result.get("data", {}).get("remainingCredits")
        if result.get("success") is not True or type(value) is not int or value < 0:
            raise BillingPolicyError("Credit balance unavailable")
        return value

    def _scrape(self, url, kind, script=None):
        parts = urlsplit(url)
        if (
            parts.scheme != "https"
            or parts.netloc != "mainelistings.com"
            or not parts.path.startswith("/listings")
        ):
            raise RefreshIncomplete("Unexpected scrape destination")
        verify_billing_policy(self.policy_path, self.key)
        payload = {
            "url": url,
            "formats": ["rawHtml" if script else "markdown"],
            "proxy": "basic",
            "parsers": [],
            "maxAge": 0,
            "waitFor": 8000,
            "timeout": 90000,
            "onlyMainContent": False,
        }
        if script:
            payload["actions"] = [
                {"type": "wait", "milliseconds": 5000},
                {"type": "executeJavascript", "script": script},
            ]
        self.reserve(kind, url)
        response = self.session.post(
            API + "/scrape",
            json=payload,
            headers={"Authorization": "Bearer " + self.key},
            timeout=(10, 100),
            allow_redirects=False,
        )
        response.raise_for_status()
        result = response.json()
        data = result.get("data")
        if result.get("success") is not True or not isinstance(data, dict):
            raise RefreshIncomplete("Firecrawl scrape did not return a document")
        status = data.get("metadata", {}).get("statusCode")
        if type(status) is not int or status != 200:
            raise RefreshIncomplete("Listing source response not verified successful")
        text = data.get("rawHtml" if script else "markdown")
        if not isinstance(text, str) or len(text) > 10_000_000:
            raise RefreshIncomplete("Missing or excessive listing source content")
        if any(
            block in text.casefold()
            for block in ("access denied", "captcha", "unexpected occurred")
        ):
            raise RefreshIncomplete("Listing source blocked")
        return data

    def summary(self, town, page):
        query = urlencode(
            {
                "city": town.title(),
                "mls_status": "Active",
                "sort_by": "on_market_date",
                "sort_order": "desc",
                "page": page,
            }
        )
        data = self._scrape("https://mainelistings.com/listings?" + query, "summary")
        text = data["markdown"]
        count, pagination = parse_total_results(text), parse_pagination(text)
        if count == 0 and page == 1 and pagination is None:
            pagination = (1, 1)
        if count is None or pagination is None:
            raise RefreshIncomplete("Missing result count or pagination evidence")
        return SummaryPage(
            town,
            pagination[0],
            pagination[1],
            count,
            parse_active_cards(text),
        )

    def _detail_result(self, url, kind, script):
        data = self._scrape(url, kind, script)
        results = data.get("actions", {}).get("javascriptReturns")
        if not isinstance(results, list) or len(results) != 1:
            raise RefreshIncomplete("Missing structured listing extraction")
        return results[0]

    def detail(self, url):
        result = self._detail_result(url, "new_detail", DETAIL_EXTRACT_JS)
        return parse_detail_response(result)

    def status(self, url):
        result = self._detail_result(url, "status", STATUS_JS)
        data = parse_detail_response(result)
        if not data or not data.get("mls_number"):
            return None
        return data
