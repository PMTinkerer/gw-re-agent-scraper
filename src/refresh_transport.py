"""No-email, basic-only Firecrawl adapter for the incremental refresh lane."""

from __future__ import annotations

import hashlib
import json
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlencode, urlsplit

import requests

from .active_refresh import RefreshIncomplete, SummaryPage, _town
from .incremental_cards import ActiveCardParseError, parse_active_cards
from .incremental_detail import INCREMENTAL_DETAIL_JS
from .refresh_diagnostics import SummaryDiagnostics
from .maine_parser import (
    parse_detail_response,
    parse_pagination,
    parse_total_results,
)

TRANSIENT_STATUSES = frozenset({502, 503, 504})
TRANSIENT_BACKOFF_SECONDS = (5, 15)
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


def _result_count_tokens(text):
    """Match whole count tokens without rescanning long non-count tokens.

    Preserve the previous findall pattern's non-overlapping consumption,
    including malformed evidence attached immediately after ``Results``.
    """
    token_pattern = re.compile(r"[^\s*]+")
    suffix_pattern = re.compile(r"\s+Results\b")
    tokens = []
    position = 0
    while token := token_pattern.search(text, position):
        suffix = suffix_pattern.match(text, token.end())
        if suffix:
            tokens.append(token.group())
            position = suffix.end()
        else:
            position = token.end()
    return tokens


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
    def __init__(
        self, *, api_key, policy_path, reserve, session=None, diagnostics_path=None
    ):
        if not api_key:
            raise BillingPolicyError("Explicit Firecrawl credential required")
        verify_billing_policy(policy_path, api_key)
        self.key, self.policy_path, self.reserve = api_key, policy_path, reserve
        self.diagnostics = (
            SummaryDiagnostics(diagnostics_path, redact=api_key)
            if diagnostics_path is not None
            else None
        )
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

    def _post_scrape(self, kind, url, payload):
        """One paid request, retried only for transient provider gateway failures.

        Lucas approved at most two retries (5 s, then 15 s) on 2026-10-04 after a
        single Firecrawl 502 ended an otherwise complete canary. Every attempt is
        reserved first; other errors and content failures still stop the run.
        """
        for attempt, wait in enumerate((0, *TRANSIENT_BACKOFF_SECONDS)):
            last = attempt == len(TRANSIENT_BACKOFF_SECONDS)
            if wait:
                time.sleep(wait)
            self.reserve(kind, url)
            try:
                response = self.session.post(
                    API + "/scrape",
                    json=payload,
                    headers={"Authorization": "Bearer " + self.key},
                    timeout=(10, 100),
                    allow_redirects=False,
                )
            except (requests.ConnectionError, requests.Timeout):
                if last:
                    raise
                continue
            if getattr(response, "status_code", 200) in TRANSIENT_STATUSES and not last:
                continue
            response.raise_for_status()
            return response.json()

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
        if kind == "summary" and self.diagnostics:
            self.diagnostics.check_capacity()
        result = self._post_scrape(kind, url, payload)
        data = result.get("data")
        if kind == "summary" and self.diagnostics and isinstance(data, dict):
            self.diagnostics.save(url, data)
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
                # Public Price ordering avoids observed date-sort overlap. It
                # is not a snapshot: strict coverage and duplicate guards remain.
                "sort_by": "list_price",
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
        try:
            listings = parse_active_cards(text)
        except ActiveCardParseError as exc:
            raise RefreshIncomplete("Malformed Active discovery page") from exc
        return SummaryPage(
            town,
            pagination[0],
            pagination[1],
            count,
            listings,
        )

    def summary_range(self, town, minimum, maximum):
        """Read actual page-one evidence for an inclusive price interval.

        Never page through the result set or declare a truncated probe complete.
        Reservation, billing verification and diagnostics stay in _scrape.
        """
        town = _town(town)
        if any(
            value is not None and (type(value) is not int or value < 0)
            for value in (minimum, maximum)
        ) or (minimum is not None and maximum is not None and minimum > maximum):
            raise RefreshIncomplete("Invalid price bounds")
        query = {
            "city": town.title(),
            "mls_status": "Active",
            "sort_by": "list_price",
            "sort_order": "desc",
            "page": 1,
        }
        if minimum is not None:
            query["min_list_price"] = minimum
        if maximum is not None:
            query["max_list_price"] = maximum
        data = self._scrape(
            "https://mainelistings.com/listings?" + urlencode(query), "summary"
        )
        text = data["markdown"]
        # Unlike the legacy parser, don't interpret '-1' or '1.5' as '1'/'5'.
        # Keep whole malformed tokens, without quadratic long-token backtracking.
        tokens = _result_count_tokens(text)
        if not tokens or any(
            not re.fullmatch(r"(?:\d+|\d{1,3}(?:,\d{3})+)", token) for token in tokens
        ):
            raise RefreshIncomplete("Missing or invalid result count evidence")
        counts = {int(token.replace(",", "")) for token in tokens}
        if len(counts) != 1:
            raise RefreshIncomplete("Conflicting result counts")
        count = counts.pop()
        try:
            listings = parse_active_cards(text)
        except ActiveCardParseError as exc:
            raise RefreshIncomplete("Malformed Active discovery page") from exc
        if len(listings) != min(count, 24) or len(
            {r["detail_url"] for r in listings}
        ) != len(listings):
            raise RefreshIncomplete("Incomplete or duplicate first-page cards")
        pages = re.findall(
            r"(?m)^[ \t*]*(?:Page[ \t]+)?([^\s*]+)[ \t]+of[ \t]+([^\n]*?)[ \t*]*$",
            text,
            re.I,
        )
        expected = (1, max(1, (count + 23) // 24))
        if pages:
            if any(
                not all(re.fullmatch(r"[0-9]+", token) for token in pair)
                or tuple(map(int, pair)) != expected
                for pair in pages
            ):
                raise RefreshIncomplete("Contradictory range pagination")
        elif count > 24 or re.search(r"\b(?:Page\s+)?\d+\s+of\b", text, re.I):
            raise RefreshIncomplete("Missing range pagination evidence")
        # A hidden/omitted paginator cannot make a linked second page disappear.
        if count <= 24 and (
            re.search(r"[?&](?:amp;)?page=(?!1(?:[&#)\s]|$))[^&#)\s]+", text, re.I)
            or re.search(r"\[(?:next|previous|[2-9]\d*)\]\(", text, re.I)
        ):
            raise RefreshIncomplete("Contradictory single-page navigation")
        from .price_discovery import RangePage

        return RangePage(town, minimum, maximum, *expected, count, listings)

    def _detail_result(self, url, kind, script):
        data = self._scrape(url, kind, script)
        results = data.get("actions", {}).get("javascriptReturns")
        if not isinstance(results, list) or len(results) != 1:
            raise RefreshIncomplete("Missing structured listing extraction")
        return results[0]

    def detail(self, url):
        result = self._detail_result(url, "new_detail", INCREMENTAL_DETAIL_JS)
        return parse_detail_response(result)

    def status(self, url):
        result = self._detail_result(url, "status", STATUS_JS)
        data = parse_detail_response(result)
        if not data or not data.get("mls_number"):
            return None
        return data
