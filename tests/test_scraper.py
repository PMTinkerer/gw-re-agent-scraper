"""Tests for scraper module — Playwright agent enrichment."""
from __future__ import annotations

import sqlite3
from unittest.mock import MagicMock, patch

import pytest

from src.database import init_db, upsert_transaction
from src.scraper import (
    _extract_agent_data,
    _check_page_status,
    _LISTED_BY_RE,
    _BOUGHT_WITH_RE,
    _COURTESY_RE,
    enrich_agents_from_redfin,
)


@pytest.fixture
def db():
    """In-memory SQLite database with test transactions."""
    conn = sqlite3.connect(':memory:')
    conn.row_factory = sqlite3.Row
    init_db(conn)
    return conn


@pytest.fixture
def populated_db(db):
    """DB with 5 transactions needing enrichment."""
    for i in range(5):
        upsert_transaction(db, {
            'mls_number': f'MLS{i}',
            'address': f'{i} Main St',
            'city': 'York',
            'source_url': f'https://www.redfin.com/ME/York/{i}-Main-St/home/{i}',
            'data_source': 'redfin',
        })
    db.commit()
    return db


# --- Regex pattern tests ---

class TestListedByRegex:
    def test_agent_only(self):
        m = _LISTED_BY_RE.search('Listed by Jane Doe\n')
        assert m and m.group(1).strip() == 'Jane Doe'

    def test_agent_and_office(self):
        m = _LISTED_BY_RE.search('Listed by Jane Doe \u2022 ABC Realty\n')
        assert m
        assert m.group(1).strip() == 'Jane Doe'
        assert m.group(2).strip() == 'ABC Realty'

    def test_agent_dot_separator(self):
        m = _LISTED_BY_RE.search('Listed by Jane Doe \u00b7 ABC Realty\n')
        assert m
        assert m.group(1).strip() == 'Jane Doe'
        assert m.group(2).strip() == 'ABC Realty'

    def test_case_insensitive(self):
        m = _LISTED_BY_RE.search('listed BY Jane Doe\n')
        assert m and m.group(1).strip() == 'Jane Doe'


class TestBoughtWithRegex:
    def test_basic(self):
        m = _BOUGHT_WITH_RE.search('Bought with John Smith\n')
        assert m and m.group(1).strip() == 'John Smith'

    def test_with_office(self):
        m = _BOUGHT_WITH_RE.search('Bought with John Smith \u2022 XYZ Realty\n')
        assert m
        assert m.group(1).strip() == 'John Smith'
        assert m.group(2).strip() == 'XYZ Realty'


class TestCourtesyRegex:
    def test_provided_by(self):
        m = _COURTESY_RE.search('Listing provided by ABC Realty\n')
        assert m and m.group(1).strip() == 'ABC Realty'

    def test_courtesy_of(self):
        m = _COURTESY_RE.search('Listing courtesy of ABC Realty\n')
        assert m and m.group(1).strip() == 'ABC Realty'


# --- _is_blocked tests ---

class TestCheckPageStatus:
    def test_normal_page(self):
        page = MagicMock()
        page.text_content.return_value = 'Property Details - 123 Main St, York ME'
        assert _check_page_status(page) == 'ok'

    def test_captcha_page(self):
        page = MagicMock()
        page.text_content.return_value = 'Please verify you are a human to continue'
        assert _check_page_status(page) == 'captcha'

    def test_cloudfront_error(self):
        page = MagicMock()
        page.text_content.return_value = 'ERROR The request could not be satisfied'
        assert _check_page_status(page) == 'error'

    def test_access_denied(self):
        page = MagicMock()
        page.text_content.return_value = 'Access Denied - You cannot view this page'
        assert _check_page_status(page) == 'error'

    def test_exception_returns_error(self):
        page = MagicMock()
        page.text_content.side_effect = Exception('timeout')
        assert _check_page_status(page) == 'error'


# --- _extract_agent_data tests ---

class TestExtractAgentData:
    def test_agent_card_extraction(self):
        """Strategy 1: .agent-card-wrapper DOM selectors."""
        page = MagicMock()
        # evaluate is called for strategy 1 (agent cards) — return structured data
        page.evaluate.return_value = {
            'listing_agent': 'Jane Doe',
            'listing_office': 'ABC Realty',
        }
        result = _extract_agent_data(page)
        assert result['listing_agent'] == 'Jane Doe'
        assert result['listing_office'] == 'ABC Realty'

    def test_text_pattern_fallback(self):
        """Strategy 2: text patterns when agent cards return nothing."""
        page = MagicMock()
        # Strategy 1 returns empty
        page.evaluate.return_value = {}
        # Strategy 2 uses text_content
        page.text_content.return_value = 'Some info\nListed by Jane Doe \u2022 ABC Realty\nMore info'
        result = _extract_agent_data(page)
        assert result['listing_agent'] == 'Jane Doe'
        assert result['listing_office'] == 'ABC Realty'

    def test_bought_with_pattern(self):
        """Strategy 2: buyer agent via 'Bought with' text pattern."""
        page = MagicMock()
        page.evaluate.return_value = {}
        page.text_content.return_value = 'Bought with Robert Eccles  \u2022 PinePoint Realty\n'
        result = _extract_agent_data(page)
        assert result['buyer_agent'] == 'Robert Eccles'
        assert result['buyer_office'] == 'PinePoint Realty'

    def test_json_ld_fallback(self):
        """Strategy 3: JSON-LD when DOM and text both fail."""
        page = MagicMock()
        call_count = [0]
        def mock_evaluate(script):
            call_count[0] += 1
            if call_count[0] == 1:
                return {}  # Strategy 1: no agent cards
            # Strategy 3: JSON-LD
            return '{"@type": ["Product", "RealEstateListing"], "agent": {"name": "Jane Doe"}, "broker": {"name": "ABC Realty"}}'
        page.evaluate.side_effect = mock_evaluate
        page.text_content.return_value = 'No patterns here'
        result = _extract_agent_data(page)
        assert result['listing_agent'] == 'Jane Doe'
        assert result['listing_office'] == 'ABC Realty'

    def test_all_none_on_empty_page(self):
        page = MagicMock()
        page.evaluate.return_value = None
        page.text_content.return_value = ''
        result = _extract_agent_data(page)
        assert all(v is None for v in result.values())

    def test_json_ld_string_agent(self):
        """Strategy 3: JSON-LD where agent is a plain string, not a dict."""
        page = MagicMock()
        call_count = [0]
        def mock_evaluate(script):
            call_count[0] += 1
            if call_count[0] == 1:
                return {}  # Strategy 1: no agent cards
            # Strategy 3: JSON-LD with string agent and string broker
            return '{"@type": ["RealEstateListing"], "agent": "Jane Doe", "broker": "ABC Realty"}'
        page.evaluate.side_effect = mock_evaluate
        page.text_content.return_value = 'No patterns here'
        result = _extract_agent_data(page)
        assert result['listing_agent'] == 'Jane Doe'
        assert result['listing_office'] == 'ABC Realty'


# --- enrich_agents_from_redfin tests ---

class TestEnrichAgentsFromRedfin:
    def test_empty_queue(self, db):
        result = enrich_agents_from_redfin(db, batch_size=10)
        assert result == {'enriched': 0, 'no_agent': 0, 'errors': 0, 'total_attempted': 0}

    @patch('playwright.sync_api.sync_playwright')
    def test_successful_enrichment(self, mock_pw_ctx, populated_db):
        mock_page = MagicMock()
        mock_browser = MagicMock()
        mock_context = MagicMock()

        # Setup playwright mock
        mock_pw = MagicMock()
        mock_pw_ctx.return_value.__enter__ = MagicMock(return_value=mock_pw)
        mock_pw_ctx.return_value.__exit__ = MagicMock(return_value=False)
        mock_pw.chromium.launch.return_value = mock_browser
        mock_browser.new_context.return_value = mock_context
        mock_context.new_page.return_value = mock_page

        # Mock page extraction — agent card returns data
        mock_page.evaluate.return_value = {
            'listing_agent': 'Jane Doe',
            'listing_office': 'ABC Realty',
        }
        mock_page.text_content.return_value = 'Normal page content'

        result = enrich_agents_from_redfin(populated_db, batch_size=5, headless=True)
        assert result['enriched'] == 5
        assert result['no_agent'] == 0
        assert result['errors'] == 0

        # Verify DB updated
        row = populated_db.execute(
            "SELECT listing_agent, enrichment_status FROM transactions WHERE mls_number = 'MLS0'"
        ).fetchone()
        assert row['listing_agent'] == 'Jane Doe'
        assert row['enrichment_status'] == 'success'

    @patch('playwright.sync_api.sync_playwright')
    def test_stops_on_consecutive_errors(self, mock_pw_ctx, populated_db):
        mock_page = MagicMock()
        mock_browser = MagicMock()
        mock_context = MagicMock()

        mock_pw = MagicMock()
        mock_pw_ctx.return_value.__enter__ = MagicMock(return_value=mock_pw)
        mock_pw_ctx.return_value.__exit__ = MagicMock(return_value=False)
        mock_pw.chromium.launch.return_value = mock_browser
        mock_browser.new_context.return_value = mock_context
        mock_context.new_page.return_value = mock_page

        # All navigations fail
        mock_page.goto.side_effect = Exception('Navigation timeout')

        result = enrich_agents_from_redfin(populated_db, batch_size=5, headless=True)
        # Should stop after 3 consecutive errors
        assert result['errors'] == 3
        assert result['total_attempted'] == 3

    @patch('playwright.sync_api.sync_playwright')
    def test_stops_on_captcha(self, mock_pw_ctx, populated_db):
        mock_page = MagicMock()
        mock_browser = MagicMock()
        mock_context = MagicMock()

        mock_pw = MagicMock()
        mock_pw_ctx.return_value.__enter__ = MagicMock(return_value=mock_pw)
        mock_pw_ctx.return_value.__exit__ = MagicMock(return_value=False)
        mock_pw.chromium.launch.return_value = mock_browser
        mock_browser.new_context.return_value = mock_context
        mock_context.new_page.return_value = mock_page

        # Page loads but is a captcha — _check_page_status returns 'captcha'
        mock_page.text_content.return_value = 'Please verify you are a human'
        mock_page.evaluate.return_value = None
        mock_page.query_selector.return_value = None

        result = enrich_agents_from_redfin(populated_db, batch_size=5, headless=True)
        # Should stop after first captcha detection
        assert result['errors'] == 1
        assert result['total_attempted'] == 1
