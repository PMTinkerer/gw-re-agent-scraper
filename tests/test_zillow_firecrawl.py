"""Tests for Firecrawl-based Zillow directory discovery."""
from __future__ import annotations

import sqlite3
from unittest.mock import MagicMock, patch

import pytest

from src.database import get_zillow_connection, init_zillow_db, record_zillow_directory_profile
from src.zillow_firecrawl import (
    _classify_markdown_response,
    _clean_card_text,
    _extract_name_office_and_type,
    parse_agent_cards_from_markdown,
    parse_page_info_from_markdown,
    require_firecrawl_key,
)


# --- Fixtures with real Firecrawl markdown from smoke tests ---

TEAM_CARD_MD = (
    '[![](https://photos.zillowstatic.com/fp/abc-h_l.jpg)\\\\\n'
    '\\\\\n'
    'TEAM\\\\\n'
    '\\\\\n'
    '5.0(361)\\\\\n'
    '\\\\\n'
    '**Troy Williams** Keller Williams Coastal and Lakes & Mountains Realty\\\\\n'
    '\\\\\n'
    '$15K - $8.4Mteam price range\\\\\n'
    '\\\\\n'
    '210team sales last 12 months\\\\\n'
    '\\\\\n'
    '1,124team sales in York](https://www.zillow.com/profile/Troy-Williams-ME-RE)'
)

INDIVIDUAL_CARD_MD = (
    '[![](https://photos.zillowstatic.com/fp/def-h_l.jpg)\\\\\n'
    '\\\\\n'
    '**Cindy McKenna** \\\\\n'
    '\\\\\n'
    '4.8(23)\\\\\n'
    '\\\\\n'
    'Aland Realty\\\\\n'
    '\\\\\n'
    '$167K - $1.3Mprice range\\\\\n'
    '\\\\\n'
    '10sales last 12 months\\\\\n'
    '\\\\\n'
    '91sales in York](https://www.zillow.com/profile/1cmckenna)'
)

NO_SALES_CARD_MD = (
    '[![](https://photos.zillowstatic.com/fp/ghi-h_l.jpg)\\\\\n'
    '\\\\\n'
    '**Jane Morris** \\\\\n'
    '\\\\\n'
    '5.0(24)\\\\\n'
    '\\\\\n'
    'Keller Williams Coastal Realty\\\\\n'
    '\\\\\n'
    'No recent price range\\\\\n'
    '\\\\\n'
    'No sales last 12 months\\\\\n'
    '\\\\\n'
    '6sales in York](https://www.zillow.com/profile/Jane-Morris)'
)

BROKERAGE_CARD_MD = (
    '[![](https://photos.zillowstatic.com/fp/xyz-h_l.jpg)\\\\\n'
    '\\\\\n'
    '5.0(37)\\\\\n'
    '\\\\\n'
    '**RE/MAX Shoreline** \\\\\n'
    '\\\\\n'
    '$95K - $1.4Mprice range\\\\\n'
    '\\\\\n'
    '15sales last 12 months\\\\\n'
    '\\\\\n'
    '340sales in Kittery](https://www.zillow.com/profile/remax-shoreline)'
)

DIRECTORY_PAGE_MD = f'''# Real estate agents in York, ME

1,212 agents found

{TEAM_CARD_MD} {INDIVIDUAL_CARD_MD} {NO_SALES_CARD_MD}

- 1
- 2
- 3
- Page 1 of 25
'''


class TestRequireFirecrawlKey:
    def test_returns_key_when_set(self, monkeypatch):
        monkeypatch.setenv('FIRECRAWL_API_KEY', 'fc-test-key')
        assert require_firecrawl_key() == 'fc-test-key'

    def test_raises_when_missing(self, monkeypatch):
        monkeypatch.delenv('FIRECRAWL_API_KEY', raising=False)
        with pytest.raises(RuntimeError, match='FIRECRAWL_API_KEY'):
            require_firecrawl_key()

    def test_raises_when_blank(self, monkeypatch):
        monkeypatch.setenv('FIRECRAWL_API_KEY', '  ')
        with pytest.raises(RuntimeError, match='FIRECRAWL_API_KEY'):
            require_firecrawl_key()


class TestCleanCardText:
    def test_strips_image_markdown(self):
        result = _clean_card_text('![alt](http://example.com/img.jpg) Hello')
        assert result == 'Hello'

    def test_strips_backslashes(self):
        result = _clean_card_text('Hello\\\\ World')
        assert result == 'Hello World'

    def test_strips_bold(self):
        result = _clean_card_text('**Troy Williams**')
        assert result == 'Troy Williams'

    def test_inserts_space_before_sales(self):
        result = _clean_card_text('91sales in York')
        assert '91 sales in York' in result

    def test_inserts_space_before_team_sales(self):
        result = _clean_card_text('210team sales last 12 months')
        assert '210 team sales last 12 months' in result


class TestExtractNameOfficeAndType:
    def test_team_card(self):
        name, office, etype = _extract_name_office_and_type(
            'TEAM 5.0(361) **Troy Williams** '
            'Keller Williams Coastal and Lakes & Mountains Realty '
            '$15K - $8.4M team price range'
        )
        assert name == 'Troy Williams'
        assert office == 'Keller Williams Coastal and Lakes & Mountains Realty'
        assert etype == 'team'

    def test_individual_with_office(self):
        name, office, etype = _extract_name_office_and_type(
            '4.8(23) **Cindy McKenna** Aland Realty '
            '$167K - $1.3M price range 10 sales last 12 months'
        )
        assert name == 'Cindy McKenna'
        assert office == 'Aland Realty'
        assert etype == 'individual'

    def test_brokerage_no_office(self):
        name, office, etype = _extract_name_office_and_type(
            '5.0(37) **RE/MAX Shoreline** '
            '$95K - $1.4M price range 15 sales last 12 months'
        )
        assert name == 'RE/MAX Shoreline'
        assert office is None
        assert etype == 'brokerage'

    def test_brokerage_name_stays_intact(self):
        name, office, etype = _extract_name_office_and_type(
            '4.9(51) **Coldwell Banker Yorke Realty** '
            '$98K - $2.7M price range 110 sales last 12 months'
        )
        assert name == 'Coldwell Banker Yorke Realty'
        assert office is None
        assert etype == 'brokerage'

    def test_signature_homes_not_split(self):
        name, office, etype = _extract_name_office_and_type(
            '5.0(10) **Signature Homes Real Estate Group** '
            '$200K - $1M price range 5 sales last 12 months'
        )
        assert name == 'Signature Homes Real Estate Group'
        assert office is None
        assert etype == 'brokerage'

    def test_empty_text(self):
        name, office, etype = _extract_name_office_and_type('')
        assert name is None
        assert office is None
        assert etype == 'individual'

    def test_no_bold_markers(self):
        name, office, etype = _extract_name_office_and_type(
            '5.0(10) No bold here $200K price range'
        )
        assert name is None
        assert etype == 'individual'


class TestParseAgentCardsFromMarkdown:
    def test_parses_team_card(self):
        cards = parse_agent_cards_from_markdown(TEAM_CARD_MD, 'York')
        assert len(cards) == 1
        card = cards[0]
        assert card['profile_url'] == 'https://www.zillow.com/profile/Troy-Williams-ME-RE'
        assert card['profile_type'] == 'team'
        assert card['profile_name'] == 'Troy Williams'
        assert 'Keller Williams' in card['office_name']
        assert card['local_sales_count'] == 1124
        assert card['sales_last_12_months'] == 210

    def test_parses_individual_card(self):
        cards = parse_agent_cards_from_markdown(INDIVIDUAL_CARD_MD, 'York')
        assert len(cards) == 1
        card = cards[0]
        assert card['profile_type'] == 'individual'
        assert card['profile_name'] == 'Cindy McKenna'
        assert card['local_sales_count'] == 91

    def test_parses_brokerage_card(self):
        cards = parse_agent_cards_from_markdown(BROKERAGE_CARD_MD, 'Kittery')
        assert len(cards) == 1
        card = cards[0]
        assert card['profile_type'] == 'brokerage'
        assert card['profile_name'] == 'RE/MAX Shoreline'
        assert card['office_name'] is None

    def test_parses_no_recent_sales_card(self):
        cards = parse_agent_cards_from_markdown(NO_SALES_CARD_MD, 'York')
        assert len(cards) == 1
        assert cards[0]['local_sales_count'] == 6
        assert cards[0]['sales_last_12_months'] is None

    def test_parses_full_directory_page(self):
        cards = parse_agent_cards_from_markdown(DIRECTORY_PAGE_MD, 'York')
        assert len(cards) == 3
        types = {c['profile_type'] for c in cards}
        assert 'team' in types

    def test_ignores_wrong_town(self):
        cards = parse_agent_cards_from_markdown(TEAM_CARD_MD, 'Kennebunk')
        assert len(cards) == 0

    def test_empty_markdown(self):
        cards = parse_agent_cards_from_markdown('', 'York')
        assert cards == []


class TestParsePageInfo:
    def test_parses_bullet_format(self):
        md = '- 1\n- 2\n- 3\n- Page 1 of 25\n'
        result = parse_page_info_from_markdown(md)
        assert result == (1, 25)

    def test_parses_inline_format(self):
        md = 'Page 3 of 10'
        result = parse_page_info_from_markdown(md)
        assert result == (3, 10)

    def test_returns_none_when_absent(self):
        result = parse_page_info_from_markdown('No pagination here')
        assert result is None


class TestClassifyMarkdownResponse:
    def test_ok_with_profile_links(self):
        md = 'Some content [agent](https://www.zillow.com/profile/foo)'
        assert _classify_markdown_response(md) == 'ok'

    def test_blocked_captcha(self):
        md = 'Please verify you are human. Captcha challenge.'
        assert _classify_markdown_response(md) == 'blocked'

    def test_blocked_no_profiles(self):
        md = 'Just some navigation and footer content.'
        assert _classify_markdown_response(md) == 'blocked'

    def test_blocked_perimeterx(self):
        md = 'PerimeterX challenge detected px-captcha'
        assert _classify_markdown_response(md) == 'blocked'


class TestDiscoveryIntegration:
    """Integration tests with mocked Firecrawl client and real DB."""

    @pytest.fixture
    def db(self, tmp_path):
        db_path = str(tmp_path / 'test_zillow.db')
        conn = get_zillow_connection(db_path)
        init_zillow_db(conn)
        return conn

    @pytest.fixture
    def mock_state(self):
        return {'towns': {}, 'last_run': None, 'created_at': '2026-04-07'}

    @patch('src.zillow_firecrawl._get_firecrawl_client')
    def test_discover_single_town(self, mock_get_client, db, mock_state, tmp_path):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.scrape.return_value = MagicMock(markdown=DIRECTORY_PAGE_MD)

        from src.zillow_firecrawl import discover_zillow_profiles_firecrawl
        result = discover_zillow_profiles_firecrawl(
            db, mock_state,
            towns=['York'],
            max_pages=1,
            state_path=str(tmp_path / 'state.json'),
            delay=0,
        )

        assert result['towns_processed'] == 1
        assert result['profiles_found'] == 3
        profiles = db.execute('SELECT COUNT(*) FROM zillow_profiles').fetchone()[0]
        assert profiles == 3

    @patch('src.zillow_firecrawl._get_firecrawl_client')
    def test_discover_handles_blocked_page(self, mock_get_client, db, mock_state, tmp_path):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.scrape.return_value = MagicMock(markdown='captcha verify you are human')

        from src.zillow_firecrawl import discover_zillow_profiles_firecrawl
        result = discover_zillow_profiles_firecrawl(
            db, mock_state,
            towns=['York'],
            max_pages=1,
            state_path=str(tmp_path / 'state.json'),
            delay=0,
        )

        assert result['towns_processed'] == 0
        assert mock_state['towns']['york']['status'] == 'failed'

    @patch('src.zillow_firecrawl._get_firecrawl_client')
    def test_stores_correct_classification(self, mock_get_client, db, mock_state, tmp_path):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.scrape.return_value = MagicMock(markdown=DIRECTORY_PAGE_MD)

        from src.zillow_firecrawl import discover_zillow_profiles_firecrawl
        discover_zillow_profiles_firecrawl(
            db, mock_state, towns=['York'], max_pages=1,
            state_path=str(tmp_path / 'state.json'), delay=0,
        )

        types = db.execute(
            'SELECT profile_type, COUNT(*) FROM zillow_profiles GROUP BY profile_type'
        ).fetchall()
        type_map = {r[0]: r[1] for r in types}
        assert 'team' in type_map
        assert type_map.get('brokerage', 0) == 0  # no brokerages in test fixture


class TestDirectoryReport:
    """Test directory report generation with populated DB."""

    @pytest.fixture
    def db(self, tmp_path):
        db_path = str(tmp_path / 'test_zillow.db')
        conn = get_zillow_connection(db_path)
        init_zillow_db(conn)
        # Agents
        record_zillow_directory_profile(
            conn, 'York', 'https://www.zillow.com/profile/agent1',
            'individual', 50, profile_name='Agent One', office_name='Brokerage A',
            sales_last_12_months=10,
        )
        record_zillow_directory_profile(
            conn, 'York', 'https://www.zillow.com/profile/agent2',
            'team', 100, profile_name='Agent Two', office_name='Brokerage A',
            sales_last_12_months=20,
        )
        record_zillow_directory_profile(
            conn, 'Kittery', 'https://www.zillow.com/profile/agent3',
            'individual', 30, profile_name='Agent Three', office_name='Brokerage B',
            sales_last_12_months=5,
        )
        # Brokerage profile
        record_zillow_directory_profile(
            conn, 'York', 'https://www.zillow.com/profile/brokerage-a',
            'brokerage', 200, profile_name='Brokerage A',
            sales_last_12_months=40,
        )
        return conn

    def test_query_top_agents_excludes_brokerages(self, db):
        from src.zillow_directory_report import query_directory_top_agents
        agents = query_directory_top_agents(db, limit=10)
        assert len(agents) == 3
        assert all(a['profile_type'] != 'brokerage' for a in agents)

    def test_query_top_agents_by_town(self, db):
        from src.zillow_directory_report import query_directory_top_agents
        agents = query_directory_top_agents(db, limit=10, town='Kittery')
        assert len(agents) == 1
        assert agents[0]['profile_name'] == 'Agent Three'

    def test_brokerage_leaderboard(self, db):
        from src.zillow_directory_report import query_directory_brokerage_leaderboard
        broks = query_directory_brokerage_leaderboard(db, limit=10)
        assert len(broks) >= 2
        brok_a = next(b for b in broks if b['brokerage'] == 'Brokerage A')
        assert brok_a['direct_sales'] == 200
        assert brok_a['agent_count'] == 2

    def test_brokerage_without_direct_profile(self, db):
        from src.zillow_directory_report import query_directory_brokerage_leaderboard
        broks = query_directory_brokerage_leaderboard(db, limit=10)
        brok_b = next(b for b in broks if b['brokerage'] == 'Brokerage B')
        assert brok_b['direct_sales'] is None
        assert brok_b['agent_sales'] == 30

    def test_generate_leaderboard_has_both_sections(self, db, tmp_path):
        from src.zillow_directory_report import generate_directory_leaderboard
        path = generate_directory_leaderboard(db, str(tmp_path / 'report.md'))
        with open(path) as f:
            content = f.read()
        assert 'Top 20 Brokerages' in content
        assert 'Top 30 Agents' in content
        assert 'Agent Two' in content
        assert 'Brokerage A' in content

    def test_generate_dashboard(self, db, tmp_path):
        from src.zillow_directory_report import generate_directory_dashboard
        path = generate_directory_dashboard(db, str(tmp_path / 'dashboard.html'))
        with open(path) as f:
            content = f.read()
        assert '<!DOCTYPE html>' in content
        assert 'Zillow Leaderboard' in content

    def test_get_directory_stats(self, db):
        from src.zillow_directory_report import get_directory_stats
        stats = get_directory_stats(db)
        assert stats['agents'] == 3
        assert stats['teams'] == 1
        assert stats['individuals'] == 2
        assert stats['brokerages'] == 1
        assert stats['towns_with_data'] == 2
