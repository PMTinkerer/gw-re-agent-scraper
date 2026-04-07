"""Tests for Firecrawl-based Zillow directory discovery."""
from __future__ import annotations

import sqlite3
from unittest.mock import MagicMock, patch

import pytest

from src.database import get_zillow_connection, init_zillow_db
from src.zillow_firecrawl import (
    _classify_markdown_response,
    _clean_card_text,
    _extract_name_and_office,
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


class TestExtractNameAndOffice:
    def test_team_card(self):
        text = 'TEAM 5.0(361) Troy Williams Keller Williams Coastal and Lakes & Mountains Realty $15K - $8.4M team price range'
        name, office = _extract_name_and_office(text)
        assert name is not None
        assert 'Troy' in name or 'Williams' in name

    def test_individual_card(self):
        text = 'Cindy McKenna Aland Realty $167K - $1.3M price range 10 sales last 12 months'
        name, office = _extract_name_and_office(text)
        assert name is not None

    def test_empty_text(self):
        name, office = _extract_name_and_office('')
        assert name is None
        assert office is None


class TestParseAgentCardsFromMarkdown:
    def test_parses_team_card(self):
        cards = parse_agent_cards_from_markdown(TEAM_CARD_MD, 'York')
        assert len(cards) == 1
        card = cards[0]
        assert card['profile_url'] == 'https://www.zillow.com/profile/Troy-Williams-ME-RE'
        assert card['profile_type'] == 'team'
        assert card['local_sales_count'] == 1124
        assert card['sales_last_12_months'] == 210

    def test_parses_individual_card(self):
        cards = parse_agent_cards_from_markdown(INDIVIDUAL_CARD_MD, 'York')
        assert len(cards) == 1
        card = cards[0]
        assert card['profile_url'] == 'https://www.zillow.com/profile/1cmckenna'
        assert card['profile_type'] == 'individual'
        assert card['local_sales_count'] == 91
        assert card['sales_last_12_months'] == 10

    def test_parses_no_recent_sales_card(self):
        cards = parse_agent_cards_from_markdown(NO_SALES_CARD_MD, 'York')
        assert len(cards) == 1
        assert cards[0]['local_sales_count'] == 6
        assert cards[0]['sales_last_12_months'] is None

    def test_parses_full_directory_page(self):
        cards = parse_agent_cards_from_markdown(DIRECTORY_PAGE_MD, 'York')
        assert len(cards) == 3
        urls = {c['profile_url'] for c in cards}
        assert 'https://www.zillow.com/profile/Troy-Williams-ME-RE' in urls
        assert 'https://www.zillow.com/profile/1cmckenna' in urls

    def test_ignores_wrong_town(self):
        cards = parse_agent_cards_from_markdown(TEAM_CARD_MD, 'Kennebunk')
        assert len(cards) == 0

    def test_empty_markdown(self):
        cards = parse_agent_cards_from_markdown('', 'York')
        assert cards == []

    def test_enriches_with_name_and_office(self):
        cards = parse_agent_cards_from_markdown(INDIVIDUAL_CARD_MD, 'York')
        assert len(cards) == 1
        assert cards[0].get('profile_name') is not None


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
    def test_discover_respects_max_pages(self, mock_get_client, db, mock_state, tmp_path):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.scrape.return_value = MagicMock(markdown=DIRECTORY_PAGE_MD)

        from src.zillow_firecrawl import discover_zillow_profiles_firecrawl
        discover_zillow_profiles_firecrawl(
            db, mock_state,
            towns=['York'],
            max_pages=2,
            state_path=str(tmp_path / 'state.json'),
            delay=0,
        )

        assert mock_client.scrape.call_count == 2

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
    def test_stores_profile_name_and_office(self, mock_get_client, db, mock_state, tmp_path):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.scrape.return_value = MagicMock(markdown=INDIVIDUAL_CARD_MD)

        from src.zillow_firecrawl import discover_zillow_profiles_firecrawl
        discover_zillow_profiles_firecrawl(
            db, mock_state,
            towns=['York'],
            max_pages=1,
            state_path=str(tmp_path / 'state.json'),
            delay=0,
        )

        row = db.execute(
            'SELECT profile_name, office_name FROM zillow_profiles WHERE profile_url = ?',
            ('https://www.zillow.com/profile/1cmckenna',),
        ).fetchone()
        assert row is not None
        assert row['profile_name'] is not None


class TestDirectoryReport:
    """Test directory report generation with populated DB."""

    @pytest.fixture
    def db(self, tmp_path):
        db_path = str(tmp_path / 'test_zillow.db')
        conn = get_zillow_connection(db_path)
        init_zillow_db(conn)
        from src.database import record_zillow_directory_profile
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
        return conn

    def test_query_top_agents(self, db):
        from src.zillow_directory_report import query_directory_top_agents
        agents = query_directory_top_agents(db, limit=10)
        assert len(agents) == 3
        assert agents[0]['total_local_sales'] == 100

    def test_query_top_agents_by_town(self, db):
        from src.zillow_directory_report import query_directory_top_agents
        agents = query_directory_top_agents(db, limit=10, town='Kittery')
        assert len(agents) == 1
        assert agents[0]['profile_name'] == 'Agent Three'

    def test_query_top_brokerages(self, db):
        from src.zillow_directory_report import query_directory_top_brokerages
        brokerages = query_directory_top_brokerages(db, limit=10)
        assert len(brokerages) == 2
        assert brokerages[0]['office_name'] == 'Brokerage A'
        assert brokerages[0]['total_local_sales'] == 150

    def test_generate_leaderboard(self, db, tmp_path):
        from src.zillow_directory_report import generate_directory_leaderboard
        path = generate_directory_leaderboard(db, str(tmp_path / 'report.md'))
        with open(path) as f:
            content = f.read()
        assert 'Agent Two' in content
        assert 'Brokerage A' in content

    def test_generate_dashboard(self, db, tmp_path):
        from src.zillow_directory_report import generate_directory_dashboard
        path = generate_directory_dashboard(db, str(tmp_path / 'dashboard.html'))
        with open(path) as f:
            content = f.read()
        assert '<!DOCTYPE html>' in content
        assert 'Agent Two' in content
        assert 'Zillow Agent Leaderboard' in content

    def test_get_directory_stats(self, db):
        from src.zillow_directory_report import get_directory_stats
        stats = get_directory_stats(db)
        assert stats['total_agents'] == 3
        assert stats['teams'] == 1
        assert stats['individuals'] == 2
        assert stats['towns_with_data'] == 2
