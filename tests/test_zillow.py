"""Tests for Zillow pipeline helpers."""
from __future__ import annotations

import sqlite3
import tempfile

import pytest

from src.dashboard import generate_scoped_dashboard
from src.database import (
    build_observation_id,
    build_transaction_match_key,
    get_pending_zillow_profiles,
    get_team_gap_rows,
    init_zillow_db,
    log_team_only_sale,
    normalize_address,
    record_zillow_directory_profile,
    record_zillow_team_member,
    resolve_team_only_sales,
    sha256_text,
    upsert_zillow_transaction,
)
from src.report import query_top_agents, query_top_brokerages
from src.zillow import (
    _classify_zillow_document,
    _extract_profile_card_candidates,
    _parse_page_info,
    _parse_sold_row,
)


@pytest.fixture
def zillow_db():
    conn = sqlite3.connect(':memory:')
    conn.row_factory = sqlite3.Row
    init_zillow_db(conn)
    yield conn
    conn.close()


class TestZillowAddressKeys:
    def test_normalize_address_preserves_unit(self):
        normalized = normalize_address('45 Summer Street #1', 'Kennebunk', 'ME', '04043')
        assert normalized == '45 SUMMER ST UNIT 1 | KENNEBUNK | ME | 04043'

    def test_transaction_match_key(self):
        address_hash = sha256_text('45 SUMMER ST UNIT 1 | KENNEBUNK | ME | 04043')
        key = build_transaction_match_key(address_hash, '2025-11-03', 550000)
        assert key == sha256_text(f'{address_hash}|2025-11-03|550000')

    def test_observation_id(self):
        address_hash = sha256_text('83 BRIXHAM RD | YORK | ME | 03909')
        obs_id = build_observation_id(
            'https://www.zillow.com/profile/1cmckenna',
            'seller',
            address_hash,
            '2025-06-27',
            795000,
        )
        assert obs_id == sha256_text(
            f'https://www.zillow.com/profile/1cmckenna|seller|{address_hash}|2025-06-27|795000'
        )


class TestDirectoryCandidateParsing:
    def test_individual_candidate(self):
        candidates = _extract_profile_card_candidates([{
            'href': 'https://www.zillow.com/profile/1cmckenna',
            'text': 'Cindy McKenna 4.8 (23) Aland Realty $167K - $1.3M price range 9 sales last 12 months 91 sales in York',
        }], 'York')
        assert candidates == [{
            'profile_url': 'https://www.zillow.com/profile/1cmckenna',
            'profile_type': 'individual',
            'local_sales_count': 91,
            'sales_last_12_months': 9,
            'raw_card_text': 'Cindy McKenna 4.8 (23) Aland Realty $167K - $1.3M price range 9 sales last 12 months 91 sales in York',
        }]

    def test_team_candidate(self):
        candidates = _extract_profile_card_candidates([{
            'href': 'https://www.zillow.com/profile/Troy-Williams',
            'text': 'TEAM 5.0 (362) Troy Williams Keller Williams Coastal and Lakes & Mountains Realty $15K - $8.4M team price range 214 team sales last 12 months 1125 team sales in York',
        }], 'York')
        assert candidates[0]['profile_type'] == 'team'
        assert candidates[0]['local_sales_count'] == 1125
        assert candidates[0]['sales_last_12_months'] == 214


class TestSoldRowParsing:
    def test_parse_sold_row(self):
        row = _parse_sold_row({
            'href': 'https://www.zillow.com/homedetails/83-Brixham-Rd-York-ME-03909/123_zpid/',
            'text': '83 Brixham Road York, ME, 03909 Sold date: 6/27/2025 Closing price: $795,000 Represented: Seller',
        })
        assert row['address'] == '83 Brixham Road'
        assert row['city'] == 'York'
        assert row['sale_date'] == '2025-06-27'
        assert row['sale_price'] == 795000
        assert row['represented_side'] == 'seller'
        assert row['transaction_match_key'] is not None

    def test_parse_page_info(self):
        assert _parse_page_info('Page 1 of 20') == (1, 20)


class TestBlockClassification:
    def test_classifies_px_captcha(self):
        status = _classify_zillow_document(
            '<html><head><meta name="description" content="px-captcha"></head>'
            '<body>Before we continue... Press & Hold</body></html>',
            page_url='https://www.zillow.com/professionals/real-estate-agent-reviews/york-me/',
            status_code=200,
        )
        assert status['status'] == 'captcha'

    def test_classifies_http_403_as_blocked(self):
        status = _classify_zillow_document(
            '<html><body>Access denied</body></html>',
            page_url='https://www.zillow.com/profile/example',
            status_code=403,
        )
        assert status['status'] == 'blocked'


class TestZillowStorage:
    def test_profile_queue_and_team_members(self, zillow_db):
        record_zillow_directory_profile(
            zillow_db,
            'York',
            'https://www.zillow.com/profile/team-a',
            'team',
            30,
            raw_card_text='TEAM ... 30 sales in York',
        )
        record_zillow_team_member(
            zillow_db,
            'https://www.zillow.com/profile/team-a',
            'https://www.zillow.com/profile/member-a',
            member_name='Member A',
        )
        pending = get_pending_zillow_profiles(zillow_db, batch_size=10)
        assert pending[0]['profile_type'] == 'team'
        assert any(row['profile_url'] == 'https://www.zillow.com/profile/member-a' for row in pending)

    def test_blocked_profiles_are_retryable(self, zillow_db):
        record_zillow_directory_profile(
            zillow_db,
            'York',
            'https://www.zillow.com/profile/blocked-agent',
            'individual',
            12,
            raw_card_text='Blocked Agent ... 12 sales in York',
        )
        zillow_db.execute("""
            UPDATE zillow_profiles
            SET scrape_status = 'blocked', scrape_attempts = 1
            WHERE profile_url = 'https://www.zillow.com/profile/blocked-agent'
        """)
        zillow_db.commit()

        pending = get_pending_zillow_profiles(zillow_db, batch_size=10)
        assert any(row['profile_url'] == 'https://www.zillow.com/profile/blocked-agent' for row in pending)

    def test_upsert_zillow_transaction_and_resolve_team_gap(self, zillow_db):
        log_team_only_sale(zillow_db, {
            'team_profile_url': 'https://www.zillow.com/profile/team-a',
            'team_name': 'Team A',
            'property_url': 'https://www.zillow.com/homedetails/83-Brixham-Rd-York-ME-03909/123_zpid/',
            'represented_side': 'seller',
            'sale_date': '2025-06-27',
            'sale_price': 795000,
            'normalized_address': '83 BRIXHAM RD | YORK | ME | 03909',
            'normalized_address_hash': sha256_text('83 BRIXHAM RD | YORK | ME | 03909'),
            'transaction_match_key': sha256_text(
                f'{sha256_text("83 BRIXHAM RD | YORK | ME | 03909")}|2025-06-27|795000'
            ),
            'local_town': 'York',
        })
        inserted = upsert_zillow_transaction(zillow_db, {
            'observation_id': build_observation_id(
                'https://www.zillow.com/profile/member-a',
                'seller',
                sha256_text('83 BRIXHAM RD | YORK | ME | 03909'),
                '2025-06-27',
                795000,
            ),
            'agent_profile_url': 'https://www.zillow.com/profile/member-a',
            'represented_side': 'seller',
            'listing_agent': 'Member A',
            'listing_office': 'ABC Realty',
            'address': '83 Brixham Rd',
            'city': 'York',
            'state': 'ME',
            'zip': '03909',
            'sale_date': '2025-06-27',
            'sale_price': 795000,
            'normalized_address': '83 BRIXHAM RD | YORK | ME | 03909',
            'normalized_address_hash': sha256_text('83 BRIXHAM RD | YORK | ME | 03909'),
            'transaction_match_key': sha256_text(
                f'{sha256_text("83 BRIXHAM RD | YORK | ME | 03909")}|2025-06-27|795000'
            ),
            'source_url': 'https://www.zillow.com/homedetails/83-Brixham-Rd-York-ME-03909/123_zpid/',
            'data_source': 'zillow',
            'profile_type': 'individual',
            'local_directory_town': 'York',
            'attribution_confidence': 'profile_individual',
        })
        assert inserted is True
        resolved = resolve_team_only_sales(
            zillow_db,
            sha256_text(f'{sha256_text("83 BRIXHAM RD | YORK | ME | 03909")}|2025-06-27|795000'),
            'seller',
        )
        assert resolved == 1
        assert get_team_gap_rows(zillow_db) == []


class TestScopedQueriesAndDashboard:
    def test_source_and_role_scoping(self, zillow_db):
        upsert_zillow_transaction(zillow_db, {
            'observation_id': 'seller1',
            'agent_profile_url': 'https://www.zillow.com/profile/agent-a',
            'represented_side': 'seller',
            'listing_agent': 'Seller Agent',
            'listing_office': 'Seller Office',
            'address': '1 Main St',
            'city': 'York',
            'state': 'ME',
            'zip': '03909',
            'sale_date': '2025-01-01',
            'sale_price': 500000,
            'source_url': 'https://www.zillow.com/homedetails/1-Main-St-York-ME-03909/1_zpid/',
            'data_source': 'zillow',
            'profile_type': 'individual',
        })
        upsert_zillow_transaction(zillow_db, {
            'observation_id': 'buyer1',
            'agent_profile_url': 'https://www.zillow.com/profile/agent-b',
            'represented_side': 'buyer',
            'buyer_agent': 'Buyer Agent',
            'buyer_office': 'Buyer Office',
            'address': '2 Main St',
            'city': 'York',
            'state': 'ME',
            'zip': '03909',
            'sale_date': '2025-01-02',
            'sale_price': 600000,
            'source_url': 'https://www.zillow.com/homedetails/2-Main-St-York-ME-03909/2_zpid/',
            'data_source': 'zillow',
            'profile_type': 'individual',
        })
        zillow_db.execute('''
            INSERT INTO transactions (
                mls_number, city, sale_price, sale_date,
                listing_agent, listing_office, data_source, scraped_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            'redfin-1', 'York', 700000, '2025-01-03',
            'Redfin Agent', 'Redfin Office', 'redfin', '2026-04-06T00:00:00',
        ))
        zillow_db.commit()

        seller_agents = query_top_agents(zillow_db, source='zillow', role='seller')
        buyer_agents = query_top_agents(zillow_db, source='zillow', role='buyer')
        seller_brokerages = query_top_brokerages(zillow_db, source='zillow', role='seller')

        assert [row['agent_name'] for row in seller_agents] == ['Seller Agent']
        assert [row['agent_name'] for row in buyer_agents] == ['Buyer Agent']
        assert [row['office'] for row in seller_brokerages] == ['Seller Office']

    def test_scoped_dashboard(self, zillow_db):
        upsert_zillow_transaction(zillow_db, {
            'observation_id': 'seller-dashboard',
            'agent_profile_url': 'https://www.zillow.com/profile/agent-a',
            'represented_side': 'seller',
            'listing_agent': 'Seller Agent',
            'listing_office': 'Seller Office',
            'address': '1 Main St',
            'city': 'York',
            'state': 'ME',
            'zip': '03909',
            'sale_date': '2025-01-01',
            'sale_price': 500000,
            'source_url': 'https://www.zillow.com/homedetails/1-Main-St-York-ME-03909/1_zpid/',
            'data_source': 'zillow',
            'profile_type': 'individual',
        })
        zillow_db.commit()

        with tempfile.NamedTemporaryFile(suffix='.html', delete=False) as f:
            path = f.name
        generate_scoped_dashboard(
            zillow_db,
            output_path=path,
            source='zillow',
            role='seller',
            heading='Zillow Seller-Side Leaderboard',
            subtitle='Southern Coastal Maine',
            source_label='Zillow',
            description='Zillow seller-side test dashboard',
        )
        with open(path, encoding='utf-8') as f:
            content = f.read()

        assert 'Zillow Seller-Side Leaderboard' in content
        assert 'Data source: Zillow' in content
        assert 'Seller Agent' in content
