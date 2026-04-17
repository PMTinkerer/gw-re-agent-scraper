"""Tests for src/maine_parser.py escape decoding and detail response parsing."""
from __future__ import annotations

from src.maine_parser import _decode_escapes, parse_detail_response


class TestDecodeEscapes:
    def test_forward_slash(self):
        assert _decode_escapes(r'A\u002FB') == 'A/B'

    def test_multiple_escapes(self):
        assert _decode_escapes(r'A\u002FB\u0026C') == 'A/B&C'

    def test_plain_string(self):
        assert _decode_escapes('no escapes here') == 'no escapes here'

    def test_non_string_passthrough(self):
        assert _decode_escapes(None) is None
        assert _decode_escapes(42) == 42
        assert _decode_escapes(['list']) == ['list']


class TestParseDetailResponse:
    def test_decodes_escapes_in_fields(self):
        resp = {'value': r'{"listing_office":"Better Homes\u002FMasiello","mls_number":"123"}'}
        parsed = parse_detail_response(resp)
        assert parsed['listing_office'] == 'Better Homes/Masiello'
        assert parsed['mls_number'] == '123'

    def test_returns_none_on_error_field(self):
        resp = {'value': '{"error": "no agent data found"}'}
        assert parse_detail_response(resp) is None

    def test_returns_none_on_invalid_json(self):
        assert parse_detail_response({'value': 'not-json'}) is None

    def test_accepts_raw_string(self):
        parsed = parse_detail_response('{"listing_agent":"Jane","mls_number":"Z"}')
        assert parsed == {'listing_agent': 'Jane', 'mls_number': 'Z'}

    def test_accepts_dict_value(self):
        parsed = parse_detail_response({'value': {'listing_agent': 'Jane'}})
        assert parsed == {'listing_agent': 'Jane'}


# What the browser-side JS returns after extracting from the NUXT blob.
# Covers the new fields the active-listings pipeline depends on.
ACTIVE_JS_RETURN = (
    '{"mls_status":"Active",'
    '"status":"Active",'
    '"mls_number":"1580001",'
    '"list_price":850000,'
    '"sale_price":null,'
    '"close_date":null,'
    '"list_date":"2026-03-15",'
    '"days_on_market":33,'
    '"property_type":"Single Family Residence",'
    '"year_built":1987,'
    '"lot_sqft":15000,'
    '"description":"Stunning oceanfront retreat with private dock.",'
    '"photo_url":"https://photos.mainelistings.com/abc/hero.jpg",'
    '"listing_agent":"Jane Agent",'
    '"listing_agent_id":"12345",'
    '"listing_agent_email":"jane@example.com",'
    '"listing_office":"Beach Realty",'
    '"buyer_agent":null,'
    '"buyer_agent_id":null,'
    '"buyer_agent_email":null,'
    '"buyer_office":null}'
)


class TestParseActiveListingFields:
    def test_extracts_list_date(self):
        parsed = parse_detail_response(ACTIVE_JS_RETURN)
        assert parsed['list_date'] == '2026-03-15'

    def test_extracts_year_built(self):
        parsed = parse_detail_response(ACTIVE_JS_RETURN)
        assert parsed['year_built'] == 1987

    def test_extracts_lot_sqft(self):
        parsed = parse_detail_response(ACTIVE_JS_RETURN)
        assert parsed['lot_sqft'] == 15000

    def test_extracts_description(self):
        parsed = parse_detail_response(ACTIVE_JS_RETURN)
        assert parsed['description'] == 'Stunning oceanfront retreat with private dock.'

    def test_extracts_photo_url(self):
        parsed = parse_detail_response(ACTIVE_JS_RETURN)
        assert parsed['photo_url'] == 'https://photos.mainelistings.com/abc/hero.jpg'

    def test_extracts_status(self):
        parsed = parse_detail_response(ACTIVE_JS_RETURN)
        assert parsed['status'] == 'Active'
