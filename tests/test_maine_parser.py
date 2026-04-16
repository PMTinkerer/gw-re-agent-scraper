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
