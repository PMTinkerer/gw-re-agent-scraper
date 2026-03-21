"""Tests for state management module."""
from __future__ import annotations

import json
import os
import tempfile

import pytest

from src.state import (
    load_state, save_state, generate_all_chunks, get_next_chunks,
    mark_started, mark_complete, mark_failed,
    is_initial_complete, parse_chunk_key, slug_to_town, track_rapidapi_call,
    _town_slug,
)


class TestTownSlug:
    def test_simple(self):
        assert _town_slug('York') == 'york'

    def test_multi_word(self):
        assert _town_slug('Old Orchard Beach') == 'old_orchard_beach'

    def test_kennebunkport(self):
        assert _town_slug('Kennebunkport') == 'kennebunkport'


class TestGenerateAllChunks:
    def test_chunk_count(self):
        chunks = generate_all_chunks()
        # 10 Redfin + 30 Realtor (10 towns x 3 years) = 40
        assert len(chunks) == 40

    def test_redfin_chunks(self):
        chunks = generate_all_chunks()
        redfin = [c for c in chunks if c.startswith('redfin_')]
        assert len(redfin) == 10
        assert 'redfin_york' in redfin
        assert 'redfin_old_orchard_beach' in redfin

    def test_realtor_chunks(self):
        chunks = generate_all_chunks()
        realtor = [c for c in chunks if c.startswith('realtor_')]
        assert len(realtor) == 30
        assert 'realtor_york_2023' in realtor
        assert 'realtor_york_2024' in realtor
        assert 'realtor_york_2025' in realtor


class TestLoadSaveState:
    def test_load_missing_file(self):
        state = load_state('/nonexistent/path.json')
        assert state['mode'] == 'initial'
        assert len(state['chunks']) == 40

    def test_save_and_load(self):
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as f:
            path = f.name
        try:
            state = load_state(path)
            state['region_ids']['york'] = 12345
            save_state(state, path)

            loaded = load_state(path)
            assert loaded['region_ids']['york'] == 12345
            assert loaded['last_run'] is not None
        finally:
            os.unlink(path)


class TestGetNextChunks:
    def test_pending_first(self):
        state = {'chunks': {
            'redfin_york': {'status': 'pending'},
            'redfin_wells': {'status': 'complete'},
        }}
        chunks = get_next_chunks(state, max_chunks=5)
        assert chunks == ['redfin_york']

    def test_failed_before_pending(self):
        state = {'chunks': {
            'redfin_york': {'status': 'pending'},
            'redfin_wells': {'status': 'failed', 'error': 'timeout'},
        }}
        chunks = get_next_chunks(state, max_chunks=5)
        assert chunks[0] == 'redfin_wells'  # Failed retried first

    def test_redfin_before_realtor(self):
        state = {'chunks': {
            'realtor_york_2023': {'status': 'pending'},
            'redfin_york': {'status': 'pending'},
        }}
        chunks = get_next_chunks(state, max_chunks=5)
        assert chunks[0] == 'redfin_york'

    def test_max_chunks_limit(self):
        state = load_state('/nonexistent/path.json')
        chunks = get_next_chunks(state, max_chunks=3)
        assert len(chunks) == 3

    def test_source_filter(self):
        state = {'chunks': {
            'redfin_york': {'status': 'pending'},
            'realtor_york_2023': {'status': 'pending'},
        }}
        chunks = get_next_chunks(state, max_chunks=5, source_filter='realtor')
        assert all(c.startswith('realtor_') for c in chunks)

    def test_no_pending(self):
        state = {'chunks': {
            'redfin_york': {'status': 'complete'},
        }}
        assert get_next_chunks(state, max_chunks=5) == []


class TestMarkFunctions:
    def test_mark_started(self):
        state = {'chunks': {'redfin_york': {'status': 'pending'}}}
        mark_started(state, 'redfin_york')
        assert state['chunks']['redfin_york']['status'] == 'in_progress'
        assert 'started_at' in state['chunks']['redfin_york']

    def test_mark_complete(self):
        state = {'chunks': {'redfin_york': {'status': 'in_progress'}}}
        mark_complete(state, 'redfin_york', rows=150)
        assert state['chunks']['redfin_york']['status'] == 'complete'
        assert state['chunks']['redfin_york']['rows'] == 150

    def test_mark_failed_increments_retries(self):
        state = {'chunks': {'redfin_york': {'status': 'in_progress'}}}
        mark_failed(state, 'redfin_york', 'HTTP 429')
        assert state['chunks']['redfin_york']['retries'] == 1
        mark_failed(state, 'redfin_york', 'HTTP 429')
        assert state['chunks']['redfin_york']['retries'] == 2


class TestIsInitialComplete:
    def test_incomplete(self):
        state = {'chunks': {
            'redfin_york': {'status': 'complete'},
            'redfin_wells': {'status': 'pending'},
        }}
        assert is_initial_complete(state) is False

    def test_complete(self):
        state = {'chunks': {
            'redfin_york': {'status': 'complete'},
            'redfin_wells': {'status': 'complete'},
        }}
        assert is_initial_complete(state) is True

    def test_empty(self):
        assert is_initial_complete({'chunks': {}}) is False


class TestParseChunkKey:
    def test_redfin(self):
        result = parse_chunk_key('redfin_york')
        assert result == {'source': 'redfin', 'town_slug': 'york', 'year': None}

    def test_redfin_multi_word(self):
        result = parse_chunk_key('redfin_old_orchard_beach')
        assert result == {'source': 'redfin', 'town_slug': 'old_orchard_beach', 'year': None}

    def test_realtor(self):
        result = parse_chunk_key('realtor_york_2024')
        assert result == {'source': 'realtor', 'town_slug': 'york', 'year': 2024}

    def test_realtor_multi_word(self):
        result = parse_chunk_key('realtor_old_orchard_beach_2023')
        assert result == {'source': 'realtor', 'town_slug': 'old_orchard_beach', 'year': 2023}


class TestSlugToTown:
    def test_simple(self):
        assert slug_to_town('york') == 'York'

    def test_multi_word(self):
        assert slug_to_town('old_orchard_beach') == 'Old Orchard Beach'

    def test_unknown(self):
        assert slug_to_town('portland') is None


class TestTrackRapidapiCall:
    def test_first_call(self):
        state = {'rapidapi_calls_this_month': 0, 'rapidapi_month': None}
        assert track_rapidapi_call(state) is True
        assert state['rapidapi_calls_this_month'] == 1

    def test_budget_exhausted(self):
        state = {
            'rapidapi_calls_this_month': 95,
            'rapidapi_month': '2026-03',
        }
        # Manually set current month to match
        from datetime import datetime
        state['rapidapi_month'] = datetime.utcnow().strftime('%Y-%m')
        assert track_rapidapi_call(state) is False
