"""Failure alerts go to the Project Board as OPS_ALERT log lines, not email."""
from __future__ import annotations

import json
from unittest.mock import patch

import pytest

from src import maine_main, maine_notifier, ops_alert


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    ops_alert.errors_fired.clear()
    for key in ('OPS_ALERT_EMAIL', 'OPS_ALERT_ERRORS_FILE', 'GITHUB_ACTIONS',
                'OPS_ALERT_APP'):
        monkeypatch.delenv(key, raising=False)
    # Never let a developer's ~/.env turn the email path on during tests.
    monkeypatch.setattr(maine_notifier, '_get_env',
                        lambda key: __import__('os').environ.get(key))
    yield
    ops_alert.errors_fired.clear()


def _lines(text: str) -> list[dict]:
    return [json.loads(line.split('OPS_ALERT ', 1)[1])
            for line in text.splitlines() if 'OPS_ALERT ' in line]


def test_line_matches_contract(capsys):
    ops_alert.ops_alert('k', 'Title', 'Detail', severity='warn')
    rec = _lines(capsys.readouterr().err)[0]
    assert rec['v'] == 1
    assert rec['app'] == 'gw-re-agent-scraper'
    assert (rec['key'], rec['severity'], rec['state']) == ('k', 'warn', 'firing')
    assert rec['title'] == 'Title' and rec['detail'] == 'Detail'
    assert rec['ts'].endswith('+00:00')
    assert ops_alert.errors_fired == []


def test_error_is_recorded_and_annotated_on_actions(capsys, monkeypatch):
    monkeypatch.setenv('GITHUB_ACTIONS', 'true')
    ops_alert.ops_alert('broken', 'It broke: badly', 'line1\nline2')
    out = capsys.readouterr().out
    assert '::error title=It broke%3A badly::line1%0Aline2' in out
    assert ops_alert.errors_fired == ['broken']


def test_failure_logs_and_sends_no_email_by_default(capsys):
    with patch.object(maine_notifier, 'send_email') as email, \
            patch.object(maine_notifier, 'send_pushover'):
        maine_notifier.notify_failure('ctx', 'raw error', key='some_key',
                                      detail='plain detail')
    email.assert_not_called()
    rec = _lines(capsys.readouterr().err)[0]
    assert rec['key'] == 'some_key'
    assert rec['detail'] == 'plain detail'
    assert ops_alert.errors_fired == ['some_key']


@pytest.mark.parametrize('value', ['1', 'true', 'TRUE'])
def test_rollback_switch_restores_failure_email(monkeypatch, value):
    monkeypatch.setenv('OPS_ALERT_EMAIL', value)
    with patch.object(maine_notifier, 'send_email') as email, \
            patch.object(maine_notifier, 'send_pushover'):
        maine_notifier.notify_failure('ctx', 'raw error', key='k')
    email.assert_called_once()
    assert email.call_args.kwargs['subject'] == '[Maine Listings] FAILURE: ctx'


def test_run_summary_email_is_unchanged():
    with patch.object(maine_notifier, 'send_email') as email, \
            patch.object(maine_notifier, 'send_pushover'):
        maine_notifier.notify_success('Enrichment complete: 1/1')
    email.assert_called_once()


def test_circuit_breaker_is_an_error(capsys):
    with patch.object(maine_notifier, 'send_email'), \
            patch.object(maine_notifier, 'send_pushover'):
        maine_main._notify_enrichment_result(
            {'enriched': 3, 'failed': 20, 'total': 50, 'aborted': True}, 'r1')
    rec = _lines(capsys.readouterr().err)[0]
    assert rec['key'] == 'maine_enrichment_circuit_breaker'
    assert rec['severity'] == 'error'
    assert ops_alert.errors_fired == ['maine_enrichment_circuit_breaker']


def test_elevated_failure_rate_is_a_warning(capsys):
    with patch.object(maine_notifier, 'send_email'), \
            patch.object(maine_notifier, 'send_pushover'):
        maine_main._notify_enrichment_result(
            {'enriched': 100, 'failed': 50, 'total': 150}, 'r1')
    rec = _lines(capsys.readouterr().err)[0]
    assert rec['key'] == 'maine_enrichment_failure_rate'
    assert rec['severity'] == 'warn'
    assert ops_alert.errors_fired == []


def test_exit_code_is_nonzero_after_an_error_alert():
    assert maine_main._alert_exit_code() == 0
    ops_alert.errors_fired.append('k')
    assert maine_main._alert_exit_code() == 1


def test_errors_file_defers_failure_to_the_last_workflow_step(tmp_path,
                                                              monkeypatch):
    path = tmp_path / 'errors'
    monkeypatch.setenv('OPS_ALERT_ERRORS_FILE', str(path))
    ops_alert.errors_fired.extend(['a', 'b'])
    assert maine_main._alert_exit_code() == 0
    assert path.read_text() == 'a\nb\n'
