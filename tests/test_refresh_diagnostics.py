"""Offline response preservation; never include request credentials/headers."""

import json

import pytest

from src.active_refresh import RefreshIncomplete
from src.refresh_transport import RefreshTransport
from tests.test_refresh_transport import Session, proof


def transport(tmp_path, document):
    return RefreshTransport(
        api_key="fixture-key",
        policy_path=proof(tmp_path),
        reserve=lambda *args: None,
        session=Session(document),
        diagnostics_path=tmp_path / "diagnostics",
    )


def test_failed_parser_response_is_preserved_before_parse_without_credentials(tmp_path):
    client = transport(
        tmp_path,
        {
            "markdown": "1 Results\n1 of 1\nBrought to you by broken fixture-key",
            "metadata": {"statusCode": 200, "headers": {"Authorization": "secret"}},
            "private": "not part of a diagnostic",
        },
    )
    with pytest.raises(RefreshIncomplete):
        client.summary("york", 1)
    files = list((tmp_path / "diagnostics").glob("*.json"))
    assert len(files) == 1
    raw = files[0].read_text()
    assert (
        "fixture-key" not in raw and "Authorization" not in raw and "private" not in raw
    )
    record = json.loads(raw)
    assert (
        record["markdown"] == "1 Results\n1 of 1\nBrought to you by broken [REDACTED]"
    )
    assert record["status_code"] == 200 and record["truncated"] is False
    assert "city=York" in record["url"]
    assert files[0].stat().st_mode & 0o777 == 0o600


def test_blocked_response_retained_and_repeated_response_not_overwritten(tmp_path):
    client = transport(
        tmp_path, {"markdown": "access denied", "metadata": {"statusCode": 403}}
    )
    for _ in range(2):
        with pytest.raises(RefreshIncomplete):
            client.summary("york", 1)
    assert len(list((tmp_path / "diagnostics").glob("*.json"))) == 2


def test_diagnostic_can_replay_successful_summary_offline(tmp_path):
    client = transport(
        tmp_path, {"markdown": "0 Results", "metadata": {"statusCode": 200}}
    )
    expected = client.summary("york", 1)
    record = json.loads(next((tmp_path / "diagnostics").glob("*.json")).read_text())
    replay = transport(
        tmp_path,
        {
            "markdown": record["markdown"],
            "metadata": {"statusCode": record["status_code"]},
        },
    )
    assert replay.summary("york", 1) == expected


def test_diagnostics_are_bounded_and_stop_before_another_paid_request(tmp_path):
    client = transport(
        tmp_path, {"markdown": "x" * 10000, "metadata": {"statusCode": 200}}
    )
    client.diagnostics.max_record_bytes = 1024
    client.diagnostics.max_total_bytes = 1024
    with pytest.raises(RefreshIncomplete):
        client.summary("york", 1)
    record_path = next((tmp_path / "diagnostics").glob("*.json"))
    assert record_path.stat().st_size <= 1024
    assert json.loads(record_path.read_text())["truncated"] is True
    with pytest.raises(RefreshIncomplete, match="Diagnostic"):
        client.summary("york", 1)
    assert len(client.session.posts) == 1


def test_unicode_and_json_escapes_respect_byte_limit(tmp_path):
    client = transport(
        tmp_path, {"markdown": '\u2603"\\\n' * 5000, "metadata": {"statusCode": 200}}
    )
    client.diagnostics.max_record_bytes = 1024
    with pytest.raises(RefreshIncomplete):
        client.summary("york", 1)
    path = next((tmp_path / "diagnostics").glob("*.json"))
    assert path.stat().st_size <= 1024
    assert json.loads(path.read_text())["truncated"] is True


def test_record_count_cap_stops_before_paid_call(tmp_path):
    client = transport(
        tmp_path, {"markdown": "0 Results", "metadata": {"statusCode": 200}}
    )
    client.diagnostics.max_records = 1
    client.summary("york", 1)
    with pytest.raises(RefreshIncomplete, match="Diagnostic"):
        client.summary("york", 1)
    assert len(client.session.posts) == 1
