"""Tests for the anomaly alert path (zero-delta run → notify_failure)."""
from __future__ import annotations

from unittest.mock import patch

from src.maine_main import (
    detect_daily_active_anomaly,
    send_anomaly_alert,
)


class TestAnomalyDetector:
    def test_zero_new_zero_changes_across_all_towns_triggers(self):
        result = {
            'towns_scraped': 10,
            'new_listings': 0,
            'status_changes': 0,
        }
        assert detect_daily_active_anomaly(result) is True

    def test_any_new_listing_prevents_anomaly(self):
        result = {'towns_scraped': 10, 'new_listings': 1, 'status_changes': 0}
        assert detect_daily_active_anomaly(result) is False

    def test_any_status_change_prevents_anomaly(self):
        result = {'towns_scraped': 10, 'new_listings': 0, 'status_changes': 1}
        assert detect_daily_active_anomaly(result) is False

    def test_partial_town_coverage_no_anomaly(self):
        """If fewer than all towns were scraped, don't fire — per-town
        failures have their own alerts."""
        result = {'towns_scraped': 3, 'new_listings': 0, 'status_changes': 0}
        assert detect_daily_active_anomaly(result) is False


class TestAnomalyNotification:
    @patch('src.maine_main.notify_failure')
    def test_anomaly_sends_failure_notification(self, mock_notify):
        send_anomaly_alert(run_id='test-123')
        mock_notify.assert_called_once()
        args, kwargs = mock_notify.call_args
        assert 'suspicious' in args[0].lower() or 'anomaly' in args[0].lower()


class TestAlreadyRefreshedToday:
    """The weekday incremental refresh writes the same database earlier on
    Monday mornings, so the weekly scan legitimately finds nothing new."""

    def test_refresh_earlier_today_suppresses_anomaly(self):
        result = {'towns_scraped': 10, 'new_listings': 0, 'status_changes': 0}
        assert detect_daily_active_anomaly(
            result, already_refreshed_today=854) is False

    def test_no_earlier_refresh_still_triggers(self):
        result = {'towns_scraped': 10, 'new_listings': 0, 'status_changes': 0}
        assert detect_daily_active_anomaly(
            result, already_refreshed_today=0) is True


class TestCountActiveSeenOn:
    def _conn(self):
        import sqlite3
        from src.maine_database import init_db
        conn = sqlite3.connect(':memory:')
        conn.row_factory = sqlite3.Row
        init_db(conn)
        return conn

    def _add(self, conn, url, status, last_seen):
        conn.execute(
            'INSERT INTO maine_transactions '
            '(detail_url, status, last_seen_at, discovered_at, scraped_at) '
            'VALUES (?, ?, ?, ?, ?)',
            (url, status, last_seen, '2026-11-01', '2026-11-01'))

    def test_counts_only_current_listings_seen_that_day(self):
        from src.maine_database import count_active_seen_on
        conn = self._conn()
        # Both timestamp styles in the live database: naive and +00:00.
        self._add(conn, 'u1', 'Active', '2026-11-16T10:43:06.165598')
        self._add(conn, 'u2', 'Pending', '2026-11-16T10:23:03.245762+00:00')
        self._add(conn, 'u3', 'Active Under Contract', '2026-11-16T10:30:00')
        self._add(conn, 'u4', 'Active', '2026-11-11T10:00:00')
        self._add(conn, 'u5', 'Closed', '2026-11-16T10:00:00')
        self._add(conn, 'u6', 'Active', None)
        assert count_active_seen_on(conn, '2026-11-16') == 3
        assert count_active_seen_on(conn, '2026-11-17') == 0
