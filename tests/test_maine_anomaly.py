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
