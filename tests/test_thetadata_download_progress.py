import unittest
from datetime import date
from unittest.mock import patch

from lumibot.entities import Asset


class TestThetaDownloadProgress(unittest.TestCase):
    def setUp(self):
        from lumibot.tools.thetadata_helper import clear_download_status

        clear_download_status()

    def tearDown(self):
        from lumibot.tools.thetadata_helper import clear_download_status

        clear_download_status()

    def test_finalize_download_status_keeps_progress_payload(self):
        from lumibot.tools.thetadata_helper import (
            finalize_download_status,
            get_download_status,
            set_download_status,
        )

        asset = Asset(symbol="SPY")
        set_download_status(asset, "USD", "ohlc", "minute", 3, 3)
        finalize_download_status()

        status = get_download_status()
        self.assertFalse(status["active"])
        self.assertEqual(status["asset"]["symbol"], "SPY")
        self.assertEqual(status["progress"], 100)
        self.assertEqual(status["current"], 3)
        self.assertEqual(status["total"], 3)

    def test_advance_download_status_progress_preserves_queue_fields(self):
        from lumibot.tools.thetadata_helper import (
            advance_download_status_progress,
            get_download_status,
            set_download_status,
            update_download_status_queue_info,
        )

        asset = Asset(symbol="SPY")
        set_download_status(asset, "USD", "ohlc", "minute", 0, 2)
        update_download_status_queue_info(
            request_id="req-1",
            queue_status="pending",
            queue_position=8,
            estimated_wait=12.0,
            submitted_at=1.0,
        )

        advance_download_status_progress(asset=asset, data_type="ohlc", timespan="minute", step=1)
        status = get_download_status()

        self.assertEqual(status["current"], 1)
        self.assertEqual(status["progress"], 50)
        self.assertEqual(status["request_id"], "req-1")
        self.assertEqual(status["queue_position"], 8)

    def test_update_download_status_queue_info_switches_request_on_submit(self):
        from lumibot.tools.thetadata_helper import (
            get_download_status,
            set_download_status,
            update_download_status_queue_info,
        )

        asset = Asset(symbol="SPY")
        set_download_status(asset, "USD", "ohlc", "minute", 0, 2)

        update_download_status_queue_info(
            request_id="req-1",
            queue_status="pending",
            queue_position=8,
            submitted_at=1.0,
        )
        update_download_status_queue_info(
            request_id="req-2",
            queue_status="pending",
            queue_position=7,
            submitted_at=2.0,
        )
        status = get_download_status()
        self.assertEqual(status["request_id"], "req-2")
        self.assertEqual(status["queue_position"], 7)

        # A refresh from an older request without submitted_at should be ignored.
        update_download_status_queue_info(
            request_id="req-1",
            queue_status="processing",
            queue_position=1,
        )
        status = get_download_status()
        self.assertEqual(status["request_id"], "req-2")
        self.assertEqual(status["queue_position"], 7)

        # A submit with an older submitted_at should also be ignored.
        update_download_status_queue_info(
            request_id="req-3",
            queue_status="pending",
            queue_position=0,
            submitted_at=1.5,
        )
        status = get_download_status()
        self.assertEqual(status["request_id"], "req-2")

    def test_get_historical_data_advances_download_progress_per_trading_day(self):
        from lumibot.tools import thetadata_helper
        from lumibot.tools.thetadata_helper import get_download_status, set_download_status

        asset = Asset(symbol="SPY")
        trading_days = [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)]
        set_download_status(asset, "USD", "ohlc", "minute", 0, len(trading_days))

        with patch.object(thetadata_helper, "get_trading_dates", return_value=trading_days), patch.object(
            thetadata_helper, "get_request", return_value=None
        ):
            thetadata_helper.get_historical_data(
                asset=asset,
                start_dt=trading_days[0],
                end_dt=trading_days[-1],
                ivl=60000,
                datastyle="ohlc",
                include_after_hours=False,
                download_timespan="minute",
            )

        status = get_download_status()
        self.assertEqual(status["current"], len(trading_days))
        self.assertEqual(status["progress"], 100)


if __name__ == "__main__":
    unittest.main()
