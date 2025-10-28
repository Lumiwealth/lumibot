import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

import polars as pl
import pytz

from lumibot.backtesting.databento_backtesting_polars import DataBentoDataBacktestingPolars
from lumibot.entities import Asset
from lumibot.tools.databento_helper_polars import get_price_data_from_databento_polars


class TestDataBentoDataBacktestingPolars(unittest.TestCase):
    """Regression tests for the polars DataBento backtesting implementation."""

    def setUp(self):
        self.api_key = "test_key"
        self.start_date = datetime(2022, 1, 1)
        self.end_date = datetime(2022, 12, 31)
        self.asset = Asset("MNQ", asset_type=Asset.AssetType.CONT_FUTURE)
        self.utc = pytz.UTC

    @patch("lumibot.tools.databento_helper_polars.get_price_data_from_databento_polars")
    def test_daily_history_is_fetched_once_for_full_range(self, mock_get_data):
        """Daily data should be cached so repeated requests avoid redundant API calls."""
        date_range = pl.datetime_range(
            start=datetime(2021, 12, 1, tzinfo=self.utc),
            end=datetime(2022, 12, 30, tzinfo=self.utc),
            interval="1d",
            eager=True,
        )
        num_rows = len(date_range)
        base_values = [float(i) for i in range(num_rows)]
        mock_get_data.return_value = pl.DataFrame(
            {
                "datetime": date_range,
                "open": [v + 1.0 for v in base_values],
                "high": [v + 2.0 for v in base_values],
                "low": base_values,
                "close": [v + 1.5 for v in base_values],
                "volume": [1000.0] * num_rows,
            }
        )

        datasource = DataBentoDataBacktestingPolars(
            datetime_start=self.start_date,
            datetime_end=self.end_date,
            api_key=self.api_key,
        )

        # First request should trigger a fetch
        datasource._datetime = self.start_date + timedelta(days=40)
        first_bars = datasource.get_historical_prices(
            self.asset,
            length=20,
            timestep="day",
            return_polars=True,
        )

        self.assertIsNotNone(first_bars)
        self.assertGreaterEqual(first_bars.df.height, 20)
        self.assertEqual(mock_get_data.call_count, 1)

        metadata = datasource._cache_metadata.get((self.asset, "day"))
        self.assertIsNotNone(metadata)

        # Subsequent request later in the backtest should use cached data only
        datasource._datetime = self.end_date - timedelta(days=5)
        second_bars = datasource.get_historical_prices(
            self.asset,
            length=20,
            timestep="day",
            return_polars=True,
        )

        self.assertIsNotNone(second_bars)
        self.assertGreaterEqual(second_bars.df.height, 20)
        self.assertEqual(mock_get_data.call_count, 1, "Expected cached data to satisfy the second call")

        metadata = datasource._cache_metadata.get((self.asset, "day"))
        self.assertIsNotNone(metadata)
        max_dt = datasource._to_naive_datetime(metadata.get("max_dt"))
        expected_end = datasource._to_naive_datetime(datasource.datetime_end)
        # Allow a small tolerance because fetched data is midnight whereas the backtest end is end-of-day
        self.assertGreaterEqual(max_dt, expected_end - timedelta(days=2))

    @patch("lumibot.tools.databento_helper_polars.get_price_data_from_databento_polars")
    def test_minute_history_request_has_valid_range(self, mock_get_data):
        """Minute requests should never invert the start/end timestamps handed to DataBento."""

        captured = {}

        def fake_databento_fetch(api_key, asset, start, end, timestep, venue=None, force_cache_update=False, reference_date=None, **kwargs):
            captured["start"] = start
            captured["end"] = end
            date_range = pl.datetime_range(
                start=datetime(2022, 1, 31, 22, 0, tzinfo=self.utc),
                end=datetime(2022, 2, 1, 0, 0, tzinfo=self.utc),
                interval="1m",
                eager=True,
            )
            base_values = [float(i) for i in range(len(date_range))]
            return pl.DataFrame(
                {
                    "datetime": date_range,
                    "open": base_values,
                    "high": [v + 1.0 for v in base_values],
                    "low": [v - 1.0 for v in base_values],
                    "close": base_values,
                    "volume": [10.0] * len(base_values),
                    "symbol": ["MNQH2"] * len(base_values),
                }
            )

        mock_get_data.side_effect = fake_databento_fetch

        datasource = DataBentoDataBacktestingPolars(
            datetime_start=datetime(2022, 1, 1),
            datetime_end=datetime(2022, 1, 31),
            api_key=self.api_key,
        )

        datasource._datetime = pytz.timezone("America/New_York").localize(datetime(2022, 1, 31, 18, 0))
        bars = datasource.get_historical_prices(
            self.asset,
            length=30,
            timestep="minute",
            return_polars=True,
        )

        self.assertIsNotNone(bars)
        self.assertIn("datetime", bars.df.columns)
        self.assertIn("start", captured)
        self.assertIn("end", captured)
        self.assertLess(captured["start"], captured["end"], "Expected start < end for DataBento request")

    @patch("lumibot.tools.databento_helper_polars._load_cache", return_value=None)
    @patch("lumibot.tools.databento_helper_polars._save_cache")
    @patch("lumibot.tools.databento_helper_polars.DataBentoClientPolars.get_hybrid_historical_data")
    def test_continuous_futures_roll_filters_front_month(self, mock_get_range, mock_save_cache, mock_load_cache):
        """Combined contract data should reduce to the front month according to roll rules."""

        def make_df(start_dt, end_dt, symbol_code):
            rng = pl.datetime_range(
                start=start_dt,
                end=end_dt,
                interval="1d",
                eager=True,
            )
            base = [float(i) for i in range(len(rng))]
            return pl.DataFrame(
                {
                    "datetime": rng,
                    "open": base,
                    "high": [v + 1.0 for v in base],
                    "low": [v - 1.0 for v in base],
                    "close": base,
                    "volume": [1_000.0] * len(rng),
                    "symbol": [symbol_code] * len(rng),
                }
            )

        def fetch_side_effect(dataset, symbols, schema, start, end, **kwargs):
            if symbols == "MNQZ4":
                return make_df(
                    datetime(2024, 12, 10, tzinfo=self.utc),
                    datetime(2024, 12, 14, tzinfo=self.utc),
                    "MNQZ4",
                )
            if symbols == "MNQH5":
                return make_df(
                    datetime(2024, 12, 15, tzinfo=self.utc),
                    datetime(2024, 12, 20, tzinfo=self.utc),
                    "MNQH5",
                )
            return pl.DataFrame({})

        mock_get_range.side_effect = fetch_side_effect

        result = get_price_data_from_databento_polars(
            api_key=self.api_key,
            asset=self.asset,
            start=datetime(2024, 12, 10),
            end=datetime(2024, 12, 20),
            timestep="day",
            force_cache_update=True,
        )

        self.assertIsNotNone(result)
        self.assertIn("symbol", result.columns)
        unique_symbols = set(result["symbol"].to_list())
        self.assertEqual(unique_symbols, {"MNQZ4", "MNQH5"})

        # Convert Python datetime to Polars datetime to ensure consistent precision
        roll_date = pl.lit(datetime(2024, 12, 15, tzinfo=self.utc))
        post_roll = result.filter(pl.col("datetime") >= roll_date)
        self.assertTrue((post_roll["symbol"] == "MNQH5").all(), "Expected post-roll data to use next quarter contract")


if __name__ == "__main__":
    unittest.main()
