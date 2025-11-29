import os
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import polars as pl

from lumibot.data_sources import DataBentoData
from lumibot.entities import Asset, Bars


class TestDataBentoData(unittest.TestCase):
    """Unit tests for the canonical DataBento data source (Polars-backed)."""

    def setUp(self):
        self.api_key = "test_api_key"
        patchers = [
            patch("lumibot.tools.databento_helper.DATABENTO_AVAILABLE", True),
            patch("lumibot.tools.databento_helper_polars.DATABENTO_AVAILABLE", True),
            patch("lumibot.tools.databento_helper_polars.DataBentoClient", MagicMock()),
        patch("lumibot.tools.databento_helper_polars._fetch_and_update_futures_multiplier", lambda *args, **kwargs: None),
        ]
        for patcher in patchers:
            patched = patcher.start()
            self.addCleanup(patcher.stop)

        import importlib

        polars_module = importlib.import_module("lumibot.data_sources.databento_data_polars")
        patcher_db = patch.object(polars_module, "db", MagicMock())
        patcher_db.start()
        self.addCleanup(patcher_db.stop)

        self.future_asset = Asset(
            symbol="ES",
            asset_type=Asset.AssetType.FUTURE,
            expiration=datetime(2025, 3, 15).date(),
        )
        self.cont_future_asset = Asset(
            symbol="MES",
            asset_type=Asset.AssetType.CONT_FUTURE,
        )
        self.equity_asset = Asset("AAPL", asset_type=Asset.AssetType.STOCK)
        # Disable live streaming threads for unit-speed tests
        self.datasource_kwargs = {"api_key": self.api_key, "enable_live_stream": False}

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _polars_ohlcv(rows: int = 3) -> pl.DataFrame:
        base_time = datetime(2025, 1, 1, 9, 30, tzinfo=timezone.utc)
        minutes = [base_time + timedelta(minutes=i) for i in range(rows)]
        return pl.DataFrame(
            {
                "datetime": minutes,
                "open": [100.0 + i for i in range(rows)],
                "high": [101.0 + i for i in range(rows)],
                "low": [99.0 + i for i in range(rows)],
                "close": [100.5 + i for i in range(rows)],
                "volume": [1_000 + 10 * i for i in range(rows)],
            }
        )

    def _bars(self, rows: int = 2) -> Bars:
        df = self._polars_ohlcv(rows)
        return Bars(df=df, source="DATABENTO", asset=self.future_asset)

    # ------------------------------------------------------------------
    # Initialization
    # ------------------------------------------------------------------
    def test_initialization_sets_core_attributes(self):
        data_source = DataBentoData(**self.datasource_kwargs)

        self.assertEqual(data_source._api_key, self.api_key)
        self.assertEqual(data_source.SOURCE, "DATABENTO")
        # Live streaming disabled for tests should be reflected on the instance
        self.assertFalse(data_source.enable_live_stream)
        # Name comes from DataSource base class
        self.assertEqual(data_source.name, "data_source")

    # ------------------------------------------------------------------
    # Historical data
    # ------------------------------------------------------------------
    def test_get_historical_prices_returns_bars(self):
        with patch(
            "lumibot.data_sources.databento_data_polars.databento_helper_polars.get_price_data_from_databento_polars",
            return_value=self._polars_ohlcv(3),
        ) as mock_get_data:
            data_source = DataBentoData(**self.datasource_kwargs)
            bars = data_source.get_historical_prices(
                asset=self.future_asset,
                length=3,
                timestep="minute",
            )

        self.assertIsInstance(bars, Bars)
        self.assertEqual(len(bars.df), 3)
        mock_get_data.assert_called_once()

    def test_get_historical_prices_returns_none_for_non_futures(self):
        with patch(
            "lumibot.data_sources.databento_data_polars.databento_helper_polars.get_price_data_from_databento_polars"
        ) as mock_get_data:
            data_source = DataBentoData(**self.datasource_kwargs)
            result = data_source.get_historical_prices(
                asset=self.equity_asset,
                length=5,
                timestep="minute",
            )

        self.assertIsNone(result)
        mock_get_data.assert_not_called()

    def test_get_historical_prices_handles_exceptions(self):
        data_source = DataBentoData(**self.datasource_kwargs)
        with patch(
            "lumibot.data_sources.databento_data_polars.databento_helper_polars.get_price_data_from_databento_polars",
            side_effect=RuntimeError("boom"),
        ):
            with self.assertRaises(RuntimeError):
                data_source.get_historical_prices(
                    asset=self.future_asset,
                    length=2,
                    timestep="minute",
                )

    def test_get_historical_prices_trims_to_requested_length(self):
        with patch(
            "lumibot.data_sources.databento_data_polars.databento_helper_polars.get_price_data_from_databento_polars",
            return_value=self._polars_ohlcv(10),
        ):
            data_source = DataBentoData(**self.datasource_kwargs)
            bars = data_source.get_historical_prices(
                asset=self.future_asset,
                length=4,
                timestep="minute",
            )

        self.assertEqual(len(bars.df), 4)
        self.assertTrue((bars.df.index[-1] > bars.df.index[0]))

    # ------------------------------------------------------------------
    # Last price & quotes
    # ------------------------------------------------------------------
    def test_get_last_price_uses_historical_fallback(self):
        frame = self._polars_ohlcv(2)
        last_close = float(frame.select("close").to_series().tail(1)[0])

        with patch(
            "lumibot.data_sources.databento_data_polars.databento_helper_polars.get_price_data_from_databento_polars",
            return_value=frame,
        ):
            data_source = DataBentoData(**self.datasource_kwargs)
            price = data_source.get_last_price(asset=self.future_asset)

        self.assertEqual(price, last_close)

    def test_get_last_price_returns_none_when_no_data(self):
        with patch(
            "lumibot.data_sources.databento_data_polars.databento_helper_polars.get_price_data_from_databento_polars",
            return_value=None,
        ):
            data_source = DataBentoData(**self.datasource_kwargs)
            price = data_source.get_last_price(asset=self.future_asset)

        self.assertIsNone(price)

    def test_get_quote_falls_back_to_last_price(self):
        with patch.object(DataBentoData, "get_last_price", return_value=123.45):
            data_source = DataBentoData(**self.datasource_kwargs)
            quote = data_source.get_quote(asset=self.future_asset)

        self.assertEqual(quote.asset, self.future_asset)
        self.assertEqual(quote.price, 123.45)
        self.assertGreaterEqual(quote.ask, quote.bid)

    # ------------------------------------------------------------------
    # Continuous futures resolution
    # ------------------------------------------------------------------
    def test_continuous_future_resolves_symbol(self):
        with patch(
            "lumibot.data_sources.databento_data_polars.databento_helper_polars.get_price_data_from_databento_polars",
            return_value=self._polars_ohlcv(2),
        ):
            data_source = DataBentoData(**self.datasource_kwargs)
            bars = data_source.get_historical_prices(
                asset=self.cont_future_asset,
                length=2,
                timestep="minute",
            )

        self.assertIsNotNone(bars)
        self.assertEqual(bars.asset, self.cont_future_asset)

    # ------------------------------------------------------------------
    # Integration-style helpers (mocked)
    # ------------------------------------------------------------------
    def test_environment_dates_integration(self):
        from dotenv import load_dotenv

        env_path = "/Users/robertgrzesik/Documents/Development/Strategy Library/Alligator Futures Bot Strategy/src/.env"
        if os.path.exists(env_path):
            load_dotenv(env_path)

        with patch.object(DataBentoData, "get_historical_prices", return_value=self._bars(3)) as mock_get_hist:
            data_source = DataBentoData(**self.datasource_kwargs)
            bars = data_source.get_historical_prices(
                asset=self.cont_future_asset,
                length=60,
                timestep="minute",
            )

        mock_get_hist.assert_called_once()
        self.assertIsNotNone(bars)
        self.assertEqual(len(bars.df), 3)

    def test_mes_strategy_logic_simulation(self):
        data_source = DataBentoData(**self.datasource_kwargs)
        mock_bars = MagicMock()
        mock_df = pd.DataFrame(
            {
                "open": [4500 + i for i in range(60)],
                "high": [4510 + i for i in range(60)],
                "low": [4490 + i for i in range(60)],
                "close": [4505 + i for i in range(60)],
                "volume": [1_000 + i * 10 for i in range(60)],
            },
            index=pd.date_range(start=datetime(2024, 6, 10, 8, 0), periods=60, freq="min"),
        )
        mock_bars.df = mock_df

        with patch.object(data_source, "get_historical_prices", return_value=mock_bars):
            bars = data_source.get_historical_prices(
                asset=self.cont_future_asset,
                length=60,
                timestep="minute",
            )

        self.assertEqual(len(bars.df), 60)
        current_price = bars.df["close"].iloc[-1]
        sma_60 = bars.df["close"].mean()

        self.assertGreater(current_price, sma_60)
        self.assertGreater(current_price, 4500)


if __name__ == "__main__":
    unittest.main()
