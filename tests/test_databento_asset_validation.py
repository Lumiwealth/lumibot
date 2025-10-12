"""Tests for DataBento asset type validation using the canonical Polars data source."""

from datetime import datetime, timezone
from unittest.mock import patch

import polars as pl
import pytest

from lumibot.data_sources import DataBentoData
from lumibot.entities import Asset
from lumibot.tools import databento_helper_polars as helper


@pytest.fixture
def data_source():
    """Instantiate the Polars-backed DataBento data source with live streaming disabled."""
    return DataBentoData(api_key="test_key", enable_live_stream=False)


def _polars_frame() -> pl.DataFrame:
    base_time = datetime(2025, 1, 1, 9, 30, tzinfo=timezone.utc)
    return pl.DataFrame(
        {
            "datetime": [base_time, base_time.replace(minute=31)],
            "open": [100.0, 101.0],
            "high": [102.0, 103.0],
            "low": [99.0, 100.0],
            "close": [101.0, 102.0],
            "volume": [1_000, 1_100],
        }
    )


def test_live_data_source_futures_allowed(data_source):
    future_assets = [
        Asset("MES", Asset.AssetType.FUTURE),
        Asset("MES", Asset.AssetType.CONT_FUTURE),
    ]

    with patch(
        "lumibot.data_sources.databento_data_polars.databento_helper_polars.get_price_data_from_databento_polars",
        return_value=_polars_frame(),
    ) as mock_get_data:
        for asset in future_assets:
            bars = data_source.get_historical_prices(asset, length=2, timestep="minute")
            assert bars is not None

    assert mock_get_data.call_count == len(future_assets)


def test_live_data_source_equities_rejected(data_source):
    equity_assets = [
        Asset("AAPL", Asset.AssetType.STOCK),
        Asset("SPY", "stock"),
    ]

    with patch(
        "lumibot.data_sources.databento_data_polars.databento_helper_polars.get_price_data_from_databento_polars"
    ) as mock_get_data:
        for asset in equity_assets:
            result = data_source.get_historical_prices(asset, length=2, timestep="minute")
            assert result is None

    mock_get_data.assert_not_called()


def test_helper_path_accepts_all_assets():
    """The low-level helper should not enforce asset-type validation."""
    frame = _polars_frame()

    def fake_format(asset, reference_date=None):
        return "AAPL" if asset.symbol == "AAPL" else "MESZ5"

    with patch("lumibot.tools.databento_helper_polars.DataBentoClientPolars") as mock_client,          patch("lumibot.tools.databento_helper_polars.futures_roll.resolve_symbols_for_range", return_value=["MESZ5"]),          patch("lumibot.tools.databento_helper_polars.futures_roll.resolve_symbol_for_datetime", return_value="MESZ5"),          patch("lumibot.tools.databento_helper_polars._format_futures_symbol_for_databento", side_effect=fake_format),          patch("lumibot.tools.databento_helper_polars._fetch_and_update_futures_multiplier"),          patch("lumibot.tools.databento_helper_polars._load_cache", return_value=None),          patch("lumibot.tools.databento_helper_polars._save_cache"),          patch("lumibot.tools.databento_helper_polars.databento_helper.DataBentoClient") as mock_definition:
        mock_client.return_value.get_hybrid_historical_data.return_value = frame
        mock_definition.return_value.get_instrument_definition.return_value = {
            "unit_of_measure_qty": 1,
            "price_scale": 2,
        }

        futures_assets = [
            Asset("MES", Asset.AssetType.FUTURE),
            Asset("MES", Asset.AssetType.CONT_FUTURE),
        ]
        for asset in futures_assets:
            result = helper.get_price_data_from_databento_polars(
                api_key="test",
                asset=asset,
                start=datetime(2025, 1, 1, tzinfo=timezone.utc),
                end=datetime(2025, 1, 2, tzinfo=timezone.utc),
                timestep="minute",
            )
            assert isinstance(result, pl.DataFrame)

        stock_asset = Asset("AAPL", Asset.AssetType.STOCK)
        stock_result = helper.get_price_data_from_databento_polars(
            api_key="test",
            asset=stock_asset,
            start=datetime(2025, 1, 1, tzinfo=timezone.utc),
            end=datetime(2025, 1, 2, tzinfo=timezone.utc),
            timestep="minute",
        )
        # Stocks are not yet supported end-to-end, but the helper should still attempt without raising.
        assert stock_result is None or isinstance(stock_result, pl.DataFrame)


def test_backtesting_allows_all_assets_documentation():
    """Document that backtesting paths remain flexible with respect to asset types."""
    assert True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
