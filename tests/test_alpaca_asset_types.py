"""Asset identities must not fall through to an unrelated Alpaca equity symbol."""

import datetime as dt
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from lumibot.data_sources import AlpacaData
from lumibot.entities import Asset


@pytest.mark.parametrize("asset_type", ["future", "cont_future", "crypto_future", "forex", "index"])
@pytest.mark.parametrize("operation", ["quote", "last_price", "history", "batch_history"])
def test_unsupported_asset_type_fails_before_any_provider_call(asset_type, operation):
    source = AlpacaData({"API_KEY": "test", "API_SECRET": "test"})
    source._stock_client = Mock()
    source._option_client = Mock()
    source._crypto_client = Mock()
    asset = Asset("ES" if asset_type != "forex" else "EUR", asset_type=asset_type,
                  expiration=dt.date(2026, 12, 18))
    quote = Asset("USD", asset_type="forex")
    with pytest.raises(ValueError, match="Alpaca.*asset type"):
        if operation == "quote":
            source.get_quote(asset, quote=quote)
        elif operation == "last_price":
            source.get_last_price(asset, quote=quote)
        elif operation == "history":
            source.get_historical_prices(asset, 2, timestep="day", quote=quote)
        else:
            source.get_bars([Asset("SPY"), asset], 2, timestep="day", quote=quote)
    for client in (source._stock_client, source._option_client, source._crypto_client):
        assert client.mock_calls == []


@pytest.mark.parametrize("asset_type,symbol", [("stock", "SPY"), ("crypto", "BTC/USD"),
                                               ("option", "SPY261218C00600000")])
def test_supported_quote_retains_identity_and_timestamp(asset_type, symbol):
    source = AlpacaData({"API_KEY": "test", "API_SECRET": "test"})
    timestamp = dt.datetime(2026, 10, 16, 15, 0, tzinfo=dt.timezone.utc)
    native = SimpleNamespace(bid_price=10.0, ask_price=12.0, timestamp=timestamp)
    client = Mock()
    client.get_stock_latest_quote.return_value = {symbol: native}
    client.get_crypto_latest_quote.return_value = {symbol: native}
    client.get_option_latest_quote.return_value = {symbol: native}
    source._stock_client = source._crypto_client = source._option_client = client
    asset = Asset("BTC" if asset_type == "crypto" else "SPY", asset_type=asset_type,
                  expiration=dt.date(2026, 12, 18), strike=600, right="CALL")
    result = source.get_quote(asset, quote=Asset("USD", asset_type="forex"))
    assert result.asset is asset
    assert result.timestamp == timestamp
    assert result.bid == 10.0
    assert result.ask == 12.0
    assert result.price == 11.0
    assert client.mock_calls[0].args[0].symbol_or_symbols == symbol


@pytest.mark.parametrize("asset_type", ["future", "cont_future", "crypto_future", "forex", "index"])
def test_public_strategy_quote_preserves_unsupported_asset_error(asset_type):
    from lumibot.strategies import Strategy

    source = AlpacaData({"API_KEY": "test", "API_SECRET": "test"})
    asset = Asset("ES", asset_type=asset_type, expiration=dt.date(2026, 12, 18))
    strategy = SimpleNamespace(
        _sanitize_user_asset=lambda asset: asset,
        broker=SimpleNamespace(option_source=None, data_source=source),
        log_message=Mock(),
    )
    with pytest.raises(ValueError, match="Alpaca.*asset type"):
        Strategy.get_quote(strategy, asset)


def test_public_strategy_quote_keeps_transient_provider_failure_fallback():
    from lumibot.strategies import Strategy

    asset = Asset("SPY")
    strategy = SimpleNamespace(
        _sanitize_user_asset=lambda asset: asset,
        broker=SimpleNamespace(option_source=None, data_source=SimpleNamespace(
            get_quote=Mock(side_effect=RuntimeError("Provider temporarily unavailable")))),
        log_message=Mock(),
    )
    result = Strategy.get_quote(strategy, asset)
    assert result.asset is asset
    assert result.price is None
    strategy.log_message.assert_called_once()
