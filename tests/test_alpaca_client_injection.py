"""Caller-owned SDK clients preserve ordinary adapter behavior without extra keys."""
from types import SimpleNamespace
from unittest.mock import MagicMock
from datetime import date, datetime, timezone

import pytest

from alpaca.data.historical import (
    CryptoHistoricalDataClient, OptionHistoricalDataClient, StockHistoricalDataClient,
)
from alpaca.trading.client import TradingClient
from lumibot.brokers.alpaca import Alpaca
from lumibot.data_sources.alpaca_data import AlpacaData
from lumibot.trading_builtins import PollingStream
from lumibot.entities import Asset


def clients():
    return dict(
        stock_client=MagicMock(spec=StockHistoricalDataClient),
        crypto_client=MagicMock(spec=CryptoHistoricalDataClient),
        option_client=MagicMock(spec=OptionHistoricalDataClient),
    )


def test_injected_trading_client_reads_balance_without_duplicate_authentication():
    trading = MagicMock(spec=TradingClient)
    trading.get_account.return_value = SimpleNamespace(
        cash="10000.00", long_market_value="250.00", short_market_value="0.00",
        portfolio_value="10250.00",
    )
    data = AlpacaData({"PAPER": True}, **clients())
    broker = Alpaca(
        {"PAPER": True}, trading_client=trading, data_source=data,
        connect_stream=False, start_orders_thread=False,
    )
    assert broker._get_balances_at_broker(None, None) == (10000.0, 250.0, 10250.0)
    trading.get_account.assert_called_once_with()
    assert isinstance(broker._get_stream_object(), PollingStream)
    assert broker.api_key == broker.api_secret == broker.oauth_token == ""


@pytest.mark.parametrize("kind", ["stock", "crypto", "option"])
def test_injected_data_client_survives_authentication_reset(kind):
    supplied = clients()
    data = AlpacaData({"PAPER": True}, **supplied)
    selected = getattr(data, f"_get_{kind}_client")
    assert selected() is supplied[f"{kind}_client"]
    data.reset_auth_failure()
    assert selected() is supplied[f"{kind}_client"]


def test_injecting_one_data_client_does_not_require_unrelated_product_credentials():
    stock = clients()["stock_client"]
    data = AlpacaData({}, stock_client=stock)
    assert data._get_stock_client() is stock


@pytest.mark.parametrize("kind,symbol,asset", [
    ("stock", "SPY", Asset("SPY")),
    ("crypto", "BTC/USD", Asset("BTC", asset_type="crypto")),
    ("option", "SPY270219C00500000", Asset("SPY", asset_type="option",
        expiration=date(2027, 2, 19), strike=500, right="CALL")),
])
def test_quotes_keep_injected_clients_through_reauthentication(kind, symbol, asset):
    supplied = clients()
    response = SimpleNamespace(bid_price=10.0, ask_price=12.0,
        timestamp=datetime(2027, 1, 4, 15, tzinfo=timezone.utc))
    method = getattr(supplied[f"{kind}_client"], f"get_{kind}_latest_quote")
    method.return_value = {symbol: response}
    data = AlpacaData({}, **supplied)
    data._auth_failed = True
    quote = data.get_quote(asset)
    assert (quote.bid, quote.ask, quote.price) == (10.0, 12.0, 11.0)
    method.assert_called_once()


def test_injected_client_keeps_provider_permission_errors_visible():
    trading = MagicMock(spec=TradingClient)
    trading.get_account.side_effect = PermissionError("account access denied")
    data = AlpacaData({}, **clients())
    broker = Alpaca({}, trading_client=trading, data_source=data,
                    connect_stream=False, start_orders_thread=False)
    with pytest.raises(PermissionError, match="account access denied"):
        broker._get_balances_at_broker(None, None)


def test_authentication_diagnostic_does_not_expose_token_or_raw_provider_body(caplog):
    token = "synthetic-private-credential-8dd034c7"
    data = AlpacaData({"OAUTH_TOKEN": token})
    with pytest.raises(ValueError) as error:
        data._handle_auth_error(ValueError(f"401 unauthorized: {token}"), "quote")
    assert token not in str(error.value)
    assert token[:10] not in str(error.value)
    assert token not in caplog.text
    assert data._auth_failed


@pytest.mark.parametrize('make_config', [dict, SimpleNamespace])
def test_configured_clients_work_with_default_data_source(make_config):
    supplied = clients()
    trading = MagicMock(spec=TradingClient)
    config = make_config(PAPER=True, TRADING_CLIENT=trading,
                         **{name.upper(): client for name, client in supplied.items()})
    standalone_data = AlpacaData(config)
    standalone_data.reset_auth_failure()
    for kind in ("stock", "crypto", "option"):
        assert getattr(standalone_data, f"_get_{kind}_client")() is supplied[f"{kind}_client"]
    broker = Alpaca(config, connect_stream=False, start_orders_thread=False)
    assert broker.api is trading
    assert broker._create_trading_client() is trading
    for kind in ('stock', 'crypto', 'option'):
        assert getattr(broker.data_source, f'_get_{kind}_client')() is supplied[f'{kind}_client']


def test_explicit_clients_take_precedence_over_configured_clients():
    configured = clients()
    stock = MagicMock(spec=StockHistoricalDataClient)
    data = AlpacaData({name.upper(): client for name, client in configured.items()}, stock_client=stock)
    assert data._get_stock_client() is stock
    assert data._get_crypto_client() is configured['crypto_client']
    data.reset_auth_failure()
    assert data._get_stock_client() is stock
    original_trading = MagicMock(spec=TradingClient)
    replacement = MagicMock(spec=TradingClient)
    broker = Alpaca({'TRADING_CLIENT': original_trading}, trading_client=replacement,
                    data_source=data, connect_stream=False, start_orders_thread=False)
    assert broker.api is replacement
    assert broker.data_source is data
