"""Schwab price-history symbol routing.

Live check on 2026-09-22: Schwab returned daily candles for an SPY call when
the request used the OCC symbol. The ticker BTC is the Grayscale Bitcoin Mini
Trust ETF (quote about $38 that day), not bitcoin. Crypto requests must not
use that equity ticker.
"""

from datetime import date
from types import SimpleNamespace

from lumibot.data_sources.schwab_data import SchwabData
from lumibot.entities import Asset


class _HistoryResponse:
    status_code = 200

    def json(self):
        return {
            "candles": [
                {
                    "open": 1.0,
                    "high": 1.1,
                    "low": 0.9,
                    "close": 1.0,
                    "volume": 10,
                    "datetime": 1790000000000,
                }
            ]
        }


def _data_source():
    data_source = SchwabData(auto_create_client=False)
    calls = []

    def get_price_history_every_day(symbol, **kwargs):
        calls.append(symbol)
        return _HistoryResponse()

    data_source.client = SimpleNamespace(get_price_history_every_day=get_price_history_every_day)
    return data_source, calls


def test_option_history_requests_the_occ_symbol():
    data_source, calls = _data_source()
    asset = Asset(
        "SPY",
        asset_type=Asset.AssetType.OPTION,
        expiration=date(2026, 9, 22),
        strike=774,
        right="CALL",
    )

    bars = data_source.get_historical_prices(asset, 5, "day")

    assert calls == ["SPY   260922C00774000"]
    assert bars is not None
    assert len(bars.df) == 1


def test_future_history_still_skips_the_request():
    data_source, calls = _data_source()
    asset = Asset("ES", asset_type=Asset.AssetType.FUTURE)

    bars = data_source.get_historical_prices(asset, 5, "day")

    assert calls == []
    assert bars is None


def test_crypto_history_does_not_request_the_equity_ticker():
    data_source, calls = _data_source()

    for symbol in ("BTC", "BTC/USD", "ETH"):
        bars = data_source.get_historical_prices(
            Asset(symbol, asset_type=Asset.AssetType.CRYPTO),
            5,
            "day",
        )
        assert bars is None

    assert calls == []


def test_second_history_does_not_call_schwab():
    data_source, calls = _data_source()

    def get_price_history(*args, **kwargs):
        calls.append("intraday")
        return _HistoryResponse()

    data_source.client.get_price_history = get_price_history
    data_source.client.PriceHistory = SimpleNamespace(
        FrequencyType=SimpleNamespace(DAILY="daily", MINUTE="minute"),
        Frequency=lambda value: value,
    )
    bars = data_source.get_historical_prices(Asset("SPY"), 10, "second")

    assert calls == []
    assert bars is None


def test_stock_btc_and_eth_still_request_the_equity_ticker():
    """Equity tickers BTC and ETH are ETFs. Only asset_type crypto is refused."""
    data_source, calls = _data_source()

    btc_bars = data_source.get_historical_prices(Asset("BTC"), 5, "day")
    eth_bars = data_source.get_historical_prices(Asset("ETH"), 5, "day")

    assert calls == ["BTC", "ETH"]
    assert btc_bars is not None
    assert eth_bars is not None

    quote_calls = []

    def get_quotes(symbols):
        quote_calls.append(list(symbols))
        return _HistoryResponse()

    data_source.client.get_quotes = get_quotes
    data_source.get_quote(Asset("BTC"))
    data_source.get_quote(Asset("ETH"))

    assert quote_calls == [["BTC"], ["ETH"]]


def test_crypto_quote_does_not_request_the_equity_ticker():
    data_source = SchwabData(auto_create_client=False)
    calls = []

    def get_quotes(symbols):
        calls.append(list(symbols))
        raise AssertionError("crypto quote must not call Schwab")

    data_source.client = SimpleNamespace(get_quotes=get_quotes)
    quote = data_source.get_quote(Asset("BTC", asset_type=Asset.AssetType.CRYPTO))
    last = data_source.get_last_price(Asset("ETH", asset_type=Asset.AssetType.CRYPTO))

    assert quote is None
    assert last is None
    assert calls == []
