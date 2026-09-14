"""Price, quote and bar contracts, including inherited plural reads."""

from datetime import datetime, timedelta, timezone

import httpx
import pytest

from lumibot.data_sources import KalshiData
from lumibot.entities import Asset, Bars, Quote
from lumibot.tools.kalshi_client import KalshiAPIError, KalshiClient

ASSET = Asset("TEST-MARKET", asset_type="prediction_contract")
NOW = datetime(2026, 1, 1, 12, tzinfo=timezone.utc)


def candle(ts, price="0.4321"):
    return {
        "end_period_ts": ts,
        "price": {name + "_dollars": price for name in ("open", "high", "low", "close")},
        "volume_fp": "1.25",
    }


@pytest.fixture
def data():
    market = {
        "ticker": ASSET.symbol,
        "last_price_dollars": "0.4321",
        "yes_bid_dollars": "0.42",
        "yes_ask_dollars": "0.44",
        "yes_bid_size_fp": "12.50",
        "yes_ask_size_fp": "8.25",
        "volume_fp": "100.50",
    }

    def handler(request):
        if request.url.path.endswith("/candlesticks"):
            end = int(request.url.params["end_ts"])
            step = int(request.url.params["period_interval"]) * 60
            return httpx.Response(
                200,
                json={"markets": [{"market_ticker": ASSET.symbol, "candlesticks": [candle(end), candle(end - step)]}]},
            )
        return httpx.Response(200, json={"market": market})

    http = httpx.Client(transport=httpx.MockTransport(handler))
    source = KalshiData(client=KalshiClient(http_client=http))
    source.get_datetime = lambda **_: NOW
    yield source
    source.shutdown()
    http.close()


def test_last_and_quote_preserve_subcent_and_fractional_values(data):
    assert data.get_last_price(ASSET) == 0.4321
    quote = data.get_quote(ASSET)
    assert isinstance(quote, Quote)
    assert (quote.bid, quote.ask, quote.mid_price) == (0.42, 0.44, 0.43)
    assert (quote.bid_size, quote.ask_size, quote.price, quote.volume) == (12.5, 8.25, 0.4321, 100.5)
    assert data.get_last_prices([ASSET])[ASSET] == 0.4321


@pytest.mark.parametrize(
    "timestep,minutes", [("minute", 1), ("1minute", 1), ("hour", 60), ("60minute", 60), ("day", 1440), ("1day", 1440)]
)
def test_candles_return_real_ohlcv_in_chronological_order(data, timestep, minutes):
    result = data.get_historical_prices(ASSET, 2, timestep=timestep)
    assert isinstance(result, Bars)
    assert len(result.df) == 2
    assert result.df.index.is_monotonic_increasing
    assert result.df.iloc[-1]["close"] == 0.4321
    assert result.df.iloc[-1]["volume"] == 1.25
    assert result.df.index[-1] - result.df.index[0] == timedelta(minutes=minutes)


def test_timeshift_excludes_future_data(data):
    result = data.get_historical_prices(ASSET, 2, timeshift=timedelta(hours=1))
    assert result.df.index[-1].timestamp() == (NOW - timedelta(hours=1)).timestamp()


def test_inherited_plural_history_preserves_individual_errors(data):
    stock = Asset("WRONG-TYPE")
    results = data.get_bars([ASSET, stock], 2, sleep_time=0)
    assert isinstance(results[ASSET], Bars)
    assert results[stock] is None
    assert stock in results.errors


def test_plural_last_prices_preserve_success_and_failure(data):
    stock = Asset("WRONG-TYPE")
    result = data.get_last_prices([stock, ASSET])
    assert result[stock] is None
    assert result[ASSET] == 0.4321
    assert result[ASSET.symbol] == 0.4321


@pytest.mark.parametrize(
    "payload", [{}, {"markets": {}}, {"markets": [{"market_ticker": ASSET.symbol, "candlesticks": {}}]}]
)
def test_malformed_candlestick_collections_are_rejected(payload):
    with httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(200, json=payload))) as http:
        with pytest.raises(KalshiAPIError, match="candlestick"):
            KalshiData(client=KalshiClient(http_client=http)).get_historical_prices(ASSET, 2)


@pytest.mark.parametrize("status", [404, 403])
def test_history_only_falls_back_on_not_found(status):
    calls = []

    def handler(request):
        calls.append(request.url.path)
        if "/historical/" in request.url.path:
            return httpx.Response(200, json={"candlesticks": []})
        return httpx.Response(status)

    with httpx.Client(transport=httpx.MockTransport(handler)) as http:
        source = KalshiData(client=KalshiClient(http_client=http))
        if status == 404:
            assert source.get_historical_prices(ASSET, 2).df.empty
            assert len(calls) == 2
        else:
            with pytest.raises(KalshiAPIError):
                source.get_historical_prices(ASSET, 2)
            assert len(calls) == 1


@pytest.mark.parametrize(
    "asset", [Asset("SPY"), "TEST-MARKET", Asset("TEST", asset_type="prediction_contract", multiplier=2)]
)
def test_invalid_assets(data, asset):
    with pytest.raises(ValueError):
        data.get_last_price(asset)


@pytest.mark.parametrize("length", [0, -1, 1.2, True, None])
def test_invalid_history_length(data, length):
    with pytest.raises(ValueError, match="length"):
        data.get_historical_prices(ASSET, length)


@pytest.mark.parametrize("timestep", ["second", "5minute", "week"])
def test_unsupported_timestep(data, timestep):
    with pytest.raises(ValueError, match="timesteps"):
        data.get_historical_prices(ASSET, 2, timestep=timestep)


def test_wrong_quote_exchange_and_timeshift(data):
    with pytest.raises(ValueError, match="USD"):
        data.get_quote(ASSET, quote=Asset("EUR", asset_type="forex"))
    with pytest.raises(ValueError, match="exchange"):
        data.get_quote(ASSET, exchange="NYSE")
    with pytest.raises(ValueError, match="timedelta"):
        data.get_historical_prices(ASSET, 2, timeshift=1)
    assert data.get_chains(ASSET) == {}


def test_archived_history_fallback_and_null_trade_candles():
    calls = []

    def handler(request):
        calls.append(request.url.path)
        if request.url.path.endswith("/markets/candlesticks"):
            return httpx.Response(200, json={"markets": []})
        ts = int(request.url.params["end_ts"])
        return httpx.Response(200, json={"candlesticks": [candle(ts, None)]})

    with httpx.Client(transport=httpx.MockTransport(handler)) as http:
        source = KalshiData(client=KalshiClient(http_client=http))
        result = source.get_historical_prices(ASSET, 2)
        assert result.df.empty
        assert calls[-1].endswith("/historical/markets/TEST-MARKET/candlesticks")


def test_long_history_is_chunked_without_gaps_or_duplicates():
    windows = []

    def handler(request):
        start, end = int(request.url.params["start_ts"]), int(request.url.params["end_ts"])
        windows.append((start, end))
        return httpx.Response(
            200,
            json={
                "markets": [
                    {"market_ticker": ASSET.symbol, "candlesticks": [candle(ts) for ts in range(start, end + 1, 60)]}
                ]
            },
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as http:
        source = KalshiData(client=KalshiClient(http_client=http))
        result = source.get_historical_prices(ASSET, 10001)
        assert len(windows) == 3
        assert len(result.df) == 10001
        assert all(a[1] + 60 == b[0] for a, b in zip(windows, windows[1:]))


def test_market_mismatch_and_missing_data_are_not_silently_substituted():
    with httpx.Client(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, json={"market": {"ticker": "WRONG"}}))
    ) as http:
        with pytest.raises(KalshiAPIError, match="market"):
            KalshiData(client=KalshiClient(http_client=http)).get_last_price(ASSET)
