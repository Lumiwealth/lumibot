from unittest.mock import Mock

import pandas as pd

from lumibot.data_sources.webull_stream import WebullMarketStream


def test_bounded_trade_bars_retain_volume_vwap_and_reference_count():
    stream = WebullMarketStream({}, paper=True)
    transport = Mock()
    # Inject an already-running transport while exercising subscriptions and callback state.
    stream._thread = Mock()
    stream._client = transport
    stream.state = "connected"
    stream.start("SAMPLE", "US_STOCK", 1)
    stream.start("SAMPLE", "US_STOCK", 1)
    transport.subscribe.assert_called_once_with(["SAMPLE"], "US_STOCK", ["TICK"])
    t = pd.Timestamp("2026-11-20T20:00:00Z")
    stream._record_tick("OTHER", t, 100.0, 2.0)
    assert stream.bars("SAMPLE", "US_STOCK") is None
    stream._record_tick("SAMPLE", t, 10.0, 2.0)
    stream._record_tick("SAMPLE", t, 16.0, 1.0)
    assert stream.bars("SAMPLE", "US_STOCK") == [
        dict(datetime=t, open=10.0, low=10.0, high=16.0, close=16.0, volume=3.0, vwap=12.0, count=2)
    ]
    t2 = t + pd.Timedelta(seconds=5)
    stream._record_tick("SAMPLE", t2, 11.0, 1.0)
    stream._record_tick("SAMPLE", t, 99.0, 1.0)
    assert stream.bars("SAMPLE", "US_STOCK")[0]["datetime"] == t2
    assert stream.bars("SAMPLE", "US_STOCK")[0]["high"] == 11.0
    stream.cancel("SAMPLE", "US_STOCK")
    transport.unsubscribe.assert_not_called()
    stream.cancel("SAMPLE", "US_STOCK")
    transport.unsubscribe.assert_called_once_with(["SAMPLE"], "US_STOCK", ["TICK"])


def test_realtime_crypto_keeps_quote_currency_in_subscription_key():
    from lumibot.data_sources import WebullData
    from lumibot.entities import Asset

    source = WebullData({})
    transport = source._market_stream = Mock()
    pair = (Asset("BTC", "crypto"), Asset("USD", "forex"))
    source.start_realtime_bars(pair, 5)
    source._market_stream.start.assert_called_once_with("BTCUSD", "US_CRYPTO", 5)
    source.get_realtime_bars(pair)
    source._market_stream.bars.assert_called_once_with("BTCUSD", "US_CRYPTO")
    source.cancel_realtime_bars(pair)
    transport.cancel.assert_called_once_with("BTCUSD", "US_CRYPTO")


def test_market_subscribe_acknowledgement_does_not_prove_ticks():
    stream = WebullMarketStream({}, paper=True)
    stream.state = "connected"
    stream._on_subscribe(None, None, "offline")
    assert stream.state == "subscribed"
    assert stream.bars("SAMPLE", "US_STOCK") is None


def test_trade_callback_uses_dated_sdk_timestamp_when_tick_time_is_display_clock():
    from datetime import datetime

    stream = WebullMarketStream({}, paper=True)
    stream._thread = Mock()
    stream.start("SAMPLE", "US_STOCK", 2)
    tick = Mock()
    tick.get_basic.return_value.get_symbol.return_value = "SAMPLE"
    tick.get_basic.return_value.get_timestamp_as_utc.return_value = datetime(2026, 11, 23, 14, 28, 59, 400000)
    tick.get_time.return_value = "09:28:59"
    tick.get_price.return_value = "42.10"
    tick.get_volume.return_value = "7"
    stream._on_message(None, "tick", tick)
    [bar] = stream.bars("SAMPLE", "US_STOCK")
    assert bar["datetime"] == pd.Timestamp("2026-11-23T14:28:55Z")
    assert (bar["close"], bar["volume"], bar["count"]) == (42.10, 7, 1)


def test_refused_market_feed_is_reported_once_and_other_symbols_stay_connected():
    import pytest
    from lumibot.tools.webull_helpers import WebullAPIError

    class MissingFeed(Exception):
        def get_error_code(self):
            return "MARKET_DATA_NOT_SUBSCRIBED"

    stream = WebullMarketStream({}, paper=True)
    stream._thread = Mock()
    stream.start("SAMPLE", "US_FUTURES", 1)
    stream.start("TEST", "US_STOCK", 1)
    client = Mock()
    def subscribe(symbols, category, topics):
        if category == "US_FUTURES":
            raise MissingFeed("signed metadata must remain private")
    client.subscribe.side_effect = subscribe
    stream._on_connect(client, None, "first")
    with pytest.raises(WebullAPIError, match="MARKET_DATA_NOT_SUBSCRIBED") as refused:
        stream.bars("SAMPLE", "US_FUTURES")
    assert "signed metadata" not in str(refused.value)
    stream._on_connect(client, None, "reconnected")
    assert [call.args[1] for call in client.subscribe.call_args_list] == ["US_FUTURES", "US_STOCK", "US_STOCK"]
    stream._record_tick("TEST", pd.Timestamp("2026-11-23T15:00Z"), 5, 1)
    assert stream.bars("TEST", "US_STOCK")[0]["close"] == 5


def test_last_market_data_consumer_closes_the_connection():
    from lumibot.data_sources import WebullData
    from lumibot.entities import Asset

    source = WebullData({})
    stream = Mock()
    stream.cancel.side_effect = [False, True]
    source._market_stream = stream
    source.cancel_realtime_bars(Asset("SAMPLE"))
    stream.close.assert_not_called()
    source.cancel_realtime_bars(Asset("SAMPLE"))
    stream.close.assert_called_once_with()
    assert source._market_stream is None


def test_last_consumer_disconnects_even_if_native_unsubscribe_fails():
    from lumibot.data_sources import WebullData
    from lumibot.entities import Asset

    source = WebullData({})
    stream = source._market_stream = WebullMarketStream({}, paper=True)
    stream._thread, stream._client = Mock(), Mock()
    stream.state = "connected"
    stream._client.unsubscribe.side_effect = RuntimeError("connection already gone")
    stream.start("SAMPLE", "US_STOCK", 2)
    source.cancel_realtime_bars(Asset("SAMPLE"))
    stream._client.disconnect.assert_called_once_with()
    assert source._market_stream is None
