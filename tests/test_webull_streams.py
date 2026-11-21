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
    source._market_stream = Mock()
    pair = (Asset("BTC", "crypto"), Asset("USD", "forex"))
    source.start_realtime_bars(pair, 5)
    source._market_stream.start.assert_called_once_with("BTCUSD", "US_CRYPTO", 5)
    source.get_realtime_bars(pair)
    source._market_stream.bars.assert_called_once_with("BTCUSD", "US_CRYPTO")
    source.cancel_realtime_bars(pair)
    source._market_stream.cancel.assert_called_once_with("BTCUSD", "US_CRYPTO")


def test_market_subscribe_acknowledgement_does_not_prove_ticks():
    stream = WebullMarketStream({}, paper=True)
    stream.state = "connected"
    stream._on_subscribe(None, None, "offline")
    assert stream.state == "subscribed"
    assert stream.bars("SAMPLE", "US_STOCK") is None
