from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd

from lumibot.backtesting import WebullHistoryBacktesting
from lumibot.data_sources import WebullData
from lumibot.entities import Asset
from lumibot.tools.broker_history import WebullHistory


def test_daily_replay_respects_early_close_and_reads_window_once():
    bar = dict(open="32", high="34", low="31", close="33", volume="100")
    client = Mock()
    client.market_data.get_batch_history_bar.return_value = SimpleNamespace(
        json=lambda: {"result": [{"symbol": "SAMPLE", "result": [
            {**bar, "time": "2024-11-27T05:00:00.000+0000"},
            {**bar, "time": "2024-11-29T05:00:00.000+0000", "close": "34"},
        ]}]}, raise_for_status=lambda: None,
    )
    ny = "America/New_York"
    adapter = WebullHistory(WebullData({}, data_client=client))
    source = WebullHistoryBacktesting(pd.Timestamp("2024-11-27", tz=ny), pd.Timestamp("2024-12-03", tz=ny),
                                      adapter=adapter)
    symbol = Asset("SAMPLE")
    source._datetime = pd.Timestamp("2024-11-29 12:59", tz=ny)
    assert source.get_historical_prices(symbol, 1, "day").df["close"].tolist() == [33]
    source._datetime = pd.Timestamp("2024-11-29 13:00", tz=ny)
    assert source.get_historical_prices(symbol, 1, "day").df["close"].tolist() == [34]
    client.market_data.get_batch_history_bar.assert_called_once()


def test_crypto_daily_replay_reads_exact_pair_and_excludes_open_candle():
    client = Mock()
    middle = dict(open="32", high="34", low="31", close="33")
    client.crypto_market_data.get_crypto_history_bar.return_value = SimpleNamespace(
        json=lambda: [{"symbol": "BTCUSD", "result": [
            {**middle, "time": "2026-11-17T05:00:00.000+0000"},
            {**middle, "time": "2026-11-18T05:00:00.000+0000", "close": "37"},
        ]}], raise_for_status=lambda: None,
    )
    ny = "America/New_York"
    adapter = WebullHistory(WebullData({}, data_client=client))
    source = WebullHistoryBacktesting(pd.Timestamp("2026-11-17", tz=ny), pd.Timestamp("2026-11-20", tz=ny),
                                      adapter=adapter)
    asset, quote = Asset("BTC", "crypto"), Asset("USD", "forex")
    source._datetime = pd.Timestamp("2026-11-18 23:59", tz=ny)
    assert source.get_historical_prices(asset, 1, "day", quote=quote).df["close"].tolist() == [33]
    source._datetime = pd.Timestamp("2026-11-19 00:00", tz=ny)
    assert source.get_historical_prices(asset, 1, "day", quote=quote).df["close"].tolist() == [37]
    client.crypto_market_data.get_crypto_history_bar.assert_called_once()
    assert client.crypto_market_data.get_crypto_history_bar.call_args.args[:3] == ("BTCUSD", "US_CRYPTO", "D")


def test_live_equity_daily_history_includes_just_closed_short_session(monkeypatch):
    client = Mock()
    bars = [
        dict(open="32", high="34", low="31", close="33", volume="100", time="2024-11-27T05:00:00.000+0000"),
        dict(open="33", high="35", low="32", close="34", volume="100", time="2024-11-29T05:00:00.000+0000"),
    ]
    client.market_data.get_batch_history_bar.return_value = SimpleNamespace(
        json=lambda: {"result": [{"symbol": "SAMPLE", "result": bars}]}, raise_for_status=lambda: None,
    )
    source = WebullData({}, data_client=client)
    ny = "America/New_York"
    monkeypatch.setattr(source, "get_datetime", lambda: pd.Timestamp("2024-11-29 13:10", tz=ny))
    history = source.get_historical_prices(Asset("SAMPLE"), 1, timestep="day")
    assert history.df["close"].tolist() == [34]
    assert history.df.index[-1] == pd.Timestamp("2024-11-29 13:00", tz=ny)
