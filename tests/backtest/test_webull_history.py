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


def test_option_daily_replay_resolves_exact_series_and_never_reads_a_later_expiry():
    from datetime import date

    early, target = "2026-11-24", "2026-11-27"
    wrong_symbol, right_symbol = "SAMPLE261124C00050000", "SAMPLE261127C00050000"
    contracts = [{"expiration_date": early, "symbol": wrong_symbol},
                 {"expiration_date": target, "symbol": right_symbol}]
    for terms in contracts:
        terms.update(option_type="CALL", strike_price="50")
    client = Mock()
    client.instrument.list_option_contracts.return_value = SimpleNamespace(
        json=lambda: {"data": contracts}, raise_for_status=lambda: None,
    )
    bar = dict(open="32", high="34", low="31", close="33", volume="100")
    client.option_market_data.get_option_history_bars.return_value = SimpleNamespace(
        json=lambda: {"result": [{"symbol": right_symbol, "result": [
            {**bar, "time": "2026-11-17T05:00:00.000+0000"},
            {**bar, "time": "2026-11-18T05:00:00.000+0000", "close": "37"},
        ]}]}, raise_for_status=lambda: None,
    )
    ny = "America/New_York"
    adapter = WebullHistory(WebullData({}, data_client=client))
    source = WebullHistoryBacktesting(pd.Timestamp("2026-11-17", tz=ny), pd.Timestamp("2026-11-20", tz=ny),
                                      adapter=adapter)
    asset = Asset("SAMPLE", "option", expiration=date.fromisoformat(target), strike=50, right="call")
    source._datetime = pd.Timestamp("2026-11-18 15:59", tz=ny)
    assert source.get_historical_prices(asset, 1, "day").df["close"].tolist() == [33]
    source._datetime = pd.Timestamp("2026-11-18 16:00", tz=ny)
    assert source.get_historical_prices(asset, 1, "day").df["close"].tolist() == [37]
    query = client.option_market_data.get_option_history_bars.call_args
    assert query.args[:3] == (right_symbol, "US_OPTION", "D")
    client.option_market_data.get_option_history_bars.assert_called_once()
    client.market_data.get_batch_history_bar.assert_not_called()


def test_minute_data_paginates_exact_future_honors_range_and_drops_incomplete_bar():
    def reply(timestamps):
        rows = [dict(open="30", high="32", low="29", close="31", time=t) for t in timestamps]
        return SimpleNamespace(json=lambda: [{"symbol": "MESZ6", "result": rows}], raise_for_status=lambda: None)
    client = Mock()
    get_bars = client.futures_market_data.get_futures_history_bars
    get_bars.side_effect = [reply(["2026-11-20T15:02:00Z", "2026-11-20T15:03:00Z"]),
                            reply(["2026-11-20T15:00:00Z", "2026-11-20T15:01:00Z"])]
    source = WebullData({}, data_client=client)
    start, end = pd.Timestamp("2026-11-20T15:00Z"), pd.Timestamp("2026-11-20T15:03:30Z")
    frame = source.get_minute_range(Asset("MESZ6", "future"), start, end)
    assert list(frame.index) == list(pd.date_range(start, periods=3, freq="min"))
    assert frame.volume.isna().all()
    assert get_bars.call_count == 2
    assert get_bars.call_args.args == ("MESZ6", "US_FUTURES", "M1")
    assert get_bars.call_args.kwargs["end_time"] < int((start + pd.Timedelta(minutes=2)).timestamp() * 1_000)


def test_minute_replay_discards_newer_crypto_page_when_provider_ignores_end_time():
    client = Mock()
    client.crypto_market_data.get_crypto_history_bar.return_value = SimpleNamespace(
        json=lambda: [{"symbol": "BTCUSD", "result": [{"time": "2026-11-23T14:00:00Z", "close": "100"}]}],
        raise_for_status=lambda: None,
    )
    source = WebullData({}, data_client=client)
    assert source.get_minute_range(Asset("BTC", "crypto"), pd.Timestamp("2026-11-20T14:00Z"),
                                   pd.Timestamp("2026-11-20T15:00Z"), quote=Asset("USD", "forex")) is None
    client.crypto_market_data.get_crypto_history_bar.assert_called_once()
