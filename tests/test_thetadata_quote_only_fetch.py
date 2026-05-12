import warnings
from datetime import date, datetime

import pandas as pd
import pytz


def _build_minute_df(columns, start_dt):
    rows = [
        dict(datetime=start_dt, **{k: v[0] for k, v in columns.items()}),
        dict(datetime=start_dt.replace(minute=start_dt.minute + 1), **{k: v[1] for k, v in columns.items()}),
    ]
    return pd.DataFrame(rows)


def test_thetadata_quote_only_fetch_skips_ohlc(monkeypatch):
    """When quote-only is requested, do not hit the OHLC endpoint."""
    from lumibot.backtesting.thetadata_backtesting_pandas import ThetaDataBacktestingPandas
    from lumibot.entities import Asset

    monkeypatch.setattr(ThetaDataBacktestingPandas, "kill_processes_by_name", lambda *_a, **_k: None)
    monkeypatch.setattr("lumibot.tools.data_downloader_queue_client.set_queue_client_id", lambda *_a, **_k: None)
    monkeypatch.setattr("lumibot.tools.thetadata_helper.reset_theta_terminal_tracking", lambda *_a, **_k: None)

    start = datetime(2024, 1, 1, tzinfo=pytz.UTC)
    end = datetime(2024, 1, 2, tzinfo=pytz.UTC)
    source = ThetaDataBacktestingPandas(datetime_start=start, datetime_end=end, pandas_data={})
    source._datetime = datetime(2024, 1, 1, 9, 31, tzinfo=pytz.UTC)

    option = Asset("SPXW", "option", expiration=date(2024, 1, 19), strike=4500.0, right="CALL")

    calls = []

    def _fake_get_price_data(_asset, _start, _end, *, datastyle, **_kwargs):
        calls.append(datastyle)
        if datastyle == "ohlc":
            raise AssertionError("OHLC should not be fetched for quote-only requests")
        return _build_minute_df(
            {
                "bid": (1.0, 1.1),
                "ask": (2.0, 2.1),
                "bid_size": (10, 11),
                "ask_size": (12, 13),
            },
            source._datetime,
        )

    monkeypatch.setattr("lumibot.tools.thetadata_helper.get_price_data", _fake_get_price_data)

    source._update_pandas_data(
        option,
        quote=None,
        length=1,
        timestep="minute",
        start_dt=source._datetime,
        require_quote_data=True,
        require_ohlc_data=False,
    )

    assert "quote" in calls
    assert "ohlc" not in calls


def test_thetadata_quote_only_cache_does_not_satisfy_ohlc_requirements(monkeypatch):
    """If we only cached quotes, a later OHLC-required call must still fetch OHLC."""
    from lumibot.backtesting.thetadata_backtesting_pandas import ThetaDataBacktestingPandas
    from lumibot.entities import Asset

    monkeypatch.setattr(ThetaDataBacktestingPandas, "kill_processes_by_name", lambda *_a, **_k: None)
    monkeypatch.setattr("lumibot.tools.data_downloader_queue_client.set_queue_client_id", lambda *_a, **_k: None)
    monkeypatch.setattr("lumibot.tools.thetadata_helper.reset_theta_terminal_tracking", lambda *_a, **_k: None)

    start = datetime(2024, 1, 1, tzinfo=pytz.UTC)
    end = datetime(2024, 1, 2, tzinfo=pytz.UTC)
    source = ThetaDataBacktestingPandas(datetime_start=start, datetime_end=end, pandas_data={})
    source._datetime = datetime(2024, 1, 1, 9, 31, tzinfo=pytz.UTC)

    option = Asset("SPXW", "option", expiration=date(2024, 1, 19), strike=4500.0, right="CALL")

    def _quote_df(*_a, **_k):
        return _build_minute_df({"bid": (1.0, 1.1), "ask": (2.0, 2.1)}, source._datetime)

    def _ohlc_df(*_a, **_k):
        return _build_minute_df(
            {"open": (1.0, 1.1), "high": (1.2, 1.3), "low": (0.9, 1.0), "close": (1.1, 1.2), "volume": (0, 0)},
            source._datetime,
        )

    # First: quote-only cache fill.
    monkeypatch.setattr("lumibot.tools.thetadata_helper.get_price_data", lambda *_a, datastyle, **_k: _quote_df())
    source._update_pandas_data(
        option,
        quote=None,
        length=1,
        timestep="minute",
        start_dt=source._datetime,
        require_quote_data=True,
        require_ohlc_data=False,
    )

    calls = []

    def _fake_get_price_data(_asset, _start, _end, *, datastyle, **_kwargs):
        calls.append(datastyle)
        if datastyle == "quote":
            return _quote_df()
        return _ohlc_df()

    monkeypatch.setattr("lumibot.tools.thetadata_helper.get_price_data", _fake_get_price_data)

    # Second: OHLC-required call should still fetch OHLC despite existing quote-only cache.
    source._update_pandas_data(
        option,
        quote=None,
        length=1,
        timestep="minute",
        start_dt=source._datetime,
        require_quote_data=False,
        require_ohlc_data=True,
    )

    assert "ohlc" in calls


def test_thetadata_quote_only_merge_preserves_existing_ohlc(monkeypatch):
    """Merging quote-only refreshes must not wipe existing OHLC columns."""
    from lumibot.backtesting.thetadata_backtesting_pandas import ThetaDataBacktestingPandas
    from lumibot.entities import Asset, Data

    monkeypatch.setattr(ThetaDataBacktestingPandas, "kill_processes_by_name", lambda *_a, **_k: None)
    monkeypatch.setattr("lumibot.tools.data_downloader_queue_client.set_queue_client_id", lambda *_a, **_k: None)
    monkeypatch.setattr("lumibot.tools.thetadata_helper.reset_theta_terminal_tracking", lambda *_a, **_k: None)

    start = datetime(2024, 1, 1, tzinfo=pytz.UTC)
    end = datetime(2024, 1, 2, tzinfo=pytz.UTC)
    dt = datetime(2024, 1, 1, 9, 31, tzinfo=pytz.UTC)

    option = Asset("SPXW", "option", expiration=date(2024, 1, 19), strike=4500.0, right="CALL")
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

    existing_df = pd.DataFrame(
        {"open": [5.0], "high": [5.2], "low": [4.8], "close": [5.1], "volume": [10]},
        index=pd.DatetimeIndex([dt], name="datetime"),
    )
    existing_data = Data(asset=option, df=existing_df, quote=quote, timestep="minute")

    source = ThetaDataBacktestingPandas(datetime_start=start, datetime_end=end, pandas_data=[existing_data])
    source._datetime = dt

    def _fake_get_price_data(_asset, _start, _end, *, datastyle, **_kwargs):
        if datastyle != "quote":
            raise AssertionError("Only quote data should be fetched in this scenario")
        return _build_minute_df(
            {
                "bid": (1.0, 1.1),
                "ask": (2.0, 2.1),
            },
            dt,
        )

    monkeypatch.setattr("lumibot.tools.thetadata_helper.get_price_data", _fake_get_price_data)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", FutureWarning)
        source._update_pandas_data(
            option,
            quote=None,
            length=1,
            timestep="minute",
            start_dt=source._datetime,
            require_quote_data=True,
            require_ohlc_data=False,
        )

    assert not any(issubclass(w.category, FutureWarning) for w in caught)

    key = source.find_asset_in_data_store(option, quote, "minute")
    assert key is not None
    df_after = source.pandas_data[key].df
    assert float(df_after.loc[dt, "close"]) == 5.1
    assert float(df_after.loc[dt, "bid"]) == 1.0
