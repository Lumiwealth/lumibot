from datetime import datetime

import pytz


def test_thetadata_price_snapshot_memoized_within_dt(monkeypatch):
    """Ensure repeated get_price_snapshot calls within a bar reuse cached results."""
    from lumibot.backtesting.thetadata_backtesting_pandas import ThetaDataBacktestingPandas
    from lumibot.entities import Asset

    # Avoid side effects (process kills / queue ids) in unit tests.
    monkeypatch.setattr(ThetaDataBacktestingPandas, "kill_processes_by_name", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("lumibot.tools.data_downloader_queue_client.set_queue_client_id", lambda *_a, **_k: None)
    monkeypatch.setattr("lumibot.tools.thetadata_helper.reset_theta_terminal_tracking", lambda *_a, **_k: None)

    start = datetime(2024, 1, 1, tzinfo=pytz.UTC)
    end = datetime(2024, 1, 2, tzinfo=pytz.UTC)
    source = ThetaDataBacktestingPandas(datetime_start=start, datetime_end=end, pandas_data={})
    source._datetime = datetime(2024, 1, 1, 10, 0, tzinfo=pytz.UTC)

    asset = Asset("SPY", "stock")
    dataset_key = ("dummy", "USD", "minute")

    class DummyData:
        def __init__(self):
            self.calls = 0

        def get_price_snapshot(self, _dt):
            self.calls += 1
            return {"bid": 1.0, "ask": 2.0, "close": 1.5}

    dummy_data = DummyData()
    source.pandas_data[dataset_key] = dummy_data

    monkeypatch.setattr(source, "find_asset_in_data_store", lambda *_a, **_k: dataset_key)

    update_calls = {"count": 0}

    def _fake_update(*_a, **_k):
        update_calls["count"] += 1

    monkeypatch.setattr(source, "_update_pandas_data", _fake_update)

    snap1 = source.get_price_snapshot(asset, timestep="minute")
    snap2 = source.get_price_snapshot(asset, timestep="minute")

    assert update_calls["count"] == 1
    assert dummy_data.calls == 1
    assert snap1 == snap2
