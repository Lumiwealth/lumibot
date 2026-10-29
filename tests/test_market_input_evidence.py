"""Input-observation unit fixtures, never strategy execution or eval proof."""
import json
from types import SimpleNamespace

import pandas as pd

from lumibot.entities import Asset


def test_framework_input_rows_preserve_time_identity_and_changed_values(tmp_path):
    from lumibot.backtesting.market_input_evidence import record_market_input, write_market_input_evidence
    source = SimpleNamespace()
    frame = pd.DataFrame({"open": [9., 10.], "close": [10., 11.]},
        index=pd.date_range("2025-01-01", periods=2, tz="America/New_York"))
    kwargs = dict(source=source, asset=Asset("SPY"), timestep="day", when=frame.index[-1], purpose="history")
    record_market_input(frame=frame, **kwargs)
    record_market_input(frame=frame, **kwargs)  # Repeated reads are not duplicated.
    changed = frame.copy()
    changed.iloc[1, changed.columns.get_loc("close")] = 12.
    record_market_input(frame=changed, **kwargs)
    write_market_input_evidence(source, tmp_path)
    proof = json.loads((tmp_path / "market_input_manifest.json").read_text())
    data = pd.read_csv(tmp_path / "market_inputs.csv")
    observations = pd.read_csv(tmp_path / "market_input_observations.csv")
    assert proof["complete"] is True
    assert len(data) == 3
    assert len(observations) == 2
    assert set(data["close"]) == {10., 11., 12.}
    assert all("-05:00" in value for value in data["barTime"])
    assert len(set(observations["frameSha256"])) == 2
    assert all(json.loads(value)["assetType"] == "stock" and json.loads(value)["symbol"] == "SPY"
               for value in data["asset"])


def test_overlapping_windows_reuse_serialized_rows_without_losing_corrections(monkeypatch):
    import lumibot.backtesting.market_input_evidence as evidence
    scalar = evidence._scalar
    calls = []
    def counted(value):
        calls.append(value)
        return scalar(value)
    monkeypatch.setattr(evidence, "_scalar", counted)
    frame = pd.DataFrame({"open": [9., 10.], "close": [10., 11.]},
        index=pd.date_range("2025-01-01", periods=2, tz="UTC"))
    recorder = evidence.MarketInputEvidence()
    kw = dict(asset=Asset("SPY"), timestep="day", frame=frame, purpose="history")
    recorder.record(when=frame.index[-1], **kw)
    original_hash = recorder.observations[0]["frameSha256"]
    assert len(calls) == 4
    recorder.record(when=frame.index[-1] + pd.Timedelta(days=1), **kw)
    assert len(calls) == 4, "Do not serialize identical historical rows at every decision"
    assert len(recorder.observations) == 2
    assert recorder.observations[1]["frameSha256"] == original_hash
    changed = frame.copy()
    changed.iloc[-1, 1] = 12.
    recorder.record(when=frame.index[-1] + pd.Timedelta(days=2), **{**kw, "frame": changed})
    assert len(calls) == 6, "A corrected row must be serialized and retained"
    assert len(recorder.rows) == 3
    assert recorder.observations[-1]["frameSha256"] != original_hash
    assert {r["close"] for r in recorder.rows.values()} == {10., 11., 12.}


def test_serialization_cache_preserves_signed_zero_types_and_stays_bounded():
    import hashlib
    from lumibot.backtesting.market_input_evidence import MarketInputEvidence, _encoded
    recorder = MarketInputEvidence(max_rows=2)
    index = pd.date_range("2025-01-01", periods=1, tz="UTC")
    for value in (0., -0., 1):
        frame = pd.DataFrame({"close": [value]}, index=index)
        recorder.record(asset=Asset("SPY"), timestep="day", when=index[0], frame=frame, purpose="history")
        expected = hashlib.sha256(_encoded({"columns": ["close"],
            "rows": [[index[0].isoformat(), value]]}).encode()).hexdigest()
        assert recorder.observations[-1]["frameSha256"] == expected
        assert len(recorder._serialized_rows) <= 2
    assert len({o["frameSha256"] for o in recorder.observations}) == 3
    assert recorder.incomplete == {"row_limit"}


def test_capture_limit_is_explicit_and_does_not_change_input_data(tmp_path):
    from lumibot.backtesting.market_input_evidence import MarketInputEvidence, write_market_input_evidence
    recorder = MarketInputEvidence(max_rows=1)
    source = SimpleNamespace(_market_input_evidence=recorder)
    frame = pd.DataFrame({"close": [10., 11.]}, index=pd.date_range("2025-01-01", periods=2, tz="UTC"))
    original = frame.copy()
    recorder.record(asset=Asset("SPY"), timestep="day", when=frame.index[-1], frame=frame, purpose="history")
    write_market_input_evidence(source, tmp_path)
    assert frame.equals(original)
    proof = json.loads((tmp_path / "market_input_manifest.json").read_text())
    assert proof["complete"] is False
    assert "row_limit" in proof["incompleteReasons"]


def test_option_and_crypto_quote_identities_never_collapse():
    from lumibot.backtesting.market_input_evidence import asset_identity
    call = Asset("SPY", "option", strike=500, right="call")
    put = Asset("SPY", "option", strike=500, right="put")
    assert asset_identity(call) != asset_identity(put)
    btc = Asset("BTC", "crypto")
    assert asset_identity((btc, Asset("USD", "forex"))) != asset_identity((btc, Asset("USDT", "crypto")))


def test_missing_rows_and_input_window_remain_visible(tmp_path):
    from lumibot.backtesting.market_input_evidence import record_market_input, write_market_input_evidence
    source = SimpleNamespace()
    frame = pd.DataFrame({"close": [10., float("nan")]}, index=pd.date_range("2025-01-01", periods=2, tz="UTC"))
    record_market_input(source=source, asset=Asset("SPY"), timestep="day", when=frame.index[0],
        frame=frame, purpose="history")  # Preserve a future row if an API actually returned one.
    write_market_input_evidence(source, tmp_path)
    data = pd.read_csv(tmp_path / "market_inputs.csv")
    observations = pd.read_csv(tmp_path / "market_input_observations.csv")
    assert data["close"].isna().sum() == 1
    assert observations.iloc[0]["observedAt"] < observations.iloc[0]["windowEnd"]
    assert observations.iloc[0]["returnedRows"] == 2


def test_batch_history_records_each_actual_result_without_replacing_missing_assets():
    import logging
    from lumibot.strategies.strategy import Strategy
    a, b = Asset("AAPL"), Asset("MSFT")
    frame = pd.DataFrame({"close": [10., 11.]}, index=pd.date_range("2025-01-01", periods=2, tz="UTC"))
    results = {a: SimpleNamespace(pandas_df=frame), b: None}
    source = SimpleNamespace(IS_BACKTESTING_DATA_SOURCE=True, get_bars=lambda *_, **__: results)
    strategy = SimpleNamespace(broker=SimpleNamespace(data_source=source),
        is_backtesting=True, quote_asset=Asset("USD", "forex"), get_datetime=lambda: frame.index[-1],
        _sanitize_user_asset=lambda v: v, _logged_get_historical_prices_assets=set(),
        logger=logging.getLogger("batch-input-unit"))
    assert Strategy.get_historical_prices_for_assets(strategy, [a, b], 2, timestep="day") is results
    observed = source._market_input_evidence.observations
    assert [r["returnedRows"] for r in observed] == [2, 0]
    assert [json.loads(r["asset"])["symbol"] for r in observed] == ["AAPL", "MSFT"]


def test_default_capture_continues_after_legacy_read_ceiling(tmp_path):
    from lumibot.backtesting.market_input_evidence import MarketInputEvidence, write_market_input_evidence
    recorder = MarketInputEvidence()
    frame = pd.DataFrame({"close": [10.]}, index=pd.date_range("2025-01-01", periods=1, tz="UTC"))
    for minute in range(4100):
        recorder.record(asset=Asset("TEST"), timestep="minute", frame=frame,
                        when=frame.index[0] + pd.Timedelta(minutes=minute), purpose="history")
    assert len(recorder.observations) == 4100
    assert len(recorder.rows) == 1
    write_market_input_evidence(SimpleNamespace(_market_input_evidence=recorder), tmp_path)
    manifest = json.loads((tmp_path / "market_input_manifest.json").read_text())
    assert manifest["complete"] is True
    assert manifest["observationLimit"] is None
    assert manifest["files"]["market_input_observations.csv"]["rows"] == 4100
    recorder.close()


def test_default_capture_streams_beyond_legacy_unique_row_ceiling(tmp_path):
    import csv
    from lumibot.backtesting.market_input_evidence import MarketInputEvidence, write_market_input_evidence
    recorder = MarketInputEvidence()
    frame = pd.DataFrame({"close": range(50_001)},
                         index=pd.date_range("2025-01-01", periods=50_001, freq="min", tz="UTC"))
    recorder.record(asset=Asset("TEST"), timestep="minute", frame=frame,
                    when=frame.index[-1], purpose="history")
    assert len(recorder.rows) == 50_001
    assert recorder.observations[0]["returnedRows"] == 50_001
    write_market_input_evidence(SimpleNamespace(_market_input_evidence=recorder), tmp_path)
    manifest = json.loads((tmp_path / "market_input_manifest.json").read_text())
    assert manifest["complete"] is True
    assert manifest["rowLimit"] is None
    with (tmp_path / "market_inputs.csv").open(newline="") as handle:
        assert sum(1 for _ in csv.DictReader(handle)) == 50_001
    recorder.close()


def test_serialization_cache_bound_does_not_truncate_record_history(tmp_path):
    from lumibot.backtesting.market_input_evidence import MarketInputEvidence, write_market_input_evidence
    recorder = MarketInputEvidence(max_cache_rows=8)
    start = pd.Timestamp("2025-01-01", tz="UTC")
    for minute in range(100):
        frame = pd.DataFrame({"close": [float(minute)]}, index=[start + pd.Timedelta(minutes=minute)])
        recorder.record(asset=Asset("TEST"), timestep="minute", when=frame.index[0], frame=frame, purpose="history")
        assert len(recorder._serialized_rows) <= 8
    assert len(recorder.rows) == len(recorder.observations) == 100
    write_market_input_evidence(SimpleNamespace(_market_input_evidence=recorder), tmp_path)
    assert json.loads((tmp_path / "market_input_manifest.json").read_text())["complete"] is True
    recorder.close()


def test_artifact_export_streams_and_finalization_cleans_owned_spool(tmp_path, monkeypatch):
    from pathlib import Path
    import lumibot.backtesting.market_input_evidence as evidence
    recorder = evidence.MarketInputEvidence()
    frame = pd.DataFrame({"close": [10., 11.]}, index=pd.date_range("2025-01-01", periods=2, tz="UTC"))
    recorder.record(asset=Asset("TEST"), timestep="day", when=frame.index[-1], frame=frame, purpose="history")
    def materialized(*args, **kwargs):
        raise AssertionError("Export must stream records and hash chunks")
    monkeypatch.setattr(evidence.pd, "DataFrame", materialized)
    monkeypatch.setattr(Path, "read_bytes", materialized)
    spool = recorder.spool_directory
    evidence.write_market_input_evidence(SimpleNamespace(_market_input_evidence=recorder), tmp_path,
                                         finalize=True)
    assert not spool.exists()
    assert json.loads((tmp_path / "market_input_manifest.json").read_text())["complete"] is True


def test_failed_final_export_closes_only_its_owned_spool(tmp_path):
    import pytest
    import lumibot.backtesting.market_input_evidence as evidence
    recorder = evidence.MarketInputEvidence()
    blocked_directory = tmp_path / "occupied"
    blocked_directory.write_text("original user file")
    spool = recorder.spool_directory
    with pytest.raises(FileExistsError):
        evidence.write_market_input_evidence(SimpleNamespace(_market_input_evidence=recorder),
                                             blocked_directory, finalize=True)
    assert not spool.exists()
    assert blocked_directory.read_text() == "original user file"


def test_actual_backtest_finalizes_input_spool_even_if_other_provenance_fails(tmp_path, monkeypatch):
    from lumibot.backtesting import PandasDataBacktesting
    from lumibot.entities import Data
    from lumibot.strategies import Strategy
    import lumibot.backtesting.data_provenance as provenance
    import lumibot.backtesting.market_input_evidence as evidence

    class ObserveHistory(Strategy):
        def initialize(self):
            self.sleeptime = "1M"
            self.set_market("24/7")

        def on_trading_iteration(self):
            self.get_historical_prices(self.parameters["asset"], 1, timestep="minute")

    def unavailable(*args, **kwargs):
        raise OSError("unrelated provenance failure")

    monkeypatch.setattr(provenance, "write_backtest_data_provenance", unavailable)
    monkeypatch.chdir(tmp_path)
    asset = Asset("TEST")
    dates = pd.date_range("2025-01-01", periods=6, freq="min", tz="UTC")
    frame = pd.DataFrame({column: 10. for column in ("open", "high", "low", "close")}, index=dates)
    frame["volume"] = 1000
    data = Data(asset, frame, timestep="minute")
    _, strategy = ObserveHistory.run_backtest(
        PandasDataBacktesting, dates[1], dates[-1], pandas_data=[data],
        parameters={"asset": asset}, benchmark_asset=None, risk_free_rate=0,
        show_plot=False, show_tearsheet=False, save_tearsheet=False,
        show_indicators=False, show_progress_bar=False, quiet_logs=True,
        save_logfile=False, save_stats_file=False,
    )
    recorder = strategy.broker.data_source._market_input_evidence
    assert not recorder.spool_directory.exists()
    manifest = json.loads((tmp_path / "logs/market_input_manifest.json").read_text())
    assert manifest["complete"] is True
    assert manifest["files"]["market_input_observations.csv"]["rows"] >= 1


def test_warm_serialization_cache_avoids_duplicate_disk_row_writes():
    import lumibot.backtesting.market_input_evidence as evidence
    recorder = evidence.MarketInputEvidence()
    frame = pd.DataFrame({"close": [10., 11.]}, index=pd.date_range("2025-01-01", periods=2, tz="UTC"))
    arguments = dict(asset=Asset("TEST"), timestep="day", frame=frame, purpose="history")
    recorder.record(when=frame.index[-1], **arguments)
    queries = []
    recorder._connection.set_trace_callback(queries.append)
    recorder.record(when=frame.index[-1] + pd.Timedelta(days=1), **arguments)
    assert not any("INSERT OR IGNORE INTO input_rows" in query for query in queries)
    assert len(recorder.rows) == 2 and len(recorder.observations) == 2
    recorder.close()


def test_capture_transaction_rolls_back_rows_and_invalidates_cache_after_error():
    import lumibot.backtesting.market_input_evidence as evidence
    recorder = evidence.MarketInputEvidence()
    source = SimpleNamespace(_market_input_evidence=recorder)
    frame = pd.DataFrame({"close": [10.]}, index=pd.date_range("2025-01-01", periods=1, tz="UTC"))
    arguments = dict(source=source, asset=Asset("TEST"), timestep="day", purpose="history")
    evidence.record_market_input(when=frame.index[0], frame=frame, **arguments)
    recorder._connection.execute("CREATE TRIGGER fail_observation BEFORE INSERT ON observations WHEN NEW.sequence = 1 BEGIN SELECT RAISE(FAIL, 'synthetic storage failure'); END")
    recorder._connection.commit()
    changed = frame.copy()
    changed.iloc[0, 0] = 12.
    evidence.record_market_input(when=frame.index[0] + pd.Timedelta(days=1), frame=changed, **arguments)
    assert len(recorder.rows) == len(recorder.observations) == 1
    assert "capture_error" in recorder.incomplete
    recorder._connection.execute("DROP TRIGGER fail_observation")
    recorder._connection.commit()
    evidence.record_market_input(when=frame.index[0] + pd.Timedelta(days=2), frame=changed, **arguments)
    assert len(recorder.rows) == len(recorder.observations) == 2
    assert {row["close"] for row in recorder.rows.values()} == {10., 12.}
    recorder.close()


def test_streaming_capture_accepts_concurrent_duplicate_and_distinct_reads(tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    import lumibot.backtesting.market_input_evidence as evidence
    recorder = evidence.MarketInputEvidence(max_cache_rows=8)
    frame = pd.DataFrame({"close": [10.]}, index=pd.date_range("2025-01-01", periods=1, tz="UTC"))
    def capture(number):
        recorder.record(asset=Asset("TEST"), timestep="day", frame=frame,
                        when=frame.index[0] + pd.Timedelta(minutes=number), purpose="history")
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(capture, list(range(50)) * 2))
    assert len(recorder.rows) == 1 and len(recorder.observations) == 50
    assert [row["sequence"] for row in recorder.observations] == list(range(50))
    evidence.write_market_input_evidence(SimpleNamespace(_market_input_evidence=recorder), tmp_path, finalize=True)
    assert json.loads((tmp_path / "market_input_manifest.json").read_text())["complete"] is True


def test_snapshot_export_remains_compatible_with_more_capture(tmp_path):
    import lumibot.backtesting.market_input_evidence as evidence
    recorder = evidence.MarketInputEvidence()
    source = SimpleNamespace(_market_input_evidence=recorder)
    frame = pd.DataFrame({"close": [10.]}, index=pd.date_range("2025-01-01", periods=1, tz="UTC"))
    arguments = dict(asset=Asset("TEST"), timestep="day", frame=frame, purpose="history")
    recorder.record(when=frame.index[0], **arguments)
    evidence.write_market_input_evidence(source, tmp_path / "first")
    recorder.record(when=frame.index[0] + pd.Timedelta(days=1), **arguments)
    evidence.write_market_input_evidence(source, tmp_path / "second", finalize=True)
    first = json.loads((tmp_path / "first/market_input_manifest.json").read_text())
    second = json.loads((tmp_path / "second/market_input_manifest.json").read_text())
    assert first["files"]["market_input_observations.csv"]["rows"] == 1
    assert second["files"]["market_input_observations.csv"]["rows"] == 2


def test_explicit_observation_ceiling_still_reports_incomplete_capture(tmp_path):
    import lumibot.backtesting.market_input_evidence as evidence
    recorder = evidence.MarketInputEvidence(max_observations=1)
    frame = pd.DataFrame({"close": [10.]}, index=pd.date_range("2025-01-01", periods=1, tz="UTC"))
    for day in (0, 1):
        recorder.record(asset=Asset("TEST"), timestep="day", frame=frame,
                        when=frame.index[0] + pd.Timedelta(days=day), purpose="history")
    evidence.write_market_input_evidence(SimpleNamespace(_market_input_evidence=recorder), tmp_path, finalize=True)
    manifest = json.loads((tmp_path / "market_input_manifest.json").read_text())
    assert manifest["complete"] is False
    assert manifest["incompleteReasons"] == ["observation_limit"]
