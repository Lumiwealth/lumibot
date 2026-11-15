"""Observe actual public last-price returns without executing a trading strategy."""
import csv
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from types import MethodType, SimpleNamespace

import pandas as pd
import pytest

from lumibot.backtesting.market_input_evidence import (
    MarketInputEvidence, record_market_input, write_market_input_evidence,
)
from lumibot.entities import Asset
from lumibot.strategies.strategy import Strategy


def _strategy(value, *, source=None, error=None, backtesting=True):
    source = source or SimpleNamespace()
    calls, milestones = [], []
    def lookup(asset, **kwargs):
        calls.append((asset, kwargs))
        if error is not None:
            raise error
        return value
    broker = SimpleNamespace(data_source=source, datetime=datetime(2025, 2, 3, 12, tzinfo=timezone.utc),
                             IS_BACKTESTING_BROKER=backtesting, get_last_price=lookup)
    strategy = SimpleNamespace(broker=broker, is_backtesting=backtesting,
        quote_asset=Asset("USD", "forex"), _sanitize_user_asset=lambda value: value,
        _should_use_daily_last_price=lambda asset: False,
        _record_backtest_runtime_milestone=milestones.append,
        log_message=lambda *args, **kwargs: None)
    strategy._note_backtest_data = MethodType(Strategy._note_backtest_data, strategy)
    strategy._record_usable_backtest_price = MethodType(Strategy._record_usable_backtest_price, strategy)
    return strategy, source, calls, milestones


def _observations(source):
    recorder = getattr(source, "_market_input_evidence", None)
    return [] if recorder is None else [row for row in recorder.observations if row["purpose"] == "last_price"]


def test_missing_last_price_is_visible_even_when_history_exists():
    strategy, source, _, _ = _strategy(None)
    asset, quote = Asset("TOKEN/USDT:USDT", "crypto_future"), Asset("USDT", "crypto")
    frame = pd.DataFrame({"close": [10., 11.]}, index=pd.date_range("2025-02-01", periods=2, tz="UTC"))
    record_market_input(source=source, asset=asset, quote=quote, timestep="day",
        when=strategy.broker.datetime, frame=frame, purpose="history")
    assert Strategy.get_last_price(strategy, asset, quote=quote) is None
    quotes = _observations(source)
    assert len(quotes) == 1, "History must not hide a missing last-price return"
    assert quotes[0]["value"] is None and quotes[0]["unavailableReason"] == "returned_none"
    assert quotes[0]["method"] == "get_last_price" and quotes[0]["valueType"] == "NoneType"
    assert len(source._market_input_evidence.rows) == 2
    source._market_input_evidence.close()


def test_last_price_observation_preserves_literal_type_identity_time_and_cache():
    value = Decimal("12.3400")
    strategy, source, calls, milestones = _strategy(value)
    asset = Asset("TOKEN/USDT:USDT", "crypto_future")
    usdt, usdc = Asset("USDT", "crypto"), Asset("USDC", "crypto")
    assert Strategy.get_last_price(strategy, asset, quote=usdt, exchange="venue") is value
    assert Strategy.get_last_price(strategy, asset, quote=usdt, exchange="venue") is value
    assert Strategy.get_last_price(strategy, asset, quote=usdc, exchange="venue") is value
    assert len(calls) == 2, "Observation must not cause another price lookup"
    quotes = _observations(source)
    assert len(quotes) == 2, "Repeated same-clock cached observations deduplicate"
    assert {json.loads(row["asset"])["quote"]["symbol"] for row in quotes} == {"USDT", "USDC"}
    for row in quotes:
        assert json.loads(row["asset"])["symbol"] == "TOKEN/USDT:USDT"
        assert row["observedAt"] == strategy.broker.datetime.isoformat()
        assert row["value"] == "12.3400" and row["valueType"] == "Decimal"
        assert row["exchange"] == "venue" and row["unavailableReason"] is None
        assert "frameSha256" not in row, "A scalar result is not a historical frame"
    assert milestones.count("first_usable_price_at") == 2  # Existing cache/milestone behavior.
    assert len(source._market_input_evidence.rows) == 0
    source._market_input_evidence.close()


@pytest.mark.parametrize("value,literal,reason", [
    (float("nan"), "nan", "non_finite"),
    (float("inf"), "inf", "non_finite"),
    (0.0, 0.0, None),
    (-2.0, -2.0, None),
])
def test_last_price_observation_preserves_nonfinite_and_nonpositive_returns(value, literal, reason):
    strategy, source, _, _ = _strategy(value)
    assert Strategy.get_last_price(strategy, Asset("TEST")) is value
    row = _observations(source)[0]
    assert row["value"] == literal and row["unavailableReason"] == reason
    source._market_input_evidence.close()


def test_caught_last_price_error_is_unavailable_without_recording_exception_text():
    strategy, source, _, _ = _strategy(None, error=RuntimeError("private provider detail"))
    assert Strategy.get_last_price(strategy, Asset("TEST")) is None
    row = _observations(source)[0]
    assert row["value"] is None and row["unavailableReason"] == "lookup_error"
    assert "private provider detail" not in json.dumps(row)
    source._market_input_evidence.close()


def test_last_price_observations_share_capture_limit_export_hash_and_cleanup(tmp_path):
    recorder = MarketInputEvidence(max_observations=2)
    strategy, source, _, _ = _strategy(Decimal("1.2500"), source=SimpleNamespace(_market_input_evidence=recorder))
    asset = Asset("TEST")
    frame = pd.DataFrame({"close": [10.]}, index=pd.date_range("2025-02-01", periods=1, tz="UTC"))
    record_market_input(source=source, asset=asset, timestep="day", when=strategy.broker.datetime,
                        frame=frame, purpose="history")
    assert Strategy.get_last_price(strategy, asset) == Decimal("1.2500")
    strategy.broker.datetime = strategy.broker.datetime.replace(hour=13)
    assert Strategy.get_last_price(strategy, asset) == Decimal("1.2500")
    spool = recorder.spool_directory
    write_market_input_evidence(source, tmp_path, finalize=True)
    assert not spool.exists()
    manifest = json.loads((tmp_path / "market_input_manifest.json").read_text())
    assert manifest["complete"] is False and manifest["incompleteReasons"] == ["observation_limit"]
    with (tmp_path / "market_input_observations.csv").open() as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 2 and [row["purpose"] for row in rows] == ["history", "last_price"]
    assert rows[0]["value"] == "" and rows[1]["value"] == "1.2500"
    metadata = manifest["files"]["market_input_observations.csv"]
    assert metadata["sha256"] == hashlib.sha256((tmp_path / "market_input_observations.csv").read_bytes()).hexdigest()


def test_last_price_capture_failure_cannot_change_strategy_return():
    recorder = MarketInputEvidence()
    recorder.close()
    value = Decimal("8.200")
    strategy, source, _, _ = _strategy(value, source=SimpleNamespace(_market_input_evidence=recorder))
    assert Strategy.get_last_price(strategy, Asset("TEST")) is value
    assert "capture_error" in recorder.incomplete


def test_default_last_price_capture_has_a_byte_bound_without_stopping_price_calls(tmp_path):
    recorder = MarketInputEvidence()
    assert getattr(recorder, "max_last_price_bytes", None) == 10 * 1024 * 1024
    # A large literal return proves the ordinary default without thousands of writes.
    value = "1" * recorder.max_last_price_bytes
    strategy, source, calls, _ = _strategy(value, source=SimpleNamespace(_market_input_evidence=recorder))
    assert Strategy.get_last_price(strategy, Asset("TEST")) is value
    assert len(calls) == 1 and _observations(source) == []
    write_market_input_evidence(source, tmp_path, finalize=True)
    manifest = json.loads((tmp_path / "market_input_manifest.json").read_text())
    assert manifest["lastPriceByteLimit"] == 10 * 1024 * 1024
    assert manifest["lastPricePayloadBytes"] == 0
    assert manifest["complete"] is False
    assert manifest["incompleteReasons"] == ["last_price_byte_limit"]


def test_last_price_byte_limit_is_scalar_only_and_exports_omitted_evidence(tmp_path):
    recorder = MarketInputEvidence()
    recorder.max_last_price_bytes = 450
    strategy, source, calls, _ = _strategy(Decimal("1.2500"), source=SimpleNamespace(_market_input_evidence=recorder))
    asset = Asset("TEST")
    frame = pd.DataFrame({"close": [10.]}, index=pd.date_range("2025-02-01", periods=1, tz="UTC"))
    kwargs = dict(source=source, asset=asset, timestep="day", when=strategy.broker.datetime, purpose="history")
    record_market_input(frame=frame, **kwargs)
    assert Strategy.get_last_price(strategy, asset) == Decimal("1.2500")
    strategy.broker.datetime = strategy.broker.datetime.replace(hour=13)
    assert Strategy.get_last_price(strategy, asset) == Decimal("1.2500")
    record_market_input(frame=frame.assign(close=11.), **kwargs)
    assert len(calls) == 2 and len(_observations(source)) == 1
    assert len(recorder.rows) == 2 and len(recorder.observations) == 3
    consumed = recorder._connection.execute(
        "SELECT SUM(LENGTH(CAST(payload AS BLOB))) FROM observations WHERE json_extract(payload, '$.purpose') = 'last_price'"
    ).fetchone()[0]
    assert recorder._last_price_payload_bytes == consumed <= recorder.max_last_price_bytes
    spool = recorder.spool_directory
    write_market_input_evidence(source, tmp_path, finalize=True)
    assert not spool.exists()
    manifest = json.loads((tmp_path / "market_input_manifest.json").read_text())
    assert manifest["lastPriceByteLimit"] == 450 and manifest["lastPricePayloadBytes"] == consumed
    assert manifest["rowLimit"] is None and manifest["observationLimit"] is None
    assert manifest["complete"] is False and manifest["incompleteReasons"] == ["last_price_byte_limit"]
    path = tmp_path / "market_input_observations.csv"
    with path.open() as handle:
        rows = list(csv.DictReader(handle))
    assert [row["purpose"] for row in rows] == ["history", "last_price", "history"]
    assert manifest["files"][path.name]["sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()


def test_duplicate_last_price_does_not_consume_byte_budget_or_report_truncation():
    recorder = MarketInputEvidence()
    strategy, source, calls, _ = _strategy(Decimal("1.2500"), source=SimpleNamespace(_market_input_evidence=recorder))
    for asset in [Asset("FIRST"), Asset("SECOND")]:
        assert Strategy.get_last_price(strategy, asset) == Decimal("1.2500")
    consumed = recorder._last_price_payload_bytes
    recorder.max_last_price_bytes = consumed
    assert Strategy.get_last_price(strategy, Asset("FIRST")) == Decimal("1.2500")
    assert len(calls) == 2 and len(_observations(source)) == 2
    assert recorder._last_price_payload_bytes == consumed
    assert "last_price_byte_limit" not in recorder.incomplete
    recorder.close()


def test_rolled_back_batch_restores_shared_capture_counts_and_accepts_later_price():
    import sqlite3

    recorder = MarketInputEvidence(max_observations=3)
    recorder.max_last_price_bytes = 450  # Only one literal observation fits.
    value = Decimal("8.200")
    strategy, source, _, _ = _strategy(value, source=SimpleNamespace(_market_input_evidence=recorder))
    asset = Asset("TEST")
    frame = pd.DataFrame({"close": [10.]}, index=pd.date_range("2025-02-01", periods=1, tz="UTC"))
    kwargs = dict(source=source, asset=asset, timestep="day", when=strategy.broker.datetime, purpose="history")
    record_market_input(frame=frame, **kwargs)
    # Actual SQLite transaction refusal, not a fabricated database response.
    def refuse_commit(action, argument, *rest):
        return sqlite3.SQLITE_DENY if action == sqlite3.SQLITE_TRANSACTION and argument == "COMMIT" else sqlite3.SQLITE_OK
    recorder._connection.set_authorizer(refuse_commit)
    with recorder.batch():
        assert Strategy.get_last_price(strategy, asset) is value
        record_market_input(frame=frame.assign(close=11.), **kwargs)
    recorder._connection.set_authorizer(None)
    assert recorder._connection.execute("SELECT COUNT(*) FROM observations").fetchone()[0] == 1
    assert len(recorder.observations) == len(recorder.rows) == 1
    assert recorder._last_price_payload_bytes == 0
    assert "capture_error" in recorder.incomplete
    assert Strategy.get_last_price(strategy, asset) is value  # Same-clock cache, now captured successfully.
    assert len(_observations(source)) == 1 and len(recorder.observations) == 2
    assert 0 < recorder._last_price_payload_bytes <= recorder.max_last_price_bytes
    assert "observation_limit" not in recorder.incomplete
    recorder.close()


@pytest.mark.parametrize("forward_fill", [False, True])
def test_daily_last_price_paths_record_the_actual_return_without_extra_lookup(forward_fill):
    strategy, source, calls, _ = _strategy(None, error=AssertionError("No broker fallback expected"))
    strategy._should_use_daily_last_price = lambda asset: True
    strategy._supports_daily_last_price_optimization = lambda: True
    frame = pd.DataFrame({"close": [Decimal("9.25")]},
        index=pd.date_range("2025-02-02", periods=1, tz="UTC"))
    requested = []
    def history(asset, length, **kwargs):
        requested.append(length)
        return SimpleNamespace(df=frame.iloc[:0] if forward_fill and length == 1 else frame)
    strategy.get_historical_prices = history
    assert Strategy.get_last_price(strategy, Asset("TEST")) == 9.25
    assert calls == [] and requested == ([1, 5] if forward_fill else [1])
    assert _observations(source)[0]["value"] == 9.25
    assert _observations(source)[0]["valueType"] == "float"
    source._market_input_evidence.close()


def test_live_last_price_does_not_create_backtest_input_capture(monkeypatch):
    import lumibot.strategies.strategy as owner
    monkeypatch.setattr(owner, "IS_BACKTESTING", False)
    strategy, source, _, _ = _strategy(Decimal("2.100"), backtesting=False)
    assert Strategy.get_last_price(strategy, Asset("TEST")) == Decimal("2.100")
    assert not hasattr(source, "_market_input_evidence")
