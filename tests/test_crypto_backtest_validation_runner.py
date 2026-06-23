import importlib.util
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import duckdb


def _load_runner_module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "run_crypto_backtest_validation.py"
    spec = importlib.util.spec_from_file_location("run_crypto_backtest_validation", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _bare_strategy(strategy_cls, parameters):
    strategy = strategy_cls.__new__(strategy_cls)
    strategy.parameters = parameters
    strategy.broker = SimpleNamespace(market=None)
    strategy.logger = MagicMock()
    return strategy


def test_crypto_validation_runner_uses_backtest_parameters_for_requested_quote():
    runner = _load_runner_module()
    strategy_cls = runner._build_strategy_classes()["buy_hold"]
    strategy = _bare_strategy(
        strategy_cls,
        {
            "base_symbol": "BTC",
            "quote_symbol": "USD",
            "sleeptime": "1H",
        },
    )

    strategy.initialize()

    assert strategy.base.symbol == "BTC"
    assert strategy.quote.symbol == "USD"
    assert strategy.quote.asset_type == "forex"
    assert strategy.asset_pair[1] is strategy.quote
    assert strategy.broker.market == "24/7"


def test_crypto_validation_runner_preserves_crypto_quote_assets():
    runner = _load_runner_module()
    strategy_cls = runner._build_strategy_classes()["buy_hold"]
    strategy = _bare_strategy(
        strategy_cls,
        {
            "base_symbol": "BTC",
            "quote_symbol": "USDT",
            "sleeptime": "1H",
        },
    )

    strategy.initialize()

    assert strategy.base.symbol == "BTC"
    assert strategy.quote.symbol == "USDT"
    assert strategy.quote.asset_type == "crypto"
    assert strategy.asset_pair[1] is strategy.quote


def test_crypto_validation_runner_normalizes_supported_timeframes():
    runner = _load_runner_module()

    assert runner._normalize_timestep("1m") == "minute"
    assert runner._normalize_timestep("minute") == "minute"
    assert runner._normalize_timestep("1h") == "hour"
    assert runner._normalize_timestep("hour") == "hour"
    assert runner._normalize_timestep("1d") == "day"
    assert runner._normalize_timestep("day") == "day"


def test_crypto_validation_runner_sets_backtest_data_source_timestep():
    runner = _load_runner_module()
    strategy_cls = runner._build_strategy_classes()["buy_hold"]
    strategy = _bare_strategy(
        strategy_cls,
        {
            "base_symbol": "BTC",
            "quote_symbol": "USDT",
            "sleeptime": "1D",
            "timestep": "1d",
        },
    )
    strategy.broker.data_source = SimpleNamespace(_timestep=None)

    strategy.initialize()

    assert strategy.data_timestep == "day"
    assert strategy.broker.data_source._timestep == "day"
    assert strategy.validation_events[0]["event"] == "data_timestep_set"
    assert strategy.validation_events[0]["timestep"] == "day"


def test_crypto_validation_runner_uses_backtest_parameters_for_scheduled_cases():
    runner = _load_runner_module()
    strategy_cls = runner._build_strategy_classes()["round_trip"]
    strategy = _bare_strategy(
        strategy_cls,
        {
            "base_symbol": "BTC",
            "quote_symbol": "EUR",
            "buy_at": "2026-03-15T02:00:00+00:00",
            "sell_at": "2026-03-15T10:00:00+00:00",
        },
    )

    strategy.initialize()

    assert strategy.quote.symbol == "EUR"
    assert strategy.quote.asset_type == "forex"
    assert strategy.buy_at.isoformat() == "2026-03-15T02:00:00+00:00"
    assert strategy.sell_at.isoformat() == "2026-03-15T10:00:00+00:00"


def test_crypto_validation_runner_long_profile_spreads_round_trip_across_window():
    runner = _load_runner_module()
    strategies = {
        "buy_hold": object,
        "round_trip": object,
        "alternating": object,
        "order_matrix": object,
    }
    start = datetime(2026, 3, 1, tzinfo=timezone.utc)
    end = datetime(2026, 6, 1, tzinfo=timezone.utc)

    cases = runner._build_case_plan(
        start=start,
        end=end,
        profile="long",
        strategy_classes=strategies,
    )

    round_trip = dict((name, params) for name, _cls, params in cases)["round_trip"]
    assert round_trip["buy_at"] == "2026-03-19T09:36:00+00:00"
    assert round_trip["sell_at"] == "2026-05-13T14:24:00+00:00"


def test_crypto_validation_runner_long_profile_spreads_alternating_trades():
    runner = _load_runner_module()
    strategies = {
        "buy_hold": object,
        "round_trip": object,
        "alternating": object,
        "order_matrix": object,
    }
    start = datetime(2026, 3, 1, tzinfo=timezone.utc)
    end = datetime(2026, 6, 1, tzinfo=timezone.utc)

    cases = runner._build_case_plan(
        start=start,
        end=end,
        profile="long",
        strategy_classes=strategies,
    )

    alternating = dict((name, params) for name, _cls, params in cases)["alternating"]
    assert alternating["max_orders"] == 12
    assert alternating["interval_hours"] == 184


def test_crypto_validation_runner_includes_order_matrix_case():
    runner = _load_runner_module()
    strategies = {
        "buy_hold": object,
        "round_trip": object,
        "alternating": object,
        "order_matrix": object,
    }
    start = datetime(2026, 3, 1, tzinfo=timezone.utc)
    end = datetime(2026, 6, 1, tzinfo=timezone.utc)

    cases = runner._build_case_plan(
        start=start,
        end=end,
        profile="long",
        strategy_classes=strategies,
    )

    assert [name for name, _cls, _params in cases] == [
        "buy_hold",
        "round_trip",
        "alternating",
        "order_matrix",
    ]


def test_crypto_validation_runner_buy_hold_uses_full_allocation():
    runner = _load_runner_module()
    strategies = {
        "buy_hold": object,
        "round_trip": object,
        "alternating": object,
        "order_matrix": object,
    }
    start = datetime(2026, 3, 1, tzinfo=timezone.utc)
    end = datetime(2026, 6, 1, tzinfo=timezone.utc)

    cases = runner._build_case_plan(
        start=start,
        end=end,
        profile="long",
        strategy_classes=strategies,
    )

    params_by_name = {name: params for name, _cls, params in cases}
    assert params_by_name["buy_hold"]["allocation"] == 1.0
    assert "allocation" not in params_by_name["round_trip"]
    assert "allocation" not in params_by_name["alternating"]
    assert "allocation" not in params_by_name["order_matrix"]


def test_crypto_validation_runner_flags_incomplete_actual_cache_coverage(tmp_path, monkeypatch):
    runner = _load_runner_module()
    import lumibot.constants as constants

    monkeypatch.setattr(constants, "LUMIBOT_CACHE_FOLDER", str(tmp_path))
    cache_dir = tmp_path / "coinbase"
    cache_dir.mkdir()
    cache_file = cache_dir / "BTC_USDT_1m.duckdb"
    with duckdb.connect(str(cache_file)) as con:
        con.execute(
            "create table candles (datetime timestamp, open decimal, high decimal, low decimal, close decimal, volume decimal, missing integer)"
        )
        con.execute(
            "create table cache_dt_ranges (id string, start_dt timestamp, end_dt timestamp)"
        )
        con.execute(
            "insert into candles values (timestamp '2026-03-01 00:00:00', 100, 101, 99, 100, 1, 0)"
        )
        con.execute(
            "insert into cache_dt_ranges values ('range1', timestamp '2026-03-01 00:00:00', timestamp '2026-06-01 00:00:00')"
        )

    coverage = runner._cache_coverage_checks(
        symbol="BTC/USDT",
        exchange_id="coinbase",
        timestep="minute",
        start=datetime(2026, 3, 1, tzinfo=timezone.utc),
        end=datetime(2026, 6, 1, tzinfo=timezone.utc),
        tolerance_hours=24,
    )

    assert coverage["row_count"] == 1
    assert coverage["range_metadata_end"] == "2026-06-01T00:00:00"
    assert coverage["coverage_end"] == "2026-03-01T00:00:00"
    assert coverage["coverage_ok"] is False
    assert coverage["error"] == "actual candle coverage does not reach requested window"
