import importlib.util
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock


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
