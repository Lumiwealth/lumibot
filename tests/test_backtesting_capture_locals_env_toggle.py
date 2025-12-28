class DummyBroker:
    IS_BACKTESTING_BROKER = True


class DummyStrategy:
    def __init__(self):
        self.broker = DummyBroker()

    def on_trading_iteration(self):
        local_value = 123
        return local_value


def test_backtesting_capture_locals_default_off(monkeypatch):
    monkeypatch.delenv("BACKTESTING_CAPTURE_LOCALS", raising=False)

    from lumibot.strategies.strategy_executor import StrategyExecutor

    strategy = DummyStrategy()
    executor = StrategyExecutor(strategy)

    assert executor._capture_locals is False
    assert executor._on_trading_iteration_callable == strategy.on_trading_iteration
    assert not hasattr(executor._on_trading_iteration_callable, "locals")


def test_backtesting_capture_locals_enabled(monkeypatch):
    monkeypatch.setenv("BACKTESTING_CAPTURE_LOCALS", "true")

    from lumibot.strategies.strategy_executor import StrategyExecutor

    strategy = DummyStrategy()
    executor = StrategyExecutor(strategy)

    assert executor._capture_locals is True
    assert executor._on_trading_iteration_callable != strategy.on_trading_iteration

    executor._on_trading_iteration_callable()
    assert hasattr(executor._on_trading_iteration_callable, "locals")
    assert isinstance(executor._on_trading_iteration_callable.locals, (dict, type(None)))
