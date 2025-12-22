from lumibot.strategies.strategy_executor import _ensure_strategy_log_message_accepts_framework_kwargs


class _StrategyNoKwargs:
    def __init__(self):
        self.messages = []

    def log_message(self, message: str):
        self.messages.append(message)
        return message


class _StrategySomeKwargs:
    def __init__(self):
        self.calls = []

    def log_message(self, message: str, level: str = "info"):
        self.calls.append((message, level))
        return message


def test_strategy_log_message_wrapper_drops_unsupported_kwargs():
    strategy = _StrategyNoKwargs()

    _ensure_strategy_log_message_accepts_framework_kwargs(strategy)
    strategy.log_message("hello", color="green", broadcast=True)

    assert strategy.messages == ["hello"]


def test_strategy_log_message_wrapper_preserves_supported_kwargs():
    strategy = _StrategySomeKwargs()

    _ensure_strategy_log_message_accepts_framework_kwargs(strategy)
    strategy.log_message("hello", level="warning", color="green")

    assert strategy.calls == [("hello", "warning")]


def test_strategy_log_message_wrapper_is_idempotent():
    strategy = _StrategyNoKwargs()

    _ensure_strategy_log_message_accepts_framework_kwargs(strategy)
    first = strategy.log_message
    _ensure_strategy_log_message_accepts_framework_kwargs(strategy)
    second = strategy.log_message

    assert first is second

