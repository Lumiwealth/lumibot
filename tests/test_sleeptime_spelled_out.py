"""sleeptime accepts the spelled-out forms bar sizes use ("15 minutes", "1 hour").

An AI agent wrote ``self.sleeptime = "15 minutes"`` and the strategy crashed with a bare
``int()`` ValueError. Spelled-out units now work, the short forms are unchanged, and
anything else raises an error that lists valid examples.
"""

import pytest

from lumibot.strategies.strategy_executor import StrategyExecutor

to_seconds = StrategyExecutor._sleeptime_to_seconds


@pytest.mark.parametrize("value, seconds", [
    ("15 minutes", 900), ("1 minute", 60), ("1 hour", 3600), ("2 hours", 7200),
    ("1 day", 86400), ("3 days", 259200), ("30 seconds", 30), ("1 second", 1),
    ("15min", 900), ("15 mins", 900), ("2 hrs", 7200), ("15 Minutes", 900), ("  15 minutes  ", 900),
])
def test_spelled_out_units(value, seconds):
    assert to_seconds(value) == seconds


@pytest.mark.parametrize("value, seconds", [
    ("15M", 900), ("15m", 900), ("15T", 900), ("1H", 3600), ("1D", 86400), ("300S", 300), (5, 300),
])
def test_short_forms_unchanged(value, seconds):
    assert to_seconds(value) == seconds


@pytest.mark.parametrize("value", ["fifteen minutes", "15 fortnights", "", "M", "1.5H", None, "15"])
def test_unparseable_sleeptime_gives_a_clear_error(value):
    with pytest.raises(ValueError) as error:
        to_seconds(value)
    message = str(error.value)
    assert "15 minutes" in message and "15M" in message and "1D" in message
    assert repr(value) in message


@pytest.mark.parametrize("value, count, unit", [
    ("15 minutes", 15, "M"), ("2 hours", 2, "H"), ("1 day", 1, "D"), ("15T", 15, "M"), ("10S", 10, "S"), (5, 5, "M"),
])
def test_parse_sleeptime_returns_count_and_unit(value, count, unit):
    assert StrategyExecutor._parse_sleeptime(value) == (count, unit)
