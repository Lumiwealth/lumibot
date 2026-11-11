"""OptionsHelper must not flood minute-level runs with identical routine lines.

A minute strategy calling find_next_valid_option / evaluate_option_market /
find_strike_for_delta on every bar printed about 850 lines per second in a cloud backtest
(2026-11-10), almost all identical. Routine detail is INFO the first time and when the
outcome changes, DEBUG otherwise. Anything that explains why no option was usable stays INFO.
"""

from datetime import date, datetime, timedelta

from lumibot.components.options_helper import OptionsHelper
from lumibot.entities import Asset, Quote


class _Logger:
    def __init__(self):
        self.debug_lines = []

    def isEnabledFor(self, _level):
        return True

    def debug(self, message, *args, **kwargs):
        self.debug_lines.append(str(message))


class _DataSource:
    option_quote_fallback_allowed = True


class _Broker:
    IS_BACKTESTING_BROKER = True
    data_source = _DataSource()


class _Strategy:
    is_backtesting = True

    def __init__(self, *, bid=None, ask=None, last=2.5):
        self.broker = _Broker()
        self.logger = _Logger()
        self.info_lines = []
        self.now = datetime(2026, 10, 19, 10, 0)
        self._bid, self._ask, self._last = bid, ask, last

    def log_message(self, message, color=None, broadcast=False):
        self.info_lines.append(str(message))
        return message

    def get_datetime(self):
        return self.now

    def get_quote(self, asset, **_kwargs):
        return Quote(asset=asset, bid=self._bid, ask=self._ask, price=self._last)

    def get_last_price(self, _asset, **_kwargs):
        return self._last

    def get_chains(self, _asset):
        return {
            "Multiplier": 100,
            "Exchange": "SMART",
            "Chains": {"CALL": {"2026-10-23": [495.0, 500.0, 505.0]}, "PUT": {"2026-10-23": [495.0, 500.0, 505.0]}},
        }

    def get_greeks(self, asset, asset_price=None, underlying_price=None, **_kwargs):
        return {"delta": max(0.05, min(0.95, 0.5 - (float(asset.strike) - 500.0) / 50.0))}


def _count(lines, text):
    return sum(1 for line in lines if text in line)


def _option():
    return Asset("SPY", asset_type="option", expiration=date(2026, 10, 23), strike=500, right="call",
                 underlying_asset=Asset("SPY"))


def test_repeated_trade_only_evaluations_log_once_then_debug():
    strategy = _Strategy()
    helper = OptionsHelper(strategy)
    option = _option()
    for minute in range(60):
        strategy.now = datetime(2026, 10, 19, 10, 0) + timedelta(minutes=minute)
        evaluation = helper.evaluate_option_market(option, max_spread_pct=0.25)
        assert evaluation.buy_price == 2.5  # behaviour unchanged

    assert _count(strategy.info_lines, "Option market evaluation for") == 1
    assert _count(strategy.info_lines, "Using last-price fallback") == 1
    assert _count(strategy.logger.debug_lines, "Option market evaluation for") == 59


def test_blocked_evaluations_stay_at_info_every_time():
    strategy = _Strategy(bid=1.0, ask=2.0)  # 67 percent spread, max 25 percent: trade blocked
    helper = OptionsHelper(strategy)
    option = _option()
    for minute in range(5):
        strategy.now = datetime(2026, 10, 19, 10, 0) + timedelta(minutes=minute)
        assert helper.evaluate_option_market(option, max_spread_pct=0.25).spread_too_wide

    assert _count(strategy.info_lines, "Option market evaluation for") == 5


def test_repeated_next_valid_option_search_logs_once_per_outcome():
    strategy = _Strategy()
    helper = OptionsHelper(strategy)
    underlying = Asset("SPY")
    for minute in range(30):
        strategy.now = datetime(2026, 10, 19, 10, 0) + timedelta(minutes=minute)
        found = helper.find_next_valid_option(underlying, 500, date(2026, 10, 23), "call")
        assert found is not None and float(found.strike) == 500.0

    assert _count(strategy.info_lines, "Finding next valid option") == 1
    assert _count(strategy.info_lines, "Target strike 500 ->") == 1
    assert _count(strategy.logger.debug_lines, "Finding next valid option") == 29


def test_repeated_delta_search_logs_detail_once_and_result_on_change():
    strategy = _Strategy()
    helper = OptionsHelper(strategy)
    underlying = Asset("SPY")
    for minute in range(10):
        strategy.now = datetime(2026, 10, 19, 10, 0) + timedelta(minutes=minute)
        helper.find_strike_for_delta(underlying, 500.0 + minute * 0.01, 0.5, date(2026, 10, 23), "call")

    assert _count(strategy.info_lines, "STRIKE SEARCH") == 1
    assert _count(strategy.info_lines, "Trying strike") <= 8
    found_lines = _count(strategy.info_lines, "RESULT: Closest strike") + _count(strategy.info_lines, "Exact match found")
    assert found_lines == 1
    assert _count(strategy.logger.debug_lines, "STRIKE SEARCH") == 9
