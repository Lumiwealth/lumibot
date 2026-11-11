"""find_strike_for_delta returns the listed strike whose delta is closest to the target.

Before, the search probed a few strikes near a Black-Scholes estimate and stopped once one
was within 0.02 of the target, so on real chains it could return a strike up to about 0.12
delta away while a closer strike was listed. Live bots and backtests call the same method,
so the rule is the same on both paths: compute the delta of every candidate strike that
has a real price, then pick the closest. Ties go to the smaller absolute delta (further out
of the money), then to the lower strike. Strikes with no price are never candidates.
"""

import math
from datetime import date, datetime

import pytest
import pytz

from lumibot.backtesting import PandasDataBacktesting
from lumibot.components.options_helper import OptionsHelper
from lumibot.entities import Asset
from lumibot.strategies.strategy import Strategy
from lumibot.tools.black_scholes import BS
from tests.test_options_helper import _StubBroker

TZ = pytz.timezone("America/New_York")
EXPIRY = date(2026, 10, 23)
UNDERLYING = 500.0
RATE = 0.0412
STRIKES = [float(k) for k in range(470, 531)]


def _price(strike, right):
    """A smile with uneven noise: realistic enough that a fixed-volatility guess is off."""
    vol = 18 + 0.004 * (strike - 497) ** 2 + (1.7 if int(strike) % 3 == 0 else 0.0)
    days = 4.25
    model = BS([UNDERLYING, strike, RATE * 100, days], volatility=vol)
    value = model.callPrice if right == "CALL" else model.putPrice
    return round(value, 2) if value >= 0.01 else None


class _LiveBroker(_StubBroker):
    IS_BACKTESTING_BROKER = False


class _Strategy:
    get_greeks = Strategy.get_greeks
    is_backtesting = True
    risk_free_rate = RATE

    def __init__(self, broker_cls, missing=()):
        source = PandasDataBacktesting(
            datetime_start=TZ.localize(datetime(2026, 10, 19)),
            datetime_end=TZ.localize(datetime(2026, 10, 23)),
            pandas_data={},
        )
        source._datetime = TZ.localize(datetime(2026, 10, 19, 10, 0))
        source.option_quote_fallback_allowed = True
        self.broker = broker_cls(source)
        self.missing = set(missing)
        self.lines = []

    def log_message(self, message, color=None, broadcast=False):
        self.lines.append(str(message))

    def get_datetime(self):
        return self.broker.data_source._datetime

    def get_quote(self, asset, **_kwargs):
        return None

    def get_last_price(self, asset, **_kwargs):
        if getattr(asset, "asset_type", None) != "option":
            return UNDERLYING
        if float(asset.strike) in self.missing:
            return None
        return _price(float(asset.strike), str(asset.right).upper())

    def get_chains(self, _asset):
        listed = {EXPIRY.isoformat(): STRIKES}
        return {"Multiplier": 100, "Exchange": "SMART", "Chains": {"CALL": listed, "PUT": listed}}


def _closest(strategy, target, right):
    """Brute force over every listed strike in the search window, by the documented rule."""
    helper = OptionsHelper(strategy)
    best = None
    for strike in STRIKES:
        if not (UNDERLYING - 20 <= strike <= UNDERLYING + 30):
            continue
        delta = helper.get_delta_for_strike(Asset("SPY"), UNDERLYING, strike, EXPIRY, right)
        if delta is None:
            continue
        key = (abs(delta - target), abs(delta), strike)
        if best is None or key < best[0]:
            best = (key, strike)
    return best[1]


def _pick(strategy, target, right):
    return OptionsHelper(strategy).find_strike_for_delta(Asset("SPY"), UNDERLYING, target, EXPIRY, right)


CASES = [(0.30, "call"), (0.16, "call"), (0.5, "call"), (-0.30, "put"), (-0.16, "put"), (-0.05, "put")]


@pytest.mark.parametrize("target,right", CASES)
def test_pick_is_the_closest_listed_strike(target, right):
    strategy = _Strategy(_StubBroker)
    assert _pick(strategy, target, right) == _closest(_Strategy(_StubBroker), target, right)


@pytest.mark.parametrize("target,right", CASES)
def test_live_and_backtest_pick_the_same_strike(target, right):
    assert _pick(_Strategy(_LiveBroker), target, right) == _pick(_Strategy(_StubBroker), target, right)


def test_strikes_without_a_price_are_never_picked():
    exact = _closest(_Strategy(_StubBroker), -0.30, "put")
    strategy = _Strategy(_StubBroker, missing={exact})
    picked = _pick(strategy, -0.30, "put")
    assert picked is not None and picked != exact
    assert picked == _closest(_Strategy(_StubBroker, missing={exact}), -0.30, "put")


def test_no_priced_strike_returns_none():
    strategy = _Strategy(_StubBroker, missing=set(STRIKES))
    assert _pick(strategy, 0.30, "call") is None


def test_a_later_minute_is_searched_again():
    """The pick is remembered within one bar only; a new minute is a new search."""
    strategy = _Strategy(_StubBroker)
    helper = OptionsHelper(strategy)
    first = helper.find_strike_for_delta(Asset("SPY"), UNDERLYING, 0.30, EXPIRY, "call")
    strategy.missing = {first}
    strategy.broker.data_source._datetime = TZ.localize(datetime(2026, 10, 19, 10, 1))
    second = helper.find_strike_for_delta(Asset("SPY"), UNDERLYING, 0.30, EXPIRY, "call")
    assert second != first
    assert math.isfinite(second)


class _QuoteSource:
    """A live quote source: one call per contract, or many contracts in one get_quotes call."""

    def __init__(self, batched=True):
        self.single = 0
        self.bulk = []
        if not batched:
            self.get_quotes = None

    def _quote(self, asset):
        from lumibot.entities import Quote

        price = _price(float(asset.strike), str(asset.right).upper())
        if price is None:
            return Quote(asset=asset)
        return Quote(asset=asset, bid=round(price - 0.01, 2), ask=round(price + 0.01, 2))

    def get_quote(self, asset, quote=None, exchange=None):
        self.single += 1
        return self._quote(asset)

    def get_quotes(self, assets, quote=None, exchange=None):
        self.bulk.append(len(assets))
        return {a: self._quote(a) for a in assets}


class _LiveQuoteStrategy(_Strategy):
    """Reads quotes through the standard Strategy.get_quote, like a real live strategy."""

    get_quote = Strategy.get_quote
    is_backtesting = False

    def __init__(self, batched=True):
        super().__init__(_LiveBroker)
        self.source = _QuoteSource(batched)
        self.broker.option_source = self.source

    def _sanitize_user_asset(self, asset):
        return asset


def _brute_force(strategy, target, right):
    helper = OptionsHelper(strategy)
    best = None
    for strike in STRIKES:
        if not (UNDERLYING - 20 <= strike <= UNDERLYING + 30):
            continue
        helper._per_bar_option_mark_cache = {}
        delta = helper.get_delta_for_strike(Asset("SPY"), UNDERLYING, strike, EXPIRY, right)
        if delta is not None:
            key = (abs(delta - target), abs(delta), strike)
            best = min(best, (key, strike)) if best else (key, strike)
    return best[1]


@pytest.mark.parametrize("target,right", CASES)
def test_live_pick_loads_all_quotes_in_one_request(target, right):
    strategy = _LiveQuoteStrategy(batched=True)
    picked = _pick(strategy, target, right)
    assert strategy.source.bulk == [51]
    assert strategy.source.single == 0
    assert picked == _brute_force(_LiveQuoteStrategy(batched=False), target, right)


@pytest.mark.parametrize("target,right", CASES)
def test_live_pick_without_bulk_quotes_prices_few_strikes(target, right):
    strategy = _LiveQuoteStrategy(batched=False)
    picked = _pick(strategy, target, right)
    assert picked == _brute_force(_LiveQuoteStrategy(batched=False), target, right)
    assert strategy.source.single <= 15  # 51 listed strikes in the window
