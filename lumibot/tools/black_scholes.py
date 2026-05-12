from __future__ import annotations

import warnings
from collections.abc import Sequence
from math import e, erf, exp, log, pi, sqrt
from typing import Literal, Protocol, TypeAlias

warnings.filterwarnings("ignore", category=RuntimeWarning)

NumberLike: TypeAlias = int | float | str  # noqa: UP040
OptionArgs: TypeAlias = Sequence[NumberLike]  # noqa: UP040
PricePair: TypeAlias = list[float]  # noqa: UP040
PriceSide: TypeAlias = Literal["call", "put"]  # noqa: UP040

_SQRT_TWO = sqrt(2.0)
_INV_SQRT_TWO_PI = 1.0 / sqrt(2.0 * pi)
_MIN_VOLATILITY = 0.001
_MIN_BISECTION_VOLATILITY = 0.00001
_MAX_BISECTION_ITERATIONS = 10_000


class _NormalDistribution:
    @staticmethod
    def cdf(value: float) -> float:
        return 0.5 * (1.0 + erf(value / _SQRT_TWO))

    @staticmethod
    def pdf(value: float) -> float:
        return exp(-0.5 * value * value) * _INV_SQRT_TWO_PI


class _PricedModel(Protocol):
    @property
    def callPrice(self) -> float | None: ...

    @property
    def putPrice(self) -> float | None: ...


class _OptionModelFactory(Protocol):
    def __call__(
        self,
        args: OptionArgs,
        volatility: NumberLike | None = None,
        callPrice: NumberLike | None = None,
        putPrice: NumberLike | None = None,
        performance: bool | None = None,
    ) -> _PricedModel: ...


norm = _NormalDistribution()


def _arg(args: OptionArgs, index: int) -> float:
    return float(args[index])


def _discount(rate: float, years: float) -> float:
    return e ** (-rate * years)


def _safe_d1(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return numerator / denominator


def _decimal_places(value: float) -> int:
    text = str(value)
    if "e" in text.lower():
        return 8
    if "." not in text:
        return 0
    return len(text.split(".", 1)[1])


def _coerce_price(price: NumberLike | None) -> float | None:
    if price is None:
        return None
    return float(price)


def _require_price(price: float | None, name: str) -> float:
    if price is None:
        raise ValueError(f"{name} is required")
    return price


def _price_for_side(model: _PricedModel, side: PriceSide) -> float:
    if side == "call":
        return _require_price(model.callPrice, "callPrice")
    return _require_price(model.putPrice, "putPrice")


def _resolve_model(class_name: str) -> _OptionModelFactory:
    try:
        return _OPTION_MODELS[class_name]
    except KeyError as exc:
        supported = ", ".join(sorted(_OPTION_MODELS))
        raise ValueError(f"Unsupported option model {class_name!r}. Supported models: {supported}") from exc


def impliedVolatility(
    className: str,
    args: OptionArgs,
    callPrice: NumberLike | None = None,
    putPrice: NumberLike | None = None,
    high: float = 500.0,
    low: float = 0.0,
) -> float:
    """Return the estimated implied volatility using bisection search."""
    call_price = _coerce_price(callPrice)
    put_price = _coerce_price(putPrice)
    if call_price is None and put_price is None:
        raise ValueError("Either callPrice or putPrice is required")

    model_factory = _resolve_model(className)
    side: PriceSide = "call"
    target = _require_price(call_price if call_price is not None else put_price, "option price")
    underlying_price = _arg(args, 0)
    strike_price = _arg(args, 1)

    if call_price is not None:
        high_estimate = _price_for_side(model_factory(args, volatility=high, performance=True), "call")
        if high_estimate < call_price:
            return high
        if underlying_price > strike_price + call_price:
            return _MIN_VOLATILITY

    if put_price is not None:
        side = "put"
        target = put_price
        high_estimate = _price_for_side(model_factory(args, volatility=high, performance=True), "put")
        if high_estimate < put_price:
            return high
        if strike_price > underlying_price + put_price:
            return _MIN_VOLATILITY

    decimals = _decimal_places(target)
    mid = high
    for _ in range(_MAX_BISECTION_ITERATIONS):
        mid = (high + low) / 2.0
        if mid < _MIN_BISECTION_VOLATILITY:
            mid = _MIN_BISECTION_VOLATILITY

        estimate = _price_for_side(model_factory(args, volatility=mid, performance=True), side)
        if round(estimate, decimals) == target:
            break
        if estimate > target:
            high = mid
        elif estimate < target:
            low = mid

    return mid


class _OptionBase:
    underlyingPrice: float
    strikePrice: float
    daysToExpiration: float
    volatility: float
    _a_: float
    _d1_: float
    _d2_: float
    callPrice: float | None
    putPrice: float | None
    callDelta: float | None
    putDelta: float | None
    callDelta2: float | None
    putDelta2: float | None
    callTheta: float | None
    putTheta: float | None
    vega: float | None
    gamma: float | None
    impliedVolatility: float | None
    putCallParity: float | None
    exerciceProbability: float | None

    def _init_common_state(self) -> None:
        self.volatility = 0.0
        self._a_ = 0.0
        self._d1_ = 0.0
        self._d2_ = 0.0
        self.callPrice = None
        self.putPrice = None
        self.callDelta = None
        self.putDelta = None
        self.callDelta2 = None
        self.putDelta2 = None
        self.callTheta = None
        self.putTheta = None
        self.vega = None
        self.gamma = None
        self.impliedVolatility = None
        self.putCallParity = None
        self.exerciceProbability = None

    def _has_degenerate_vol_or_time(self) -> bool:
        return self.volatility == 0 or self.daysToExpiration == 0

    def _check_strike(self) -> None:
        if self.strikePrice == 0:
            raise ZeroDivisionError("The strike price cannot be zero")

    def _intrinsic_price(self) -> PricePair:
        call = max(0.0, self.underlyingPrice - self.strikePrice)
        put = max(0.0, self.strikePrice - self.underlyingPrice)
        return [call, put]

    def _intrinsic_delta(self) -> PricePair:
        call = 1.0 if self.underlyingPrice > self.strikePrice else 0.0
        put = -1.0 if self.underlyingPrice < self.strikePrice else 0.0
        return [call, put]

    def _intrinsic_dual_delta(self) -> PricePair:
        call = -1.0 if self.underlyingPrice > self.strikePrice else 0.0
        put = 1.0 if self.underlyingPrice < self.strikePrice else 0.0
        return [call, put]

    def _zero_pair(self) -> PricePair:
        return [0.0, 0.0]


class GK(_OptionBase):
    """Garman-Kohlhagen pricing for European options on currencies."""

    domesticRate: float
    foreignRate: float
    callRhoD: float | None
    putRhoD: float | None
    callRhoF: float | None
    putRhoF: float | None

    def __init__(
        self,
        args: OptionArgs,
        volatility: NumberLike | None = None,
        callPrice: NumberLike | None = None,
        putPrice: NumberLike | None = None,
        performance: bool | None = None,
    ) -> None:
        self._init_common_state()
        self.callRhoD = None
        self.putRhoD = None
        self.callRhoF = None
        self.putRhoF = None

        self.underlyingPrice = _arg(args, 0)
        self.strikePrice = _arg(args, 1)
        self.domesticRate = _arg(args, 2) / 100.0
        self.foreignRate = _arg(args, 3) / 100.0
        self.daysToExpiration = _arg(args, 4) / 365.0

        if volatility is not None:
            self.volatility = float(volatility) / 100.0
            self._a_ = self.volatility * sqrt(self.daysToExpiration)
            numerator = log(self.underlyingPrice / self.strikePrice) + (
                self.domesticRate - self.foreignRate + (self.volatility**2) / 2.0
            ) * self.daysToExpiration
            self._d1_ = _safe_d1(numerator, self._a_)
            self._d2_ = self._d1_ - self._a_

            self.callPrice, self.putPrice = self._price()
            if not performance:
                self.callDelta, self.putDelta = self._delta()
                self.callDelta2, self.putDelta2 = self._delta2()
                self.callTheta, self.putTheta = self._theta()
                self.callRhoD, self.putRhoD = self._rhod()
                self.callRhoF, self.putRhoF = self._rhof()
                self.vega = self._vega()
                self.gamma = self._gamma()
                self.exerciceProbability = norm.cdf(self._d2_)

        if callPrice is not None:
            self.callPrice = round(float(callPrice), 6)
            self.impliedVolatility = impliedVolatility(self.__class__.__name__, args, callPrice=self.callPrice)
        if putPrice is not None and callPrice is None:
            self.putPrice = round(float(putPrice), 6)
            self.impliedVolatility = impliedVolatility(self.__class__.__name__, args, putPrice=self.putPrice)
        if callPrice is not None and putPrice is not None:
            self.callPrice = float(callPrice)
            self.putPrice = float(putPrice)
            self.putCallParity = self._parity()

    def _price(self) -> PricePair:
        """Returns the option price: [Call price, Put price]."""
        self._check_strike()
        if self._has_degenerate_vol_or_time():
            return self._intrinsic_price()

        foreign_discount = _discount(self.foreignRate, self.daysToExpiration)
        domestic_discount = _discount(self.domesticRate, self.daysToExpiration)
        call = (
            foreign_discount * self.underlyingPrice * norm.cdf(self._d1_)
            - domestic_discount * self.strikePrice * norm.cdf(self._d2_)
        )
        put = (
            domestic_discount * self.strikePrice * norm.cdf(-self._d2_)
            - foreign_discount * self.underlyingPrice * norm.cdf(-self._d1_)
        )
        return [call, put]

    def _delta(self) -> PricePair:
        """Returns the option delta: [Call delta, Put delta]."""
        self._check_strike()
        if self._has_degenerate_vol_or_time():
            return self._intrinsic_delta()

        foreign_discount = _discount(self.foreignRate, self.daysToExpiration)
        call = norm.cdf(self._d1_) * foreign_discount
        put = -norm.cdf(-self._d1_) * foreign_discount
        return [call, put]

    def _delta2(self) -> PricePair:
        """Returns the dual delta: [Call dual delta, Put dual delta]."""
        self._check_strike()
        if self._has_degenerate_vol_or_time():
            return self._intrinsic_dual_delta()

        domestic_discount = _discount(self.domesticRate, self.daysToExpiration)
        call = -norm.cdf(self._d2_) * domestic_discount
        put = norm.cdf(-self._d2_) * domestic_discount
        return [call, put]

    def _vega(self) -> float:
        """Returns the option vega."""
        self._check_strike()
        if self._has_degenerate_vol_or_time():
            return 0.0

        return (
            self.underlyingPrice
            * _discount(self.foreignRate, self.daysToExpiration)
            * norm.pdf(self._d1_)
            * sqrt(self.daysToExpiration)
        )

    def _theta(self) -> PricePair:
        """Returns the option theta: [Call theta, Put theta]."""
        self._check_strike()
        if self._has_degenerate_vol_or_time():
            return self._zero_pair()

        foreign_discount = _discount(self.foreignRate, self.daysToExpiration)
        domestic_discount = _discount(self.domesticRate, self.daysToExpiration)
        call = (
            -self.underlyingPrice * foreign_discount * norm.pdf(self._d1_) * self.volatility
            / (2.0 * sqrt(self.daysToExpiration))
            + self.foreignRate * self.underlyingPrice * foreign_discount * norm.cdf(self._d1_)
            - self.domesticRate * self.strikePrice * domestic_discount * norm.cdf(self._d2_)
        )
        put = (
            -self.underlyingPrice * foreign_discount * norm.pdf(self._d1_) * self.volatility
            / (2.0 * sqrt(self.daysToExpiration))
            - self.foreignRate * self.underlyingPrice * foreign_discount * norm.cdf(-self._d1_)
            + self.domesticRate * self.strikePrice * domestic_discount * norm.cdf(-self._d2_)
        )
        return [call / 365.0, put / 365.0]

    def _rhod(self) -> PricePair:
        """Returns the option domestic rho: [Call rho, Put rho]."""
        if self.daysToExpiration == 0:
            return self._zero_pair()

        domestic_discount = _discount(self.domesticRate, self.daysToExpiration)
        call = self.strikePrice * self.daysToExpiration * domestic_discount * norm.cdf(self._d2_) / 100.0
        put = -self.strikePrice * self.daysToExpiration * domestic_discount * norm.cdf(-self._d2_) / 100.0
        return [call, put]

    def _rhof(self) -> PricePair:
        """Returns the option foreign rho: [Call rho, Put rho]."""
        if self.daysToExpiration == 0:
            return self._zero_pair()

        foreign_discount = _discount(self.foreignRate, self.daysToExpiration)
        call = -self.underlyingPrice * self.daysToExpiration * foreign_discount * norm.cdf(self._d1_) / 100.0
        put = self.underlyingPrice * self.daysToExpiration * foreign_discount * norm.cdf(-self._d1_) / 100.0
        return [call, put]

    def _gamma(self) -> float:
        """Returns the option gamma."""
        self._check_strike()
        if self._has_degenerate_vol_or_time():
            return 0.0

        return norm.pdf(self._d1_) * _discount(self.foreignRate, self.daysToExpiration) / (
            self.underlyingPrice * self._a_
        )

    def _parity(self) -> float:
        """Returns the put-call parity."""
        call_price = _require_price(self.callPrice, "callPrice")
        put_price = _require_price(self.putPrice, "putPrice")
        return (
            call_price
            - put_price
            - (self.underlyingPrice / ((1.0 + self.foreignRate) ** self.daysToExpiration))
            + (self.strikePrice / ((1.0 + self.domesticRate) ** self.daysToExpiration))
        )


class BS(_OptionBase):
    """Black-Scholes pricing for European options on stocks without dividends."""

    interestRate: float
    callRho: float | None
    putRho: float | None

    def __init__(
        self,
        args: OptionArgs,
        volatility: NumberLike | None = None,
        callPrice: NumberLike | None = None,
        putPrice: NumberLike | None = None,
        performance: bool | None = None,
    ) -> None:
        self._init_common_state()
        self.callRho = None
        self.putRho = None

        self.underlyingPrice = _arg(args, 0)
        self.strikePrice = _arg(args, 1)
        self.interestRate = _arg(args, 2) / 100.0
        self.daysToExpiration = _arg(args, 3) / 365.0

        if volatility is not None:
            self.volatility = float(volatility) / 100.0
            self._a_ = self.volatility * sqrt(self.daysToExpiration)
            numerator = log(self.underlyingPrice / self.strikePrice) + (
                self.interestRate + (self.volatility**2) / 2.0
            ) * self.daysToExpiration
            self._d1_ = _safe_d1(numerator, self._a_)
            self._d2_ = self._d1_ - self._a_

            self.callPrice, self.putPrice = self._price()
            if not performance:
                self.callDelta, self.putDelta = self._delta()
                self.callDelta2, self.putDelta2 = self._delta2()
                self.callTheta, self.putTheta = self._theta()
                self.callRho, self.putRho = self._rho()
                self.vega = self._vega()
                self.gamma = self._gamma()
                self.exerciceProbability = norm.cdf(self._d2_)

        if callPrice is not None:
            self.callPrice = round(float(callPrice), 6)
            self.impliedVolatility = impliedVolatility(self.__class__.__name__, args, callPrice=self.callPrice)
        if putPrice is not None and callPrice is None:
            self.putPrice = round(float(putPrice), 6)
            self.impliedVolatility = impliedVolatility(self.__class__.__name__, args, putPrice=self.putPrice)
        if callPrice is not None and putPrice is not None:
            self.callPrice = float(callPrice)
            self.putPrice = float(putPrice)
            self.putCallParity = self._parity()

    def _price(self) -> PricePair:
        """Returns the option price: [Call price, Put price]."""
        self._check_strike()
        if self._has_degenerate_vol_or_time():
            return self._intrinsic_price()

        discount = _discount(self.interestRate, self.daysToExpiration)
        call = self.underlyingPrice * norm.cdf(self._d1_) - self.strikePrice * discount * norm.cdf(self._d2_)
        put = self.strikePrice * discount * norm.cdf(-self._d2_) - self.underlyingPrice * norm.cdf(-self._d1_)
        return [call, put]

    def _delta(self) -> PricePair:
        """Returns the option delta: [Call delta, Put delta]."""
        self._check_strike()
        if self._has_degenerate_vol_or_time():
            return self._intrinsic_delta()

        call = norm.cdf(self._d1_)
        put = -norm.cdf(-self._d1_)
        return [call, put]

    def _delta2(self) -> PricePair:
        """Returns the dual delta: [Call dual delta, Put dual delta]."""
        self._check_strike()
        if self._has_degenerate_vol_or_time():
            return self._intrinsic_dual_delta()

        discount = _discount(self.interestRate, self.daysToExpiration)
        call = -norm.cdf(self._d2_) * discount
        put = norm.cdf(-self._d2_) * discount
        return [call, put]

    def _vega(self) -> float:
        """Returns the option vega."""
        self._check_strike()
        if self._has_degenerate_vol_or_time():
            return 0.0

        return self.underlyingPrice * norm.pdf(self._d1_) * sqrt(self.daysToExpiration) / 100.0

    def _theta(self) -> PricePair:
        """Returns the option theta: [Call theta, Put theta]."""
        self._check_strike()
        if self._has_degenerate_vol_or_time():
            return self._zero_pair()

        discount = _discount(self.interestRate, self.daysToExpiration)
        common = -self.underlyingPrice * norm.pdf(self._d1_) * self.volatility / (2.0 * sqrt(self.daysToExpiration))
        call = common - self.interestRate * self.strikePrice * discount * norm.cdf(self._d2_)
        put = common + self.interestRate * self.strikePrice * discount * norm.cdf(-self._d2_)
        return [call / 365.0, put / 365.0]

    def _rho(self) -> PricePair:
        """Returns the option rho: [Call rho, Put rho]."""
        if self.daysToExpiration == 0:
            return self._zero_pair()

        discount = _discount(self.interestRate, self.daysToExpiration)
        call = self.strikePrice * self.daysToExpiration * discount * norm.cdf(self._d2_) / 100.0
        put = -self.strikePrice * self.daysToExpiration * discount * norm.cdf(-self._d2_) / 100.0
        return [call, put]

    def _gamma(self) -> float:
        """Returns the option gamma."""
        self._check_strike()
        if self._has_degenerate_vol_or_time():
            return 0.0

        return norm.pdf(self._d1_) / (self.underlyingPrice * self._a_)

    def _parity(self) -> float:
        """Put-call parity."""
        call_price = _require_price(self.callPrice, "callPrice")
        put_price = _require_price(self.putPrice, "putPrice")
        return (
            call_price
            - put_price
            - self.underlyingPrice
            + (self.strikePrice / ((1.0 + self.interestRate) ** self.daysToExpiration))
        )


class Me(_OptionBase):
    """Merton pricing for European options on stocks with dividends."""

    interestRate: float
    dividend: float
    dividendYield: float
    callRho: float | None
    putRho: float | None

    def __init__(
        self,
        args: OptionArgs,
        volatility: NumberLike | None = None,
        callPrice: NumberLike | None = None,
        putPrice: NumberLike | None = None,
        performance: bool | None = None,
    ) -> None:
        self._init_common_state()
        self.callRho = None
        self.putRho = None

        self.underlyingPrice = _arg(args, 0)
        self.strikePrice = _arg(args, 1)
        self.interestRate = _arg(args, 2) / 100.0
        self.dividend = _arg(args, 3)
        self.dividendYield = self.dividend / self.underlyingPrice
        self.daysToExpiration = _arg(args, 4) / 365.0

        if volatility is not None:
            self.volatility = float(volatility) / 100.0
            self._a_ = self.volatility * sqrt(self.daysToExpiration)
            numerator = log(self.underlyingPrice / self.strikePrice) + (
                self.interestRate - self.dividendYield + (self.volatility**2) / 2.0
            ) * self.daysToExpiration
            self._d1_ = _safe_d1(numerator, self._a_)
            self._d2_ = self._d1_ - self._a_

            self.callPrice, self.putPrice = self._price()
            if not performance:
                self.callDelta, self.putDelta = self._delta()
                self.callDelta2, self.putDelta2 = self._delta2()
                self.callTheta, self.putTheta = self._theta()
                self.callRho, self.putRho = self._rho()
                self.vega = self._vega()
                self.gamma = self._gamma()
                self.exerciceProbability = norm.cdf(self._d2_)

        if callPrice is not None:
            self.callPrice = round(float(callPrice), 6)
            self.impliedVolatility = impliedVolatility(self.__class__.__name__, args, callPrice=self.callPrice)
        if putPrice is not None and callPrice is None:
            self.putPrice = round(float(putPrice), 6)
            self.impliedVolatility = impliedVolatility(self.__class__.__name__, args, putPrice=self.putPrice)
        if callPrice is not None and putPrice is not None:
            self.callPrice = float(callPrice)
            self.putPrice = float(putPrice)
            self.putCallParity = self._parity()

    def _price(self) -> PricePair:
        """Returns the option price: [Call price, Put price]."""
        self._check_strike()
        if self._has_degenerate_vol_or_time():
            return self._intrinsic_price()

        dividend_discount = _discount(self.dividendYield, self.daysToExpiration)
        rate_discount = _discount(self.interestRate, self.daysToExpiration)
        call = (
            self.underlyingPrice * dividend_discount * norm.cdf(self._d1_)
            - self.strikePrice * rate_discount * norm.cdf(self._d2_)
        )
        put = (
            self.strikePrice * rate_discount * norm.cdf(-self._d2_)
            - self.underlyingPrice * dividend_discount * norm.cdf(-self._d1_)
        )
        return [call, put]

    def _delta(self) -> PricePair:
        """Returns the option delta: [Call delta, Put delta]."""
        self._check_strike()
        if self._has_degenerate_vol_or_time():
            return self._intrinsic_delta()

        dividend_discount = _discount(self.dividendYield, self.daysToExpiration)
        call = dividend_discount * norm.cdf(self._d1_)
        put = dividend_discount * (norm.cdf(self._d1_) - 1.0)
        return [call, put]

    def _delta2(self) -> PricePair:
        """Returns the dual delta: [Call dual delta, Put dual delta]."""
        self._check_strike()
        if self._has_degenerate_vol_or_time():
            return self._intrinsic_dual_delta()

        rate_discount = _discount(self.interestRate, self.daysToExpiration)
        call = -norm.cdf(self._d2_) * rate_discount
        put = norm.cdf(-self._d2_) * rate_discount
        return [call, put]

    def _vega(self) -> float:
        """Returns the option vega."""
        self._check_strike()
        if self._has_degenerate_vol_or_time():
            return 0.0

        return (
            self.underlyingPrice
            * _discount(self.dividendYield, self.daysToExpiration)
            * norm.pdf(self._d1_)
            * sqrt(self.daysToExpiration)
            / 100.0
        )

    def _theta(self) -> PricePair:
        """Returns the option theta: [Call theta, Put theta]."""
        self._check_strike()
        if self._has_degenerate_vol_or_time():
            return self._zero_pair()

        rate_discount = _discount(self.interestRate, self.daysToExpiration)
        dividend_discount = _discount(self.dividendYield, self.daysToExpiration)
        call = (
            -self.underlyingPrice * dividend_discount * norm.pdf(self._d1_) * self.volatility
            / (2.0 * sqrt(self.daysToExpiration))
            + self.dividendYield * self.underlyingPrice * dividend_discount * norm.cdf(self._d1_)
            - self.interestRate * self.strikePrice * rate_discount * norm.cdf(self._d2_)
        )
        put = (
            -self.underlyingPrice * dividend_discount * norm.pdf(self._d1_) * self.volatility
            / (2.0 * sqrt(self.daysToExpiration))
            - self.dividendYield * self.underlyingPrice * dividend_discount * norm.cdf(-self._d1_)
            + self.interestRate * self.strikePrice * rate_discount * norm.cdf(-self._d2_)
        )
        return [call / 365.0, put / 365.0]

    def _rho(self) -> PricePair:
        """Returns the option rho: [Call rho, Put rho]."""
        if self.daysToExpiration == 0:
            return self._zero_pair()

        rate_discount = _discount(self.interestRate, self.daysToExpiration)
        call = self.strikePrice * self.daysToExpiration * rate_discount * norm.cdf(self._d2_) / 100.0
        put = -self.strikePrice * self.daysToExpiration * rate_discount * norm.cdf(-self._d2_) / 100.0
        return [call, put]

    def _gamma(self) -> float:
        """Returns the option gamma."""
        self._check_strike()
        if self._has_degenerate_vol_or_time():
            return 0.0

        return _discount(self.dividendYield, self.daysToExpiration) * norm.pdf(self._d1_) / (
            self.underlyingPrice * self._a_
        )

    def _parity(self) -> float:
        """Put-call parity."""
        call_price = _require_price(self.callPrice, "callPrice")
        put_price = _require_price(self.putPrice, "putPrice")
        return (
            call_price
            - put_price
            - self.underlyingPrice
            + (self.strikePrice / ((1.0 + self.interestRate) ** self.daysToExpiration))
        )


_OPTION_MODELS: dict[str, _OptionModelFactory] = {
    "GK": GK,
    "BS": BS,
    "Me": Me,
}
