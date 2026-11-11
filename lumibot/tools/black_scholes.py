from math import e, log
import numpy as np

import warnings
warnings.filterwarnings("ignore", category=RuntimeWarning)

try:
    from scipy.stats import norm
except ImportError:
    print("Mibian requires scipy to work properly")

try:
    from scipy.special import ndtr
except ImportError:
    ndtr = None

# WARNING: All numbers should be floats -> x = 1.0

_SQRT_2PI = np.sqrt(2 * np.pi)


class _FastNorm:
    """``scipy.stats.norm.cdf`` and ``.pdf`` for one number, without the argument checks.

    For a scalar, scipy validates the arguments, broadcasts them to arrays and only then
    computes ``ndtr(x)`` (cdf) or ``exp(-x**2/2)/sqrt(2*pi)`` (pdf). This computes the same
    expression directly, so the bits are identical (infinities and NaN included); arrays and
    anything unusual still go through scipy. Pricing every strike of a chain made the checks
    the largest cost of a delta pick.
    """

    @staticmethod
    def cdf(x):
        if ndtr is not None and type(x) in (float, np.float64):
            return ndtr(x)
        return norm.cdf(x)

    @staticmethod
    def pdf(x):
        if type(x) in (float, np.float64) and np.isfinite(x):
            # scipy evaluates this on a one-element array; numpy's array exp can differ from
            # the scalar exp in the last bit, so do the same.
            a = np.array([x], dtype=np.float64)
            return (np.exp(-a ** 2 / 2.0) / _SQRT_2PI / 1.0)[0]
        return norm.pdf(x)


_norm = _FastNorm()


def _bs_call_put(args, volatility):
    """Black-Scholes [call, put] with the same arithmetic as ``BS(..., performance=True)``.

    Same operations in the same order, and ``ndtr`` is the function ``scipy.stats.norm.cdf``
    evaluates, so the prices are bit-identical to the class. It skips building an object and
    the ``norm.cdf`` argument checks, which made each bisection step about 40 times slower.
    """
    underlying = float(args[0])
    strike = float(args[1])
    rate = float(args[2]) / 100
    years = float(args[3]) / 365
    vol = float(volatility) / 100
    a = vol * years**0.5
    try:
        d1 = (log(underlying / strike) + (rate + (vol**2) / 2) * years) / a
    except ZeroDivisionError:
        d1 = 0
    d2 = d1 - a
    if strike == 0:
        raise ZeroDivisionError("The strike price cannot be zero")
    discount = e ** (-rate * years)
    call = underlying * ndtr(d1) - strike * discount * ndtr(d2)
    put = strike * discount * ndtr(-d2) - underlying * ndtr(-d1)
    return call, put


def _implied_volatility_bs(args, callPrice=None, putPrice=None, high=500.0, low=0.0):
    """The bisection below for the BS model, without an object per step.

    Returns exactly what the generic loop returns. Two differences in cost only: prices come
    from ``_bs_call_put``, and the loop stops once a step leaves ``mid`` unchanged. From that
    point ``high`` and ``low`` can no longer move, so every later step of the original
    10,000 would produce the same ``mid``.
    """
    if callPrice:
        target = callPrice
        if _bs_call_put(args, high)[0] < target:
            return high
        if args[0] > args[1] + callPrice:
            return 0.001
    if putPrice:
        target = putPrice
        if _bs_call_put(args, high)[1] < target:
            return high
        if args[1] > args[0] + putPrice:
            return 0.001
    decimals = len(str(target).split(".")[1])  # Count decimals
    previous = None
    for i in range(10000):  # To avoid infinite loops
        mid = (high + low) / 2
        if mid < 0.00001:
            mid = 0.00001
        if mid == previous:
            break
        previous = mid
        call, put = _bs_call_put(args, mid)
        if callPrice:
            estimate = call
        if putPrice:
            estimate = put
        if np.round(estimate, decimals) == target:
            break
        elif estimate > target:
            high = mid
        elif estimate < target:
            low = mid
    return mid


def implied_volatility_bs_many(underlying, strike, rate_pct, days, price, is_call, high=500.0, low=0.0):
    """``_implied_volatility_bs`` for many options at once, with bit-identical results.

    Each element runs the same bisection as the scalar solver: the same early exits, the same
    arithmetic in the same order (``log``, ``e ** x``, ``x ** 0.5`` and ``ndtr`` evaluate the
    same way on arrays here), the same rounding test and the same "mid stopped moving" stop.
    Elements finish independently. A price whose text has no decimal point (for example
    ``1e-05``) makes the scalar solver raise; those come back as NaN so the caller can use the
    scalar path and its error. Returns a float numpy array.
    """
    und = np.asarray(underlying, dtype=float)
    k = np.asarray(strike, dtype=float)
    rate = np.asarray(rate_pct, dtype=float) / 100
    years = np.asarray(days, dtype=float) / 365
    target = np.asarray(price, dtype=float)
    calls = np.asarray(is_call, dtype=bool)
    n = len(und)
    out = np.full(n, np.nan)
    hi = np.full(n, float(high))
    lo = np.full(n, float(low))
    sqrt_years = years**0.5
    with np.errstate(divide="ignore", invalid="ignore"):
        log_s_k = np.log(und / k)
    discount = np.power(e, -rate * years)

    def priced(volatility_pct):
        vol = volatility_pct / 100
        a = vol * sqrt_years
        with np.errstate(divide="ignore", invalid="ignore"):
            d1 = (log_s_k + (rate + (vol**2) / 2) * years) / a
        d1 = np.where(a == 0, 0.0, d1)
        d2 = d1 - a
        call = und * ndtr(d1) - k * discount * ndtr(d2)
        put = k * discount * ndtr(-d2) - und * ndtr(-d1)
        return np.where(calls, call, put)

    texts = [str(float(t)) for t in target]
    unparsable = np.array(["." not in t for t in texts], dtype=bool)
    decimals = np.array([len(t.split(".")[1]) if "." in t else 0 for t in texts])
    done = unparsable.copy()
    capped = ~done & (priced(hi) < target)
    out[capped] = high
    done |= capped
    deep = ~done & np.where(calls, und > k + target, k > und + target)
    out[deep] = 0.001
    done |= deep
    active = ~done
    groups = np.unique(decimals[active]) if active.any() else []
    previous = np.full(n, np.nan)
    mid = np.full(n, np.nan)
    for _ in range(10000):  # same cap as the scalar loop
        if not active.any():
            break
        m = (hi + lo) / 2
        m = np.where(m < 0.00001, 0.00001, m)
        stopped = active & (m == previous)
        active &= ~stopped
        previous = np.where(active, m, previous)
        mid = np.where(active, m, mid)
        estimate = priced(np.where(active, m, 1.0))
        rounded = np.empty(n)
        for places in groups:
            chosen = decimals == places
            rounded[chosen] = np.round(estimate[chosen], places)
        hit = active & (rounded == target)
        mid = np.where(hit, m, mid)
        active &= ~hit
        hi = np.where(active & (estimate > target), m, hi)
        lo = np.where(active & (estimate < target), m, lo)
    finished = ~unparsable & ~capped & ~deep
    out[finished] = mid[finished]
    return out


def impliedVolatility(className, args, callPrice=None, putPrice=None, high=500.0, low=0.0):
    """Returns the estimated implied volatility"""
    if className == "BS" and ndtr is not None:
        return _implied_volatility_bs(args, callPrice=callPrice, putPrice=putPrice, high=high, low=low)
    if callPrice:
        target = callPrice
        restimate = eval(className)(args, volatility=high, performance=True).callPrice
        if restimate < target:
            return high
        if args[0] > args[1] + callPrice:
            return 0.001
    if putPrice:
        target = putPrice
        restimate = eval(className)(args, volatility=high, performance=True).putPrice
        if restimate < target:
            return high
        if args[1] > args[0] + putPrice:
            return 0.001
    decimals = len(str(target).split(".")[1])  # Count decimals
    for i in range(10000):  # To avoid infinite loops
        mid = (high + low) / 2
        if mid < 0.00001:
            mid = 0.00001
        if callPrice:
            estimate = eval(className)(args, volatility=mid, performance=True).callPrice
        if putPrice:
            estimate = eval(className)(args, volatility=mid, performance=True).putPrice
        if np.round(estimate, decimals) == target:
            break
        elif estimate > target:
            high = mid
        elif estimate < target:
            low = mid
    return mid


class GK:
    """Garman-Kohlhagen
	Used for pricing European options on currencies
	
	GK([underlyingPrice, strikePrice, domesticRate, foreignRate, \
			daysToExpiration], volatility=x, callPrice=y, putPrice=z)

	eg: 
		c = mibian.GK([1.4565, 1.45, 1, 2, 30], volatility=20)
		c.callPrice				# Returns the call price
		c.putPrice				# Returns the put price
		c.callDelta				# Returns the call delta
		c.putDelta				# Returns the put delta
		c.callDelta2			# Returns the call dual delta
		c.putDelta2				# Returns the put dual delta
		c.callTheta				# Returns the call theta
		c.putTheta				# Returns the put theta
		c.callRhoD				# Returns the call domestic rho
		c.putRhoD				# Returns the put domestic rho
		c.callRhoF				# Returns the call foreign rho
		c.putRhoF				# Returns the call foreign rho
		c.vega					# Returns the option vega
		c.gamma					# Returns the option gamma

		c = mibian.GK([1.4565, 1.45, 1, 2, 30], callPrice=0.0359)
		c.impliedVolatility		# Returns the implied volatility from the call price
		
		c = mibian.GK([1.4565, 1.45, 1, 2, 30], putPrice=0.03)
		c.impliedVolatility		# Returns the implied volatility from the put price
		
		c = mibian.GK([1.4565, 1.45, 1, 2, 30], callPrice=0.0359, putPrice=0.03)
		c.putCallParity			# Returns the put-call parity
	"""

    def __init__(self, args, volatility=None, callPrice=None, putPrice=None, performance=None):
        self.underlyingPrice = float(args[0])
        self.strikePrice = float(args[1])
        self.domesticRate = float(args[2]) / 100
        self.foreignRate = float(args[3]) / 100
        self.daysToExpiration = float(args[4]) / 365

        for i in [
            "callPrice",
            "putPrice",
            "callDelta",
            "putDelta",
            "callDelta2",
            "putDelta2",
            "callTheta",
            "putTheta",
            "callRhoD",
            "putRhoD",
            "callRhoF",
            "callRhoF",
            "vega",
            "gamma",
            "impliedVolatility",
            "putCallParity",
        ]:
            self.__dict__[i] = None

        if volatility:
            self.volatility = float(volatility) / 100

            self._a_ = self.volatility * self.daysToExpiration**0.5
            self._d1_ = (
                log(self.underlyingPrice / self.strikePrice)
                + (self.domesticRate - self.foreignRate + (self.volatility**2) / 2) * self.daysToExpiration
            ) / self._a_
            self._d2_ = self._d1_ - self._a_
            # Reduces performance overhead when computing implied volatility
            if performance:
                [self.callPrice, self.putPrice] = self._price()
            else:
                [self.callPrice, self.putPrice] = self._price()
                [self.callDelta, self.putDelta] = self._delta()
                [self.callDelta2, self.putDelta2] = self._delta2()
                [self.callTheta, self.putTheta] = self._theta()
                [self.callRhoD, self.putRhoD] = self._rhod()
                [self.callRhoF, self.putRhoF] = self._rhof()
                self.vega = self._vega()
                self.gamma = self._gamma()
                self.exerciceProbability = _norm.cdf(self._d2_)
        if callPrice:
            self.callPrice = round(float(callPrice), 6)
            self.impliedVolatility = impliedVolatility(self.__class__.__name__, args, callPrice=self.callPrice)
        if putPrice and not callPrice:
            self.putPrice = round(float(putPrice), 6)
            self.impliedVolatility = impliedVolatility(self.__class__.__name__, args, putPrice=self.putPrice)
        if callPrice and putPrice:
            self.callPrice = float(callPrice)
            self.putPrice = float(putPrice)
            self.putCallParity = self._parity()

    def _price(self):
        """Returns the option price: [Call price, Put price]"""
        if self.volatility == 0 or self.daysToExpiration == 0:
            call = max(0.0, self.underlyingPrice - self.strikePrice)
            put = max(0.0, self.strikePrice - self.underlyingPrice)
        if self.strikePrice == 0:
            raise ZeroDivisionError("The strike price cannot be zero")
        else:
            call = e ** (-self.foreignRate * self.daysToExpiration) * self.underlyingPrice * _norm.cdf(
                self._d1_
            ) - e ** (-self.domesticRate * self.daysToExpiration) * self.strikePrice * _norm.cdf(self._d2_)
            put = e ** (-self.domesticRate * self.daysToExpiration) * self.strikePrice * _norm.cdf(-self._d2_) - e ** (
                -self.foreignRate * self.daysToExpiration
            ) * self.underlyingPrice * _norm.cdf(-self._d1_)
        return [call, put]

    def _delta(self):
        """Returns the option delta: [Call delta, Put delta]"""
        if self.volatility == 0 or self.daysToExpiration == 0:
            call = 1.0 if self.underlyingPrice > self.strikePrice else 0.0
            put = -1.0 if self.underlyingPrice < self.strikePrice else 0.0
        if self.strikePrice == 0:
            raise ZeroDivisionError("The strike price cannot be zero")
        else:
            _b_ = e ** -(self.foreignRate * self.daysToExpiration)
            call = _norm.cdf(self._d1_) * _b_
            put = -_norm.cdf(-self._d1_) * _b_
        return [call, put]

    def _delta2(self):
        """Returns the dual delta: [Call dual delta, Put dual delta]"""
        if self.volatility == 0 or self.daysToExpiration == 0:
            call = -1.0 if self.underlyingPrice > self.strikePrice else 0.0
            put = 1.0 if self.underlyingPrice < self.strikePrice else 0.0
        if self.strikePrice == 0:
            raise ZeroDivisionError("The strike price cannot be zero")
        else:
            _b_ = e ** -(self.domesticRate * self.daysToExpiration)
            call = -_norm.cdf(self._d2_) * _b_
            put = _norm.cdf(-self._d2_) * _b_
        return [call, put]

    def _vega(self):
        """Returns the option vega"""
        if self.volatility == 0 or self.daysToExpiration == 0:
            return 0.0
        if self.strikePrice == 0:
            raise ZeroDivisionError("The strike price cannot be zero")
        else:
            return (
                self.underlyingPrice
                * e ** -(self.foreignRate * self.daysToExpiration)
                * _norm.pdf(self._d1_)
                * self.daysToExpiration**0.5
            )

    def _theta(self):
        """Returns the option theta: [Call theta, Put theta]"""
        _b_ = e ** -(self.foreignRate * self.daysToExpiration)
        call = (
            -self.underlyingPrice * _b_ * _norm.pdf(self._d1_) * self.volatility / (2 * self.daysToExpiration**0.5)
            + self.foreignRate * self.underlyingPrice * _b_ * _norm.cdf(self._d1_)
            - self.domesticRate * self.strikePrice * _b_ * _norm.cdf(self._d2_)
        )
        put = (
            -self.underlyingPrice * _b_ * _norm.pdf(self._d1_) * self.volatility / (2 * self.daysToExpiration**0.5)
            - self.foreignRate * self.underlyingPrice * _b_ * _norm.cdf(-self._d1_)
            + self.domesticRate * self.strikePrice * _b_ * _norm.cdf(-self._d2_)
        )
        return [call / 365, put / 365]

    def _rhod(self):
        """Returns the option domestic rho: [Call rho, Put rho]"""
        call = (
            self.strikePrice
            * self.daysToExpiration
            * e ** (-self.domesticRate * self.daysToExpiration)
            * _norm.cdf(self._d2_)
            / 100
        )
        put = (
            -self.strikePrice
            * self.daysToExpiration
            * e ** (-self.domesticRate * self.daysToExpiration)
            * _norm.cdf(-self._d2_)
            / 100
        )
        return [call, put]

    def _rhof(self):
        """Returns the option foreign rho: [Call rho, Put rho]"""
        call = (
            -self.underlyingPrice
            * self.daysToExpiration
            * e ** (-self.foreignRate * self.daysToExpiration)
            * _norm.cdf(self._d1_)
            / 100
        )
        put = (
            self.underlyingPrice
            * self.daysToExpiration
            * e ** (-self.foreignRate * self.daysToExpiration)
            * _norm.cdf(-self._d1_)
            / 100
        )
        return [call, put]

    def _gamma(self):
        """Returns the option gamma"""
        return (_norm.pdf(self._d1_) * e ** -(self.foreignRate * self.daysToExpiration)) / (
            self.underlyingPrice * self._a_
        )

    def _parity(self):
        """Returns the put-call parity"""
        return (
            self.callPrice
            - self.putPrice
            - (self.underlyingPrice / ((1 + self.foreignRate) ** self.daysToExpiration))
            + (self.strikePrice / ((1 + self.domesticRate) ** self.daysToExpiration))
        )


class BS:
    """Black-Scholes
	Used for pricing European options on stocks without dividends

	BS([underlyingPrice, strikePrice, interestRate, daysToExpiration], \
			volatility=x, callPrice=y, putPrice=z)

	eg: 
		c = mibian.BS([1.4565, 1.45, 1, 30], volatility=20)
		c.callPrice				# Returns the call price
		c.putPrice				# Returns the put price
		c.callDelta				# Returns the call delta
		c.putDelta				# Returns the put delta
		c.callDelta2			# Returns the call dual delta
		c.putDelta2				# Returns the put dual delta
		c.callTheta				# Returns the call theta
		c.putTheta				# Returns the put theta
		c.callRho				# Returns the call rho
		c.putRho				# Returns the put rho
		c.vega					# Returns the option vega
		c.gamma					# Returns the option gamma

		c = mibian.BS([1.4565, 1.45, 1, 30], callPrice=0.0359)
		c.impliedVolatility		# Returns the implied volatility from the call price
		
		c = mibian.BS([1.4565, 1.45, 1, 30], putPrice=0.0306)
		c.impliedVolatility		# Returns the implied volatility from the put price
		
		c = mibian.BS([1.4565, 1.45, 1, 30], callPrice=0.0359, putPrice=0.0306)
		c.putCallParity			# Returns the put-call parity
	"""

    def __init__(self, args, volatility=None, callPrice=None, putPrice=None, performance=None):
        self.underlyingPrice = float(args[0])
        self.strikePrice = float(args[1])
        self.interestRate = float(args[2]) / 100
        self.daysToExpiration = float(args[3]) / 365

        for i in [
            "callPrice",
            "putPrice",
            "callDelta",
            "putDelta",
            "callDelta2",
            "putDelta2",
            "callTheta",
            "putTheta",
            "callRho",
            "putRho",
            "vega",
            "gamma",
            "impliedVolatility",
            "putCallParity",
        ]:
            self.__dict__[i] = None

        if volatility:
            self.volatility = float(volatility) / 100

            self._a_ = self.volatility * self.daysToExpiration**0.5
            try:
                self._d1_ = (
                    log(self.underlyingPrice / self.strikePrice)
                    + (self.interestRate + (self.volatility**2) / 2) * self.daysToExpiration
                ) / self._a_
            except ZeroDivisionError:
                # TODO: This happens when daysToExpiration is zero, how should we deal with this?
                self._d1_ = 0

            self._d2_ = self._d1_ - self._a_
            if performance:
                [self.callPrice, self.putPrice] = self._price()
            else:
                [self.callPrice, self.putPrice] = self._price()
                [self.callDelta, self.putDelta] = self._delta()
                [self.callDelta2, self.putDelta2] = self._delta2()
                [self.callTheta, self.putTheta] = self._theta()
                [self.callRho, self.putRho] = self._rho()
                self.vega = self._vega()
                self.gamma = self._gamma()
                self.exerciceProbability = _norm.cdf(self._d2_)
        if callPrice:
            self.callPrice = round(float(callPrice), 6)
            self.impliedVolatility = impliedVolatility(self.__class__.__name__, args, callPrice=self.callPrice)
        if putPrice and not callPrice:
            self.putPrice = round(float(putPrice), 6)
            self.impliedVolatility = impliedVolatility(self.__class__.__name__, args, putPrice=self.putPrice)
        if callPrice and putPrice:
            self.callPrice = float(callPrice)
            self.putPrice = float(putPrice)
            self.putCallParity = self._parity()

    def _price(self):
        """Returns the option price: [Call price, Put price]"""
        if self.volatility == 0 or self.daysToExpiration == 0:
            call = max(0.0, self.underlyingPrice - self.strikePrice)
            put = max(0.0, self.strikePrice - self.underlyingPrice)
        if self.strikePrice == 0:
            raise ZeroDivisionError("The strike price cannot be zero")
        else:
            call = self.underlyingPrice * _norm.cdf(self._d1_) - self.strikePrice * e ** (
                -self.interestRate * self.daysToExpiration
            ) * _norm.cdf(self._d2_)
            put = self.strikePrice * e ** (-self.interestRate * self.daysToExpiration) * _norm.cdf(
                -self._d2_
            ) - self.underlyingPrice * _norm.cdf(-self._d1_)
        return [call, put]

    def _delta(self):
        """Returns the option delta: [Call delta, Put delta]"""
        if self.volatility == 0 or self.daysToExpiration == 0:
            call = 1.0 if self.underlyingPrice > self.strikePrice else 0.0
            put = -1.0 if self.underlyingPrice < self.strikePrice else 0.0
        if self.strikePrice == 0:
            raise ZeroDivisionError("The strike price cannot be zero")
        else:
            call = _norm.cdf(self._d1_)
            put = -_norm.cdf(-self._d1_)
        return [call, put]

    def _delta2(self):
        """Returns the dual delta: [Call dual delta, Put dual delta]"""
        if self.volatility == 0 or self.daysToExpiration == 0:
            call = -1.0 if self.underlyingPrice > self.strikePrice else 0.0
            put = 1.0 if self.underlyingPrice < self.strikePrice else 0.0
        if self.strikePrice == 0:
            raise ZeroDivisionError("The strike price cannot be zero")
        else:
            _b_ = e ** -(self.interestRate * self.daysToExpiration)
            call = -_norm.cdf(self._d2_) * _b_
            put = _norm.cdf(-self._d2_) * _b_
        return [call, put]

    def _vega(self):
        """Returns the option vega"""
        if self.volatility == 0 or self.daysToExpiration == 0:
            return 0.0
        if self.strikePrice == 0:
            raise ZeroDivisionError("The strike price cannot be zero")
        else:
            return self.underlyingPrice * _norm.pdf(self._d1_) * self.daysToExpiration**0.5 / 100

    def _theta(self):
        """Returns the option theta: [Call theta, Put theta]"""
        _b_ = e ** -(self.interestRate * self.daysToExpiration)
        call = -self.underlyingPrice * _norm.pdf(self._d1_) * self.volatility / (
            2 * self.daysToExpiration**0.5
        ) - self.interestRate * self.strikePrice * _b_ * _norm.cdf(self._d2_)
        put = -self.underlyingPrice * _norm.pdf(self._d1_) * self.volatility / (
            2 * self.daysToExpiration**0.5
        ) + self.interestRate * self.strikePrice * _b_ * _norm.cdf(-self._d2_)
        return [call / 365, put / 365]

    def _rho(self):
        """Returns the option rho: [Call rho, Put rho]"""
        _b_ = e ** -(self.interestRate * self.daysToExpiration)
        call = self.strikePrice * self.daysToExpiration * _b_ * _norm.cdf(self._d2_) / 100
        put = -self.strikePrice * self.daysToExpiration * _b_ * _norm.cdf(-self._d2_) / 100
        return [call, put]

    def _gamma(self):
        """Returns the option gamma"""
        return _norm.pdf(self._d1_) / (self.underlyingPrice * self._a_)

    def _parity(self):
        """Put-Call Parity"""
        return (
            self.callPrice
            - self.putPrice
            - self.underlyingPrice
            + (self.strikePrice / ((1 + self.interestRate) ** self.daysToExpiration))
        )


class Me:
    """Merton
	Used for pricing European options on stocks with dividends

	Me([underlyingPrice, strikePrice, interestRate, annualDividends, \
			daysToExpiration], volatility=x, callPrice=y, putPrice=z)

	eg: 
		c = mibian.Me([52, 50, 1, 1, 30], volatility=20)
		c.callPrice				# Returns the call price
		c.putPrice				# Returns the put price
		c.callDelta				# Returns the call delta
		c.putDelta				# Returns the put delta
		c.callDelta2			# Returns the call dual delta
		c.putDelta2				# Returns the put dual delta
		c.callTheta				# Returns the call theta
		c.putTheta				# Returns the put theta
		c.callRho				# Returns the call rho
		c.putRho				# Returns the put rho
		c.vega					# Returns the option vega
		c.gamma					# Returns the option gamma

		c = mibian.Me([52, 50, 1, 1, 30], callPrice=0.0359)
		c.impliedVolatility		# Returns the implied volatility from the call price
		
		c = mibian.Me([52, 50, 1, 1, 30], putPrice=0.0306)
		c.impliedVolatility		# Returns the implied volatility from the put price
		
		c = mibian.Me([52, 50, 1, 1, 30], callPrice=0.0359, putPrice=0.0306)
		c.putCallParity			# Returns the put-call parity
	"""

    def __init__(self, args, volatility=None, callPrice=None, putPrice=None, performance=None):
        self.underlyingPrice = float(args[0])
        self.strikePrice = float(args[1])
        self.interestRate = float(args[2]) / 100
        self.dividend = float(args[3])
        self.dividendYield = self.dividend / self.underlyingPrice
        self.daysToExpiration = float(args[4]) / 365

        for i in [
            "callPrice",
            "putPrice",
            "callDelta",
            "putDelta",
            "callDelta2",
            "putDelta2",
            "callTheta",
            "putTheta",
            "callRho",
            "putRho",
            "vega",
            "gamma",
            "impliedVolatility",
            "putCallParity",
        ]:
            self.__dict__[i] = None

        if volatility:
            self.volatility = float(volatility) / 100

            self._a_ = self.volatility * self.daysToExpiration**0.5
            self._d1_ = (
                log(self.underlyingPrice / self.strikePrice)
                + (self.interestRate - self.dividendYield + (self.volatility**2) / 2) * self.daysToExpiration
            ) / self._a_
            self._d2_ = self._d1_ - self._a_
            if performance:
                [self.callPrice, self.putPrice] = self._price()
            else:
                [self.callPrice, self.putPrice] = self._price()
                [self.callDelta, self.putDelta] = self._delta()
                [self.callDelta2, self.putDelta2] = self._delta2()
                [self.callTheta, self.putTheta] = self._theta()
                [self.callRho, self.putRho] = self._rho()
                self.vega = self._vega()
                self.gamma = self._gamma()
                self.exerciceProbability = _norm.cdf(self._d2_)
        if callPrice:
            self.callPrice = round(float(callPrice), 6)
            self.impliedVolatility = impliedVolatility(self.__class__.__name__, args, self.callPrice)
        if putPrice and not callPrice:
            self.putPrice = round(float(putPrice), 6)
            self.impliedVolatility = impliedVolatility(self.__class__.__name__, args, putPrice=self.putPrice)
        if callPrice and putPrice:
            self.callPrice = float(callPrice)
            self.putPrice = float(putPrice)
            self.putCallParity = self._parity()

    def _price(self):
        """Returns the option price: [Call price, Put price]"""
        if self.volatility == 0 or self.daysToExpiration == 0:
            call = max(0.0, self.underlyingPrice - self.strikePrice)
            put = max(0.0, self.strikePrice - self.underlyingPrice)
        if self.strikePrice == 0:
            raise ZeroDivisionError("The strike price cannot be zero")
        else:
            call = self.underlyingPrice * e ** (-self.dividendYield * self.daysToExpiration) * _norm.cdf(
                self._d1_
            ) - self.strikePrice * e ** (-self.interestRate * self.daysToExpiration) * _norm.cdf(self._d2_)
            put = self.strikePrice * e ** (-self.interestRate * self.daysToExpiration) * _norm.cdf(
                -self._d2_
            ) - self.underlyingPrice * e ** (-self.dividendYield * self.daysToExpiration) * _norm.cdf(-self._d1_)
        return [call, put]

    def _delta(self):
        """Returns the option delta: [Call delta, Put delta]"""
        if self.volatility == 0 or self.daysToExpiration == 0:
            call = 1.0 if self.underlyingPrice > self.strikePrice else 0.0
            put = -1.0 if self.underlyingPrice < self.strikePrice else 0.0
        if self.strikePrice == 0:
            raise ZeroDivisionError("The strike price cannot be zero")
        else:
            _b_ = e ** (-self.dividendYield * self.daysToExpiration)
            call = _b_ * _norm.cdf(self._d1_)
            put = _b_ * (_norm.cdf(self._d1_) - 1)
        return [call, put]

    # Verify
    def _delta2(self):
        """Returns the dual delta: [Call dual delta, Put dual delta]"""
        if self.volatility == 0 or self.daysToExpiration == 0:
            call = -1.0 if self.underlyingPrice > self.strikePrice else 0.0
            put = 1.0 if self.underlyingPrice < self.strikePrice else 0.0
        if self.strikePrice == 0:
            raise ZeroDivisionError("The strike price cannot be zero")
        else:
            _b_ = e ** -(self.interestRate * self.daysToExpiration)
            call = -_norm.cdf(self._d2_) * _b_
            put = _norm.cdf(-self._d2_) * _b_
        return [call, put]

    def _vega(self):
        """Returns the option vega"""
        if self.volatility == 0 or self.daysToExpiration == 0:
            return 0.0
        if self.strikePrice == 0:
            raise ZeroDivisionError("The strike price cannot be zero")
        else:
            return (
                self.underlyingPrice
                * e ** (-self.dividendYield * self.daysToExpiration)
                * _norm.pdf(self._d1_)
                * self.daysToExpiration**0.5
                / 100
            )

    def _theta(self):
        """Returns the option theta: [Call theta, Put theta]"""
        _b_ = e ** -(self.interestRate * self.daysToExpiration)
        _d_ = e ** (-self.dividendYield * self.daysToExpiration)
        call = (
            -self.underlyingPrice * _d_ * _norm.pdf(self._d1_) * self.volatility / (2 * self.daysToExpiration**0.5)
            + self.dividendYield * self.underlyingPrice * _d_ * _norm.cdf(self._d1_)
            - self.interestRate * self.strikePrice * _b_ * _norm.cdf(self._d2_)
        )
        put = (
            -self.underlyingPrice * _d_ * _norm.pdf(self._d1_) * self.volatility / (2 * self.daysToExpiration**0.5)
            - self.dividendYield * self.underlyingPrice * _d_ * _norm.cdf(-self._d1_)
            + self.interestRate * self.strikePrice * _b_ * _norm.cdf(-self._d2_)
        )
        return [call / 365, put / 365]

    def _rho(self):
        """Returns the option rho: [Call rho, Put rho]"""
        _b_ = e ** -(self.interestRate * self.daysToExpiration)
        call = self.strikePrice * self.daysToExpiration * _b_ * _norm.cdf(self._d2_) / 100
        put = -self.strikePrice * self.daysToExpiration * _b_ * _norm.cdf(-self._d2_) / 100
        return [call, put]

    def _gamma(self):
        """Returns the option gamma"""
        return (
            e ** (-self.dividendYield * self.daysToExpiration) * _norm.pdf(self._d1_) / (self.underlyingPrice * self._a_)
        )

    # Verify
    def _parity(self):
        """Put-Call Parity"""
        return (
            self.callPrice
            - self.putPrice
            - self.underlyingPrice
            + (self.strikePrice / ((1 + self.interestRate) ** self.daysToExpiration))
        )
