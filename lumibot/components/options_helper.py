import logging
import math
import warnings
from bisect import bisect_left
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from statistics import NormalDist
from typing import Any, Dict, List, Optional, Tuple, Union

from lumibot.entities import Asset, Order, Quote
from lumibot.entities.chains import Chains
from lumibot.tools.helpers import get_multileg_quantity_units

logger = logging.getLogger(__name__)


@dataclass
class OptionMarketEvaluation:
    """Structured result from evaluate_option_market."""

    bid: Optional[float]
    ask: Optional[float]
    last_price: Optional[float]
    spread_pct: Optional[float]
    has_bid_ask: bool
    spread_too_wide: bool
    missing_bid_ask: bool
    missing_last_price: bool
    buy_price: Optional[float]
    sell_price: Optional[float]
    used_last_price_fallback: bool
    max_spread_pct: Optional[float]
    data_quality_flags: List[str]


class OptionsHelper:
    """
    OptionsHelper is a utility component for constructing and managing various options strategies.
    It provides functions for:

    - Finding valid options (e.g. handling expiries on holidays/weekends)
    - Calculating option deltas and finding strikes that best match target deltas
    - Calculating a multi-leg limit price for orders
    - Building orders for various spread strategies (vertical, calendar, butterfly, straddle, strangle, diagonal, ratio)
    - Executing (submitting) the constructed orders separately from building them
    - Advanced risk management functions such as aggregating portfolio Greeks and checking spread profit
    - Additional utility functions for liquidity checking and order detail summaries
    """

    def __init__(self, strategy) -> None:
        """
        Initialize the OptionsHelper.

        Parameters
        ----------
        strategy : Strategy
            The strategy instance which must implement functions like get_last_price(), get_quote(),
            get_greeks(), create_order(), submit_orders(), log_message(), etc.
        """
        self.strategy = strategy
        # Cache for expiries known to yield no valid option
        self.non_existing_expiry_dates: List[Dict[str, Union[str, date]]] = []
        # For risk management in condor orders
        self.last_condor_prices: Optional[Dict[Order, float]] = None
        self.last_call_sell_strike: Optional[float] = None
        self.last_put_sell_strike: Optional[float] = None
        self._liquidity_deprecation_warned = False

        # PERF: Option-heavy strategies often call quote/greeks helpers multiple times per bar.
        # In backtesting, quotes are immutable within a bar, so cache derived values keyed by the
        # current strategy datetime to avoid repeated get_quote()/get_greeks() work.
        self._per_bar_cache_dt: Optional[datetime] = None
        self._per_bar_option_mark_cache: Dict[Asset, Tuple[Optional[float], Optional[float], Optional[float]]] = {}
        self._per_bar_delta_cache: Dict[Tuple[Asset, Optional[float]], Optional[float]] = {}
        self._per_bar_greeks_cache: Dict[Tuple[Asset, Optional[float], Optional[float]], Optional[Dict[str, Any]]] = {}

        # PERF: When quote-based expiration validation fails repeatedly in long-window backtests,
        # temporarily disable that validation per underlying to avoid day-after-day probe storms.
        self._expiration_validation_disabled_until: Dict[str, date] = {}
        self.strategy.log_message("OptionsHelper initialized.", color="blue")

    def _first_time(self, kind: str, key: Any, value: Any = None) -> bool:
        """True the first time ``(kind, key)`` is seen, or when its ``value`` changed.

        Minute strategies call the search helpers on every bar with the same inputs. Their
        routine detail lines are logged at INFO the first time and when the outcome changes,
        and at DEBUG otherwise, so a run is not flooded with hundreds of identical lines per
        minute. Warnings and anything explaining why no option was usable stay at INFO.
        """
        seen = self.__dict__.setdefault("_routine_log_seen", {})
        slot = (kind, key)
        if slot in seen and seen[slot] == value:
            return False
        seen[slot] = value
        return True

    def _detail(self, loud: bool, message: str, color: Optional[str] = None) -> None:
        """Log routine detail at INFO when ``loud``, otherwise at DEBUG."""
        if loud:
            self.strategy.log_message(message, color=color)
            return
        strategy_logger = getattr(self.strategy, "logger", None)
        if strategy_logger is None:
            return
        try:
            if strategy_logger.isEnabledFor(logging.DEBUG):
                strategy_logger.debug(message)
        except Exception:
            return

    def _prefetch_option_contracts(self, underlying_asset: Asset, expiry: date, right: str,
                                   strikes: List[float]) -> None:
        """Ask the data source to load many contracts' prices at once, when it can.

        A delta search probes many strikes; without this each probe downloads its own
        contract. Data sources that implement ``prefetch_option_bars`` (for example Alpaca
        backtests) load them in batched requests. The prices read afterwards are the same
        stored data, so the chosen strike cannot change. Sources without it do nothing.
        """
        broker = getattr(self.strategy, "broker", None)
        prefetch = getattr(getattr(broker, "data_source", None), "prefetch_option_bars", None)
        if not callable(prefetch) or not strikes:
            return
        until = None
        try:
            until = self.strategy.get_datetime() if getattr(self.strategy, "is_backtesting", False) else None
        except Exception:
            until = None
        # Minute strategies search on every bar: ask once per contract and day, so a
        # repeated search costs a set lookup, not a batch of new Asset objects.
        day = until.date() if until is not None and hasattr(until, "date") else None
        done = self.__dict__.setdefault("_prefetched_option_contracts", set())
        side = (getattr(underlying_asset, "symbol", underlying_asset), expiry, str(right).lower(), day)
        missing = [strike for strike in strikes if (side, strike) not in done]
        if not missing:
            return
        done.update((side, strike) for strike in missing)
        assets = [
            self._option_contract(underlying_asset, expiry, strike, right)
            for strike in missing
        ]
        try:
            prefetch(assets, until=until) if until is not None else prefetch(assets)
        except Exception as exc:  # each probe still loads its own contract and reports errors
            logger.warning("Batched option price prefetch failed; loading contracts one at a time: %s", exc)

    # Contracts kept for reuse; cleared when it grows past this many (about 1 KB each).
    _OPTION_CONTRACT_CACHE_LIMIT = 20000

    def _option_contract(self, underlying_asset: Asset, expiry, strike, right) -> Asset:
        """The option Asset for this contract, built once and reused.

        Snapshots and delta picks name the same contracts over and over (row, delta lookup,
        prefetch); building an Asset each time was about a fifth of a warm snapshot. Equal
        inputs give an equal Asset either way, so callers see no difference.
        """
        cache = self.__dict__.setdefault("_option_contracts", {})
        key = (underlying_asset, expiry, strike, right)
        try:
            option = cache.get(key)
        except TypeError:  # unhashable input: build it as before
            option = None
            key = None
        if option is None:
            option = Asset(underlying_asset.symbol, asset_type="option", expiration=expiry, strike=strike,
                           right=right, underlying_asset=underlying_asset)
            if key is not None:
                if len(cache) >= self._OPTION_CONTRACT_CACHE_LIMIT:
                    cache.clear()
                cache[key] = option
        return option

    def _prefetch_option_quotes(self, underlying, expiry, right, strikes) -> None:
        """Load live quotes for many strikes with one request, for the current pick only.

        Live only, when the quote source offers the bulk ``get_quotes(assets)`` adapter used by
        ``Strategy.get_quotes`` (one request for many contracts) and the strategy reads quotes
        through the standard ``get_quote``. The quotes are kept in ``_quote_batch`` until
        ``_end_quote_batch``; a contract the source returned no quote for counts as no quote,
        the same as a failed single request.
        """
        from lumibot.strategies.strategy import Strategy

        self._quote_batch = None
        broker = getattr(self.strategy, "broker", None)
        if broker is None or getattr(broker, "IS_BACKTESTING_BROKER", False):
            return
        if getattr(type(self.strategy), "get_quote", None) is not Strategy.get_quote:
            return
        source = getattr(broker, "option_source", None) or getattr(broker, "data_source", None)
        many = getattr(source, "get_quotes", None)
        if not callable(many):
            return
        assets = [
            self._option_contract(underlying, expiry, strike, right)
            for strike in strikes
        ]
        try:
            quotes = many(assets, quote=None, exchange=None) or {}
        except Exception as exc:  # each strike still asks for its own quote
            logger.warning("Batched option quotes failed; asking one contract at a time: %s", exc)
            return
        self._quote_batch = {asset: quotes.get(asset) for asset in assets}

    def _end_quote_batch(self) -> None:
        self._quote_batch = None

    def _reset_per_bar_caches_if_needed(self) -> None:
        """Reset per-bar caches when the strategy datetime advances."""
        try:
            current_dt = self.strategy.get_datetime()
        except Exception:
            return

        if self._per_bar_cache_dt != current_dt:
            self._per_bar_cache_dt = current_dt
            self._per_bar_option_mark_cache = {}
            self._per_bar_delta_cache = {}
            self._per_bar_greeks_cache = {}

    @staticmethod
    def _coerce_price(value: Any, field_name: str, flags: List[str], notes: List[str]) -> Optional[float]:
        """Normalize quote values and record data quality issues."""
        raw_value = value

        if value is None:
            flags.append(f"{field_name}_missing")
            return None

        try:
            if isinstance(value, Decimal):
                value = float(value)
            else:
                value = float(value)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            flags.append(f"{field_name}_non_numeric")
            notes.append(f"{field_name} value {raw_value!r} is non-numeric; dropping.")
            return None

        if math.isnan(value) or math.isinf(value):
            flags.append(f"{field_name}_non_finite")
            notes.append(f"{field_name} value {value!r} is not finite; dropping.")
            return None

        if value <= 0:
            flags.append(f"{field_name}_non_positive")
            notes.append(f"{field_name} value {value!r} is non-positive; dropping.")
            return None

        return value

    @staticmethod
    def has_actionable_price(evaluation: Optional["OptionMarketEvaluation"]) -> bool:
        """Return True when the evaluation contains usable buy and sell prices.

        A quote can be "one-sided" (ask-only or bid-only). In that case we cannot
        safely execute both buy and sell actions, so treat it as non-actionable.
        """
        if evaluation is None:
            return False

        buy_price = evaluation.buy_price
        sell_price = evaluation.sell_price
        if buy_price is None or sell_price is None:
            return False

        try:
            buy_price = float(buy_price)
            sell_price = float(sell_price)
        except (TypeError, ValueError):
            return False

        return (
            math.isfinite(buy_price)
            and buy_price > 0
            and math.isfinite(sell_price)
            and sell_price > 0
            and not evaluation.spread_too_wide
        )

    @staticmethod
    def _float_positive(value: Any) -> Optional[float]:
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(numeric) or numeric <= 0:
            return None
        return numeric

    def _get_option_mark_from_quote(
        self,
        option_asset: Asset,
        *,
        snapshot: bool = False,
    ) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        """Return (mark_price, bid, ask) derived from quotes; never calls get_last_price()."""
        self._reset_per_bar_caches_if_needed()

        cache_key = (option_asset, bool(snapshot))
        cached = self._per_bar_option_mark_cache.get(cache_key)
        if cached is not None:
            return cached

        batch = getattr(self, "_quote_batch", None)
        try:
            if batch is not None and option_asset in batch:
                quote = batch[option_asset]
            else:
                quote = self._fetch_option_quote(option_asset, snapshot)
        except Exception:
            return None, None, None
        return self._mark_from_quote(cache_key, quote)

    def _fetch_option_quote(self, option_asset: Asset, snapshot: bool):
        # PERF: delta/strike probing can touch many strikes that will never be traded. For those,
        # we want a point-in-time quote snapshot (1-2 bars), not a full-day prefetch that can
        # balloon to ~956 minute rows per strike. Use a ThetaData backtesting fast-path when
        # available; otherwise fall back to the normal Strategy.get_quote().
        broker = getattr(self.strategy, "broker", None)
        if snapshot and broker is not None and getattr(broker, "IS_BACKTESTING_BROKER", False) is True:
            asset_type = getattr(option_asset, "asset_type", None)
            is_option_asset = asset_type == Asset.AssetType.OPTION or "option" in str(asset_type).lower()

            # Mirror Strategy.get_quote behavior: prefer the broker's option_source when
            # available; otherwise fall back to the primary data source.
            source = getattr(broker, "option_source", None) if is_option_asset else None
            if source is None:
                source = getattr(broker, "data_source", None)
            # Use the data source's snapshot_only fast-path when available so option selection
            # doesn't trigger full-day minute downloads for strikes that will never be traded.
            # ThetaDataBacktestingPandas handles daily-cadence quirks internally (forward window).
            if source is not None:
                try:
                    quote = source.get_quote(option_asset, quote=None, exchange=None, snapshot_only=True)
                except TypeError:
                    quote = source.get_quote(option_asset, quote=None, exchange=None)
            else:
                quote = self.strategy.get_quote(option_asset)
        else:
            quote = self.strategy.get_quote(option_asset)
        return quote

    def _mark_from_quote(self, cache_key, quote) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        bid = self._float_positive(getattr(quote, "bid", None))
        ask = self._float_positive(getattr(quote, "ask", None))

        if bid is not None and ask is not None:
            result = ((bid + ask) / 2.0, bid, ask)
            self._per_bar_option_mark_cache[cache_key] = result
            return result
        if bid is not None:
            result = (bid, bid, None)
            self._per_bar_option_mark_cache[cache_key] = result
            return result
        if ask is not None:
            result = (ask, None, ask)
            self._per_bar_option_mark_cache[cache_key] = result
            return result

        price = self._float_positive(getattr(quote, "price", None))
        result = (price, bid, ask)
        self._per_bar_option_mark_cache[cache_key] = result
        return result

    # ============================================================
    # Basic Utility Functions
    # ============================================================

    def find_next_valid_option(
        self,
        underlying_asset: Asset,
        rounded_underlying_price: float,
        expiry: date,
        put_or_call: str = "call",
        chains: Optional[Any] = None,
    ) -> Optional[Asset]:
        """
        Find a valid option with the given expiry and strike.
        First tries the requested strike, then searches nearby strikes from the option chain.
        If no strikes work for this expiry, tries the next expiry date.

        Parameters
        ----------
        underlying_asset : Asset
            The underlying asset.
        rounded_underlying_price : float
            The intended strike (usually the underlying price rounded to a strike step).
        expiry : date
            The target expiry date.
        put_or_call : str, optional
            "call" or "put" (default is "call").

        Returns
        -------
        Optional[Asset]
            The valid option asset or None if not found.
        """
        # Provider compatibility: some chains represent the option expiry as OCC "Saturday".
        # Our trading model expects the last tradable session (Friday).
        expiry = self._normalize_to_trading_expiry(expiry)

        # PERF: When a strategy repeatedly fails to find actionable quotes for a given expiry (common in
        # long-window backtests with sparse historical option coverage), scanning the chain every bar/day
        # can dominate runtime. Keep a cooldown so repeated attempts return quickly.
        current_dt = None
        try:
            current_dt = self.strategy.get_datetime()
        except Exception:
            current_dt = None

        broker = getattr(self.strategy, "broker", None)
        is_backtesting = bool(getattr(self.strategy, "is_backtesting", False)) or (
            broker is not None and getattr(broker, "IS_BACKTESTING_BROKER", False) is True
        )

        cooldown = getattr(self, "_valid_option_search_cooldown", None)
        if not isinstance(cooldown, dict):
            cooldown = {}
            self._valid_option_search_cooldown = cooldown

        # PERF (backtesting-only): Long-window option strategies can attempt the same "find option"
        # scan on consecutive days (or every bar) while the provider has no quote history for those
        # expirations/strikes. Add a short cooldown so missing-data periods don't dominate runtime.
        #
        # Keep this disabled for live trading so a transient quote gap never suppresses trading.
        cooldown_days = 7 if is_backtesting else 0
        search_key = (underlying_asset.symbol, expiry, put_or_call.upper())
        if cooldown_days and current_dt is not None:
            next_retry_at = cooldown.get(search_key)
            if next_retry_at is not None and current_dt < next_retry_at:
                return None

        search_log_key = (search_key, rounded_underlying_price)
        self._detail(
            self._first_time("next_valid_option_search", search_log_key),
            f"Finding next valid option for {underlying_asset.symbol} at strike {rounded_underlying_price} and expiry {expiry}",
            color="blue",
        )

        data_source = getattr(broker, "data_source", None) if broker is not None else None
        is_theta_backtest = (
            is_backtesting
            and data_source is not None
            and data_source.__class__.__name__ == "ThetaDataBacktestingPandas"
        )
        is_daily_cadence = False
        if is_backtesting:
            try:
                sleeptime = getattr(self.strategy, "sleeptime", None)
                if isinstance(sleeptime, str) and sleeptime.strip().upper().endswith("D"):
                    is_daily_cadence = True
            except Exception:
                pass
            if getattr(data_source, "_timestep", None) == "day":
                is_daily_cadence = True

        invalid_expiries = {
            (record.get("underlying_asset_symbol"), record.get("expiry"))
            for record in self.non_existing_expiry_dates
            if isinstance(record, dict)
        }

        max_expiration_hint = None
        try:
            max_expiration_hint = self._default_chain_max_expiration_date(
                underlying_asset=underlying_asset,
                min_expiration_date=expiry,
            )
        except Exception:
            max_expiration_hint = None

        chains_data = chains
        if chains_data is None:
            with self._chain_hint(expiry, max_expiration_hint):
                chains_data = self.strategy.get_chains(underlying_asset)
        if not chains_data:
            self.strategy.log_message("Option chains unavailable; cannot locate a valid option.", color="yellow")
            return None

        # Use the chain expirations (not "next day") to avoid churn and weekend/non-expiry dates.
        expirations = self._get_chain_expirations(chains=chains_data, side=put_or_call)
        if not expirations:
            expirations = [expiry]
        expirations = [exp for exp in expirations if exp >= expiry]

        attempts = 0
        max_expirations_to_try = 10
        if is_theta_backtest and is_backtesting:
            # ThetaData historical options can have sparse coverage; scanning too many expirations
            # with network-backed quote probes can dominate long-window backtests.
            max_expirations_to_try = 5
            # NVDA-style hourly backtests can end up walking deep into the expiration list when
            # nearby expiries have missing/placeholder quote history (472). That produces many
            # downloader submits per trading day and can turn a single backtest into hours.
            #
            # Keep index underlyings slightly higher (dense expiry schedules), but for equities
            # cap the probe depth more aggressively.
            try:
                if not self._is_index_like_underlying(underlying_asset, getattr(underlying_asset, "symbol", None)):
                    max_expirations_to_try = 1
            except Exception:
                max_expirations_to_try = 1
        for exp_date in expirations:
            if attempts >= max_expirations_to_try:
                break

            exp_date = self._normalize_to_trading_expiry(exp_date)
            if (underlying_asset.symbol, exp_date) in invalid_expiries:
                continue
            next_retry_at = None
            if cooldown_days and current_dt is not None:
                exp_key = (underlying_asset.symbol, exp_date, put_or_call.upper())
                next_retry_at = cooldown.get(exp_key)
            if next_retry_at is not None and current_dt < next_retry_at:
                continue

            attempts += 1

            max_spread_pct = None
            params = getattr(self.strategy, "parameters", None)
            if isinstance(params, dict):
                for key in ("max_option_spread_pct", "max_spread_pct"):
                    if params.get(key) is None:
                        continue
                    try:
                        max_spread_pct = float(params[key])
                        break
                    except (TypeError, ValueError):
                        max_spread_pct = None

            try:
                available_strikes = chains_data.strikes(exp_date, put_or_call.upper())
            except Exception:
                if isinstance(chains_data, dict):
                    strike_map = chains_data.get("Chains", {}).get(str(put_or_call).upper(), {})
                    available_strikes = strike_map.get(exp_date.strftime("%Y-%m-%d"), []) if strike_map else []
                else:
                    available_strikes = []
            if not available_strikes:
                # Fallback: if the chain doesn't provide strikes for this expiry (common when
                # providers prune strike lists for performance), attempt to validate the requested
                # strike directly. This avoids any potential lazy strike hydration fanout in
                # backtests while still allowing strategies to proceed when the target strike is
                # tradeable.
                try:
                    requested_strike = float(rounded_underlying_price)
                except Exception:
                    requested_strike = None

                if requested_strike is not None and requested_strike > 0:
                    option = Asset(
                        underlying_asset.symbol,
                        asset_type="option",
                        expiration=exp_date,
                        strike=requested_strike,
                        right=put_or_call,
                        underlying_asset=underlying_asset,
                    )

                    if is_theta_backtest:
                        mark_price, bid, ask = self._get_option_mark_from_quote(option, snapshot=True)
                        if mark_price is None:
                            mark_price, bid, ask = self._get_option_mark_from_quote(option, snapshot=False)
                        if mark_price is not None:
                            if max_spread_pct is not None and (bid is None or ask is None):
                                mark_price = None
                            if mark_price is not None:
                                self._detail(
                                    self._first_time("next_valid_option_result", search_log_key, ('Using requested strike with price data', requested_strike)),
                                    f"Target strike {rounded_underlying_price} -> Using requested strike with price data: {requested_strike}",
                                    color="green",
                                )
                                return option
                    else:
                        self._detail(
                            self._first_time("next_valid_option_result", search_log_key, ('Using requested strike', requested_strike)),
                            f"Target strike {rounded_underlying_price} -> Using requested strike: {requested_strike}",
                            color="green",
                        )
                        return option

                self.non_existing_expiry_dates.append(
                    {"underlying_asset_symbol": underlying_asset.symbol, "expiry": exp_date}
                )
                continue

            strikes_sorted = sorted(
                [float(s) for s in available_strikes if s is not None],
                key=lambda s: abs(s - float(rounded_underlying_price)),
            )
            if not strikes_sorted:
                self.non_existing_expiry_dates.append(
                    {"underlying_asset_symbol": underlying_asset.symbol, "expiry": exp_date}
                )
                continue

            # PERF (intraday index strategies):
            # For liquid index underlyings, the nearest strike is almost always tradeable. Doing
            # snapshot-only quote probes for every candidate strike adds per-day remote requests
            # (dominating runtime in long-window backtests). Keep the strict quote validation for
            # non-index underlyings and for far-OTM index selections where liquidity is less certain.
            if (
                is_theta_backtest
                and not is_daily_cadence
                and self._is_index_like_underlying(underlying_asset, getattr(underlying_asset, "symbol", None))
            ):
                try:
                    underlying_price = None
                    price_value = self.strategy.get_last_price(underlying_asset)
                    if price_value is not None:
                        underlying_price = float(price_value)
                    if underlying_price and math.isfinite(underlying_price) and underlying_price > 0:
                        distance = abs(float(rounded_underlying_price) - underlying_price) / underlying_price
                        if distance <= 0.05:
                            closest_strike = strikes_sorted[0]
                            self._detail(
                                self._first_time("next_valid_option_result", search_log_key, ('Using closest strike without quote probe (index fast-path)', closest_strike)),
                                f"Target strike {rounded_underlying_price} -> Using closest strike without quote probe (index fast-path): {closest_strike}",
                                color="green",
                            )
                            return Asset(
                                underlying_asset.symbol,
                                asset_type="option",
                                expiration=exp_date,
                                strike=closest_strike,
                                right=put_or_call,
                                underlying_asset=underlying_asset,
                            )
                except Exception:
                    pass

            if is_theta_backtest:
                # ThetaData backtests: validate the option has *some* price signal at the current
                # strategy datetime so strategies don't get stuck selecting contracts with no history
                # (472 placeholder-only). Use a small bounded scan to keep this fast.
                if is_daily_cadence:
                    # Daily-cadence: prefer actionable NBBO from a point-in-time snapshot when
                    # available, but fall back to the bar-aligned quote path so we can still
                    # validate tradeable coverage in older historical windows.
                    #
                    # NOTE: Strategies that set `max_option_spread_pct` intend to evaluate
                    # bid/ask width. For those, require bid *and* ask so we don't "validate"
                    # a contract that cannot be priced for both entry/exit decisions.
                    max_strikes_to_try = 10

                    for candidate_strike in strikes_sorted[:max_strikes_to_try]:
                        option = Asset(
                            underlying_asset.symbol,
                            asset_type="option",
                            expiration=exp_date,
                            strike=candidate_strike,
                            right=put_or_call,
                            underlying_asset=underlying_asset,
                        )

                        mark_price, bid, ask = self._get_option_mark_from_quote(option, snapshot=True)
                        if mark_price is None:
                            mark_price, bid, ask = self._get_option_mark_from_quote(option, snapshot=False)
                        if mark_price is None:
                            continue

                        if max_spread_pct is not None:
                            if bid is None or ask is None:
                                continue
                            mid = (bid + ask) / 2.0
                            if mid <= 0:
                                continue
                            spread_pct = (ask - bid) / mid
                            if spread_pct > max_spread_pct:
                                continue

                        self._detail(
                            self._first_time("next_valid_option_result", search_log_key, ('Using strike with price data', candidate_strike)),
                            f"Target strike {rounded_underlying_price} -> Using strike with price data: {candidate_strike}",
                            color="green",
                        )
                        return option

                    if cooldown_days and current_dt is not None:
                        cooldown[(underlying_asset.symbol, exp_date, put_or_call.upper())] = current_dt + timedelta(
                            days=cooldown_days
                        )
                    continue

                # Intraday strategies: prefer a strike with actionable NBBO snapshot when available.
                max_strikes_to_try = 10
                if is_backtesting:
                    # PERF: In long-window equity option backtests (e.g., NVDA drawdown calls),
                    # scanning many strikes can generate hundreds/thousands of snapshot quote probes
                    # that mostly return placeholder-only (472). Keep the scan tight; strategies that
                    # need broader search should do so explicitly.
                    max_strikes_to_try = 5
                    try:
                        if not self._is_index_like_underlying(underlying_asset, getattr(underlying_asset, "symbol", None)):
                            max_strikes_to_try = 3
                    except Exception:
                        max_strikes_to_try = 3

                saw_any_mark_price = False
                for candidate_strike in strikes_sorted[:max_strikes_to_try]:
                    option = Asset(
                        underlying_asset.symbol,
                        asset_type="option",
                        expiration=exp_date,
                        strike=candidate_strike,
                        right=put_or_call,
                        underlying_asset=underlying_asset,
                    )
                    mark_price, bid, ask = self._get_option_mark_from_quote(option, snapshot=True)
                    if mark_price is None:
                        continue
                    saw_any_mark_price = True
                    # Intraday strategies depend on actionable NBBO to select a tradeable contract.
                    # Prefer strikes that have both bid and ask (two-sided) rather than one-sided quotes.
                    if bid is None or ask is None:
                        continue
                    if max_spread_pct is not None:
                        if bid is None or ask is None:
                            continue
                        mid = (bid + ask) / 2.0
                        if mid <= 0:
                            continue
                        spread_pct = (ask - bid) / mid
                        if spread_pct > max_spread_pct:
                            continue
                    self._detail(
                        self._first_time("next_valid_option_result", search_log_key, ('Using actionable strike', candidate_strike)),
                        f"Target strike {rounded_underlying_price} -> Using actionable strike: {candidate_strike}",
                        color="green",
                    )
                    return option
                # No strikes had actionable quotes for this expiry. Back off for a few days to
                # avoid re-scanning the same expiry on every subsequent bar/day.
                if cooldown_days and current_dt is not None:
                    retry_days = cooldown_days
                    # If *none* of the candidate strikes had even a single price signal (all 472 /
                    # placeholder-only), the provider likely has no quote history coverage for this
                    # expiry in the current window. Back off longer so long-window backtests don't
                    # spend most of their time re-probing known-empty periods.
                    if is_backtesting and not saw_any_mark_price:
                        retry_days = max(retry_days, 30)
                    cooldown[(underlying_asset.symbol, exp_date, put_or_call.upper())] = current_dt + timedelta(days=retry_days)
                continue

            closest_strike = strikes_sorted[0]
            self._detail(
                self._first_time("next_valid_option_result", search_log_key, ('Closest available strike', closest_strike)),
                f"Target strike {rounded_underlying_price} -> Closest available strike: {closest_strike}",
                color="green",
            )
            return Asset(
                underlying_asset.symbol,
                asset_type="option",
                expiration=exp_date,
                strike=closest_strike,
                right=put_or_call,
                underlying_asset=underlying_asset,
            )

        self.strategy.log_message("Exceeded maximum attempts to find a valid option.", color="red")
        if cooldown_days and current_dt is not None:
            proposed = current_dt + timedelta(days=cooldown_days)
            existing = cooldown.get(search_key)
            if existing is None or existing < proposed:
                cooldown[search_key] = proposed
        return None

    @staticmethod
    def _normalize_to_trading_expiry(expiry: date) -> date:
        """Normalize expirations to the last tradable session (Friday).

        Some providers represent expirations using the OCC "Saturday" date.
        LumiBot backtesting and order routing treat the Friday session as the last tradable day.
        """
        try:
            if isinstance(expiry, datetime):
                expiry = expiry.date()
            if expiry.weekday() == 5:
                return expiry - timedelta(days=1)
            if expiry.weekday() == 6:
                return expiry - timedelta(days=2)
        except Exception:
            return expiry
        return expiry

    @staticmethod
    def _get_chain_expirations(*, chains: Any, side: str) -> List[date]:
        """Return sorted expiration dates for the given side ("call"/"put")."""
        side_key = str(side).upper()
        if side_key.startswith("C"):
            side_key = "CALL"
        elif side_key.startswith("P"):
            side_key = "PUT"

        try:
            from lumibot.entities.chains import _normalise_expiry
        except Exception:
            _normalise_expiry = None

        expirations: List[date] = []
        chain_map = None
        if isinstance(chains, dict):
            chain_map = chains.get("Chains", {}).get(side_key, {})
        elif hasattr(chains, "get"):
            try:
                chain_map = chains.get("Chains", {}).get(side_key, {})
            except Exception:
                chain_map = None

        if isinstance(chain_map, dict):
            for expiry_key in chain_map.keys():
                try:
                    if _normalise_expiry is not None:
                        exp_date = _normalise_expiry(expiry_key)
                    else:
                        exp_date = datetime.strptime(str(expiry_key), "%Y-%m-%d").date()
                except Exception:
                    continue
                expirations.append(exp_date)

        expirations = sorted({OptionsHelper._normalize_to_trading_expiry(d) for d in expirations})
        return expirations

    def get_option_chain_snapshot(
        self,
        underlying_asset: Asset,
        expiry: date,
        right: str,
        strikes: Optional[List[float]] = None,
        underlying_price: Optional[float] = None,
        chains: Optional[Any] = None,
    ) -> List[Dict[str, Any]]:
        """Price, bid/ask, implied volatility and delta for many strikes at the current time.

        One call per expiry and side. Data sources that can load many contracts at once
        (``prefetch_option_bars``) are asked once for all strikes, then each strike is priced
        with the same mark and greeks rules as :meth:`get_delta_for_strike`, so the numbers
        match what a strike-by-strike search would see. Missing prices stay ``None``; nothing
        is estimated or filled in.

        Parameters
        ----------
        underlying_asset : Asset
            The underlying.
        expiry : date
            Option expiration.
        right : str
            "call" or "put".
        strikes : list of float, optional
            Strikes to include. Default: every strike the chain lists for this expiry and side.
        underlying_price : float, optional
            Underlying price to use. Default: the current last price.
        chains : optional
            A chain object already fetched with ``get_chains``.

        Returns
        -------
        list of dict
            One row per strike, sorted by strike, with keys ``strike``, ``price``, ``bid``,
            ``ask``, ``implied_volatility`` and ``delta``.
        """
        expiry = self._normalize_to_trading_expiry(expiry)
        option_type = "CALL" if str(right).upper().startswith("C") else "PUT"
        if strikes is None:
            chains_data = chains if chains is not None else self.strategy.get_chains(underlying_asset)
            strikes = []
            if chains_data:
                try:
                    strikes = list(chains_data.strikes(expiry, option_type))
                except Exception:
                    strike_map = chains_data.get("Chains", {}).get(option_type, {}) if isinstance(chains_data, dict) else {}
                    strikes = strike_map.get(expiry.strftime("%Y-%m-%d"), []) if strike_map else []
        strikes = sorted({float(k) for k in strikes if k is not None and float(k) > 0})
        if underlying_price is None:
            underlying_price = self.strategy.get_last_price(underlying_asset)
        if not strikes:
            return []
        return self._price_strikes(underlying_asset, expiry, right, strikes, underlying_price)

    def _price_strikes(self, underlying_asset: Asset, expiry: date, right: str, strikes: List[float],
                       underlying_price: Optional[float]) -> List[Dict[str, Any]]:
        """Snapshot rows for these strikes of one expiry and side (see get_option_chain_snapshot)."""
        rows: List[Dict[str, Any]] = []
        if not strikes:
            return rows
        self._prefetch_option_contracts(underlying_asset, expiry, right, strikes)
        self._reset_per_bar_caches_if_needed()
        entries = []
        for strike in strikes:
            option = self._option_contract(underlying_asset, expiry, strike, right)
            price, bid, ask = self._get_option_mark_from_quote(option, snapshot=True)
            if price is None:
                try:
                    last = self.strategy.get_last_price(option)
                    price = float(last) if last is not None and math.isfinite(float(last)) and float(last) > 0 else None
                except Exception:
                    price = None
            entries.append((strike, option, price, bid, ask))
        if underlying_price is not None:
            self._batch_greeks_for_snapshot(entries, float(underlying_price))
        for strike, option, price, bid, ask in entries:
            iv = delta = None
            if underlying_price is not None:
                # get_delta_for_strike reads the same mark (and returns None without one).
                delta = self.get_delta_for_strike(underlying_asset, underlying_price, strike, expiry, right)
            if price is not None and underlying_price is not None:
                greeks = self._per_bar_greeks_cache.get((option, float(underlying_price), price))
                if isinstance(greeks, dict):
                    iv = greeks.get("implied_volatility")
            rows.append({"strike": strike, "price": price, "bid": bid, "ask": ask,
                         "implied_volatility": iv, "delta": delta})
        return rows

    def _batch_greeks_for_snapshot(self, entries, underlying_price: float) -> None:
        """Fill the per-bar greeks cache for many strikes in one array pass, when it is exact.

        Only when the strategy and broker use the standard greeks path (backtesting broker,
        ``Strategy.get_greeks`` -> ``Broker.get_greeks`` -> ``DataSource.calculate_greeks``):
        ``calculate_greeks_many`` then returns exactly what those calls return, so
        ``get_delta_for_strike`` reads the same numbers it would compute. Anything else
        (live brokers that may query greeks, custom ``get_greeks``) keeps the one-by-one path.
        """
        from lumibot.brokers.broker import Broker
        from lumibot.strategies.strategy import Strategy

        broker = getattr(self.strategy, "broker", None)
        source = getattr(broker, "data_source", None)
        many = getattr(source, "calculate_greeks_many", None)
        if (
            not callable(many)
            or not getattr(broker, "IS_BACKTESTING_BROKER", False)
            or getattr(type(self.strategy), "get_greeks", None) is not Strategy.get_greeks
            or getattr(type(broker), "get_greeks", None) is not Broker.get_greeks
        ):
            return
        todo = [(option, price) for _strike, option, price, _bid, _ask in entries
                if price is not None
                and (option, underlying_price) not in self._per_bar_delta_cache
                and (option, underlying_price, price) not in self._per_bar_greeks_cache]
        if not todo:
            return
        try:
            rate = self.strategy.risk_free_rate
            greeks = many([o for o, _p in todo], [p for _o, p in todo], underlying_price, rate)
        except Exception as exc:  # one-by-one path below still runs and reports its own errors
            logger.debug("Batched greeks unavailable; computing one strike at a time: %s", exc)
            return
        for (option, price), result in zip(todo, greeks):
            self._per_bar_greeks_cache[(option, underlying_price, price)] = result

    def get_strike_deltas(self, underlying_asset: Asset, expiry: date, strikes: List[float],
                          right: str, stop_greater_than: Optional[float] = None,
                          stop_less_than: Optional[float] = None) -> Dict[float, Optional[float]]:
        """
        Compute the delta for each strike in a given list.

        Parameters
        ----------
        underlying_asset : Asset
            The underlying asset.
        expiry : date
            The expiry date.
        strikes : List[float]
            List of strike prices.
        right : str
            "call" or "put".
        stop_greater_than : Optional[float]
            Stop iteration if a computed delta is >= this value.
        stop_less_than : Optional[float]
            Stop iteration if a computed delta is <= this value.

        Returns
        -------
        Dict[float, Optional[float]]
            Mapping from strike price to its delta.
        """
        self._detail(
            self._first_time("strike_deltas", (underlying_asset.symbol, expiry)),
            f"Computing strike deltas for {underlying_asset.symbol} at expiry {expiry}.",
            color="blue",
        )
        strike_deltas: Dict[float, Optional[float]] = {}

        underlying_price = self.strategy.get_last_price(underlying_asset)
        if underlying_price is None:
            return strike_deltas

        # ------------------------------------------------------------------
        # PERF: Many AI-generated strategies pass hundreds of strikes into this
        # method and then select the closest-to-target delta outside of LumiBot.
        # Doing a quote snapshot + greeks solve per strike becomes thousands of
        # remote calls in production (each with ~1s overhead).
        #
        # When the strategy exposes a target delta (common names: target_call_delta
        # / target_put_delta) and the strike list is large, evaluate only O(log N)
        # candidates using a bounded binary search over strikes.
        # ------------------------------------------------------------------
        target_delta: Optional[float] = None
        if stop_greater_than is None and stop_less_than is None and len(strikes) >= 80:
            right_norm = str(right).strip().lower()
            if right_norm.startswith("c"):
                var_name = "target_call_delta"
            elif right_norm.startswith("p"):
                var_name = "target_put_delta"
            else:
                var_name = ""

            if var_name:
                vars_obj = getattr(self.strategy, "vars", None)
                if vars_obj is not None and hasattr(vars_obj, var_name):
                    try:
                        target_delta = float(getattr(vars_obj, var_name))
                    except Exception:
                        target_delta = None
                if target_delta is None:
                    params_obj = getattr(self.strategy, "parameters", None)
                    if isinstance(params_obj, dict) and var_name in params_obj:
                        try:
                            target_delta = float(params_obj[var_name])
                        except Exception:
                            target_delta = None

            if target_delta is not None and (not math.isfinite(target_delta) or abs(target_delta) > 1):
                target_delta = None

        if target_delta is not None:
            strikes_sorted: List[float] = []
            for value in strikes:
                try:
                    strikes_sorted.append(float(value))
                except Exception:
                    continue
            strikes_sorted = sorted(set(strikes_sorted))

            if len(strikes_sorted) >= 2:
                self.strategy.log_message(
                    f"Computing strike deltas fast-path: {len(strikes_sorted)} strikes -> binary search (target_delta={target_delta}).",
                    color="blue",
                )

                lo = 0
                hi = len(strikes_sorted) - 1
                visited: set[int] = set()
                max_iters = int(math.ceil(math.log2(len(strikes_sorted) + 1))) + 8

                best_idx: Optional[int] = None
                best_delta: Optional[float] = None

                # PERF: Seed strike evaluation near the target delta using a Black–Scholes delta
                # inversion estimate and probe only a small neighborhood. This avoids the full
                # binary-walk (and its many quote probes) for option-heavy strategies.
                strike_estimate: Optional[float] = None
                try:
                    right_norm = str(right).strip().lower()
                    is_call = right_norm.startswith("c")
                    call_delta = float(target_delta) if is_call else float(target_delta) + 1.0
                    if 0.01 < call_delta < 0.99:
                        as_of = None
                        try:
                            now = self.strategy.get_datetime()
                        except Exception:
                            now = None
                        if isinstance(now, datetime):
                            as_of = now.date()
                        elif isinstance(now, date):
                            as_of = now
                        if as_of is None:
                            as_of = date.today()

                        days_to_expiry = (expiry - as_of).days
                        t_years = max(1.0 / 365.0, float(days_to_expiry) / 365.0)

                        try:
                            is_index_like = self._is_index_like_underlying(
                                underlying_asset, getattr(underlying_asset, "symbol", None)
                            )
                        except Exception:
                            is_index_like = False
                        sigma = 0.25 if is_index_like else 0.35

                        nd = NormalDist()
                        d1 = nd.inv_cdf(call_delta)
                        sig_sqrt_t = sigma * math.sqrt(t_years)
                        if sig_sqrt_t > 0:
                            ln_s_over_k = (d1 * sig_sqrt_t) - (0.5 * sigma * sigma * t_years)
                            strike_estimate = float(underlying_price) / math.exp(ln_s_over_k)
                except Exception:
                    strike_estimate = None

                if strike_estimate is not None:
                    try:
                        insert_at = bisect_left(strikes_sorted, strike_estimate)
                        start_idx = min(max(insert_at, 0), len(strikes_sorted) - 1)
                        if start_idx > 0:
                            before = strikes_sorted[start_idx - 1]
                            after = strikes_sorted[start_idx]
                            if abs(before - strike_estimate) <= abs(after - strike_estimate):
                                start_idx -= 1

                        for offset in range(0, 5):
                            for idx in (start_idx - offset, start_idx + offset):
                                if idx < lo or idx > hi or idx in visited:
                                    continue
                                visited.add(idx)
                                strike = strikes_sorted[idx]
                                delta = self.get_delta_for_strike(
                                    underlying_asset,
                                    float(underlying_price),
                                    strike,
                                    expiry,
                                    right,
                                )
                                strike_deltas[strike] = delta
                                if delta is None:
                                    continue
                                if best_delta is None or abs(delta - target_delta) < abs(best_delta - target_delta):
                                    best_delta = delta
                                    best_idx = idx

                        if best_idx is not None:
                            # Add a wider neighborhood around the best strike. The Black–Scholes
                            # inversion estimate is intentionally coarse (fixed sigma) and can be
                            # off by a few strikes; ensure we still include the true closest delta
                            # without falling back to the full binary-walk.
                            for idx in range(max(0, best_idx - 6), min(len(strikes_sorted), best_idx + 7)):
                                strike = strikes_sorted[idx]
                                if strike in strike_deltas:
                                    continue
                                strike_deltas[strike] = self.get_delta_for_strike(
                                    underlying_asset,
                                    float(underlying_price),
                                    strike,
                                    expiry,
                                    right,
                                )
                            return strike_deltas
                    except Exception:
                        # Fall back to the legacy binary-walk below.
                        pass

                for _ in range(max_iters):
                    if lo > hi:
                        break

                    mid = (lo + hi) // 2
                    if mid in visited:
                        break
                    visited.add(mid)

                    strike = strikes_sorted[mid]
                    delta = self.get_delta_for_strike(
                        underlying_asset,
                        float(underlying_price),
                        strike,
                        expiry,
                        right,
                    )
                    strike_deltas[strike] = delta

                    if delta is None:
                        # Try a few nearby strikes when the midpoint has no actionable prices.
                        for offset in range(1, 6):
                            resolved = False
                            for idx in (mid - offset, mid + offset):
                                if idx < lo or idx > hi or idx in visited:
                                    continue
                                visited.add(idx)
                                strike_candidate = strikes_sorted[idx]
                                delta_candidate = self.get_delta_for_strike(
                                    underlying_asset,
                                    float(underlying_price),
                                    strike_candidate,
                                    expiry,
                                    right,
                                )
                                strike_deltas[strike_candidate] = delta_candidate
                                if delta_candidate is None:
                                    continue
                                mid = idx
                                strike = strike_candidate
                                delta = delta_candidate
                                resolved = True
                                break
                            if resolved:
                                break

                        if delta is None:
                            break

                    if delta is None:
                        break

                    if best_delta is None or abs(delta - target_delta) < abs(best_delta - target_delta):
                        best_delta = delta
                        best_idx = mid

                    # Delta decreases as strike increases for both calls and puts.
                    if delta > target_delta:
                        lo = mid + 1
                    elif delta < target_delta:
                        hi = mid - 1
                    else:
                        break

                # Add a small neighborhood around the best strike so callers that
                # choose the closest delta externally still have tie-break context.
                if best_idx is not None:
                    for idx in range(max(0, best_idx - 2), min(len(strikes_sorted), best_idx + 3)):
                        strike = strikes_sorted[idx]
                        if strike in strike_deltas:
                            continue
                        strike_deltas[strike] = self.get_delta_for_strike(
                            underlying_asset,
                            float(underlying_price),
                            strike,
                            expiry,
                            right,
                        )

                return strike_deltas

        # Default: full scan (legacy behavior).
        for strike in strikes:
            option = Asset(
                underlying_asset.symbol,
                asset_type="option",
                expiration=expiry,
                strike=strike,
                right=right,
                underlying_asset=underlying_asset,
            )
            option_price, _, _ = self._get_option_mark_from_quote(option, snapshot=True)
            if option_price is None:
                try:
                    option_price = self._float_positive(self.strategy.get_last_price(option))
                except Exception:
                    option_price = None
            if option_price is None:
                self.strategy.log_message(f"No price for option at strike {strike}. Skipping.", color="yellow")
                continue

            try:
                greeks = self.strategy.get_greeks(option, underlying_price=underlying_price, asset_price=option_price)
            except TypeError:
                # Some unit tests (and custom strategy stubs) mock get_greeks without the asset_price kwarg.
                greeks = self.strategy.get_greeks(option, underlying_price=underlying_price)
            if greeks is None:
                self.strategy.log_message(f"Could not calculate Greeks for {option.symbol} at strike {strike}", color="yellow")
                continue
            delta = greeks.get("delta")
            strike_deltas[float(strike)] = delta
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "[OptionsHelper] strike delta symbol=%s expiry=%s right=%s strike=%s delta=%s",
                    getattr(underlying_asset, "symbol", None),
                    expiry,
                    right,
                    strike,
                    delta,
                )
            if stop_greater_than is not None and delta is not None and delta >= stop_greater_than:
                break
            if stop_less_than is not None and delta is not None and delta <= stop_less_than:
                break
        return strike_deltas

    def get_delta_for_strike(self, underlying_asset: Asset, underlying_price: float,
                             strike: float, expiry: date, right: str) -> Optional[float]:
        """
        Retrieve the delta for an option with a specific strike.

        Parameters
        ----------
        underlying_asset : Asset
            The underlying asset.
        underlying_price : float
            Current underlying price.
        strike : float
            The strike price.
        expiry : date
            The option's expiry date.
        right : str
            "call" or "put".

        Returns
        -------
        Optional[float]
            The computed delta or None if unavailable.
        """
        self._reset_per_bar_caches_if_needed()

        option = self._option_contract(underlying_asset, expiry, strike, right)

        cache_underlying: Optional[float]
        try:
            cache_underlying = float(underlying_price) if underlying_price is not None else None
        except Exception:
            cache_underlying = None

        delta_cache_key = (option, cache_underlying)
        if delta_cache_key in self._per_bar_delta_cache:
            return self._per_bar_delta_cache[delta_cache_key]

        def _coerce_price(value: Any) -> Optional[float]:
            try:
                numeric = float(value)
            except (TypeError, ValueError):
                return None
            if not math.isfinite(numeric) or numeric <= 0:
                return None
            return numeric

        option_price, _, _ = self._get_option_mark_from_quote(option, snapshot=True)
        if option_price is None:
            try:
                option_price = _coerce_price(self.strategy.get_last_price(option))
            except Exception:
                option_price = None

        if option_price is None:
            self._per_bar_delta_cache[delta_cache_key] = None
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "[OptionsHelper] delta unavailable (missing price) option=%s expiry=%s strike=%s right=%s",
                    option.symbol,
                    option.expiration,
                    option.strike,
                    option.right,
                )
            return None

        greeks_cache_key = (option, cache_underlying, option_price)
        greeks = self._per_bar_greeks_cache.get(greeks_cache_key)
        if greeks is None and greeks_cache_key not in self._per_bar_greeks_cache:
            try:
                greeks = self.strategy.get_greeks(option, underlying_price=underlying_price, asset_price=option_price)
            except TypeError:
                # Some unit tests (and custom strategy stubs) mock get_greeks without the asset_price kwarg.
                greeks = self.strategy.get_greeks(option, underlying_price=underlying_price)
            self._per_bar_greeks_cache[greeks_cache_key] = greeks
        # Handle None from get_greeks - can happen when option price or underlying price unavailable
        if greeks is None:
            self._per_bar_delta_cache[delta_cache_key] = None
            self.strategy.log_message(
                f"Could not calculate Greeks for {option.symbol} at strike {strike} (greeks returned None)",
                color="yellow",
            )
            return None
        delta = greeks.get("delta")
        self._per_bar_delta_cache[delta_cache_key] = delta

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "[OptionsHelper] delta option=%s expiry=%s strike=%s right=%s underlying_price=%s option_price=%s delta=%s",
                option.symbol,
                option.expiration,
                option.strike,
                option.right,
                underlying_price,
                option_price,
                delta,
            )
        return delta

    def _is_theta_option_backtest(self) -> bool:
        """Return True when option pricing is routed through ThetaData in a backtest."""
        broker = getattr(self.strategy, "broker", None)
        if broker is None or getattr(broker, "IS_BACKTESTING_BROKER", False) is not True:
            return False

        option_source = getattr(broker, "option_source", None)
        data_source = option_source if option_source is not None else getattr(broker, "data_source", None)
        if data_source is None:
            return False

        source_name = data_source.__class__.__name__
        if source_name == "ThetaDataBacktestingPandas":
            is_theta_option_provider = True
        elif source_name == "RoutedBacktestingPandas":
            routing = getattr(data_source, "_routing", {}) or {}
            raw_option_provider = routing.get("option", routing.get("default", "thetadata"))
            option_provider = str(raw_option_provider).strip().replace("-", "").replace("_", "").lower()
            is_theta_option_provider = option_provider in {"thetadata", "theta"}
        else:
            is_theta_option_provider = False

        if not is_theta_option_provider:
            return False
        return True

    def _is_theta_option_daily_backtest(self) -> bool:
        """Return True when option pricing is routed through ThetaData in a daily backtest cadence."""
        if not self._is_theta_option_backtest():
            return False

        broker = getattr(self.strategy, "broker", None)
        option_source = getattr(broker, "option_source", None)
        data_source = option_source if option_source is not None else getattr(broker, "data_source", None)

        is_daily_cadence = False
        try:
            sleeptime = getattr(self.strategy, "sleeptime", None)
            if isinstance(sleeptime, str) and sleeptime.strip().upper().endswith("D"):
                is_daily_cadence = True
        except Exception:
            pass
        try:
            if getattr(data_source, "_timestep", None) == "day":
                is_daily_cadence = True
        except Exception:
            pass
        return is_daily_cadence

    def _normalize_target_delta_for_right(self, target_delta: float, option_type: str) -> float:
        """Accept absolute-style user inputs while preserving signed-delta internals."""
        normalized = float(target_delta)
        if option_type == "PUT" and normalized > 0:
            self.strategy.log_message(
                f"⚠️ Received positive put delta {normalized:.4f}; interpreting as {-normalized:.4f}.",
                color="yellow",
            )
            return -normalized
        if option_type == "CALL" and normalized < 0:
            self.strategy.log_message(
                f"⚠️ Received negative call delta {normalized:.4f}; interpreting as {abs(normalized):.4f}.",
                color="yellow",
            )
            return abs(normalized)
        return normalized

    @staticmethod
    def _model_delta_for_strike(
        *,
        underlying_price: float,
        strike: float,
        t_years: float,
        sigma: float,
        is_call: bool,
    ) -> Optional[float]:
        """Cheap Black-Scholes-style delta approximation used to avoid expensive strike probes."""
        if underlying_price <= 0 or strike <= 0 or t_years <= 0 or sigma <= 0:
            return None
        try:
            sig_sqrt_t = sigma * math.sqrt(t_years)
            if sig_sqrt_t <= 0:
                return None
            d1 = (math.log(underlying_price / strike) + 0.5 * sigma * sigma * t_years) / sig_sqrt_t
            call_delta = NormalDist().cdf(d1)
            return call_delta if is_call else (call_delta - 1.0)
        except Exception:
            return None

    def _select_strike_by_model_delta(
        self,
        *,
        underlying_asset: Asset,
        underlying_price: float,
        expiry: date,
        option_type: str,
        target_delta: float,
        candidate_strikes: List[float],
        as_of: Optional[date],
    ) -> Optional[float]:
        """Pick the chain strike closest to target delta using a model-only approximation."""
        if not candidate_strikes:
            return None

        as_of_date = as_of or date.today()
        days_to_expiry = (expiry - as_of_date).days
        t_years = max(1.0 / 365.0, float(days_to_expiry) / 365.0)
        is_call = option_type == "CALL"

        try:
            is_index_like = self._is_index_like_underlying(underlying_asset, getattr(underlying_asset, "symbol", None))
        except Exception:
            is_index_like = False
        sigma = 0.25 if is_index_like else 0.35

        best_strike: Optional[float] = None
        best_delta: Optional[float] = None
        best_diff: Optional[float] = None

        for strike in candidate_strikes:
            delta = self._model_delta_for_strike(
                underlying_price=float(underlying_price),
                strike=float(strike),
                t_years=t_years,
                sigma=sigma,
                is_call=is_call,
            )
            if delta is None:
                continue
            diff = abs(float(delta) - float(target_delta))
            if best_diff is None or diff < best_diff:
                best_diff = diff
                best_delta = float(delta)
                best_strike = float(strike)

        if best_strike is not None:
            self.strategy.log_message(
                f"⚡ Model strike pick: {best_strike:g} (approx delta={best_delta:.4f}, target={target_delta:.4f})",
                color="blue",
            )
        return best_strike

    # Valid strikes past the target, each farther from it than the best so far, that close one
    # side of the walk. Delta falls as the strike rises, so on a well-behaved chain one is
    # enough. Stale prints at the open can break that order for a strike or two: with 2, two
    # of 780 picks on a week of real SPY prices missed the closest strike; with 3, none did.
    _DELTA_WALK_CONFIRM = 3

    def _delta_walk_start(self, underlying_asset: Asset, underlying_price: float, expiry: date,
                          is_call: bool, target_delta: float, strikes: List[float]) -> int:
        """Index of the listed strike nearest a Black-Scholes estimate for the target delta."""
        estimate: Optional[float] = None
        try:
            call_delta = target_delta if is_call else target_delta + 1.0
            if 0.01 < call_delta < 0.99:
                as_of = None
                try:
                    now = self.strategy.get_datetime()
                    as_of = now.date() if isinstance(now, datetime) else now if isinstance(now, date) else None
                except Exception:
                    as_of = None
                as_of = as_of or date.today()
                t_years = max(1.0 / 365.0, float((expiry - as_of).days) / 365.0)
                try:
                    index_like = self._is_index_like_underlying(underlying_asset, getattr(underlying_asset, "symbol", None))
                except Exception:
                    index_like = False
                sigma = 0.25 if index_like else 0.35
                d1 = NormalDist().inv_cdf(call_delta)
                sig_sqrt_t = sigma * math.sqrt(t_years)
                if sig_sqrt_t > 0:
                    estimate = underlying_price / math.exp(d1 * sig_sqrt_t - 0.5 * sigma * sigma * t_years)
        except Exception:
            estimate = None
        if estimate is None:
            estimate = underlying_price
        at = min(max(bisect_left(strikes, estimate), 0), len(strikes) - 1)
        if at > 0 and abs(strikes[at - 1] - estimate) <= abs(strikes[at] - estimate):
            at -= 1
        return at

    def _closest_delta_strike(self, underlying_asset: Asset, underlying_price: float, expiry: date,
                              right: str, strikes: List[float], target_delta: float,
                              loud: bool = False) -> Tuple[Optional[float], Optional[float]]:
        """The strike whose delta is closest to ``target_delta``, pricing as few strikes as possible.

        Starts at the strike nearest a closed-form estimate and walks outward on both sides.
        Delta falls as the strike rises, so once a side has passed the target and its deltas
        are moving away (``_DELTA_WALK_CONFIRM`` valid strikes in a row farther than the best),
        no strike beyond can be closer and that side stops. Gives the same strike as checking
        every listed strike whenever delta is monotonic over the chain, with far fewer quotes.
        """
        if not strikes:
            return None, None
        is_call = str(right).upper().startswith("C")
        # One batched load for all candidates where the data source offers it (backtest bars,
        # live latest quotes); the walk below then reads them from memory.
        self._prefetch_option_contracts(underlying_asset, expiry, right, strikes)
        self._prefetch_option_quotes(underlying_asset, expiry, right, strikes)
        try:
            return self._walk_strikes(underlying_asset, underlying_price, expiry, right, strikes,
                                      target_delta, loud, is_call)
        finally:
            self._end_quote_batch()

    def _walk_strikes(self, underlying_asset, underlying_price, expiry, right, strikes, target_delta,
                      loud, is_call) -> Tuple[Optional[float], Optional[float]]:
        start = self._delta_walk_start(underlying_asset, underlying_price, expiry, is_call, target_delta, strikes)
        self._detail(loud,
            f"🔍 Trying strikes from {strikes[start]:g} outward ({len(strikes)} listed "
            f"{strikes[0]:g}-{strikes[-1]:g}; the closest delta wins)",
            color="blue",
        )
        best_key = None
        best: Tuple[Optional[float], Optional[float]] = (None, None)

        def visit(idx: int) -> Optional[float]:
            nonlocal best_key, best
            strike = strikes[idx]
            delta = self.get_delta_for_strike(underlying_asset, underlying_price, strike, expiry, right)
            try:
                delta = float(delta) if delta is not None else None
            except (TypeError, ValueError):
                delta = None
            if delta is None or not math.isfinite(delta):
                return None
            key = (abs(delta - target_delta), abs(delta), float(strike))
            if best_key is None or key < best_key:
                best_key = key
                best = (float(strike), delta)
            return delta

        visit(start)
        # Lower strikes have higher deltas: that side is past the target once delta > target.
        for step, beyond in ((-1, lambda d: d > target_delta), (1, lambda d: d < target_delta)):
            confirmed = 0
            idx = start + step
            while 0 <= idx < len(strikes) and confirmed < self._DELTA_WALK_CONFIRM:
                delta = visit(idx)
                if delta is not None:
                    if beyond(delta) and best_key is not None and abs(delta - target_delta) > best_key[0]:
                        confirmed += 1
                    else:
                        confirmed = 0
                idx += step
        return best

    def find_strike_for_delta(
        self,
        underlying_asset: Asset,
        underlying_price: float,
        target_delta: float,
        expiry: date,
        right: str,
        chains: Optional[Any] = None,
    ) -> Optional[float]:
        """
        Find the listed strike whose delta is closest to the target delta.

        Every candidate strike with a real price is checked (strikes from $20 below to $30
        above the underlying price). Ties go to the smaller absolute delta (further out of
        the money), then to the lower strike. Strikes with no price are skipped. Live trading
        and backtests use the same rule. The pick is remembered for the current bar only.

        Parameters
        ----------
        underlying_asset : Asset
            The underlying asset.
        underlying_price : float
            Current (rounded) price of the underlying.
        target_delta : float
            Desired target delta (positive for calls, negative for puts).
        expiry : date
            The option expiry date.
        right : str
            "call" or "put".

        Returns
        -------
        Optional[float]
            The strike price that best matches the target delta, or None.
        """
        delta_log_key = (getattr(underlying_asset, "symbol", underlying_asset), expiry, str(right).upper(), target_delta)
        search_loud = self._first_time("delta_strike_search", delta_log_key)
        self._detail(
            search_loud,
            f"🎯 STRIKE SEARCH: Finding strike for {underlying_asset.symbol} "
            f"(underlying_price=${underlying_price}, target_delta={target_delta}, right={right}, expiry={expiry})", 
            color="blue"
        )
        
        # Validate input parameters
        if underlying_price <= 0:
            self.strategy.log_message(f"❌ ERROR: Invalid underlying price {underlying_price}", color="red")
            return None
            
        if target_delta is None:
            self.strategy.log_message("❌ ERROR: target_delta is None", color="red")
            return None
            
        if abs(target_delta) > 1:
            self.strategy.log_message(f"❌ ERROR: Invalid target delta {target_delta} (should be between -1 and 1)", color="red")
            return None

        strike_min = max(1.0, float(underlying_price) - 20.0)
        strike_max = float(underlying_price) + 30.0

        # Prefer the actual strikes from the option chain to avoid querying non-existent strikes
        # (e.g., contracts that only list strikes every $5.00).
        candidate_strikes: List[float] = []
        chains_data = chains
        if chains_data is None:
            try:
                max_expiration_hint = self._default_chain_max_expiration_date(
                    underlying_asset=underlying_asset,
                    min_expiration_date=expiry,
                )
                with self._chain_hint(expiry, max_expiration_hint):
                    chains_data = self.strategy.get_chains(underlying_asset)
            except Exception:
                chains_data = None

        option_type = str(right).upper()
        if option_type.startswith("C"):
            option_type = "CALL"
        elif option_type.startswith("P"):
            option_type = "PUT"

        normalized_target_delta = self._normalize_target_delta_for_right(float(target_delta), option_type)

        if chains_data:
            strikes_raw = []
            try:
                strikes_raw = chains_data.strikes(expiry, option_type)
            except Exception:
                if isinstance(chains_data, dict):
                    strike_map = chains_data.get("Chains", {}).get(option_type, {})
                    strikes_raw = strike_map.get(expiry.strftime("%Y-%m-%d"), []) if strike_map else []

            if not isinstance(strikes_raw, (list, tuple, set)):
                strikes_raw = []

            for value in strikes_raw or []:
                try:
                    numeric = float(value)
                except (TypeError, ValueError):
                    continue
                if numeric > 0:
                    candidate_strikes.append(numeric)

        candidate_strikes = sorted(set(candidate_strikes))
        if candidate_strikes:
            filtered = [s for s in candidate_strikes if strike_min <= s <= strike_max]
            if filtered:
                candidate_strikes = filtered
        else:
            # Fallback for data sources that don't provide chains: preserve original behavior.
            candidate_strikes = [float(s) for s in range(int(strike_min), int(strike_max) + 1)]

        self._detail(search_loud,
            f"🔍 Search range: strikes {strike_min:.2f} to {strike_max:.2f} (underlying=${underlying_price})",
            color="blue",
        )
        self._detail(search_loud,
            f"🔍 Search strikes: {len(candidate_strikes)} candidates (range {strike_min:.2f}-{strike_max:.2f})",
            color="blue",
        )

        cache_date: Optional[date] = None
        now = None
        try:
            now = self.strategy.get_datetime()
            if isinstance(now, datetime):
                cache_date = now.date()
            elif isinstance(now, date):
                cache_date = now
        except Exception:
            cache_date = None

        strike_cache = getattr(self, "_strike_for_delta_cache", None)
        if not isinstance(strike_cache, dict) or getattr(self, "_strike_for_delta_cache_dt", None) != now:
            strike_cache = {}
            self._strike_for_delta_cache = strike_cache
            self._strike_for_delta_cache_dt = now

        # Remembered for the current bar only (exact time and underlying price): a later
        # minute has different prices and time to expiry, so it is a new search.
        cache_key = None
        if now is not None:
            cache_key = (
                getattr(underlying_asset, "symbol", str(underlying_asset)),
                now,
                expiry,
                option_type,
                float(normalized_target_delta),
                float(underlying_price),
            )
            cached_strike = strike_cache.get(cache_key)
            if cached_strike is not None and cached_strike in candidate_strikes:
                return float(cached_strike)

        is_theta_option_backtest = self._is_theta_option_backtest()

        if is_theta_option_backtest and candidate_strikes:
            # ThetaData option backtests can turn live-style delta probing into many minute
            # quote/OHLC downloads for nearby strikes before the strategy has selected a
            # contract. Use the model-based selector for the strike target, then let
            # find_next_valid_option/evaluate_option_market validate real historical prices.
            modeled = self._select_strike_by_model_delta(
                underlying_asset=underlying_asset,
                underlying_price=float(underlying_price),
                expiry=expiry,
                option_type=option_type,
                target_delta=float(normalized_target_delta),
                candidate_strikes=candidate_strikes,
                as_of=cache_date,
            )
            if modeled is not None:
                if cache_key is not None:
                    strike_cache[cache_key] = float(modeled)
                return float(modeled)

        # One rule for live trading and backtests: the closest delta among the listed strikes
        # that have a real price. Ties go to the smaller absolute delta (further out of the
        # money), then to the lower strike. Strikes with no price are skipped, never estimated.
        best_strike, best_delta = self._closest_delta_strike(
            underlying_asset, float(underlying_price), expiry, right, candidate_strikes,
            float(normalized_target_delta), search_loud,
        )

        if best_strike is None:
            self.strategy.log_message(f"❌ No valid strike found for target delta {normalized_target_delta}", color="red")
            return None

        self._detail(
            self._first_time("delta_strike_result", delta_log_key, best_strike),
            f"✅ RESULT: Closest strike {best_strike:g} with delta {best_delta:.4f} (target was {normalized_target_delta})",
            color="green",
        )
        if cache_key is not None:
            strike_cache[cache_key] = float(best_strike)
        if underlying_price > 50 and best_strike < 10:
            self.strategy.log_message(
                f"⚠️  WARNING: Strike {best_strike:g} seems too low for underlying price ${underlying_price}. "
                f"This might indicate a data issue.",
                color="red",
            )

        return best_strike

    def calculate_multileg_limit_price(
        self, orders: List[Order], limit_type: str, *, price_details: Optional[List[Dict[str, Any]]] = None
    ) -> Optional[float]:
        """
        Calculate an aggregate limit price for a multi-leg order by combining quotes from each leg.

        Parameters
        ----------
        orders : List[Order]
            List of orders (each order has an Asset).
        limit_type : str
            One of "best", "fastest", or "mid" indicating which price to use.
        price_details : list, optional
            Receives pricing inputs in leg order, from the same quote reads.
            Cleared first; remains empty if the complete package cannot be priced.

        Returns
        -------
        Optional[float]
            Net price per reduced package, or None if quotes are missing.
            For example, six long and three short contracts price one 2:1
            package; three such packages preserve the requested quantities.
        """
        if limit_type not in {"best", "mid", "fastest"}:
            raise ValueError("limit_type must be best, mid, or fastest.")
        if price_details is not None:
            price_details.clear()
        self.strategy.log_message("Calculating multi-leg limit price.", color="blue")
        quotes: List[float] = []
        details = []
        if not orders:
            return None
        _, ratios = get_multileg_quantity_units(order.quantity for order in orders)
        for order, ratio in zip(orders, ratios):
            asset = order.asset
            if asset.asset_type != Asset.AssetType.OPTION:
                # A partial package price can reverse debit/credit economics.
                # Never omit a leg, including unsupported mixed-asset packages.
                return None
            try:
                quote = self.strategy.get_quote(asset)
            except Exception as e:
                self.strategy.log_message(f"Error fetching quote for {asset.symbol}: {e}", color="red")
                return None
            if not quote or quote.ask is None or quote.bid is None:
                self.strategy.log_message(f"Missing quote for {asset.symbol}", color="red")
                return None
            try:
                bid, ask = float(quote.bid), float(quote.ask)
                valid_quote = math.isfinite(bid) and math.isfinite(ask) and 0 <= bid <= ask
            except (TypeError, ValueError, OverflowError):
                valid_quote = False
            if not valid_quote:
                self.strategy.log_message(f"Invalid bid/ask for {asset.symbol}; package remains unpriced.", color="red")
                return None
            mid = (ask + bid) / 2
            if limit_type == "mid":
                quotes.append(mid if order.is_buy_order() else -mid)
            elif limit_type == "best":
                quotes.append(bid if order.is_buy_order() else -ask)
            elif limit_type == "fastest":
                quotes.append(ask if order.is_buy_order() else -bid)
            leg_price = abs(quotes[-1])
            quotes[-1] *= ratio
            if price_details is not None:
                details.append({
                    "price_basis": "bid_ask", "bid": bid, "ask": ask,
                    "spread_pct": (ask - bid) / mid if math.isfinite(mid) and mid > 0 else None,
                    "price": leg_price, "ratio": ratio, "signed_price": quotes[-1],
                })
        if not quotes:
            self.strategy.log_message("No valid quotes for calculating limit price.", color="red")
            return None
        limit_price = sum(quotes)
        if price_details is not None:
            price_details.extend(details)
        self.strategy.log_message(f"Calculated limit price: {limit_price}", color="green")
        return limit_price

    def evaluate_option_market(
        self,
        option_asset: Asset,
        max_spread_pct: Optional[float] = None,
        *,
        quote: Optional[Quote] = None,
    ) -> OptionMarketEvaluation:
        """Evaluate available quote data for an option and produce execution anchors.

        Parameters
        ----------
        option_asset : Asset
            The option to evaluate.
        max_spread_pct : float, optional
            Maximum acceptable bid/ask spread as a fraction (e.g. 0.25 for 25%).
        quote : Quote, optional
            An already acquired quote for this exact option. Applies the same
            price checks without additional market-data reads or the helper's
            per-datetime cache. The caller owns snapshot freshness and historical
            availability. Missing prices remain non-actionable; no last-price
            network fallback is performed for an explicit snapshot.

        Returns
        -------
        OptionMarketEvaluation
            Dataclass containing quote fields, derived spread information, and
            suggested buy/sell prices (with automatic fallback when the data
            source allows it).
        """

        # PERF: Option strategies often evaluate the same contract multiple times within a single
        # strategy datetime (MTM, liquidity checks, multi-leg pricing). In backtesting, quotes are
        # immutable per bar, so cache evaluations for the current datetime.
        supplied_quote = quote is not None
        if supplied_quote and (not isinstance(quote, Quote) or quote.asset != option_asset):
            raise ValueError("The supplied quote must describe the same asset")
        current_dt = None
        try:
            current_dt = self.strategy.get_datetime()
        except Exception:
            current_dt = None

        cache_dt = getattr(self, "_option_market_eval_cache_dt", None)
        if cache_dt != current_dt:
            self._option_market_eval_cache_dt = current_dt
            self._option_market_eval_cache = {}

        cache_key = (option_asset, max_spread_pct)
        cache = getattr(self, "_option_market_eval_cache", {})
        if not supplied_quote and cache_key in cache:
            return cache[cache_key]

        # PERF: `log_message()` respects BACKTESTING_QUIET_LOGS, but callers still pay the cost
        # of building large f-strings. Gate string construction in this hot path.
        should_log_info = False
        try:
            should_log_info = bool(getattr(self.strategy, "logger", None) and self.strategy.logger.isEnabledFor(logging.INFO))
        except Exception:
            should_log_info = False

        data_source = getattr(getattr(self.strategy, "broker", None), "data_source", None)
        allow_fallback = not supplied_quote and bool(getattr(data_source, "option_quote_fallback_allowed", False))

        bid: Optional[float] = None
        ask: Optional[float] = None
        last_price: Optional[float] = None
        spread_pct: Optional[float] = None
        has_bid_ask = False
        spread_too_wide = False
        missing_bid_ask = False
        missing_last_price = False
        used_last_price_fallback = False
        buy_price: Optional[float] = None
        sell_price: Optional[float] = None

        data_quality_flags: List[str] = []
        sanitization_notes: List[str] = []

        broker = getattr(self.strategy, "broker", None)
        data_source = getattr(broker, "data_source", None) if broker is not None else None
        is_theta_backtest = (
            broker is not None
            and (
                getattr(broker, "IS_BACKTESTING_BROKER", False) is True
                or getattr(data_source, "IS_BACKTESTING_DATA_SOURCE", False) is True
            )
            and data_source is not None
            and data_source.__class__.__name__ == "ThetaDataBacktestingPandas"
        )

        is_daily_cadence = False
        if is_theta_backtest:
            try:
                sleeptime = getattr(self.strategy, "sleeptime", None)
                if isinstance(sleeptime, str) and sleeptime.strip().upper().endswith("D"):
                    is_daily_cadence = True
            except Exception:
                pass
            try:
                if getattr(data_source, "_timestep", None) == "day":
                    is_daily_cadence = True
            except Exception:
                pass

        # PERF/CORRECTNESS: In ThetaData daily-cadence backtests, calling Strategy.get_quote() can
        # trigger expensive day/EOD quote fetches for options, only to be immediately overridden by
        # the intraday snapshot NBBO (which is the correct execution anchor).
        #
        # Prefer snapshot NBBO first. Only fall back to the regular quote path when the snapshot is
        # missing so we avoid unnecessary downloader/cache churn.
        if not supplied_quote and getattr(option_asset, "asset_type", None) == Asset.AssetType.OPTION and is_theta_backtest and is_daily_cadence:
            _, snap_bid, snap_ask = self._get_option_mark_from_quote(option_asset, snapshot=True)
            if snap_bid is not None and snap_ask is not None:
                bid = snap_bid
                ask = snap_ask
                # Backwards-compatible flag name used by tests/docs.
                data_quality_flags.append("snapshot_nbbo_override")
            else:
                try:
                    quote = self.strategy.get_quote(option_asset)
                except Exception as exc:
                    if should_log_info:
                        self.strategy.log_message(
                            f"Error fetching quote for {option_asset}: {exc}",
                            color="red",
                        )
                if quote is not None:
                    if getattr(quote, "bid", None) is not None:
                        bid = self._coerce_price(getattr(quote, "bid", None), "bid", data_quality_flags, sanitization_notes)
                    if getattr(quote, "ask", None) is not None:
                        ask = self._coerce_price(getattr(quote, "ask", None), "ask", data_quality_flags, sanitization_notes)
                    if (bid is None or ask is None) and (snap_bid is not None or snap_ask is not None):
                        data_quality_flags.append("snapshot_nbbo_partial")
                        if bid is None and snap_bid is not None:
                            bid = snap_bid
                        if ask is None and snap_ask is not None:
                            ask = snap_ask
        else:
            # Attempt to get quotes first (default behavior)
            if not supplied_quote:
                try:
                    quote = self.strategy.get_quote(option_asset)
                except Exception as exc:
                    if should_log_info:
                        self.strategy.log_message(
                            f"Error fetching quote for {option_asset}: {exc}",
                            color="red",
                        )

            if quote is not None:
                if getattr(quote, "bid", None) is not None:
                    bid = self._coerce_price(getattr(quote, "bid", None), "bid", data_quality_flags, sanitization_notes)
                if getattr(quote, "ask", None) is not None:
                    ask = self._coerce_price(getattr(quote, "ask", None), "ask", data_quality_flags, sanitization_notes)

            if not supplied_quote and getattr(option_asset, "asset_type", None) == Asset.AssetType.OPTION and is_theta_backtest:
                if (bid is None or ask is None):
                    # Prefer actionable NBBO from a point-in-time snapshot when bar-aligned quotes omit
                    # bid/ask (common in historical option backtests). Only fall back to `quote.price`
                    # or last-trade pricing when NBBO is unavailable.
                    _, snap_bid, snap_ask = self._get_option_mark_from_quote(option_asset, snapshot=True)
                    if snap_bid is not None and snap_ask is not None:
                        bid = snap_bid
                        ask = snap_ask
                    else:
                        if bid is None and snap_bid is not None:
                            bid = snap_bid
                        if ask is None and snap_ask is not None:
                            ask = snap_ask

        has_bid_ask = bid is not None and ask is not None

        if has_bid_ask and bid is not None and ask is not None:
            buy_price = ask
            sell_price = bid
            mid = (ask + bid) / 2
            if not math.isfinite(mid) or mid <= 0:
                spread_pct = None
            else:
                spread_pct = (ask - bid) / mid
                if max_spread_pct is not None:
                    spread_too_wide = spread_pct > max_spread_pct
        else:
            missing_bid_ask = True
            # One-sided quotes can still be useful as execution anchors and should not force an
            # expensive last-trade fetch.
            if ask is not None:
                buy_price = ask
            if bid is not None:
                sell_price = bid

        # Last price as secondary signal / fallback anchor.
        #
        # PERFORMANCE: Do not fetch trade-derived OHLC/last when bid/ask is already actionable.
        # ThetaData can provide NBBO without trades, and calling get_last_price() can trigger
        # expensive historical OHLC downloads (especially in backtests).
        if buy_price is None and sell_price is None:
            # First try to use the quote's price field. This is free (no extra downloader calls)
            # and is often populated even when bid/ask is absent (e.g., daily OHLC-based quotes).
            quote_price = None
            if quote is not None:
                quote_price = getattr(quote, "price", None)
            if quote_price is not None:
                last_price = self._coerce_price(quote_price, "quote_price", data_quality_flags, sanitization_notes)

            # As a last resort, allow data-source-specific fallback to trade-only last price.
            if last_price is None and allow_fallback:
                try:
                    last_price = self.strategy.get_last_price(option_asset)
                except Exception as exc:
                    if should_log_info:
                        self.strategy.log_message(
                            f"Error fetching last price for {option_asset}: {exc}",
                            color="red",
                        )

                if last_price is not None:
                    last_price = self._coerce_price(last_price, "last_price", data_quality_flags, sanitization_notes)

            if last_price is None:
                missing_last_price = True

        if last_price is not None and buy_price is None and sell_price is None:
            buy_price = last_price
            sell_price = last_price
            used_last_price_fallback = True
            if should_log_info:
                # Once per contract: data sources with trade bars only (no bid/ask) would
                # otherwise repeat this on every evaluation.
                self._detail(
                    self._first_time("last_price_fallback", option_asset),
                    f"Using last-price fallback for {option_asset} due to missing bid/ask quotes.",
                    color="yellow",
                )
        elif not has_bid_ask and allow_fallback and last_price is None:
            data_quality_flags.append("last_price_unusable")

        if buy_price is not None and (not math.isfinite(buy_price) or buy_price <= 0):
            sanitization_notes.append(f"buy_price {buy_price!r} is not actionable; clearing.")
            data_quality_flags.append("buy_price_non_finite")
            buy_price = None
            sell_price = None

        if should_log_info:
            spread_str = f"{spread_pct:.2%}" if spread_pct is not None else "None"
            max_spread_str = f"{max_spread_pct:.2%}" if max_spread_pct is not None else "None"
            log_color = "red" if spread_too_wide else (
                "yellow" if (missing_bid_ask or missing_last_price or used_last_price_fallback) else "blue"
            )
            if sanitization_notes:
                note_summary = "; ".join(sanitization_notes)
                self.strategy.log_message(
                    f"Option data sanitization for {option_asset}: {note_summary}",
                    color="yellow",
                )
            # INFO when the evaluation blocks a trade (spread too wide, no usable price) and the
            # first time or when the outcome changes for a contract; DEBUG for repeats.
            not_actionable = spread_too_wide or buy_price is None or sell_price is None
            evaluation_outcome = (
                missing_bid_ask,
                missing_last_price,
                spread_too_wide,
                used_last_price_fallback,
                tuple(data_quality_flags),
            )
            self._detail(
                not_actionable or self._first_time("option_market_evaluation", option_asset, evaluation_outcome),
                (
                    f"Option market evaluation for {option_asset}: "
                    f"bid={bid}, ask={ask}, last={last_price}, spread={spread_str}, "
                    f"max_spread={max_spread_str}, missing_bid_ask={missing_bid_ask}, "
                    f"missing_last_price={missing_last_price}, spread_too_wide={spread_too_wide}, "
                    f"used_last_price_fallback={used_last_price_fallback}, "
                    f"buy_price={buy_price}, sell_price={sell_price}, "
                    f"data_quality_flags={data_quality_flags}"
                ),
                color=log_color,
            )

        evaluation = OptionMarketEvaluation(
            bid=bid,
            ask=ask,
            last_price=last_price,
            spread_pct=spread_pct,
            has_bid_ask=has_bid_ask,
            spread_too_wide=spread_too_wide,
            missing_bid_ask=missing_bid_ask,
            missing_last_price=missing_last_price,
            buy_price=buy_price,
            sell_price=sell_price,
            used_last_price_fallback=used_last_price_fallback,
            max_spread_pct=max_spread_pct,
            data_quality_flags=data_quality_flags,
        )

        if not supplied_quote:
            try:
                self._option_market_eval_cache[cache_key] = evaluation
            except Exception:
                pass

        return evaluation

    def check_option_liquidity(self, option_asset: Asset, max_spread_pct: float) -> bool:
        """
        Check if an option's bid-ask spread is within an acceptable threshold.

        Parameters
        ----------
        option_asset : Asset
            The option asset to check.
        max_spread_pct : float
            Maximum allowed spread as a fraction (e.g. 0.15 for 15%).

        Returns
        -------
        bool
            True if the option is sufficiently liquid; False otherwise.
        """
        if not self._liquidity_deprecation_warned:
            warnings.warn(
                "OptionsHelper.check_option_liquidity is deprecated. "
                "Use OptionsHelper.evaluate_option_market instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            self._liquidity_deprecation_warned = True

        evaluation = self.evaluate_option_market(
            option_asset=option_asset,
            max_spread_pct=max_spread_pct,
        )

        return evaluation.has_bid_ask and not evaluation.spread_too_wide

    def get_order_details(self, order: Order) -> Dict[str, Optional[Union[str, float, date]]]:
        """
        Return a summary of key details of an order for logging and debugging.

        Parameters
        ----------
        order : Order
            The order to summarize.

        Returns
        -------
        Dict[str, Optional[Union[str, float, date]]]
            A dictionary containing symbol, strike, expiration, right, side, and last price.
        """
        asset = order.asset
        mark_price = None
        bid = None
        ask = None
        if getattr(asset, "asset_type", None) == Asset.AssetType.OPTION:
            mark_price, bid, ask = self._get_option_mark_from_quote(asset)
        details = {
            "symbol": asset.symbol,
            "strike": getattr(asset, "strike", None),
            "expiration": getattr(asset, "expiration", None),
            "right": getattr(asset, "right", None),
            "side": order.side,
            "last_price": self.strategy.get_last_price(asset) if mark_price is None else None,
            "bid": bid,
            "ask": ask,
            "mark_price": mark_price,
        }
        self.strategy.log_message(f"Order details: {details}", color="blue")
        return details

    def _default_chain_max_expiration_date(
        self,
        *,
        underlying_asset: Optional[Asset],
        min_expiration_date: date,
    ) -> date:
        """Return a conservative max-expiration hint to avoid chain-scan storms.

        The ThetaData chain builder fetches strike lists per expiration. For index underlyings
        (SPX/SPXW/etc) that can mean *daily* expirations, so a wide window can generate thousands
        of strike-list requests during a single backtest day.
        """

        try:
            symbol = getattr(underlying_asset, "symbol", None) if underlying_asset is not None else None
        except Exception:
            symbol = None

        is_index_like = self._is_index_like_underlying(underlying_asset, symbol)
        days_out = 45 if is_index_like else 60
        return min_expiration_date + timedelta(days=days_out)

    def _chain_hint(self, min_expiration_date: date, max_expiration_date: Optional[date] = None):
        """Temporarily set chain constraints on the underlying data source."""
        broker = getattr(self.strategy, "broker", None)
        data_source = getattr(broker, "data_source", None) if broker else None

        class _ChainHintContext:
            def __init__(self, ds, min_dt):
                self.ds = ds
                self.min_dt = min_dt
                self.max_dt = max_expiration_date
                self.prev = None

            def __enter__(self):
                if not self.ds:
                    return
                self.prev = getattr(self.ds, "_chain_constraints", None)
                constraints = {"min_expiration_date": self.min_dt}
                if self.max_dt is not None:
                    constraints["max_expiration_date"] = self.max_dt
                self.ds._chain_constraints = constraints

            def __exit__(self, exc_type, exc_val, exc_tb):
                if not self.ds:
                    return
                if self.prev is None:
                    if hasattr(self.ds, "_chain_constraints"):
                        delattr(self.ds, "_chain_constraints")
                else:
                    self.ds._chain_constraints = self.prev

        return _ChainHintContext(data_source, min_expiration_date)

    @staticmethod
    def _is_index_like_underlying(underlying_asset: Optional[Asset], symbol: Optional[str]) -> bool:
        """Return True when the underlying behaves like an index for option-liquidity heuristics.

        Some strategies represent indices as plain stock Assets (asset_type="stock"). We treat a
        small allowlist of known index symbols as "index-like" so intraday backtests can avoid
        excessively expensive validation probes that are unnecessary for highly liquid index options.
        """

        try:
            if underlying_asset is not None and getattr(underlying_asset, "asset_type", None) == Asset.AssetType.INDEX:
                return True
        except Exception:
            pass

        symbol_upper = None
        try:
            symbol_upper = (symbol or getattr(underlying_asset, "symbol", None) or "").upper()
        except Exception:
            symbol_upper = None

        if not symbol_upper:
            return False

        return symbol_upper in {
            "SPX",
            "SPXW",
            "NDX",
            "NDXP",
            "RUT",
            "RUTW",
            "VIX",
            "VIXW",
            "XSP",
            "DJX",
            "OEX",
            "XEO",
        }

    def get_expiration_on_or_after_date(
        self,
        dt: Union[date, datetime],
        chains: Union[Dict[str, Any], Chains],
        call_or_put: str,
        underlying_asset: Optional[Asset] = None,
        allow_prior: bool = False,
    ) -> Optional[date]:
        """
        Get the expiration date that is on or after a given date, validating that the option has tradeable data.

        Parameters
        ----------
        dt : date
            The starting date. Can be a datetime.date or datetime.datetime object.
        chains : dict or Chains
            A dictionary or Chains object containing option chains.
        call_or_put : str
            One of "call" or "put".
        underlying_asset : Asset, optional
            The underlying asset to validate option data. If provided, will verify option has tradeable data.
        allow_prior : bool, optional
            When True, if no valid expiration exists on/after ``dt`` (often because far-dated expirations were not
            listed yet at the backtest date), fall back to the latest valid expiration before ``dt``.

        Returns
        -------
        date
            The adjusted expiration date with valid tradeable data.
        """

        # Handle both datetime.datetime and datetime.date objects
        if isinstance(dt, datetime):
            dt = dt.date()
        elif not isinstance(dt, date):
            raise TypeError(f"dt must be a datetime.date or datetime.datetime object, got {type(dt)}")

        # Current "as-of" date for quote validation in backtests. This is NOT the same as `dt`,
        # which is the target expiration date to search for.
        as_of_date: Optional[date] = None
        try:
            as_of_dt = self.strategy.get_datetime()
            if isinstance(as_of_dt, datetime):
                as_of_date = as_of_dt.date()
            elif isinstance(as_of_dt, date):
                as_of_date = as_of_dt
        except Exception:
            as_of_date = None

        # Make it all caps and get the specific chain.
        call_or_put_caps = call_or_put.upper()

        chains_map = chains if isinstance(chains, dict) else {}
        options_map = chains_map.get("Chains") if isinstance(chains_map.get("Chains"), dict) else None
        if options_map is None:
            self.strategy.log_message(
                f"Option chains unavailable for {call_or_put_caps}; skipping option selection.",
                color="yellow",
            )
            return None

        specific_chain = options_map.get(call_or_put_caps)
        if not isinstance(specific_chain, dict) or not specific_chain:
            self.strategy.log_message(
                f"Option chains lack data for {call_or_put_caps}; skipping option selection.",
                color="yellow",
            )
            return None

        def _try_resolve_expiration(chain_map: Dict[str, Any]) -> List[Tuple[str, date]]:
            expiration_dates: List[Tuple[str, date]] = []
            for expiry_str in chain_map.keys():
                try:
                    from lumibot.entities.chains import _normalise_expiry
                    expiry_date = _normalise_expiry(expiry_str)
                    # Many providers represent option expirations as the OCC "Saturday" date.
                    # LumiBot backtesting expects the last tradable session (Friday). Normalize
                    # weekend expirations to the prior Friday so downstream quote/fill logic works.
                    if expiry_date.weekday() == 5:
                        expiry_date -= timedelta(days=1)
                    elif expiry_date.weekday() == 6:
                        expiry_date -= timedelta(days=2)
                    expiration_dates.append((expiry_str, expiry_date))
                except Exception:
                    continue
            expiration_dates.sort(key=lambda x: x[1])
            return expiration_dates

        # Get underlying symbol for validation
        underlying_symbol = None
        if underlying_asset:
            underlying_symbol = underlying_asset.symbol
        elif hasattr(chains, 'underlying_symbol'):
            underlying_symbol = chains.underlying_symbol
        elif 'UnderlyingSymbol' in chains_map:
            underlying_symbol = chains_map['UnderlyingSymbol']

        # PERF: Intraday strategies often request the "next valid expiration" on every bar, even
        # though the answer is stable for the trading day. Cache per (underlying, date, side, chain
        # fingerprint) so repeated calls avoid re-validating strikes/quotes.
        broker = getattr(self.strategy, "broker", None)
        data_source = getattr(broker, "data_source", None) if broker is not None else None
        is_backtesting = (
            getattr(broker, "IS_BACKTESTING_BROKER", False) is True
            or getattr(data_source, "IS_BACKTESTING_DATA_SOURCE", False) is True
        )

        is_theta_backtest = (
            is_backtesting
            and data_source is not None
            and data_source.__class__.__name__ == "ThetaDataBacktestingPandas"
        )
        is_daily_cadence = False
        if is_backtesting:
            try:
                sleeptime = getattr(self.strategy, "sleeptime", None)
                if isinstance(sleeptime, str) and sleeptime.strip().upper().endswith("D"):
                    is_daily_cadence = True
            except Exception:
                pass
            if getattr(data_source, "_timestep", None) == "day":
                is_daily_cadence = True

        max_validation_checks = 30
        max_strike_probes = 2
        if is_theta_backtest and is_backtesting and not is_daily_cadence:
            # PERF: ThetaData intraday equity backtests can spend most of their wall-clock in
            # expiration validation quote probes (each probe is a remote round-trip). Tighten the
            # validation budget and the number of strikes probed per expiration.
            #
            # This is used only to decide whether an expiration appears tradeable at the current
            # simulation datetime; the actual strike selection still validates actionable NBBO for
            # the specific contracts we plan to trade.
            try:
                if not self._is_index_like_underlying(underlying_asset, underlying_symbol):
                    max_validation_checks = 12
                    max_strike_probes = 1
            except Exception:
                max_validation_checks = 12
                max_strike_probes = 1

        skip_quote_validation = False
        if is_backtesting and underlying_symbol and as_of_date is not None:
            disabled_until = self._expiration_validation_disabled_until.get(underlying_symbol)
            if isinstance(disabled_until, date) and as_of_date < disabled_until:
                # Only skip quote validation for intraday backtests. In daily-cadence strategies,
                # returning an unvalidated expiration can cause massive downstream cache churn
                # (and, in CI acceptance runs, violate the warm-cache invariant).
                if not is_daily_cadence:
                    skip_quote_validation = True

        # PERF: Quote-based expiration validation can be very expensive in long-window backtests,
        # especially when far-dated expirations are not listed yet (or quote history is sparse).
        #
        # Cache validation outcomes per (underlying, expiration, side) with a small TTL for
        # negative results so we don't re-probe the same expirations day after day.
        as_of_date_for_cache: Optional[date] = None
        quote_validation_cache: Optional[Dict[Tuple[str, date, str], Dict[str, Any]]] = None
        if is_backtesting and underlying_symbol and as_of_date is not None:
            as_of_date_for_cache = as_of_date
            existing_cache = getattr(self, "_expiration_quote_validation_cache", None)
            if not isinstance(existing_cache, dict):
                existing_cache = {}
                setattr(self, "_expiration_quote_validation_cache", existing_cache)
            quote_validation_cache = existing_cache

        cache_dt = getattr(self, "_expiration_cache_dt", None)
        if cache_dt != dt:
            self._expiration_cache_dt = dt
            self._expiration_cache = {}

        try:
            chain_fingerprint = (len(specific_chain), min(specific_chain.keys()), max(specific_chain.keys()))
        except Exception:
            chain_fingerprint = (len(specific_chain) if isinstance(specific_chain, dict) else 0, None, None)

        # PERF: Many strategies pick expirations relative to "today" (as_of_date) using a fixed horizon
        # (e.g., 270D out). In that pattern, `dt` changes every day but the resolved expiration is
        # typically stable until the chain itself changes. Cache resolution by horizon-days with a
        # short TTL and invalidate automatically when the chain fingerprint changes.
        horizon_cache: Optional[Dict[Tuple[str, str, int, bool], Dict[str, Any]]] = None
        horizon_cache_key: Optional[Tuple[str, str, int, bool]] = None
        horizon_cache_ttl_future_days = 180
        horizon_cache_ttl_fallback_days = 30
        if is_backtesting and underlying_symbol and as_of_date is not None:
            horizon_days = None
            try:
                horizon_days = (dt - as_of_date).days
            except Exception:
                horizon_days = None
            if isinstance(horizon_days, int) and horizon_days >= 0:
                existing_horizon_cache = getattr(self, "_expiration_horizon_cache", None)
                if not isinstance(existing_horizon_cache, dict):
                    existing_horizon_cache = {}
                    setattr(self, "_expiration_horizon_cache", existing_horizon_cache)
                horizon_cache = existing_horizon_cache
                horizon_cache_key = (underlying_symbol, call_or_put_caps, horizon_days, bool(allow_prior))

                cached = horizon_cache.get(horizon_cache_key)
                if isinstance(cached, dict):
                    cached_until = cached.get("valid_until")
                    if isinstance(cached_until, date) and as_of_date < cached_until:
                        cached_expiry = cached.get("expiry")
                        if cached_expiry is None:
                            return None
                        if isinstance(cached_expiry, date) and cached_expiry >= as_of_date:
                            # Prefer the fast path when the chain fingerprint matches, but be resilient to
                            # benign chain growth/shrink (max expiry changes frequently in long backtests).
                            #
                            # NOTE: During early backtest windows the chain often lacks far-dated expirations,
                            # so we fall back to a nearer expiry (< dt). Cache those fallbacks too (with a short
                            # TTL) so strategies don't re-run expensive quote validation on every attempt.
                            is_fallback_expiry = cached_expiry < dt

                            if cached.get("chain_fingerprint") == chain_fingerprint:
                                return cached_expiry

                            # If the fingerprint changed, only reuse "good" expirations (>= dt). For fallback
                            # expirations (< dt), allow the cache to short-circuit repeated work only when the
                            # chain is stable; when the chain changes, re-run selection so we can adopt newly
                            # available horizons promptly.
                            if not is_fallback_expiry:
                                try:
                                    expiry_key = cached_expiry.strftime("%Y-%m-%d")
                                    if expiry_key in specific_chain:
                                        return cached_expiry
                                    # OCC Saturday representation (provider uses Saturday, trading model uses Friday).
                                    if (cached_expiry + timedelta(days=1)).strftime("%Y-%m-%d") in specific_chain:
                                        return cached_expiry
                                    if (cached_expiry - timedelta(days=1)).strftime("%Y-%m-%d") in specific_chain:
                                        return cached_expiry
                                except Exception:
                                    pass

        cache_underlying = underlying_symbol or getattr(underlying_asset, "symbol", None) or "<unknown>"
        expiration_cache_key = (cache_underlying, dt, call_or_put_caps, bool(allow_prior), chain_fingerprint)
        expiration_cache = getattr(self, "_expiration_cache", {})
        if expiration_cache_key in expiration_cache:
            return expiration_cache[expiration_cache_key]

        # Convert string expiries to dates for comparison
        expiration_dates: List[Tuple[str, date]] = _try_resolve_expiration(specific_chain)
        future_candidates = [(s, d) for s, d in expiration_dates if d >= dt]

        # PERF (intraday 0DTE index strategies):
        # For highly liquid index options (SPX/SPXW/etc), validating the *expiration itself* via
        # per-day snapshot quote probes adds a guaranteed remote round-trip (and dominates runtime
        # in long-window backtests) while rarely changing the outcome. The strike-selection step
        # still validates actionable NBBO for the actual contracts we intend to trade.
        if (
            is_backtesting
            and as_of_date is not None
            and dt == as_of_date
            and self._is_index_like_underlying(underlying_asset, underlying_symbol)
        ):
            for _exp_str, exp_date in expiration_dates:
                if exp_date == dt:
                    try:
                        self._expiration_cache[expiration_cache_key] = exp_date
                    except Exception:
                        pass
                    return exp_date

        # Log chain search (DEBUG level for details)
        logger.debug("[OptionsHelper] Finding expiration >= %s: %d candidates from %d total expirations",
                     dt, len(future_candidates), len(expiration_dates))

        # If we couldn't find any expirations beyond the requested date, attempt a deeper fetch
        if not future_candidates and underlying_asset is not None:
            self.strategy.log_message(
                f"No expirations >= {dt} found in cached chains; requesting extended range...",
                color="yellow",
            )
            max_expiration_hint = None
            try:
                max_expiration_hint = self._default_chain_max_expiration_date(
                    underlying_asset=underlying_asset,
                    min_expiration_date=dt,
                )
            except Exception:
                max_expiration_hint = None

            with self._chain_hint(dt, max_expiration_hint):
                refreshed_chains = self.strategy.get_chains(underlying_asset)
            if refreshed_chains:
                chains_map = refreshed_chains if isinstance(refreshed_chains, dict) else {}
                options_map = chains_map.get("Chains") if isinstance(chains_map.get("Chains"), dict) else None
                if options_map:
                    specific_chain = options_map.get(call_or_put_caps) if isinstance(options_map.get(call_or_put_caps), dict) else None
                    if specific_chain:
                        expiration_dates = _try_resolve_expiration(specific_chain)
                        future_candidates = [(s, d) for s, d in expiration_dates if d >= dt]
            if future_candidates:
                self.strategy.log_message(
                    f"Extended chain request delivered {len(future_candidates)} expirations >= {dt}.",
                    color="blue",
                )
            else:
                self.strategy.log_message(
                    f"Extended chain request still lacks expirations on/after {dt}; giving up.",
                    color="red",
                )
                return None

        def _is_standard_monthly(exp_date: date) -> bool:
            try:
                return exp_date.weekday() == 4 and 15 <= exp_date.day <= 21
            except Exception:
                return False

        # In long-horizon backtests, chains can include many weekly expirations that were not
        # listed historically that far out. Prefer standard monthly expirations (3rd Friday) to
        # reduce expensive validation probes and improve hit rate.
        if is_backtesting and as_of_date is not None:
            try:
                horizon_days = (dt - as_of_date).days
            except Exception:
                horizon_days = None
            if horizon_days is not None and horizon_days >= 60 and future_candidates:
                monthly_candidates = [(s, d) for s, d in future_candidates if _is_standard_monthly(d)]
                if monthly_candidates:
                    future_candidates = monthly_candidates

        # =====================================================================================
        # POINT-OF-USE VALIDATION (2025-12-07)
        # =====================================================================================
        # This is where we validate that an expiration has actual tradeable data for the
        # backtest date. This validation was moved here FROM build_historical_chain() because:
        #
        # 1. LEAPS SUPPORT: Far-dated expirations (2+ years out) may not have quote data
        #    for every historical date. Previously, these were incorrectly filtered out
        #    during chain building, causing strategies to buy short-term options instead.
        #
        # 2. EFFICIENCY: Strategies typically only need 1-2 expirations from a chain with
        #    100+ entries. Validating only the candidates we need is much faster.
        #
        # 3. ACCURACY: We can now return the FIRST valid expiration, rather than pre-filtering
        #    and potentially missing valid options.
        # =====================================================================================
        underlying_price: Optional[float] = None
        if underlying_symbol:
            try:
                validation_underlying = (
                    underlying_asset
                    if underlying_asset is not None
                    else Asset(underlying_symbol, asset_type=Asset.AssetType.STOCK)
                )
                price_value = self.strategy.get_last_price(validation_underlying)
                if price_value is not None:
                    underlying_price = float(price_value)
                    if not math.isfinite(underlying_price) or underlying_price <= 0:
                        underlying_price = None
            except Exception:
                underlying_price = None

        quote_validation_attempted = False

        def _validate_candidates(
            candidates: List[Tuple[str, date]],
            *,
            validate_quotes: bool = True,
            scan_mode: str = "future",
        ) -> Optional[date]:
            # In historical backtests (especially long windows), validating tradeable quote data
            # for every expiration can be extremely expensive when the provider has sparse quote
            # history. Keep a bounded validation budget (quote probes) and fall back to "closest by date"
            # selection in backtesting mode so strategies can proceed (and handle missing pricing
            # via their own price checks).
            max_checks = max_validation_checks
            checks = 0

            candidates_to_check = candidates
            if is_backtesting and scan_mode == "future" and len(candidates) > 8:
                # PERF: Long-horizon strategies can have hundreds of expirations. Validating them
                # strictly sequentially can be extremely slow when the downloader queue is under
                # load (each probe is a network round-trip). Use a sparse, exponentially spaced
                # probe order so we can quickly reach far-dated expirations that may be the first
                # historically listed contracts, while still checking the nearest expiries.
                try:
                    indices: List[int] = list(range(min(3, len(candidates))))
                    idx = 4
                    while idx < len(candidates) and len(indices) < max_checks:
                        indices.append(idx)
                        idx *= 2
                    for i in range(3, len(candidates)):
                        if len(indices) >= max_checks:
                            break
                        if i not in indices:
                            indices.append(i)
                    candidates_to_check = [candidates[i] for i in indices]
                except Exception:
                    candidates_to_check = candidates

            for exp_str, exp_date in candidates_to_check:
                strikes = specific_chain.get(exp_str)
                if not strikes:
                    continue

                # Prefer a strike near the underlying's current price for validation.
                # Middle-of-chain can be far OTM when chains include very wide strike ranges,
                # leading to false "no data" results during backtests.
                strike_candidates: List[float] = []
                try:
                    iterable = strikes if isinstance(strikes, (list, tuple, set)) else list(strikes)
                except Exception:
                    iterable = strikes
                for raw_strike in iterable:
                    try:
                        strike_candidates.append(float(raw_strike))
                    except (TypeError, ValueError):
                        continue
                if not strike_candidates:
                    continue
                strike_candidates.sort()
                if underlying_price is not None:
                    near_strikes = [
                        s
                        for s in strike_candidates
                        if (underlying_price * 0.5) <= s <= (underlying_price * 1.5)
                    ]
                    if not near_strikes:
                        continue
                    # Validate the expiration using a small set of strikes likely to be traded.
                    #
                    # Why: Some providers have sparse historical quote/trade coverage per strike,
                    # especially for long-dated options. Validating only the nearest-to-underlying
                    # strike can produce false negatives (expiry rejected even though an OTM strike
                    # has data). Probe a small ATM+OTM set to reduce churn.
                    atm_strike = min(near_strikes, key=lambda s: abs(s - underlying_price))
                    strike_probe_candidates: List[float] = []

                    target_mult = 1.2 if call_or_put_caps == "CALL" else 0.8
                    target_strike = underlying_price * target_mult
                    try:
                        otm_strike = min(near_strikes, key=lambda s: abs(s - target_strike))
                    except Exception:
                        otm_strike = None
                    # Prefer the OTM probe first so we validate the kind of strikes many strategies
                    # actually trade (e.g., deep-drawdown call buys at ~1.2x underlying).
                    if otm_strike is not None and otm_strike not in strike_probe_candidates:
                        strike_probe_candidates.append(otm_strike)
                    if atm_strike not in strike_probe_candidates:
                        strike_probe_candidates.append(atm_strike)
                else:
                    strike_probe_candidates = [strike_candidates[len(strike_candidates) // 2]]

                if underlying_symbol:
                    if not validate_quotes:
                        return exp_date

                    cache_key: Optional[Tuple[str, date, str]] = None
                    if (
                        quote_validation_cache is not None
                        and as_of_date_for_cache is not None
                        and underlying_symbol
                    ):
                        cache_key = (underlying_symbol, exp_date, call_or_put_caps)
                        cached = quote_validation_cache.get(cache_key)
                        if isinstance(cached, dict):
                            cached_available_from = cached.get("available_from")
                            if isinstance(cached_available_from, date) and as_of_date_for_cache >= cached_available_from:
                                return exp_date
                            cached_disabled_until = cached.get("disabled_until")
                            if isinstance(cached_disabled_until, date) and as_of_date_for_cache < cached_disabled_until:
                                continue

                    checks += 1
                    if checks > max_checks:
                        if is_backtesting:
                            self.strategy.log_message(
                                f"Expiration validation exceeded {max_checks} candidates; no tradeable expiry found "
                                f"for {call_or_put_caps} (dt={dt}).",
                                color="yellow",
                            )
                        return None

                    # Use a point-in-time quote probe for validation.
                    #
                    # NOTE: Intraday quote history is often more complete than day-level NBBO
                    # for historical options (ThetaData can omit bid/ask on day bars). For
                    # performance and robustness we use the quote-derived mark path, which can
                    # use a snapshot-only implementation in backtesting data sources.
                    #
                    # IMPORTANT: Even for daily-cadence backtests, we intentionally validate expirations
                    # using intraday quote snapshots when available. ThetaData's historical option day
                    # quotes can omit bid/ask (or be placeholder-only) even when minute NBBO exists.
                    #
                    # This validation is used only to determine whether a contract is tradeable on the
                    # current simulation date; it should not introduce lookahead into fill prices.
                    try:
                        nonlocal quote_validation_attempted
                        quote_validation_attempted = True
                        for test_strike in strike_probe_candidates[:max_strike_probes]:
                            test_option = Asset(
                                underlying_symbol,
                                asset_type="option",
                                expiration=exp_date,
                                strike=float(test_strike),
                                right=call_or_put,
                            )

                            mark_price, bid, ask = self._get_option_mark_from_quote(
                                test_option,
                                # Validate expirations using a point-in-time quote probe. For ThetaData,
                                # intraday quote snapshots are often the most reliable way to detect
                                # tradeable historical contracts without pulling full-day data.
                                snapshot=True,
                            )
                            # Some providers (and some historical contracts) have sparse intraday NBBO
                            # coverage but still expose a usable day-level quote/price. If the snapshot
                            # probe returns no price signal at all, fall back to the normal quote path
                            # so we don't incorrectly skip otherwise tradeable expirations.
                            if bid is None and ask is None and mark_price is None:
                                # ThetaData backtests should not validate expirations using bar-aligned
                                # quotes: it can turn "no intraday NBBO" into a false-positive validity
                                # signal and cause downstream cache churn (e.g., selecting far-dated
                                # expirations with no usable intraday pricing).
                                if not is_theta_backtest:
                                    mark_price, bid, ask = self._get_option_mark_from_quote(
                                        test_option,
                                        snapshot=False,
                                    )
                            has_bid_ask = bid is not None or ask is not None

                            if has_bid_ask:
                                self.strategy.log_message(
                                    f"Found valid expiry {exp_date} with quote data for {call_or_put_caps}",
                                    color="blue",
                                )
                                if cache_key is not None and as_of_date_for_cache is not None and quote_validation_cache is not None:
                                    existing = quote_validation_cache.get(cache_key)
                                    if isinstance(existing, dict) and isinstance(existing.get("available_from"), date):
                                        available_from = existing["available_from"]
                                    else:
                                        available_from = as_of_date_for_cache
                                    quote_validation_cache[cache_key] = {"available_from": available_from}
                                return exp_date
                            if mark_price is not None:
                                self.strategy.log_message(
                                    f"Found valid expiry {exp_date} with quote-derived price data for {call_or_put_caps}",
                                    color="blue",
                                )
                                if cache_key is not None and as_of_date_for_cache is not None and quote_validation_cache is not None:
                                    existing = quote_validation_cache.get(cache_key)
                                    if isinstance(existing, dict) and isinstance(existing.get("available_from"), date):
                                        available_from = existing["available_from"]
                                    else:
                                        available_from = as_of_date_for_cache
                                    quote_validation_cache[cache_key] = {"available_from": available_from}
                                return exp_date

                        if cache_key is not None and as_of_date_for_cache is not None and quote_validation_cache is not None:
                            existing = quote_validation_cache.get(cache_key)
                            if not (isinstance(existing, dict) and isinstance(existing.get("available_from"), date)):
                                quote_validation_cache[cache_key] = {
                                    "disabled_until": as_of_date_for_cache + timedelta(days=7),
                                }
                    except Exception:
                        pass
                else:
                    # Backward compatibility: If no underlying symbol available, we can't
                    # validate data availability. Assume the expiry is valid.
                    self.strategy.log_message(
                        f"Cannot validate data without underlying symbol, returning {exp_date}",
                        color="yellow",
                    )
                    return exp_date

            return None

        resolved = _validate_candidates(future_candidates, validate_quotes=not skip_quote_validation)
        if resolved is not None:
            try:
                self._expiration_cache[expiration_cache_key] = resolved
            except Exception:
                pass
            if horizon_cache is not None and horizon_cache_key is not None and as_of_date is not None:
                try:
                    ttl_days = horizon_cache_ttl_future_days if resolved >= dt else horizon_cache_ttl_fallback_days
                    horizon_cache[horizon_cache_key] = {
                        "expiry": resolved,
                        "valid_until": as_of_date + timedelta(days=ttl_days),
                        "chain_fingerprint": chain_fingerprint,
                    }
                except Exception:
                    pass
            return resolved

        if allow_prior or is_backtesting:
            prior_candidates = [(s, d) for s, d in expiration_dates if d < dt]
            if is_backtesting and as_of_date is not None:
                # A "prior-to-target-horizon" expiry can still be in the future relative to the
                # current simulation date (as_of_date) when far-dated expirations were not listed yet.
                #
                # Never return expirations that are already in the past relative to the current
                # simulation date: those contracts cannot be traded and can cause strategies to
                # thrash (repeated selection attempts every bar).
                prior_candidates = [(s, d) for s, d in prior_candidates if d >= as_of_date]
            prior_candidates.sort(key=lambda x: x[1], reverse=True)
            resolved = _validate_candidates(prior_candidates, validate_quotes=not skip_quote_validation, scan_mode="prior")
            if resolved is not None:
                self.strategy.log_message(
                    f"Falling back to prior valid expiry {resolved} (< {dt}) for {call_or_put_caps}.",
                    color="yellow",
                )
                try:
                    self._expiration_cache[expiration_cache_key] = resolved
                except Exception:
                    pass
                if horizon_cache is not None and horizon_cache_key is not None and as_of_date is not None:
                    try:
                        ttl_days = horizon_cache_ttl_future_days if resolved >= dt else horizon_cache_ttl_fallback_days
                        horizon_cache[horizon_cache_key] = {
                            "expiry": resolved,
                            "valid_until": as_of_date + timedelta(days=ttl_days),
                            "chain_fingerprint": chain_fingerprint,
                        }
                    except Exception:
                        pass
                return resolved

        if (
            is_backtesting
            and underlying_symbol
            and as_of_date is not None
            and quote_validation_attempted
            and not skip_quote_validation
            and not is_daily_cadence
        ):
            disabled_until = as_of_date + timedelta(days=7)
            existing_until = self._expiration_validation_disabled_until.get(underlying_symbol)
            if not isinstance(existing_until, date) or disabled_until > existing_until:
                self._expiration_validation_disabled_until[underlying_symbol] = disabled_until

        msg = f"No valid expirations on or after {dt} with tradeable data for {call_or_put_caps}; skipping."
        self.strategy.log_message(msg, color="yellow")
        try:
            self._expiration_cache[expiration_cache_key] = None
        except Exception:
            pass
        if horizon_cache is not None and horizon_cache_key is not None and as_of_date is not None:
            try:
                horizon_cache[horizon_cache_key] = {
                    "expiry": None,
                    "valid_until": as_of_date + timedelta(days=horizon_cache_ttl_fallback_days),
                    "chain_fingerprint": chain_fingerprint,
                }
            except Exception:
                pass
        return None

    # ============================================================
    # Order Building Functions (Build orders without submission)
    # ============================================================

    def build_call_orders(self, underlying_asset: Asset, expiry: date, call_strike: float,
                          quantity_to_trade: int, wing_size: float) -> Tuple[Optional[Order], Optional[Order]]:
        """
        Build call orders for a spread without submitting them.
        This builds a sell order at the given call_strike and a buy order at (call_strike + wing_size).

        Parameters
        ----------
        underlying_asset : Asset
            The underlying asset.
        expiry : date
            Option expiry date.
        call_strike : float
            Selected call strike for the short leg.
        quantity_to_trade : int
            Number of contracts.
        wing_size : float
            Offset for the long leg (buy leg).

        Returns
        -------
        Tuple[Optional[Order], Optional[Order]]
            (call_sell_order, call_buy_order) or (None, None) if prices are unavailable.
        """
        self.strategy.log_message(f"Building call orders for strike {call_strike} with wing size {wing_size}", color="blue")
        call_sell_asset = Asset(
            underlying_asset.symbol, asset_type="option", expiration=expiry,
            strike=call_strike, right="call", underlying_asset=underlying_asset
        )
        call_sell_price, _, _ = self._get_option_mark_from_quote(call_sell_asset)
        if call_sell_price is None:
            call_sell_price = self.strategy.get_last_price(call_sell_asset)
        call_sell_order = self.strategy.create_order(call_sell_asset, quantity_to_trade, "sell")
        call_buy_asset = Asset(
            underlying_asset.symbol, asset_type="option", expiration=expiry,
            strike=call_strike + wing_size, right="call", underlying_asset=underlying_asset
        )
        call_buy_price, _, _ = self._get_option_mark_from_quote(call_buy_asset)
        if call_buy_price is None:
            call_buy_price = self.strategy.get_last_price(call_buy_asset)
        call_buy_order = self.strategy.create_order(call_buy_asset, quantity_to_trade, "buy")
        if call_sell_price is None or call_buy_price is None:
            self.strategy.log_message("Call order build failed due to missing prices.", color="red")
            return None, None
        return call_sell_order, call_buy_order

    def build_put_orders(self, underlying_asset: Asset, expiry: date, put_strike: float,
                         quantity_to_trade: int, wing_size: float) -> Tuple[Optional[Order], Optional[Order]]:
        """
        Build put orders for a spread without submitting them.
        This builds a sell order at the given put_strike and a buy order at (put_strike - wing_size).

        Parameters
        ----------
        underlying_asset : Asset
            The underlying asset.
        expiry : date
            Option expiry date.
        put_strike : float
            Selected put strike for the short leg.
        quantity_to_trade : int
            Number of contracts.
        wing_size : float
            Offset for the long leg (buy leg).

        Returns
        -------
        Tuple[Optional[Order], Optional[Order]]
            (put_sell_order, put_buy_order) or (None, None) if prices are unavailable.
        """
        self.strategy.log_message(f"Building put orders for strike {put_strike} with wing size {wing_size}", color="blue")
        put_sell_asset = Asset(
            underlying_asset.symbol, asset_type="option", expiration=expiry,
            strike=put_strike, right="put", underlying_asset=underlying_asset
        )
        put_sell_price, _, _ = self._get_option_mark_from_quote(put_sell_asset)
        if put_sell_price is None:
            put_sell_price = self.strategy.get_last_price(put_sell_asset)
        put_sell_order = self.strategy.create_order(put_sell_asset, quantity_to_trade, "sell")
        put_buy_asset = Asset(
            underlying_asset.symbol, asset_type="option", expiration=expiry,
            strike=put_strike - wing_size, right="put", underlying_asset=underlying_asset
        )
        put_buy_price, _, _ = self._get_option_mark_from_quote(put_buy_asset)
        if put_buy_price is None:
            put_buy_price = self.strategy.get_last_price(put_buy_asset)
        put_buy_order = self.strategy.create_order(put_buy_asset, quantity_to_trade, "buy")
        if put_sell_price is None or put_buy_price is None:
            self.strategy.log_message("Put order build failed due to missing prices.", color="red")
            return None, None
        return put_sell_order, put_buy_order

    def build_call_vertical_spread_orders(self, underlying_asset: Asset, expiry: date,
                                          lower_strike: float, upper_strike: float,
                                          quantity: int) -> List[Order]:
        """
        Build orders for a call vertical spread (bull call spread) without submitting them.
        The spread consists of buying a call at lower_strike and selling a call at upper_strike.

        Parameters
        ----------
        underlying_asset : Asset
            The underlying asset.
        expiry : date
            Option expiry.
        lower_strike : float
            Strike for the long call.
        upper_strike : float
            Strike for the short call.
        quantity : int
            Number of contracts.

        Returns
        -------
        List[Order]
            A list containing the buy order (long call) and the sell order (short call).
        """
        self.strategy.log_message(f"Building call vertical spread orders: Buy at {lower_strike}, Sell at {upper_strike}", color="blue")
        buy_call = Asset(underlying_asset.symbol, asset_type="option", expiration=expiry,
                         strike=lower_strike, right="call", underlying_asset=underlying_asset)
        sell_call = Asset(underlying_asset.symbol, asset_type="option", expiration=expiry,
                          strike=upper_strike, right="call", underlying_asset=underlying_asset)
        buy_order = self.strategy.create_order(buy_call, quantity, "buy")
        sell_order = self.strategy.create_order(sell_call, quantity, "sell")
        return [buy_order, sell_order]

    def build_put_vertical_spread_orders(self, underlying_asset: Asset, expiry: date,
                                         upper_strike: float, lower_strike: float,
                                         quantity: int) -> List[Order]:
        """
        Build orders for a put vertical spread (bull put spread) without submitting them.
        The spread consists of selling a put at upper_strike and buying a put at lower_strike.

        Parameters
        ----------
        underlying_asset : Asset
            The underlying asset.
        expiry : date
            Option expiry.
        upper_strike : float
            Strike for the short put.
        lower_strike : float
            Strike for the long put.
        quantity : int
            Number of contracts.

        Returns
        -------
        List[Order]
            A list containing the sell order (short put) and the buy order (long put).
        """
        self.strategy.log_message(f"Building put vertical spread orders: Sell at {upper_strike}, Buy at {lower_strike}", color="blue")
        sell_put = Asset(underlying_asset.symbol, asset_type="option", expiration=expiry,
                         strike=upper_strike, right="put", underlying_asset=underlying_asset)
        buy_put = Asset(underlying_asset.symbol, asset_type="option", expiration=expiry,
                        strike=lower_strike, right="put", underlying_asset=underlying_asset)
        sell_order = self.strategy.create_order(sell_put, quantity, "sell")
        buy_order = self.strategy.create_order(buy_put, quantity, "buy")
        return [sell_order, buy_order]

    def build_calendar_spread_orders(self, underlying_asset: Asset, strike: float,
                                     near_expiry: date, far_expiry: date,
                                     quantity: int, right: str) -> List[Order]:
        """
        Build orders for a calendar spread (same strike, different expiries) without submitting them.
        Typically, the near expiry option is sold and the far expiry option is bought.

        Parameters
        ----------
        underlying_asset : Asset
            The underlying asset.
        strike : float
            Strike price for both legs.
        near_expiry : date
            Near expiry date (sell leg).
        far_expiry : date
            Far expiry date (buy leg).
        quantity : int
            Number of contracts.
        right : str
            Option type ("call" or "put").

        Returns
        -------
        List[Order]
            A list containing the sell order and the buy order.
        """
        self.strategy.log_message(f"Building calendar spread orders at strike {strike} with near expiry {near_expiry} and far expiry {far_expiry}", color="blue")
        sell_option = Asset(underlying_asset.symbol, asset_type="option", expiration=near_expiry,
                              strike=strike, right=right, underlying_asset=underlying_asset)
        buy_option  = Asset(underlying_asset.symbol, asset_type="option", expiration=far_expiry,
                              strike=strike, right=right, underlying_asset=underlying_asset)
        sell_order = self.strategy.create_order(sell_option, quantity, "sell")
        buy_order  = self.strategy.create_order(buy_option, quantity, "buy")
        return [sell_order, buy_order]

    def build_butterfly_spread_orders(self, underlying_asset: Asset, expiry: date,
                                      lower_strike: float, middle_strike: float, upper_strike: float,
                                      quantity: int, right: str) -> List[Order]:
        """
        Build orders for a butterfly spread without submitting them.
        For a call butterfly: buy 1 call at lower_strike, sell 2 calls at middle_strike, and buy 1 call at upper_strike.
        For a put butterfly, similar logic applies.

        Parameters
        ----------
        underlying_asset : Asset
            The underlying asset.
        expiry : date
            Option expiry.
        lower_strike : float
            Lower strike (long leg).
        middle_strike : float
            Middle strike (short leg, double quantity).
        upper_strike : float
            Upper strike (long leg).
        quantity : int
            Number of butterfly spreads (each spread uses a 1-2-1 ratio).
        right : str
            Option type ("call" or "put").

        Returns
        -------
        List[Order]
            A list of orders representing the butterfly spread.
        """
        self.strategy.log_message(f"Building butterfly spread orders: Long at {lower_strike} and {upper_strike}, Short at {middle_strike}", color="blue")
        long_lower = Asset(underlying_asset.symbol, asset_type="option", expiration=expiry,
                           strike=lower_strike, right=right, underlying_asset=underlying_asset)
        short_middle = Asset(underlying_asset.symbol, asset_type="option", expiration=expiry,
                             strike=middle_strike, right=right, underlying_asset=underlying_asset)
        long_upper = Asset(underlying_asset.symbol, asset_type="option", expiration=expiry,
                           strike=upper_strike, right=right, underlying_asset=underlying_asset)
        order_long_lower = self.strategy.create_order(long_lower, quantity, "buy")
        order_short_middle = self.strategy.create_order(short_middle, 2 * quantity, "sell")
        order_long_upper = self.strategy.create_order(long_upper, quantity, "buy")
        return [order_long_lower, order_short_middle, order_long_upper]

    def build_straddle_orders(self, underlying_asset: Asset, expiry: date, strike: float,
                              quantity: int) -> List[Order]:
        """
        Build orders for a straddle without submitting them by buying both a call and a put at the same strike.

        Parameters
        ----------
        underlying_asset : Asset
            The underlying asset.
        expiry : date
            Option expiry.
        strike : float
            The strike price.
        quantity : int
            Number of contracts.

        Returns
        -------
        List[Order]
            A list containing the call order and the put order.
        """
        self.strategy.log_message(f"Building straddle orders at strike {strike}", color="blue")
        call_option = Asset(underlying_asset.symbol, asset_type="option", expiration=expiry,
                            strike=strike, right="call", underlying_asset=underlying_asset)
        put_option = Asset(underlying_asset.symbol, asset_type="option", expiration=expiry,
                           strike=strike, right="put", underlying_asset=underlying_asset)
        call_order = self.strategy.create_order(call_option, quantity, "buy")
        put_order = self.strategy.create_order(put_option, quantity, "buy")
        return [call_order, put_order]

    def build_strangle_orders(self, underlying_asset: Asset, expiry: date,
                              lower_strike: float, upper_strike: float,
                              quantity: int) -> List[Order]:
        """
        Build orders for a strangle without submitting them by buying a put at a lower strike
        and a call at a higher strike.

        Parameters
        ----------
        underlying_asset : Asset
            The underlying asset.
        expiry : date
            Option expiry.
        lower_strike : float
            Strike for the put.
        upper_strike : float
            Strike for the call.
        quantity : int
            Number of contracts.

        Returns
        -------
        List[Order]
            A list containing the put order and the call order.
        """
        self.strategy.log_message(f"Building strangle orders: Put at {lower_strike}, Call at {upper_strike}", color="blue")
        call_option = Asset(underlying_asset.symbol, asset_type="option", expiration=expiry,
                            strike=upper_strike, right="call", underlying_asset=underlying_asset)
        put_option = Asset(underlying_asset.symbol, asset_type="option", expiration=expiry,
                           strike=lower_strike, right="put", underlying_asset=underlying_asset)
        call_order = self.strategy.create_order(call_option, quantity, "buy")
        put_order = self.strategy.create_order(put_option, quantity, "buy")
        return [put_order, call_order]

    def build_diagonal_spread_orders(self, underlying_asset: Asset, near_expiry: date, far_expiry: date,
                                     near_strike: float, far_strike: float, quantity: int,
                                     right: str) -> List[Order]:
        """
        Build orders for a diagonal spread without submitting them.
        For example, for a call diagonal spread, sell a near-expiry call at near_strike and buy a far-expiry call at far_strike.

        Parameters
        ----------
        underlying_asset : Asset
            The underlying asset.
        near_expiry : date
            The near expiry date (sell leg).
        far_expiry : date
            The far expiry date (buy leg).
        near_strike : float
            Strike for the near-expiry (sell) option.
        far_strike : float
            Strike for the far-expiry (buy) option.
        quantity : int
            Number of contracts.
        right : str
            Option type ("call" or "put").

        Returns
        -------
        List[Order]
            A list containing the sell order and the buy order.
        """
        self.strategy.log_message(f"Building diagonal spread orders: Sell at {near_strike} (expiry {near_expiry}), Buy at {far_strike} (expiry {far_expiry})", color="blue")
        sell_option = Asset(underlying_asset.symbol, asset_type="option", expiration=near_expiry,
                            strike=near_strike, right=right, underlying_asset=underlying_asset)
        buy_option = Asset(underlying_asset.symbol, asset_type="option", expiration=far_expiry,
                           strike=far_strike, right=right, underlying_asset=underlying_asset)
        sell_order = self.strategy.create_order(sell_option, quantity, "sell")
        buy_order = self.strategy.create_order(buy_option, quantity, "buy")
        return [sell_order, buy_order]

    def build_ratio_spread_orders(self, underlying_asset: Asset, expiry: date, buy_strike: float, sell_strike: float,
                                  buy_qty: int, sell_qty: int, right: str) -> List[Order]:
        """
        Build orders for a ratio spread without submitting them.
        For example, buy one option at buy_strike and sell a different number at sell_strike.

        Parameters
        ----------
        underlying_asset : Asset
            The underlying asset.
        expiry : date
            Option expiry.
        buy_strike : float
            Strike for the long leg.
        sell_strike : float
            Strike for the short leg.
        buy_qty : int
            Quantity for the long leg.
        sell_qty : int
            Quantity for the short leg.
        right : str
            Option type ("call" or "put").

        Returns
        -------
        List[Order]
            A list containing the long order and the short order.
        """
        self.strategy.log_message(f"Building ratio spread orders: Long at {buy_strike} ({buy_qty}), Short at {sell_strike} ({sell_qty})", color="blue")
        long_leg = Asset(underlying_asset.symbol, asset_type="option", expiration=expiry,
                         strike=buy_strike, right=right, underlying_asset=underlying_asset)
        short_leg = Asset(underlying_asset.symbol, asset_type="option", expiration=expiry,
                          strike=sell_strike, right=right, underlying_asset=underlying_asset)
        long_order = self.strategy.create_order(long_leg, buy_qty, "buy")
        short_order = self.strategy.create_order(short_leg, sell_qty, "sell")
        return [long_order, short_order]

    # ============================================================
    # Order Execution Functions (Build then submit orders)
    # ============================================================

    def _determine_multileg_order_type(self, limit_price: float) -> str:
        """
        Determine the Tradier multileg order type based on the limit price.
        Returns "debit" if price > 0, "credit" if price < 0, "even" if price == 0.
        """
        # Handle None limit price
        if limit_price is None:
            self.strategy.log_message("Warning: limit_price is None, defaulting to 'even' order type", color="yellow")
            return "even"
            
        if limit_price > 0:
            return "debit"
        elif limit_price < 0:
            return "credit"
        else:
            return "even"

    def execute_orders(self, orders: List[Order], limit_type: Optional[str] = None) -> bool:
        """
        Submit a list of orders as a multi-leg order.
        If a limit_type is provided, calculate a limit price and submit with that price.

        Parameters
        ----------
        orders : List[Order]
            A list of orders to submit.
        limit_type : Optional[str]
            One of "best", "fastest", or "mid" for limit pricing.

        Returns
        -------
        bool
            True if orders are submitted successfully.
        """
        self.strategy.log_message("Executing orders...", color="blue")
        if limit_type:
            limit_price = self.calculate_multileg_limit_price(orders, limit_type)
            if limit_price is None:
                self.strategy.log_message("Failed to calculate limit price - cannot execute orders", color="red")
                return False
            order_type = self._determine_multileg_order_type(limit_price)
            self.strategy.log_message(
                f"Submitting multileg order at price {limit_price} as {order_type}", color="blue"
            )
            self.strategy.submit_orders(
                orders,
                is_multileg=True,
                order_type=order_type,
                price=abs(limit_price)
            )
        else:
            self.strategy.log_message("Submitting orders without a limit price.", color="blue")
            self.strategy.submit_orders(orders, is_multileg=True)
        return True

    def execute_call_vertical_spread(self, underlying_asset: Asset, expiry: date,
                                     lower_strike: float, upper_strike: float,
                                     quantity: int, limit_type: Optional[str] = None) -> bool:
        """
        Build and submit orders for a call vertical spread.

        Parameters
        ----------
        underlying_asset : Asset
            The underlying asset.
        expiry : date
            Option expiry.
        lower_strike : float
            Strike for the long call.
        upper_strike : float
            Strike for the short call.
        quantity : int
            Number of contracts.
        limit_type : Optional[str]
            Limit pricing type.

        Returns
        -------
        bool
            True if orders are submitted successfully.
        """
        self.strategy.log_message("Executing call vertical spread.", color="blue")
        orders = self.build_call_vertical_spread_orders(underlying_asset, expiry, lower_strike, upper_strike, quantity)
        return self.execute_orders(orders, limit_type)

    def execute_put_vertical_spread(self, underlying_asset: Asset, expiry: date,
                                    upper_strike: float, lower_strike: float,
                                    quantity: int, limit_type: Optional[str] = None) -> bool:
        """
        Build and submit orders for a put vertical spread.

        Parameters
        ----------
        underlying_asset : Asset
            The underlying asset.
        expiry : date
            Option expiry.
        upper_strike : float
            Strike for the short put.
        lower_strike : float
            Strike for the long put.
        quantity : int
            Number of contracts.
        limit_type : Optional[str]
            Limit pricing type.

        Returns
        -------
        bool
            True if orders are submitted successfully.
        """
        self.strategy.log_message("Executing put vertical spread.", color="blue")
        orders = self.build_put_vertical_spread_orders(underlying_asset, expiry, upper_strike, lower_strike, quantity)
        return self.execute_orders(orders, limit_type)

    def execute_calendar_spread(self, underlying_asset: Asset, strike: float,
                                near_expiry: date, far_expiry: date,
                                quantity: int, right: str, limit_type: Optional[str] = None) -> bool:
        """
        Build and submit orders for a calendar spread.

        Parameters
        ----------
        underlying_asset : Asset
            The underlying asset.
        strike : float
            Strike price for both legs.
        near_expiry : date
            Near expiry date (sell leg).
        far_expiry : date
            Far expiry date (buy leg).
        quantity : int
            Number of contracts.
        right : str
            Option type ("call" or "put").
        limit_type : Optional[str]
            Limit pricing type.

        Returns
        -------
        bool
            True if orders are submitted successfully.
        """
        self.strategy.log_message("Executing calendar spread.", color="blue")
        orders = self.build_calendar_spread_orders(underlying_asset, strike, near_expiry, far_expiry, quantity, right)
        return self.execute_orders(orders, limit_type)

    def execute_butterfly_spread(self, underlying_asset: Asset, expiry: date,
                                 lower_strike: float, middle_strike: float, upper_strike: float,
                                 quantity: int, right: str, limit_type: Optional[str] = None) -> bool:
        """
        Build and submit orders for a butterfly spread.

        Parameters
        ----------
        underlying_asset : Asset
            The underlying asset.
        expiry : date
            Option expiry.
        lower_strike : float
            Lower strike for long leg.
        middle_strike : float
            Middle strike for the short leg.
        upper_strike : float
            Upper strike for long leg.
        quantity : int
            Number of butterfly spreads (1-2-1 ratio).
        right : str
            Option type ("call" or "put").
        limit_type : Optional[str]
            Limit pricing type.

        Returns
        -------
        bool
            True if orders are submitted successfully.
        """
        self.strategy.log_message("Executing butterfly spread.", color="blue")
        orders = self.build_butterfly_spread_orders(underlying_asset, expiry, lower_strike, middle_strike, upper_strike, quantity, right)
        return self.execute_orders(orders, limit_type)

    def execute_straddle(self, underlying_asset: Asset, expiry: date, strike: float,
                         quantity: int, limit_type: Optional[str] = None) -> bool:
        """
        Build and submit orders for a straddle.

        Parameters
        ----------
        underlying_asset : Asset
            The underlying asset.
        expiry : date
            Option expiry.
        strike : float
            The strike price.
        quantity : int
            Number of contracts.
        limit_type : Optional[str]
            Limit pricing type.

        Returns
        -------
        bool
            True if orders are submitted successfully.
        """
        self.strategy.log_message("Executing straddle.", color="blue")
        orders = self.build_straddle_orders(underlying_asset, expiry, strike, quantity)
        return self.execute_orders(orders, limit_type)

    def execute_strangle(self, underlying_asset: Asset, expiry: date, lower_strike: float, upper_strike: float,
                         quantity: int, limit_type: Optional[str] = None) -> bool:
        """
        Build and submit orders for a strangle.

        Parameters
        ----------
        underlying_asset : Asset
            The underlying asset.
        expiry : date
            Option expiry.
        lower_strike : float
            Strike for the put.
        upper_strike : float
            Strike for the call.
        quantity : int
            Number of contracts.
        limit_type : Optional[str]
            Limit pricing type.

        Returns
        -------
        bool
            True if orders are submitted successfully.
        """
        self.strategy.log_message("Executing strangle.", color="blue")
        orders = self.build_strangle_orders(underlying_asset, expiry, lower_strike, upper_strike, quantity)
        return self.execute_orders(orders, limit_type)

    def execute_diagonal_spread(self, underlying_asset: Asset, near_expiry: date, far_expiry: date,
                                near_strike: float, far_strike: float, quantity: int,
                                right: str, limit_type: Optional[str] = None) -> bool:
        """
        Build and submit orders for a diagonal spread.

        Parameters
        ----------
        underlying_asset : Asset
            The underlying asset.
        near_expiry : date
            Near expiry date (sell leg).
        far_expiry : date
            Far expiry date (buy leg).
        near_strike : float
            Strike for the near-expiry (sell) option.
        far_strike : float
            Strike for the far-expiry (buy) option.
        quantity : int
            Number of contracts.
        right : str
            Option type ("call" or "put").
        limit_type : Optional[str]
            Limit pricing type.

        Returns
        -------
        bool
            True if orders are submitted successfully.
        """
        self.strategy.log_message("Executing diagonal spread.", color="blue")
        orders = self.build_diagonal_spread_orders(underlying_asset, near_expiry, far_expiry, near_strike, far_strike, quantity, right)
        return self.execute_orders(orders, limit_type)

    def execute_ratio_spread(self, underlying_asset: Asset, expiry: date, buy_strike: float, sell_strike: float,
                             buy_qty: int, sell_qty: int, right: str, limit_type: Optional[str] = None) -> bool:
        """
        Build and submit orders for a ratio spread.

        Parameters
        ----------
        underlying_asset : Asset
            The underlying asset.
        expiry : date
            Option expiry.
        buy_strike : float
            Strike for the long leg.
        sell_strike : float
            Strike for the short leg.
        buy_qty : int
            Quantity for the long leg.
        sell_qty : int
            Quantity for the short leg.
        right : str
            Option type ("call" or "put").
        limit_type : Optional[str]
            Limit pricing type.

        Returns
        -------
        bool
            True if orders are submitted successfully.
        """
        self.strategy.log_message("Executing ratio spread.", color="blue")
        orders = self.build_ratio_spread_orders(underlying_asset, expiry, buy_strike, sell_strike, buy_qty, sell_qty, right)
        return self.execute_orders(orders, limit_type)

    # ============================================================
    # Advanced / Risk Management Functions
    # ============================================================

    def aggregate_portfolio_greeks(self, positions: List, underlying_asset: Asset, *,
                                   include_details: bool = False,
                                   underlying_price: Optional[float] = None,
                                   risk_free_rate: Optional[float] = None,
                                   query_greeks: bool = False) -> Dict[str, Any]:
        """
        Aggregate the Greeks (delta, gamma, theta, and vega) for a list of option positions.
        Useful for obtaining an overall risk profile of the options portfolio.

        Parameters
        ----------
        positions : List
            A list of position objects. Each position should have an 'asset' and a 'quantity'.
        underlying_asset : Asset
            The underlying asset.

        Returns
        -------
        Dict[str, float]
        A dictionary with contract-weighted delta, gamma, theta and vega.
        include_details adds exact-leg evidence, multiplier-scaled totals and
        completeness. In that format an unknown field stays null; it is never
        presented as a verified zero. The default retains the legacy four-key
        best-effort result for compatible direct callers.
        """
        self.strategy.log_message("Aggregating portfolio greeks.", color="blue")
        names = ("delta", "gamma", "theta", "vega")
        totals = dict.fromkeys(names, 0.0)
        scaled = dict.fromkeys(names, 0.0)
        complete = dict.fromkeys(names, True)
        legs = []
        if underlying_price is None:
            underlying_price = self.strategy.get_last_price(underlying_asset)
        greek_arguments = {"underlying_price": underlying_price}
        if risk_free_rate is not None:
            greek_arguments["risk_free_rate"] = risk_free_rate
        if query_greeks:
            greek_arguments["query_greeks"] = True
        for pos in positions:
            option = pos.asset
            failure = None
            try:
                greeks = self.strategy.get_greeks(option, **greek_arguments)
            except Exception as e:
                self.strategy.log_message(f"Error getting greeks for {option.symbol}: {e}", color="red")
                greeks, failure = None, type(e).__name__
            quantity = float(pos.quantity)
            multiplier = float(option.multiplier)
            raw, contribution = {}, {}
            for name in names:
                value = greeks.get(name) if isinstance(greeks, dict) else None
                try:
                    value = float(value)
                    if not math.isfinite(value):
                        value = None
                except (TypeError, ValueError):
                    value = None
                raw[name] = value
                contribution[name] = value * quantity if value is not None else None
                if value is None:
                    complete[name] = False
                else:
                    totals[name] += value * quantity
                    scaled[name] += value * quantity * multiplier
            legs.append({"asset": option, "quantity": quantity, "multiplier": multiplier,
                         "underlying_quantity": quantity * multiplier, "greeks": raw,
                         "contract_weighted_greeks": contribution,
                         **({"error_type": failure} if failure else {})})
        reported = {name: totals[name] if complete[name] else None for name in names}
        self.strategy.log_message(f"Aggregated Greeks: {reported if include_details else totals}", color="blue")
        if not include_details:
            return totals
        return {"greeks": reported,
                "underlying_equivalent_greeks": {name: scaled[name] if complete[name] else None for name in names},
                "complete": complete, "available": all(complete.values()), "legs": legs}

    def check_spread_profit(self, initial_cost: float, orders: List[Order], contract_multiplier: Optional[float] = None) -> Optional[float]:
        """
        Calculate the current profit or loss percentage of a spread based on updated market prices.

        Parameters
        ----------
        initial_cost : float
            Total opening cash cost: positive for a debit, negative for a credit.
            Quantities must represent the remaining exposure covered by this basis.
        orders : List[Order]
            Orders describing the original long/short directions and remaining
            quantities, not opposite-side closing orders.
        contract_multiplier : float, optional
            Explicit multiplier override. Otherwise use each asset's multiplier.

        Returns
        -------
        Optional[float]
            Marked P&L divided by the absolute opening cost, or None when
            the basis is zero or any leg's mark is unavailable. This is not an
            executable liquidation quote; fees belong in the supplied cash basis.
        """
        self.strategy.log_message("Calculating spread profit percentage.", color="blue")
        if initial_cost == 0:
            return None
        current_value = 0.0
        for order in orders:
            price = None
            if getattr(order.asset, "asset_type", None) == Asset.AssetType.OPTION:
                price, _, _ = self._get_option_mark_from_quote(order.asset)
            if price is None:
                price = self.strategy.get_last_price(order.asset)
            if price is None:
                self.strategy.log_message(f"Price unavailable for {order.asset.symbol}; cannot calculate spread profit.", color="red")
                return None
            multiplier = order.asset.multiplier if contract_multiplier is None else contract_multiplier
            signed_quantity = float(order.quantity) * (1 if order.is_buy_order() else -1)
            current_value += float(price) * signed_quantity * float(multiplier)
        profit_pct = ((current_value - initial_cost) / abs(initial_cost)) * 100
        self.strategy.log_message(f"Spread profit percentage: {profit_pct:.2f}%", color="blue")
        return profit_pct

    # ============================================================
    # End of OptionsHelper Component
    # ============================================================
