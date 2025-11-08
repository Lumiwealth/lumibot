from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
import math
from typing import Any, Dict, List, Optional, Tuple, Union
import warnings

from lumibot.entities import Asset, Order
from lumibot.entities.chains import Chains


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
        self.strategy.log_message("OptionsHelper initialized.", color="blue")

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
        """Return True when the evaluation contains a usable buy price."""
        if evaluation is None:
            return False

        price = evaluation.buy_price
        if price is None:
            return False

        try:
            price = float(price)
        except (TypeError, ValueError):
            return False

        return math.isfinite(price) and price > 0 and not evaluation.spread_too_wide

    # ============================================================
    # Basic Utility Functions
    # ============================================================

    def find_next_valid_option(self, underlying_asset: Asset, rounded_underlying_price: float,
                                 expiry: date, put_or_call: str = "call") -> Optional[Asset]:
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
        self.strategy.log_message(f"Finding next valid option for {underlying_asset.symbol} at strike {rounded_underlying_price} and expiry {expiry}", color="blue")

        expiry_attempts = 0
        while expiry_attempts < 10:
            # Check if this expiry was previously marked as invalid
            for record in self.non_existing_expiry_dates:
                if (record["underlying_asset_symbol"] == underlying_asset.symbol and
                    record["expiry"] == expiry):
                    self.strategy.log_message(f"Expiry {expiry} previously invalid for {underlying_asset.symbol}; trying next day.", color="yellow")
                    expiry += timedelta(days=1)
                    expiry_attempts += 1
                    continue

            # Try to get option chain to find available strikes
            try:
                chains = self.strategy.get_chains(underlying_asset)
                self.strategy.log_message(f"Got chains for {underlying_asset.symbol}: {bool(chains)}", color="cyan")

                if chains:
                    # Get available strikes for this expiry
                    available_strikes = chains.strikes(expiry, put_or_call.upper())
                    self.strategy.log_message(f"Available strikes for {expiry}: {len(available_strikes) if available_strikes else 0} strikes", color="cyan")

                    if not available_strikes:
                        self.strategy.log_message(f"No strikes available for {put_or_call.upper()} on {expiry}; trying next expiry.", color="yellow")
                        self.non_existing_expiry_dates.append({
                            "underlying_asset_symbol": underlying_asset.symbol,
                            "expiry": expiry,
                        })
                        expiry += timedelta(days=1)
                        expiry_attempts += 1
                        continue

                    # Find the closest strike to our target
                    closest_strike = min(available_strikes, key=lambda x: abs(x - rounded_underlying_price))
                    self.strategy.log_message(f"Target strike {rounded_underlying_price} -> Closest available strike: {closest_strike}", color="green")

                    # Create option with the closest available strike
                    option = Asset(
                        underlying_asset.symbol,
                        asset_type="option",
                        expiration=expiry,
                        strike=closest_strike,
                        right=put_or_call,
                        underlying_asset=underlying_asset,
                    )

                    # Verify this option has price data
                    try:
                        quote = self.strategy.get_quote(option)
                        has_valid_quote = quote and (quote.bid is not None or quote.ask is not None)
                        if has_valid_quote:
                            self.strategy.log_message(f"Found valid option: {option.symbol} {option.right} {option.strike} exp {option.expiration}", color="green")
                            return option
                    except Exception as e:
                        self.strategy.log_message(f"Error getting quote for {option.symbol}: {e}", color="yellow")

                    # Fallback to last price
                    try:
                        price = self.strategy.get_last_price(option)
                        if price is not None:
                            self.strategy.log_message(f"Found valid option (via last price): {option.symbol} {option.right} {option.strike} exp {option.expiration}", color="green")
                            return option
                    except Exception as e:
                        pass

                    # If closest strike didn't work, this expiry might be invalid
                    self.strategy.log_message(f"Could not get price data for strike {closest_strike} on {expiry}; trying next expiry.", color="yellow")
                    self.non_existing_expiry_dates.append({
                        "underlying_asset_symbol": underlying_asset.symbol,
                        "expiry": expiry,
                    })
                    expiry += timedelta(days=1)
                    expiry_attempts += 1
                    continue

            except Exception as e:
                self.strategy.log_message(f"Error getting chains for {underlying_asset.symbol}: {e}; falling back to direct strike attempt.", color="yellow")
                # Fallback: Try the exact strike requested (old behavior)
                option = Asset(
                    underlying_asset.symbol,
                    asset_type="option",
                    expiration=expiry,
                    strike=rounded_underlying_price,
                    right=put_or_call,
                    underlying_asset=underlying_asset,
                )

                try:
                    price = self.strategy.get_last_price(option)
                    if price is not None:
                        return option
                except Exception:
                    pass

            # No valid option found for this expiry, try next day
            self.strategy.log_message(f"No valid option found for expiry {expiry}; trying next expiry.", color="yellow")
            self.non_existing_expiry_dates.append({
                "underlying_asset_symbol": underlying_asset.symbol,
                "expiry": expiry,
            })
            expiry += timedelta(days=1)
            expiry_attempts += 1

        self.strategy.log_message("Exceeded maximum attempts to find a valid option.", color="red")
        return None

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
        self.strategy.log_message(f"Computing strike deltas for {underlying_asset.symbol} at expiry {expiry}.", color="blue")
        strike_deltas: Dict[float, Optional[float]] = {}
        underlying_price = self.strategy.get_last_price(underlying_asset)
        for strike in strikes:
            option = Asset(
                underlying_asset.symbol,
                asset_type="option",
                expiration=expiry,
                strike=strike,
                right=right,
                underlying_asset=underlying_asset,
            )
            if self.strategy.get_last_price(option) is None:
                self.strategy.log_message(f"No price for option at strike {strike}. Skipping.", color="yellow")
                continue
            greeks = self.strategy.get_greeks(option, underlying_price=underlying_price)
            delta = greeks.get("delta")
            strike_deltas[strike] = delta
            self.strategy.log_message(f"Strike {strike}: delta = {delta}", color="blue")
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
        self.strategy.log_message(f"Getting delta for {underlying_asset.symbol} at strike {strike}, expiry {expiry}", color="blue")
        option = Asset(
            underlying_asset.symbol,
            asset_type="option",
            expiration=expiry,
            strike=strike,
            right=right,
            underlying_asset=underlying_asset,
        )
        if self.strategy.get_last_price(option) is None:
            self.strategy.log_message(f"No price for option {option.symbol} at strike {strike}", color="yellow")
            return None
        greeks = self.strategy.get_greeks(option, underlying_price=underlying_price)
        delta = greeks.get("delta")
        self.strategy.log_message(f"Delta for strike {strike} is {delta}", color="blue")
        return delta

    def find_strike_for_delta(self, underlying_asset: Asset, underlying_price: float,
                              target_delta: float, expiry: date, right: str) -> Optional[float]:
        """
        Find the strike whose delta is closest to the target delta using binary search.
        (This function replaces the older "find_strike_for_delta_original".)

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
        self.strategy.log_message(
            f"🎯 STRIKE SEARCH: Finding strike for {underlying_asset.symbol} "
            f"(underlying_price=${underlying_price}, target_delta={target_delta}, right={right}, expiry={expiry})", 
            color="blue"
        )
        
        # Validate input parameters
        if underlying_price <= 0:
            self.strategy.log_message(f"❌ ERROR: Invalid underlying price {underlying_price}", color="red")
            return None
            
        if target_delta is None:
            self.strategy.log_message(f"❌ ERROR: target_delta is None", color="red")
            return None
            
        if abs(target_delta) > 1:
            self.strategy.log_message(f"❌ ERROR: Invalid target delta {target_delta} (should be between -1 and 1)", color="red")
            return None
        
        low_strike = int(underlying_price - 20)
        high_strike = int(underlying_price + 30)
        
        # Ensure strikes are positive
        low_strike = max(1, low_strike)
        
        self.strategy.log_message(
            f"🔍 Search range: strikes {low_strike} to {high_strike} (underlying=${underlying_price})", 
            color="blue"
        )
        
        closest_strike: Optional[float] = None
        closest_delta: Optional[float] = None

        while low_strike <= high_strike:
            mid_strike = (low_strike + high_strike) // 2
            self.strategy.log_message(f"🔎 Trying strike {mid_strike} (range: {low_strike}-{high_strike})", color="blue")
            
            mid_delta = self.get_delta_for_strike(underlying_asset, underlying_price, mid_strike, expiry, right)
            if mid_delta is None:
                self.strategy.log_message(f"⚠️  Mid delta at strike {mid_strike} is None; adjusting search.", color="yellow")
                high_strike -= 1
                continue

            self.strategy.log_message(f"📈 Strike {mid_strike} has delta {mid_delta:.4f} (target: {target_delta})", color="blue")

            if abs(mid_delta - target_delta) < 0.001:
                self.strategy.log_message(f"🎯 Exact match found at strike {mid_strike} with delta {mid_delta:.4f}", color="green")
                return mid_strike

            if mid_delta < target_delta:
                high_strike = mid_strike - 1
            else:
                low_strike = mid_strike + 1

            if closest_delta is None or abs(mid_delta - target_delta) < abs(closest_delta - target_delta):
                closest_delta = mid_delta
                closest_strike = mid_strike
                self.strategy.log_message(f"📊 New closest strike: {mid_strike} (delta {mid_delta:.4f})", color="blue")

        if closest_strike is not None:
            self.strategy.log_message(
                f"✅ RESULT: Closest strike {closest_strike} with delta {closest_delta:.4f} "
                f"(target was {target_delta})", 
                color="green"
            )
            
            # Sanity check the result
            if underlying_price > 50 and closest_strike < 10:
                self.strategy.log_message(
                    f"⚠️  WARNING: Strike {closest_strike} seems too low for underlying price ${underlying_price}. "
                    f"This might indicate a data issue.",
                    color="red"
                )
        else:
            self.strategy.log_message(f"❌ No valid strike found for target delta {target_delta}", color="red")
            
        return closest_strike

    def calculate_multileg_limit_price(self, orders: List[Order], limit_type: str) -> Optional[float]:
        """
        Calculate an aggregate limit price for a multi-leg order by combining quotes from each leg.

        Parameters
        ----------
        orders : List[Order]
            List of orders (each order has an Asset).
        limit_type : str
            One of "best", "fastest", or "mid" indicating which price to use.

        Returns
        -------
        Optional[float]
            The aggregated limit price, or None if quotes are missing.
        """
        self.strategy.log_message("Calculating multi-leg limit price.", color="blue")
        quotes: List[float] = []
        for order in orders:
            asset = order.asset
            if asset.asset_type != Asset.AssetType.OPTION:
                continue
            try:
                quote = self.strategy.get_quote(asset)
                self.strategy.log_message(f"Quote for {asset.symbol}: bid={quote.bid}, ask={quote.ask}", color="blue")
            except Exception as e:
                self.strategy.log_message(f"Error fetching quote for {asset.symbol}: {e}", color="red")
                continue
            if not quote or quote.ask is None or quote.bid is None:
                self.strategy.log_message(f"Missing quote for {asset.symbol}", color="red")
                continue
            if limit_type == "mid":
                mid = (quote.ask + quote.bid) / 2
                quotes.append(mid if order.side.lower() == "buy" else -mid)
            elif limit_type == "best":
                quotes.append(quote.bid if order.side.lower() == "buy" else -quote.ask)
            elif limit_type == "fastest":
                quotes.append(quote.ask if order.side.lower() == "buy" else -quote.bid)
        if not quotes:
            self.strategy.log_message("No valid quotes for calculating limit price.", color="red")
            return None
        limit_price = sum(quotes)
        self.strategy.log_message(f"Calculated limit price: {limit_price}", color="green")
        return limit_price

    def evaluate_option_market(
        self,
        option_asset: Asset,
        max_spread_pct: Optional[float] = None,
    ) -> OptionMarketEvaluation:
        """Evaluate available quote data for an option and produce execution anchors.

        Parameters
        ----------
        option_asset : Asset
            The option to evaluate.
        max_spread_pct : float, optional
            Maximum acceptable bid/ask spread as a fraction (e.g. 0.25 for 25%).

        Returns
        -------
        OptionMarketEvaluation
            Dataclass containing quote fields, derived spread information, and
            suggested buy/sell prices (with automatic fallback when the data
            source allows it).
        """

        data_source = getattr(getattr(self.strategy, "broker", None), "data_source", None)
        allow_fallback = bool(getattr(data_source, "option_quote_fallback_allowed", False))

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

        # Attempt to get quotes first
        quote = None
        try:
            quote = self.strategy.get_quote(option_asset)
        except Exception as exc:
            self.strategy.log_message(
                f"Error fetching quote for {option_asset}: {exc}",
                color="red",
            )

        if quote and quote.bid is not None and quote.ask is not None:
            bid = self._coerce_price(quote.bid, "bid", data_quality_flags, sanitization_notes)
            ask = self._coerce_price(quote.ask, "ask", data_quality_flags, sanitization_notes)
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

        # Last price as secondary signal / fallback anchor
        try:
            last_price = self.strategy.get_last_price(option_asset)
        except Exception as exc:
            self.strategy.log_message(
                f"Error fetching last price for {option_asset}: {exc}",
                color="red",
            )

        if last_price is None:
            missing_last_price = True
        else:
            last_price = self._coerce_price(last_price, "last_price", data_quality_flags, sanitization_notes)
            if last_price is None:
                missing_last_price = True

        if not has_bid_ask and allow_fallback and last_price is not None:
            buy_price = last_price
            sell_price = last_price
            used_last_price_fallback = True
            self.strategy.log_message(
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

        # Compose log message
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
        self.strategy.log_message(
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

        return OptionMarketEvaluation(
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
        details = {
            "symbol": asset.symbol,
            "strike": getattr(asset, "strike", None),
            "expiration": getattr(asset, "expiration", None),
            "right": getattr(asset, "right", None),
            "side": order.side,
            "last_price": self.strategy.get_last_price(asset)
        }
        self.strategy.log_message(f"Order details: {details}", color="blue")
        return details

    def _chain_hint(self, min_expiration_date):
        """Temporarily set chain constraints on the underlying data source."""
        broker = getattr(self.strategy, "broker", None)
        data_source = getattr(broker, "data_source", None) if broker else None

        class _ChainHintContext:
            def __init__(self, ds, min_dt):
                self.ds = ds
                self.min_dt = min_dt
                self.prev = None

            def __enter__(self):
                if not self.ds:
                    return
                self.prev = getattr(self.ds, "_chain_constraints", None)
                self.ds._chain_constraints = {"min_expiration_date": self.min_dt}

            def __exit__(self, exc_type, exc_val, exc_tb):
                if not self.ds:
                    return
                if self.prev is None:
                    if hasattr(self.ds, "_chain_constraints"):
                        delattr(self.ds, "_chain_constraints")
                else:
                    self.ds._chain_constraints = self.prev

        return _ChainHintContext(data_source, min_expiration_date)

    def get_expiration_on_or_after_date(self, dt: Union[date, datetime], chains: Union[Dict[str, Any], Chains], call_or_put: str, underlying_asset: Optional[Asset] = None) -> Optional[date]:
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

        # Convert string expiries to dates for comparison
        expiration_dates: List[Tuple[str, date]] = _try_resolve_expiration(specific_chain)
        future_candidates = [(s, d) for s, d in expiration_dates if d >= dt]

        # If we couldn't find any expirations beyond the requested date, attempt a deeper fetch
        if not future_candidates and underlying_asset is not None:
            self.strategy.log_message(
                f"No expirations >= {dt} found in cached chains; requesting extended range...",
                color="yellow",
            )
            with self._chain_hint(dt):
                refreshed_chains = self.strategy.get_chains(underlying_asset)
            if refreshed_chains:
                chains_map = refreshed_chains if isinstance(refreshed_chains, dict) else {}
                options_map = chains_map.get("Chains") if isinstance(chains_map.get("Chains"), dict) else None
                if options_map:
                    specific_chain = options_map.get(call_or_put_caps) if isinstance(options_map.get(call_or_put_caps), dict) else None
                    if specific_chain:
                        expiration_dates = _try_resolve_expiration(specific_chain)
                        future_candidates = [(s, d) for s, d in expiration_dates if d >= dt]
                        chain_refetched = True
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

        # Check each candidate expiry to find one with valid data
        for exp_str, exp_date in future_candidates:
            strikes = specific_chain.get(exp_str)
            if strikes and len(strikes) > 0:
                # Check if at least one strike has valid data
                # Pick a strike near the middle (likely to be ATM and have data)
                test_strike = strikes[len(strikes) // 2] if isinstance(strikes, list) else list(strikes)[len(strikes) // 2]

                # Try to get the underlying symbol from the first available asset
                if underlying_symbol:
                    test_option = Asset(
                        underlying_symbol,
                        asset_type="option",
                        expiration=exp_date,
                        strike=float(test_strike),
                        right=call_or_put,
                    )

                    # Check if this option has tradeable data
                    try:
                        quote = self.strategy.get_quote(test_option)
                        has_valid_quote = quote and (quote.bid is not None or quote.ask is not None)
                        if has_valid_quote:
                            self.strategy.log_message(f"Found valid expiry {exp_date} with quote data for {call_or_put_caps}", color="blue")
                            return exp_date
                    except:
                        pass

                    # Fallback to checking last price
                    try:
                        price = self.strategy.get_last_price(test_option)
                        if price is not None:
                            self.strategy.log_message(f"Found valid expiry {exp_date} with price data for {call_or_put_caps}", color="blue")
                            return exp_date
                    except:
                        pass
                else:
                    # If we can't determine underlying, assume the expiry is valid (backward compatibility)
                    self.strategy.log_message(f"Cannot validate data without underlying symbol, returning {exp_date}", color="yellow")
                    return exp_date

        # No future expirations with tradeable data; let the caller skip entries gracefully.
        msg = f"No valid expirations on or after {dt} with tradeable data for {call_or_put_caps}; skipping."
        self.strategy.log_message(msg, color="yellow")
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
        call_sell_price = self.strategy.get_last_price(call_sell_asset)
        call_sell_order = self.strategy.create_order(call_sell_asset, quantity_to_trade, "sell")
        call_buy_asset = Asset(
            underlying_asset.symbol, asset_type="option", expiration=expiry,
            strike=call_strike + wing_size, right="call", underlying_asset=underlying_asset
        )
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
        put_sell_price = self.strategy.get_last_price(put_sell_asset)
        put_sell_order = self.strategy.create_order(put_sell_asset, quantity_to_trade, "sell")
        put_buy_asset = Asset(
            underlying_asset.symbol, asset_type="option", expiration=expiry,
            strike=put_strike - wing_size, right="put", underlying_asset=underlying_asset
        )
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

    def aggregate_portfolio_greeks(self, positions: List, underlying_asset: Asset) -> Dict[str, float]:
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
            A dictionary with aggregated values for "delta", "gamma", "theta", and "vega".
        """
        self.strategy.log_message("Aggregating portfolio greeks.", color="blue")
        total_delta = total_gamma = total_theta = total_vega = 0.0
        underlying_price = self.strategy.get_last_price(underlying_asset)
        for pos in positions:
            option = pos.asset
            try:
                greeks = self.strategy.get_greeks(option, underlying_price=underlying_price)
            except Exception as e:
                self.strategy.log_message(f"Error getting greeks for {option.symbol}: {e}", color="red")
                continue
            quantity = pos.quantity
            total_delta += greeks.get("delta", 0) * quantity
            total_gamma += greeks.get("gamma", 0) * quantity
            total_theta += greeks.get("theta", 0) * quantity
            total_vega  += greeks.get("vega", 0) * quantity
        aggregated = {
            "delta": total_delta,
            "gamma": total_gamma,
            "theta": total_theta,
            "vega": total_vega
        }
        self.strategy.log_message(f"Aggregated Greeks: {aggregated}", color="blue")
        return aggregated

    def check_spread_profit(self, initial_cost: float, orders: List[Order], contract_multiplier: int = 100) -> Optional[float]:
        """
        Calculate the current profit or loss percentage of a spread based on updated market prices.

        Parameters
        ----------
        initial_cost : float
            The initial net cost (or credit) of establishing the spread.
        orders : List[Order]
            The list of orders that constitute the spread.
        contract_multiplier : int, optional
            The Option contract multiplier to use (default is 100) 

        Returns
        -------
        Optional[float]
            The profit/loss percentage relative to the initial cost, or None if any leg's price is unavailable.
        """
        self.strategy.log_message("Calculating spread profit percentage.", color="blue")
        current_value = 0.0
        for order in orders:
            price = self.strategy.get_last_price(order.asset)
            if price is None:
                self.strategy.log_message(f"Price unavailable for {order.asset.symbol}; cannot calculate spread profit.", color="red")
                return None
            multiplier = -1 if order.side.lower() == "buy" else 1
            current_value += price * order.quantity * contract_multiplier
        profit_pct = ((current_value - initial_cost) / initial_cost) * 100
        self.strategy.log_message(f"Spread profit percentage: {profit_pct:.2f}%", color="blue")
        return profit_pct

    # ============================================================
    # End of OptionsHelper Component
    # ============================================================
