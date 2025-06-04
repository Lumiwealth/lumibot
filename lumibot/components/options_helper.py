from datetime import timedelta, date, datetime
from typing import Optional, List, Dict, Union, Tuple
from lumibot.entities import Asset, Order

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
        self.strategy.log_message("OptionsHelper initialized.", color="blue")

    # ============================================================
    # Basic Utility Functions
    # ============================================================

    def find_next_valid_option(self, underlying_asset: Asset, rounded_underlying_price: float,
                                 expiry: date, put_or_call: str = "call") -> Optional[Asset]:
        """
        Find a valid option with the given expiry and strike.
        If no option is available (e.g. due to holidays/weekends), increment the expiry day-by-day
        (up to 10 times) until a valid option is found.

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
        loop_counter = 0
        while True:
            for record in self.non_existing_expiry_dates:
                if (record["underlying_asset_symbol"] == underlying_asset.symbol and 
                    record["expiry"] == expiry):
                    self.strategy.log_message(f"Expiry {expiry} previously invalid for {underlying_asset.symbol}; trying next day.", color="yellow")
                    expiry += timedelta(days=1)
                    continue

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
                self.strategy.log_message(f"Price for option {option.symbol} at expiry {expiry} is {price}", color="blue")
            except Exception as e:
                self.strategy.log_message(f"Error getting price for {option.symbol}: {e}", color="red")
                price = None

            if price is not None:
                return option

            self.strategy.log_message(f"No price found for option {option.symbol} at expiry {expiry}.", color="yellow")
            self.non_existing_expiry_dates.append({
                "underlying_asset_symbol": underlying_asset.symbol,
                "expiry": expiry,
            })
            expiry += timedelta(days=1)
            loop_counter += 1
            if loop_counter >= 10:
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
        self.strategy.log_message(f"Finding strike for target delta {target_delta} on {underlying_asset.symbol}", color="blue")
        low_strike = int(underlying_price - 20)
        high_strike = int(underlying_price + 30)
        closest_strike: Optional[float] = None
        closest_delta: Optional[float] = None

        while low_strike <= high_strike:
            mid_strike = (low_strike + high_strike) // 2
            mid_delta = self.get_delta_for_strike(underlying_asset, underlying_price, mid_strike, expiry, right)
            if mid_delta is None:
                self.strategy.log_message(f"Mid delta at strike {mid_strike} is None; adjusting search.", color="yellow")
                high_strike -= 1
                continue

            if abs(mid_delta - target_delta) < 0.001:
                self.strategy.log_message(f"Exact match found at strike {mid_strike} with delta {mid_delta}", color="green")
                return mid_strike

            if mid_delta < target_delta:
                high_strike = mid_strike - 1
            else:
                low_strike = mid_strike + 1

            if closest_delta is None or abs(mid_delta - target_delta) < abs(closest_delta - target_delta):
                closest_delta = mid_delta
                closest_strike = mid_strike
                self.strategy.log_message(f"New closest strike: {mid_strike} (delta {mid_delta})", color="blue")

        self.strategy.log_message(f"Closest strike found: {closest_strike} with delta {closest_delta}", color="blue")
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
                self.strategy.log_message(f"Quote for {asset.symbol}: bid={quote.get('bid')}, ask={quote.get('ask')}", color="blue")
            except Exception as e:
                self.strategy.log_message(f"Error fetching quote for {asset.symbol}: {e}", color="red")
                continue
            if not quote or quote.get("ask") is None or quote.get("bid") is None:
                self.strategy.log_message(f"Missing quote for {asset.symbol}", color="red")
                continue
            if limit_type == "mid":
                mid = (quote["ask"] + quote["bid"]) / 2
                quotes.append(mid if order.side.lower() == "buy" else -mid)
            elif limit_type == "best":
                quotes.append(quote["bid"] if order.side.lower() == "buy" else -quote["ask"])
            elif limit_type == "fastest":
                quotes.append(quote["ask"] if order.side.lower() == "buy" else -quote["bid"])
        if not quotes:
            self.strategy.log_message("No valid quotes for calculating limit price.", color="red")
            return None
        limit_price = sum(quotes)
        self.strategy.log_message(f"Calculated limit price: {limit_price}", color="green")
        return limit_price

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
        self.strategy.log_message(f"Checking liquidity for {option_asset.symbol}", color="blue")
        try:
            quote = self.strategy.get_quote(option_asset)
        except Exception as e:
            self.strategy.log_message(f"Error fetching quote for liquidity check on {option_asset.symbol}: {e}", color="red")
            return False
        if not quote or quote.get("bid") is None or quote.get("ask") is None:
            self.strategy.log_message(f"Liquidity check: Missing quote for {option_asset.symbol}", color="red")
            return False
        bid = quote["bid"]
        ask = quote["ask"]
        mid = (bid + ask) / 2
        spread_pct = (ask - bid) / mid
        self.strategy.log_message(f"{option_asset.symbol} liquidity spread: {spread_pct:.2%}", color="blue")
        return spread_pct <= max_spread_pct

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
    
    def get_expiration_on_or_after_date(self, dt: date, chains: dict, call_or_put: str) -> date:
        """
        Get the expiration date that is on or after a given date.

        Parameters
        ----------
        dt : date
            The starting date.
        chains : dict
            A dictionary containing option chains.
        call_or_put : str
            One of "call" or "put".

        Returns
        -------
        date
            The adjusted expiration date.
        """
        
        # Make it all caps and get the specific chain.
        call_or_put_caps = call_or_put.upper()
        specific_chain = chains["Chains"][call_or_put_caps]

        # Get the list of expiration dates as strings.
        expiration_dates = list(specific_chain.keys())

        # Since dt is a date object and expiration_dates contains strings, dt won't be found.
        # Find the closest expiration date (as a string) and convert it back to a date.
        if dt not in expiration_dates:
            closest_str = min(expiration_dates, key=lambda x: abs(datetime.strptime(x, "%Y-%m-%d").date() - dt))
            dt = datetime.strptime(closest_str, "%Y-%m-%d").date()

        return dt

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
