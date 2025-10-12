from datetime import timedelta, date
import math
from decimal import Decimal

from lumibot.strategies.strategy import Strategy
from lumibot.traders import Trader
from lumibot.entities import Asset, Order, TradingFee
from lumibot.backtesting import ThetaDataBacktesting
from lumibot.credentials import IS_BACKTESTING
from lumibot.components.options_helper import OptionsHelper

class AAPLDeepDipCalls(Strategy):
    """
    Strategy Description
    --------------------
    Buys 2-year, ~20% out-of-the-money call options on TSLA after a 25% decline from the running high since the bot started.
    Exits the calls after holding for approximately 1 year, or if protective rules trigger earlier (stop-loss or near-expiration exit).

    This code was refined based on the user prompt: 'change it from aapl to use tesla'
    """

    # User-tunable parameters kept together for easy changes
    parameters = {
        "symbol": "TSLA",               # CHANGED: Underlying switched to TSLA
        "decline_trigger_pct": 0.25,      # Buy trigger: 25% drop from running high
        # RECOVERY EXIT REMOVED (time-based exit instead)
        "hold_duration_days": 365,        # Sell after holding for ~1 year
        "otm_pct": 0.20,                  # Target 20% OTM for the call strike
        "target_exp_days": 730,           # About two years
        "risk_per_trade": 0.95,           # Use up to 95% of available cash for contracts
        "max_contracts": 50,              # Safety cap on number of contracts
        "stop_loss_pct": 0.50,            # Exit if the option premium is down 50%
        "min_days_to_expiry_exit": 30,    # Exit 30 days before expiration if still open
        "max_spread_pct": 0.35,           # Skip illiquid options with very wide spreads (when quotes are available)
        "sleeptime": "1D",               # Check once per trading day
    }

    def initialize(self):
        # Runs once when the bot starts. We keep simple daily check since this is a swing-style options plan.
        self.sleeptime = self.parameters.get("sleeptime", "1D")
        # Helper to discover expirations/strikes and execute multi-leg orders
        self.options_helper = OptionsHelper(self)

        # Persistent state for tracking signals and positions
        self.vars.ath_price = None                  # Running all-time high of underlying since bot start
        self.vars.option_asset = None               # The specific option we own (if any)
        self.vars.entry_stock_price = None          # Underlying stock price at the time we bought the calls (kept for logs)
        self.vars.entry_option_price = None         # Option fill price when we bought
        self.vars.entry_datetime = None             # Timestamp of entry
        self.vars.contracts = 0                     # Number of option contracts held

        # Small label for logs and chart usage
        self.vars.label = "TSLA 2Y 20% OTM Calls on 25% Dip (1Y Hold)"  # CHANGED: label to TSLA

    def on_trading_iteration(self):
        # This runs on each iteration (daily by default). It checks for buy/sell conditions and manages risk.
        params = self.get_parameters()
        symbol = params["symbol"]
        decline_trigger = params["decline_trigger_pct"]
        hold_duration_days = params["hold_duration_days"]  # 1-year hold parameter
        otm_pct = params["otm_pct"]
        stop_loss_pct = params["stop_loss_pct"]
        max_spread_pct = params["max_spread_pct"]
        min_days_to_expiry_exit = params["min_days_to_expiry_exit"]

        underlying = Asset(symbol, asset_type=Asset.AssetType.STOCK)
        last = self.get_last_price(underlying)
        if last is None:
            self.log_message(f"No price available for {symbol} right now. Will try again next iteration.", color="yellow")
            return

        # Draw simple lines for context: price and the running high
        self.add_line(symbol, float(last), color="black", width=2, detail_text=f"{symbol} Price")

        # Update running high (ATH) since the bot started
        if self.vars.ath_price is None or last > self.vars.ath_price:
            self.vars.ath_price = float(last)
        self.add_line(f"{symbol}_ATH", float(self.vars.ath_price), color="blue", width=1, detail_text="Running High")

        # Check if we currently hold the target option position
        open_pos = None
        if self.vars.option_asset is not None:
            open_pos = self.get_position(self.vars.option_asset)
            if open_pos is None:
                # Clean up stale state if position closed elsewhere
                self._clear_position_state()

        # If not in a position, look for the buy signal (25% below running high)
        if open_pos is None:
            buy_trigger_price = self.vars.ath_price * (1.0 - decline_trigger)
            if last <= buy_trigger_price:
                self._attempt_buy_option(underlying, last, otm_pct, max_spread_pct)
            else:
                self.log_message(
                    f"No buy: {symbol} at {last:.2f} is above dip trigger {buy_trigger_price:.2f}", color="white"
                )
            return

        # If we have a position, monitor for exits: 1-year hold, stop-loss on option, or near expiration

        # 1) Exit after holding the option for hold_duration_days (~1 year)
        if self.vars.entry_datetime is not None:
            days_held = (self.get_datetime().date() - self.vars.entry_datetime.date()).days
            if days_held >= hold_duration_days:
                self.log_message(
                    f"Exit on 1-year hold: Held {days_held} days (>= {hold_duration_days})", color="blue"
                )
                self._close_option_position()
                return

        # 2) Exit on option stop loss (-50% by default)
        if self.vars.option_asset is not None and self.vars.entry_option_price is not None:
            opt_mid = self._get_option_mid(self.vars.option_asset)
            if opt_mid is not None:
                if opt_mid <= self.vars.entry_option_price * (1.0 - stop_loss_pct):
                    self.log_message(
                        f"Exit on stop-loss: option mid {opt_mid:.2f} <= {self.vars.entry_option_price * (1.0 - stop_loss_pct):.2f}",
                        color="red",
                    )
                    self._close_option_position()
                    return
            else:
                self.log_message("Option quote unavailable for stop-loss check; holding for now.", color="yellow")

        # 3) Time-based exit: close before expiration if we are within the cutoff window
        if self.vars.option_asset is not None and isinstance(self.vars.option_asset.expiration, date):
            days_to_expiry = (self.vars.option_asset.expiration - self.get_datetime().date()).days
            if days_to_expiry <= min_days_to_expiry_exit:
                self.log_message(
                    f"Exit near expiration: {days_to_expiry} days to expiry <= {min_days_to_expiry_exit}", color="blue"
                )
                self._close_option_position()
                return

        # If none of the exit conditions are met, we simply continue to the next iteration
        self.log_message("Holding position; exit conditions not met.", color="white")

    def _attempt_buy_option(self, underlying: Asset, stock_price: float, otm_pct: float, max_spread_pct: float):
        """Select a ~20% OTM call with ~2y expiry and place a limit buy using OptionsHelper evaluation for price/liquidity."""
        params = self.get_parameters()
        symbol = params["symbol"]
        risk_per_trade = params["risk_per_trade"]
        target_exp_days = params["target_exp_days"]
        max_contracts = int(params["max_contracts"]) if params.get("max_contracts") is not None else 50

        # Determine a target expiration around two years out, then pick the closest available from the chains
        target_exp_dt = self.get_datetime() + timedelta(days=int(target_exp_days))
        chains = self.get_chains(underlying)
        if not chains:
            self.log_message("Could not retrieve options chains; skipping buy.", color="yellow")
            return

        # CRITICAL: pass underlying_asset to validate expiries with data
        expiry_date = self.options_helper.get_expiration_on_or_after_date(target_exp_dt, chains, "call", underlying_asset=underlying)
        if expiry_date is None:
            # Fallback: choose the farthest available expiration if helper returns None
            call_chains = chains.get("Chains", {}).get("CALL")
            if not call_chains:
                self.log_message("No CALL chains found; skipping buy.", color="yellow")
                return
            available_exps = list(call_chains.keys())
            # Choose latest available
            try:
                parsed = [self.get_datetime().__class__.strptime(e, "%Y-%m-%d").date() for e in available_exps]
                if not parsed:
                    self.log_message("No valid expiration dates in chains; skipping buy.", color="yellow")
                    return
                expiry_date = max(parsed)
            except Exception:
                self.log_message("Error parsing expirations; skipping buy.", color="yellow")
                return

        # Pick the strike nearest to 20% OTM from the current stock price
        strike_target = stock_price * (1.0 + otm_pct)
        call_chains = chains.get("Chains", {}).get("CALL")
        if not call_chains:
            self.log_message("No CALL chains data; skipping buy.", color="yellow")
            return
        expiry_str = expiry_date.strftime("%Y-%m-%d")
        strikes = call_chains.get(expiry_str)
        if strikes is None or len(strikes) == 0:
            self.log_message(f"No strikes for expiration {expiry_str}; skipping buy.", color="yellow")
            return

        # Convert to a python list for processing
        try:
            strike_list = list(strikes)
        except Exception:
            strike_list = strikes

        chosen_strike = min(strike_list, key=lambda s: abs(float(s) - strike_target))

        # Build the initial option asset we intend to trade
        option_asset = Asset(
            symbol,
            asset_type=Asset.AssetType.OPTION,
            expiration=expiry_date,
            strike=float(chosen_strike),
            right=Asset.OptionRight.CALL,
        )

        # Use OptionsHelper to validate marketability and derive executable limit price
        # First, try to adjust to the nearest valid contract if needed
        validated_option = self.options_helper.find_next_valid_option(
            underlying, float(chosen_strike), expiry_date, "call"
        )
        if validated_option is not None:
            option_asset = validated_option  # prefer validated contract

        # Evaluate liquidity/spread and get a buy_price to drive our limit order
        evaluation = self.options_helper.evaluate_option_market(option_asset, max_spread_pct=max_spread_pct)
        # Log the evaluation summary for traceability
        self.log_message(
            f"Option eval -> bid: {evaluation.bid}, ask: {evaluation.ask}, last: {evaluation.last_price}, spread%: {evaluation.spread_pct}, too_wide: {evaluation.spread_too_wide}",
            color="white",
        )

        if evaluation.spread_too_wide or evaluation.buy_price is None:
            self.log_message("Skipping entry: no executable price or spread too wide.", color="yellow")
            return

        limit_price = float(evaluation.buy_price)
        if limit_price <= 0:
            self.log_message("Invalid limit price computed; skipping buy.", color="yellow")
            return

        # Position sizing: spend up to the risk_per_trade portion of cash, rounded to whole contracts
        cash = self.get_cash()
        contracts = int(math.floor((cash * float(risk_per_trade)) / (limit_price * 100.0)))
        if max_contracts is not None:
            contracts = min(contracts, max_contracts)
        if contracts < 1:
            self.log_message(
                f"Insufficient cash for even 1 contract at ~${limit_price:.2f} (x100). Holding cash.", color="yellow"
            )
            return

        # Place a limit BUY using the evaluated buy price
        order = self.create_order(option_asset, contracts, Order.OrderSide.BUY, limit_price=limit_price)
        submitted = self.submit_order(order)
        if submitted is not None:
            self.log_message(
                f"Submitted BUY {contracts}x {symbol} {expiry_str} {option_asset.strike:.2f}C at ~${limit_price:.2f} (limit)",
                color="green",
            )
        else:
            self.log_message("Order submission failed; will try again next iteration.", color="red")

    def _close_option_position(self):
        # Gracefully close the current option position if present
        if self.vars.option_asset is None:
            self.log_message("No option position to close.", color="yellow")
            return
        pos = self.get_position(self.vars.option_asset)
        if pos is None or pos.quantity is None or pos.quantity == 0:
            self.log_message("Position already closed.", color="white")
            self._clear_position_state()
            return
        qty = int(pos.quantity)
        order = self.create_order(self.vars.option_asset, qty, Order.OrderSide.SELL)  # Market sell for a clean exit
        submitted = self.submit_order(order)
        if submitted is not None:
            self.log_message(
                f"Submitted SELL {qty}x {self.vars.option_asset.symbol} {self.vars.option_asset.expiration} {self.vars.option_asset.strike:.2f}C",
                color="red",
            )
        else:
            self.log_message("Sell order submission failed; will attempt again next iteration.", color="red")

    def _clear_position_state(self):
        # Reset all trade-related state after we are flat
        self.vars.option_asset = None
        self.vars.entry_stock_price = None
        self.vars.entry_option_price = None
        self.vars.entry_datetime = None
        self.vars.contracts = 0

    def _get_option_mid(self, option_asset: Asset):
        # Helper to get the option's mid price with a fallback to last if needed
        q = self.get_quote(option_asset)
        if q is not None and q.mid_price is not None:
            return float(q.mid_price)
        lp = self.get_last_price(option_asset)
        return float(lp) if lp is not None else None

    # Order lifecycle hooks to record fills and show chart markers for important events
    def on_filled_order(self, position, order, price, quantity, multiplier):
        asset = order.asset
        if asset is None:
            return

        # If we just bought the option, capture entry details
        if order.side in [Order.OrderSide.BUY, Order.OrderSide.BUY_TO_OPEN] and asset.asset_type == Asset.AssetType.OPTION:
            self.vars.option_asset = asset
            self.vars.contracts = int(quantity) if quantity is not None else self.vars.contracts
            self.vars.entry_option_price = float(price) if price is not None else self._get_option_mid(asset)
            self.vars.entry_stock_price = self.get_last_price(Asset(self.parameters["symbol"], asset_type=Asset.AssetType.STOCK))
            self.vars.entry_datetime = self.get_datetime()

            # Add a visual marker for the buy signal (rare, so safe to mark)
            try:
                underlying_last = self.get_last_price(self.parameters["symbol"]) or 0
            except Exception:
                underlying_last = 0
            self.add_marker(
                name="Bought Calls",
                value=float(underlying_last) if underlying_last else None,
                color="green",
                symbol="arrow-up",
                size=10,
                detail_text=f"Buy: {asset.symbol} {asset.expiration} {asset.strike}C x{self.vars.contracts} at ~${self.vars.entry_option_price:.2f}"
            )
            self.log_message(
                f"FILLED BUY: {asset.symbol} {asset.expiration} {asset.strike:.2f}C x{self.vars.contracts} at ${self.vars.entry_option_price:.2f}",
                color="green",
            )

        # If we sold the option, clear state and mark the event
        if order.side in [Order.OrderSide.SELL, Order.OrderSide.SELL_TO_CLOSE] and asset.asset_type == Asset.AssetType.OPTION:
            try:
                underlying_last = self.get_last_price(self.parameters["symbol"]) or 0
            except Exception:
                underlying_last = 0
            self.add_marker(
                name="Sold Calls",
                value=float(underlying_last) if underlying_last else None,
                color="red",
                symbol="arrow-down",
                size=10,
                detail_text=f"Sell: {asset.symbol} {asset.expiration} {asset.strike}C x{quantity} at ~${float(price) if price else 0:.2f}"
            )
            self.log_message(
                f"FILLED SELL: {asset.symbol} {asset.expiration} {asset.strike:.2f}C x{int(quantity) if quantity else 0} at ${float(price) if price else 0:.2f}",
                color="red",
            )
            self._clear_position_state()


if __name__ == "__main__":
    # Strategy configuration parameters (can be modified as needed)
    params = {
        "symbol": "TSLA",  # CHANGED: use TSLA
        "decline_trigger_pct": 0.25,
        # "recovery_exit_pct": 0.25,  # Removed (time-based exit)
        "hold_duration_days": 365,     # exit after ~1 year hold
        "otm_pct": 0.20,
        "target_exp_days": 730,
        "risk_per_trade": 0.95,
        "max_contracts": 50,
        "stop_loss_pct": 0.50,
        "min_days_to_expiry_exit": 30,
        "max_spread_pct": 0.35,
        "sleeptime": "1D",
    }

    if IS_BACKTESTING:
        # Backtesting using PolygonDataBacktesting for options data
        trading_fee = TradingFee(percent_fee=0.001)  # Simple fee model; adjust if needed

        results = AAPLDeepDipCalls.backtest(
            ThetaDataBacktesting,  # Correct signature: pass datasource_class positionally
            benchmark_asset=Asset("SPY", Asset.AssetType.STOCK),
            buy_trading_fees=[trading_fee],
            sell_trading_fees=[trading_fee],
            parameters=params,
            budget=100000,
            quote_asset=Asset("USD", Asset.AssetType.FOREX),
        )
    else:
        # Live trading path; the broker and credentials are handled by LumiBot via environment variables
        trader = Trader()
        strategy = AAPLDeepDipCalls(
            quote_asset=Asset("USD", Asset.AssetType.FOREX),
            parameters=params,
        )
        trader.add_strategy(strategy)
        strategies = trader.run_all()
