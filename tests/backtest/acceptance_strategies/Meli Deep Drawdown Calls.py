################################################################################
# Must Be Imported First If Run Locally
if True:
    import os
    import sys

    myPath = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, "/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot")
    sys.path.insert(
        0,
        "/Users/robertgrzesik/Development/lumiwealth_tradier/",
    )
    sys.path.insert(0, "/Users/robertgrzesik/Development/quantstats_lumi/")
################################################################################

"""
MELI Deep Drawdown Call Strategy.

This code was generated based on the user prompt: 'Create a strategy for MELI (MercadoLibre) that buys long-dated OTM calls during deep drawdowns.'
This code was refined based on the user prompt: 'The backtest failed without producing logs, so wrap initialize and on_trading_iteration in try/except blocks with traceback logging.'
"""
import traceback
from datetime import date, datetime, timedelta

from lumibot.backtesting import PolygonDataBacktesting
from lumibot.components.options_helper import OptionsHelper
from lumibot.credentials import IS_BACKTESTING
from lumibot.entities import Asset, Order, TradingFee
from lumibot.strategies.strategy import Strategy
from lumibot.traders import Trader


class MeliDeepDrawdownCalls(Strategy):
    entry_allocations = {"entry1": 0.5, "entry2": 0.25, "entry3": 0.25}

    def initialize(self):
        try:
            # Set the market schedule to US equities hours so the bot runs with the right timing
            self.set_market("XNYS")
            # Run once per day because the signals are based on daily drawdowns
            self.sleeptime = "1D"
            # OptionsHelper keeps the option selection logic safe and consistent
            self.options_helper = OptionsHelper(self)
            # Store the underlying asset once so every method can refer to the same object
            self.vars.underlying_asset = Asset("MELI", asset_type=Asset.AssetType.STOCK)
            # Make sure the entry tracking structures exist for warm restarts
            self._ensure_entry_state_structure()
            # Track exit orders that are already in flight so we do not duplicate them
            if not hasattr(self.vars, "pending_exit_assets"):
                self.vars.pending_exit_assets = []
            # Build the initial all-time high reference from history so drawdowns are meaningful
            self._initialize_all_time_high()
        except Exception as e:
            self._log_exception("initialize", e)

    def _log_exception(self, context, error):
        # Centralized error logging helper ensures every exception includes a traceback
        trace = traceback.format_exc()
        self.log_message(f"CRITICAL ERROR in {context}: {error}\n{trace}", color="red")

    def _build_default_entry_states(self):
        states = {}
        for key, allocation in self.entry_allocations.items():
            states[key] = {"executed": False, "reference_price": None, "allocation": allocation}
        return states

    def _ensure_entry_state_structure(self):
        if not hasattr(self.vars, "entry_states") or not isinstance(self.vars.entry_states, dict):
            self.vars.entry_states = self._build_default_entry_states()
        else:
            for key, allocation in self.entry_allocations.items():
                if key not in self.vars.entry_states or not isinstance(self.vars.entry_states[key], dict):
                    self.vars.entry_states[key] = {"executed": False, "reference_price": None, "allocation": allocation}
                else:
                    self.vars.entry_states[key].setdefault("executed", False)
                    self.vars.entry_states[key].setdefault("reference_price", None)
                    self.vars.entry_states[key]["allocation"] = allocation

    def _initialize_all_time_high(self):
        if getattr(self.vars, "all_time_high", None):
            return
        try:
            bars = self.get_historical_prices(self.vars.underlying_asset, 1000, "day")
            if bars is not None and hasattr(bars, "df") and not bars.df.empty:
                ath = float(bars.df["close"].max())
                self.vars.all_time_high = ath
                self.log_message(f"Initial ATH set from history at {ath:.2f}", color="blue")
            else:
                price = self.get_last_price(self.vars.underlying_asset)
                if price is not None:
                    self.vars.all_time_high = float(price)
                    self.log_message(f"Initial ATH fallback to last price {price:.2f}", color="yellow")
                else:
                    self.vars.all_time_high = 0.0
                    self.log_message("Unable to establish ATH due to missing prices.", color="red")
        except Exception as e:
            self.log_message(f"Error initializing ATH: {e}", color="red")
            self.vars.all_time_high = 0.0

    def on_trading_iteration(self):
        try:
            # Grab the latest price data so we know where MELI stands
            underlying = self.vars.underlying_asset
            current_dt = self.get_datetime()
            price = self.get_last_price(underlying)
            if price is None:
                self.log_message("MELI price unavailable, skipping iteration.", color="red")
                return
            # Update ATH and draw helpful lines for visualization
            self._update_all_time_high(price, current_dt)
            self.add_line(
                "MELI_Price", price, color="black", dt=current_dt, asset=underlying, detail_text="Latest MELI price"
            )
            ath_value = getattr(self.vars, "all_time_high", 0.0)
            if ath_value > 0:
                self.add_line(
                    "MELI_ATH",
                    ath_value,
                    color="blue",
                    style="dashed",
                    dt=current_dt,
                    asset=underlying,
                    detail_text="Rolling ATH",
                )
                drawdown_pct = price / ath_value - 1.0
            else:
                drawdown_pct = 0.0
            self.log_message(
                f"MELI price {price:.2f}, ATH {ath_value:.2f}, drawdown {drawdown_pct:.2%}.", color="white"
            )
            # Evaluate each entry gate sequentially
            self._handle_entry_logic(price, drawdown_pct, current_dt)
            # Make sure we flatten positions before options expire
            self._manage_option_expiries(current_dt)
        except Exception as e:
            self._log_exception("on_trading_iteration", e)

    def _update_all_time_high(self, price, current_dt):
        existing = getattr(self.vars, "all_time_high", 0.0)
        if price is None:
            return
        if price > existing:
            self.vars.all_time_high = float(price)
            self._reset_entries("a new all-time high was set")
            self.add_marker(
                "ATH_Reset",
                price,
                color="green",
                symbol="star",
                dt=current_dt,
                asset=self.vars.underlying_asset,
                detail_text="ATH updated",
            )
            self.log_message(f"New ATH recorded at {price:.2f}.", color="green")

    def _reset_entries(self, reason):
        self.vars.entry_states = self._build_default_entry_states()
        self.log_message(f"Entry states reset because {reason}.", color="yellow")

    def _handle_entry_logic(self, price, drawdown_pct, current_dt):
        states = self.vars.entry_states
        if not states["entry1"]["executed"]:
            if drawdown_pct <= -0.30:
                self.log_message("Entry 1 condition met: drawdown beyond 30%.", color="green")
                self._attempt_option_purchase("entry1", price, current_dt)
            else:
                self.log_message("Entry 1 condition not met yet.", color="blue")
        if states["entry1"]["executed"] and not states["entry2"]["executed"]:
            ref_price = states["entry1"]["reference_price"]
            if ref_price and price <= ref_price * 0.8:
                self.log_message("Entry 2 condition met: price dropped another 20% from Entry 1.", color="green")
                self._attempt_option_purchase("entry2", price, current_dt)
            else:
                self.log_message("Entry 2 waiting for additional 20% drop.", color="blue")
        if states["entry2"]["executed"] and not states["entry3"]["executed"]:
            ref_price = states["entry2"]["reference_price"]
            if ref_price and price <= ref_price * 0.8:
                self.log_message("Entry 3 condition met: price dropped another 20% from Entry 2.", color="green")
                self._attempt_option_purchase("entry3", price, current_dt)
            else:
                self.log_message("Entry 3 waiting for final 20% drop.", color="blue")

    def _attempt_option_purchase(self, entry_key, current_price, current_dt):
        states = self.vars.entry_states
        allocation_pct = states[entry_key]["allocation"]
        portfolio_value = self.get_portfolio_value()
        cash_available = self.get_cash()
        if cash_available <= 0:
            self.log_message("No cash available for option purchase.", color="red")
            return
        target_cash = portfolio_value * allocation_pct
        spendable_cash = min(target_cash, cash_available) * 0.98
        if spendable_cash <= 0:
            self.log_message("Spendable cash is zero after safety buffer.", color="yellow")
            return
        self.log_message(f"{entry_key} plans to deploy up to {spendable_cash:.2f} cash.", color="white")
        underlying = self.vars.underlying_asset
        target_date = (current_dt + timedelta(days=270)).date()
        chains = self.get_chains(underlying)
        if chains is None:
            self.log_message("Option chains unavailable, cannot trade.", color="red")
            return
        expiry = self.options_helper.get_expiration_on_or_after_date(
            target_date, chains, "call", underlying_asset=underlying
        )
        if expiry is None:
            self.log_message("No valid expiration near 270 days, skipping trade.", color="yellow")
            return
        raw_strike = current_price * 1.2
        rounded_strike = max(5.0, round(raw_strike / 5) * 5)
        option_asset = self.options_helper.find_next_valid_option(
            underlying, rounded_strike, expiry, put_or_call="call"
        )
        if option_asset is None:
            self.log_message("Unable to locate a valid option at the desired strike.", color="yellow")
            return
        evaluation = self.options_helper.evaluate_option_market(option_asset, max_spread_pct=0.2)
        if evaluation is None:
            self.log_message("Option evaluation failed, skipping trade.", color="red")
            return
        actionable = self.options_helper.has_actionable_price(evaluation)
        spread_text = f"{evaluation.spread_pct:.2%}" if evaluation.spread_pct is not None else "N/A"
        self.log_message(
            f"Option evaluation for strike {rounded_strike:.2f} exp {expiry}: bid {evaluation.bid}, ask {evaluation.ask}, spread {spread_text}, actionable {actionable}.",
            color="white",
        )
        if not actionable:
            flag_text = getattr(evaluation, "data_quality_flags", None)
            self.log_message(f"Skipping trade due to liquidity flags: {flag_text}", color="yellow")
            return
        option_price = evaluation.buy_price
        if option_price is None or option_price <= 0:
            self.log_message("Buy price missing, cannot size trade.", color="red")
            return
        contract_cost = option_price * 100.0
        contracts = int(spendable_cash // contract_cost)
        if contracts <= 0:
            self.log_message("Allocation does not cover a single contract.", color="yellow")
            return
        order = self.create_order(
            option_asset,
            contracts,
            Order.OrderSide.BUY_TO_OPEN,
            order_type=Order.OrderType.LIMIT,
            limit_price=option_price,
        )
        submitted_order = self.submit_order(order)
        if submitted_order is None:
            self.log_message("Order submission returned None, trade aborted.", color="red")
            return
        states[entry_key]["executed"] = True
        states[entry_key]["reference_price"] = current_price
        estimated_cost = contracts * contract_cost
        self.add_marker(
            f"{entry_key}_buy",
            current_price,
            color="green",
            symbol="star",
            dt=current_dt,
            asset=underlying,
            detail_text=f"{entry_key} call entry placed",
        )
        self.log_message(
            f"{entry_key} order for {contracts} contracts placed at {option_price:.2f} (about {estimated_cost:.2f} notional).",
            color="green",
        )

    def _manage_option_expiries(self, current_dt):
        positions = self.get_positions()
        has_meli_options = False
        active_keys = []
        for position in positions:
            asset = getattr(position, "asset", None)
            if asset is None:
                continue
            if asset.asset_type != Asset.AssetType.OPTION:
                continue
            if asset.symbol != self.vars.underlying_asset.symbol:
                continue
            has_meli_options = True
            key = self._build_position_key(asset)
            active_keys.append(key)
            expiration_date = asset.expiration
            if isinstance(expiration_date, datetime):
                expiration_date = expiration_date.date()
            elif isinstance(expiration_date, str):
                try:
                    expiration_date = date.fromisoformat(expiration_date)
                except ValueError:
                    expiration_date = None
            if expiration_date is None:
                self.log_message("Option expiration missing, cannot manage exit.", color="red")
                continue
            days_to_expiry = (expiration_date - current_dt.date()).days
            self.log_message(
                f"Option {asset.symbol} {asset.right} {asset.strike} expires in {days_to_expiry} days.", color="white"
            )
            if days_to_expiry <= 7:
                if key in self.vars.pending_exit_assets:
                    self.log_message("Exit already pending for this option.", color="blue")
                    continue
                quantity = abs(position.quantity)
                if quantity <= 0:
                    continue
                exit_order = self.create_order(
                    asset,
                    quantity,
                    Order.OrderSide.SELL_TO_CLOSE,
                    order_type=Order.OrderType.MARKET,
                )
                submitted = self.submit_order(exit_order)
                if submitted is None:
                    self.log_message("Failed to submit expiration exit order.", color="red")
                    continue
                self.vars.pending_exit_assets.append(key)
                self.log_message(f"Closing {quantity} contracts before expiration to avoid assignment.", color="yellow")
        self.vars.pending_exit_assets = [key for key in self.vars.pending_exit_assets if key in active_keys]
        if not has_meli_options:
            executed_any = any(state["executed"] for state in self.vars.entry_states.values())
            if executed_any:
                self._reset_entries("all MELI option positions have been closed")

    def _build_position_key(self, asset):
        expiry = asset.expiration
        if isinstance(expiry, datetime):
            expiry = expiry.date()
        right = getattr(asset, "right", "")
        strike = getattr(asset, "strike", "")
        return f"{asset.symbol}_{right}_{strike}_{expiry}"

    def on_filled_order(self, position, order, price, quantity, multiplier):
        # Log fills so we can trace every completed trade
        asset = getattr(order, "asset", None)
        side = getattr(order, "side", None)
        self.log_message(f"Order filled: side={side}, qty={quantity}, asset={asset}", color="green")
        if asset is None:
            return
        if asset.asset_type == Asset.AssetType.OPTION and asset.symbol == self.vars.underlying_asset.symbol:
            key = self._build_position_key(asset)
            if side in (Order.OrderSide.SELL, Order.OrderSide.SELL_TO_CLOSE) and key in self.vars.pending_exit_assets:
                self.vars.pending_exit_assets.remove(key)


if __name__ == "__main__":
    try:
        # When backtesting we plug into Polygon to get the option history that MELI requires
        if IS_BACKTESTING:
            trading_fee = TradingFee(percent_fee=0.001)
            result = MeliDeepDrawdownCalls.backtest(
                datasource_class=PolygonDataBacktesting,
                benchmark_asset=Asset("SPY", Asset.AssetType.STOCK),
                buy_trading_fees=[trading_fee],
                sell_trading_fees=[trading_fee],
                quote_asset=Asset("USD", Asset.AssetType.FOREX),
            )
        else:
            # In live trading the Trader helper wires up the configured broker automatically
            trader = Trader()
            strategy = MeliDeepDrawdownCalls(
                quote_asset=Asset("USD", Asset.AssetType.FOREX),
            )
            trader.add_strategy(strategy)
            strategies = trader.run_all()
    except Exception as exc:
        # Ensure even top-level crashes surface a traceback in console logs
        print(f"Unhandled exception in main: {exc}\n{traceback.format_exc()}")
