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


#!/usr/bin/env python3

import pandas as pd

from lumibot.backtesting import PolygonDataBacktesting
from lumibot.components.options_helper import OptionsHelper
from lumibot.credentials import IS_BACKTESTING
from lumibot.entities import Asset, Order, TradingFee
from lumibot.strategies.strategy import Strategy
from lumibot.traders import Trader


class BackdoorButterfly0DTE(Strategy):
    """Backdoor 10-wide butterfly refined for momentum entries plus SL/TP.

    This code was refined based on the user prompt: 'Remove the debit entry timeout, remove the Price > EMA9 filter for neutral entries, keep RSI < 75 but add RSI > 25, keep SL/TP checks every iteration, keep max_trades_per_day at 5, and keep EOD logic.'
    """

    parameters = {
        "sleeptime": "2M",  # Run every two minutes to align with fast 0DTE dynamics
        "bars_length": 60,  # One hour of minute bars keeps RSI14 well-fed
        "max_debit_price": 5.0,  # Wider debit cap so more spreads qualify (was 3.5)
        "min_credit_over_debit": 0.2,  # Smaller net credit requirement to complete butterflies (was 0.5)
        "target_wing": 5,  # 5-point wings form the 10-wide butterfly
        "max_spread_pct": 0.25,  # Liquidity guardrail for option quotes
        "max_trades_per_day": 5,  # Keep at 5 per user request
        "max_dte_fallback": 7,  # Accept expirations up to one week out when 0DTE is missing
        "rsi_upper_band": 75,  # Keep upper bound filter
        "rsi_lower_band": 25,  # NEW: avoid extreme selloffs/volatility where fills degrade
        "stop_loss_pct": 0.15,  # 15% stop loss
        "take_profit_pct": 0.30,  # 30% take profit
    }

    def initialize(self):
        self.log_message("Initializing BackdoorButterfly0DTE...", color="blue")
        self.vars.initialized = False
        try:
            self.set_market("XNYS")
            self.sleeptime = self.parameters.get("sleeptime", "2M")
            self.options_helper = OptionsHelper(self)

            # Persistent state MUST live under self.vars (do not assign arbitrary attrs on self)
            self.vars.underlying_asset = Asset("SPX", asset_type=Asset.AssetType.INDEX)
            self.vars.open_butterflies = []  # Track active butterflies for SL/TP enforcement
            self.vars.butterfly_counter = 0  # Unique IDs for easier logging
            self.vars.reset_state = True
            self._reset_intraday_state()

            self.vars.initialized = True
            self.log_message("Initialization successful.", color="green")
        except Exception as exc:  # Catch startup issues so trading loop can bail gracefully
            self.log_message(f"Initialization failed: {exc}", color="red")
            self.vars.initialization_error = str(exc)

    def on_trading_iteration(self):
        if not getattr(self.vars, "initialized", False):
            self.log_message("Initialization flag is false; skipping iteration.", color="red")
            return

        current_dt = self.get_datetime()
        if getattr(self.vars, "tracked_session", None) != current_dt.date():
            self._reset_intraday_state()
            self.vars.tracked_session = current_dt.date()
            self.log_message("New session detected. State reset.", color="blue")

        # --- Position-level risk management must run every iteration ---
        # NOTE: kept per user request.
        self._check_open_butterflies()

        # --- Removed debit entry timeout logic entirely per user request ---
        # (No arbitrary cancellation of entry orders based on elapsed time.)

        # --- CRITICAL: Enforce EOD exit before any other logic so expired options never persist ---
        if self._check_eod_exit(current_dt):
            return

        if getattr(self.vars, "have_active_orders", False):
            active_orders = []
            try:
                if hasattr(self.broker, "get_active_tracked_orders"):
                    active_orders = self.broker.get_active_tracked_orders(strategy=self.name)
                else:
                    active_orders = [order for order in self.broker.get_tracked_orders(self.name) if order.is_active()]
            except Exception:
                active_orders = []
            if not active_orders:
                self.log_message("No active orders remain; clearing active flag.", color="yellow")
                self.vars.have_active_orders = False
                self.vars.pending_debit_orders = []
                self.vars.pending_credit_orders = []
            else:
                self.log_message("Active orders in flight; waiting for fills.", color="yellow")
                return

        start_entry_time = pd.Timestamp("09:45").time()  # User-requested explicit morning start
        cutoff_entry_time = pd.Timestamp(
            "15:45"
        ).time()  # Extended entry window so signals later in the day are eligible

        if current_dt.time() < start_entry_time:
            self.log_message(
                f"Waiting for start_time {start_entry_time.strftime('%H:%M')} ET. Current time: {current_dt.strftime('%H:%M')}.",
                color="white",
            )
            return

        if self.vars.trades_taken_today >= self.parameters.get("max_trades_per_day", 5):
            self.log_message("Daily trade limit reached; standing by.", color="yellow")
            return

        if current_dt.time() >= cutoff_entry_time:
            self.log_message("Entry window closed (15:45 ET). Skipping new trade attempts.", color="yellow")
            return

        bars = self.get_historical_prices(
            asset=self.vars.underlying_asset,
            length=self.parameters.get("bars_length", 60),
            timestep="minute",
            include_after_hours=False,
        )
        if bars is None:
            self.log_message("No SPX data available for signal computation.", color="yellow")
            return

        df = bars.df.copy()
        if df.shape[0] < 20:  # Need enough history for RSI14 calculation
            self.log_message(
                f"Insufficient SPX data ({df.shape[0]} bars) for RSI14 calculation.",
                color="yellow",
            )
            return

        # --- Signal calculations ---
        # User change: remove Price > EMA9 trend filter because this is a neutral butterfly.
        # Keep RSI gate, but add lower bound RSI > 25 to avoid extreme conditions.
        delta = df["close"].diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1 / 14, min_periods=14, adjust=False).mean().replace(0, 1e-9)
        rs = avg_gain / avg_loss
        df["rsi14"] = 100 - (100 / (1 + rs))
        df["rsi14"].fillna(50.0, inplace=True)

        latest_row = df.iloc[-1]
        price = float(latest_row["close"])
        rsi14 = float(latest_row["rsi14"])

        # Chart lines (keep minimal and meaningful)
        self.add_line("SPX_Close", price, color="black", detail_text="SPX close", asset=self.vars.underlying_asset)

        rsi_upper_band = self.parameters.get("rsi_upper_band", 75)
        rsi_lower_band = self.parameters.get("rsi_lower_band", 25)

        rsi_gate_passed = (rsi14 < rsi_upper_band) and (rsi14 > rsi_lower_band)
        if not rsi_gate_passed:
            self.log_message(
                f"RSI gate failed -> RSI14 {rsi14:.2f} must be between {rsi_lower_band} and {rsi_upper_band}.",
                color="white",
            )
            return

        self.log_message(
            f"Entry Signal: RSI14 {rsi14:.2f} is within [{rsi_lower_band}, {rsi_upper_band}]. Preparing neutral butterfly entry.",
            color="green",
        )
        self.add_marker(
            name="RSI_Gate",
            value=price,
            color="green",
            symbol="arrow-up",
            detail_text="RSI gate passed",
            asset=self.vars.underlying_asset,
        )
        self._submit_butterfly_entry(df)

    def on_filled_order(self, position, order, price, quantity, multiplier):
        tag = getattr(order, "tag", "")
        self.log_message(f"Filled {tag} at {price:.2f} for {quantity} contracts.", color="green")

        if tag == "debit_buy_lower":
            self.vars.debit_lower_filled = True
            self.vars.debit_lower_price = price
            self.vars.lower_call_asset = order.asset  # Persist leg for SL/TP tracking
        elif tag == "debit_sell_middle":
            self.vars.debit_middle_filled = True
            self.vars.debit_middle_price = price
            self.vars.middle_call_asset = order.asset
        elif tag == "credit_sell_middle":
            self.vars.credit_middle_filled = True
            self.vars.credit_middle_price = price
        elif tag == "credit_buy_upper":
            self.vars.credit_upper_filled = True
            self.vars.credit_upper_price = price
            self.vars.upper_call_asset = order.asset

        if self.vars.debit_lower_filled and self.vars.debit_middle_filled and not self.vars.debit_complete:
            self.vars.debit_complete = True
            self.vars.debit_net_paid = self.vars.debit_lower_price - self.vars.debit_middle_price
            self.log_message(
                f"Trade entry achieved. Debit spread cost: {self.vars.debit_net_paid:.2f} per contract.",
                color="green",
            )
            self._submit_credit_spread()

        if self.vars.credit_middle_filled and self.vars.credit_upper_filled and not self.vars.credit_complete:
            self.vars.credit_complete = True
            credit_received = self.vars.credit_middle_price - self.vars.credit_upper_price
            self.vars.credit_net_received = credit_received
            net_result = credit_received - self.vars.debit_net_paid
            self.log_message(
                f"Butterfly completed. Credit: {credit_received:.2f}, Net:{net_result:.2f}",
                color="green",
            )

            # Store active butterfly so SL/TP can manage the position until expiry
            butterfly_id = self.vars.butterfly_counter
            self.vars.butterfly_counter += 1
            self.vars.open_butterflies.append(
                {
                    "id": butterfly_id,
                    "lower_asset": self.vars.lower_call_asset,
                    "middle_asset": self.vars.middle_call_asset,
                    "upper_asset": self.vars.upper_call_asset,
                    "initial_net_price": (self.vars.debit_net_paid - credit_received),
                    "quantity": quantity,
                    "entry_time": self.get_datetime(),
                }
            )
            self.vars.have_active_orders = False

    # --------------------------- Helper methods ---------------------------
    def _check_eod_exit(self, current_dt):
        cutoff_time = pd.Timestamp("15:55").time()
        if current_dt.time() < cutoff_time:
            return False

        # Keep EOD logic per user request: cancel open orders but allow filled butterflies to reach expiry.
        self.log_message("EOD reached. Canceling open orders but holding positions through expiration.", color="blue")
        self.cancel_open_orders()
        self._reset_intraday_state()  # Ensure next session starts fresh while positions ride into expiration
        self.vars.open_butterflies = []  # Cash settlement will resolve outstanding contracts, so tracking can reset
        return True

    def _reset_intraday_state(self):
        self.vars.trades_taken_today = 0
        self.vars.debit_lower_filled = False
        self.vars.debit_middle_filled = False
        self.vars.debit_complete = False
        self.vars.debit_net_paid = 0.0
        self.vars.debit_lower_price = 0.0
        self.vars.debit_middle_price = 0.0
        self.vars.credit_middle_filled = False
        self.vars.credit_upper_filled = False
        self.vars.credit_complete = False
        self.vars.credit_net_received = 0.0
        self.vars.have_active_orders = False
        self.vars.pending_debit_orders: list[Order] = []
        self.vars.pending_credit_orders: list[Order] = []

        # Removed: self.vars.debit_order_timestamp (timeout logic removed)

        self.vars.lower_call_asset = None
        self.vars.middle_call_asset = None
        self.vars.upper_call_asset = None

    def _submit_butterfly_entry(self, df: pd.DataFrame):
        dt = self.get_datetime()
        expiry = self._find_valid_expiration(dt)
        if expiry is None:
            return

        price = float(df["close"].iloc[-1])
        step = self.parameters.get("target_wing", 5)
        middle_strike = round(price / step) * step
        lower_strike = middle_strike - step
        upper_strike = middle_strike + step
        quantity = 1  # 1-lot structure as baseline

        # NOTE: keep Asset objects persisted (do not store only symbols)
        lower_call = Asset(
            "SPX",
            asset_type=Asset.AssetType.OPTION,
            expiration=expiry,
            strike=lower_strike,
            right=Asset.OptionRight.CALL,
        )
        middle_call = Asset(
            "SPX",
            asset_type=Asset.AssetType.OPTION,
            expiration=expiry,
            strike=middle_strike,
            right=Asset.OptionRight.CALL,
        )
        upper_call = Asset(
            "SPX",
            asset_type=Asset.AssetType.OPTION,
            expiration=expiry,
            strike=upper_strike,
            right=Asset.OptionRight.CALL,
        )

        max_spread_pct = self.parameters.get("max_spread_pct", 0.25)
        eval_lower = self.options_helper.evaluate_option_market(lower_call, max_spread_pct=max_spread_pct)
        eval_middle = self.options_helper.evaluate_option_market(middle_call, max_spread_pct=max_spread_pct)

        # Always log evaluations so it's obvious why trades are/aren't happening
        self.log_message(f"Debit leg eval lower: {getattr(eval_lower, 'data_quality_flags', None)}", color="white")
        self.log_message(f"Debit leg eval middle: {getattr(eval_middle, 'data_quality_flags', None)}", color="white")

        if not (
            self.options_helper.has_actionable_price(eval_lower)
            and self.options_helper.has_actionable_price(eval_middle)
        ):
            self.log_message("Option markets not actionable for debit legs; skipping.", color="yellow")
            return

        debit_estimate = eval_lower.buy_price - eval_middle.sell_price
        if debit_estimate is None or debit_estimate > self.parameters.get("max_debit_price", 5.0):
            self.log_message(
                f"Debit estimate {debit_estimate} breaches cap {self.parameters.get('max_debit_price', 5.0)}.",
                color="yellow",
            )
            return

        buy_lower = self.create_order(
            asset=lower_call,
            quantity=quantity,
            side=Order.OrderSide.BUY,
            limit_price=eval_lower.buy_price,
            order_type=Order.OrderType.LIMIT,
            time_in_force="day",
        )
        buy_lower.tag = "debit_buy_lower"

        sell_middle = self.create_order(
            asset=middle_call,
            quantity=quantity,
            side=Order.OrderSide.SELL,
            limit_price=eval_middle.sell_price,
            order_type=Order.OrderType.LIMIT,
            time_in_force="day",
        )
        sell_middle.tag = "debit_sell_middle"

        orders = self.submit_order([buy_lower, sell_middle])
        self.vars.pending_debit_orders = orders if isinstance(orders, list) else [orders]
        self.vars.have_active_orders = True
        # Count each entry attempt at submission time so max_trades_per_day caps risk even if
        # the credit legs never complete the butterfly.
        self.vars.trades_taken_today += 1

        # Persist for later SL/TP + credit completion
        self.vars.lower_call_asset = lower_call
        self.vars.middle_call_asset = middle_call
        self.vars.upper_call_asset = upper_call

        self.log_message("Debit spread orders submitted. Awaiting fills to enter trade.", color="green")

    def _submit_credit_spread(self):
        max_spread_pct = self.parameters.get("max_spread_pct", 0.25)
        middle_call = getattr(self.vars, "middle_call_asset", None)
        upper_call = getattr(self.vars, "upper_call_asset", None)
        if middle_call is None or upper_call is None:
            self.log_message("Missing call references for credit spread; cannot continue.", color="red")
            return

        eval_middle = self.options_helper.evaluate_option_market(middle_call, max_spread_pct=max_spread_pct)
        eval_upper = self.options_helper.evaluate_option_market(upper_call, max_spread_pct=max_spread_pct)

        self.log_message(f"Credit leg eval middle: {getattr(eval_middle, 'data_quality_flags', None)}", color="white")
        self.log_message(f"Credit leg eval upper: {getattr(eval_upper, 'data_quality_flags', None)}", color="white")

        if not (
            self.options_helper.has_actionable_price(eval_middle)
            and self.options_helper.has_actionable_price(eval_upper)
        ):
            self.log_message("Option markets not actionable for credit legs; skipping.", color="yellow")
            return

        credit_estimate = eval_middle.sell_price - eval_upper.buy_price
        if credit_estimate is None:
            self.log_message("Credit estimate unavailable; abandoning butterfly completion.", color="yellow")
            return

        net_result = credit_estimate - self.vars.debit_net_paid
        if net_result < self.parameters.get("min_credit_over_debit", 0.2):
            self.log_message(
                f"Net credit {net_result:.2f} below target {self.parameters.get('min_credit_over_debit', 0.2):.2f}; skipping credit leg.",
                color="yellow",
            )
            return

        sell_middle = self.create_order(
            asset=middle_call,
            quantity=1,
            side=Order.OrderSide.SELL,
            limit_price=eval_middle.sell_price,
            order_type=Order.OrderType.LIMIT,
            time_in_force="day",
        )
        sell_middle.tag = "credit_sell_middle"

        buy_upper = self.create_order(
            asset=upper_call,
            quantity=1,
            side=Order.OrderSide.BUY,
            limit_price=eval_upper.buy_price,
            order_type=Order.OrderType.LIMIT,
            time_in_force="day",
        )
        buy_upper.tag = "credit_buy_upper"

        orders = self.submit_order([sell_middle, buy_upper])
        self.vars.pending_credit_orders = orders if isinstance(orders, list) else [orders]
        self.vars.have_active_orders = True
        self.log_message("Credit spread orders submitted to finalize butterfly.", color="blue")

    def _find_valid_expiration(self, current_dt) -> pd.Timestamp | None:
        chains = self.get_chains(self.vars.underlying_asset)
        if not chains:
            self.log_message("Option chains unavailable; cannot trade today.", color="yellow")
            return None

        max_fallback = self.parameters.get("max_dte_fallback", 7)
        expiry = self.options_helper.get_expiration_on_or_after_date(
            current_dt.date(), chains, "call", underlying_asset=self.vars.underlying_asset
        )
        if expiry is None:
            self.log_message("No valid expiration returned by OptionsHelper.", color="yellow")
            return None

        dte = (expiry - current_dt.date()).days
        if dte == 0:
            return expiry
        if 0 < dte <= max_fallback:
            self.log_message(f"0DTE unavailable. Falling back to {dte} DTE.", color="yellow")
            return expiry

        self.log_message(f"Next expiry {expiry} is {dte} days out; beyond fallback window.", color="yellow")
        return None

    def _check_open_butterflies(self):
        # Kept and still runs every iteration (per user request)
        if not getattr(self.vars, "open_butterflies", None):
            return

        stop_pct = self.parameters.get("stop_loss_pct", 0.15)
        take_pct = self.parameters.get("take_profit_pct", 0.30)
        max_spread_pct = self.parameters.get("max_spread_pct", 0.25)
        indexes_to_remove: list[int] = []

        for idx, butterfly in enumerate(list(self.vars.open_butterflies)):
            quantity = butterfly.get("quantity", 1)
            valuations = {}
            leg_data = {
                "lower_asset": 1,
                "middle_asset": -2,
                "upper_asset": 1,
            }

            missing_data = False
            for leg_name in leg_data:
                evaluation = self.options_helper.evaluate_option_market(
                    butterfly[leg_name], max_spread_pct=max_spread_pct
                )
                mid_price = self._extract_mid_price(evaluation)
                if mid_price is None:
                    self.log_message(
                        f"Skipping SL/TP check for butterfly {butterfly['id']} due to missing data on {leg_name}.",
                        color="yellow",
                    )
                    missing_data = True
                    break
                valuations[leg_name] = mid_price

            if missing_data:
                continue

            current_value = (
                (valuations["lower_asset"] - 2 * valuations["middle_asset"] + valuations["upper_asset"])
                * quantity
                * 100
            )
            initial_value = butterfly["initial_net_price"] * quantity * 100
            if initial_value == 0:
                self.log_message(
                    f"Initial net premium was zero for butterfly {butterfly['id']}; skipping SL/TP logic.",
                    color="yellow",
                )
                continue

            pnl = current_value - initial_value
            pnl_ratio = pnl / abs(initial_value)

            if pnl_ratio <= -stop_pct:
                self.log_message(
                    f"Stop loss hit for butterfly {butterfly['id']}: PnL ${pnl:.2f} ({pnl_ratio:.2%}). Exiting.",
                    color="red",
                )
                self._liquidate_butterfly(butterfly, reason="stop loss", color="red")
                indexes_to_remove.append(idx)
            elif pnl_ratio >= take_pct:
                self.log_message(
                    f"Take profit hit for butterfly {butterfly['id']}: PnL ${pnl:.2f} ({pnl_ratio:.2%}). Exiting.",
                    color="green",
                )
                self._liquidate_butterfly(butterfly, reason="take profit", color="green")
                indexes_to_remove.append(idx)

        for idx in sorted(indexes_to_remove, reverse=True):
            self.vars.open_butterflies.pop(idx)

    def _extract_mid_price(self, evaluation):
        if evaluation is None:
            return None
        if evaluation.bid is not None and evaluation.ask is not None:
            return (evaluation.bid + evaluation.ask) / 2
        if evaluation.buy_price is not None and evaluation.sell_price is not None:
            return (evaluation.buy_price + evaluation.sell_price) / 2
        if evaluation.last_price is not None:
            return evaluation.last_price
        return None

    def _liquidate_butterfly(self, butterfly, reason: str, color: str):
        quantity = butterfly.get("quantity", 1)
        orders = [
            self.create_order(
                butterfly["lower_asset"],
                quantity,
                Order.OrderSide.SELL,
                order_type=Order.OrderType.MARKET,
                time_in_force="day",
            ),
            self.create_order(
                butterfly["middle_asset"],
                quantity * 2,
                Order.OrderSide.BUY,
                order_type=Order.OrderType.MARKET,
                time_in_force="day",
            ),
            self.create_order(
                butterfly["upper_asset"],
                quantity,
                Order.OrderSide.SELL,
                order_type=Order.OrderType.MARKET,
                time_in_force="day",
            ),
        ]
        self.submit_order(orders)
        self.log_message(f"Submitted {reason} exit for butterfly {butterfly['id']}.", color=color)


if __name__ == "__main__":
    if IS_BACKTESTING:
        trading_fee = TradingFee(percent_fee=0.0005)
        BackdoorButterfly0DTE.backtest(
            datasource_class=PolygonDataBacktesting,
            benchmark_asset=Asset("SPY", Asset.AssetType.STOCK),
            buy_trading_fees=[trading_fee],
            sell_trading_fees=[trading_fee],
            quote_asset=Asset("USD", Asset.AssetType.FOREX),
            budget=100000,
        )
    else:
        trader = Trader()
        strategy = BackdoorButterfly0DTE(
            quote_asset=Asset("USD", Asset.AssetType.FOREX),
        )
        trader.add_strategy(strategy)
        strategies = trader.run_all()
