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

from datetime import date, datetime

from lumibot.components.options_helper import OptionsHelper
from lumibot.credentials import IS_BACKTESTING
from lumibot.entities import Asset, Order, TradingFee
from lumibot.strategies.strategy import Strategy
from lumibot.traders import Trader

"""
Strategy Description
--------------------
LeapsCallDebitSpread

This code was refined based on the user prompt: 'Remove the \'from __future__ import annotations\' line. The backtest failed with a SyntaxError because this import was found on line 35 (likely due to code injection), and it must be the first line. The code already uses standard `typing` module imports (List, Dict, Optional), so this future import is not strictly necessary.'

Notes
-----
- Uses OptionsHelper for option selection and market validation.
- Uses PolygonDataBacktesting because the strategy trades options.
"""


class LeapsCallDebitSpread(Strategy):
    parameters = {
        "symbols": ["UBER", "STRL", "CLS", "MFC", "APP"],
        "budget": 10000,
        "per_symbol_allocation": None,
        "sleeptime": "1D",
        "min_days_to_expiry": 300,
        "target_delta": 0.50,
        "otm_short_multiplier": 1.25,
        "max_option_spread_pct": 0.60,
        "roll_out_days": 30,
        "roll_up_if_underlying_above_short_strike": True,
        # Added: safety knobs for throttling management checks (keeps logs cleaner in minute-level live loops)
        "manage_once_per_day": True,
    }

    def initialize(self):
        self.sleeptime = self.parameters.get("sleeptime", "1D")
        self.set_market("stock")

        # CRITICAL: store state on self.vars (not on self) to avoid collisions with framework internals
        self.vars.options_helper = OptionsHelper(self)

        if not hasattr(self.vars, "initial_budget_value"):
            self.vars.initial_budget_value = None
        if not hasattr(self.vars, "symbol_states"):
            self.vars.symbol_states = {}
        if not hasattr(self.vars, "open_spreads"):
            self.vars.open_spreads = {}
        if not hasattr(self.vars, "last_management_dt"):
            self.vars.last_management_dt = None

        for sym in self.parameters.get("symbols", []):
            self.vars.symbol_states.setdefault(sym, "pending")

    def _safe_parse_expiry(self, exp) -> date | None:
        # Kept logic; Python 3.9 safe typing
        if isinstance(exp, date):
            return exp
        if isinstance(exp, str):
            try:
                return datetime.strptime(exp, "%Y-%m-%d").date()
            except Exception:
                return None
        return None

    def _get_furthest_valid_expiration(self, underlying_asset: Asset, chains) -> date | None:
        min_days = int(self.parameters.get("min_days_to_expiry", 300))
        today = self.get_datetime().date()

        expirations = []
        # Chains can be Chains() or dict depending on datasource; keep robust fallback
        try:
            expirations = chains.expirations("CALL")
        except Exception:
            call_dict = chains.get("Chains", {}).get("CALL", {}) if isinstance(chains, dict) else {}
            expirations = list(call_dict.keys()) if call_dict else []

        parsed: list[date] = []
        for e in expirations:
            d = self._safe_parse_expiry(e)
            if d is not None:
                parsed.append(d)

        parsed = sorted(set(parsed), reverse=True)
        if not parsed:
            return None

        preferred = [d for d in parsed if (d - today).days >= min_days]
        candidates = preferred if preferred else parsed

        # CRITICAL: validate expirations have tradeable data using OptionsHelper
        for candidate in candidates:
            validated = self.vars.options_helper.get_expiration_on_or_after_date(
                candidate,
                chains,
                "call",
                underlying_asset=underlying_asset,
            )
            if validated is not None and validated == candidate:
                return validated

        return None

    def _pick_short_strike_from_chain(
        self, chains, expiry: date, long_strike: float, target_short: float
    ) -> float | None:
        strikes = []
        try:
            strikes = chains.strikes(expiry, "CALL")
        except Exception:
            call_dict = chains.get("Chains", {}).get("CALL", {}) if isinstance(chains, dict) else {}
            strikes = call_dict.get(expiry.strftime("%Y-%m-%d"), []) if call_dict else []

        if not strikes:
            return None

        higher = [s for s in strikes if s is not None and float(s) > float(long_strike)]
        if not higher:
            return None

        return float(min(higher, key=lambda s: abs(float(s) - float(target_short))))

    def _compute_per_symbol_budget(self, symbols: list[str]) -> float:
        # FIX: Python 3.9 compatible typing (List[str] instead of list[str])
        manual = self.parameters.get("per_symbol_allocation", None)
        if manual is not None:
            return float(manual)
        total = float(self.parameters.get("budget", 10000))
        return total / max(1, len(symbols))

    def _log_option_eval(self, label: str, option_asset: Asset, evaluation) -> None:
        # Added: always log evaluations so skips are traceable
        if evaluation is None:
            self.log_message(f"{label}: evaluation is None for {option_asset}", color="yellow")
            return

        flags = getattr(evaluation, "data_quality_flags", None)
        self.log_message(
            f"{label}: bid={getattr(evaluation, 'bid', None)} ask={getattr(evaluation, 'ask', None)} "
            f"buy={getattr(evaluation, 'buy_price', None)} sell={getattr(evaluation, 'sell_price', None)} "
            f"spread_pct={getattr(evaluation, 'spread_pct', None)} flags={flags}",
            color="blue",
        )

    def _attempt_open_spread(self, symbol: str, per_symbol_budget: float) -> None:
        if symbol in self.vars.open_spreads:
            self.log_message(f"{symbol}: spread already tracked as open; skipping new entry.", color="yellow")
            return

        underlying_asset = Asset(symbol, asset_type=Asset.AssetType.STOCK)
        max_spread_pct = float(self.parameters.get("max_option_spread_pct", 0.60))
        target_delta = float(self.parameters.get("target_delta", 0.50))
        otm_mult = float(self.parameters.get("otm_short_multiplier", 1.25))

        last_price = self.get_last_price(underlying_asset)
        if last_price is None or last_price <= 0:
            self.log_message(f"{symbol}: no valid last price; skipping.", color="yellow")
            self.vars.symbol_states[symbol] = "skipped"
            return

        # Optional charting: keep to 1-2 lines total to avoid clutter
        dt_now = self.get_datetime()
        if float(last_price) > 0:
            self.add_line(
                symbol,
                float(last_price),
                color="black",
                detail_text="Underlying price",
                dt=dt_now,
                asset=underlying_asset,
            )

        chains = self.get_chains(underlying_asset)
        if not chains:
            self.log_message(f"{symbol}: no options chains available; skipping.", color="yellow")
            self.vars.symbol_states[symbol] = "skipped"
            return

        expiry = self._get_furthest_valid_expiration(underlying_asset, chains)
        if expiry is None:
            self.log_message(f"{symbol}: could not find a valid long-dated expiration; skipping.", color="yellow")
            self.vars.symbol_states[symbol] = "skipped"
            return

        long_strike = self.vars.options_helper.find_strike_for_delta(
            underlying_asset,
            float(last_price),
            target_delta,
            expiry,
            right="call",
        )
        if long_strike is None:
            self.log_message(
                f"{symbol}: could not find a call strike near target delta {target_delta:.2f}; skipping.",
                color="yellow",
            )
            self.vars.symbol_states[symbol] = "skipped"
            return

        long_option = self.vars.options_helper.find_next_valid_option(
            underlying_asset,
            float(long_strike),
            expiry,
            "call",
        )
        if long_option is None:
            self.log_message(
                f"{symbol}: long option not tradeable/priceable at strike {float(long_strike):.2f}; skipping.",
                color="yellow",
            )
            self.vars.symbol_states[symbol] = "skipped"
            return

        target_short = float(last_price) * otm_mult
        short_strike = self._pick_short_strike_from_chain(chains, expiry, float(long_strike), target_short)
        if short_strike is None:
            self.log_message(
                f"{symbol}: could not pick a valid short strike above long strike; skipping.", color="yellow"
            )
            self.vars.symbol_states[symbol] = "skipped"
            return

        short_option = self.vars.options_helper.find_next_valid_option(
            underlying_asset,
            float(short_strike),
            expiry,
            "call",
        )
        if short_option is None:
            self.log_message(
                f"{symbol}: short option not tradeable/priceable at strike {float(short_strike):.2f}; skipping.",
                color="yellow",
            )
            self.vars.symbol_states[symbol] = "skipped"
            return

        # Evaluate market quality and log it every time
        long_eval = self.vars.options_helper.evaluate_option_market(long_option, max_spread_pct=max_spread_pct)
        short_eval = self.vars.options_helper.evaluate_option_market(short_option, max_spread_pct=max_spread_pct)
        self._log_option_eval(f"{symbol} LONG", long_option, long_eval)
        self._log_option_eval(f"{symbol} SHORT", short_option, short_eval)

        if long_eval is None or short_eval is None:
            self.vars.symbol_states[symbol] = "skipped"
            return

        if not (
            self.vars.options_helper.has_actionable_price(long_eval)
            and self.vars.options_helper.has_actionable_price(short_eval)
        ):
            self.log_message(
                f"{symbol}: option market not actionable (spread too wide or missing prices); skipping.",
                color="yellow",
            )
            self.vars.symbol_states[symbol] = "skipped"
            return

        buy_price = float(long_eval.buy_price) if long_eval.buy_price is not None else None
        sell_price = float(short_eval.sell_price) if short_eval.sell_price is not None else None
        if buy_price is None or sell_price is None or buy_price <= 0 or sell_price <= 0:
            self.log_message(f"{symbol}: invalid debit/credit prices; skipping.", color="yellow")
            self.vars.symbol_states[symbol] = "skipped"
            return

        multiplier = getattr(long_option, "multiplier", 100) or 100
        cost_per_spread = (buy_price - sell_price) * float(multiplier)
        if cost_per_spread <= 0:
            self.log_message(f"{symbol}: non-positive estimated debit per spread; skipping.", color="yellow")
            self.vars.symbol_states[symbol] = "skipped"
            return

        cash = float(self.get_cash())
        spend_cap = min(float(per_symbol_budget), cash)
        qty = int(spend_cap // cost_per_spread)
        if qty < 1:
            self.log_message(
                f"{symbol}: not enough cash for 1 spread (need ~${cost_per_spread:.2f}, cap ${spend_cap:.2f}); skipping.",
                color="yellow",
            )
            self.vars.symbol_states[symbol] = "skipped"
            return

        dt = self.get_datetime()
        self.log_message(
            f"{symbol}: opening {qty} LEAPS call debit spreads exp {expiry} | "
            f"long {float(long_strike):.2f} | short {float(short_strike):.2f} | est debit ${cost_per_spread:.2f}",
            color="green",
        )

        # Keep original logic: submit legs separately as LIMIT orders
        long_order = self.create_order(
            long_option,
            qty,
            Order.OrderSide.BUY,
            order_type=Order.OrderType.LIMIT,
            limit_price=buy_price,
        )
        short_order = self.create_order(
            short_option,
            qty,
            Order.OrderSide.SELL,
            order_type=Order.OrderType.LIMIT,
            limit_price=sell_price,
        )

        self.submit_order(long_order)
        self.submit_order(short_order)

        # Persist full Asset objects for later management
        self.vars.open_spreads[symbol] = {
            "underlying_asset": underlying_asset,
            "long_asset": long_option,
            "short_asset": short_option,
            "expiry": expiry,
            "long_strike": float(long_strike),
            "short_strike": float(short_strike),
            "qty": qty,
            "opened_dt": dt,
        }
        self.vars.symbol_states[symbol] = "open"

        # Optional marker: only on entry events (rare)
        self.add_marker(
            "Open Spread",
            float(last_price),
            color="green",
            symbol="arrow-up",
            size=10,
            detail_text=f"Opened {qty}x {expiry} {float(long_strike):.0f}/{float(short_strike):.0f}",
            dt=dt,
            asset=underlying_asset,
        )

    def _close_spread(self, symbol: str, reason: str) -> None:
        spread = self.vars.open_spreads.get(symbol)
        if not spread:
            return

        long_asset = spread["long_asset"]
        short_asset = spread["short_asset"]
        qty = int(spread["qty"])

        self.log_message(f"{symbol}: closing spread ({reason}).", color="yellow")

        close_long = self.create_order(long_asset, qty, Order.OrderSide.SELL, order_type=Order.OrderType.MARKET)
        close_short = self.create_order(short_asset, qty, Order.OrderSide.BUY, order_type=Order.OrderType.MARKET)

        self.submit_order(close_long)
        self.submit_order(close_short)

        del self.vars.open_spreads[symbol]
        self.vars.symbol_states[symbol] = "pending"

    def _manage_spreads(self) -> None:
        roll_out_days = int(self.parameters.get("roll_out_days", 30))
        roll_up = bool(self.parameters.get("roll_up_if_underlying_above_short_strike", True))
        today = self.get_datetime().date()

        for symbol, spread in list(self.vars.open_spreads.items()):
            underlying_asset = spread["underlying_asset"]
            expiry = spread["expiry"]
            short_strike = float(spread["short_strike"])

            dte = (expiry - today).days
            self.log_message(f"{symbol}: managing spread | DTE={dte} | short_strike={short_strike:.2f}", color="blue")

            if dte <= roll_out_days:
                self._close_spread(symbol, reason=f"Roll Out (DTE {dte} <= {roll_out_days})")
                continue

            if roll_up:
                last_price = self.get_last_price(underlying_asset)
                if last_price is None:
                    self.log_message(f"{symbol}: cannot manage roll-up; missing underlying price.", color="yellow")
                    continue

                if float(last_price) >= short_strike:
                    self._close_spread(
                        symbol,
                        reason=f"Roll Up (price {float(last_price):.2f} >= short strike {short_strike:.2f})",
                    )

    def on_trading_iteration(self):
        symbols = self.parameters.get("symbols", [])
        if not symbols:
            self.log_message("No symbols provided; nothing to do.", color="yellow")
            return

        if self.vars.initial_budget_value is None:
            self.vars.initial_budget_value = float(self.parameters.get("budget", 10000))
            self.log_message(f"Initial strategy budget set to ${self.vars.initial_budget_value:.2f}", color="blue")

        per_symbol_budget = self._compute_per_symbol_budget(symbols)
        self.log_message(f"Per-symbol budget: ${per_symbol_budget:.2f} across {len(symbols)} symbols.", color="blue")

        # Added: optional throttle to avoid repeated management actions in non-daily loops
        if self.vars.open_spreads:
            manage_once_per_day = bool(self.parameters.get("manage_once_per_day", True))
            dt_now = self.get_datetime()
            if (
                (not manage_once_per_day)
                or (self.vars.last_management_dt is None)
                or (dt_now.date() != self.vars.last_management_dt.date())
            ):
                self.vars.last_management_dt = dt_now
                self._manage_spreads()
            else:
                self.log_message("Spread management already run today; skipping.", color="blue")

        for sym in symbols:
            state = self.vars.symbol_states.get(sym, "pending")
            if state == "pending":
                self._attempt_open_spread(sym, per_symbol_budget)
            else:
                self.log_message(f"{sym}: state={state}; no new entry attempt.", color="blue")

    def on_filled_order(self, position, order, price, quantity, multiplier):
        asset = order.asset if order is not None else None
        if asset is not None:
            self.log_message(f"Filled {order.side} {quantity}x {asset.symbol} @ {price}", color="green")


if __name__ == "__main__":
    # Options strategy => must use PolygonDataBacktesting for backtests
    if IS_BACKTESTING:
        from lumibot.backtesting import PolygonDataBacktesting

        trading_fee = TradingFee(percent_fee=0.001)

        results = LeapsCallDebitSpread.backtest(
            datasource_class=PolygonDataBacktesting,
            benchmark_asset=Asset("SPY", Asset.AssetType.STOCK),
            buy_trading_fees=[trading_fee],
            sell_trading_fees=[trading_fee],
            quote_asset=Asset("USD", Asset.AssetType.FOREX),
            budget=100000,
        )
    else:
        trader = Trader()
        strategy = LeapsCallDebitSpread(
            quote_asset=Asset("USD", Asset.AssetType.FOREX),
        )
        trader.add_strategy(strategy)
        trader.run_all()
