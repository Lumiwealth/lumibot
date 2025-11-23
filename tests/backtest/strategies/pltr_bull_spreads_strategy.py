from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


# -*- coding: utf-8 -*-
from datetime import timedelta
from zoneinfo import ZoneInfo
from typing import Dict, List, Tuple
import traceback

from lumibot.strategies.strategy import Strategy
from lumibot.traders import Trader
from lumibot.entities import Asset, TradingFee, Order
from lumibot.backtesting import PolygonDataBacktesting
from lumibot.credentials import IS_BACKTESTING
from lumibot.components.options_helper import OptionsHelper

"""
This code was refined based on the user prompt: 'make a bot that trades bull call spreads on pltr. it should get the chains on pltr that are 30 days to expiry to later and the buy call should be about 10% out of the money and the sell about 20% out of the money. it should keep buying these options every month and trade 20 spreads at a time. the initial budget should be $1000; the symbol should also be in the parameters'
and the update: 'the spread quantity should actually be based on the portfolio value rather than a fixed number' as well as 'remove spread_quantity from the parameters because it is no longer used, and incorporate parameters to control the dynamic quantity calculation'

Latest refinement: 'make the budget $100,000 and make the whole strategy adjust to the size of the account, when we buy the options we should use 5-10% of the account each time'.

Newest refinement: 'Increase the percentage that we trade each time to 10-15% of the account.'

Current refinement: 'Add a take-profit check at 11:00 AM MST if spread price is above entry and trade a list of symbols (OKLO, TEM, CRWV, NVDA, ACHR, HOOD, IBKR, APLD, HIMS, MP, NVTS, RGTI, QS, NIO, SOFI, QUBT, AMD, FIG, NBIS, RKLB, FROG, POWL, HOND, USAR, PATH, CRDO, FSLY). Only buy options when the stock is above prior day close and showing upward momentum; close positions if downward momentum persists for 15 minutes.'

Newest change in this revision: 'implement a stop loss at 2% at entry point.'

Backtest stability fix: 'reduce data pressure by rotating a small subset of symbols each iteration and increasing the iteration interval; cache trade cadence and avoid heavy calls when unnecessary.'

This code was refined based on the user prompt: 'show me the error for my back test' — added explicit crash handlers and per-symbol error logging so the exact failure is surfaced in logs and the console during backtests.

Earlier change: 'Keep most of the stuff the same. However lets do less transaction. only do trade for symbols that have a high initial slope and the VIX is below 15' — High initial slope gate retained.

Latest user request applied here: 'remove the vix settings put back code to where it was before. However start initial balance to $50,000' — Removed all VIX gating/parameters and set the backtest budget to $50,000.

Newest user request: 'Have the same as version 17 but use $50,000 as your money' — Preserved V17-style crash visibility and confirmed backtest budget is $50,000.

Current request: 'keep the same strategy, However add a stoploss of 15%' — Updated stop_loss_pct default parameter to 0.15 and kept the existing stop-loss logic.

Newest request in this revision: 'only trade stocks that have a high momentum. When the 30 minute momentum shifts to a downwards slope and passes the 15 minute momentum. Close the order and get out of the trade.' — Implemented dual-momentum exit and stricter high-momentum entry.

Latest user request (this revision): 'I'm going to keep the same strategy, however this time we are only going to trade 3 symbols per day. The symbols that have the highest momentum for that day are the ones that will be traded.' — Implemented daily momentum ranking selecting the top 3 symbols per day for new entries only.

Newest user request (this revision): 'make  max_symbols_per_day=3 to 6. IN addition you need to add a parameter where it will scan the listed stocks and only trade the stock that has the momentum with the highest slope.' — Set max_symbols_per_day to 6 and added trade_only_top_slope parameter to optionally restrict daily entries to the single highest-slope symbol.

Latest user request (this revision): 'Remove the slope and put in the symbol that starts gaining a high momentum.' — Removed initial slope filter and set single-symbol top-momentum selection by default.
"""

class BullCallSpreadStrategy(Strategy):
    # Parameters: multi-symbol momentum filters, time-based take profit, stop loss, data-throttling, and high-momentum gating.
    parameters = {
        # Increased iteration interval to reduce backtest data load (kept from prior stability fix)
        "sleeptime": "5M",

        # Spread construction rules (same core logic as before)
        "target_days_to_expiry": 30,     # Minimum days to expiration
        "buy_otm": 1.10,                 # ~10% OTM for the long call (multiplier of underlying price)
        "sell_otm": 1.20,                # ~20% OTM for the short call (multiplier of underlying price)

        # Allocation controls: per-trade budget bounds (10-15%) with a target (12.5%)
        "min_allocation_pct": 0.10,      # Lower bound spend per trade
        "max_allocation_pct": 0.15,      # Upper bound spend per trade
        "target_allocation_pct": 0.125,  # Preferred spend per trade

        # Trading universe (uppercase tickers as provided)
        "symbols": [
            "OKLO","TEM","CRWV","NVDA","ACHR","HOOD","IBKR","APLD","HIMS","MP","NVTS","RGTI","QS","NIO","SOFI","QUBT","AMD","FIG","RKLB","FROG","POWL","HOND","PATH","CRDO","FSLY"
        ],

        # Momentum and timing controls
        "momentum_lookback_min": 15,                 # Short-term momentum window (15 minutes)
        "momentum_exit_lookback_long_min": 30,       # Long-term momentum window for exits (30 minutes)
        "entry_momentum_min_pct": 0.005,             # Require at least +0.5% 15m momentum for entries ("high momentum")
        "take_profit_hour_mst": 11,                  # 11:00 AM MST time-of-day profit check
        "take_profit_minute": 0,                     # At the top of the hour by default

        # Stop loss control: closes spread when current value falls ≥15% below entry estimate
        "stop_loss_pct": 0.15,  # default 15%

        # Trade cadence gate to avoid over-trading, keep "monthly" as before per symbol
        "buy_once_per_month": True,

        # Backtest stability throttling — process at most N symbols per iteration (round-robin)
        # Note: kept for compatibility, but daily top-momentum selection overrides batch rotation for new entries
        "max_symbols_per_iteration": 6,

        # Daily cap — only trade the top N symbols by 15m momentum each day (kept as 6)
        "max_symbols_per_day": 6,

        # Updated: trade only the single symbol with the highest 15m momentum each day (per latest user request)
        "trade_only_top_slope": True,

        # NOTE: VIX gating was removed per user request; no VIX parameters are used anymore.
    }

    def initialize(self):
        # This is called once at the start
        self.sleeptime = self.parameters.get("sleeptime")

        # Instantiate helpers
        self.options_helper = OptionsHelper(self)          # MANDATORY for options selection & order execution

        # Persistent state holders:
        # - last trade timestamp by symbol as (year, month)
        self.vars.last_trade_ym_by_symbol: Dict[str, Tuple[int, int]] = self.vars.get("last_trade_ym_by_symbol", {})
        # - track open spreads by symbol for take-profit/stop-loss checks and targeted exits
        #   each item: {"expiry": date, "buy_strike": float, "sell_strike": float, "quantity": int, "entry_debit_est": float}
        self.vars.open_spreads: Dict[str, List[dict]] = self.vars.get("open_spreads", {})
        # - rotation pointer for symbol throttling (round-robin across large lists)
        self.vars.symbol_pointer = self.vars.get("symbol_pointer", 0)
        # NEW: daily momentum selection state
        self.vars.daily_selected_symbols: List[str] = self.vars.get("daily_selected_symbols", [])
        self.vars.last_selection_date = self.vars.get("last_selection_date", None)  # stores a date object

    # Crash visibility: surface any unexpected errors in logs (including backtests)
    def on_bot_crash(self, error: Exception):
        tb = traceback.format_exc()
        self.log_message(f"Strategy crash: {error}", color="red")
        for line in tb.splitlines():
            self.log_message(line[:500], color="red")

    # -------------- Helper methods --------------
    def _get_prev_day_close(self, asset: Asset) -> float:
        # Defensive guard against data range issues
        try:
            bars = self.get_historical_prices(asset, 2, "day")
        except Exception as e:
            self.log_message(f"[{asset.symbol}] Daily bars fetch failed for prev close: {e}", color="yellow")
            return None
        if bars is None or getattr(bars, "df", None) is None or len(bars.df) < 2:
            return None
        if "close" not in bars.df.columns:
            self.log_message(f"[{asset.symbol}] Daily bars missing 'close' column.", color="yellow")
            return None
        return float(bars.df["close"].iloc[-2])

    def _get_minute_bars(self, asset: Asset, length: int) -> List[float]:
        # Keep minute window requests as small as possible to reduce load on the data source
        try:
            bars = self.get_historical_prices(asset, length, "minute")
        except Exception as e:
            self.log_message(f"[{asset.symbol}] Minute bars fetch failed: {e}", color="yellow")
            return []
        if bars is None or getattr(bars, "df", None) is None or len(bars.df) < 1:
            return []
        if "close" not in bars.df.columns:
            self.log_message(f"[{asset.symbol}] Minute bars missing 'close' column.", color="yellow")
            return []
        closes = list(bars.df["close"].astype(float))
        return closes[-length:]

    def _calculate_momentum_pct_change(self, all_minute_closes: List[float], lookback_min: int) -> float:
        """
        Calculate percentage change over `lookback_min` using provided minute closes.
        Returns 0.0 if not enough data.
        """
        if len(all_minute_closes) < lookback_min + 1:
            return 0.0
        start_price = all_minute_closes[-(lookback_min + 1)]
        end_price = all_minute_closes[-1]
        if start_price is None or end_price is None or start_price <= 0:
            return 0.0
        return (end_price - start_price) / start_price

    def _momentum_flags(self, underlying_asset: Asset) -> Tuple[bool, bool, float, float, float]:
        """
        Returns a tuple:
        (upward_momentum, downward_short_term_original, last_price, momentum_15m_pct, momentum_30m_pct)
        - Upward momentum: price above prior day close AND last close is near the top of recent range (last 5 mins).
        - Downward short-term: original 15m down filter used for preventing entries.
        - momentum_15m_pct: simple 15-minute percent change used for entry strength.
        - momentum_30m_pct: simple 30-minute percent change used for exit acceleration.
        """
        prev_close = self._get_prev_day_close(underlying_asset)
        try:
            last_price = self.get_last_price(underlying_asset)
        except Exception as e:
            self.log_message(f"[{underlying_asset.symbol}] get_last_price failed: {e}", color="yellow")
            last_price = None
        if last_price is None or prev_close is None:
            return (False, False, last_price if last_price is not None else 0.0, 0.0, 0.0)

        lookback_short = int(self.parameters.get("momentum_lookback_min", 15))
        lookback_long = int(self.parameters.get("momentum_exit_lookback_long_min", 30))
        max_minute_lookback = max(6, lookback_short + 1, lookback_long + 1)
        all_minute_closes = self._get_minute_bars(underlying_asset, max_minute_lookback)
        if len(all_minute_closes) < 2:
            return (False, False, last_price, 0.0, 0.0)

        # Upward momentum: price above prior day close AND last close >= max of recent last 5 bars
        recent_slice = all_minute_closes[-min(5, len(all_minute_closes)) :]
        recent_max = max(recent_slice) if recent_slice else last_price
        upward = (last_price > prev_close) and (last_price >= recent_max)

        # Original 15m downward momentum (kept for entry prevention)
        downward_short = False
        if len(all_minute_closes) >= (lookback_short + 1):
            window_short = all_minute_closes[-(lookback_short + 1) :]
            down_moves_short = sum(1 for i in range(1, len(window_short)) if window_short[i] < window_short[i - 1])
            downward_short = (down_moves_short >= int(lookback_short * 2 / 3)) and (window_short[-1] < window_short[0])

        # Percentage momentum values for 15m and 30m
        momentum_15m_pct = self._calculate_momentum_pct_change(all_minute_closes, lookback_short)
        momentum_30m_pct = self._calculate_momentum_pct_change(all_minute_closes, lookback_long)

        return (upward, downward_short, last_price, momentum_15m_pct, momentum_30m_pct)

    def _get_mst_now(self):
        # Convert LumiBot's datetime to America/Phoenix (MST year-round)
        dt = self.get_datetime()
        try:
            return dt.astimezone(ZoneInfo("America/Phoenix"))
        except Exception:
            return dt

    def _record_open_spread(self, symbol: str, expiry, buy_strike: float, sell_strike: float, quantity: int, entry_debit_est: float):
        self.vars.open_spreads.setdefault(symbol, [])
        self.vars.open_spreads[symbol].append({
            "expiry": expiry,
            "buy_strike": float(buy_strike),
            "sell_strike": float(sell_strike),
            "quantity": int(quantity),
            "entry_debit_est": float(entry_debit_est),
        })

    def _close_spread_positions_for_symbol(self, symbol: str, reason: str):
        # Close any open option positions for the given symbol (both legs)
        positions = self.get_positions()
        close_orders = []
        for pos in positions:
            asset = pos.asset
            if asset.asset_type != Asset.AssetType.OPTION:
                continue
            if asset.symbol != symbol:
                continue
            qty = abs(float(pos.quantity))
            if qty <= 0:
                continue
            side = Order.OrderSide.SELL if pos.quantity > 0 else Order.OrderSide.BUY
            close_orders.append(self.create_order(asset, qty, side))

        if close_orders:
            self.submit_order(close_orders)
            self.log_message(f"Closed option positions for {symbol} due to: {reason}", color="yellow")
            if symbol in self.vars.open_spreads:
                self.vars.open_spreads[symbol] = []

    def _check_11am_take_profit(self, symbol: str):
        # Evaluate open spreads for the symbol and close profitable ones at the TP time
        mst_now = self._get_mst_now()
        if (mst_now.hour != int(self.parameters.get("take_profit_hour_mst", 11)) or
                mst_now.minute != int(self.parameters.get("take_profit_minute", 0))):
            return  # Not TP time

        if symbol not in self.vars.open_spreads or len(self.vars.open_spreads[symbol]) == 0:
            return

        still_open = []
        for rec in self.vars.open_spreads[symbol]:
            expiry = rec["expiry"]
            buy_strike = rec["buy_strike"]
            sell_strike = rec["sell_strike"]
            qty = rec["quantity"]
            entry_debit_est = rec["entry_debit_est"]

            long_call = Asset(symbol, asset_type=Asset.AssetType.OPTION, expiration=expiry, strike=buy_strike, right=Asset.OptionRight.CALL)
            short_call = Asset(symbol, asset_type=Asset.AssetType.OPTION, expiration=expiry, strike=sell_strike, right=Asset.OptionRight.CALL)

            eval_long = self.options_helper.evaluate_option_market(long_call, max_spread_pct=0.30)
            eval_short = self.options_helper.evaluate_option_market(short_call, max_spread_pct=0.30)
            if eval_long is None or eval_short is None:
                self.log_message(f"[{symbol}] Unable to evaluate option market for TP check.", color="yellow")
                still_open.append(rec)
                continue

            self.log_message(f"[{symbol}] TP eval long: bid={eval_long.bid} ask={eval_long.ask} buy={eval_long.buy_price}", color="blue")
            self.log_message(f"[{symbol}] TP eval short: bid={eval_short.bid} ask={eval_short.ask} sell={eval_short.sell_price}", color="blue")

            if eval_long.buy_price is None or eval_short.sell_price is None:
                self.log_message(f"[{symbol}] Missing prices for TP check (long buy price or short sell price).", color="yellow")
                still_open.append(rec)
                continue

            current_debit = (eval_long.buy_price - eval_short.sell_price) * 100.0
            if current_debit > entry_debit_est:
                close_orders = [
                    self.create_order(long_call, qty, Order.OrderSide.SELL),
                    self.create_order(short_call, qty, Order.OrderSide.BUY),
                ]
                self.submit_order(close_orders)
                self.log_message(
                    f"[{symbol}] 11:00 MST take-profit: entry ~${entry_debit_est:.2f}, now ~${current_debit:.2f}. Closing spread.",
                    color="green",
                )
                last_px = self.get_last_price(Asset(symbol, asset_type=Asset.AssetType.STOCK))
                if last_px is not None:
                    self.add_marker("TP Close", float(last_px), color="green", symbol="star", size=10,
                                    detail_text=f"{symbol} TP {buy_strike}/{sell_strike}")
            else:
                still_open.append(rec)
        self.vars.open_spreads[symbol] = still_open

    def _check_stop_loss(self, symbol: str):
        """
        Stop-loss check
        - Closes a tracked bull call spread if the current debit to enter (buy_long - sell_short) drops
          below entry_debit_est by the configured percentage (default now 15%).
        """
        if symbol not in self.vars.open_spreads or len(self.vars.open_spreads[symbol]) == 0:
            return

        stop_loss_threshold = float(self.parameters.get("stop_loss_pct", 0.15))  # Uses the parameter (default 15%)
        if stop_loss_threshold <= 0:
            return

        still_open = []
        for rec in self.vars.open_spreads[symbol]:
            expiry = rec["expiry"]
            buy_strike = rec["buy_strike"]
            sell_strike = rec["sell_strike"]
            qty = rec["quantity"]
            entry_debit_est = rec["entry_debit_est"]

            long_call = Asset(symbol, asset_type=Asset.AssetType.OPTION, expiration=expiry, strike=buy_strike, right=Asset.OptionRight.CALL)
            short_call = Asset(symbol, asset_type=Asset.AssetType.OPTION, expiration=expiry, strike=sell_strike, right=Asset.OptionRight.CALL)

            eval_long = self.options_helper.evaluate_option_market(long_call, max_spread_pct=0.30)
            eval_short = self.options_helper.evaluate_option_market(short_call, max_spread_pct=0.30)
            if eval_long is None or eval_short is None:
                self.log_message(f"[{symbol}] Unable to evaluate option market for SL check.", color="yellow")
                still_open.append(rec)
                continue

            self.log_message(f"[{symbol}] SL eval long: bid={eval_long.bid} ask={eval_long.ask} buy={eval_long.buy_price}", color="blue")
            self.log_message(f"[{symbol}] SL eval short: bid={eval_short.bid} ask={eval_short.ask} sell={eval_short.sell_price}", color="blue")

            if eval_long.buy_price is None or eval_short.sell_price is None:
                self.log_message(f"[{symbol}] Missing prices for SL check (long buy price or short sell price).", color="yellow")
                still_open.append(rec)
                continue

            current_debit = (eval_long.buy_price - eval_short.sell_price) * 100.0
            if current_debit < (entry_debit_est * (1.0 - stop_loss_threshold)):
                close_orders = [
                    self.create_order(long_call, qty, Order.OrderSide.SELL),
                    self.create_order(short_call, qty, Order.OrderSide.BUY),
                ]
                self.submit_order(close_orders)
                self.log_message(
                    f"[{symbol}] Stop-loss hit ({stop_loss_threshold*100:.1f}%): entry ~${entry_debit_est:.2f}, now ~${current_debit:.2f}. Closing spread.",
                    color="red",
                )
                last_px = self.get_last_price(Asset(symbol, asset_type=Asset.AssetType.STOCK))
                if last_px is not None:
                    self.add_marker("SL Close", float(last_px), color="red", symbol="arrow-down", size=10,
                                    detail_text=f"{symbol} SL {buy_strike}/{sell_strike}")
            else:
                still_open.append(rec)
        self.vars.open_spreads[symbol] = still_open

    def _check_momentum_exit(self, symbol: str):
        """
        Dual-momentum exit condition for an open spread — close if 30m down-momentum is steeper than 15m.
        """
        if symbol not in self.vars.open_spreads or not self.vars.open_spreads[symbol]:
            return
        underlying = Asset(symbol, asset_type=Asset.AssetType.STOCK)
        _, _, _, momentum_15m_pct, momentum_30m_pct = self._momentum_flags(underlying)
        if (momentum_30m_pct < 0) and (momentum_30m_pct < momentum_15m_pct):
            self.log_message(
                f"[{symbol}] Momentum exit: 30m={momentum_30m_pct:.2%} < 15m={momentum_15m_pct:.2%}. Closing.",
                color="red",
            )
            self._close_spread_positions_for_symbol(symbol, reason="30m momentum steeper than 15m (down)")

    def _daily_select_top_symbols(self, symbols: List[str]) -> List[str]:
        """
        Select the top-N symbols by 15-minute momentum once per day.
        Applies the entry_momentum_min_pct threshold and skips symbols already traded this month.
        If trade_only_top_slope=True, restrict selection to the single highest-momentum symbol daily.
        """
        mst_today = self._get_mst_now().date()
        if self.vars.last_selection_date == mst_today:
            return self.vars.daily_selected_symbols  # already selected today

        self.log_message(
            f"New day {mst_today} — ranking symbols by 15m momentum to select top {int(self.parameters.get('max_symbols_per_day', 3))}.",
            color="blue",
        )
        lookback_short = int(self.parameters.get("momentum_lookback_min", 15))
        lookback_long = int(self.parameters.get("momentum_exit_lookback_long_min", 30))
        needed_len = max(6, lookback_short + 1, lookback_long + 1)
        entry_mom_min = float(self.parameters.get("entry_momentum_min_pct", 0.005))

        dt = self.get_datetime()
        current_ym = (dt.year, dt.month)
        scored: List[Tuple[str, float]] = []
        for sym in symbols:
            try:
                if self.parameters.get("buy_once_per_month", True):
                    last_ym = self.vars.last_trade_ym_by_symbol.get(sym)
                    if last_ym == current_ym:
                        # Skip symbols that already traded this month for new entries
                        continue
                asset = Asset(sym, asset_type=Asset.AssetType.STOCK)
                closes = self._get_minute_bars(asset, needed_len)
                if len(closes) < lookback_short + 1:
                    continue
                mom15 = self._calculate_momentum_pct_change(closes, lookback_short)
                scored.append((sym, mom15))
            except Exception as e:
                self.log_message(f"Momentum scoring error for {sym}: {e}", color="yellow")
                continue

        # Rank by 15m momentum descending and keep those over threshold
        scored.sort(key=lambda x: x[1], reverse=True)
        max_n = int(self.parameters.get("max_symbols_per_day", 3))
        selected_all = [s for s, m in scored if m >= entry_mom_min]

        # Restriction — only trade the single highest-momentum symbol when enabled
        if self.parameters.get("trade_only_top_slope", False):
            selected = selected_all[:1]
            if selected:
                self.log_message(f"Top-momentum mode ON — Selected single highest momentum: {selected[0]}", color="blue")
            else:
                self.log_message("Top-momentum mode ON — No symbol met the momentum threshold.", color="yellow")
        else:
            selected = selected_all[:max_n]

        self.vars.daily_selected_symbols = selected
        self.vars.last_selection_date = mst_today
        if selected:
            self.log_message(f"Daily selected symbols: {', '.join(selected)}", color="blue")
        else:
            self.log_message("No symbols met daily momentum criteria today.", color="yellow")
        return selected

    # -------------- Core iteration --------------
    def on_trading_iteration(self):
        # Global safety net so a single unexpected provider error never crashes the whole iteration
        try:
            dt = self.get_datetime()
            current_ym = (dt.year, dt.month)

            symbols: List[str] = [s.upper() for s in self.parameters.get("symbols", [])]
            if not symbols:
                self.log_message("No symbols configured.", color="red")
                return

            # Always run the 11:00 MST TP check for symbols that actually have open spreads, regardless of selection batch
            try:
                mst_now = self._get_mst_now()
                if (mst_now.hour == int(self.parameters.get("take_profit_hour_mst", 11)) and
                    mst_now.minute == int(self.parameters.get("take_profit_minute", 0))):
                    for sym_with_open in list(self.vars.open_spreads.keys()):
                        if self.vars.open_spreads.get(sym_with_open):
                            try:
                                self._check_11am_take_profit(sym_with_open)
                            except Exception as e:
                                self.log_message(f"[TP loop] Error for {sym_with_open}: {e}", color="yellow")
            except Exception as e:
                self.log_message(f"[TP scheduler] Error: {e}", color="yellow")

            # Select top momentum symbols once per day; only these are eligible for new entries
            daily_selected = self._daily_select_top_symbols(symbols)
            batch = daily_selected  # override rotation with top-momentum selection for entries

            # Proactively run SL and momentum exit checks for ALL open symbols each iteration (safety first)
            for sym_open in list(self.vars.open_spreads.keys()):
                if self.vars.open_spreads.get(sym_open):
                    try:
                        self._check_stop_loss(sym_open)
                        self._check_momentum_exit(sym_open)
                    except Exception as e:
                        self.log_message(f"[{sym_open}] Exit checks error: {e}", color="yellow")

            self.log_message(
                f"Processing daily selected symbols ({len(batch)}): {', '.join(batch) if batch else 'None'}", color="blue"
            )

            # Process each symbol in the daily selection for potential new entries (and manage exits if they are present here)
            for symbol in batch:
                try:
                    underlying = Asset(symbol, asset_type=Asset.AssetType.STOCK)

                    # Get and plot the last price once per symbol (safe/optional)
                    last_px = None
                    try:
                        last_px = self.get_last_price(underlying)
                    except Exception as e:
                        self.log_message(f"[{symbol}] get_last_price error: {e}", color="yellow")
                    if last_px is not None:
                        try:
                            self.add_line(symbol, float(last_px), color="black", width=2, detail_text=f"{symbol} Last Price")
                        except Exception:
                            pass

                    has_open = bool(self.vars.open_spreads.get(symbol))

                    # If there are open spreads: handle exits first and skip any new entries
                    if has_open:
                        # Stop Loss check early to reduce risk (default 15%)
                        try:
                            self._check_stop_loss(symbol)
                        except Exception as e:
                            self.log_message(f"[{symbol}] SL check error: {e}", color="yellow")

                        # Dual-momentum exit condition — close if 30m down-momentum is steeper than 15m
                        _, _, _, momentum_15m_pct, momentum_30m_pct = self._momentum_flags(underlying)
                        if (momentum_30m_pct < 0) and (momentum_30m_pct < momentum_15m_pct):
                            self.log_message(
                                f"[{symbol}] Momentum exit: 30m={momentum_30m_pct:.2%} < 15m={momentum_15m_pct:.2%}. Closing.",
                                color="red",
                            )
                            self._close_spread_positions_for_symbol(symbol, reason="30m momentum steeper than 15m (down)")
                            continue

                        # Scheduled TP check (no-op if not TP time)
                        try:
                            self._check_11am_take_profit(symbol)
                        except Exception as e:
                            self.log_message(f"[{symbol}] TP check error: {e}", color="yellow")

                        # Do not open a new spread while one is open
                        self.log_message(f"[{symbol}] Open spread exists; skipping new entries.", color="yellow")
                        continue

                    # If no open position: skip heavy work when we already know we won't trade
                    if self.parameters.get("buy_once_per_month", True):
                        last_ym = self.vars.last_trade_ym_by_symbol.get(symbol)
                        if last_ym == current_ym:
                            self.log_message(f"[{symbol}] Already traded this month. Skipping.", color="yellow")
                            continue

                    # Momentum evaluation (only when we're eligible to consider a new entry)
                    upward, downward_short, _, momentum_15m_pct, momentum_30m_pct = self._momentum_flags(underlying)

                    # Enforce "only trade stocks that have a high momentum": require strong 15m momentum
                    entry_mom_min = float(self.parameters.get("entry_momentum_min_pct", 0.005))
                    if momentum_15m_pct < entry_mom_min:
                        self.log_message(
                            f"[{symbol}] 15m momentum {momentum_15m_pct:.2%} < min {entry_mom_min:.2%}. Skipping entry.",
                            color="yellow",
                        )
                        continue

                    if downward_short:
                        # Persistent short-term downward momentum means skip entry
                        self.log_message(f"[{symbol}] Downward 15m momentum persists. Skipping entry.", color="yellow")
                        continue

                    if not upward:
                        self.log_message(f"[{symbol}] No upward momentum above prior close. Skipping buy.", color="yellow")
                        continue

                    # Build target expiry using OptionsHelper (ALWAYS use the helper for options)
                    try:
                        chains = self.get_chains(underlying)
                    except Exception as e:
                        self.log_message(f"[{symbol}] Option chains fetch error: {e}", color="yellow")
                        chains = None
                    if not chains:
                        self.log_message(f"[{symbol}] Option chains unavailable. Skipping.", color="red")
                        continue

                    target_expiry_dt = dt + timedelta(days=int(self.parameters.get("target_days_to_expiry", 30)))
                    try:
                        # Wrap to avoid rare chain edge-case crashes
                        expiry = self.options_helper.get_expiration_on_or_after_date(
                            target_expiry_dt, chains, "call", underlying_asset=underlying
                        )
                    except Exception as e:
                        self.log_message(f"[{symbol}] Failed to get valid options expiration: {e}", color="yellow")
                        expiry = None
                    if not expiry:
                        self.log_message(f"[{symbol}] No valid expiration on/after {target_expiry_dt.date()}. Skipping.", color="red")
                        continue

                    # Compute target strikes and validate with OptionsHelper
                    underlying_price = None
                    try:
                        underlying_price = self.get_last_price(underlying)
                    except Exception as e:
                        self.log_message(f"[{symbol}] get_last_price error: {e}", color="yellow")
                    if underlying_price is None:
                        self.log_message(f"[{symbol}] Price unavailable. Skipping.", color="red")
                        continue

                    buy_target_strike = underlying_price * float(self.parameters.get("buy_otm", 1.10))
                    sell_target_strike = underlying_price * float(self.parameters.get("sell_otm", 1.20))

                    # Find tradeable options near the targets; the helper checks that data exists
                    long_call = self.options_helper.find_next_valid_option(underlying, buy_target_strike, expiry, "call")
                    short_call = self.options_helper.find_next_valid_option(underlying, sell_target_strike, expiry, "call")
                    if not long_call or not short_call:
                        self.log_message(f"[{symbol}] Could not find valid calls near target strikes. Skipping.", color="red")
                        continue

                    # Ensure bull call direction: long lower strike, short higher strike
                    if short_call.strike <= long_call.strike:
                        short_call = self.options_helper.find_next_valid_option(underlying, long_call.strike * 1.02, expiry, "call")
                        if not short_call or short_call.strike <= long_call.strike:
                            self.log_message(f"[{symbol}] Unable to find a higher short strike for a bull call. Skipping.", color="red")
                            continue

                    # Evaluate both legs to estimate spread debit and confirm liquidity
                    eval_long = self.options_helper.evaluate_option_market(long_call, max_spread_pct=0.30)
                    eval_short = self.options_helper.evaluate_option_market(short_call, max_spread_pct=0.30)

                    if (eval_long is None or eval_short is None or
                        eval_long.buy_price is None or eval_short.sell_price is None or
                        eval_long.spread_too_wide or eval_short.spread_too_wide):
                        self.log_message(
                            f"[{symbol}] Market evaluation failed or spreads too wide. Skipping entry.", color="yellow"
                        )
                        continue

                    # Log the evals so backtests surface pricing/liq issues clearly
                    self.log_message(f"[{symbol}] Entry eval long: bid={eval_long.bid} ask={eval_long.ask} buy={eval_long.buy_price}", color="blue")
                    self.log_message(f"[{symbol}] Entry eval short: bid={eval_short.bid} ask={eval_short.ask} sell={eval_short.sell_price}", color="blue")

                    spread_debit_est = (eval_long.buy_price - eval_short.sell_price) * 100.0  # options x100
                    if spread_debit_est <= 0:
                        self.log_message(f"[{symbol}] Estimated spread debit <= 0. Skipping.", color="red")
                        continue

                    # Portfolio-aware sizing between 10% and 15% (target 12.5%)
                    pv = self.get_portfolio_value()
                    min_pct = float(self.parameters.get("min_allocation_pct", 0.10))
                    max_pct = float(self.parameters.get("max_allocation_pct", 0.15))
                    target_pct = float(self.parameters.get("target_allocation_pct", 0.125))

                    min_alloc = pv * min_pct
                    max_alloc = pv * max_pct
                    target_alloc = pv * target_pct

                    max_affordable_qty = int(max_alloc // spread_debit_est)
                    target_qty = int(target_alloc // spread_debit_est)

                    if max_affordable_qty < 1:
                        self.log_message(
                            f"[{symbol}] Not enough budget up to {max_pct*100:.1f}% (need ~${spread_debit_est:.2f} per spread).", color="yellow"
                        )
                        continue

                    quantity = target_qty if target_qty >= 1 else 1
                    quantity = min(quantity, max_affordable_qty)

                    est_spend = quantity * spread_debit_est
                    self.log_message(
                        f"[{symbol}] PV=${pv:,.2f} | Debit≈${spread_debit_est:.2f} | Qty={quantity} | Est spend=${est_spend:,.2f} "
                        f"(Bounds: {min_pct*100:.1f}%=${min_alloc:,.2f}, {max_pct*100:.1f}%=${max_alloc:,.2f}, Target {target_pct*100:.1f}%=${target_alloc:,.2f})",
                        color="blue",
                    )

                    # Build and execute the vertical spread via OptionsHelper for correctness and price handling
                    orders = self.options_helper.build_call_vertical_spread_orders(
                        underlying_asset=underlying,
                        expiry=expiry,
                        lower_strike=long_call.strike,
                        upper_strike=short_call.strike,
                        quantity=quantity,
                    )
                    success = self.options_helper.execute_orders(orders, limit_type="mid")
                    if not success:
                        self.log_message(f"[{symbol}] Failed to submit vertical spread orders.", color="red")
                        continue

                    self.log_message(
                        f"[{symbol}] Submitted bull call spread: BUY {quantity}x {long_call.strike}C / SELL {quantity}x {short_call.strike}C exp {expiry}",
                        color="green",
                    )

                    # Record this trade for later TP/SL evaluation (uses entry_debit_est as baseline)
                    self._record_open_spread(symbol, expiry, long_call.strike, short_call.strike, quantity, spread_debit_est)

                    # Add a marker for the new trade (sparingly)
                    if last_px is None:
                        last_px = self.get_last_price(underlying)
                    if last_px is not None:
                        try:
                            self.add_marker(
                                name="Bull Call Opened",
                                value=float(last_px),
                                color="green",
                                symbol="arrow-up",
                                size=10,
                                detail_text=f"{symbol} {quantity}x {long_call.strike}/{short_call.strike}C"
                            )
                        except Exception:
                            pass

                    # Mark this month as traded for the symbol (to keep original monthly cadence)
                    if self.parameters.get("buy_once_per_month", True):
                        self.vars.last_trade_ym_by_symbol[symbol] = current_ym

                except Exception as e:
                    # Per-symbol error surfacing so you can see exactly which ticker/step failed in a backtest
                    self.log_message(f"Error while processing {symbol}: {e}", color="red")
                    for line in traceback.format_exc().splitlines():
                        self.log_message(line[:500], color="red")
                    continue
        except Exception as e:
            # Global catch-all for any iteration-level failures
            self.log_message(f"Iteration error: {e}", color="red")
            for line in traceback.format_exc().splitlines():
                self.log_message(line[:500], color="red")


if __name__ == "__main__":
    if IS_BACKTESTING:
        # Polygon is required for options + minute data
        trading_fee = TradingFee(percent_fee=0.001)
        try:
            result = BullCallSpreadStrategy.backtest(
                PolygonDataBacktesting,
                benchmark_asset=Asset("SPY", Asset.AssetType.STOCK),
                buy_trading_fees=[trading_fee],
                sell_trading_fees=[trading_fee],
                quote_asset=Asset("USD", Asset.AssetType.FOREX),
                budget=50000  # Budget explicitly set to $50,000 per prior user request
            )
        except Exception as e:
            # Explicitly print the backtest error and full traceback so it's visible even if the engine swallows logs
            print("BACKTEST ERROR:", str(e))
            print(traceback.format_exc())
            raise
    else:
        # Live trading (broker auto-selected via environment)
        trader = Trader()
        strategy = BullCallSpreadStrategy(
            quote_asset=Asset("USD", Asset.AssetType.FOREX)
        )
        trader.add_strategy(strategy)
        strategies = trader.run_all()
