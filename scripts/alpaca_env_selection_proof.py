"""Real runs of AlpacaBacktesting selected ONLY through BACKTESTING_DATA_SOURCE=alpaca.

This mirrors how BotSpot runs a backtest: the strategy calls
``backtest(datasource_class=None)`` with no config and no timestep, and the environment
carries BACKTESTING_DATA_SOURCE=alpaca, ALPACA_IS_PAPER=true, the Alpaca credentials
(ALPACA_API_KEY/ALPACA_API_SECRET or ALPACA_OAUTH_TOKEN) and BACKTESTING_START/BACKTESTING_END.
See docs/investigations/2026-09-23_alpaca-options-backtesting-and-ibkr-4592-window-regression.md.

Strategies:

- ``orb``: SPY 5-minute opening range breakout. The range is the 09:30, 09:35 and 09:40
  five-minute bars. From 09:50 to 11:00 ET, buy 10 shares on the first completed five-minute
  bar that closes above the range high. Sell at or after 15:50 ET. Only completed bars are
  used. The strategy keeps its own completed-bar filter as a guard and counts every bar the
  data source returned that was still forming; since the 2026-09-23 lookahead fix the count
  must be zero, and the run logs it at the end. At its first bar it also asks for 250
  five-minute bars and 15 daily bars (what an ATR(14) filter needs) and logs what came back,
  so the run shows history reaching before BACKTESTING_START. That probe never changes a trade.
- ``weekly_call``: the weekly SPY call from scripts/alpaca_options_backtest_proof.py.

Usage (credentials come from your environment; nothing here is secret):

    LUMIBOT_DISABLE_DOTENV_LOCAL=1 LUMIBOT_CACHE_BACKEND=local LUMIBOT_CACHE_MODE=disabled \\
    BACKTESTING_DATA_SOURCE=alpaca ALPACA_IS_PAPER=true \\
    BACKTESTING_START=2026-08-03 BACKTESTING_END=2026-08-08 \\
    PYTHONPATH=. python scripts/alpaca_env_selection_proof.py orb --name alpaca_env_orb_2026
"""

from __future__ import annotations

import argparse
import os
from datetime import timedelta

import pandas as pd

from lumibot.entities import Asset
from lumibot.strategies import Strategy


class SpyOpeningRangeBreakout(Strategy):
    parameters = {"symbol": "SPY", "shares": 10}

    def initialize(self):
        self.sleeptime = "5M"
        self.vars.day = None
        self.vars.range_high = None
        self.vars.entered = False
        self.vars.history_calls = 0
        self.vars.forming_bars_returned = 0
        self.vars.probed_first_bar = False

    def _completed_five_minute_bars(self, count: int) -> pd.DataFrame:
        now = pd.Timestamp(self.get_datetime())
        bars = self.get_historical_prices(self.parameters["symbol"], count + 1, "5minute")
        if bars is None or bars.df.empty:
            return pd.DataFrame()
        df = bars.df
        completed = df[df.index + pd.Timedelta(minutes=5) <= now]
        # Evidence for the lookahead fix: a bar labeled t closes at t + 5 minutes, so any row
        # the filter removes is a bar the source returned while it was still forming.
        self.vars.history_calls += 1
        self.vars.forming_bars_returned += len(df) - len(completed)
        return completed

    def _probe_history_at_first_bar(self) -> None:
        """Evidence only: the history a strategy that needs a long lookback gets at its first bar."""
        now = pd.Timestamp(self.get_datetime())
        symbol = self.parameters["symbol"]
        report = []
        for label, length, timestep in (("five-minute", 250, "5minute"), ("daily", 15, "day")):
            try:
                bars = self.get_historical_prices(symbol, length, timestep)
            except Exception as exc:  # the old window raised "Not enough historical data"
                report.append(f"{label}: error {exc}")
                continue
            if bars is None or bars.df.empty:
                report.append(f"{label}: none of {length}")
                continue
            df = bars.df
            if timestep == "day":
                forming = int((df.index.date >= now.date()).sum())
            else:
                forming = int((df.index + pd.Timedelta(minutes=5) > now).sum())
            before_start = int((df.index < now.normalize()).sum())
            text = (
                f"{label}: {len(df)} of {length}, {df.index[0]} to {df.index[-1]}, "
                f"{before_start} dated before {now.date()}, {forming} still forming"
            )
            if timestep == "day" and len(df) >= 15:
                prev_close = df["close"].shift(1)
                true_range = pd.concat(
                    [df["high"] - df["low"], (df["high"] - prev_close).abs(), (df["low"] - prev_close).abs()],
                    axis=1,
                ).max(axis=1)
                text += f", ATR(14)={true_range.iloc[-14:].mean():.2f}"
            report.append(text)
        self.log_message(f"{now} HISTORY AT FIRST BAR: " + "; ".join(report), color="blue")

    def on_trading_iteration(self):
        now = self.get_datetime()
        if not self.vars.probed_first_bar:
            self.vars.probed_first_bar = True
            self._probe_history_at_first_bar()
        today = now.date()
        if self.vars.day != today:
            self.vars.day = today
            self.vars.range_high = None
            self.vars.entered = False
        hm = (now.hour, now.minute)
        symbol = self.parameters["symbol"]

        if self.vars.range_high is None:
            if hm < (9, 45):
                return
            df = self._completed_five_minute_bars(6)
            session = df[df.index.date == today]
            opening = session[(session.index.hour == 9) & (session.index.minute.isin([30, 35, 40]))]
            if len(opening) != 3:
                self.log_message(f"{now} opening range incomplete ({len(opening)} bars)", color="yellow")
                return
            self.vars.range_high = float(opening["high"].max())
            self.log_message(f"{now} RANGE high={self.vars.range_high:.2f} from {list(opening.index.strftime('%H:%M'))}", color="blue")
            return

        position = self.get_position(Asset(symbol))
        held = position is not None and position.quantity > 0
        if not self.vars.entered and (9, 50) <= hm <= (11, 0):
            last = self._completed_five_minute_bars(1).tail(1)
            if not last.empty and float(last["close"].iloc[-1]) > self.vars.range_high:
                self.log_message(
                    f"{now} BUY {self.parameters['shares']} {symbol}: {last.index[-1].strftime('%H:%M')} bar closed "
                    f"{float(last['close'].iloc[-1]):.2f} above {self.vars.range_high:.2f}",
                    color="green",
                )
                self.submit_order(self.create_order(symbol, self.parameters["shares"], "buy"))
                self.vars.entered = True
        if held and hm >= (15, 50):
            self.log_message(f"{now} SELL {position.quantity} {symbol}", color="green")
            self.submit_order(self.create_order(symbol, position.quantity, "sell"))

    def on_filled_order(self, position, order, price, quantity, multiplier):
        self.log_message(f"FILLED {order.side} {quantity} {order.asset} at {price} ({self.get_datetime()})", color="blue")

    def on_strategy_end(self):
        self.log_message(
            f"HISTORY CHECK: {self.vars.history_calls} five-minute history calls, "
            f"{self.vars.forming_bars_returned} bars returned while still forming",
            color="blue",
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("strategy", choices=["orb", "weekly_call"])
    parser.add_argument("--name", required=True, help="artifact name prefix")
    parser.add_argument(
        "--legacy-forming-bar",
        action="store_true",
        help="pass remove_incomplete_current_bar=False to reproduce the history the source returned "
        "before the 2026-09-23 lookahead fix (for before/after evidence only)",
    )
    parser.add_argument(
        "--no-history-before-start",
        action="store_true",
        help="pass history_before_start=False to reproduce the data window that started at "
        "BACKTESTING_START (for before/after evidence only)",
    )
    args = parser.parse_args()

    if (os.environ.get("BACKTESTING_DATA_SOURCE") or "").strip().lower() != "alpaca":
        raise SystemExit("Set BACKTESTING_DATA_SOURCE=alpaca: this proof must select the source from the environment.")

    if args.strategy == "orb":
        strategy_class = SpyOpeningRangeBreakout
    else:
        from alpaca_options_backtest_proof import WeeklySpyCall

        strategy_class = WeeklySpyCall

    extra = {"remove_incomplete_current_bar": False} if args.legacy_forming_bar else {}
    if args.no_history_before_start:
        extra["history_before_start"] = False
    # The BotSpot template: no datasource_class, no config, no timestep, dates from the environment.
    strategy_class.backtest(
        datasource_class=None,
        name=args.name,
        budget=10_000,
        benchmark_asset=Asset("SPY"),
        show_plot=False,
        show_tearsheet=False,
        save_tearsheet=True,
        show_indicators=False,
        show_progress_bar=False,
        **extra,
    )


if __name__ == "__main__":
    main()
