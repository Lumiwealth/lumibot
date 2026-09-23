"""Real Alpaca options backtest: weekly SPY call chosen from get_chains().

Proof run for AlpacaBacktesting option chains and option fills (see
docs/investigations/2026-09-23_alpaca-options-backtesting-and-ibkr-4592-window-regression.md).

Every week, on the first trading day at or after 09:35 ET, buy one SPY call:
- expiration: the nearest listed expiration 7 to 21 days out (1 to 3 weeks),
- strike: the listed strike closest to SPY's current price.
On the last trading day of that week at or after 15:30 ET, sell it.

Market orders only fill on a real option trade bar in the current minute, so the
fill time and price in trades.csv are the first print at or after submission.

Usage (credentials come from your environment; nothing here is secret):

    LUMIBOT_DISABLE_DOTENV_LOCAL=1 BACKTESTING_DATA_SOURCE=none \\
    LUMIBOT_CACHE_BACKEND=local LUMIBOT_CACHE_MODE=disabled \\
    python scripts/alpaca_options_backtest_proof.py --start 2026-07-27 --end 2026-08-19 --name alpaca_spy_weekly_call_2026

Requires ALPACA_API_KEY and ALPACA_API_SECRET (or ALPACA_OAUTH_TOKEN) for a paper account.
AlpacaBacktesting stops three trading days before `--end` (existing behavior), so pass an
end date three sessions after the last Friday you want traded.
"""

from __future__ import annotations

import argparse
import os
from datetime import date, datetime, timedelta

import pytz

from lumibot.backtesting import AlpacaBacktesting
from lumibot.entities import Asset
from lumibot.strategies import Strategy

NY = pytz.timezone("America/New_York")


class WeeklySpyCall(Strategy):
    parameters = {
        "underlying": "SPY",
        "min_days_to_expiration": 7,
        "max_days_to_expiration": 21,
        "entry_after": "09:35",
        "exit_after": "15:30",
    }

    def initialize(self):
        self.sleeptime = "1M"
        self.vars.week_bought = None
        self.vars.open_contract = None

    def _hm(self, text: str) -> tuple[int, int]:
        hour, minute = text.split(":")
        return int(hour), int(minute)

    def _is_first_session_of_week(self, today: date) -> bool:
        monday = today - timedelta(days=today.weekday())
        days = self.get_trading_days() if hasattr(self, "get_trading_days") else None
        if days is None:
            return today.weekday() == 0
        sessions = [d for d in days if monday <= d <= today]
        return bool(sessions) and sessions[0] == today

    def _is_last_session_of_week(self, today: date) -> bool:
        friday = today - timedelta(days=today.weekday()) + timedelta(days=4)
        days = self.get_trading_days() if hasattr(self, "get_trading_days") else None
        if days is None:
            return today.weekday() == 4
        sessions = [d for d in days if today <= d <= friday]
        return bool(sessions) and sessions[-1] == today

    def get_trading_days(self):
        cached = getattr(self, "_proof_trading_days", None)
        if cached is None:
            import pandas_market_calendars as mcal

            start = self.broker.data_source.datetime_start.date() - timedelta(days=7)
            end = self.broker.data_source.datetime_end.date() + timedelta(days=14)
            schedule = mcal.get_calendar("NYSE").schedule(start_date=start, end_date=end)
            cached = [ts.date() for ts in schedule.index]
            self._proof_trading_days = cached
        return cached

    def on_trading_iteration(self):
        now = self.get_datetime()
        today = now.date()
        underlying = Asset(self.parameters["underlying"])

        if self.vars.open_contract is None:
            if self.vars.week_bought == today.isocalendar()[:2]:
                return
            if not self._is_first_session_of_week(today) or (now.hour, now.minute) < self._hm(self.parameters["entry_after"]):
                return
            spot = self.get_last_price(underlying)
            if spot is None:
                self.log_message(f"{now} no SPY price yet", color="yellow")
                return
            chains = self.get_chains(underlying)
            calls = chains.get("Chains", {}).get("CALL", {}) if chains else {}
            eligible = []
            for expiry_text, strikes in calls.items():
                days_out = (date.fromisoformat(expiry_text) - today).days
                if self.parameters["min_days_to_expiration"] <= days_out <= self.parameters["max_days_to_expiration"] and strikes:
                    eligible.append((days_out, expiry_text, strikes))
            if not eligible:
                self.log_message(f"{now} no call expiration {self.parameters['min_days_to_expiration']}-"
                                 f"{self.parameters['max_days_to_expiration']} days out in the chain", color="red")
                self.vars.week_bought = today.isocalendar()[:2]
                return
            days_out, expiry_text, strikes = sorted(eligible)[0]
            strike = min(strikes, key=lambda value: (abs(value - float(spot)), value))
            contract = Asset(
                self.parameters["underlying"],
                asset_type=Asset.AssetType.OPTION,
                expiration=date.fromisoformat(expiry_text),
                strike=strike,
                right=Asset.OptionRight.CALL,
            )
            self.log_message(
                f"{now} BUY 1 {contract} spot={float(spot):.2f} chain_calls={len(calls)} expirations,"
                f" picked {expiry_text} ({days_out}d) strike {strike}",
                color="green",
            )
            self.submit_order(self.create_order(contract, 1, "buy"))
            self.vars.open_contract = contract
            self.vars.week_bought = today.isocalendar()[:2]
            return

        position = self.get_position(self.vars.open_contract)
        if position is None or not position.quantity:
            return  # entry not filled yet (waiting for a real print)
        if self._is_last_session_of_week(today) and (now.hour, now.minute) >= self._hm(self.parameters["exit_after"]):
            open_sells = [
                o for o in self.get_orders()
                if o.asset == self.vars.open_contract and o.is_sell_order() and o.is_active()
            ]
            if open_sells:
                return
            self.log_message(f"{now} SELL {position.quantity} {self.vars.open_contract}", color="green")
            self.submit_order(self.create_order(self.vars.open_contract, position.quantity, "sell"))

    def on_filled_order(self, position, order, price, quantity, multiplier):
        self.log_message(f"FILLED {order.side} {quantity} {order.asset} at {price} ({self.get_datetime()})", color="blue")
        if order.is_sell_order() and order.asset == self.vars.open_contract:
            self.vars.open_contract = None


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD (three sessions after the last traded Friday)")
    parser.add_argument("--name", required=True, help="artifact name prefix")
    parser.add_argument("--source", choices=["alpaca", "polygon"], default="alpaca", help="data source (default alpaca)")
    args = parser.parse_args()

    start = NY.localize(datetime.fromisoformat(args.start))
    end = NY.localize(datetime.fromisoformat(args.end))
    common = dict(
        backtesting_start=start,
        backtesting_end=end,
        name=args.name,
        budget=10_000,
        benchmark_asset=Asset("SPY"),
        show_plot=False,
        show_tearsheet=False,
        save_tearsheet=True,
        show_indicators=False,
        show_progress_bar=False,
    )

    if args.source == "polygon":
        # Same strategy on a customer's own Polygon key. Bound the chain listing with
        # LUMIBOT_OPTION_CHAIN_MAX_DAYS (for example 21) so a free key is not rate limited.
        from lumibot.backtesting import PolygonDataBacktesting

        WeeklySpyCall.run_backtest(
            PolygonDataBacktesting,
            polygon_api_key=os.environ.get("POLYGON_API_KEY"),
            **common,
        )
        return

    config = {
        "API_KEY": os.environ.get("ALPACA_API_KEY"),
        "API_SECRET": os.environ.get("ALPACA_API_SECRET"),
        "OAUTH_TOKEN": os.environ.get("ALPACA_OAUTH_TOKEN"),
        "PAPER": True,
    }
    WeeklySpyCall.run_backtest(
        AlpacaBacktesting,
        # AlpacaBacktesting kwargs
        timestep="minute",
        market="NYSE",
        config=config,
        **common,
    )


if __name__ == "__main__":
    main()
