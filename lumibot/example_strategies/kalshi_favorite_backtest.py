"""Kalshi prediction market backtest example: buy the favorite, hold to settlement.

For every listed market the strategy waits until either the YES or the NO side
can be bought inside a price band (default 0.80 to 0.95), buys a fixed dollar
amount of that side once, and holds until the market settles at 1 or 0.

Run it on real settled markets with public history (no account or key needed):

    python -m lumibot.example_strategies.kalshi_favorite_backtest

Optional environment variables: KALSHI_EXAMPLE_SERIES (default KXHIGHNY),
BACKTESTING_START, BACKTESTING_END (ISO dates), KALSHI_EXAMPLE_MAX_MARKETS.
Fills use the historical candle's closing ask; depth and queue position are
not modeled, so treat results as an estimate.
"""

import os
from datetime import datetime, timezone

from lumibot.entities import Asset
from lumibot.strategies.strategy import Strategy


class KalshiFavoriteStrategy(Strategy):
    parameters = {
        "tickers": [],  # Kalshi market tickers to watch
        "min_price": 0.80,  # only buy a side whose ask is at least this
        "max_price": 0.95,  # and at most this
        "dollars_per_market": 10,
        "sleeptime": "1H",
    }

    def initialize(self):
        self.sleeptime = self.parameters["sleeptime"]
        self.set_market("24/7")
        self.vars.entries = {}  # ticker -> {"side", "price", "quantity"}

    def on_trading_iteration(self):
        low, high = self.parameters["min_price"], self.parameters["max_price"]
        for ticker in self.parameters["tickers"]:
            if ticker in self.vars.entries:
                continue
            for side in ("yes", "no"):
                contract = Asset(ticker, Asset.AssetType.PREDICTION_CONTRACT, outcome=side)
                ask = self.get_quote(contract).ask
                if ask is None or not low <= ask <= high:
                    continue
                quantity = int(self.parameters["dollars_per_market"] // ask)
                if quantity < 1 or quantity * ask > self.get_cash():
                    continue
                self.submit_order(self.create_order(contract, quantity, "buy", time_in_force="ioc"))
                self.vars.entries[ticker] = {"side": side, "price": ask, "quantity": quantity}
                break


def settled_markets(client, series_ticker, start, end, limit):
    """Settled markets of one series that closed inside [start, end], oldest first."""
    found = {}
    for path, extra in (("/historical/markets", {}), ("/markets", {"status": "settled"})):
        cursor = None
        while True:
            params = {"series_ticker": series_ticker, "limit": 1000, **extra}
            if cursor:
                params["cursor"] = cursor
            page = client.request("GET", path, params=params, authenticated=False)
            for row in page.get("markets", []):
                closed = datetime.fromisoformat(row["close_time"].replace("Z", "+00:00"))
                if start <= closed <= end and row.get("result") in ("yes", "no"):
                    found[row["ticker"]] = row
            cursor = page.get("cursor")
            if not cursor or not page.get("markets"):
                break
    return sorted(found.values(), key=lambda row: row["close_time"])[:limit]


if __name__ == "__main__":
    from lumibot.backtesting import KalshiBacktesting
    from lumibot.data_sources import KalshiData
    from lumibot.tools.kalshi_client import KalshiClient

    def _day(name, default):
        return datetime.fromisoformat(os.environ.get(name, default)).replace(tzinfo=timezone.utc)

    start, end = _day("BACKTESTING_START", "2026-07-01"), _day("BACKTESTING_END", "2026-10-01")
    series = os.environ.get("KALSHI_EXAMPLE_SERIES", "KXHIGHNY")
    public = {"IS_DEMO": False, "BASE_URL": KalshiClient.PRODUCTION_URL,
              "API_KEY_ID": None, "PRIVATE_KEY": None, "PRIVATE_KEY_PATH": None}
    with KalshiClient(public) as client:
        markets = settled_markets(client, series, start, end, int(os.environ.get("KALSHI_EXAMPLE_MAX_MARKETS", "300")))
        data = KalshiData(client=client)
        budget = 1000
        _, strategy = KalshiFavoriteStrategy.run_backtest(
            KalshiBacktesting, start, end,
            kalshi_data=data, market_metadata={row["ticker"]: row for row in markets},
            timestep="hour", market="24/7", budget=budget, benchmark_asset=None,
            parameters={"tickers": [row["ticker"] for row in markets]},
            show_plot=False, show_tearsheet=False, save_tearsheet=False, show_progress_bar=False,
        )
        results = {row["ticker"]: row["result"] for row in markets}
        entries = strategy.vars.entries
        wins = sum(1 for ticker, entry in entries.items() if results[ticker] == entry["side"])
        print(f"Series {series}, {start.date()} to {end.date()}")
        print(f"Settled markets watched = {len(markets)}")
        print(f"Trades = {len(entries)}")
        print(f"Wins = {wins} ({100 * wins / max(len(entries), 1):.1f}%)")
        print(f"Average entry price = {sum(e['price'] for e in entries.values()) / max(len(entries), 1):.3f}")
        print(f"Account value = ${strategy.get_portfolio_value():,.2f} (started ${budget:,.2f})")
