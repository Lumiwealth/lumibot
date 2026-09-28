"""SEC Form 4 insider-filings strategy for a fixed watchlist.

Python creates the agents and runs them. It does not download filings or place orders.
The research agent reads point-in-time Form 4 filings with the SEC tools. The trading agent
places the orders.
"""

from lumibot.example_strategies.agent_cycle import add_agent, run_cycle, trader_prompt
from lumibot.strategies import Strategy


class AISECInsiderFilingsStrategy(Strategy):
    parameters = {
        "watchlist": ["AAPL", "MSFT", "JPM", "BAC", "XOM", "CVX", "PFE", "INTC", "F", "KO"],
        "lookback_days": 30,
    }

    def initialize(self):
        self.sleeptime = "1D"
        add_agent(
            self,
            "insider_trade_researcher",
            (
                "For each ticker in the watchlist, call get_filings(symbol, form='4', limit=20). "
                "Keep only filings accepted within lookback_days before as_of, then open each one with "
                "get_filing_document(symbol, accession_number, primary_document). Ignore any filing "
                "published after as_of. Read the non-derivative transaction table. Transaction code P is an "
                "open-market purchase; code S is an open-market sale. Ignore grants, gifts, option "
                "exercises, tax withholding, sales under a checked 10b5-1 automatic plan, and amendments. "
                "Report each ticker with its open-market purchases and discretionary sales: insider role, "
                "shares, price, and acceptance date. Report tickers with no qualifying rows as neutral. "
                "Do not submit orders."
            ),
            allow_trading=False,
        )
        add_agent(
            self,
            "bull",
            "Argue for overweighting the watchlist names with open-market insider buys. Do not submit orders.",
            allow_trading=False,
        )
        add_agent(
            self,
            "bear",
            (
                "Argue the risks: discretionary insider sales, small purchases, stale filings, and "
                "misread amendments. Do not submit orders."
            ),
            allow_trading=False,
        )
        add_agent(
            self,
            "interpreter",
            (
                "Read both cases. Assign account weights across the watchlist tickers only, "
                "summing to 95% to 100%. Do not submit orders."
            ),
            allow_trading=False,
        )
        add_agent(
            self,
            "trading_risk_manager",
            trader_prompt(
                book_rule=(
                    "Trade only watchlist tickers. Start from equal weight across the watchlist, tilt toward "
                    "names with open-market purchases already public on as_of, and trim names with "
                    "discretionary open-market sales. Never short. Never treat a grant, gift, or option "
                    "exercise as an open-market purchase."
                ),
                exit_rule=(
                    "Reduce a name with the order tool when a newer visible filing is a discretionary "
                    "open-market sale. Otherwise hold the target weights, with cash near 0% to 5%."
                ),
            ),
            allow_trading=True,
        )

    def on_trading_iteration(self):
        as_of = self.get_datetime()
        watchlist = list(self.parameters["watchlist"])
        context = {
            "as_of": as_of.isoformat(),
            "watchlist": watchlist,
            "lookback_days": self.parameters["lookback_days"],
            "clock_rule": "Ignore any filing published after as_of.",
            "risk_policy": {
                "cash_target": "0% to 5%",
                "sizing": "equal weight across the watchlist, tilted by open-market insider activity",
                "never_short": True,
            },
        }
        run_cycle(
            self,
            context,
            researcher="insider_trade_researcher",
            bull="bull",
            bear="bear",
            interpreter="interpreter",
            trader="trading_risk_manager",
            research_task="Read the watchlist Form 4 filings and report open-market activity public on as_of.",
            bull_task="Make the bull case for the names with insider buying.",
            bear_task="Make the bear case against the tilts.",
            interpret_task="Assign account weights across the watchlist tickers.",
            trade_task="Move the account to the target watchlist weights with cash near 0% to 5%.",
        )
