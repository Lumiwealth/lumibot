"""SEC Form 4 public-insider-filings strategy.

Python creates the agents and runs them. It does not download the feed or place orders.
The research agent fetches the public Atom feed. The trading agent places the orders.
"""

from lumibot.example_strategies.agent_cycle import add_agent, run_cycle, trader_prompt
from lumibot.strategies import Strategy

_FEED_URL = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&output=atom"


class AISECInsiderFilingsStrategy(Strategy):
    parameters = {
        "feed_url": _FEED_URL,
    }

    def initialize(self):
        self.sleeptime = "1D"
        add_agent(
            self,
            "insider_trade_researcher",
            (
                "Use rss_fetch on the supplied SEC Form 4 feed. Ignore any entry whose published time "
                "is after as_of. Ignore grants, gifts, option exercises, automatic plans, and amendments. "
                "Keep open-market purchases and sales only. Report the ticker, the side, and the published time. "
                "Do not submit orders."
            ),
            allow_trading=False,
        )
        add_agent(
            self,
            "bull",
            "Argue for copying the open-market insider buys. Do not submit orders.",
            allow_trading=False,
        )
        add_agent(
            self,
            "bear",
            "Argue the risks: grants, amendments, thin names, and sales. Do not submit orders.",
            allow_trading=False,
        )
        add_agent(
            self,
            "interpreter",
            "Read both cases. Assign account weights for the open-market tickers only. Do not submit orders.",
            allow_trading=False,
        )
        add_agent(
            self,
            "trading_risk_manager",
            trader_prompt(
                book_rule=(
                    "Trade only exact tickers from open-market Form 4 rows already public on as_of. "
                    "Never short. Never treat a grant, gift, or option exercise as an open-market purchase."
                ),
                exit_rule=(
                    "Sell a name with the order tool when a later visible filing is a sale. "
                    "Otherwise stay invested, with cash near 0% to 5%."
                ),
            ),
            allow_trading=True,
        )

    def on_trading_iteration(self):
        as_of = self.get_datetime()
        context = {
            "as_of": as_of.isoformat(),
            "feed_url": self.parameters["feed_url"],
            "clock_rule": "Ignore any feed entry published after as_of.",
            "risk_policy": {
                "cash_target": "0% to 5%",
                "sizing": "scale the open-market filings to account value",
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
            research_task="Fetch the Form 4 feed and report only open-market rows already public on as_of.",
            bull_task="Make the bull case for the open-market buys.",
            bear_task="Make the bear case against copying the filings.",
            interpret_task="Assign account weights for the open-market tickers.",
            trade_task="Scale the visible open-market book to this account. Sell names the filings sold.",
        )
