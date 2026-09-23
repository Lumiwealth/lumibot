"""Public-page research strategy.

Python creates the agents and runs them. It does not download the page or place orders.
The research agent fetches the page. The trading agent places an order only when the page is a trade.
"""

from lumibot.example_strategies.agent_cycle import add_agent, run_cycle, trader_prompt
from lumibot.strategies.strategy import Strategy

_PDF_URL = "https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/20033725.pdf"


class AIPublicWebFetchStrategy(Strategy):
    parameters = {
        "pdf_url": _PDF_URL,
    }

    def initialize(self):
        self.sleeptime = "1D"
        add_agent(
            self,
            "page_researcher",
            "Use http_request to read the supplied public page. Report what it actually says. Do not submit orders.",
            allow_trading=False,
        )
        add_agent(
            self,
            "bull",
            "Argue any investment case that is actually in the page. Do not submit orders.",
            allow_trading=False,
        )
        add_agent(
            self,
            "bear",
            "Argue why the page is not a trade. Do not submit orders.",
            allow_trading=False,
        )
        add_agent(
            self,
            "interpreter",
            "Read both cases. Say whether there is a real position to take. Do not submit orders.",
            allow_trading=False,
        )
        add_agent(
            self,
            "trading_risk_manager",
            trader_prompt(
                book_rule="Trade only when the page itself is an investment decision with a ticker and a size.",
                exit_rule="If you opened a position on an earlier session, sell it before opening another.",
            ),
            allow_trading=True,
        )

    def on_trading_iteration(self):
        context = {
            "as_of": self.get_datetime().isoformat(),
            "pdf_url": self.parameters["pdf_url"],
        }
        run_cycle(
            self,
            context,
            researcher="page_researcher",
            bull="bull",
            bear="bear",
            interpreter="interpreter",
            trader="trading_risk_manager",
            research_task="Fetch the public page and say what it contains.",
            bull_task="Make the bull case from the page.",
            bear_task="Make the bear case from the page.",
            interpret_task="Decide whether the page is a trade.",
            trade_task="Apply the interpreter. Size from the account. No order is a valid result.",
        )
