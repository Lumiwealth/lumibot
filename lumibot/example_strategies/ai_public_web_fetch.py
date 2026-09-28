"""Public-page research strategy.

Python creates the agents and runs them. It does not download the page or place orders.
The research agent fetches the page. When the published page reports purchases, the trading
agent follows them, sized from the account.
"""

from lumibot.example_strategies.agent_cycle import add_agent, run_cycle, trader_prompt
from lumibot.strategies.strategy import Strategy

_PDF_URL = "https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/20033725.pdf"
_BOOK = (
    "Use the page only if it was published on or before the session time. A page not published "
    "yet, or published after the session time, is not evidence and means no trade. "
    "When the page reports real purchases of stocks or exchange-listed units by name and ticker, "
    "those purchases are the trade. The page does not need to state a size for this account: "
    "you size from our account value. Skip option lines, sales, gifts, and exchanges. "
    "When the page gives a dollar amount or range for each purchase, weight each ticker by its "
    "amount (use the midpoint of a range; it is dollars, never a share count). Otherwise weight "
    "the purchased tickers equally. Weights sum to 100% of account value. For each ticker, call "
    "risk_calculate_stock_quantity with maximum_notional equal to account value times its weight "
    "and available_cash equal to cash still unspent, then submit exactly the quantity it returns. "
    "A page with no ticker-level purchase is not a trade. Never short."
)
_EXIT = (
    "Once the positions are open, hold them on later sessions while the page is unchanged. "
    "Sell a position only if the page later reports a sale of that ticker."
)


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
            allow_network=True,
        )
        add_agent(
            self,
            "bull",
            "Argue the case for following the purchases the page actually reports. Do not submit orders.",
            allow_trading=False,
        )
        add_agent(
            self,
            "bear",
            "Argue the risks of following the page: stale or unpublished content, unclear tickers, "
            "or lines that are not purchases. Do not submit orders.",
            allow_trading=False,
        )
        add_agent(
            self,
            "interpreter",
            (
                "Read both cases and apply this policy. A page published on or before the session "
                "time that reports ticker-level purchases of stocks or units is a trade: follow those "
                "purchases, sized from our account. A page not yet published, or with no ticker-level "
                "purchase, is no trade. List each purchased ticker and its disclosed amount. "
                "If you recommend no trade, name the failed condition. Do not submit orders."
            ),
            allow_trading=False,
        )
        add_agent(
            self,
            "trading_risk_manager",
            trader_prompt(book_rule=_BOOK, exit_rule=_EXIT),
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
            research_task=(
                "Fetch the public page. Report its published time and, if it is admissible, every "
                "line with its ticker, transaction type, and dollar amount."
            ),
            bull_task="Make the bull case from the page.",
            bear_task="Make the bear case from the page.",
            interpret_task="Decide whether the page is a trade under the policy and list the purchases.",
            trade_task="Apply the book rule. Size from the account value.",
        )
