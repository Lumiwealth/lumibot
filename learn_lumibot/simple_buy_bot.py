import os
from datetime import datetime
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

from lumibot.backtesting import AlpacaBacktesting
from lumibot.strategies import Strategy

load_dotenv()


def get_alpaca_config():
    api_key = os.environ.get("ALPACA_API_KEY") or os.environ.get("APCA_API_KEY_ID")
    api_secret = (
        os.environ.get("ALPACA_API_SECRET")
        or os.environ.get("ALPACA_SECRET_KEY")
        or os.environ.get("APCA_API_SECRET_KEY")
    )

    if not api_key or not api_secret:
        raise ValueError(
            "Missing Alpaca credentials. Set ALPACA_API_KEY and ALPACA_API_SECRET "
            "(or legacy ALPACA_SECRET_KEY/APCA_* aliases) in your environment or .env file."
        )

    return {
        "API_KEY": api_key,
        "API_SECRET": api_secret,
        "PAPER": os.environ.get("ALPACA_IS_PAPER", "true").lower() != "false",
    }


ALPACA_CONFIG = get_alpaca_config()


class MyStrategy(Strategy):
    def on_trading_iteration(self):
        if self.first_iteration:
            sndk = self.create_order("MU", 10, "buy")
            self.submit_order(sndk)


if __name__ == "__main__":
    nyc = ZoneInfo("America/New_York")
    start = datetime(2026, 1, 1, tzinfo=nyc)
    end = datetime(2026, 9, 10, tzinfo=nyc)

    MyStrategy.backtest(
        datasource_class=AlpacaBacktesting,
        backtesting_start=start,
        backtesting_end=end,
        config=ALPACA_CONFIG,
    )
