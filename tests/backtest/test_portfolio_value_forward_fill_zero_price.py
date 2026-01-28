import datetime
import logging
import threading
from types import SimpleNamespace

from lumibot.entities import Asset
from lumibot.strategies.strategy import Strategy


class _PortfolioValueZeroPriceStrategy(Strategy):
    @property
    def cash(self):
        return float(self._cash)

    def update_broker_balances(self, force_update=False):
        return None


def test_update_portfolio_value_forward_fills_on_zero_price():
    """
    Regression test: portfolio value must not collapse to cash when a data source
    returns 0 for a held asset price on a non-trading timestamp.

    Observed in production-like runs as a ~99% drop in portfolio value on Sunday
    timestamps (e.g., 2019-06-09) when the held position's mark price was 0.
    """
    gld = Asset("GLD", Asset.AssetType.STOCK)
    usd = Asset("USD", Asset.AssetType.FOREX)

    position = SimpleNamespace(asset=gld, quantity=10, avg_fill_price=None)
    broker = SimpleNamespace(
        get_tracked_positions=lambda _: [position],
        data_source=object(),
        option_source=None,
        datetime=datetime.datetime(2019, 6, 9, 19, 0, 0, tzinfo=datetime.timezone.utc),
    )

    strategy = _PortfolioValueZeroPriceStrategy.__new__(_PortfolioValueZeroPriceStrategy)
    strategy.is_backtesting = True
    strategy._executor = SimpleNamespace(lock=threading.Lock())
    strategy._name = "test"
    strategy._quote_asset = usd
    strategy.broker = broker
    strategy.logger = logging.getLogger("test.portfolio_value.zero_price")

    # Starting cash and last known price (e.g., last close before the non-trading timestamp).
    strategy._cash = 1000.0
    strategy._last_known_prices = {gld: 100.0}

    # Simulate a data source returning 0.0 for "no price".
    strategy._get_price_from_source = lambda _source, _asset: 0.0

    pv = Strategy._update_portfolio_value(strategy)
    assert pv == 2000.0
    assert strategy._last_known_prices[gld] == 100.0

