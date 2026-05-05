from decimal import Decimal
from typing import Optional, Union

from termcolor import colored

from lumibot.data_sources import DataSource
from lumibot.entities import Asset, Bars, Quote
from lumibot.tools.lumibot_logger import get_logger

logger = get_logger(__name__)


class TastytradeData(DataSource):
    """
    Data source backed by the unofficial ``tastytrade`` Python SDK.

    The Tastytrade SDK is fully asynchronous, so this class shares the
    asyncio event-loop bridge owned by the :class:`Tastytrade` broker. The
    broker passes its own ``async_runner`` callable in via the ``runner``
    kwarg; if the data source is constructed standalone, it spins up its
    own private bridge.

    Only the methods strictly required by the strategy executor are
    implemented in this initial scaffold: chain / quote / historical-price
    plumbing is intentionally left as logged stubs and will be filled in
    via :class:`tastytrade.market_data.MarketDataAPI` and the DXLink
    streamer in a follow-up commit.
    """

    MIN_TIMESTEP = "minute"
    SOURCE = "Tastytrade"

    def __init__(
        self,
        session=None,
        runner=None,
        **kwargs,
    ):
        super().__init__()
        self._session = session
        self._runner = runner

    def get_chains(self, asset: Asset, quote: Optional[Asset] = None) -> dict:
        logger.warning(colored(
            "TastytradeData.get_chains is not yet implemented; returning {}.",
            "yellow",
        ))
        return {}

    def get_historical_prices(
        self,
        asset,
        length,
        timestep="",
        timeshift=None,
        quote=None,
        exchange=None,
        include_after_hours=True,
    ) -> Optional[Bars]:
        logger.warning(colored(
            "TastytradeData.get_historical_prices is not yet implemented.",
            "yellow",
        ))
        return None

    def get_last_price(
        self,
        asset,
        quote: Optional[Asset] = None,
        exchange: Optional[str] = None,
    ) -> Union[float, Decimal, None]:
        logger.warning(colored(
            "TastytradeData.get_last_price is not yet implemented.",
            "yellow",
        ))
        return None

    def get_quote(
        self,
        asset: Asset,
        quote: Optional[Asset] = None,
        exchange: Optional[str] = None,
    ) -> Quote:
        logger.warning(colored(
            "TastytradeData.get_quote is not yet implemented.",
            "yellow",
        ))
        return Quote(asset=asset)
