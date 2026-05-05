import datetime
from decimal import Decimal
from typing import Callable, Optional, Union

from termcolor import colored

from lumibot.data_sources import DataSource
from lumibot.entities import Asset, Bars, Quote
from lumibot.tools.lumibot_logger import get_logger

logger = get_logger(__name__)

try:
    from tastytrade.instruments import get_option_chain as _tt_get_option_chain
    from tastytrade.market_data import get_market_data as _tt_get_market_data
    from tastytrade.order import InstrumentType as _TTInstrumentType
except Exception:  # pragma: no cover
    _tt_get_option_chain = None
    _tt_get_market_data = None
    _TTInstrumentType = None


class TastytradeData(DataSource):
    """
    Data source backed by the unofficial ``tastytrade`` Python SDK.

    The SDK is fully asynchronous, so this class shares the asyncio event-
    loop bridge owned by the :class:`Tastytrade` broker. The broker passes
    its ``async_runner`` callable (``self._run``) in via the ``runner``
    kwarg so every SDK call is dispatched through the same bridge.

    Implemented:
    - ``get_last_price``: REST market-data snapshot (mid → last → mark)
    - ``get_quote``: REST market-data snapshot with bid/ask/mid/last
    - ``get_chains``: option chain expanded into Lumibot's nested
      ``{"Multiplier": 100, "Chains": {"CALL": {...}, "PUT": {...}}}`` shape

    Stubbed (follow-up):
    - ``get_historical_prices``: needs the DXLink streamer or a separate
      historical-bar source.
    """

    MIN_TIMESTEP = "minute"
    SOURCE = "Tastytrade"

    def __init__(
        self,
        session=None,
        runner: Optional[Callable] = None,
        **kwargs,
    ):
        super().__init__()
        self._session = session
        # ``runner`` is the broker's _AsyncBridge.run; if absent we fall back
        # to ``asyncio.run`` per call (slow but functional in standalone use).
        self._runner = runner

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _await(self, coro):
        if self._runner is not None:
            return self._runner(coro)
        import asyncio
        return asyncio.run(coro)

    def _instrument_type_for(self, asset: Asset) -> "_TTInstrumentType":
        if asset.asset_type == Asset.AssetType.STOCK:
            return _TTInstrumentType.EQUITY
        if asset.asset_type == Asset.AssetType.OPTION:
            return _TTInstrumentType.EQUITY_OPTION
        if asset.asset_type == Asset.AssetType.INDEX:
            return _TTInstrumentType.INDEX
        raise ValueError(
            f"Tastytrade data source does not yet support asset_type "
            f"{asset.asset_type!r}."
        )

    def _symbol_for(self, asset: Asset) -> str:
        if asset.asset_type == Asset.AssetType.OPTION:
            # Reuse the broker's OCC formatter.
            from lumibot.brokers.tastytrade import Tastytrade
            return Tastytrade._to_occ_symbol(asset)
        return (asset.symbol or "").upper()

    @staticmethod
    def _decimal_to_float(d) -> Optional[float]:
        if d is None:
            return None
        try:
            return float(d)
        except Exception:
            return None

    # ------------------------------------------------------------------
    # Implemented
    # ------------------------------------------------------------------
    def get_last_price(
        self,
        asset,
        quote: Optional[Asset] = None,
        exchange: Optional[str] = None,
    ) -> Union[float, Decimal, None]:
        if self._session is None or _tt_get_market_data is None:
            return None
        try:
            md = self._await(_tt_get_market_data(
                self._session,
                self._symbol_for(asset),
                self._instrument_type_for(asset),
            ))
        except Exception as e:
            logger.error(colored(
                f"[TastytradeData] get_last_price({asset!r}) failed: {e}", "red",
            ))
            return None

        # Preference order: mid > last > mark > (bid+ask)/2.
        for attr in ("mid", "last", "mark"):
            val = getattr(md, attr, None)
            if val is not None:
                return self._decimal_to_float(val)
        bid = getattr(md, "bid", None)
        ask = getattr(md, "ask", None)
        if bid is not None and ask is not None:
            return self._decimal_to_float((Decimal(str(bid)) + Decimal(str(ask))) / 2)
        return None

    def get_quote(
        self,
        asset: Asset,
        quote: Optional[Asset] = None,
        exchange: Optional[str] = None,
    ) -> Quote:
        if self._session is None or _tt_get_market_data is None:
            return Quote(asset=asset)
        try:
            md = self._await(_tt_get_market_data(
                self._session,
                self._symbol_for(asset),
                self._instrument_type_for(asset),
            ))
        except Exception as e:
            logger.error(colored(
                f"[TastytradeData] get_quote({asset!r}) failed: {e}", "red",
            ))
            return Quote(asset=asset)

        last = getattr(md, "last", None) or getattr(md, "mark", None)
        mid = getattr(md, "mid", None)
        bid = getattr(md, "bid", None)
        ask = getattr(md, "ask", None)
        if mid is None and bid is not None and ask is not None:
            mid = (Decimal(str(bid)) + Decimal(str(ask))) / 2

        return Quote(
            asset=asset,
            price=self._decimal_to_float(last),
            bid=self._decimal_to_float(bid),
            ask=self._decimal_to_float(ask),
            mid_price=self._decimal_to_float(mid),
            bid_size=self._decimal_to_float(getattr(md, "bid_size", None)),
            ask_size=self._decimal_to_float(getattr(md, "ask_size", None)),
            volume=self._decimal_to_float(getattr(md, "volume", None)),
            timestamp=getattr(md, "updated_at", None),
            raw_data={"tt_market_data": md},
        )

    def get_chains(self, asset: Asset, quote: Optional[Asset] = None) -> dict:
        """Return option chain in Lumibot's nested shape.

        Output:
            {
                "Multiplier": 100,
                "Chains": {
                    "CALL": {"YYYY-MM-DD": [strike1, strike2, ...], ...},
                    "PUT":  {"YYYY-MM-DD": [strike1, strike2, ...], ...},
                },
            }
        """
        if self._session is None or _tt_get_option_chain is None:
            return {}
        symbol = (asset.symbol or "").upper()
        try:
            tt_chain = self._await(_tt_get_option_chain(self._session, symbol))
        except Exception as e:
            logger.error(colored(
                f"[TastytradeData] get_chains({symbol!r}) failed: {e}", "red",
            ))
            return {}

        calls: dict = {}
        puts: dict = {}
        for expiration, options in (tt_chain or {}).items():
            exp_str = expiration.strftime("%Y-%m-%d") if isinstance(
                expiration, (datetime.date, datetime.datetime)
            ) else str(expiration)
            for opt in options:
                opt_type = getattr(getattr(opt, "option_type", None), "value", None)
                strike = getattr(opt, "strike_price", None)
                if strike is None:
                    continue
                strike_f = self._decimal_to_float(strike)
                if str(opt_type).lower().startswith("c"):
                    calls.setdefault(exp_str, []).append(strike_f)
                elif str(opt_type).lower().startswith("p"):
                    puts.setdefault(exp_str, []).append(strike_f)

        # Sort strikes per expiration for deterministic output.
        for buckets in (calls, puts):
            for exp in buckets:
                buckets[exp] = sorted(buckets[exp])

        return {
            "Multiplier": 100,
            "Chains": {"CALL": calls, "PUT": puts},
        }

    # ------------------------------------------------------------------
    # Stubbed
    # ------------------------------------------------------------------
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
            "TastytradeData.get_historical_prices is not yet implemented. "
            "Use a separate historical bar source for now.",
            "yellow",
        ))
        return None
