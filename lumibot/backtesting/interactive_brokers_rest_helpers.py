from __future__ import annotations

from typing import Optional

from lumibot.entities import Asset
from lumibot.tools.lazy_import import _LazyModule

USD_FOREX = Asset("USD", "forex")

pd = _LazyModule("pandas")
ibkr_helper = _LazyModule("lumibot.tools.ibkr_helper")

_DATA_CLASS = None
_BARS_CLASS = None
_PARSE_TIMESTEP_QTY_AND_UNIT = None


def normalize_exchange_key(exchange: Optional[str]) -> str:
    exch = (exchange or "").strip().upper()
    return exch or "AUTO"


def normalize_asset_type(value: object) -> str:
    raw = str(value or "").strip().lower()
    if "." in raw:
        raw = raw.split(".")[-1]
    return raw


def ibkr_include_after_hours(asset_type: str, timestep_unit: str) -> bool:
    """Return IBKR outsideRth policy for backtests."""
    return not (asset_type in {"stock", "index"} and timestep_unit == "day")


def build_dataset_keys(
    asset: Asset,
    quote: Optional[Asset],
    dataset_key: str,
    exchange: Optional[str],
) -> tuple[tuple, tuple]:
    quote_asset = quote if quote is not None else USD_FOREX
    exch = normalize_exchange_key(exchange)
    canonical_key = (asset, quote_asset, dataset_key, exch)
    legacy_key = (asset, quote_asset, exch)
    return canonical_key, legacy_key


def parse_timestep_qty_and_unit(*args, **kwargs):
    global _PARSE_TIMESTEP_QTY_AND_UNIT
    if _PARSE_TIMESTEP_QTY_AND_UNIT is None:
        from lumibot.tools.helpers import parse_timestep_qty_and_unit as parser

        _PARSE_TIMESTEP_QTY_AND_UNIT = parser
    return _PARSE_TIMESTEP_QTY_AND_UNIT(*args, **kwargs)


def normalize_timestep_key(timestep: str) -> str:
    """Normalize a user-facing timestep into a stable series key."""
    if timestep in {"minute", "day", "hour"}:
        return timestep
    qty, unit = parse_timestep_qty_and_unit(timestep)
    qty = int(qty)
    unit = str(unit)
    if qty == 1:
        return unit
    return f"{qty}{unit}"


def data_class():
    global _DATA_CLASS
    if _DATA_CLASS is None:
        from lumibot.entities import Data

        _DATA_CLASS = Data
    return _DATA_CLASS


def bars_class():
    global _BARS_CLASS
    if _BARS_CLASS is None:
        from lumibot.entities.bars import Bars

        _BARS_CLASS = Bars
    return _BARS_CLASS
