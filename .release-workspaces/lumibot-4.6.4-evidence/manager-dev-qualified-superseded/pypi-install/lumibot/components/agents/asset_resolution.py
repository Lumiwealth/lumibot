from datetime import date, datetime
from typing import Any

from lumibot.entities import Asset


_ASSET_TYPE_ALIASES = {
    "us_equity": Asset.AssetType.STOCK,
}


def _normalize_asset_type(value: Any) -> Any:
    if value is None or isinstance(value, Asset.AssetType):
        return value
    normalized = str(value).strip().lower()
    if normalized in _ASSET_TYPE_ALIASES:
        return _ASSET_TYPE_ALIASES[normalized]
    try:
        return Asset.AssetType(normalized)
    except Exception:
        return value


def _normalize_right(value: Any) -> Any:
    if isinstance(value, str) and value.strip():
        return value.strip().upper()
    return value


def _normalize_expiration(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.date()
    return value


def _normalized_strike(asset_type: Any, strike: Any) -> Any:
    if strike is not None:
        return strike
    normalized_type = _normalize_asset_type(asset_type)
    if normalized_type in {Asset.AssetType.STOCK, Asset.AssetType.FOREX, Asset.AssetType.CRYPTO, Asset.AssetType.INDEX}:
        return 0.0
    return strike


def _assets_match(
    asset: Any,
    *,
    symbol: str,
    asset_type: Any,
    expiration: Any = None,
    strike: Any = None,
    right: Any = None,
) -> bool:
    if not isinstance(asset, Asset):
        return False
    if asset.symbol != symbol.upper():
        return False
    if _normalize_asset_type(asset.asset_type) != _normalize_asset_type(asset_type):
        return False
    if _normalize_expiration(getattr(asset, "expiration", None)) != _normalize_expiration(expiration):
        return False
    if _normalized_strike(asset.asset_type, getattr(asset, "strike", None)) != _normalized_strike(asset_type, strike):
        return False
    if _normalize_right(getattr(asset, "right", None)) != _normalize_right(right):
        return False
    return True


def _iter_data_store_assets(strategy: Any):
    data_source = getattr(strategy, "_data_source", None) or getattr(strategy, "data_source", None)
    data_store = getattr(data_source, "_data_store", None)
    if not isinstance(data_store, dict):
        return
    for key in data_store.keys():
        if isinstance(key, tuple) and len(key) == 2:
            yield key[0], key[1]
        else:
            yield key, None


def _build_asset(
    *,
    symbol: str,
    asset_type: Any,
    expiration: Any = None,
    strike: Any = None,
    right: Any = None,
) -> Asset:
    return Asset(
        symbol=symbol,
        asset_type=_normalize_asset_type(asset_type) or Asset.AssetType.STOCK,
        expiration=_normalize_expiration(expiration),
        strike=_normalized_strike(asset_type, strike),
        right=_normalize_right(right),
    )


def resolve_asset_and_quote(
    strategy: Any,
    *,
    symbol: str,
    asset_type: Any = Asset.AssetType.STOCK,
    expiration: date | datetime | None = None,
    strike: float | None = None,
    right: str | None = None,
    quote_symbol: str | None = None,
) -> tuple[Asset, Asset | None]:
    normalized_asset_type = _normalize_asset_type(asset_type) or Asset.AssetType.STOCK
    normalized_expiration = _normalize_expiration(expiration)
    normalized_right = _normalize_right(right)
    normalized_strike_value = _normalized_strike(normalized_asset_type, strike)
    normalized_quote_symbol = quote_symbol.upper() if isinstance(quote_symbol, str) and quote_symbol.strip() else None

    for store_asset, store_quote in _iter_data_store_assets(strategy) or []:
        if not _assets_match(
            store_asset,
            symbol=symbol,
            asset_type=normalized_asset_type,
            expiration=normalized_expiration,
            strike=normalized_strike_value,
            right=normalized_right,
        ):
            continue
        if normalized_quote_symbol is None:
            return store_asset, store_quote
        if isinstance(store_quote, Asset) and store_quote.symbol == normalized_quote_symbol:
            return store_asset, store_quote

    asset = _build_asset(
        symbol=symbol,
        asset_type=normalized_asset_type,
        expiration=normalized_expiration,
        strike=normalized_strike_value,
        right=normalized_right,
    )
    quote = None
    if normalized_quote_symbol:
        quote_asset_type = Asset.AssetType.CRYPTO if normalized_asset_type == Asset.AssetType.CRYPTO else Asset.AssetType.FOREX
        quote = _build_asset(symbol=normalized_quote_symbol, asset_type=quote_asset_type)
    return asset, quote
