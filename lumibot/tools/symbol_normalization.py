import re

# Canonical LumiBot internal form for US class-share equities.
INTERNAL_CLASS_SHARE_SEPARATOR = "."

_CLASS_SHARE_PATTERNS = {
    ".": re.compile(r"^(?P<root>[A-Z]{1,6})\.(?P<suffix>[A-Z])$"),
    "/": re.compile(r"^(?P<root>[A-Z]{1,6})/(?P<suffix>[A-Z])$"),
    " ": re.compile(r"^(?P<root>[A-Z]{1,6}) (?P<suffix>[A-Z])$"),
}

# Broker-native preferred separators for class-share stock symbols.
_BROKER_CLASS_SHARE_SEPARATORS = {
    "tradier": "/",
    "schwab": "/",
    "interactive_brokers": " ",
    "ibkr": " ",
    "alpaca": ".",
}


def _is_nan(value: object) -> bool:
    try:
        # NaN is the only value that is not equal to itself.
        return value != value
    except Exception:
        return False


def _normalize_asset_type(asset_type: object | None) -> str | None:
    if asset_type is None:
        return None
    return str(asset_type).strip().lower()


def _asset_type_supports_class_share_normalization(asset_type: object | None) -> bool:
    normalized = _normalize_asset_type(asset_type)
    if normalized is None:
        return True
    return normalized in {"stock", "option", "index"}


def _normalize_symbol_text(symbol: object) -> object:
    if symbol is None or _is_nan(symbol):
        return symbol
    return str(symbol).strip().upper()


def _parse_class_share_symbol(symbol: object) -> tuple[str, str] | None:
    sym = _normalize_symbol_text(symbol)
    if not isinstance(sym, str) or not sym:
        return None

    for pattern in _CLASS_SHARE_PATTERNS.values():
        match = pattern.match(sym)
        if match:
            return match.group("root"), match.group("suffix")
    return None


def normalize_symbol_for_internal(symbol: object, asset_type: object | None = None):
    """
    Normalize broker/native class-share stock symbols to LumiBot's internal canonical format (dot notation).

    Examples:
    - BRK/B -> BRK.B
    - BRK B -> BRK.B
    - BRK.B -> BRK.B
    """
    sym = _normalize_symbol_text(symbol)
    if not isinstance(sym, str):
        return sym

    if not _asset_type_supports_class_share_normalization(asset_type):
        return sym

    parsed = _parse_class_share_symbol(sym)
    if not parsed:
        return sym

    root, suffix = parsed
    return f"{root}{INTERNAL_CLASS_SHARE_SEPARATOR}{suffix}"


def normalize_symbol_for_broker(
    symbol: object,
    broker_name: str | None,
    asset_type: object | None = None,
):
    """
    Convert an internal LumiBot symbol to a broker-native class-share symbol format.

    Non class-share symbols are returned unchanged (uppercased/trimmed when string).
    """
    canonical = normalize_symbol_for_internal(symbol, asset_type=asset_type)
    if not isinstance(canonical, str):
        return canonical

    if not _asset_type_supports_class_share_normalization(asset_type):
        return canonical

    broker_key = (broker_name or "").strip().lower()
    separator = _BROKER_CLASS_SHARE_SEPARATORS.get(broker_key, INTERNAL_CLASS_SHARE_SEPARATOR)

    parsed = _parse_class_share_symbol(canonical)
    if not parsed:
        return canonical

    root, suffix = parsed
    return f"{root}{separator}{suffix}"
