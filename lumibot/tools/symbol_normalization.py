import re
from typing import Optional


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


def _is_nan(value) -> bool:
    try:
        # NaN is the only value that is not equal to itself.
        return value != value
    except Exception:
        return False


def _normalize_asset_type(asset_type: Optional[object]) -> Optional[str]:
    if asset_type is None:
        return None
    return str(asset_type).strip().lower()


def _asset_type_supports_class_share_normalization(asset_type: Optional[object]) -> bool:
    normalized = _normalize_asset_type(asset_type)
    if normalized is None:
        return True
    return normalized in {"stock", "option", "index"}


def _normalize_symbol_text(symbol: object):
    if symbol is None or _is_nan(symbol):
        return symbol
    return str(symbol).strip().upper()


def _parse_class_share_symbol(symbol: object):
    sym = _normalize_symbol_text(symbol)
    if not isinstance(sym, str) or not sym:
        return None

    for pattern in _CLASS_SHARE_PATTERNS.values():
        match = pattern.match(sym)
        if match:
            return match.group("root"), match.group("suffix")
    return None


def normalize_symbol_for_internal(symbol: object, asset_type: Optional[object] = None):
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
    broker_name: Optional[str],
    asset_type: Optional[object] = None,
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



# Common quote currencies appended to concatenated crypto pair strings such as BTCUSD.
_CRYPTO_QUOTE_SUFFIXES = (
    "USDT",
    "USDC",
    "USD",
    "EUR",
    "GBP",
    "BTC",
    "ETH",
    "BUSD",
    "DAI",
)


def parse_crypto_pair_symbol(symbol: object) -> tuple[Optional[str], Optional[str]]:
    """
    Parse a crypto base or pair string into (base, quote).

    Accepts common exchange/UI forms:
    - BTC/USD, BTC-USD, BTC:USD
    - BTCUSD (concatenated with a known quote suffix)
    - BTC (base only; quote is None)
    """
    sym = _normalize_symbol_text(symbol)
    if not isinstance(sym, str) or not sym:
        return None, None

    parts = [part for part in re.split(r"[/:\-]", sym) if part]
    if len(parts) >= 2:
        return parts[0], parts[1]

    for suffix in _CRYPTO_QUOTE_SUFFIXES:
        if sym.endswith(suffix) and len(sym) > len(suffix):
            base = sym[: -len(suffix)]
            if base:
                return base, suffix
    return sym, None


def build_ccxt_crypto_symbol(
    base_or_pair: object,
    quote: Optional[object] = None,
    *,
    exchange_id: Optional[str] = None,
) -> Optional[str]:
    """
    Build a CCXT unified market symbol (BASE/QUOTE) for crypto history and orders.

    Coinbase's native product id is ``BTC-USD``, but CCXT's unified symbol is
    ``BTC/USD``. Pair strings accidentally used as the crypto base asset
    (``Asset("BTC-USD")`` + quote ``USD``) otherwise become the nonexistent
    ``BTC-USD/USD`` market and silently yield empty history / zero-trade
    backtests.

    The ``exchange_id`` argument is reserved for exchange-specific quote
    preferences and is currently unused beyond documentation.
    """
    del exchange_id  # Reserved for future exchange-specific quote remaps.

    base_sym = _normalize_symbol_text(base_or_pair)
    quote_sym = _normalize_symbol_text(quote) if quote is not None else None
    if not isinstance(base_sym, str) or not base_sym:
        return None

    parsed_base, parsed_quote = parse_crypto_pair_symbol(base_sym)
    if not parsed_base:
        return None

    explicit_quote = quote_sym if isinstance(quote_sym, str) and quote_sym else None
    if parsed_quote and explicit_quote and parsed_quote == explicit_quote:
        # Asset("BTC-USD") + quote USD -> BTC/USD (not BTC-USD/USD).
        resolved_quote = parsed_quote
    elif parsed_quote and explicit_quote and parsed_quote != explicit_quote:
        # Pair already encodes a quote; prefer the pair's quote over a second
        # appended quote that would create BASE-QUOTE/OTHER nonsense.
        resolved_quote = parsed_quote
    elif parsed_quote:
        resolved_quote = parsed_quote
    else:
        resolved_quote = explicit_quote

    if not resolved_quote:
        return parsed_base

    return f"{parsed_base}/{resolved_quote}"


def crypto_ccxt_symbol_candidates(
    symbol: object,
    *,
    exchange_id: Optional[str] = None,
) -> list[str]:
    """
    Return unique CCXT symbol candidates to try when resolving a market.

    Starts with the normalized BASE/QUOTE form, then includes a few common
    aliases so Coinbase-style hyphen ids and redundant pair/quote forms can
    still resolve.
    """
    raw = _normalize_symbol_text(symbol)
    candidates: list[str] = []

    def _add(value: Optional[str]) -> None:
        if isinstance(value, str) and value and value not in candidates:
            candidates.append(value)

    if isinstance(raw, str) and raw:
        _add(raw)
        # Hyphen native product ids (BTC-USD) -> CCXT unified (BTC/USD).
        if "-" in raw and "/" not in raw:
            _add(raw.replace("-", "/"))
        # Redundant pair/quote (BTC-USD/USD) -> BTC/USD.
        if "/" in raw:
            left, right = raw.split("/", 1)
            left_base, left_quote = parse_crypto_pair_symbol(left)
            if left_base and left_quote and left_quote == right:
                _add(f"{left_base}/{left_quote}")

    normalized = build_ccxt_crypto_symbol(raw, exchange_id=exchange_id) if isinstance(raw, str) else None
    _add(normalized)

    # Prefer USD form on Coinbase when a USDT request is made and callers want
    # exchange-native USD pairs; keep USDT first so intentional USDT stays
    # authoritative when the market exists.
    if isinstance(normalized, str) and normalized.endswith("/USDT") and (exchange_id or "").strip().lower() in {
        "coinbase",
        "coinbaseexchange",
        "coinbaseadvanced",
        "coinbasepro",
    }:
        _add(normalized[: -len("USDT")] + "USD")

    return candidates


def resolve_ccxt_market_symbol(markets: Optional[dict], symbol: object, *, exchange_id: Optional[str] = None) -> Optional[str]:
    """
    Resolve ``symbol`` to a key present in ``markets``, trying crypto aliases.

    Returns the resolved CCXT unified symbol, or None when no candidate exists
    in ``markets``.
    """
    market_map = markets if isinstance(markets, dict) else {}
    for candidate in crypto_ccxt_symbol_candidates(symbol, exchange_id=exchange_id):
        if candidate in market_map:
            return candidate
    return None
