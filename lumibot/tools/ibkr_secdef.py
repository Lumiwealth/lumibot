from __future__ import annotations


IBKR_US_FUTURES_EXCHANGES = {"CME", "CBOT", "COMEX", "NYMEX"}


class IbkrFuturesExchangeAmbiguousError(RuntimeError):
    """Raised when IBKR secdef search returns multiple equally-good FUT exchanges."""


def select_futures_exchange_from_secdef_search_payload(
    symbol: str,
    payload: Any,
    *,
    prefer_currency: str = "USD",
    prefer_exchanges: Optional[set[str]] = None,
) -> str:
    """Select the best FUT exchange from an IBKR `iserver/secdef/search` response.

    Tie-break rules (highest priority first):
    - prefer currency == USD (if present)
    - prefer exchange in {CME, CBOT, COMEX, NYMEX}
    - otherwise choose the first FUT exchange

    If multiple distinct exchanges tie for best score, raises IbkrFuturesExchangeAmbiguousError.
    """
    symbol_upper = str(symbol or "").strip().upper()
    prefer_currency = str(prefer_currency or "").strip().upper()
    prefer_exchanges = prefer_exchanges or set(IBKR_US_FUTURES_EXCHANGES)

    if not isinstance(payload, list) or not payload:
        raise RuntimeError(f"IBKR secdef/search returned no results for FUT symbol={symbol_upper!r}")

    candidates: list[tuple[str, str]] = []  # (exchange, currency)
    for entry in payload:
        if not isinstance(entry, dict):
            continue
        entry_currency = str(entry.get("currency") or "").strip().upper()
        sections = entry.get("sections") or []
        if not isinstance(sections, list):
            continue
        for section in sections:
            if not isinstance(section, dict):
                continue
            if str(section.get("secType") or "").strip().upper() != "FUT":
                continue
            exch = str(section.get("exchange") or "").strip().upper()
            if not exch:
                continue
            section_currency = str(section.get("currency") or "").strip().upper()
            currency = section_currency or entry_currency
            candidates.append((exch, currency))

    if not candidates:
        raise RuntimeError(f"IBKR secdef/search returned no FUT sections for symbol={symbol_upper!r}")

    best_score = -1
    best: set[str] = set()
    for exch, currency in candidates:
        score = 0
        if prefer_currency and currency == prefer_currency:
            score += 10
        if exch in prefer_exchanges:
            score += 5
        if score > best_score:
            best_score = score
            best = {exch}
        elif score == best_score:
            best.add(exch)

    if len(best) != 1:
        raise IbkrFuturesExchangeAmbiguousError(
            f"Ambiguous IBKR FUT exchange for {symbol_upper}: {sorted(best)}. "
            "Pass exchange=... explicitly."
        )
    return next(iter(best))
