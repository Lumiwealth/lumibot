#!/usr/bin/env python3
"""Write a facts file for every exchange in the installed CCXT package.

Why: the per-exchange documentation pages (``docsrc/brokers.ccxt.<id>.rst``)
must only state facts that come from CCXT's own exchange metadata. This script
reads ``describe()`` for each exchange, offline, and writes one JSON file the
page generator consumes. Nothing here is invented or hand-typed.

No network calls, no credentials, no orders. Constructing a CCXT exchange
object does not contact the exchange.

Usage::

    python scripts/generate_ccxt_exchange_facts.py \
        --out docsrc/_data/ccxt_exchange_facts.json

Notes and invariants:

* ``countries`` is CCXT's own label and can be out of date. Pages must call it
  "listed by CCXT", never "headquarters" or "legal home".
* ``urls.referral`` is recorded only as a yes/no flag (``has_referral_url``).
  The referral URL itself is never written, so a page cannot accidentally use
  an affiliate link without approval.
* ``has`` values can be True, False, None (unknown) or "emulated". They are
  copied as-is; None is not the same as False.
"""


import argparse
import json
import sys
from datetime import datetime, timezone

import ccxt

try:  # CCXT Pro ships inside the ccxt package
    import ccxt.pro as ccxtpro
except Exception:  # pragma: no cover - older CCXT builds
    ccxtpro = None

MARKET_TYPES = ("spot", "margin", "swap", "future", "option")
TRADING_METHODS = (
    "createOrder",
    "createMarketOrder",
    "createLimitOrder",
    "createStopOrder",
    "createTriggerOrder",
    "createStopLossOrder",
    "createTakeProfitOrder",
    "createReduceOnlyOrder",
    "cancelOrder",
    "editOrder",
    "fetchBalance",
    "fetchPositions",
    "fetchOpenOrders",
    "fetchMyTrades",
    "fetchOHLCV",
    "fetchTicker",
    "fetchOrderBook",
    "fetchTradingFees",
    "fetchFundingRate",
    "setLeverage",
    "setMarginMode",
    "setPositionMode",
)
STREAM_METHODS = ("watchTicker", "watchOrderBook", "watchTrades", "watchOHLCV", "watchOrders", "watchBalance", "watchPositions")


def _first_url(value):
    """CCXT stores some URLs as str, list or nested dict. Return one string or None."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, (list, tuple)):
        for item in value:
            url = _first_url(item)
            if url:
                return url
        return None
    if isinstance(value, dict):
        for item in value.values():
            url = _first_url(item)
            if url:
                return url
    return None


def _unique(urls):
    seen, out = set(), []
    for url in urls:
        if url and url not in seen:
            seen.add(url)
            out.append(url)
    return out


def _all_urls(value):
    return _unique(_collect_urls(value))


def _collect_urls(value):
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, (list, tuple)):
        out = []
        for item in value:
            out.extend(_collect_urls(item))
        return out
    if isinstance(value, dict):
        out = []
        for item in value.values():
            out.extend(_collect_urls(item))
        return out
    return []


def _clean(value):
    """Make CCXT values JSON-safe and stable."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    return str(value)


def exchange_facts(exchange_id: str) -> dict:
    exchange = getattr(ccxt, exchange_id)()
    desc = exchange.describe()
    has = desc.get("has", {}) or {}
    urls = desc.get("urls", {}) or {}
    hostname = desc.get("hostname")

    def expand(url):
        if url and hostname and "{hostname}" in url:
            return url.replace("{hostname}", hostname)
        return url

    pro_has = {}
    if ccxtpro is not None and hasattr(ccxtpro, exchange_id):
        try:
            pro_has = getattr(ccxtpro, exchange_id)().describe().get("has", {}) or {}
        except Exception:
            pro_has = {}

    required = desc.get("requiredCredentials", {}) or {}
    test_urls = _unique(expand(u) for u in _all_urls(urls.get("test")))
    fees = desc.get("fees", {}) or {}
    trading_fees = fees.get("trading", {}) if isinstance(fees, dict) else {}

    return {
        "id": exchange_id,
        "name": desc.get("name") or exchange_id,
        "countries_listed_by_ccxt": list(desc.get("countries") or []),
        "certified": bool(desc.get("certified")),
        "pro": bool(desc.get("pro")),
        "dex": bool(desc.get("dex")),
        "alias": bool(desc.get("alias")),
        "api_version": _clean(desc.get("version")),
        "rate_limit_ms": _clean(desc.get("rateLimit")),
        "urls": {
            "www": _first_url(urls.get("www")),
            "doc": _all_urls(urls.get("doc")),
            "fees": _all_urls(urls.get("fees")),
            "api_management": _first_url(urls.get("api_management")),
            "api_base": expand(_first_url(urls.get("api"))),
            "test": test_urls,
        },
        "has_referral_url": bool(urls.get("referral")),
        "sandbox": bool(has.get("sandbox")) or bool(test_urls),
        "markets": {t: _clean(has.get(t)) for t in MARKET_TYPES},
        "methods": {m: _clean(has.get(m)) for m in TRADING_METHODS},
        "streams": {m: _clean(pro_has.get(m)) for m in STREAM_METHODS},
        "required_credentials": sorted(k for k, v in required.items() if v),
        "default_trading_fee": {
            "maker": _clean(trading_fees.get("maker")) if isinstance(trading_fees, dict) else None,
            "taker": _clean(trading_fees.get("taker")) if isinstance(trading_fees, dict) else None,
            "note": "CCXT default schedule; the exchange's fee page is the source of truth",
        },
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", default="docsrc/_data/ccxt_exchange_facts.json")
    parser.add_argument("--only", nargs="*", help="Limit to these exchange ids")
    args = parser.parse_args(argv)

    ids = args.only or list(ccxt.exchanges)
    facts, errors = [], {}
    for exchange_id in ids:
        try:
            facts.append(exchange_facts(exchange_id))
        except Exception as error:  # record, never hide
            errors[exchange_id] = f"{type(error).__name__}: {error}"

    payload = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "ccxt_version": ccxt.__version__,
        "exchange_count": len(facts),
        "errors": errors,
        "exchanges": facts,
    }
    with open(args.out, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=1, sort_keys=False)
        handle.write("\n")
    print(f"ccxt {ccxt.__version__}: wrote {len(facts)} exchanges to {args.out}; errors: {len(errors)}")
    return 0 if not errors else 1


if __name__ == "__main__":
    sys.exit(main())
