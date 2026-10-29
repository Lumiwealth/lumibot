"""Phemex compatibility for documented metadata and bounded public history."""


def normalize_market(api, market):
    info = market.get("info") or {}
    if (getattr(api, "id", None) == "phemex" and market.get("contractSize") is None
            and info.get("type") == "PerpetualV2"
            and info.get("contractUnderlyingAssets") == market.get("base")):
        # V2 quantities are base-coin units. CCXT 4.5.64 supplies this for USDT
        # but leaves USDC missing even though both use the same quantity model.
        market["contractSize"] = 1.0
    return market


def normalize_markets(api):
    if getattr(api, "id", None) == "phemex":
        for market in api.markets.values():
            normalize_market(api, market)


def fetch_ohlcv(api, symbol, timeframe="1m", since=None, limit=None, params=None):
    params = dict(params or {})
    if getattr(api, "id", None) != "phemex" or since is None:
        return api.fetch_ohlcv(symbol, timeframe, since=since, limit=limit, params=params)
    market = normalize_market(api, api.market(symbol))
    duration = api.parse_timeframe(timeframe)
    until = params.pop("until", None)
    end = int(until // 1000) if until is not None else min(api.seconds(), int(since // 1000) + duration*(limit or 1000))
    request = {"symbol": market["id"], "resolution": api.timeframes.get(timeframe, timeframe),
               "from": int(since // 1000), "to": end, "limit": limit or 1000, **params}
    # The legacy CCXT branch discards `since` for spot/inverse and returns the
    # latest candles. Phemex's list endpoint preserves explicit from/to for all
    # families; keep CCXT's native price/volume-scale parser.
    response = api.publicGetMdV2KlineList(request)
    return api.parse_ohlcvs(response["data"]["rows"], market, timeframe, since, limit)
