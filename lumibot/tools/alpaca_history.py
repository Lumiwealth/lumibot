"""Alpaca bars through the shared, broker-agnostic bar cache.

Live analysis (charts) and AlpacaBacktesting read the same series files through
:mod:`lumibot.tools.shared_bar_cache`. This module only maps an Alpaca SDK bars
request to cache series and back to the SDK ``BarSet.df`` shape.
"""

from __future__ import annotations

from types import SimpleNamespace

import pandas as pd

from lumibot.data_sources.exceptions import InvalidBars
from lumibot.tools import shared_bar_cache


def _value(value):
    return getattr(value, "value", value)


def _frame(value, symbols):
    frame = value.df.copy()
    if frame.empty:
        return frame
    if not isinstance(frame.index, pd.MultiIndex):
        if len(symbols) != 1:
            raise InvalidBars("Alpaca multi-symbol bars require a symbol/timestamp index")
        frame = pd.concat([frame], keys=symbols, names=["symbol", "timestamp"])
    return frame


def _timestep(timeframe) -> str:
    unit = str(_value(getattr(timeframe, "unit", "Day"))).lower()
    return shared_bar_cache.normalize_timestep(getattr(timeframe, "amount", 1), unit)


def _empty():
    return pd.DataFrame(
        columns=["open", "high", "low", "close", "volume"],
        index=pd.MultiIndex.from_arrays([[], []], names=["symbol", "timestamp"]),
    )


def _request_asset_class(request) -> str | None:
    name = type(request).__name__.lower()
    for word in ("option", "crypto", "stock"):
        if word in name:
            return word
    return None


def _cache_identity(client) -> str | None:
    """Stable account identity attached to an SDK data client, or None when unknown.

    ``client._lumibot_cache_identity`` is either a string or a zero-argument callable that
    returns one (resolved lazily, at most once, by the code that created the client).
    """
    resolver = getattr(client, "_lumibot_cache_identity", None)
    try:
        value = resolver() if callable(resolver) else resolver
    except Exception:  # pragma: no cover - a resolver must never break a history read
        return None
    return value if isinstance(value, str) and value else None


def fetch_alpaca_bars(client, request, method, *, now=None, refresh=False, empty_ok=None):
    """Run an Alpaca SDK bars read through the shared bar cache (see ``_fetch_alpaca_bars``)."""
    from lumibot.tools import backtest_telemetry

    with backtest_telemetry.provider_scope("alpaca", _request_asset_class(request)):
        return _fetch_alpaca_bars(client, request, method, now=now, refresh=refresh, empty_ok=empty_ok)


def _fetch_alpaca_bars(client, request, method, *, now=None, refresh=False, empty_ok=None):
    """Run an Alpaca SDK bars read through the shared bar cache.

    Returns an object with the ``BarSet.df`` contract LumiBot adapters consume.
    Provider exceptions propagate; invalid provider bars raise ``InvalidBars``.
    Explicit limits, sort orders and unknown credential scopes bypass reuse
    because they cannot establish complete coverage.

    ``empty_ok(symbol)`` True lets a zero-row answer for that symbol be remembered as
    confirmed empty (see ``shared_bar_cache``). Pass it only for symbols Alpaca itself
    listed: ``method`` raises on HTTP errors and rate limits and the SDK follows every page
    token, so an answer without the symbol is a complete "no bars" for a real contract.
    """
    raw_symbols = request.symbol_or_symbols
    symbols = [raw_symbols] if isinstance(raw_symbols, str) else list(raw_symbols)
    api_key = getattr(client, "_api_key", None)
    oauth_token = getattr(client, "_oauth_token", None)
    credential = oauth_token if isinstance(oauth_token, str) and oauth_token else api_key
    if credential is oauth_token and credential:
        # An OAuth access token is refreshed over time but still belongs to one account.
        # Scoping the cache by the token itself orphaned every cached bar at each refresh,
        # so prefer the stable account identity when the owner of the client knows it.
        account_identity = _cache_identity(client)
        if account_identity:
            credential = account_identity
    if (
        not isinstance(credential, str)
        or not credential
        or request.start is None
        or request.end is None
        or getattr(request, "limit", None) is not None
        or getattr(request, "sort", None) is not None
    ):
        return method(request)

    adjustment = _value(getattr(request, "adjustment", None))
    series = {
        symbol: shared_bar_cache.BarSeries.make(
            "alpaca",
            symbol,
            _timestep(request.timeframe),
            # Default feed/entitlement can differ across credentials. Reuse between
            # chart and backtest with the same connection, never infer entitlements.
            scope=shared_bar_cache.credential_scope("alpaca", credential),
            adjusted=adjustment not in (None, "raw"),
            requestType=type(request).__name__,
            timeframe=str(request.timeframe),
            feed=_value(getattr(request, "feed", None)),
            adjustment=adjustment,
            asof=getattr(request, "asof", None),
            currency=_value(getattr(request, "currency", None)),
        )
        for symbol in symbols
    }
    by_series = {item: symbol for symbol, item in series.items()}

    def fetch(members, start, stop):
        wanted = [by_series[item] for item in members]
        params = request.model_copy(
            update={"symbol_or_symbols": wanted, "start": start.to_pydatetime(), "end": stop.to_pydatetime()}
        )
        frame = _frame(method(params), wanted)
        from lumibot.tools import backtest_telemetry

        backtest_telemetry.record_provider_rows(len(frame))
        out = {}
        for item in members:
            symbol = by_series[item]
            if not frame.empty and symbol in frame.index.get_level_values(0):
                out[item] = frame.xs(symbol, level=0)
            else:
                out[item] = None
        return out

    try:
        results = shared_bar_cache.read_bars_many(
            list(series.values()), request.start, request.end, fetch, now=now, refresh=refresh,
            strict_ohlcv=True,  # Alpaca's existing contract: invalid provider OHLCV is an explicit error
            confirmed_empty=(lambda item: bool(empty_ok(by_series[item]))) if empty_ok is not None else None,
        )
    except InvalidBars as exc:
        raise InvalidBars("Alpaca returned invalid OHLCV bars; shared cache was not written") from exc
    pieces = [
        pd.concat([results[series[symbol]]], keys=[symbol], names=["symbol", "timestamp"])
        for symbol in symbols
        if not results[series[symbol]].empty
    ]
    result = pd.concat(pieces).sort_index() if pieces else _empty()
    stats = next(iter(results.values())).attrs.get("historyCache", {}) if results else {}
    result.attrs["historyCache"] = dict(stats)
    return SimpleNamespace(df=result)
