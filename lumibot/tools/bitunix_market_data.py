"""Bitunix public futures market data for perpetual backtests.

Reads only Bitunix's public, keyless market endpoints:

* ``/api/v1/futures/market/trading_pairs`` for contract terms,
* ``/api/v1/futures/market/kline`` for perpetual candles (traded or mark price),
* ``/api/v1/futures/market/get_funding_rate_history`` for settled funding.

Docs: https://www.bitunix.com/api-docs/futures/market/get_kline.html and
https://www.bitunix.com/api-docs/futures/market/get_funding_rate_history.html

Nothing here fills, forward-fills or invents a candle or a funding payment. A
missing answer stays missing, and an unsupported contract raises a clear error.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from decimal import Decimal

import pandas as pd

logger = logging.getLogger(__name__)

BASE_URL = "https://fapi.bitunix.com/api/v1/futures/market"
PAGE_LIMIT = 200  # documented maximum for kline and funding history
_INTERVALS = {"minute": ("1m", pd.Timedelta(minutes=1)), "hour": ("1h", pd.Timedelta(hours=1)),
              "day": ("1d", pd.Timedelta(days=1))}
_LINEAR_QUOTES = ("USDT", "USDC")


class BitunixMarketDataError(ValueError):
    """Bitunix public market data cannot answer this backtest request.

    ``reason`` is a stable machine-readable code (see ``PERPETUAL_DATA_REASONS`` in
    ``lumibot.backtesting.routed_backtesting``).
    """

    def __init__(self, message: str, reason: str = "source_error"):
        super().__init__(message)
        self.reason = reason


def _ms(value) -> int:
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is None:
        stamp = stamp.tz_localize("UTC")
    return int(stamp.tz_convert("UTC").timestamp() * 1000)


def resolve_symbol(asset, quote=None) -> str:
    """Bitunix symbol for a crypto perpetual asset, e.g. ``BTCUSDT``.

    ``Asset("BTCUSDT", "crypto_future")`` is used as written. A base symbol such as
    ``Asset("BTC", "crypto_future")`` needs an explicit USDT or USDC quote; the
    default USD quote would mean a different (coin-margined) contract, so it is
    never guessed.
    """
    symbol = str(getattr(asset, "symbol", asset) or "").strip().upper()
    # "BTC/USDT:USDT" (CCXT style) -> "BTCUSDT"; "BTC-USDT" -> "BTCUSDT".
    symbol = symbol.split(":")[0].replace("/", "").replace("-", "").replace("_", "")
    if symbol.endswith(_LINEAR_QUOTES) or symbol.endswith("USD"):
        return symbol
    quote_symbol = str(getattr(quote, "symbol", quote) or "").strip().upper()
    if quote_symbol in _LINEAR_QUOTES:
        return symbol + quote_symbol
    raise BitunixMarketDataError(
        f"Cannot tell which Bitunix perpetual {symbol!r} means. Name the contract in the asset, "
        f"for example Asset('{symbol}USDT', asset_type='crypto_future').",
        reason="ambiguous_symbol",
    )


class BitunixMarketData:
    """Small client for Bitunix's public futures market endpoints."""

    def __init__(self, session=None, base_url: str = BASE_URL, pause_seconds: float = 0.11):
        if session is None:
            import requests

            session = requests.Session()
            session.headers.update({"User-Agent": "lumibot-backtesting"})
        self._session = session
        self._base_url = base_url.rstrip("/")
        self._pause = pause_seconds  # documented rate limit: 10 requests per second per IP
        self._pairs: dict[str, dict] = {}
        self._funding: dict[str, list[dict]] = {}
        self._candles: dict[tuple, pd.DataFrame] = {}

    # -- transport -------------------------------------------------------------------
    def _get(self, path: str, params: dict) -> list | dict:
        url = f"{self._base_url}/{path}"
        last_error = None
        for attempt in range(4):
            try:
                response = self._session.get(url, params=params, timeout=30)
                if response.status_code == 429 or response.status_code >= 500:
                    last_error = f"HTTP {response.status_code}"
                    time.sleep(1 + attempt)
                    continue
                if response.status_code != 200:
                    raise BitunixMarketDataError(
                        f"Bitunix market data {path} answered HTTP {response.status_code}; "
                        "perpetual history is unavailable from this location.",
                        reason="source_unreachable",
                    )
                body = response.json()
            except BitunixMarketDataError:
                raise
            except Exception as exc:  # network errors: retry, then report plainly
                last_error = f"{type(exc).__name__}: {exc}"
                time.sleep(1 + attempt)
                continue
            if str(body.get("code")) != "0":
                raise BitunixMarketDataError(f"Bitunix market data {path} returned code {body.get('code')}: "
                                             f"{body.get('msg')}", reason="source_error")
            if self._pause:
                time.sleep(self._pause)
            return body.get("data")
        raise BitunixMarketDataError(f"Bitunix market data {path} did not answer ({last_error}).",
                                     reason="source_unreachable")

    # -- contract terms --------------------------------------------------------------
    def trading_pair(self, symbol: str) -> dict:
        if symbol not in self._pairs:
            data = self._get("trading_pairs", {"symbols": symbol}) or []
            match = next((row for row in data if row.get("symbol") == symbol), None)
            if match is None:
                raise BitunixMarketDataError(f"Bitunix does not list a perpetual named {symbol}.",
                                             reason="contract_not_listed")
            self._pairs[symbol] = match
        return self._pairs[symbol]

    def contract_spec(self, symbol: str):
        """Contract terms for a USDT- or USDC-margined Bitunix perpetual.

        Quantities are in the base coin, as with live Bitunix orders. Coin-margined
        (inverse) contracts such as ``BTCUSD`` are not modeled: Bitunix's public
        metadata does not state their contract face value or margin coin, so a
        backtest would have to guess the payoff.
        """
        from lumibot.entities import ContractSpec

        pair = self.trading_pair(symbol)
        quote = str(pair.get("quote") or "").upper()
        if quote not in _LINEAR_QUOTES:
            raise BitunixMarketDataError(
                f"{symbol} is a Bitunix coin-margined (inverse) perpetual. Backtests of coin-margined "
                "perpetuals are not supported yet because Bitunix does not publish their contract size "
                f"and margin coin. Use the USDT-margined contract instead, for example {pair.get('base')}USDT.",
                reason="coin_margined_unsupported",
            )
        settle = str(pair.get("marginCoin") or quote).upper()
        return ContractSpec(pair["base"], quote, settle, quantity_unit="base")

    # -- candles -----------------------------------------------------------------------
    def candles(self, symbol: str, timestep: str, start, end, price_type: str = "LAST_PRICE") -> pd.DataFrame:
        """Closed candles whose open time is in ``[start, end)``, indexed by UTC open time."""
        interval, step = _INTERVALS[timestep]
        start_ms, end_ms = _ms(start), _ms(end)
        memo_key = (symbol, timestep, start_ms, end_ms, price_type)
        if memo_key in self._candles:
            # The same series is often asked for under several quote spellings in one run;
            # download it once per process.
            return self._candles[memo_key].copy()
        rows: dict[int, dict] = {}
        window_ms = PAGE_LIMIT * int(step.total_seconds() * 1000)
        cursor = end_ms
        while cursor > start_ms:
            params = {"symbol": symbol, "interval": interval, "endTime": cursor,
                      "limit": PAGE_LIMIT, "type": price_type}
            page = self._get("kline", params) or []
            if not page:
                # Bitunix occasionally answers an empty page mid-history; ask once more.
                logger.info("Bitunix %s %s candles: empty page before %s; retrying once", symbol, interval, cursor)
                time.sleep(1 if self._pause else 0)
                page = self._get("kline", params) or []
            times = [int(item["time"]) for item in page]
            for item, opened in zip(page, times):
                if start_ms <= opened < end_ms:
                    rows[opened] = item
            # Short or empty pages are not the end of history (a short page can come mid-history,
            # an empty one covers a gap or the time before listing). Keep stepping back one
            # window at a time until the requested start, so a gap never truncates the series.
            cursor = min(times) if times and min(times) < cursor else cursor - window_ms
        if not rows:
            return pd.DataFrame(columns=["open", "high", "low", "close", "volume"],
                                index=pd.DatetimeIndex([], tz="UTC"))
        frame = pd.DataFrame.from_dict(rows, orient="index").sort_index()
        frame.index = pd.to_datetime(frame.index, unit="ms", utc=True)
        frame = frame.rename(columns={"baseVol": "volume"})
        frame = frame[["open", "high", "low", "close", "volume"]].astype(float)
        self._candles[memo_key] = frame
        return frame.copy()

    # -- funding -------------------------------------------------------------------------
    def funding_history(self, symbol: str, start, end) -> list[dict]:
        """Settled funding events in ``[start, end]`` with Bitunix's mark price at settlement."""
        start_ms, end_ms = _ms(start), _ms(end)
        key = f"{symbol}:{start_ms}:{end_ms}"
        if key not in self._funding:
            seen: dict[int, dict] = {}
            cursor = end_ms
            while cursor >= start_ms:
                page = self._get("get_funding_rate_history",
                                 {"symbol": symbol, "startTime": start_ms, "endTime": cursor,
                                  "limit": PAGE_LIMIT}) or []
                times = [int(row["fundingTime"]) for row in page]
                for row, settled in zip(page, times):
                    if start_ms <= settled <= end_ms:
                        seen[settled] = {"timestamp": settled, "fundingRate": Decimal(str(row["fundingRate"])),
                                         "markPrice": Decimal(str(row["markPrice"])) if row.get("markPrice") else None}
                if len(page) < PAGE_LIMIT or not times or min(times) >= cursor:
                    break
                cursor = min(times) - 1
            self._funding[key] = [seen[t] for t in sorted(seen)]
        return list(self._funding[key])


def shared_series(symbol: str, timestep: str, price_type: str = "LAST_PRICE"):
    """Shared bar cache identity for Bitunix public perpetual candles."""
    from lumibot.tools import shared_bar_cache

    return shared_bar_cache.BarSeries.make(
        "bitunix", symbol, shared_bar_cache.normalize_timestep(1, timestep),
        scope=shared_bar_cache.credential_scope("bitunix", "public"),
        adjusted=False, instrument="perpetual", price_type=price_type, market_data="public",
    )


def cached_candles(client: BitunixMarketData, symbol: str, timestep: str, start, end,
                   price_type: str = "LAST_PRICE", now=None) -> pd.DataFrame:
    """Candles through the shared bar cache when it is enabled (local files plus S3)."""
    from lumibot.tools import shared_bar_cache

    start_ts = pd.Timestamp(start).tz_convert("UTC") if pd.Timestamp(start).tzinfo else pd.Timestamp(start, tz="UTC")
    end_ts = pd.Timestamp(end).tz_convert("UTC") if pd.Timestamp(end).tzinfo else pd.Timestamp(end, tz="UTC")
    if not shared_bar_cache.enabled():
        return client.candles(symbol, timestep, start_ts, end_ts, price_type)

    def fetch(gap_start, gap_stop):
        frame = client.candles(symbol, timestep, gap_start, gap_stop, price_type)
        return None if frame.empty else frame

    rows = shared_bar_cache.read_bars(
        shared_series(symbol, timestep, price_type), start_ts, end_ts, fetch,
        now=pd.Timestamp(now or datetime.now(timezone.utc)), coverage_from_rows=True,
    )
    rows = rows[["open", "high", "low", "close", "volume"]].astype(float)
    rows.index = pd.DatetimeIndex(rows.index).tz_convert("UTC")
    return rows[(rows.index >= start_ts) & (rows.index < end_ts)]
