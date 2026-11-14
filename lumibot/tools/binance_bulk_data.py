"""Binance public bulk market-data files for crypto backtests (spot and USD-M perpetuals).

Reads only the public, keyless file archive at ``https://data.binance.vision``:

* ``spot`` klines (``data/spot/{monthly,daily}/klines/<SYMBOL>/<interval>/...``),
* USD-M futures klines, mark-price klines and funding rates
  (``data/futures/um/{monthly,daily}/{klines,markPriceKlines,fundingRate}/...``).

Every zip is checked against its published ``.CHECKSUM`` (SHA-256) before it is read.
Complete months come from monthly files; days not yet covered by a monthly file come
from daily files. Binance publishes daily files about one day late and funding only
monthly, so the newest data is reported as unavailable, never filled or invented.

The live Binance REST API is not used: it refuses requests from some cloud regions.
"""

from __future__ import annotations

import hashlib
import io
import logging
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from decimal import Decimal

import pandas as pd

logger = logging.getLogger(__name__)

BASE_URL = "https://data.binance.vision/data"
MARKETS = {"spot": "spot", "um": "futures/um"}
_INTERVALS = {"minute": ("1m", pd.Timedelta(minutes=1)), "hour": ("1h", pd.Timedelta(hours=1)),
              "day": ("1d", pd.Timedelta(days=1))}
LINEAR_QUOTES = ("USDT", "USDC", "FDUSD")
_KLINE_COLUMNS = ["open_time", "open", "high", "low", "close", "volume"]


class BinanceBulkDataError(ValueError):
    """Binance bulk files cannot answer this request; ``reason`` is a stable code
    from ``PERPETUAL_DATA_REASONS`` in ``lumibot.backtesting.routed_backtesting``."""

    def __init__(self, message: str, reason: str = "source_error"):
        super().__init__(message)
        self.reason = reason


def _utc(value) -> pd.Timestamp:
    stamp = pd.Timestamp(value)
    return stamp.tz_localize("UTC") if stamp.tzinfo is None else stamp.tz_convert("UTC")


def _epoch_to_utc(values: pd.Series) -> pd.DatetimeIndex:
    """Binance spot files switched to microseconds in 2025; futures files use milliseconds."""
    numbers = pd.to_numeric(values).astype("int64")
    micro = numbers > 10**14
    millis = numbers.where(~micro, numbers // 1000)
    return pd.DatetimeIndex(pd.to_datetime(millis, unit="ms", utc=True))


def perpetual_symbol(asset, quote=None) -> str:
    """Binance USD-M perpetual symbol, e.g. ``BTCUSDT`` or ``1000PEPEUSDT`` (used as written).

    A base coin alone needs an explicit USDT or USDC quote; a USD quote would mean a
    coin-margined contract, which is not supported. Dated contracts are not perpetuals.
    """
    raw = str(getattr(asset, "symbol", asset) or "").strip().upper()
    symbol = raw.split(":")[0].replace("/", "").replace("-", "")
    if symbol.endswith("_PERP") or (symbol.endswith("USD") and not symbol.endswith(LINEAR_QUOTES)):
        raise BinanceBulkDataError(
            f"{raw} is a coin-margined (inverse) perpetual. Backtests of coin-margined perpetuals are not "
            f"supported; use the USDT-margined contract, for example {symbol.split('USD')[0]}USDT.",
            reason="coin_margined_unsupported",
        )
    if "_" in symbol:
        raise BinanceBulkDataError(f"{raw} is a dated futures contract, not a perpetual.",
                                   reason="contract_not_listed")
    if symbol.endswith(("USDT", "USDC")):
        return symbol
    quote_symbol = str(getattr(quote, "symbol", quote) or "").strip().upper()
    if quote_symbol in ("USDT", "USDC"):
        return symbol + quote_symbol
    raise BinanceBulkDataError(
        f"Cannot tell which Binance perpetual {symbol!r} means. Name the contract in the asset, "
        f"for example Asset('{symbol}USDT', asset_type='crypto_future').",
        reason="ambiguous_symbol",
    )


FIAT_QUOTES = ("USD", "EUR", "GBP")


def spot_symbol(asset, quote=None) -> str:
    """Binance spot pair for the exact quote asked for, e.g. ``BTCUSDT`` for BTC/USDT.

    Binance spot has no USD pairs, and USDT trades slightly below USD, so a USD request is
    refused rather than served from the USDT pair.
    """
    base = str(getattr(asset, "symbol", asset) or "").strip().upper()
    base = base.split(":")[0]
    if "/" in base:
        base, quote = base.split("/", 1)
    quote_symbol = str(getattr(quote, "symbol", quote) or "USD").strip().upper()
    if quote_symbol in FIAT_QUOTES:
        raise BinanceBulkDataError(
            f"Binance spot has no {base}/{quote_symbol} pair, and {base}{'USDT'} is not substituted (USDT does not "
            f"trade at exactly one {quote_symbol}). Ask for {base}/USDT with quote=Asset('USDT', 'crypto'), or route "
            f"crypto to 'public' to read {base}/{quote_symbol} from Coinbase public data.",
            reason="ambiguous_symbol",
        )
    return f"{base}{quote_symbol}"


def _months(start: pd.Timestamp, end: pd.Timestamp):
    month = start.normalize().replace(day=1)
    while month < end:
        yield month
        month = (month + pd.offsets.MonthBegin(1)).normalize()


class BinanceBulkData:
    """Downloads, verifies and parses Binance bulk files. ``session.get(url, timeout=)`` must
    return an object with ``status_code`` and ``content`` (``requests`` does)."""

    def __init__(self, session=None, base_url: str = BASE_URL, workers: int = 8, now=None):
        if session is None:
            import requests

            session = requests.Session()
            session.headers.update({"User-Agent": "lumibot-backtesting"})
        self._session = session
        self._base_url = base_url.rstrip("/")
        self._workers = max(1, int(workers))
        self._now = now  # tests pin the clock
        self._files: dict[str, pd.DataFrame | None] = {}
        self._memo: dict[tuple, pd.DataFrame] = {}

    def now(self) -> pd.Timestamp:
        return _utc(self._now) if self._now is not None else pd.Timestamp(datetime.now(timezone.utc))

    # -- transport ------------------------------------------------------------------------
    def _get(self, url: str):
        last_error = None
        for _ in range(3):
            try:
                response = self._session.get(url, timeout=60)
            except Exception as exc:  # network errors: retry, then report plainly
                last_error = f"{type(exc).__name__}: {exc}"
                continue
            if response.status_code in (403, 404):
                return None  # the archive answers 404 (or 403) for files it does not have
            if response.status_code == 200:
                return response.content
            last_error = f"HTTP {response.status_code}"
            if response.status_code < 500 and response.status_code != 429:
                break
        raise BinanceBulkDataError(f"Binance bulk file {url} did not answer ({last_error}).",
                                   reason="source_unreachable")

    def _read_file(self, path: str) -> pd.DataFrame | None:
        """One verified CSV from one zip; None when Binance has not published the file."""
        if path in self._files:
            return self._files[path]
        url = f"{self._base_url}/{path}"
        payload = self._get(url)
        frame = None
        if payload is not None:
            checksum = self._get(url + ".CHECKSUM")
            if checksum is None:
                raise BinanceBulkDataError(f"Binance published {path} without a CHECKSUM file; not used.",
                                           reason="source_error")
            expected = checksum.decode().split()[0].strip().lower()
            if hashlib.sha256(payload).hexdigest() != expected:
                raise BinanceBulkDataError(f"Binance file {path} failed its SHA-256 checksum; not used.",
                                           reason="source_error")
            with zipfile.ZipFile(io.BytesIO(payload)) as archive:
                with archive.open(archive.namelist()[0]) as handle:
                    text = handle.read().decode()
            first = text.split("\n", 1)[0]
            has_header = not first[:1].isdigit()
            frame = pd.read_csv(io.StringIO(text), header=0 if has_header else None)
        self._files[path] = frame
        return frame

    def _paths(self, market: str, kind: str, symbol: str, interval: str | None, start, end) -> list[list[str]]:
        """Per month: the monthly file, then daily files as the fallback for that month."""
        prefix = MARKETS[market]
        today = self.now().normalize()
        plan = []
        for month in _months(start, end):
            suffix = f"{interval}/" if interval else ""
            name = f"{symbol}-{interval}" if interval else f"{symbol}-{kind}"
            monthly = f"{prefix}/monthly/{kind}/{symbol}/{suffix}{name}-{month:%Y-%m}.zip"
            month_end = (month + pd.offsets.MonthBegin(1)).normalize()
            days = [day for day in pd.date_range(max(month, start.normalize()), min(month_end, end), freq="D",
                                                 inclusive="left") if day < today]
            daily = [f"{prefix}/daily/{kind}/{symbol}/{suffix}{name}-{day:%Y-%m-%d}.zip" for day in days]
            if kind == "fundingRate":
                daily = []  # Binance publishes funding monthly only
            plan.append([monthly] + daily if month_end <= today else [None] + daily)
        return plan

    def _load(self, market, kind, symbol, interval, start, end) -> list[pd.DataFrame]:
        plan = self._paths(market, kind, symbol, interval, start, end)
        with ThreadPoolExecutor(max_workers=self._workers) as pool:
            monthly = list(pool.map(lambda p: self._read_file(p[0]) if p[0] else None, plan))
            fallbacks = [p[1:] for p, frame in zip(plan, monthly) if frame is None]
            daily = list(pool.map(self._read_file, [path for group in fallbacks for path in group]))
        return [frame for frame in monthly + daily if frame is not None and not frame.empty]

    # -- candles ----------------------------------------------------------------------------
    def candles(self, market: str, symbol: str, timestep: str, start, end, kind: str = "klines") -> pd.DataFrame:
        """Closed candles with open time in ``[start, end)``, indexed by UTC open time.

        Periods Binance has not published (before listing, after delisting, or the last
        day) are simply absent; nothing is filled.
        """
        interval, _ = _INTERVALS[timestep]
        start_ts, end_ts = _utc(start), _utc(end)
        key = (market, kind, symbol, interval, start_ts, end_ts)
        if key not in self._memo:
            frames = []
            for raw in self._load(market, kind, symbol, interval, start_ts, end_ts):
                part = raw.iloc[:, :6].copy()
                part.columns = _KLINE_COLUMNS
                part.index = _epoch_to_utc(part.pop("open_time"))
                frames.append(part.astype(float))
            if frames:
                frame = pd.concat(frames).sort_index()
                frame = frame[~frame.index.duplicated(keep="last")]
                frame = frame[(frame.index >= start_ts) & (frame.index < end_ts)]
            else:
                frame = pd.DataFrame(columns=_KLINE_COLUMNS[1:], index=pd.DatetimeIndex([], tz="UTC"))
            self._memo[key] = frame
        return self._memo[key].copy()

    # -- funding ----------------------------------------------------------------------------
    def funding_history(self, symbol: str, start, end) -> tuple[list[dict], pd.Timestamp | None]:
        """Settled funding in ``[start, end]`` with Binance's mark price at each settlement,
        plus the last time Binance's published funding files cover (None if none)."""
        start_ts, end_ts = _utc(start), _utc(end)
        rows: dict[pd.Timestamp, Decimal] = {}
        covered = None
        for raw in self._load("um", "fundingRate", symbol, None, start_ts, end_ts + pd.Timedelta(seconds=1)):
            times = _epoch_to_utc(raw.iloc[:, 0]).floor("min")
            rates = raw.iloc[:, -1]
            for stamp, rate in zip(times, rates):
                covered = stamp if covered is None or stamp > covered else covered
                if start_ts <= stamp <= end_ts:
                    rows[stamp] = Decimal(str(rate))
            # A monthly file covers through its month end even when the last settlement is earlier.
            month_end = (times.max().normalize().replace(day=1) + pd.offsets.MonthBegin(1)).normalize()
            covered = max(covered, month_end - pd.Timedelta(seconds=1))
        if not rows:
            return [], covered
        marks = self.candles("um", symbol, "hour", min(rows).floor("h"), max(rows).floor("h") + pd.Timedelta(hours=1),
                             kind="markPriceKlines")
        events = []
        for stamp in sorted(rows):
            hour = stamp.floor("h")
            mark = Decimal(str(marks.at[hour, "open"])) if hour in marks.index and stamp == hour else None
            events.append({"timestamp": stamp, "fundingRate": rows[stamp], "markPrice": mark})
        return events, covered


def shared_series(market: str, symbol: str, timestep: str, kind: str = "klines"):
    """Shared bar cache identity: provider, market, symbol, bar size and price kind only."""
    from lumibot.tools import shared_bar_cache

    provider = "binance_um" if market == "um" else "binance_spot"
    return shared_bar_cache.BarSeries.make(
        provider, symbol, shared_bar_cache.normalize_timestep(1, timestep),
        scope=shared_bar_cache.shared_scope(provider), adjusted=False,
        instrument="perpetual" if market == "um" else "spot", price_type=kind, market_data="public_bulk",
    )


def cached_candles(client: BinanceBulkData, market: str, symbol: str, timestep: str, start, end,
                   kind: str = "klines") -> pd.DataFrame:
    """Candles through the shared bar cache when it is enabled (one cache for every user)."""
    from lumibot.tools import shared_bar_cache

    start_ts, end_ts = _utc(start), _utc(end)
    if not shared_bar_cache.enabled():
        return client.candles(market, symbol, timestep, start_ts, end_ts, kind)

    def fetch(gap_start, gap_stop):
        frame = client.candles(market, symbol, timestep, gap_start, gap_stop, kind)
        return None if frame.empty else frame

    rows = shared_bar_cache.read_bars(
        shared_series(market, symbol, timestep, kind), start_ts, end_ts, fetch,
        now=client.now().normalize(), coverage_from_rows=True,
    )
    rows = rows[["open", "high", "low", "close", "volume"]].astype(float)
    rows.index = pd.DatetimeIndex(rows.index).tz_convert("UTC")
    return rows[(rows.index >= start_ts) & (rows.index < end_ts)]
