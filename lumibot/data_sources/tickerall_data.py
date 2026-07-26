"""
TickerAll hosted MT5 API data source for Lumibot.

Connects to the hosted TickerAll MetaTrader 5 API (https://tickerall.com) to
serve historical bars, last prices, and quotes for MT5 instruments (Forex,
metals, indices, CFDs, crypto). Because the data comes from a hosted API, this
data source runs on any OS with no local MetaTrader 5 terminal installed.

It is paired with ``lumibot.brokers.TickerAll`` (which shares this data source's
client), but can also be used stand-alone for research/backtesting-data pulls.

License: MIT
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from threading import RLock

import pandas as pd

from lumibot.entities import Asset, Bars, Quote
from lumibot.tools.lumibot_logger import get_logger

from .data_source import DataSource

logger = get_logger(__name__)

# MT5 timeframes the hosted API serves natively.
_MT5_TIMEFRAMES = {"M1", "M5", "M15", "M30", "H1", "H4", "D1", "W1", "MN1"}
# Minutes-per-bar, used to translate a ``timeshift`` timedelta into a bar count.
_TF_MINUTES = {
    "M1": 1, "M5": 5, "M15": 15, "M30": 30,
    "H1": 60, "H4": 240, "D1": 1440, "W1": 10080, "MN1": 43200,
}
# The canonical asset type this integration uses for every MT5 instrument, so
# that positions parsed from the broker match orders submitted by a strategy
# (Lumibot keys positions on symbol AND asset_type). See the broker docstring.
CANONICAL_ASSET_TYPE = "forex"


class TickerAllData(DataSource):
    """Serves market data from the hosted TickerAll MT5 API."""

    SOURCE = "TICKERALL"
    MIN_TIMESTEP = "minute"
    IS_BACKTESTING_DATA_SOURCE = False
    # Lumibot canonical timestep -> the hosted API's timeframe string.
    TIMESTEP_MAPPING = [
        {"timestep": "minute", "representations": ["M1"]},
        {"timestep": "hour", "representations": ["H1"]},
        {"timestep": "day", "representations": ["D1"]},
        {"timestep": "week", "representations": ["W1"]},
        {"timestep": "month", "representations": ["MN1"]},
    ]

    def __init__(self, config, **kwargs):
        super().__init__(**kwargs)
        self.name = "tickerall"
        self._config = config or {}

        api_key = self._cfg("API_KEY") or self._cfg("TICKERALL_API_KEY")
        if not api_key:
            raise ValueError(
                "TickerAll data source requires an API key. Set config['API_KEY'] "
                "or the TICKERALL_API_KEY environment variable."
            )

        # Import lazily so lumibot stays importable without the optional dependency.
        try:
            from tickerall import Tickerall
        except ImportError as e:
            raise ImportError(
                "The 'tickerall' package is required for the TickerAll data source. "
                "Install it with: pip install tickerall"
            ) from e

        base_url = self._cfg("BASE_URL")
        self.api = Tickerall(api_key=api_key, base_url=base_url) if base_url else Tickerall(api_key=api_key)

        self._configured_account_id = self._cfg("ACCOUNT_ID")
        self._account_id: str | None = None
        self._symbols: list | None = None
        self._stream = None
        self._subscribed: set[str] = set()
        # Reentrant: account_id and _ensure_symbols both guard state under this
        # lock, and _ensure_symbols resolves account_id while holding it.
        self._lock = RLock()

    # ── config helper ────────────────────────────────────────────────────────
    def _cfg(self, key):
        if isinstance(self._config, dict):
            return self._config.get(key)
        return getattr(self._config, key, None)

    # ── account / symbol resolution ──────────────────────────────────────────
    @property
    def account_id(self) -> str:
        if self._account_id is not None:
            return self._account_id
        with self._lock:
            if self._account_id is not None:
                return self._account_id
            if self._configured_account_id:
                self._account_id = self._configured_account_id
                return self._account_id
            accounts = self.api.accounts.list()
            if len(accounts) == 1:
                self._account_id = accounts[0].id
            elif not accounts:
                raise ValueError(
                    "No accounts are connected to this TickerAll API key. Connect one "
                    "in the dashboard, or set config['ACCOUNT_ID']."
                )
            else:
                ids = ", ".join(f"{a.id} ({a.server} #{a.account_number})" for a in accounts)
                raise ValueError(
                    "This TickerAll API key has multiple accounts; set config['ACCOUNT_ID'] "
                    f"to one of: {ids}"
                )
            return self._account_id

    def _ensure_symbols(self) -> list:
        if self._symbols is None:
            with self._lock:
                if self._symbols is None:
                    try:
                        self._symbols = list(self.api.accounts.symbols(self.account_id))
                    except Exception as e:  # pragma: no cover - network dependent
                        logger.warning(f"Could not fetch symbol list from TickerAll: {e}")
                        self._symbols = []
        return self._symbols

    def resolve_symbol(self, asset: Asset | str) -> str:
        """Map a lumibot Asset/str to the broker's real (case-sensitive) symbol.

        Lumibot upper-cases asset symbols at construction, but MT5 broker symbols
        are case-sensitive (e.g. ``EURUSDm``). Resolve case-insensitively against
        the account's live symbol list; fall back to the requested symbol.
        """
        requested = asset.symbol if isinstance(asset, Asset) else str(asset)
        symbols = self._ensure_symbols()
        if requested in symbols:
            return requested
        lowered = requested.lower()
        for s in symbols:
            if s.lower() == lowered:
                return s
        return requested

    def _to_timeframe(self, timestep: str) -> str:
        """Translate a lumibot timestep (or a raw MT5 timeframe) to an API timeframe."""
        if not timestep:
            timestep = self.MIN_TIMESTEP
        ts = str(timestep).strip()
        # Accept a raw MT5 timeframe (M5, M15, H4, ...) passed through directly.
        if ts.upper() in _MT5_TIMEFRAMES:
            return ts.upper()
        # Otherwise map the lumibot canonical timestep via TIMESTEP_MAPPING.
        try:
            return self._parse_source_timestep(ts, reverse=True)
        except Exception:
            # Common shorthand fallbacks.
            fallback = {"minute": "M1", "hour": "H1", "day": "D1", "week": "W1", "month": "MN1"}
            return fallback.get(ts.lower(), "M1")

    # ── market data ──────────────────────────────────────────────────────────
    def get_historical_prices(
        self, asset, length, timestep="", timeshift=None, quote=None,
        exchange=None, include_after_hours=True, **kwargs
    ) -> Bars | None:
        """Return the most recent ``length`` bars as a Bars object, or None if unavailable."""
        if isinstance(asset, str):
            asset = Asset(symbol=asset, asset_type=CANONICAL_ASSET_TYPE)
        if exchange is not None:
            logger.warning(f"TickerAllData ignores the 'exchange' parameter (got {exchange}).")

        tf = self._to_timeframe(timestep or self.get_timestep())
        symbol = self.resolve_symbol(asset)

        # If a timeshift is requested, fetch extra bars and trim the most-recent ones.
        shift_bars = 0
        if timeshift is not None:
            if not isinstance(timeshift, dt.timedelta):
                timeshift = dt.timedelta(days=timeshift)
            shift_bars = int(timeshift.total_seconds() // 60 // _TF_MINUTES.get(tf, 1))

        want = int(length) + shift_bars
        candles = self.api.candles.get(self.account_id, symbol=symbol, count=want, timeframe=tf)
        if not candles:
            # RULE #1: no fabricated data - surface the absence honestly.
            logger.warning(f"No {tf} bars returned by TickerAll for {symbol}.")
            return None

        candles = sorted(candles, key=lambda c: c.timestamp)
        if shift_bars:
            candles = candles[:-shift_bars] if shift_bars < len(candles) else []
        candles = candles[-int(length):]
        if not candles:
            return None

        df = pd.DataFrame(
            [
                {
                    "datetime": c.timestamp,
                    "open": c.open,
                    "high": c.high,
                    "low": c.low,
                    "close": c.close,
                    "volume": c.tick_volume,
                }
                for c in candles
            ]
        )
        df["datetime"] = pd.to_datetime(df["datetime"], unit="s", utc=True)
        df = df.set_index("datetime")
        return Bars(df, self.SOURCE, asset, quote=quote, raw=df)

    def get_last_price(self, asset, quote=None, exchange=None, **kwargs) -> float | Decimal | None:
        """Return the latest price (mid of live bid/ask; falls back to last close)."""
        symbol = self.resolve_symbol(asset)
        tick = self._latest_tick(symbol)
        if tick is not None:
            bid, ask = tick
            if bid and ask:
                return (float(bid) + float(ask)) / 2.0
            return float(ask or bid)
        # Fallback: most recent 1-minute close.
        candles = self.api.candles.get(self.account_id, symbol=symbol, count=1, timeframe="M1")
        if candles:
            return float(candles[-1].close)
        logger.warning(f"No last price available for {symbol} via TickerAll.")
        return None

    def get_quote(self, asset: Asset, quote: Asset = None, exchange: str = None) -> Quote:
        """Return a Quote with live bid/ask when available."""
        symbol = self.resolve_symbol(asset)
        tick = self._latest_tick(symbol)
        if tick is not None:
            bid, ask = tick
            mid = (float(bid) + float(ask)) / 2.0 if (bid and ask) else float(ask or bid)
            return Quote(asset=asset, price=mid, bid=float(bid) if bid else None,
                         ask=float(ask) if ask else None)
        candles = self.api.candles.get(self.account_id, symbol=symbol, count=1, timeframe="M1")
        if candles:
            c = candles[-1]
            return Quote(asset=asset, price=float(c.close),
                         bid=float(c.bid) if c.bid else None, ask=None)
        return Quote(asset=asset)

    def get_chains(self, asset: Asset, quote: Asset = None, exchange: str = None) -> dict:
        # MT5 instruments have no option chains.
        return {}

    # ── live tick helpers ────────────────────────────────────────────────────
    def _latest_tick(self, symbol: str):
        """Return (bid, ask) from the live stream, or None if unavailable."""
        try:
            stream = self._ensure_stream()
            if symbol not in self._subscribed:
                stream.subscribe_ticks(self.account_id, [symbol])
                self._subscribed.add(symbol)
            ev = stream.wait_for_tick(symbol, account_id=self.account_id, timeout=6.0)
            return (ev.bid, ev.ask)
        except Exception as e:
            logger.debug(f"No live tick for {symbol} ({e}); falling back to candle close.")
            return None

    def _ensure_stream(self):
        if self._stream is None or not self._stream.is_connected():
            self._stream = self.api.stream.connect(timeout=15.0)
            self._subscribed = set()
        return self._stream

    def close(self):
        """Release the stream and HTTP client."""
        try:
            if self._stream is not None:
                self._stream.close()
                self._stream = None
        except Exception:
            pass
        try:
            self.api.close()
        except Exception:
            pass
