"""One shared candle builder: turn small bars into bigger bars.

Three tools live here, so every data source, strategy and indicator uses the
same rules:

* :func:`resample_ohlcv` is the batch resampler LumiBot's data paths use. It
  takes the exact options those paths always used (``label``/``closed`` on
  the left edge, pandas' default bin origin, an explicit aggregation map, a
  drop rule), so moving a caller onto it never changes a backtest result.
* :func:`build_candles` builds candles from finer bars cut on **UTC
  boundaries** (5 minute candles start at :00/:05 UTC, daily candles at UTC
  midnight), whatever time zone the input index is in. This matches how
  crypto exchanges label their own candles.
* :class:`CandleBuilder` does the same thing one bar at a time for live
  data. It keeps a **forming** (not yet closed) candle that updates on every
  incoming bar, wicks included, and **locks** it when the bucket ends.

:func:`choose_candle_source` picks where candles should come from: the
exchange's own (native) timeframe when it has one, otherwise 1 minute bars
built up with :class:`CandleBuilder`.

Nothing here invents data. A bucket with no source bars produces no candle,
and a missing volume stays missing (``None``), never zero.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

import pandas as pd

#: Standard OHLCV aggregation. Copy it before adding columns.
OHLCV_AGGREGATION = {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}

_PRICE_COLUMNS = ("open", "high", "low", "close")

_UNIT_ALIASES = {
    "s": "s", "sec": "s", "secs": "s", "second": "s", "seconds": "s",
    "m": "m", "min": "m", "mins": "m", "minute": "m", "minutes": "m", "t": "m",
    "h": "h", "hr": "h", "hrs": "h", "hour": "h", "hours": "h",
    "d": "d", "day": "d", "days": "d",
}
_UNIT_DELTA = {"s": pd.Timedelta(seconds=1), "m": pd.Timedelta(minutes=1), "h": pd.Timedelta(hours=1), "d": pd.Timedelta(days=1)}
_TIMEFRAME_RE = re.compile(r"^\s*(\d+)?\s*([A-Za-z]+)\s*$")


# ---------------------------------------------------------------------------
# Timeframes
# ---------------------------------------------------------------------------


def parse_timeframe(timeframe) -> pd.Timedelta:
    """Turn ``"5m"``, ``"5min"``, ``"5 minutes"``, ``"1h"``, ``"1d"``,
    ``"minute"`` or a ``Timedelta`` into a positive ``pandas.Timedelta``.

    Only fixed-length timeframes (seconds, minutes, hours, days) are
    accepted. Weeks and months have no fixed UTC grid, so they raise
    ``ValueError`` instead of guessing. ``"1M"`` (capital M) is refused
    because exchanges use it for one month.
    """
    if isinstance(timeframe, pd.Timedelta):
        delta = timeframe
    elif hasattr(timeframe, "total_seconds") and not isinstance(timeframe, (str, bytes)):
        delta = pd.Timedelta(timeframe)
    elif isinstance(timeframe, str):
        match = _TIMEFRAME_RE.match(timeframe)
        if not match:
            raise ValueError(f"unknown timeframe {timeframe!r}; use forms like '1m', '15m', '1h', '1d'")
        count_text, unit_text = match.groups()
        if unit_text == "M":
            raise ValueError("'M' means a month on exchanges; months have no fixed length. Use minutes ('m').")
        unit = _UNIT_ALIASES.get(unit_text.lower())
        if unit is None:
            raise ValueError(f"unknown timeframe unit in {timeframe!r}; use s, m, h or d")
        count = int(count_text) if count_text else 1
        delta = _UNIT_DELTA[unit] * count
    else:
        raise ValueError(f"timeframe must be a string or timedelta, got {timeframe!r}")
    if delta <= pd.Timedelta(0):
        raise ValueError(f"timeframe must be positive, got {timeframe!r}")
    return delta


def timeframe_label(timeframe) -> str:
    """Short exchange-style label: ``Timedelta(minutes=15)`` -> ``"15m"``."""
    delta = parse_timeframe(timeframe)
    for suffix, unit in (("d", _UNIT_DELTA["d"]), ("h", _UNIT_DELTA["h"]), ("m", _UNIT_DELTA["m"]), ("s", _UNIT_DELTA["s"])):
        if delta % unit == pd.Timedelta(0):
            return f"{delta // unit}{suffix}"
    raise ValueError(f"timeframe {timeframe!r} is not a whole number of seconds")


def choose_candle_source(timeframe, native_timeframes: Iterable | Mapping, fallback="1m") -> tuple[str, bool]:
    """Pick where candles for ``timeframe`` should come from.

    ``native_timeframes`` is what the exchange serves directly (for a CCXT
    exchange, ``exchange.timeframes``; a dict's keys are used). Returns
    ``(source_timeframe, needs_building)``:

    * the target itself when the exchange has it natively: ``("15m", False)``;
    * otherwise ``fallback`` (1 minute) when available: ``("1m", True)``;
    * otherwise the largest native timeframe that divides the target evenly.

    Raises ``ValueError`` when nothing native can build the target.
    """
    target = parse_timeframe(timeframe)
    native = {}
    for item in native_timeframes:
        try:
            native[parse_timeframe(item)] = item
        except ValueError:
            continue  # weeks/months and other labels we cannot build from
    if target in native:
        return timeframe_label(target), False
    fallback_delta = parse_timeframe(fallback)
    if fallback_delta in native and target % fallback_delta == pd.Timedelta(0):
        return timeframe_label(fallback_delta), True
    divisors = [d for d in native if d < target and target % d == pd.Timedelta(0)]
    if divisors:
        return timeframe_label(max(divisors)), True
    raise ValueError(f"no native timeframe can build {timeframe_label(target)} candles from {sorted(native.values())}")


# ---------------------------------------------------------------------------
# Batch
# ---------------------------------------------------------------------------


def resample_ohlcv(
    df: pd.DataFrame,
    rule,
    *,
    aggregation: Mapping[str, str] | None = None,
    dropna: str | None = "ohlc",
    sort: bool = True,
    label: str = "left",
    closed: str = "left",
    origin: Any = "start_day",
) -> pd.DataFrame:
    """Resample an OHLCV DataFrame indexed by time into bigger bars.

    Parameters
    ----------
    rule
        A pandas offset alias (``"5min"``, ``"1h"``, ``"1D"``) or anything
        :func:`parse_timeframe` accepts.
    aggregation
        Column -> pandas aggregation. ``None`` uses :data:`OHLCV_AGGREGATION`
        limited to the columns present. An explicit map is passed through
        as is, so a missing column raises ``KeyError`` exactly as pandas does.
    dropna
        ``"ohlc"`` drops buckets where any present price column is missing
        (empty buckets, e.g. nights and weekends). ``"any"`` drops a bucket
        when any aggregated column is missing. ``None`` keeps every bucket.
    sort
        Sort by time first (``first``/``last`` depend on order).
    origin
        Passed to pandas. The default (``"start_day"``) is pandas' own
        default and is what LumiBot's backtest paths have always used.
        :func:`build_candles` uses UTC epoch boundaries instead.
    """
    try:
        rule = pd.tseries.frequencies.to_offset(parse_timeframe(rule))
    except ValueError:
        pass  # a pandas alias we do not parse ourselves ("W", "ME"): pandas decides
    frame = df.sort_index() if sort else df
    if aggregation is None:
        aggregation = {column: how for column, how in OHLCV_AGGREGATION.items() if column in frame.columns}
    kwargs = {"label": label, "closed": closed}
    if origin != "start_day":
        kwargs["origin"] = origin
    out = frame.resample(rule, **kwargs).agg(aggregation)
    if dropna == "any":
        out = out.dropna()
    elif dropna == "ohlc":
        required = [column for column in _PRICE_COLUMNS if column in out.columns]
        if required:
            out = out.dropna(subset=required, how="any")
    elif dropna is not None:
        raise ValueError(f"dropna must be 'ohlc', 'any' or None, got {dropna!r}")
    return out


def build_candles(df: pd.DataFrame, timeframe, *, include_forming: bool = True, source_timeframe="1m") -> pd.DataFrame:
    """Build ``timeframe`` candles from finer OHLCV bars, cut on UTC boundaries.

    The index may be in any time zone (naive means UTC); the result keeps the
    input's zone. With ``include_forming=False`` the last candle is dropped
    unless its bucket is complete, judged by the last source bar's end time
    (``source_timeframe`` long).
    """
    if df.empty:
        return df.copy()
    target = parse_timeframe(timeframe)
    index = pd.DatetimeIndex(df.index)
    original_tz = index.tz
    utc = df.copy()
    utc.index = index.tz_localize("UTC") if original_tz is None else index.tz_convert("UTC")
    out = resample_ohlcv(utc, pd.tseries.frequencies.to_offset(target), origin="epoch")
    if not include_forming and len(out):
        last_source_end = utc.index.max() + parse_timeframe(source_timeframe)
        if last_source_end < out.index[-1] + target:
            out = out.iloc[:-1]
    out.index = out.index.tz_localize(None) if original_tz is None else out.index.tz_convert(original_tz)
    return out


# ---------------------------------------------------------------------------
# Streaming
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Candle:
    """One built candle. ``time`` is the bucket start (UTC); ``closed`` is
    ``False`` while the candle is still forming. Indicators accept it
    directly: ``indicator.update(candle, closed=candle.closed)``."""

    time: pd.Timestamp
    open: float
    high: float
    low: float
    close: float
    volume: float | None
    closed: bool = False


_FIELDS = {
    "time": ("t", "time", "timestamp", "datetime", "date"),
    "open": ("o", "open"),
    "high": ("h", "high"),
    "low": ("l", "low"),
    "close": ("c", "close"),
    "volume": ("v", "volume"),
}


def _field(bar, name):
    for alias in _FIELDS[name]:
        if isinstance(bar, Mapping):
            if alias in bar:
                return bar[alias]
        elif hasattr(bar, alias):
            return getattr(bar, alias)
    return None


def _utc(value) -> pd.Timestamp:
    if value is None:
        raise ValueError("a bar needs a time")
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        ts = pd.Timestamp(int(value), unit="ms", tz="UTC")  # epoch milliseconds, the exchange format
    else:
        ts = pd.Timestamp(value)
    return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")


def _normalize(bar) -> tuple:
    if isinstance(bar, (list, tuple)) and not hasattr(bar, "_fields") and len(bar) >= 5:
        # CCXT row: [ms, open, high, low, close, volume]
        when, o, h, l, c = bar[:5]
        v = bar[5] if len(bar) > 5 else None
    else:
        when, o, h, l, c, v = (_field(bar, name) for name in ("time", "open", "high", "low", "close", "volume"))
    prices = []
    for value in (o, h, l, c):
        number = float(value)
        if number != number or number in (float("inf"), float("-inf")):
            raise ValueError(f"bar prices must be finite, got {value!r}")
        prices.append(number)
    volume = None
    if v is not None:
        volume = float(v)
        if volume != volume:
            volume = None
    return (_utc(when), *prices, volume)


class CandleBuilder:
    """Build ``timeframe`` candles one source bar at a time.

    ``update(bar, closed=False)`` takes one source bar (a dict with
    ``t/o/h/l/c/v`` or ``time/open/...``, an object with those attributes,
    or a CCXT ``[ms, o, h, l, c, v]`` row). The same source bar may be sent
    again while it is still forming (exchanges re-send the current minute);
    it replaces the earlier copy instead of being counted twice.

    It returns a list of :class:`Candle`: any candles that just closed, then
    the current forming candle (if any). A candle locks when

    * a source bar from a later bucket arrives,
    * the bucket's last source bar arrives with ``closed=True``, or
    * :meth:`close_through` is called with a time at or after the bucket end.

    Buckets are cut on UTC boundaries. ``source_timeframe`` must divide
    ``timeframe``; when they are equal (a native exchange candle) bars pass
    straight through as forming/closed candles.
    """

    def __init__(self, timeframe, source_timeframe="1m"):
        self.timeframe = parse_timeframe(timeframe)
        self.source_timeframe = parse_timeframe(source_timeframe)
        if self.source_timeframe > self.timeframe or self.timeframe % self.source_timeframe != pd.Timedelta(0):
            raise ValueError(
                f"source timeframe {timeframe_label(self.source_timeframe)} must divide "
                f"{timeframe_label(self.timeframe)} evenly"
            )
        self._tf_ns = self.timeframe.value
        self._bucket = None  # bucket start (Timestamp)
        self._done = None  # aggregate of finished source bars: (o, h, l, c, v, has_volume)
        self._latest = None  # (time, o, h, l, c, v) of the newest source bar, may still change
        self._locked_through = None  # end of the last closed bucket

    @property
    def forming(self) -> Candle | None:
        """The candle still being built, or ``None``."""
        return self._candle(closed=False) if self._bucket is not None else None

    def _bucket_start(self, when: pd.Timestamp) -> pd.Timestamp:
        return pd.Timestamp(when.value - when.value % self._tf_ns, tz="UTC")

    def _candle(self, closed: bool) -> Candle:
        t, o, h, l, c, v = self._latest
        if self._done is None:
            return Candle(self._bucket, o, h, l, c, v, closed)
        do, dh, dl, _dc, dv, has_volume = self._done
        volume = None
        if has_volume or v is not None:
            volume = (dv or 0.0) + (v or 0.0)
        return Candle(self._bucket, do, max(dh, h), min(dl, l), c, volume, closed)

    def _fold_latest(self) -> None:
        _t, o, h, l, c, v = self._latest
        if self._done is None:
            self._done = (o, h, l, c, v or 0.0, v is not None)
        else:
            do, dh, dl, _dc, dv, has_volume = self._done
            self._done = (do, max(dh, h), min(dl, l), c, dv + (v or 0.0), has_volume or v is not None)

    def _close_current(self) -> Candle:
        candle = self._candle(closed=True)
        self._locked_through = self._bucket + self.timeframe
        self._bucket = self._done = self._latest = None
        return candle

    def update(self, bar, closed: bool = False) -> list:
        """Apply one source bar. See the class docstring."""
        when, o, h, l, c, v = _normalize(bar)
        events = []
        if self._locked_through is not None and when < self._locked_through:
            raise ValueError(f"bar at {when} belongs to a candle that already closed")
        bucket = self._bucket_start(when)
        if self._bucket is not None and bucket != self._bucket:
            if bucket < self._bucket:
                raise ValueError(f"bar at {when} is older than the forming candle {self._bucket}")
            events.append(self._close_current())
        if self._bucket is None:
            self._bucket = bucket
        if self._latest is not None:
            if when < self._latest[0]:
                raise ValueError(f"bar at {when} is older than the latest bar {self._latest[0]}")
            if when > self._latest[0]:
                self._fold_latest()
        self._latest = (when, o, h, l, c, v)
        if closed and when + self.source_timeframe >= self._bucket + self.timeframe:
            events.append(self._close_current())
        else:
            events.append(self._candle(closed=False))
        return events

    def close_through(self, now) -> list:
        """Lock the forming candle if ``now`` is at or past its bucket end.

        Call it from a clock when the exchange may not send another bar soon
        (quiet markets). Returns the closed candle in a list, or ``[]``."""
        if self._bucket is None:
            return []
        if _utc(now) >= self._bucket + self.timeframe:
            return [self._close_current()]
        return []

    def reset(self) -> None:
        """Forget everything (for example after a reconnect gap)."""
        self._bucket = self._done = self._latest = self._locked_through = None


__all__ = [
    "Candle",
    "CandleBuilder",
    "OHLCV_AGGREGATION",
    "build_candles",
    "choose_candle_source",
    "parse_timeframe",
    "resample_ohlcv",
    "timeframe_label",
]
