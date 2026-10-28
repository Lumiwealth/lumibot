"""Incremental (one value at a time) series primitives for Indicator v2.

Every primitive takes one value per bar through ``update(x)`` and returns the
current output, or ``None`` while it is still warming up. ``None`` input is
TradingView Pine's ``na``: the output is ``None`` on that bar and the state does
not advance, so an average always sees its last ``length`` real values.

Cost: ``update`` is O(1) per bar (amortized) for every primitive. None of them
keeps or rescans history. Rolling sums are re-summed exactly once every
``length`` updates, which bounds floating-point drift on very long streams at an
amortized O(1) cost.

Pine v6 semantics followed here:

* ``SeededEMA`` is seeded with the SMA of the first ``length`` values (Pine's
  built-in ``ta.ema`` returns ``na`` before that), then steps
  ``prev + alpha * (x - prev)`` with ``alpha = 2 / (length + 1)``.
* ``SeededRMA`` (Wilder) uses the same SMA seed and steps
  ``(prev * (length - 1) + x) / length``.
* ``RollingStdev`` is the population (biased) standard deviation, like
  ``ta.stdev``.

The state of each primitive is plain Python data, so it deep-copies and
pickles cleanly for snapshots and forming-bar rollback.
"""

from __future__ import annotations

import math
from collections import deque


def _check_length(length) -> int:
    if not isinstance(length, int) or isinstance(length, bool) or length < 1:
        raise ValueError(f"length must be a positive integer, got {length!r}")
    return length


_SLOT_NAMES: dict = {}


def _slot_names(cls) -> tuple:
    """All ``__slots__`` names of ``cls`` and its bases, computed once per class
    (state is copied for every forming bar, so this is on the hot path)."""
    names = _SLOT_NAMES.get(cls)
    if names is None:
        collected = []
        for klass in cls.__mro__:
            collected.extend(getattr(klass, "__slots__", ()))
        names = _SLOT_NAMES[cls] = tuple(collected)
    return names


class _State:
    """Value equality over ``__slots__`` so snapshots compare by content."""

    __slots__ = ()

    def _fields(self):
        return tuple(getattr(self, name) for name in _slot_names(type(self)))

    def __eq__(self, other):
        return type(self) is type(other) and self._fields() == other._fields()

    def __ne__(self, other):
        return not self == other

    __hash__ = None

    def __getstate__(self):
        return {name: getattr(self, name) for name in _slot_names(type(self))}

    def __setstate__(self, state):
        for name, value in state.items():
            setattr(self, name, value)


class RollingSMA(_State):
    """Simple moving average over the last ``length`` non-None values."""

    __slots__ = ("length", "window", "total", "_since_resync")

    def __init__(self, length: int):
        self.length = _check_length(length)
        self.window: deque = deque()
        self.total = 0.0
        self._since_resync = 0

    def update(self, x):
        if x is None:
            return None
        self.window.append(x)
        self.total += x
        if len(self.window) > self.length:
            self.total -= self.window.popleft()
        self._since_resync += 1
        if self._since_resync >= self.length:
            # Exact re-sum bounds drift; happens once per `length` bars.
            self.total = math.fsum(self.window)
            self._since_resync = 0
        if len(self.window) < self.length:
            return None
        return self.total / self.length


class _Seeded(_State):
    __slots__ = ("length", "seed_total", "seed_count", "value")

    def __init__(self, length: int):
        self.length = _check_length(length)
        self.seed_total = 0.0
        self.seed_count = 0
        self.value = None

    def _step(self, prev, x):  # pragma: no cover - overridden
        raise NotImplementedError

    def update(self, x):
        if x is None:
            return None
        if self.length == 1:
            self.value = x
            return x
        if self.value is None:
            self.seed_total += x
            self.seed_count += 1
            if self.seed_count < self.length:
                return None
            self.value = self.seed_total / self.length
            return self.value
        self.value = self._step(self.value, x)
        return self.value


class SeededEMA(_Seeded):
    """Exponential moving average seeded with the SMA of the first ``length`` values."""

    __slots__ = ()

    def _step(self, prev, x):
        return prev + (2.0 / (self.length + 1)) * (x - prev)


class SeededRMA(_Seeded):
    """Wilder's moving average (RMA) seeded with the SMA of the first ``length`` values."""

    __slots__ = ()

    def _step(self, prev, x):
        return (prev * (self.length - 1) + x) / self.length


class RollingStdev(_State):
    """Population standard deviation over the last ``length`` non-None values.

    Uses a sliding-window Welford update (O(1)) and recomputes the window
    exactly once every ``length`` updates to keep rounding error bounded.
    """

    __slots__ = ("length", "window", "mean", "m2", "_since_resync")

    def __init__(self, length: int):
        self.length = _check_length(length)
        self.window: deque = deque()
        self.mean = 0.0
        self.m2 = 0.0
        self._since_resync = 0

    def _resync(self):
        n = len(self.window)
        self.mean = math.fsum(self.window) / n
        self.m2 = math.fsum((v - self.mean) ** 2 for v in self.window)
        self._since_resync = 0

    def update(self, x):
        if x is None:
            return None
        self.window.append(x)
        if len(self.window) > self.length:
            old = self.window.popleft()
            new_mean = self.mean + (x - old) / self.length
            self.m2 += (x - old) * (x - new_mean + old - self.mean)
            self.mean = new_mean
        else:
            n = len(self.window)
            delta = x - self.mean
            self.mean += delta / n
            self.m2 += delta * (x - self.mean)
        self._since_resync += 1
        if self._since_resync >= self.length:
            self._resync()
        if len(self.window) < self.length:
            return None
        return math.sqrt(max(0.0, self.m2 / self.length))


class _RollingExtreme(_State):
    """Monotonic-deque rolling max/min: amortized O(1) per update."""

    __slots__ = ("length", "items", "count")

    def __init__(self, length: int):
        self.length = _check_length(length)
        self.items: deque = deque()  # (index, value)
        self.count = 0

    def _keep(self, existing, new) -> bool:  # pragma: no cover - overridden
        raise NotImplementedError

    def update(self, x):
        if x is None:
            return None
        i = self.count
        self.count += 1
        while self.items and not self._keep(self.items[-1][1], x):
            self.items.pop()
        self.items.append((i, x))
        while self.items[0][0] <= i - self.length:
            self.items.popleft()
        if self.count < self.length:
            return None
        return self.items[0][1]


class RollingMax(_RollingExtreme):
    """Highest value over the last ``length`` non-None values (Pine ``ta.highest``)."""

    __slots__ = ()

    def _keep(self, existing, new):
        return existing > new


class RollingMin(_RollingExtreme):
    """Lowest value over the last ``length`` non-None values (Pine ``ta.lowest``)."""

    __slots__ = ()

    def _keep(self, existing, new):
        return existing < new


class Previous(_State):
    """Remembers the previous non-None value (Pine ``x[1]``)."""

    __slots__ = ("value",)

    def __init__(self):
        self.value = None

    def update(self, x):
        """Return the previous value, then remember ``x``."""
        if x is None:
            return None
        prev, self.value = self.value, x
        return prev


def safe_div(num, den):
    """Pine-style division: ``None`` when either side is missing or the divisor is zero."""
    if num is None or den is None or den == 0:
        return None
    return num / den


__all__ = [
    "Previous",
    "RollingMax",
    "RollingMin",
    "RollingSMA",
    "RollingStdev",
    "SeededEMA",
    "SeededRMA",
    "safe_div",
]
