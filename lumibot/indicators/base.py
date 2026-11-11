"""Indicator v2 base class: one-bar updates with forming-bar rollback.

An indicator is a small class that declares what it needs and what it draws,
then processes **one bar at a time**::

    class Momentum(Indicator):
        key = "momentum"
        version = 1                     # bump when the math changes
        title = "Momentum"
        short_title = "MOM"
        overlay = False                 # False = own pane under the price chart
        inputs = [InputSpec("length", "int", 10, min=1, label="Length")]
        outputs = [OutputSpec("mom", "Momentum", "line", "#2962FF")]

        def init_state(self):
            return {"closes": deque(maxlen=self.inputs["length"] + 1)}

        def step(self, state, bar):
            closes = state["closes"]
            closes.append(bar.close)
            full = len(closes) == closes.maxlen
            return {"mom": closes[-1] - closes[0] if full else None}

Contract
--------

* ``update(bar, closed)`` processes ONE bar and returns ``{output_id: value}``
  with ``None`` for "no value yet" (Pine's ``na``).
* Rollback (Pine semantics): state only advances on ``closed=True``. A forming
  bar (``closed=False``) is evaluated on a throwaway copy of the committed
  state, so it may be called any number of times while the bar is still
  building, and the eventual ``closed=True`` call commits exactly as if the
  forming updates never happened.
* ``compute(bars)`` returns full series. The default runs the same ``step``
  loop on a fresh instance, so it always equals the streaming result. An
  indicator may override it with a vectorized fast path, but it must stay
  equal to the loop (the test suite checks every built-in).
* ``snapshot()`` / ``restore(state)`` hand the committed state to another
  process or instance. Snapshots are plain picklable Python data.
* ``describe()`` returns a JSON-safe schema (inputs, outputs, levels) for UIs
  and AI agents.

Cost: an update calls ``step`` once on a staged copy of the committed state.
The calculation for every built-in is O(1) per bar (rolling state, no history
recompute); the copy is sized by the
indicator's windows (its ``length`` inputs), never by how much history has
been processed.
"""

from __future__ import annotations

import copy
import math
import pickle
from dataclasses import dataclass
from typing import Any, Iterable, NamedTuple

import pandas as pd

INPUT_TYPES = ("int", "float", "bool", "color", "source", "string", "enum")
OUTPUT_KINDS = (
    "line",
    "step",
    "area",
    "histogram",
    "columns",
    "circles",
    "cross",
    "scatter",
    "markers",
    "shape",
    "fill",
    "background",
    "bar_color",
    "candle",
)
LINE_STYLES = ("solid", "dashed", "dotted")
DISPLAY_MODES = ("all", "pane", "status_line", "data_window", "none")
PANES = ("price", "own")
SHAPES = (
    "circle",
    "square",
    "diamond",
    "cross",
    "xcross",
    "triangle_up",
    "triangle_down",
    "arrow_up",
    "arrow_down",
    "label_up",
    "label_down",
    "flag",
)
SHAPE_LOCATIONS = ("above_bar", "below_bar", "absolute", "top", "bottom")
SHAPE_SIZES = ("auto", "tiny", "small", "normal", "large", "huge")
#: Colors that follow the chart's theme: its text color and its background color.
THEME_COLORS = ("chart.fg", "chart.bg")
SOURCES = ("close", "open", "high", "low", "hl2", "hlc3", "ohlc4", "hlcc4")


class InvalidIndicatorInputError(ValueError):
    """An input name, type or value does not match the indicator's schema."""


@dataclass(frozen=True)
class InputSpec:
    """One typed input. ``type`` is one of ``int``, ``float``, ``bool``,
    ``color``, ``source``, ``string`` or ``enum``."""

    name: str
    type: str
    default: Any
    min: float | None = None
    max: float | None = None
    step: float | None = None
    options: tuple | None = None
    label: str | None = None

    def __post_init__(self):
        if self.type not in INPUT_TYPES:
            raise ValueError(f"input type must be one of {INPUT_TYPES}, got {self.type!r}")
        if self.type == "source" and self.options is None:
            object.__setattr__(self, "options", SOURCES)
        if self.options is not None:
            object.__setattr__(self, "options", tuple(self.options))

    def describe(self) -> dict:
        return {
            "name": self.name,
            "type": self.type,
            "default": self.default,
            "min": self.min,
            "max": self.max,
            "step": self.step,
            "options": list(self.options) if self.options is not None else None,
            "label": self.label or self.name,
        }

    def validate(self, value):
        bad = InvalidIndicatorInputError
        if self.type == "int":
            if isinstance(value, bool) or not isinstance(value, int):
                raise bad(f"input {self.name!r} must be an integer, got {value!r}")
        elif self.type == "float":
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
                raise bad(f"input {self.name!r} must be a finite number, got {value!r}")
            value = float(value)
        elif self.type == "bool":
            if not isinstance(value, bool):
                raise bad(f"input {self.name!r} must be true or false, got {value!r}")
        elif not isinstance(value, str):
            raise bad(f"input {self.name!r} must be a string, got {value!r}")
        if self.min is not None and value < self.min:
            raise bad(f"input {self.name!r} must be >= {self.min}, got {value!r}")
        if self.max is not None and value > self.max:
            raise bad(f"input {self.name!r} must be <= {self.max}, got {value!r}")
        if self.options is not None and value not in self.options:
            raise bad(f"input {self.name!r} must be one of {list(self.options)}, got {value!r}")
        if self.type == "color" and not value.startswith("#"):
            raise bad(f"input {self.name!r} must be a #RRGGBB color, got {value!r}")
        return value


def _is_color(value) -> bool:
    """``#RRGGBB``, ``#RRGGBBAA`` (the last two digits are opacity) or a theme color."""
    if not isinstance(value, str):
        return False
    if value in THEME_COLORS:
        return True
    if not value.startswith("#") or len(value) not in (7, 9):
        return False
    try:
        int(value[1:], 16)
    except ValueError:
        return False
    return True


def _check_color(value, what: str) -> None:
    if not _is_color(value):
        raise ValueError(
            f"{what} must be #RRGGBB, #RRGGBBAA (AA = opacity) or one of {THEME_COLORS}, got {value!r}"
        )


def _check_choice(value, choices, what: str) -> None:
    if value not in choices:
        raise ValueError(f"{what} must be one of {choices}, got {value!r}")


def _check_width(value, what: str) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not 0 < value <= 10:
        raise ValueError(f"{what} must be above 0 and at most 10, got {value!r}")


@dataclass(frozen=True)
class OutputSpec:
    """One thing the indicator draws.

    Each bar's value is a number (or ``None``). Everything about how it looks
    is declared here once. The "Indicator outputs and styles" documentation
    page shows every option with an example.

    ``kind``
        ``line`` (breaks where the value is ``None``), ``step``, ``area``,
        ``histogram``, ``columns``, ``circles``, ``cross``, ``scatter``,
        ``markers``, ``shape`` (a mark on bars whose value is set and not 0),
        ``fill`` (shades between two outputs or levels, see ``between``),
        ``background`` (colors the bar's whole column), ``bar_color``
        (recolors the price candles) or ``candle`` (draws candles from four
        outputs, see ``candle``).
    ``color``
        ``#RRGGBB``, ``#RRGGBBAA`` (the last two hex digits are opacity: ``80``
        is half) or ``chart.fg`` / ``chart.bg`` to follow the chart's theme.
    ``line_width``, ``line_style``
        Width in pixels (for ``circles``, ``cross`` and ``scatter``: the mark
        size), and ``solid``, ``dashed`` or ``dotted``.
    ``palette``, ``color_by``, ``gradient``
        A color per bar. ``color_by`` names another output whose value on each
        bar picks the color: an index into ``palette`` (0, 1, 2...), or, with
        ``gradient=(low, high)``, a position blended across the palette.
        ``None`` on a bar falls back to ``color``. ``background`` and
        ``bar_color`` outputs may pick with their own value instead.
    ``offset``
        Shift the drawing by this many bars (negative: left, positive: right).
    ``display``
        ``all``, ``pane`` (drawn, value not in the status line),
        ``status_line`` (value shown, nothing drawn), ``data_window`` or
        ``none`` (a helper output, such as the one ``color_by`` reads).
    ``precision``
        Decimals shown for this output's value.
    ``pane``
        ``price`` or ``own``: draw this output in a different place than the
        indicator's ``overlay`` setting (Pine's ``force_overlay``).
    ``track_price``
        Draw a dotted line across the pane at the newest value.
    ``shape``, ``location``, ``size``, ``text``, ``text_color``
        For ``shape`` outputs: which mark, where (``above_bar``,
        ``below_bar``, ``absolute`` at the value, ``top`` or ``bottom`` of the
        pane), how big, and optional short text (one character works like
        Pine's ``plotchar``).
    ``between``
        For ``fill`` outputs: two output ids or level values to shade between.
    ``candle``
        For ``candle`` outputs: the ids of the open, high, low and close outputs.
    """

    id: str
    label: str
    kind: str = "line"
    color: str = "#2962FF"
    line_width: float = 1
    line_style: str = "solid"
    palette: tuple = ()
    color_by: str | None = None
    gradient: tuple | None = None
    offset: int = 0
    display: str = "all"
    precision: int | None = None
    pane: str | None = None
    track_price: bool = False
    shape: str = "circle"
    location: str = "above_bar"
    size: str = "auto"
    text: str = ""
    text_color: str | None = None
    between: tuple | None = None
    candle: tuple | None = None

    def __post_init__(self):
        name = f"output {self.id!r}"
        _check_choice(self.kind, OUTPUT_KINDS, "output kind")
        _check_color(self.color, f"{name} color")
        _check_width(self.line_width, f"{name} line_width")
        _check_choice(self.line_style, LINE_STYLES, f"{name} line_style")
        object.__setattr__(self, "palette", tuple(self.palette))
        for item in self.palette:
            _check_color(item, f"{name} palette color")
        if self.gradient is not None:
            low, high = self.gradient
            if not (math.isfinite(low) and math.isfinite(high)) or low == high:
                raise ValueError(f"{name} gradient must be two different numbers, got {self.gradient!r}")
            if len(self.palette) < 2:
                raise ValueError(f"{name} gradient needs a palette of at least two colors")
            object.__setattr__(self, "gradient", (float(low), float(high)))
        if self.color_by is not None and not self.palette:
            raise ValueError(f"{name} color_by needs a palette")
        if isinstance(self.offset, bool) or not isinstance(self.offset, int):
            raise ValueError(f"{name} offset must be a whole number of bars, got {self.offset!r}")
        _check_choice(self.display, DISPLAY_MODES, f"{name} display")
        if self.precision is not None and (
            isinstance(self.precision, bool) or not isinstance(self.precision, int) or not 0 <= self.precision <= 10
        ):
            raise ValueError(f"{name} precision must be a whole number from 0 to 10, got {self.precision!r}")
        if self.pane is not None:
            _check_choice(self.pane, PANES, f"{name} pane")
        _check_choice(self.shape, SHAPES, f"{name} shape")
        _check_choice(self.location, SHAPE_LOCATIONS, f"{name} location")
        _check_choice(self.size, SHAPE_SIZES, f"{name} size")
        if not isinstance(self.text, str) or len(self.text) > 40:
            raise ValueError(f"{name} text must be at most 40 characters")
        if self.text_color is not None:
            _check_color(self.text_color, f"{name} text_color")
        if self.kind == "fill":
            if self.between is None or len(self.between) != 2:
                raise ValueError(f"fill {name} needs between=(output id or level value, output id or level value)")
            object.__setattr__(self, "between", tuple(self.between))
        elif self.between is not None:
            raise ValueError(f"{name}: between is only for fill outputs")
        if self.kind == "candle":
            if self.candle is None or len(self.candle) != 4:
                raise ValueError(f"candle {name} needs candle=(open id, high id, low id, close id)")
            object.__setattr__(self, "candle", tuple(self.candle))
        elif self.candle is not None:
            raise ValueError(f"{name}: candle is only for candle outputs")

    def references(self) -> list:
        """Ids of other outputs this one reads (``color_by``, ``between``, ``candle``)."""
        refs = [self.color_by] if self.color_by else []
        refs += [item for item in (self.between or ()) if isinstance(item, str)]
        refs += list(self.candle or ())
        return refs

    def describe(self) -> dict:
        return {
            "id": self.id,
            "label": self.label,
            "kind": self.kind,
            "color": self.color,
            "line_width": self.line_width,
            "line_style": self.line_style,
            "palette": list(self.palette),
            "color_by": self.color_by,
            "gradient": list(self.gradient) if self.gradient is not None else None,
            "offset": self.offset,
            "display": self.display,
            "precision": self.precision,
            "pane": self.pane,
            "track_price": self.track_price,
            "shape": self.shape,
            "location": self.location,
            "size": self.size,
            "text": self.text,
            "text_color": self.text_color,
            "between": list(self.between) if self.between is not None else None,
            "candle": list(self.candle) if self.candle is not None else None,
        }


@dataclass(frozen=True)
class Level:
    """A fixed horizontal guide line (Pine ``hline``). ``style`` is ``solid``,
    ``dashed`` or ``dotted``; ``color`` left out uses the chart's own."""

    value: float
    label: str = ""
    style: str = "dashed"
    color: str | None = None
    line_width: float = 1

    def __post_init__(self):
        _check_choice(self.style, LINE_STYLES, "level style")
        if self.color is not None:
            _check_color(self.color, "level color")
        _check_width(self.line_width, "level line_width")

    def describe(self) -> dict:
        return {
            "value": self.value,
            "label": self.label,
            "style": self.style,
            "color": self.color,
            "line_width": self.line_width,
        }


class Bar(NamedTuple):
    """One OHLCV bar. ``time`` is a UTC ``pandas.Timestamp`` or ``None``."""

    time: Any
    open: float
    high: float
    low: float
    close: float
    volume: float | None

    @property
    def hl2(self) -> float:
        return (self.high + self.low) / 2

    @property
    def hlc3(self) -> float:
        return (self.high + self.low + self.close) / 3

    @property
    def ohlc4(self) -> float:
        return (self.open + self.high + self.low + self.close) / 4

    @property
    def hlcc4(self) -> float:
        return (self.high + self.low + 2 * self.close) / 4

    def source(self, name: str) -> float:
        """Value of a named price source (``close``, ``hl2``, ``ohlc4``...)."""
        if name not in SOURCES:
            raise InvalidIndicatorInputError(f"unknown source {name!r}; use one of {list(SOURCES)}")
        return getattr(self, name)


_FIELD_ALIASES = {
    "time": ("t", "time", "timestamp", "datetime", "date"),
    "open": ("o", "open"),
    "high": ("h", "high"),
    "low": ("l", "low"),
    "close": ("c", "close"),
    "volume": ("v", "volume"),
}


def _to_time(value):
    if value is None:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        # Numeric times are epoch milliseconds (the usual chart/exchange format).
        ts = pd.Timestamp(int(value), unit="ms", tz="UTC")
    else:
        ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")  # naive times are treated as UTC
    return ts.tz_convert("UTC")


def _pick(bar, name):
    for alias in _FIELD_ALIASES[name]:
        if isinstance(bar, dict):
            if alias in bar:
                return bar[alias]
        elif hasattr(bar, alias):
            return getattr(bar, alias)
    return None


def to_bar(bar, time=None) -> Bar:
    """Normalize a dict (``t/o/h/l/c/v`` or ``time/open/.../volume``), a
    ``Bar``, or any object with those attributes into a :class:`Bar`.

    Prices must be finite numbers. A missing or NaN volume becomes ``None``
    (never invented as zero)."""
    if isinstance(bar, Bar) and time is None:
        return bar
    values = {}
    for name in ("open", "high", "low", "close"):
        raw = _pick(bar, name)
        try:
            number = float(raw)
        except (TypeError, ValueError):
            number = math.nan
        if not math.isfinite(number):
            raise ValueError(f"bar {name} must be a finite number, got {raw!r}")
        values[name] = number
    raw_volume = _pick(bar, "volume")
    try:
        volume = None if raw_volume is None else float(raw_volume)
    except (TypeError, ValueError):
        volume = None
    if volume is not None and not math.isfinite(volume):
        volume = None
    when = time if time is not None else _pick(bar, "time")
    return Bar(_to_time(when), values["open"], values["high"], values["low"], values["close"], volume)


def iter_bars(bars) -> Iterable[Bar]:
    """Yield :class:`Bar` objects from a list of bars or an OHLCV DataFrame
    (the index is used as the bar time)."""
    if isinstance(bars, pd.DataFrame):
        frame = bars
        columns = {c.lower(): c for c in frame.columns}
        missing = [c for c in ("open", "high", "low", "close") if c not in columns]
        if missing:
            raise ValueError(f"bars DataFrame is missing columns {missing}")
        volume = frame[columns["volume"]].tolist() if "volume" in columns else [None] * len(frame)
        for when, o, h, l, c, v in zip(
            frame.index,
            frame[columns["open"]].tolist(),
            frame[columns["high"]].tolist(),
            frame[columns["low"]].tolist(),
            frame[columns["close"]].tolist(),
            volume,
        ):
            yield to_bar({"o": o, "h": h, "l": l, "c": c, "v": v}, time=when)
        return
    for bar in bars:
        yield to_bar(bar)


def _copy_state(state):
    """Staged copy of committed state for a live or historical update.

    A pickle round trip copies plain data (dicts, lists, deques, numbers and
    the primitives) several times faster than ``copy.deepcopy``. State that
    cannot be pickled (a lambda, a local class) falls back to ``deepcopy``,
    so the result is the same either way."""
    try:
        return pickle.loads(pickle.dumps(state, pickle.HIGHEST_PROTOCOL))
    except Exception:  # noqa: BLE001 - any pickling problem: use the general copier
        return copy.deepcopy(state)


def _clean(value):
    """Outputs are floats or None. NaN/inf (and numpy scalars) are normalized."""
    if value is None:
        return None
    number = float(value)
    return number if math.isfinite(number) else None


class Indicator:
    """Base class for Indicator v2. See the module docstring for the contract."""

    key: str = ""
    version: int = 1
    title: str = ""
    short_title: str = ""
    overlay: bool = True
    inputs: list = []  # list[InputSpec]; replaced by resolved values on instances
    outputs: list = []  # list[OutputSpec]
    levels: list = []  # list[Level]; override levels_for() when levels depend on inputs

    def __init__(self, **inputs):
        self.reset(inputs)

    # ---- declaration -----------------------------------------------------

    @classmethod
    def levels_for(cls, inputs: dict) -> list:
        """Levels for resolved ``inputs``. Override when levels depend on inputs."""
        return list(cls.levels)

    @classmethod
    def describe(cls, inputs: dict | None = None) -> dict:
        """JSON-safe schema for UIs and AI agents."""
        resolved = cls.resolve_inputs(inputs or {})
        cls.check_outputs()
        return {
            "key": cls.key,
            "version": cls.version,
            "title": cls.title or cls.key,
            "short_title": cls.short_title or cls.title or cls.key,
            "overlay": bool(cls.overlay),
            "inputs": [spec.describe() for spec in _class_inputs(cls)],
            "outputs": [spec.describe() for spec in cls.outputs],
            "levels": [level.describe() for level in cls.levels_for(resolved)],
        }

    @classmethod
    def check_outputs(cls) -> None:
        """Output ids are unique and every ``color_by``, ``between`` and
        ``candle`` reference names a declared output."""
        ids = [spec.id for spec in cls.outputs]
        dupes = sorted({item for item in ids if ids.count(item) > 1})
        if dupes:
            raise ValueError(f"{cls.key or cls.__name__} has duplicate output ids {dupes}")
        for spec in cls.outputs:
            missing = [ref for ref in spec.references() if ref not in ids]
            if missing:
                raise ValueError(f"{cls.key or cls.__name__} output {spec.id!r} refers to unknown output(s) {missing}")

    @classmethod
    def resolve_inputs(cls, inputs: dict | None) -> dict:
        """Validate ``inputs`` against the schema and fill defaults."""
        inputs = dict(inputs or {})
        specs = {spec.name: spec for spec in _class_inputs(cls)}
        unknown = sorted(set(inputs) - set(specs))
        if unknown:
            raise InvalidIndicatorInputError(
                f"{cls.key or cls.__name__} has no input(s) {unknown}; valid inputs: {sorted(specs)}"
            )
        resolved = {}
        for name, spec in specs.items():
            resolved[name] = spec.validate(inputs[name] if name in inputs else spec.default)
        cls.validate_inputs(resolved)
        return resolved

    @classmethod
    def validate_inputs(cls, inputs: dict) -> None:
        """Hook for cross-input checks. Raise InvalidIndicatorInputError."""

    # ---- lifecycle -------------------------------------------------------

    def reset(self, inputs: dict | None = None) -> None:
        """Validate inputs and clear all state."""
        type(self).check_outputs()
        self.inputs = self.resolve_inputs(inputs)
        self._state = self.init_state()
        self._last_time = None
        self._bars_seen = 0

    def init_state(self):
        """Return the fresh state object for the current ``self.inputs``."""
        return {}

    def step(self, state, bar: Bar) -> dict:
        """Advance ``state`` by one bar and return this bar's outputs."""
        raise NotImplementedError

    def current_levels(self) -> list:
        """Levels for this instance's inputs, as JSON-safe dicts."""
        return [level.describe() for level in self.levels_for(self.inputs)]

    @property
    def bars_seen(self) -> int:
        """Number of closed bars committed so far."""
        return self._bars_seen

    def update(self, bar, closed: bool = True) -> dict:
        """Process one bar. ``closed=False`` evaluates a forming bar without
        advancing state; ``closed=True`` commits only after calculation and
        output validation succeed. A failed update leaves committed state
        unchanged, so the same bar can be retried."""
        bar = to_bar(bar)
        if bar.time is not None and self._last_time is not None and bar.time <= self._last_time:
            raise ValueError(
                f"bar time {bar.time} must be after the last closed bar time {self._last_time}"
            )
        state = _copy_state(self._state)
        values = self.step(state, bar) or {}
        result = {spec.id: _clean(values.get(spec.id)) for spec in self.outputs}
        if closed:
            self._state = state
            self._bars_seen += 1
            if bar.time is not None:
                self._last_time = bar.time
        return result

    def feed(self, bars) -> dict:
        """Commit many closed bars to this instance and return their outputs as series."""
        out = {spec.id: [] for spec in self.outputs}
        for bar in iter_bars(bars):
            for output_id, value in self.update(bar, closed=True).items():
                out[output_id].append(value)
        return out

    def compute(self, bars) -> dict:
        """Full series for ``bars`` on a fresh instance (this instance's live
        state is untouched). Equal to looping ``update(bar, closed=True)``."""
        return type(self)(**self.inputs).feed(bars)

    def snapshot(self) -> dict:
        """Committed state for hand-off; picklable plain data."""
        return {
            "key": self.key,
            "version": self.version,
            "inputs": dict(self.inputs),
            "state": copy.deepcopy(self._state),
            "last_time": self._last_time,
            "bars_seen": self._bars_seen,
        }

    def restore(self, snapshot: dict) -> None:
        """Load a snapshot taken from the same key, version and inputs."""
        if snapshot.get("key") != self.key or snapshot.get("version") != self.version:
            raise ValueError(
                f"snapshot is for {snapshot.get('key')!r} v{snapshot.get('version')}, "
                f"not {self.key!r} v{self.version}"
            )
        if self.resolve_inputs(snapshot.get("inputs")) != self.inputs:
            raise ValueError("snapshot inputs differ from this indicator's inputs; reset() first")
        self._state = copy.deepcopy(snapshot["state"])
        self._last_time = snapshot.get("last_time")
        self._bars_seen = int(snapshot.get("bars_seen", 0))

    def __repr__(self) -> str:
        return f"{type(self).__name__}(key={self.key!r}, version={self.version}, inputs={self.inputs!r})"


def _class_inputs(cls) -> list:
    """The declared InputSpec list. Instances shadow ``inputs`` with resolved
    values, so always read the class attribute through the MRO."""
    for klass in cls.__mro__:
        if "inputs" in klass.__dict__:
            value = klass.__dict__["inputs"]
            return list(value)
    return []


__all__ = [
    "Bar",
    "DISPLAY_MODES",
    "INPUT_TYPES",
    "Indicator",
    "InputSpec",
    "InvalidIndicatorInputError",
    "LINE_STYLES",
    "Level",
    "OUTPUT_KINDS",
    "OutputSpec",
    "PANES",
    "SHAPES",
    "SHAPE_LOCATIONS",
    "SHAPE_SIZES",
    "SOURCES",
    "THEME_COLORS",
    "iter_bars",
    "to_bar",
]
