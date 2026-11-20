"""Drawing objects for indicators: boxes, lines and labels anchored to bar times.

An indicator's per-bar outputs are numbers. Some things a chart indicator
draws are not per-bar numbers: a box over a price zone that began twenty bars
ago, a line between two swing points, a text label on one bar (Pine's
``box.new``, ``line.new`` and ``label.new`` and their ``set_*`` and ``delete``
functions). Those are *drawing objects*.

Inside ``step`` an indicator reaches them through ``self.draw``::

    def step(self, state, bar):
        if state["zone"] is None:
            state["zone"] = self.draw.box(bar.time, bar.high, bar.time, bar.low,
                                          bg_color="#26A69A33", text="Zone")
        else:
            self.draw.update(state["zone"], x2=bar.time)   # grow it to this bar
        return {}

After a bar, ``indicator.drawings()`` returns the whole current set as plain
JSON-safe dicts. Times are UTC epoch milliseconds, prices are floats, colors
are ``#RRGGBB``, ``#RRGGBBAA`` or the theme colors ``chart.fg`` / ``chart.bg``.

Rules:

* Objects follow the same forming-bar rollback as indicator state: a forming
  bar may create, change or delete objects while it builds, and only the
  closed bar commits them.
* Each kind (box, line, label) keeps at most :data:`MAX_DRAWINGS_PER_KIND`
  (500, TradingView's own ceiling) objects; tables (a grid of text cells pinned to a
  corner of the chart, ``table.new`` / ``table.cell``) keep at most nine. Adding one more drops the oldest of
  that kind.
* ``update`` and ``delete`` of an object that is gone (deleted, or dropped by
  the cap) quietly do nothing, like Pine.
* Text is short, plain text. It is drawn as text, never as markup.
"""

from __future__ import annotations

import math
import numbers
from typing import Any

import pandas as pd

DRAWING_KINDS = ("box", "line", "label", "table", "polyline", "linefill")
#: TradingView's own ceiling for ``max_boxes_count``, ``max_lines_count`` and ``max_labels_count``.
MAX_DRAWINGS_PER_KIND = 500
MAX_TEXT_LENGTH = 80
#: Pine allows at most nine tables, one per screen position.
MAX_TABLES = 9
MAX_TABLE_COLUMNS = 20
MAX_TABLE_ROWS = 40
TABLE_POSITIONS = (
    "top_left",
    "top_center",
    "top_right",
    "middle_left",
    "middle_center",
    "middle_right",
    "bottom_left",
    "bottom_center",
    "bottom_right",
)
DRAWING_LINE_STYLES = ("solid", "dashed", "dotted")
DRAWING_EXTENDS = ("none", "left", "right", "both")
DRAWING_ARROWS = ("none", "left", "right", "both")
DRAWING_TEXT_SIZES = ("tiny", "small", "normal", "large", "huge")
DRAWING_TEXT_ALIGNS = ("left", "center", "right")
#: Pine's ``label.style_*``. ``none`` draws the text alone, without a bubble; ``text_outline``
#: draws the text with an outline in ``color``; the shape styles (``circle`` to ``flag``) draw a
#: small mark at the point with the text beside it (a TradingView pin or flag mark).
LABEL_STYLES = (
    "label_up",
    "label_down",
    "label_left",
    "label_right",
    "label_center",
    "label_upper_left",
    "label_upper_right",
    "label_lower_left",
    "label_lower_right",
    "none",
    "text_outline",
    "circle",
    "square",
    "diamond",
    "triangle_up",
    "triangle_down",
    "arrow_up",
    "arrow_down",
    "cross",
    "xcross",
    "flag",
)
#: Pine's own ceiling for ``polyline.new`` points (10,000) is far more than a chart can draw fast;
#: 500 keeps one object light on the wire.
MAX_POLYLINE_POINTS = 500
_THEME_COLORS = ("chart.fg", "chart.bg")
_PREFIX = {"box": "b", "line": "l", "label": "t", "table": "g", "polyline": "p", "linefill": "f"}
_KIND_CAPS = {
    "box": MAX_DRAWINGS_PER_KIND,
    "line": MAX_DRAWINGS_PER_KIND,
    "label": MAX_DRAWINGS_PER_KIND,
    "table": MAX_TABLES,
    "polyline": 100,
    "linefill": MAX_DRAWINGS_PER_KIND,
}
#: Fields that are fixed when the object is created.
_LOCKED = {"table": ("columns", "rows"), "linefill": ("line1", "line2")}

#: field name -> (check kind, choices); the defaults for each object kind follow.
_FIELDS: dict[str, dict[str, tuple]] = {
    "box": {
        "x1": ("time", None),
        "y1": ("price", None),
        "x2": ("time", None),
        "y2": ("price", None),
        "border_color": ("color", None),
        "border_width": ("width", None),
        "border_style": ("choice", DRAWING_LINE_STYLES),
        "bg_color": ("color_or_none", None),
        "text": ("text", None),
        "text_color": ("color_or_none", None),
        "text_size": ("choice", DRAWING_TEXT_SIZES),
        "text_align": ("choice", DRAWING_TEXT_ALIGNS),
        "extend": ("choice", DRAWING_EXTENDS),
    },
    "line": {
        "x1": ("time", None),
        "y1": ("price", None),
        "x2": ("time", None),
        "y2": ("price", None),
        "color": ("color", None),
        "width": ("width", None),
        "style": ("choice", DRAWING_LINE_STYLES),
        "arrow": ("choice", DRAWING_ARROWS),
        "extend": ("choice", DRAWING_EXTENDS),
    },
    "table": {
        "position": ("choice", TABLE_POSITIONS),
        "columns": ("count", MAX_TABLE_COLUMNS),
        "rows": ("count", MAX_TABLE_ROWS),
        "bg_color": ("color_or_none", None),
        "frame_color": ("color_or_none", None),
        "frame_width": ("width0", None),
        "border_color": ("color_or_none", None),
        "border_width": ("width0", None),
    },
    "label": {
        "x": ("time", None),
        "y": ("price", None),
        "text": ("text", None),
        "style": ("choice", LABEL_STYLES),
        "color": ("color", None),
        "text_color": ("color", None),
        "size": ("choice", DRAWING_TEXT_SIZES),
        "text_align": ("choice", DRAWING_TEXT_ALIGNS),
        "tooltip": ("text", None),
    },
    "polyline": {
        "points": ("points", None),
        "curved": ("bool", None),
        "closed": ("bool", None),
        "color": ("color", None),
        "width": ("width", None),
        "style": ("choice", DRAWING_LINE_STYLES),
        "fill_color": ("color_or_none", None),
    },
    "linefill": {
        "line1": ("ref", None),
        "line2": ("ref", None),
        "color": ("color", None),
    },
}
_DEFAULTS: dict[str, dict[str, Any]] = {
    "box": {
        "border_color": "#2962FF",
        "border_width": 1,
        "border_style": "solid",
        "bg_color": None,
        "text": "",
        "text_color": None,
        "text_size": "small",
        "text_align": "center",
        "extend": "none",
    },
    "line": {"color": "#2962FF", "width": 1, "style": "solid", "arrow": "none", "extend": "none"},
    "table": {
        "position": "top_right",
        "bg_color": None,
        "frame_color": None,
        "frame_width": 0,
        "border_color": None,
        "border_width": 0,
    },
    "label": {
        "text": "",
        "style": "label_down",
        "color": "#2962FF",
        "text_color": "#FFFFFF",
        "size": "small",
        "text_align": "center",
        "tooltip": "",
    },
    "polyline": {"curved": False, "closed": False, "color": "#2962FF", "width": 1, "style": "solid", "fill_color": None},
    "linefill": {"color": "#2962FF33"},
}


def _is_color(value) -> bool:
    if not isinstance(value, str):
        return False
    if value in _THEME_COLORS:
        return True
    if not value.startswith("#") or len(value) not in (7, 9):
        return False
    try:
        int(value[1:], 16)
    except ValueError:
        return False
    return True


def _to_ms(value, what: str) -> int:
    """UTC epoch milliseconds from a bar time, a datetime, an ISO string or a number of milliseconds."""
    if value is None or isinstance(value, bool):
        raise ValueError(f"{what} must be a bar time (bar.time), got {value!r}")
    if isinstance(value, numbers.Real):
        number = float(value)
        if not math.isfinite(number):
            raise ValueError(f"{what} must be a finite time, got {value!r}")
        return int(number)
    try:
        stamp = pd.Timestamp(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{what} must be a bar time (bar.time), got {value!r}") from error
    if stamp is pd.NaT:
        raise ValueError(f"{what} must be a bar time (bar.time), got {value!r}")
    if stamp.tzinfo is None:
        stamp = stamp.tz_localize("UTC")
    return int(stamp.tz_convert("UTC").value // 1_000_000)


def _check(kind: str, name: str, value, how: str, choices):
    what = f"{kind} {name}"
    if how == "time":
        return _to_ms(value, what)
    if how == "price":
        if isinstance(value, bool) or not isinstance(value, numbers.Real) or not math.isfinite(float(value)):
            raise ValueError(f"{what} must be a finite price, got {value!r}")
        return float(value)
    if how == "width":
        if isinstance(value, bool) or not isinstance(value, numbers.Real) or not 0 < float(value) <= 10:
            raise ValueError(f"{what} must be above 0 and at most 10, got {value!r}")
        return float(value) if float(value) != int(value) else int(value)
    if how == "width0":
        if isinstance(value, bool) or not isinstance(value, numbers.Real) or not 0 <= float(value) <= 10:
            raise ValueError(f"{what} must be from 0 to 10, got {value!r}")
        return float(value) if float(value) != int(value) else int(value)
    if how == "count":
        if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= choices:
            raise ValueError(f"{what} must be a whole number from 1 to {choices}, got {value!r}")
        return value
    if how in ("color", "color_or_none"):
        if value is None and how == "color_or_none":
            return None
        if not _is_color(value):
            raise ValueError(f"{what} must be #RRGGBB, #RRGGBBAA or one of {_THEME_COLORS}, got {value!r}")
        return value
    if how == "choice":
        if value not in choices:
            raise ValueError(f"{what} must be one of {choices}, got {value!r}")
        return value
    if how == "text":
        if not isinstance(value, str) or len(value) > MAX_TEXT_LENGTH:
            raise ValueError(f"{what} must be text of at most {MAX_TEXT_LENGTH} characters")
        if any(ord(ch) < 32 and ch != "\n" for ch in value) or "\x7f" in value:
            raise ValueError(f"{what} must be plain text without control characters")
        return value
    if how == "bool":
        if not isinstance(value, bool):
            raise ValueError(f"{what} must be True or False, got {value!r}")
        return value
    if how == "ref":
        if not isinstance(value, str) or not value.startswith("l") or not value[1:].isdigit():
            raise ValueError(f"{what} must be the id of a line (from draw.line), got {value!r}")
        return value
    if how == "points":
        try:
            points = list(value)
        except TypeError as error:
            raise ValueError(f"{what} must be a list of (time, price) points") from error
        if not 2 <= len(points) <= MAX_POLYLINE_POINTS:
            raise ValueError(f"{what} needs from 2 to {MAX_POLYLINE_POINTS} points, got {len(points)}")
        out = []
        for point in points:
            if isinstance(point, dict):
                x, y = point.get("x"), point.get("y")
            else:
                x, y = point
            out.append({"x": _to_ms(x, f"{what} time"), "y": _check(kind, f"{name} price", y, "price", None)})
        return out
    raise ValueError(f"unknown check {how}")  # pragma: no cover - table error


_CELL_FIELDS = {
    "text_color": ("color_or_none", None),
    "bg_color": ("color_or_none", None),
    "text_size": ("choice", DRAWING_TEXT_SIZES),
    "text_align": ("choice", DRAWING_TEXT_ALIGNS),
}
_CELL_DEFAULTS = {"text_color": None, "bg_color": None, "text_size": "small", "text_align": "center"}


def _cell_order(key: str) -> tuple:
    column, row = key.split(",")
    return (int(row), int(column))


class Drawings:
    """The set of drawing objects one indicator currently shows.

    Use it through ``self.draw`` in ``step``. Items are never changed in place,
    so :meth:`fork` (used for forming-bar rollback) is a cheap shallow copy."""

    def __init__(self):
        self._kinds: dict[str, dict[str, dict]] = {kind: {} for kind in DRAWING_KINDS}
        self._next = 1

    # ---- creating ----------------------------------------------------------

    def _add(self, kind: str, values: dict) -> str:
        checked = {name: _check(kind, name, value, *_FIELDS[kind][name]) for name, value in values.items()}
        drawing_id = f"{_PREFIX[kind]}{self._next}"
        self._next += 1
        items = self._kinds[kind]
        items[drawing_id] = {"id": drawing_id, "kind": kind, **checked}
        while len(items) > _KIND_CAPS[kind]:
            del items[next(iter(items))]
        return drawing_id

    def box(self, x1, y1, x2, y2, **style) -> str:
        """A rectangle from (``x1``, ``y1``) to (``x2``, ``y2``): a bar time and a price each.

        Style: ``border_color``, ``border_width``, ``border_style``, ``bg_color``
        (fill, use ``#RRGGBBAA`` for transparency), ``text``, ``text_color``,
        ``text_size``, ``text_align`` and ``extend`` (``none``, ``left``,
        ``right``, ``both``). Returns the object's id."""
        return self._add("box", {"x1": x1, "y1": y1, "x2": x2, "y2": y2, **self._style("box", style)})

    def line(self, x1, y1, x2, y2, **style) -> str:
        """A straight line between two (time, price) points.

        Style: ``color``, ``width``, ``style`` (``solid``, ``dashed``,
        ``dotted``), ``arrow`` (``none``, ``left``, ``right``, ``both``) and
        ``extend``. Returns the object's id."""
        return self._add("line", {"x1": x1, "y1": y1, "x2": x2, "y2": y2, **self._style("line", style)})

    def label(self, x, y, text="", **style) -> str:
        """Text at one (time, price) point, with or without a bubble.

        Style: ``style`` (any of :data:`LABEL_STYLES`: Pine's bubble
        directions, ``none``, ``text_outline`` or a shape mark such as
        ``circle`` or ``flag``), ``color`` (bubble or mark), ``text_color``,
        ``size``, ``text_align`` and ``tooltip`` (hover text). Returns the id."""
        return self._add("label", {**self._style("label", style), "x": x, "y": y, "text": text})

    def polyline(self, points, **style) -> str:
        """A line through many (time, price) points (Pine's ``polyline.new``), 2 to 500 of them.

        Style: ``curved`` (smooth curve through the points), ``closed`` (joins
        the last point to the first), ``color``, ``width``, ``style`` and
        ``fill_color`` (shades the inside of a closed shape). Returns the id."""
        return self._add("polyline", {**self._style("polyline", style), "points": points})

    def linefill(self, line1, line2, color="#2962FF33") -> str:
        """Shade the area between two lines from :meth:`line` (Pine's ``linefill.new``).

        The fill follows the lines when they change and is removed with either
        of them. Returns the id."""
        for name, ref in (("line1", line1), ("line2", line2)):
            if ref not in self._kinds["line"]:
                raise ValueError(f"linefill {name} {ref!r} is not a line on the chart")
        return self._add("linefill", {"line1": line1, "line2": line2, "color": color})

    # ---- TradingView drawing tools, built from lines and labels ------------

    def ray(self, x1, y1, x2, y2, **style) -> str:
        """A line from the first point through the second and on to the right edge."""
        return self.line(x1, y1, x2, y2, **{**style, "extend": "right"})

    def extended_line(self, x1, y1, x2, y2, **style) -> str:
        """A line through two points that runs to both edges of the chart."""
        return self.line(x1, y1, x2, y2, **{**style, "extend": "both"})

    def horizontal_line(self, price, x, **style) -> str:
        """A level across the whole chart at ``price`` (``x`` is any bar time, it only anchors it)."""
        return self.line(x, price, x, price, **{**style, "extend": "both"})

    def horizontal_ray(self, x, price, **style) -> str:
        """A level that starts at bar ``x`` and runs to the right edge."""
        return self.line(x, price, x, price, **{**style, "extend": "right"})

    def vertical_line(self, x, price=0.0, **style) -> str:
        """A line from top to bottom of the pane at bar ``x``."""
        return self.line(x, price, x, price, **{**style, "extend": "both"})

    def crossline(self, x, price, **style) -> tuple[str, str]:
        """A vertical and a horizontal line crossing at (``x``, ``price``). Returns both ids."""
        return self.vertical_line(x, price, **style), self.horizontal_line(price, x, **style)

    def trend_angle(self, x1, y1, x2, y2, label_color=None, **style) -> tuple[str, str]:
        """A trend line with its angle written at the start, like TradingView's Trend Angle.

        TradingView's angle depends on how the chart is zoomed. Here it is a
        fixed measure that reads the same on every screen: the arctangent of the
        percent change, so a 1% rise reads 45° and a flat line 0°.
        Returns (line id, label id)."""
        line_id = self.line(x1, y1, x2, y2, **style)
        change = (float(y2) - float(y1)) / abs(float(y1)) * 100 if float(y1) else 0.0
        degrees = math.degrees(math.atan(change))
        color = label_color or style.get("color", _DEFAULTS["line"]["color"])
        label_id = self.label(x1, y1, f"{degrees:,.1f}°", style="none", color=color)
        return line_id, label_id

    def info_line(self, x1, y1, x2, y2, label_color=None, **style) -> tuple[str, str]:
        """A trend line with its price change and percent written at the end (TradingView's Info Line).
        Returns (line id, label id)."""
        line_id = self.line(x1, y1, x2, y2, **style)
        diff = float(y2) - float(y1)
        pct = diff / abs(float(y1)) * 100 if float(y1) else 0.0
        color = label_color or style.get("color", _DEFAULTS["line"]["color"])
        label_id = self.label(x2, y2, f"{diff:+,.2f} ({pct:+,.2f}%)", style="label_left", color=color)
        return line_id, label_id

    def text(self, x, y, text, **style) -> str:
        """Text alone at a point, no bubble (TradingView's Text tool)."""
        return self.label(x, y, text, **{"style": "none", "text_color": style.pop("color", "#2962FF"), **style})

    def callout(self, x, y, text, **style) -> str:
        """A speech bubble pointing at a point (TradingView's Callout)."""
        return self.label(x, y, text, **{"style": "label_down", **style})

    def note(self, x, y, text, tooltip="", **style) -> str:
        """A text box at a point, with optional longer hover text (TradingView's Note)."""
        return self.label(x, y, text, **{"style": "label_center", "tooltip": tooltip, **style})

    def pin(self, x, y, tooltip, text="", **style) -> str:
        """A small round marker that shows ``tooltip`` on hover (TradingView's Pin)."""
        return self.label(x, y, text, **{"style": "circle", "tooltip": tooltip, "size": "tiny", **style})

    def flag_mark(self, x, y, text="", **style) -> str:
        """A flag at a point (TradingView's Flag Mark)."""
        return self.label(x, y, text, **{"style": "flag", **style})

    def price_label(self, x, price, **style) -> str:
        """A tag showing ``price`` at a point (TradingView's Price Label)."""
        return self.label(x, price, f"{float(price):,.2f}", **{"style": "label_left", **style})

    def price_note(self, x1, price, x2, **style) -> tuple[str, str]:
        """A short level from ``x1`` to ``x2`` at ``price`` with the price written at its end
        (TradingView's Price Note). Returns (line id, label id)."""
        color = style.pop("color", _DEFAULTS["line"]["color"])
        line_id = self.line(x1, price, x2, price, color=color, **style)
        return line_id, self.label(x2, price, f"{float(price):,.2f}", style="label_left", color=color)

    def signpost(self, x, y, text, **style) -> tuple[str, str]:
        """A vertical post at bar ``x`` with a sign reading ``text`` at ``y`` (TradingView's Signpost).
        Returns (line id, label id)."""
        color = style.pop("color", _DEFAULTS["line"]["color"])
        post = self.vertical_line(x, y, color=color, style="dotted")
        return post, self.label(x, y, text, **{"style": "label_down", "color": color, **style})

    def table(self, position="top_right", columns=1, rows=1, **style) -> str:
        """A grid of text cells pinned to a corner or edge of the chart (Pine's ``table.new``).

        ``position`` is one of ``top_left``, ``top_center``, ``top_right``,
        ``middle_left``, ``middle_center``, ``middle_right``, ``bottom_left``,
        ``bottom_center``, ``bottom_right``. Style: ``bg_color``,
        ``frame_color``, ``frame_width``, ``border_color``, ``border_width``.
        Fill cells with :meth:`cell`. At most nine tables. Returns the id."""
        drawing_id = self._add(
            "table", {**self._style("table", style), "position": position, "columns": columns, "rows": rows}
        )
        self._kinds["table"][drawing_id]["cells"] = {}
        return drawing_id

    def cell(self, table_id, column, row, text="", **style) -> bool:
        """Set one cell of a table (Pine's ``table.cell``), counted from 0.
        Style: ``text_color``, ``bg_color``, ``text_size``, ``text_align``.
        Returns ``False`` when the table no longer exists."""
        items = self._kinds["table"]
        item = items.get(table_id)
        if item is None:
            return False
        unknown = sorted(set(style) - set(_CELL_DEFAULTS))
        if unknown:
            raise ValueError(f"a table cell has no option(s) {unknown}; valid options: {sorted(_CELL_DEFAULTS)}")
        for name, value, limit in (("column", column, item["columns"]), ("row", row, item["rows"])):
            if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value < limit:
                raise ValueError(f"table cell {name} must be a whole number from 0 to {limit - 1}, got {value!r}")
        cell = {"column": column, "row": row, "text": _check("table cell", "text", text, "text", None)}
        for name, default in _CELL_DEFAULTS.items():
            cell[name] = _check("table cell", name, style.get(name, default), *_CELL_FIELDS[name])
        cells = dict(item["cells"])
        cells[f"{column},{row}"] = cell
        items[table_id] = {**item, "cells": cells}
        return True

    @staticmethod
    def _style(kind: str, style: dict) -> dict:
        unknown = sorted(set(style) - set(_DEFAULTS[kind]))
        if unknown:
            raise ValueError(f"a {kind} has no option(s) {unknown}; valid options: {sorted(_DEFAULTS[kind])}")
        return {**_DEFAULTS[kind], **style}

    # ---- changing ----------------------------------------------------------

    def _find(self, drawing_id):
        for items in self._kinds.values():
            if drawing_id in items:
                return items
        return None

    def update(self, drawing_id, **fields) -> bool:
        """Change fields of an existing object (Pine's ``set_*``). Returns
        ``False`` when the object no longer exists."""
        items = self._find(drawing_id)
        if items is None:
            return False
        item = items[drawing_id]
        locked = _LOCKED.get(item["kind"], ())
        allowed = {name: how for name, how in _FIELDS[item["kind"]].items() if name not in locked}
        unknown = sorted(set(fields) - set(allowed))
        if unknown:
            raise ValueError(f"a {item['kind']} has no field(s) {unknown}; valid fields: {sorted(allowed)}")
        changed = {name: _check(item["kind"], name, value, *allowed[name]) for name, value in fields.items()}
        items[drawing_id] = {**item, **changed}
        return True

    def delete(self, drawing_id) -> bool:
        """Remove an object (Pine's ``delete``). Returns ``False`` when it is already gone."""
        items = self._find(drawing_id)
        if items is None:
            return False
        del items[drawing_id]
        if items is self._kinds["line"]:
            fills = self._kinds["linefill"]
            for fill_id in [k for k, v in fills.items() if drawing_id in (v["line1"], v["line2"])]:
                del fills[fill_id]
        return True

    # ---- reading -----------------------------------------------------------

    @staticmethod
    def _public(item: dict) -> dict:
        out = dict(item)
        if "cells" in out:
            out["cells"] = [out["cells"][key] for key in sorted(out["cells"], key=_cell_order)]
        return out

    def get(self, drawing_id) -> dict | None:
        items = self._find(drawing_id)
        return self._public(items[drawing_id]) if items is not None else None

    def to_list(self) -> list[dict]:
        """Every object, oldest first, as JSON-safe dicts."""
        every = [item for items in self._kinds.values() for item in items.values()]
        every.sort(key=lambda item: int(item["id"][1:]))
        return [self._public(item) for item in every]

    def __len__(self) -> int:
        return sum(len(items) for items in self._kinds.values())

    # ---- rollback and hand-off ---------------------------------------------

    def fork(self) -> Drawings:
        """A staged copy. Items are shared (they are never changed in place)."""
        other = Drawings()
        other._kinds = {kind: dict(items) for kind, items in self._kinds.items()}
        other._next = self._next
        return other

    def snapshot(self) -> dict:
        return {"next": self._next, "items": self.to_list()}

    @classmethod
    def from_snapshot(cls, data) -> Drawings:
        board = cls()
        if not data:
            return board
        board._next = int(data.get("next", 1))
        for item in data.get("items", []):
            restored = dict(item)
            if restored["kind"] == "table":
                restored["cells"] = {f"{c['column']},{c['row']}": dict(c) for c in restored.get("cells", [])}
            board._kinds[restored["kind"]][restored["id"]] = restored
        return board


__all__ = [
    "DRAWING_ARROWS",
    "DRAWING_EXTENDS",
    "DRAWING_KINDS",
    "DRAWING_LINE_STYLES",
    "DRAWING_TEXT_ALIGNS",
    "DRAWING_TEXT_SIZES",
    "Drawings",
    "LABEL_STYLES",
    "MAX_DRAWINGS_PER_KIND",
    "MAX_POLYLINE_POINTS",
    "MAX_TABLES",
    "MAX_TABLE_COLUMNS",
    "MAX_TABLE_ROWS",
    "MAX_TEXT_LENGTH",
    "TABLE_POSITIONS",
]
