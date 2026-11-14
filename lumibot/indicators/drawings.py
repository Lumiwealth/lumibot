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

DRAWING_KINDS = ("box", "line", "label", "table")
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
#: ``none`` draws the text alone, without a bubble.
LABEL_STYLES = ("label_up", "label_down", "label_left", "label_right", "label_center", "none")
_THEME_COLORS = ("chart.fg", "chart.bg")
_PREFIX = {"box": "b", "line": "l", "label": "t", "table": "g"}
_KIND_CAPS = {"box": MAX_DRAWINGS_PER_KIND, "line": MAX_DRAWINGS_PER_KIND, "label": MAX_DRAWINGS_PER_KIND, "table": MAX_TABLES}
#: Fields that are fixed when the object is created.
_LOCKED = {"table": ("columns", "rows")}

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
    },
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

        Style: ``style`` (``label_up``, ``label_down``, ``label_left``,
        ``label_right``, ``label_center``, ``none``), ``color`` (bubble),
        ``text_color`` and ``size``. Returns the object's id."""
        return self._add("label", {**self._style("label", style), "x": x, "y": y, "text": text})

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
    "MAX_TABLES",
    "MAX_TABLE_COLUMNS",
    "MAX_TABLE_ROWS",
    "MAX_TEXT_LENGTH",
    "TABLE_POSITIONS",
]
