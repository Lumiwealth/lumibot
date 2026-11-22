"""Drawing objects: boxes, lines and labels an indicator anchors to earlier bars.

Per-bar outputs stay numbers. Objects that sit at a time and a price (Pine's
``box.new``, ``line.new`` and ``label.new``) are a second, JSON-safe channel:
``self.draw`` while ``step`` runs, ``indicator.drawings()`` to read them. They
follow the same forming-bar rollback as state, and each kind is capped at 500
objects, oldest dropped first.
"""

from __future__ import annotations

import json
import pickle

import pandas as pd
import pytest

from lumibot.indicators import Drawings, Indicator, OutputSpec
from lumibot.indicators.drawings import MAX_DRAWINGS_PER_KIND

T0 = pd.Timestamp("2024-03-01 14:30", tz="UTC")


def _bar(i, close=100.0):
    return {"t": T0 + pd.Timedelta(minutes=i), "o": close, "h": close + 1, "l": close - 1, "c": close, "v": 10}


class SwingBox(Indicator):
    """Neutral test indicator: draws a box over each new high, extends the newest one."""

    key = "test_swing_box"
    title = "Swing box"
    overlay = True
    outputs = [OutputSpec("close", "Close", "line")]

    def init_state(self):
        return {"box": None, "high": None}

    def step(self, state, bar):
        if state["high"] is None or bar.high > state["high"]:
            state["high"] = bar.high
            state["box"] = self.draw.box(
                bar.time, bar.high, bar.time, bar.low, border_color="#26A69A", bg_color="#26A69A33", text="High"
            )
            self.draw.line(bar.time, bar.high, bar.time, bar.high, color="#EF5350", style="dashed", extend="right")
            self.draw.label(bar.time, bar.high, "H", style="label_down", color="#EF5350")
        elif state["box"] is not None:
            self.draw.update(state["box"], x2=bar.time)
        return {"close": bar.close}


class TestDrawingsBoard:
    def test_box_line_label_have_json_safe_fields_and_epoch_ms_times(self):
        board = Drawings()
        box_id = board.box(T0, 105.5, T0 + pd.Timedelta(minutes=5), 99.5, border_color="#112233", text="Zone")
        line_id = board.line(T0, 100, T0 + pd.Timedelta(minutes=2), 101, color="#FF0000", width=2, arrow="right")
        label_id = board.label(T0, 103, "Hi", style="label_up")
        items = {item["id"]: item for item in board.to_list()}
        assert items[box_id]["kind"] == "box"
        assert items[box_id]["x1"] == int(T0.timestamp() * 1000)
        assert items[box_id]["x2"] == int((T0 + pd.Timedelta(minutes=5)).timestamp() * 1000)
        assert items[box_id]["y1"] == 105.5 and items[box_id]["y2"] == 99.5
        assert items[box_id]["text"] == "Zone" and items[box_id]["border_color"] == "#112233"
        assert items[line_id]["kind"] == "line" and items[line_id]["arrow"] == "right"
        assert items[label_id]["kind"] == "label" and items[label_id]["text"] == "Hi"
        json.dumps(board.to_list())  # plain data only

    def test_ids_are_unique_and_list_is_in_creation_order(self):
        board = Drawings()
        ids = [board.label(T0, 1, str(i)) for i in range(5)] + [board.box(T0, 2, T0, 1)]
        assert len(set(ids)) == 6
        assert [item["id"] for item in board.to_list()] == ids

    def test_epoch_millisecond_numbers_and_iso_strings_are_accepted_as_times(self):
        board = Drawings()
        ms = int(T0.timestamp() * 1000)
        a = board.label(ms, 1, "a")
        b = board.label("2024-03-01T14:30:00Z", 1, "b")
        items = {item["id"]: item for item in board.to_list()}
        assert items[a]["x"] == ms == items[b]["x"]

    @pytest.mark.parametrize(
        "call",
        [
            lambda b: b.box(T0, float("nan"), T0, 1),
            lambda b: b.line(T0, 1, None, 1),
            lambda b: b.label(T0, 1, "x", style="sparkle"),
            lambda b: b.label(T0, 1, "x", color="red"),
            lambda b: b.box(T0, 2, T0, 1, border_style="wavy"),
            lambda b: b.line(T0, 1, T0, 2, width=0),
            lambda b: b.line(T0, 1, T0, 2, extend="up"),
            lambda b: b.label(T0, 1, "x" * 500),
            lambda b: b.label(T0, 1, "bad\x00text"),
            lambda b: b.label(T0, 1, "x", size="gigantic"),
            lambda b: b.box(True, 2, T0, 1),
        ],
    )
    def test_wrong_values_are_rejected_with_a_plain_error(self, call):
        with pytest.raises(ValueError):
            call(Drawings())

    def test_update_changes_only_the_named_fields_and_keeps_the_object_in_place(self):
        board = Drawings()
        first = board.box(T0, 2, T0, 1)
        second = board.box(T0, 4, T0, 3)
        later = T0 + pd.Timedelta(minutes=9)
        assert board.update(first, x2=later, bg_color="#00FF0033") is True
        items = board.to_list()
        assert [item["id"] for item in items] == [first, second]
        assert items[0]["x2"] == int(later.timestamp() * 1000) and items[0]["bg_color"] == "#00FF0033"
        assert items[0]["y1"] == 2

    def test_update_rejects_unknown_fields_and_bad_values(self):
        board = Drawings()
        box = board.box(T0, 2, T0, 1)
        with pytest.raises(ValueError):
            board.update(box, nonsense=1)
        with pytest.raises(ValueError):
            board.update(box, border_color="blue")
        with pytest.raises(ValueError):
            board.update(box, x=T0)  # a box has x1 and x2, not x

    def test_update_or_delete_of_a_missing_object_is_a_quiet_no_op(self):
        board = Drawings()
        assert board.update("b99", x2=T0) is False
        assert board.delete("b99") is False

    def test_delete_removes_one_object(self):
        board = Drawings()
        a = board.label(T0, 1, "a")
        b = board.label(T0, 1, "b")
        assert board.delete(a) is True
        assert [item["id"] for item in board.to_list()] == [b]

    def test_each_kind_keeps_the_newest_500_and_drops_the_oldest(self):
        board = Drawings()
        boxes = [board.box(T0, 2, T0, 1, text=str(i)) for i in range(MAX_DRAWINGS_PER_KIND + 7)]
        label = board.label(T0, 1, "stays")
        kept = [item for item in board.to_list() if item["kind"] == "box"]
        assert len(kept) == MAX_DRAWINGS_PER_KIND == 500
        assert kept[0]["id"] == boxes[7] and kept[-1]["id"] == boxes[-1]
        assert board.update(boxes[0], text="gone") is False  # dropped by the cap
        assert any(item["id"] == label for item in board.to_list())

    def test_a_cap_in_one_kind_never_drops_another_kind(self):
        board = Drawings()
        for i in range(600):
            board.line(T0, 1, T0, 2)
        for i in range(10):
            board.label(T0, 1, "l")
        kinds = [item["kind"] for item in board.to_list()]
        assert kinds.count("line") == 500 and kinds.count("label") == 10

    def test_fork_is_independent_of_the_original(self):
        board = Drawings()
        keep = board.box(T0, 2, T0, 1, text="orig")
        staged = board.fork()
        staged.update(keep, text="changed")
        staged.label(T0, 1, "new")
        staged.delete(keep)
        assert [item["text"] for item in board.to_list()] == ["orig"]

    def test_snapshot_round_trips_and_is_picklable(self):
        board = Drawings()
        board.box(T0, 2, T0, 1)
        board.label(T0, 1, "x")
        data = pickle.loads(pickle.dumps(board.snapshot()))
        again = Drawings.from_snapshot(data)
        assert again.to_list() == board.to_list()
        again.label(T0, 1, "next")  # ids keep counting, never reused
        assert len({item["id"] for item in again.to_list()}) == 3


class TestIndicatorDrawings:
    def test_drawings_follow_bars_and_are_empty_before_any_bar(self):
        indicator = SwingBox()
        assert indicator.drawings() == []
        indicator.update(_bar(0), closed=True)
        kinds = sorted(item["kind"] for item in indicator.drawings())
        assert kinds == ["box", "label", "line"]

    def test_closed_bars_commit_drawings_and_later_bars_update_them(self):
        indicator = SwingBox()
        indicator.update(_bar(0, 100), closed=True)
        indicator.update(_bar(1, 99), closed=True)  # lower high: extends the box
        box = [item for item in indicator.drawings() if item["kind"] == "box"][0]
        assert box["x2"] == int(_bar(1)["t"].timestamp() * 1000)

    def test_forming_bar_shows_its_drawings_but_never_commits_them(self):
        indicator = SwingBox()
        indicator.update(_bar(0, 100), closed=True)
        committed = indicator.drawings()
        indicator.update(_bar(1, 120), closed=False)  # a new high while forming
        assert len([i for i in indicator.drawings() if i["kind"] == "box"]) == 2
        indicator.update(_bar(1, 99), closed=False)  # forming bar changed its mind
        assert len([i for i in indicator.drawings() if i["kind"] == "box"]) == 1
        indicator.update(_bar(1, 99), closed=True)
        assert [i["id"] for i in indicator.drawings() if i["kind"] == "box"] == [
            i["id"] for i in committed if i["kind"] == "box"
        ]

    def test_ids_do_not_depend_on_how_many_forming_updates_happened(self):
        quiet, noisy = SwingBox(), SwingBox()
        quiet.update(_bar(0, 100), closed=True)
        for _ in range(4):
            noisy.update(_bar(0, 100), closed=False)
        noisy.update(_bar(0, 100), closed=True)
        assert quiet.drawings() == noisy.drawings()

    def test_a_failing_step_leaves_drawings_unchanged(self):
        class Breaks(SwingBox):
            key = "test_breaks"

            def step(self, state, bar):
                values = super().step(state, bar)
                if bar.close < 0:
                    raise RuntimeError("boom")
                return values

        indicator = Breaks()
        indicator.update(_bar(0, 100), closed=True)
        before = indicator.drawings()
        with pytest.raises(RuntimeError):
            indicator.update(_bar(1, -5), closed=True)
        assert indicator.drawings() == before
        assert indicator.bars_seen == 1

    def test_feed_then_update_matches_one_streaming_pass(self):
        bars = [_bar(i, 100 + (i % 7)) for i in range(40)]
        a = SwingBox()
        a.feed(bars)
        b = SwingBox()
        for bar in bars:
            b.update(bar, closed=True)
        assert a.drawings() == b.drawings()

    def test_snapshot_and_restore_carry_drawings(self):
        a = SwingBox()
        for i in range(10):
            a.update(_bar(i, 100 + i % 3), closed=True)
        snapshot = pickle.loads(pickle.dumps(a.snapshot()))
        b = SwingBox()
        b.restore(snapshot)
        assert b.drawings() == a.drawings()
        a.update(_bar(10, 120), closed=True)
        b.update(_bar(10, 120), closed=True)
        assert a.drawings() == b.drawings()

    def test_reset_clears_drawings(self):
        indicator = SwingBox()
        indicator.update(_bar(0), closed=True)
        indicator.reset()
        assert indicator.drawings() == []

    def test_an_indicator_with_no_drawings_costs_nothing_and_returns_an_empty_list(self):
        class Plain(Indicator):
            key = "test_plain"
            outputs = [OutputSpec("v", "V")]

            def step(self, state, bar):
                return {"v": bar.close}

        indicator = Plain()
        indicator.update(_bar(0), closed=True)
        assert indicator.drawings() == []
        assert "drawings" in indicator.snapshot()

    def test_drawings_need_bar_times(self):
        class Anchored(Indicator):
            key = "test_anchored"
            outputs = [OutputSpec("v", "V")]

            def step(self, state, bar):
                self.draw.label(bar.time, bar.close, "x")
                return {"v": bar.close}

        with pytest.raises(ValueError):
            Anchored().update({"o": 1, "h": 2, "l": 0, "c": 1}, closed=True)

    def test_describe_is_unchanged_by_drawings(self):
        assert "drawings" not in SwingBox.describe()


class TestRunnerCarriesDrawings:
    def test_runner_snapshot_and_reader_include_drawings(self):
        from lumibot.indicators import IndicatorRunner

        runner = IndicatorRunner(None, None)
        instance_id = runner.add(SwingBox())
        for i in range(3):
            runner.on_bar(_bar(i, 100 + i), closed=True)
        assert runner.drawings(instance_id) == runner.instance(instance_id).drawings() != []
        assert runner.snapshot()["indicators"][instance_id]["drawings"] == runner.drawings(instance_id)


class TestTables:
    def test_a_table_holds_cells_and_lists_them_in_row_order(self):
        board = Drawings()
        table = board.table("top_right", 2, 2, bg_color="#00000080", frame_color="#FFFFFF", frame_width=1)
        assert board.cell(table, 1, 0, "Trend", text_color="#26A69A")
        assert board.cell(table, 0, 0, "Mode", bg_color="#2962FF")
        assert board.cell(table, 0, 1, "Up")
        item = board.to_list()[0]
        assert item["kind"] == "table" and item["position"] == "top_right" and item["columns"] == 2
        order = [(c["column"], c["row"], c["text"]) for c in item["cells"]]
        assert order == [(0, 0, "Mode"), (1, 0, "Trend"), (0, 1, "Up")]
        assert item["cells"][0]["bg_color"] == "#2962FF" and item["cells"][1]["text_color"] == "#26A69A"
        json.dumps(board.to_list())

    def test_a_cell_can_be_set_again_and_a_table_restyled(self):
        board = Drawings()
        table = board.table("bottom_left", 1, 1)
        board.cell(table, 0, 0, "a")
        board.cell(table, 0, 0, "b")
        assert board.update(table, bg_color="#112233", position="top_left") is True
        item = board.to_list()[0]
        assert [c["text"] for c in item["cells"]] == ["b"]
        assert item["bg_color"] == "#112233" and item["position"] == "top_left"

    def test_size_is_fixed_and_cells_must_fit(self):
        board = Drawings()
        table = board.table("top_right", 2, 1)
        with pytest.raises(ValueError):
            board.update(table, columns=3)
        with pytest.raises(ValueError):
            board.cell(table, 2, 0, "x")
        with pytest.raises(ValueError):
            board.cell(table, 0, 1, "x")
        with pytest.raises(ValueError):
            board.cell(table, 0, 0, "x", text_color="red")
        with pytest.raises(ValueError):
            board.table("nowhere", 1, 1)
        with pytest.raises(ValueError):
            board.table("top_left", 0, 1)
        assert board.cell("g99", 0, 0, "x") is False

    def test_at_most_nine_tables_oldest_dropped(self):
        board = Drawings()
        ids = [board.table("top_left", 1, 1) for _ in range(11)]
        kept = [item["id"] for item in board.to_list() if item["kind"] == "table"]
        assert kept == ids[2:]

    def test_tables_follow_rollback_snapshot_and_fork(self):
        class Info(Indicator):
            key = "test_info"
            outputs = [OutputSpec("v", "V")]

            def init_state(self):
                return {"table": None}

            def step(self, state, bar):
                if state["table"] is None:
                    state["table"] = self.draw.table("top_right", 1, 1)
                self.draw.cell(state["table"], 0, 0, f"{bar.close:.1f}")
                return {"v": bar.close}

        indicator = Info()
        indicator.update(_bar(0, 100), closed=True)
        indicator.update(_bar(1, 105), closed=False)
        assert indicator.drawings()[0]["cells"][0]["text"] == "105.0"
        indicator.update(_bar(1, 101), closed=True)
        snapshot = pickle.loads(pickle.dumps(indicator.snapshot()))
        other = Info()
        other.restore(snapshot)
        assert other.drawings() == indicator.drawings()
        assert indicator.drawings()[0]["cells"][0]["text"] == "101.0"


# ---- Pine / TradingView drawing vocabulary (Nov 20) ---------------------------------------------

from lumibot.indicators.drawings import LABEL_STYLES, MAX_POLYLINE_POINTS  # noqa: E402

T1 = T0 + pd.Timedelta(minutes=5)


def test_label_takes_every_pine_label_style_tooltip_and_text_align():
    board = Drawings()
    for style in (
        "circle", "square", "diamond", "triangle_up", "triangle_down", "arrow_up", "arrow_down",
        "cross", "xcross", "flag", "label_upper_left", "label_upper_right", "label_lower_left",
        "label_lower_right", "text_outline",
    ):
        assert style in LABEL_STYLES
        board.label(T0, 10.0, "x", style=style)
    pin = board.label(T0, 10.0, "", style="circle", tooltip="Entry: 101.25", text_align="left")
    item = board.get(pin)
    assert item["tooltip"] == "Entry: 101.25" and item["text_align"] == "left"
    with pytest.raises(ValueError):
        board.label(T0, 10.0, "x", tooltip="bad\x01")
    with pytest.raises(ValueError):
        board.label(T0, 10.0, "x", style="balloon")


def test_polyline_points_curved_closed_fill():
    board = Drawings()
    pid = board.polyline([(T0, 1.0), (T1, 2.0), (T1 + pd.Timedelta(minutes=1), 1.5)], curved=True, closed=True,
                         color="#FF0000", width=2, style="dashed", fill_color="#FF000033")
    item = board.get(pid)
    assert item["kind"] == "polyline" and pid.startswith("p")
    assert item["points"][0] == {"x": int(T0.value // 1_000_000), "y": 1.0}
    assert item["curved"] is True and item["closed"] is True and item["fill_color"] == "#FF000033"
    json.dumps(board.to_list())
    with pytest.raises(ValueError):
        board.polyline([(T0, 1.0)])
    with pytest.raises(ValueError):
        board.polyline([(T0, float(i)) for i in range(MAX_POLYLINE_POINTS + 1)])
    with pytest.raises(ValueError):
        board.polyline([(T0, 1.0), (T1, float("nan"))])
    assert board.update(pid, color="#00FF00")
    restored = Drawings.from_snapshot(json.loads(json.dumps(board.snapshot())))
    assert restored.get(pid) == board.get(pid)


def test_linefill_between_two_lines_and_dropped_with_them():
    board = Drawings()
    a = board.line(T0, 1.0, T1, 2.0)
    b = board.line(T0, 0.0, T1, 1.0)
    fid = board.linefill(a, b, color="#2962FF33")
    assert board.get(fid) == {"id": fid, "kind": "linefill", "line1": a, "line2": b, "color": "#2962FF33"}
    with pytest.raises(ValueError):
        board.linefill(a, "l999")
    board.delete(a)
    assert board.get(fid) is None  # Pine: a linefill goes with its lines


def test_line_family_helpers_compile_to_lines_and_labels():
    board = Drawings()
    assert board.get(board.ray(T0, 1.0, T1, 2.0))["extend"] == "right"
    assert board.get(board.extended_line(T0, 1.0, T1, 2.0))["extend"] == "both"
    h = board.get(board.horizontal_line(5.0, T0))
    assert h["y1"] == h["y2"] == 5.0 and h["extend"] == "both"
    hr = board.get(board.horizontal_ray(T0, 5.0))
    assert hr["y1"] == hr["y2"] == 5.0 and hr["extend"] == "right"
    v = board.get(board.vertical_line(T1))
    assert v["x1"] == v["x2"] and v["extend"] == "both"
    vid, hid = board.crossline(T1, 7.0)
    assert board.get(vid)["x1"] == board.get(vid)["x2"] and board.get(hid)["y1"] == 7.0
    line_id, label_id = board.trend_angle(T0, 100.0, T1, 110.0)
    assert board.get(line_id)["kind"] == "line" and "°" in board.get(label_id)["text"]
    line_id, label_id = board.info_line(T0, 100.0, T1, 110.0)
    assert "+10" in board.get(label_id)["text"] and "+10.00%" in board.get(label_id)["text"]
    assert board.get(board.text(T0, 1.0, "hi"))["style"] == "none"
    assert board.get(board.callout(T0, 1.0, "hi"))["style"] == "label_down"
    assert board.get(board.note(T0, 1.0, "hi", tooltip="more"))["tooltip"] == "more"
    pin = board.get(board.pin(T0, 1.0, "why"))
    assert pin["style"] == "circle" and pin["tooltip"] == "why"
    assert board.get(board.price_label(T0, 1234.5))["text"] == "1,234.50"
    note_line, note_label = board.price_note(T0, 100.0, T1)
    assert board.get(note_line)["kind"] == "line" and board.get(note_label)["text"] == "100.00"
    assert board.get(board.flag_mark(T0, 1.0))["style"] == "flag"
    post, sign = board.signpost(T0, 1.0, "Earnings")
    assert board.get(post)["x1"] == board.get(post)["x2"] and board.get(sign)["text"] == "Earnings"


# ---- Text formatting, vertical alignment, wrapping and table merges (Nov 20) --------------------

from lumibot.indicators.drawings import DRAWING_TEXT_FORMATS, DRAWING_TEXT_VALIGNS, DRAWING_TEXT_WRAPS  # noqa: E402


def test_box_text_valign_wrap_and_formatting():
    board = Drawings()
    plain = board.get(board.box(T0, 2.0, T1, 1.0, text="Zone"))
    assert plain["text_valign"] == "center" and plain["text_wrap"] == "none" and plain["text_formatting"] == "none"
    assert DRAWING_TEXT_VALIGNS == ("top", "center", "bottom")
    assert DRAWING_TEXT_WRAPS == ("none", "auto")
    assert DRAWING_TEXT_FORMATS == ("none", "bold", "italic", "bold_italic")
    box_id = board.box(T0, 2.0, T1, 1.0, text="Zone", text_valign="top", text_wrap="auto", text_formatting="bold")
    item = board.get(box_id)
    assert (item["text_valign"], item["text_wrap"], item["text_formatting"]) == ("top", "auto", "bold")
    assert board.update(box_id, text_valign="bottom", text_wrap="none", text_formatting="bold_italic")
    item = board.get(box_id)
    assert (item["text_valign"], item["text_wrap"], item["text_formatting"]) == ("bottom", "none", "bold_italic")
    for bad in ({"text_valign": "middle"}, {"text_wrap": "yes"}, {"text_formatting": "underline"}):
        with pytest.raises(ValueError):
            board.box(T0, 2.0, T1, 1.0, **bad)
        with pytest.raises(ValueError):
            board.update(box_id, **bad)
    restored = Drawings.from_snapshot(json.loads(json.dumps(board.snapshot())))
    assert restored.to_list() == board.to_list()


def test_label_text_formatting():
    board = Drawings()
    assert board.get(board.label(T0, 1.0, "a"))["text_formatting"] == "none"
    label_id = board.label(T0, 1.0, "B", text_formatting="italic")
    assert board.get(label_id)["text_formatting"] == "italic"
    assert board.update(label_id, text_formatting="bold")
    assert board.get(label_id)["text_formatting"] == "bold"
    with pytest.raises(ValueError):
        board.label(T0, 1.0, "a", text_formatting="heavy")
    with pytest.raises(ValueError):
        board.update(label_id, text_formatting="heavy")
    restored = Drawings.from_snapshot(json.loads(json.dumps(board.snapshot())))
    assert restored.get(label_id) == board.get(label_id)


def test_table_cell_valign_tooltip_formatting_and_size():
    board = Drawings()
    table = board.table("bottom_right", 2, 2)
    board.cell(table, 0, 0, "plain")
    board.cell(table, 1, 1, "Trend", text_valign="top", tooltip="Above the 50-bar average",
               text_formatting="bold", width=12.5, height=4)
    cells = board.get(table)["cells"]
    assert cells[0]["text_valign"] == "center" and cells[0]["tooltip"] == ""
    assert cells[0]["text_formatting"] == "none" and cells[0]["width"] == 0 and cells[0]["height"] == 0
    styled = cells[1]
    assert styled["text_valign"] == "top" and styled["tooltip"] == "Above the 50-bar average"
    assert styled["text_formatting"] == "bold" and styled["width"] == 12.5 and styled["height"] == 4
    for bad in ({"width": -1}, {"height": 101}, {"width": True}, {"text_valign": "mid"},
                {"tooltip": "x\x02"}, {"text_formatting": "caps"}):
        with pytest.raises(ValueError):
            board.cell(table, 0, 1, "x", **bad)
    restored = Drawings.from_snapshot(json.loads(json.dumps(board.snapshot())))
    assert restored.get(table) == board.get(table)


def test_table_merge_cells():
    board = Drawings()
    table = board.table("top_right", 3, 3)
    assert board.get(table)["merges"] == []
    assert board.merge_cells(table, 0, 0, 2, 0) is True
    assert board.merge_cells(table, 0, 1, 0, 2) is True
    merges = board.get(table)["merges"]
    assert merges == [
        {"start_column": 0, "start_row": 0, "end_column": 2, "end_row": 0},
        {"start_column": 0, "start_row": 1, "end_column": 0, "end_row": 2},
    ]
    with pytest.raises(ValueError):
        board.merge_cells(table, 1, 0, 1, 1)  # overlaps the first merge
    with pytest.raises(ValueError):
        board.merge_cells(table, 2, 2, 1, 1)  # end before start
    with pytest.raises(ValueError):
        board.merge_cells(table, 1, 1, 3, 1)  # outside the grid
    with pytest.raises(ValueError):
        board.merge_cells(table, 1, 1, 1, 1)  # one cell is not a merge
    assert board.merge_cells("g99", 0, 0, 1, 0) is False
    json.dumps(board.to_list())
    restored = Drawings.from_snapshot(json.loads(json.dumps(board.snapshot())))
    assert restored.get(table) == board.get(table)
    # A restored table keeps its merges and can take more.
    assert restored.merge_cells(table, 1, 2, 2, 2) is True
    # A snapshot from before merges existed still loads.
    old = board.snapshot()
    for item in old["items"]:
        item.pop("merges", None)
    assert Drawings.from_snapshot(old).merge_cells(table, 1, 1, 2, 1) is True


def test_merges_follow_forming_bar_rollback():
    class Merged(Indicator):
        key = "test_merged_table"
        outputs = [OutputSpec("v", "V")]

        def init_state(self):
            return {"table": None}

        def step(self, state, bar):
            if state["table"] is None:
                state["table"] = self.draw.table("top_right", 2, 2)
            if bar.close > 104:
                self.draw.merge_cells(state["table"], 0, 0, 1, 0)
            return {"v": bar.close}

    indicator = Merged()
    indicator.update(_bar(0, 100), closed=True)
    indicator.update(_bar(1, 105), closed=False)
    assert indicator.drawings()[0]["merges"] != []
    indicator.update(_bar(1, 101), closed=True)
    assert indicator.drawings()[0]["merges"] == []
