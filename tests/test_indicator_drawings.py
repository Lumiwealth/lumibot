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
