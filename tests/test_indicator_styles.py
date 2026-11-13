"""How an indicator says it looks: OutputSpec and Level styles.

Every per-bar value stays a number; styling (widths, line styles, per-bar
color palettes, fills, markers, panes...) is declared once and travels in
``describe()``. These tests cover each style option, the checks that reject a
wrong one, and the example indicators that use them.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from lumibot.example_indicators import momentum_histogram, oscillator_zones, smoothed_candles, trailing_band
from lumibot.indicators import Indicator, Level, OutputSpec
from lumibot.indicators.base import LINE_STYLES, OUTPUT_KINDS, SHAPES, SHAPE_LOCATIONS, SHAPE_SIZES


def _frame(n=160, seed=11):
    rng = np.random.default_rng(seed)
    close = 100 + np.cumsum(rng.normal(0, 1.5, n))
    index = pd.date_range("2024-01-01", periods=n, freq="1D", tz="UTC")
    return pd.DataFrame(
        {
            "open": close + rng.normal(0, 0.3, n),
            "high": close + 1 + rng.random(n),
            "low": close - 1 - rng.random(n),
            "close": close,
            "volume": rng.integers(100, 1000, n).astype(float),
        },
        index=index,
    )


class TestOutputSpecDefaults:
    def test_plain_output_keeps_its_old_fields_and_adds_defaults(self):
        described = OutputSpec("x", "X").describe()
        assert described["id"] == "x" and described["kind"] == "line" and described["color"] == "#2962FF"
        assert described["line_width"] == 1
        assert described["line_style"] == "solid"
        assert described["palette"] == [] and described["color_by"] is None
        assert described["display"] == "all" and described["offset"] == 0
        assert described["pane"] is None and described["precision"] is None

    def test_every_kind_is_accepted(self):
        for kind in OUTPUT_KINDS:
            extra = {}
            if kind == "fill":
                extra["between"] = (1.0, 2.0)
            if kind == "candle":
                extra["candle"] = ("o", "h", "l", "c")
            assert OutputSpec("x", "X", kind, **extra).kind == kind


class TestOutputSpecStyles:
    @pytest.mark.parametrize("style", LINE_STYLES)
    def test_line_styles(self, style):
        assert OutputSpec("x", "X", line_style=style).describe()["line_style"] == style

    def test_unknown_line_style_is_rejected(self):
        with pytest.raises(ValueError, match="line_style"):
            OutputSpec("x", "X", line_style="wavy")

    @pytest.mark.parametrize("width", [0, -1, 11, True, "2"])
    def test_bad_width_is_rejected(self, width):
        with pytest.raises(ValueError, match="line_width"):
            OutputSpec("x", "X", line_width=width)

    @pytest.mark.parametrize("color", ["#26A69A", "#26a69a80", "chart.fg", "chart.bg"])
    def test_colors_with_opacity_and_theme(self, color):
        assert OutputSpec("x", "X", color=color).color == color

    @pytest.mark.parametrize("color", ["red", "#12345", "#GGGGGG", "rgba(0,0,0,1)", "#1234567"])
    def test_bad_colors_are_rejected(self, color):
        with pytest.raises(ValueError, match="color"):
            OutputSpec("x", "X", color=color)

    def test_palette_and_gradient(self):
        spec = OutputSpec("x", "X", palette=["#000000", "#FFFFFF"], color_by="c", gradient=(0, 100))
        described = spec.describe()
        assert described["palette"] == ["#000000", "#FFFFFF"]
        assert described["gradient"] == [0.0, 100.0]
        assert described["color_by"] == "c"

    def test_color_by_without_palette_is_rejected(self):
        with pytest.raises(ValueError, match="palette"):
            OutputSpec("x", "X", color_by="c")

    def test_gradient_needs_two_colors_and_two_numbers(self):
        with pytest.raises(ValueError, match="two colors"):
            OutputSpec("x", "X", palette=("#000000",), gradient=(0, 1))
        with pytest.raises(ValueError, match="two different numbers"):
            OutputSpec("x", "X", palette=("#000000", "#FFFFFF"), gradient=(5, 5))

    def test_offset_display_precision_pane(self):
        spec = OutputSpec("x", "X", offset=-3, display="status_line", precision=2, pane="price", track_price=True)
        described = spec.describe()
        assert (described["offset"], described["display"], described["precision"]) == (-3, "status_line", 2)
        assert described["pane"] == "price" and described["track_price"] is True
        for bad in ({"offset": 1.5}, {"display": "maybe"}, {"precision": 11}, {"pane": "left"}):
            with pytest.raises(ValueError):
                OutputSpec("x", "X", **bad)

    @pytest.mark.parametrize("shape", SHAPES)
    def test_every_shape(self, shape):
        assert OutputSpec("x", "X", "shape", shape=shape).describe()["shape"] == shape

    @pytest.mark.parametrize("location", SHAPE_LOCATIONS)
    def test_every_location(self, location):
        assert OutputSpec("x", "X", "shape", location=location).location == location

    @pytest.mark.parametrize("size", SHAPE_SIZES)
    def test_every_size(self, size):
        assert OutputSpec("x", "X", "shape", size=size).size == size

    def test_shape_text(self):
        spec = OutputSpec("x", "X", "shape", shape="label_up", text="Buy", text_color="#FFFFFF")
        assert spec.describe()["text"] == "Buy" and spec.describe()["text_color"] == "#FFFFFF"
        with pytest.raises(ValueError, match="40 characters"):
            OutputSpec("x", "X", "shape", text="x" * 41)

    def test_fill_needs_between_and_only_fill_has_it(self):
        assert OutputSpec("f", "F", "fill", between=("a", 30.0)).describe()["between"] == ["a", 30.0]
        with pytest.raises(ValueError, match="between"):
            OutputSpec("f", "F", "fill")
        with pytest.raises(ValueError, match="only for fill"):
            OutputSpec("x", "X", between=("a", "b"))

    def test_candle_needs_four_outputs(self):
        with pytest.raises(ValueError, match="candle"):
            OutputSpec("c", "C", "candle", candle=("o", "h", "l"))
        with pytest.raises(ValueError, match="only for candle"):
            OutputSpec("x", "X", candle=("o", "h", "l", "c"))


class TestLevelStyles:
    def test_level_defaults_are_unchanged(self):
        assert Level(70, "High").describe() == {
            "value": 70,
            "label": "High",
            "style": "dashed",
            "color": None,
            "line_width": 1,
        }

    def test_level_style_color_width(self):
        described = Level(0, "Zero", "dotted", color="#787B8680", line_width=2).describe()
        assert (described["style"], described["color"], described["line_width"]) == ("dotted", "#787B8680", 2)

    def test_bad_level_style_and_color(self):
        with pytest.raises(ValueError, match="style"):
            Level(0, "Zero", "wavy")
        with pytest.raises(ValueError, match="color"):
            Level(0, "Zero", color="grey")


class TestReferences:
    def test_unknown_reference_is_rejected_when_used(self):
        class Broken(Indicator):
            key = "broken_reference"
            outputs = [OutputSpec("a", "A", palette=("#000000",), color_by="missing")]

            def step(self, state, bar):
                return {"a": 1.0}

        with pytest.raises(ValueError, match="unknown output"):
            Broken()
        with pytest.raises(ValueError, match="unknown output"):
            Broken.describe()

    def test_duplicate_ids_are_rejected(self):
        class Twice(Indicator):
            key = "twice"
            outputs = [OutputSpec("a", "A"), OutputSpec("a", "A again")]

            def step(self, state, bar):
                return {}

        with pytest.raises(ValueError, match="duplicate"):
            Twice()

    def test_fill_between_levels_needs_no_output(self):
        class Band(Indicator):
            key = "band_only"
            overlay = False
            outputs = [OutputSpec("v", "V"), OutputSpec("band", "Band", "fill", "#FFFFFF20", between=(70.0, 30.0))]

            def step(self, state, bar):
                return {"v": bar.close}

        assert Band.describe()["outputs"][1]["between"] == [70.0, 30.0]


class TestExampleIndicators:
    """Each example draws with the styles it documents and stays numeric per bar."""

    def _numbers_only(self, result):
        for values in result.values():
            assert all(value is None or isinstance(value, float) for value in values)

    def test_momentum_histogram_colors(self):
        result = momentum_histogram.MomentumHistogram().compute(_frame())
        self._numbers_only(result)
        colors = {value for value in result["momentum_color"] if value is not None}
        assert colors <= {0.0, 1.0, 2.0, 3.0} and len(colors) >= 3
        assert {value for value in result["state"] if value is not None} == {0.0}
        described = momentum_histogram.MomentumHistogram.describe()
        hist = described["outputs"][0]
        assert hist["kind"] == "histogram" and len(hist["palette"]) == 4 and hist["color_by"] == "momentum_color"
        assert described["levels"][0]["style"] == "dotted"

    def test_trailing_band_breaks_flips_and_colors(self):
        result = trailing_band.TrailingBand().compute(_frame())
        self._numbers_only(result)
        up, down = result["up_band"], result["down_band"]
        # Exactly one side is drawn on a ready bar, so each line breaks at a flip.
        ready = [i for i, value in enumerate(result["middle"]) if value is not None]
        assert all((up[i] is None) != (down[i] is None) for i in ready)
        assert any(value == 1.0 for value in result["buy"]) or any(value == 1.0 for value in result["sell"])
        assert {value for value in result["trend_color"] if value is not None} <= {0.0, 1.0}
        kinds = {spec["id"]: spec["kind"] for spec in trailing_band.TrailingBand.describe()["outputs"]}
        assert kinds["up_fill"] == "fill" and kinds["buy"] == "shape" and kinds["trend_color"] == "bar_color"

    def test_oscillator_zones_gradient_and_zones(self):
        result = oscillator_zones.OscillatorZones().compute(_frame())
        self._numbers_only(result)
        values = [value for value in result["rsi"] if value is not None]
        assert values and all(0 <= value <= 100 for value in values)
        assert result["rsi_shade"] == result["rsi"]
        assert {value for value in result["zone"] if value is not None} <= {0.0, 1.0}
        described = oscillator_zones.OscillatorZones.describe({"upper": 80, "lower": 20})
        assert [level["value"] for level in described["levels"]] == [80, 50, 20]
        assert described["outputs"][0]["gradient"] == [0.0, 100.0]

    def test_smoothed_candles(self):
        result = smoothed_candles.SmoothedCandles().compute(_frame())
        self._numbers_only(result)
        for high, low, open_, close in zip(result["ha_high"], result["ha_low"], result["ha_open"], result["ha_close"]):
            assert low <= min(open_, close) <= max(open_, close) <= high
        candle = next(spec for spec in smoothed_candles.SmoothedCandles.describe()["outputs"] if spec["kind"] == "candle")
        assert candle["candle"] == ["ha_open", "ha_high", "ha_low", "ha_close"] and candle["pane"] == "own"

    @pytest.mark.parametrize(
        "cls",
        [
            momentum_histogram.MomentumHistogram,
            trailing_band.TrailingBand,
            oscillator_zones.OscillatorZones,
            smoothed_candles.SmoothedCandles,
        ],
    )
    def test_streaming_equals_compute(self, cls):
        frame = _frame(80)
        streamed = cls()
        rows = []
        for time, row in frame.iterrows():
            bar = {"time": time, **row.to_dict()}
            streamed.update(bar, closed=False)  # a forming bar never changes the result
            rows.append(streamed.update(bar, closed=True))
        full = cls().compute(frame)
        for output_id, values in full.items():
            assert [row[output_id] for row in rows] == values
