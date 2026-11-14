from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
import pytest

from lumibot.entities import Asset
from lumibot.strategies.strategy import Strategy
from lumibot.tools.indicators import _format_indicator_plotly_text, plot_indicators


@pytest.mark.parametrize(
    ("detail_text", "expects_break"),
    [
        (None, False),
        (float("nan"), False),
        (pd.NA, False),
        ("", False),
        ("   ", False),
        ("hello", True),
        (0, True),
        (0.0, True),
        (1.2345, True),
        ({"a": 1}, True),
        (["x", "y"], True),
    ],
    ids=[
        "none",
        "nan",
        "pd_NA",
        "empty",
        "whitespace",
        "string",
        "int",
        "float_zero",
        "float",
        "dict",
        "list",
    ],
)
def test_format_indicator_plotly_text_is_nan_safe(detail_text: object, expects_break: bool) -> None:
    text = _format_indicator_plotly_text(123.45, detail_text)
    assert isinstance(text, str)
    assert text.startswith("Value: ")

    if expects_break:
        assert "<br>" in text
    else:
        assert "<br>" not in text


def test_plot_indicators_handles_lines_with_detail_text_nan(tmp_path, monkeypatch) -> None:
    mock_write = MagicMock()
    monkeypatch.setattr("plotly.graph_objects.Figure.write_html", mock_write)

    # Mixed rows: one omits the key entirely -> pandas fills NaN (float) for that row.
    chart_lines_df = pd.DataFrame(
        [
            {"plot_name": "default_plot", "name": "Trend", "datetime": pd.Timestamp("2020-01-01"), "value": 1.0},
            {
                "plot_name": "default_plot",
                "name": "Trend",
                "datetime": pd.Timestamp("2020-01-02"),
                "value": 1.1,
                "detail_text": "trend=1",
            },
        ]
    )

    plot_indicators(
        plot_file_html=str(tmp_path / "plot.html"),
        chart_markers_df=None,
        chart_lines_df=chart_lines_df,
        strategy_name="Test",
        show_indicators=True,
    )

    mock_write.assert_called_once()
    assert (tmp_path / "plot.csv").exists()
    assert (tmp_path / "plot.parquet").exists()

    parquet_df = pd.read_parquet(tmp_path / "plot.parquet")
    assert not parquet_df.empty
    assert "type" in parquet_df.columns


def test_plot_indicators_handles_lines_missing_detail_text_column(tmp_path, monkeypatch) -> None:
    mock_write = MagicMock()
    monkeypatch.setattr("plotly.graph_objects.Figure.write_html", mock_write)

    # All rows omit detail_text -> column absent.
    chart_lines_df = pd.DataFrame(
        [
            {"plot_name": "default_plot", "name": "Trend", "datetime": pd.Timestamp("2020-01-01"), "value": 1.0},
            {"plot_name": "default_plot", "name": "Trend", "datetime": pd.Timestamp("2020-01-02"), "value": 1.1},
        ]
    )

    plot_indicators(
        plot_file_html=str(tmp_path / "plot.html"),
        chart_markers_df=None,
        chart_lines_df=chart_lines_df,
        strategy_name="Test",
        show_indicators=True,
    )

    mock_write.assert_called_once()


def test_plot_indicators_emits_empty_csv_and_parquet_when_no_chart_data(tmp_path, monkeypatch) -> None:
    """Regression: indicators artifacts should exist even when a strategy emits no data.

    Updated 2026-11-14: with nothing to draw, the page is a small static file and no plot is
    built (building an empty figure cost seconds per backtest and wrote a 4 MB file). The
    artifacts themselves (HTML, CSV, parquet) must still exist with the same columns.
    """
    mock_write = MagicMock()
    monkeypatch.setattr("plotly.graph_objects.Figure.write_html", mock_write)
    mock_subplots = MagicMock()
    monkeypatch.setattr("lumibot.tools.indicators.make_subplots", mock_subplots)

    for empty in (None, pd.DataFrame()):
        plot_indicators(
            plot_file_html=str(tmp_path / "plot.html"),
            chart_markers_df=empty,
            chart_lines_df=empty,
            chart_ohlc_df=empty,
            strategy_name="Test <b>",
            show_indicators=True,
        )

        mock_write.assert_not_called()
        mock_subplots.assert_not_called()
        page = (tmp_path / "plot.html").read_text()
        assert "Indicators for Test &lt;b&gt; (no indicator data)" in page
        assert len(page) < 2000
        assert (tmp_path / "plot.parquet").exists()
        exported = pd.read_csv(tmp_path / "plot.csv")
        assert exported.empty
        assert list(exported.columns) == ["datetime", "name", "plot_name", "type", "value", "symbol", "size",
                                          "color", "detail_text", "open", "high", "low", "close"]


def test_plot_indicators_still_builds_the_plot_when_any_indicator_exists(tmp_path, monkeypatch) -> None:
    mock_write = MagicMock()
    monkeypatch.setattr("plotly.graph_objects.Figure.write_html", mock_write)
    lines = pd.DataFrame([{"plot_name": "default_plot", "name": "Trend",
                           "datetime": pd.Timestamp("2020-01-01"), "value": 1.0}])

    plot_indicators(str(tmp_path / "plot.html"), None, lines, None, "Test", show_indicators=True)

    mock_write.assert_called_once()
    assert len(pd.read_csv(tmp_path / "plot.csv")) == 1


def test_plot_indicators_handles_markers_missing_detail_text_column(tmp_path, monkeypatch) -> None:
    mock_write = MagicMock()
    monkeypatch.setattr("plotly.graph_objects.Figure.write_html", mock_write)

    chart_markers_df = pd.DataFrame(
        [
            {
                "plot_name": "default_plot",
                "name": "Entry",
                "datetime": pd.Timestamp("2020-01-01"),
                "value": 100.0,
                "symbol": "circle",
                "color": "green",
            },
            {
                "plot_name": "default_plot",
                "name": "Exit",
                "datetime": pd.Timestamp("2020-01-02"),
                "value": 101.0,
                "symbol": "circle",
                "color": "red",
            },
        ]
    )

    plot_indicators(
        plot_file_html=str(tmp_path / "plot.html"),
        chart_markers_df=chart_markers_df,
        chart_lines_df=None,
        strategy_name="Test",
        show_indicators=True,
    )

    mock_write.assert_called_once()


@pytest.mark.parametrize(
    "detail_text",
    [None, float("nan"), pd.NA, "", "hello", 1.23, {"k": "v"}],
    ids=["none", "nan", "pd_NA", "empty", "string", "float", "dict"],
)
def test_plot_indicators_handles_non_string_detail_text_values(tmp_path, monkeypatch, detail_text: object) -> None:
    mock_write = MagicMock()
    monkeypatch.setattr("plotly.graph_objects.Figure.write_html", mock_write)

    chart_lines_df = pd.DataFrame(
        [
            {
                "plot_name": "default_plot",
                "name": "Test",
                "datetime": pd.Timestamp("2020-01-01"),
                "value": 1.0,
                "detail_text": detail_text,
            }
        ]
    )

    plot_indicators(
        plot_file_html=str(tmp_path / "plot.html"),
        chart_markers_df=None,
        chart_lines_df=chart_lines_df,
        strategy_name="Test",
        show_indicators=True,
    )

    mock_write.assert_called_once()


def _make_strategy_stub():
    strat = Strategy.__new__(Strategy)
    strat._chart_markers_list = []
    strat._chart_lines_list = []
    strat.logger = MagicMock()
    strat.portfolio_value = 1_000
    strat.get_datetime = lambda: pd.Timestamp("2024-01-01")
    return strat


def test_add_line_rejects_non_string_detail_text() -> None:
    strat = _make_strategy_stub()
    with pytest.raises(ValueError, match="detail_text.*must be a string"):
        strat.add_line("line", 1.0, detail_text=123)  # type: ignore[arg-type]


def test_add_marker_rejects_non_string_detail_text() -> None:
    strat = _make_strategy_stub()
    with pytest.raises(ValueError, match="detail_text.*must be a string"):
        strat.add_marker("marker", 1.0, detail_text=123)  # type: ignore[arg-type]


def test_add_marker_deduplicates_same_timestamp_name_symbol_plot() -> None:
    strat = _make_strategy_stub()
    dt = pd.Timestamp("2024-01-01T00:00:00Z")
    asset = Asset(symbol="SPY", asset_type="stock")

    first = strat.add_marker("buy", 100.0, symbol="circle", dt=dt, plot_name="default_plot", asset=asset)
    second = strat.add_marker("buy", 100.0, symbol="circle", dt=dt, plot_name="default_plot", asset=asset)

    assert first is not None
    assert second is None
    assert len(strat._chart_markers_list) == 1


def test_get_lines_and_markers_df_include_detail_text_column() -> None:
    strat = _make_strategy_stub()
    dt = pd.Timestamp("2024-01-01")
    asset = Asset(symbol="SPY", asset_type="stock")

    strat.add_line("price", 100.0, dt=dt, asset=asset)
    strat.add_marker("buy", 100.0, dt=dt, asset=asset)

    lines_df = strat.get_lines_df()
    markers_df = strat.get_markers_df()

    assert "detail_text" in lines_df.columns
    assert "detail_text" in markers_df.columns
