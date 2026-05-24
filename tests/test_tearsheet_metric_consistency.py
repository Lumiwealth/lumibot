from types import SimpleNamespace

import pandas as pd

from lumibot.tools import indicators
from lumibot.tools.indicators import _prepare_tearsheet_returns


def test_create_tearsheet_uses_same_match_dates_for_html_and_metrics_json(monkeypatch, tmp_path):
    captured = {}

    def fake_html(returns, benchmark, **kwargs):
        captured["html_match_dates"] = kwargs.get("match_dates")
        output = kwargs["output"]
        with open(output, "w", encoding="utf-8") as handle:
            handle.write("<html></html>")
        return pd.DataFrame(
            {"QQQ": ["10.00%"], "Strategy": ["20.00%"]},
            index=["Total Return"],
        )

    def fake_metrics_json(returns, benchmark, **kwargs):
        captured["json_match_dates"] = kwargs.get("match_dates")
        output = kwargs["output"]
        with open(output, "w", encoding="utf-8") as handle:
            handle.write('{"metadata": {}, "scalar_metrics": {}}')
        return {"metadata": {}, "scalar_metrics": {}}

    monkeypatch.setattr(
        indicators.qs,
        "reports",
        SimpleNamespace(html=fake_html, metrics_json=fake_metrics_json),
    )

    index = pd.date_range("2026-04-07", periods=5, freq="D")
    strategy_df = pd.DataFrame(
        {
            "return": [0.0, 0.04, -0.02, 0.03, 0.01],
            "portfolio_value": [100, 104, 101.92, 104.9776, 106.027376],
        },
        index=index,
    )
    benchmark_df = pd.DataFrame(
        {"symbol_cumprod": [1.0, 1.01, 1.005, 1.02, 1.03]},
        index=index,
    )

    result = indicators.create_tearsheet(
        strategy_df=strategy_df,
        strat_name="Example",
        tearsheet_file=str(tmp_path / "tearsheet.html"),
        benchmark_df=benchmark_df,
        benchmark_asset="QQQ",
        show_tearsheet=False,
        save_tearsheet=True,
        risk_free_rate=0.0,
        tearsheet_metrics_file=str(tmp_path / "tearsheet_metrics.json"),
    )

    assert captured["html_match_dates"] is False
    assert captured["json_match_dates"] is False
    assert result.loc["Total Return", "Strategy"] == "20.00%"


def test_prepare_tearsheet_returns_prefers_account_value_over_return_column():
    index = pd.date_range("2026-04-07", periods=3, freq="D")
    strategy_df = pd.DataFrame(
        {
            "cash_adjusted_portfolio_value": [100.0, 110.0, 121.0],
            "portfolio_value": [100.0, 110.0, 121.0],
            "return": [0.0, 0.50, 0.50],
        },
        index=index,
    )
    benchmark_df = pd.DataFrame({"symbol_cumprod": [1.0, 1.0, 1.0]}, index=index)

    result = _prepare_tearsheet_returns(strategy_df, benchmark_df)

    assert result.loc[pd.Timestamp("2026-04-08"), "strategy"] == 0.10
    assert result.loc[pd.Timestamp("2026-04-09"), "strategy"] == 0.10


def test_prepare_tearsheet_returns_does_not_let_benchmark_rows_erase_strategy_move():
    strategy_df = pd.DataFrame(
        {
            "portfolio_value": [100.0, 120.0, 144.0],
            "return": [0.0, 0.20, 0.20],
        },
        index=pd.to_datetime(
            [
                "2026-04-10 09:30",
                "2026-04-13 09:30",
                "2026-04-14 09:30",
            ]
        ),
    )
    benchmark_df = pd.DataFrame(
        {"symbol_cumprod": [1.0, 1.01, 1.02]},
        index=pd.to_datetime(
            [
                "2026-04-10 16:00",
                "2026-04-13 16:00",
                "2026-04-14 16:00",
            ]
        ),
    )

    result = _prepare_tearsheet_returns(strategy_df, benchmark_df)

    assert result.loc[pd.Timestamp("2026-04-13"), "strategy"] == 0.20
    assert round((1 + result["strategy"]).prod() - 1, 10) == 0.44
