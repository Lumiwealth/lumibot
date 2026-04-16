# TEARSHEET_METRICS

> Machine-readable tearsheet artifacts, custom tearsheet metrics, and usage rules.

**Last Updated:** 2026-03-25
**Status:** Active
**Audience:** Developers + AI Agents

---

## Overview

LumiBot tearsheet output now includes a machine-readable JSON summary file in addition to
the traditional HTML report and CSV export. This document explains which artifact is for
what, and when strategy authors should use `tearsheet_custom_metrics(...)`.

Custom tearsheet metrics are a supported feature, but they are **rare-use only**. Most
strategies should rely on the built-in QuantStats/LumiBot metrics and should not add
strategy-defined tearsheet rows.

---

## Artifacts

When tearsheet generation succeeds, LumiBot can write:

- `*_tearsheet.html`
  - Human-facing QuantStats report and charts.
- `*_tearsheet.csv`
  - CSV export of the tearsheet metrics table.
- `*_tearsheet_metrics.json`
  - Canonical machine-readable tearsheet summary metrics for agents, APIs, and dashboards.

`*_tearsheet_metrics.json` is the preferred machine-readable summary artifact.

---

## When To Use Custom Tearsheet Metrics

Use `Strategy.tearsheet_custom_metrics(...)` only when **all** of the following are true:

1. the metric is strategy-specific,
2. it does not belong in generic QuantStats/LumiBot core,
3. the user or client explicitly needs that custom summary row.

Do **not** use this hook by default in generated strategies.

Examples of good custom metrics:

- days capital trapped
- trapped capital percent
- client-specific summary rows that are not broadly reusable

Examples of metrics that should stay in core instead:

- generic risk/return ratios
- generic drawdown or recovery metrics
- generic rolling return metrics

---

## Hook Contract

Lifecycle hook:

```python
def tearsheet_custom_metrics(
    self,
    stats_df: pd.DataFrame | None,
    strategy_returns: pd.Series,
    benchmark_returns: pd.Series | None,
    drawdown: pd.Series,
    drawdown_details: pd.DataFrame,
    risk_free_rate: float,
) -> dict:
    ...
```

Return a small `dict` mapping metric names to values.

Supported patterns:

```python
{"Custom Metric A": 1.23}
{"Custom Metric B": {"Strategy": 1.23, "Benchmark (SPY)": 0.91}}
```

Rules:

- return `{}` when no custom metrics apply
- `None` is treated as no custom metrics
- non-dict returns are ignored with a warning
- exceptions in the hook do not crash tearsheet generation

---

## Unit Semantics

Custom metrics are treated as literal scalar values.

- LumiBot does not auto-infer percent formatting for custom rows.
- `*_tearsheet_metrics.json` preserves machine-typed values.
- the HTML tearsheet displays the same values as rows in the metrics table.

Prefer unit-clear values and names:

- counts
- days
- ratios
- raw decimals with explicit naming

Good examples:

```python
{
    "Custom Return Observation Count": 252,
    "Custom Mean Absolute Daily Return": 0.0117,
    "Custom Average Drawdown Days": 14.2,
    "Custom Average Trapped Capital Pct of NLV": 0.1715,
}
```

Labeling note:

- Avoid literal ``%`` characters in custom metric names.
- Prefer ``Pct`` or ``Percent`` in the label and store the value as a raw decimal.
- This keeps the metric name stable across ``*_tearsheet.html`` and
  ``*_tearsheet_metrics.json``.

---

## Output Behavior

If the hook returns metrics, LumiBot appends them to:

- `*_tearsheet.html`
- `*_tearsheet_metrics.json`

For short or degenerate runs, LumiBot should still write a placeholder
`*_tearsheet_metrics.json` instead of failing the backtest.

---

## Related Files

- `docsrc/lifecycle_methods.tearsheet_custom_metrics.rst`
- `docsrc/backtesting.tearsheet_html.rst`
- `lumibot/strategies/strategy.py`
- `tests/test_tearsheet_custom_metrics_end_to_end.py`
