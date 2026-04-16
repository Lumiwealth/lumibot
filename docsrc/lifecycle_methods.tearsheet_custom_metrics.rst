def tearsheet_custom_metrics
============================

``tearsheet_custom_metrics`` is a strategy lifecycle hook that runs during backtest
analysis, immediately before LumiBot writes:

- ``*_tearsheet.html``
- ``*_tearsheet_metrics.json``

Use this hook when you want strategy-defined metrics in both artifacts.
This is the supported way to add strategy-specific tearsheet summary rows without
forking LumiBot or QuantStats.

When it runs
------------

1. Backtest trading completes.
2. LumiBot computes strategy/benchmark return series and drawdown context.
3. LumiBot calls ``tearsheet_custom_metrics(...)``.
4. Returned metrics are appended to the tearsheet metrics table and JSON scalar metrics.

Method signature
----------------

.. code-block:: python

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

Parameter structure
-------------------

``stats_df`` (``pd.DataFrame | None``)
  Backtest stats dataframe (same data used for ``*_stats.csv/parquet``).

``strategy_returns`` (``pd.Series``)
  Strategy return series used for tearsheet metric calculations.

``benchmark_returns`` (``pd.Series | None``)
  Benchmark return series when a benchmark is available; otherwise ``None``.

``drawdown`` (``pd.Series``)
  Strategy drawdown series derived from cumulative returns.

``drawdown_details`` (``pd.DataFrame``)
  Drawdown periods table (columns such as start/end/valley/days/max drawdown when available).

``risk_free_rate`` (``float``)
  Effective risk-free rate used by tearsheet metrics.

Return format
-------------

Return a ``dict`` mapping metric names to values.

Supported formats:

.. code-block:: python

    {"Custom Metric A": 1.23}
    {"Custom Metric B": {"strategy": 1.23, "benchmark": 0.91}}
    {"Custom Metric C": {"Strategy": 1.23, "Benchmark (SPY)": 0.91}}

Behavior rules:

1. Return ``{}`` if no custom metrics apply.
2. Returning ``None`` is treated as no custom metrics.
3. Returning a non-dict is ignored (with a warning).
4. Exceptions inside the hook are caught; tearsheet generation continues.

Unit semantics
--------------

Custom metrics are treated as literal scalars.

- LumiBot forwards them to QuantStats as-is.
- ``tearsheet_metrics.json`` preserves machine-typed scalar values.
- The tearsheet HTML table shows the same scalar values.

Because there is no automatic percent/unit inference for custom rows, prefer
unit-clear metric names and values such as:

- counts
- days
- ratios
- raw decimals with explicit naming

Good examples:

.. code-block:: python

    {
        "Custom Return Observation Count": 252,
        "Custom Mean Absolute Daily Return": 0.0117,
        "Custom Average Drawdown Days": 14.2,
        "Custom Average Trapped Capital Pct of NLV": 0.1715,
    }

Naming guidance:

- Avoid literal ``%`` characters in custom metric names.
- Prefer ``Pct`` or ``Percent`` in the label and store the value as a raw decimal.
- This keeps metric names stable across ``*_tearsheet.html`` and
  ``*_tearsheet_metrics.json``.

If you need per-column values, the most explicit form is:

.. code-block:: python

    {
        "Custom Relative Edge": {
            "Strategy": 1.23,
            "Benchmark (SPY)": 0.91,
        }
    }

Example
-------

.. code-block:: python

    class MyStrategy(Strategy):
        def tearsheet_custom_metrics(
            self,
            stats_df,
            strategy_returns,
            benchmark_returns,
            drawdown,
            drawdown_details,
            risk_free_rate,
        ):
            if strategy_returns.empty:
                return {}

            non_null_returns = strategy_returns.dropna()
            avg_dd_days = 0.0
            if not drawdown_details.empty and "days" in drawdown_details.columns:
                avg_dd_days = float(drawdown_details["days"].mean())

            return {
                "Custom Return Observation Count": int(non_null_returns.shape[0]),
                "Custom Mean Absolute Daily Return": (
                    float(non_null_returns.abs().mean()) if not non_null_returns.empty else 0.0
                ),
                "Custom Average Drawdown Days": avg_dd_days,
            }

End-to-end flow
---------------

1. Backtest finishes.
2. LumiBot builds the strategy and benchmark return series.
3. LumiBot calls ``tearsheet_custom_metrics(...)``.
4. Returned metrics are merged into the QuantStats tearsheet table.
5. The same metrics are written to:

   - ``*_tearsheet.html``
   - ``*_tearsheet_metrics.json``

Practical guidance
------------------

- This hook is uncommon. Most strategies should not implement it.
- Use this hook for strategy-specific summary rows.
- Keep generic reusable metrics in QuantStats core.
- Prefer scalar custom metrics unless you specifically need strategy/benchmark split values.
- If no custom metrics apply for a run, return ``{}``.
- Degenerate or too-short backtests should still finish; LumiBot writes a placeholder
  ``*_tearsheet_metrics.json`` instead of crashing.

API reference (source of truth)
-------------------------------

The method docstring below is auto-loaded from the Strategy class and should be
treated as the canonical API reference.

.. automethod:: lumibot.strategies.strategy.Strategy.tearsheet_custom_metrics
