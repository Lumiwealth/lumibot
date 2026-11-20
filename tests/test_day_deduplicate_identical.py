"""day_deduplicate compounds same-instant returns with one grouped product, with identical results.

Why: at the end of every backtest the stats frame is deduplicated per timestamp. Returns that
share an instant (a fill and its preceding valuation) are compounded. That used a Python lambda
per timestamp group; a 3-month MNQ minute backtest has about 95,000 instants, and the call was
31 profiled seconds of report time. A grouped product gives the same numbers.

Invariant under test: the old per-group formula and the new grouped product give bit-identical
frames on random data with singletons, duplicate instants, NaN returns and cash period columns.
"""

import numpy as np
import pandas as pd

from lumibot.tools.pandas import day_deduplicate


def _reference(df_):
    df_copy = df_.copy()
    if df_copy.index.is_unique:
        return df_copy
    grouped = df_copy.groupby(level=0, sort=False)
    latest = grouped.tail(1).copy()
    if "return" in df_copy:
        latest["return"] = grouped["return"].agg(lambda values: (1.0 + values).prod(min_count=1) - 1.0)
    for column in df_copy:
        if isinstance(column, str) and column.startswith("cash_") and column.endswith("_period"):
            latest[column] = grouped[column].sum(min_count=1)
    return latest


def _frame(seed, nan_share):
    rng = np.random.default_rng(seed)
    stamps = pd.date_range("2026-08-18 09:30", periods=4000, freq="min", tz="America/New_York")
    repeats = rng.choice([1, 1, 1, 2, 3, 6], size=len(stamps))
    index = stamps.repeat(repeats)
    returns = rng.normal(0, 0.003, size=len(index))
    returns[rng.random(len(index)) < nan_share] = np.nan
    return pd.DataFrame(
        {
            "portfolio_value": 100_000 + rng.normal(0, 50, size=len(index)).cumsum(),
            "return": returns,
            "cash_deposit_period": rng.choice([0.0, 0.0, 25.0], size=len(index)),
        },
        index=index,
    )


def test_grouped_product_matches_the_per_group_formula_exactly():
    for seed, nan_share in ((1, 0.0), (2, 0.05), (3, 0.5), (4, 1.0)):
        frame = _frame(seed, nan_share)
        expected = _reference(frame)
        actual = day_deduplicate(frame)
        pd.testing.assert_frame_equal(actual, expected, check_exact=True)


def test_unique_index_is_returned_unchanged():
    frame = _frame(5, 0.0).groupby(level=0).tail(1)
    pd.testing.assert_frame_equal(day_deduplicate(frame), frame, check_exact=True)
