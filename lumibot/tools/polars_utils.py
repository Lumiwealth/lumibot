"""Utility helpers for operating on Polars DataFrames within Lumibot."""

from __future__ import annotations

from typing import Iterable, Optional, Sequence, Set

import polars as pl


class PolarsResampleError(Exception):
    """Raised when a Polars resample operation cannot be completed."""


def _ensure_datetime_column(df: pl.DataFrame) -> str:
    """Return the datetime-like column name used for grouping."""
    if "datetime" in df.columns:
        return "datetime"

    for candidate in ("timestamp", "date", "time"):
        if candidate in df.columns:
            return candidate

    raise PolarsResampleError("Polars DataFrame lacks a datetime-like column required for resampling.")


def _aggregate_expressions(existing_cols: Sequence[str]) -> list[pl.Expr]:
    """Build aggregation expressions for OHLC-style resampling."""
    exprs: list[pl.Expr] = []
    handled: Set[str] = {"datetime", "timestamp", "date", "time"}

    if "open" in existing_cols:
        exprs.append(pl.col("open").first().alias("open"))
        handled.add("open")

    if "high" in existing_cols:
        exprs.append(pl.col("high").max().alias("high"))
        handled.add("high")

    if "low" in existing_cols:
        exprs.append(pl.col("low").min().alias("low"))
        handled.add("low")

    if "close" in existing_cols:
        exprs.append(pl.col("close").last().alias("close"))
        handled.add("close")

    if "volume" in existing_cols:
        exprs.append(pl.col("volume").sum().alias("volume"))
        handled.add("volume")

    if "dividend" in existing_cols:
        exprs.append(pl.col("dividend").sum().alias("dividend"))
        handled.add("dividend")

    # Preserve any remaining columns by taking the last observation
    for column in existing_cols:
        if column not in handled:
            exprs.append(pl.col(column).last().alias(column))

    return exprs


def resample_polars_ohlc(
    df: pl.DataFrame,
    multiplier: int,
    base_unit: str,
    length: Optional[int] = None,
    label_offset: Optional[str] = None,
) -> pl.DataFrame:
    """Resample a Polars DataFrame containing OHLC-like data.

    Parameters
    ----------
    df:
        Input DataFrame containing at least ``datetime`` plus OHLCV columns.
    multiplier:
        Number of base units to roll up. e.g. multiplier=5, base_unit="minute" -> 5-minute bars.
    base_unit:
        Currently supports "minute" or "day".
    length:
        Optional maximum number of rows to retain (tail). If ``None`` retains the full frame.
    label_offset:
        Optional duration string understood by Polars to offset labels. Useful for aligning session boundaries.

    Returns
    -------
    pl.DataFrame
        Resampled dataset sorted by datetime.
    """

    if df.is_empty():
        return df

    if multiplier <= 0:
        raise PolarsResampleError("Multiplier must be positive for resampling.")

    unit_map = {"minute": "m", "day": "d"}
    try:
        every_suffix = unit_map[base_unit]
    except KeyError as exc:
        raise PolarsResampleError(f"Unsupported base unit '{base_unit}' for polars resampling.") from exc

    every = f"{multiplier}{every_suffix}"

    datetime_column = _ensure_datetime_column(df)
    sorted_df = df.sort(datetime_column)

    agg_exprs = _aggregate_expressions(sorted_df.columns)

    group_kwargs = {
        "every": every,
        "period": every,
        "closed": "left",
        "label": "left",
    }
    if label_offset:
        group_kwargs["offset"] = label_offset

    lazy_frame = sorted_df.lazy()
    if hasattr(lazy_frame, "group_by_dynamic"):
        lazy_grouped = lazy_frame.group_by_dynamic(datetime_column, **group_kwargs)
    else:  # pragma: no cover - backward compatibility
        lazy_grouped = lazy_frame.groupby_dynamic(datetime_column, **group_kwargs)
    resampled = (
        lazy_grouped
        .agg(agg_exprs)
        .sort(datetime_column)
        .collect()
    )

    required_cols: Iterable[str] = [c for c in ("open", "high", "low", "close") if c in resampled.columns]
    if required_cols:
        condition = None
        for col in required_cols:
            expr = pl.col(col).is_not_null()
            condition = expr if condition is None else condition & expr
        resampled = resampled.filter(condition)

    if length is not None and length > 0 and resampled.height > length:
        resampled = resampled.tail(length)

    return resampled
