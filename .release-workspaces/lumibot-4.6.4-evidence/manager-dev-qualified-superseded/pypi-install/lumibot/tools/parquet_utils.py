from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Any, Callable, Optional

import pandas as pd

_TRUTHY = {"required", "require", "strict", "1", "true", "yes"}


@dataclass(frozen=True)
class ParquetWriteStats:
    artifact: str
    path: str
    rows: int
    cols: int
    bytes: int
    duration_s: float
    coerced_columns: list[str]


def get_backtest_parquet_mode() -> str:
    """Return parquet mode for backtests.

    Supported values:
    - "best_effort" (default): parquet failures log a warning; CSV remains the compatibility layer.
    - "required": parquet failures raise and should fail the backtest (contract mode).
    """

    raw = (os.environ.get("LUMIBOT_BACKTEST_PARQUET_MODE", "") or "").strip().lower()
    if raw in _TRUTHY:
        return "required"
    return "best_effort"


def is_parquet_required() -> bool:
    return get_backtest_parquet_mode() == "required"


def _json_default(value: Any) -> str:
    # Fall back to string conversion for non-serializable objects (Asset, enums, etc.).
    try:
        return str(value)
    except Exception:
        return "<unserializable>"


def _is_decimal(value: Any) -> bool:
    try:
        from decimal import Decimal

        return isinstance(value, Decimal)
    except Exception:
        return False


def coerce_object_columns_to_json_strings(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """Return a copy of df where object-ish columns are coerced to JSON strings when needed.

    This is defensive: PyArrow/parquet can't reliably serialize arbitrary Python objects
    (e.g., Asset instances inside lists/dicts). We keep pure string columns untouched.
    """

    if df.empty:
        return df.copy(), []

    out = df.copy()
    coerced: list[str] = []

    for col in out.columns:
        try:
            series = out[col]
        except Exception:
            continue

        if str(series.dtype) != "object":
            continue

        non_null = series.dropna()
        if non_null.empty:
            continue

        # If the column is already "pure string", keep it.
        sample_types = {type(v) for v in non_null.head(25).tolist()}
        if sample_types.issubset({str}):
            continue

        def _coerce_value(v: Any) -> Any:
            if v is None:
                return None
            if isinstance(v, str):
                return v
            # Keep primitives as-is to avoid unnecessary quoting in JSON (Arrow can store them).
            if isinstance(v, (int, float, bool)):
                return v
            # Decimal is common in trading code; Arrow handles floats reliably.
            if _is_decimal(v):
                try:
                    return float(v)
                except Exception:
                    return _json_default(v)
            try:
                return json.dumps(v, default=_json_default, separators=(",", ":"), sort_keys=True)
            except Exception:
                return _json_default(v)

        out[col] = series.map(_coerce_value)
        coerced.append(col)

    return out, coerced


def write_parquet_with_logging(
    *,
    df: pd.DataFrame,
    path: str,
    artifact: str,
    logger: Any,
    index: bool,
    required: bool,
    compression: str = "zstd",
    engine: str = "pyarrow",
    sanitizer: Optional[Callable[[pd.DataFrame], tuple[pd.DataFrame, list[str]]]] = None,
) -> ParquetWriteStats:
    """Write df to parquet with strong logging. Raises on failure when required=True."""

    start = time.monotonic()
    coerced_columns: list[str] = []

    df_to_write = df
    if sanitizer is not None:
        try:
            df_to_write, coerced_columns = sanitizer(df_to_write)
        except Exception as exc:
            # Sanitizer should never be the reason we fail silently; include context and re-raise in required mode.
            msg = f"Parquet sanitizer failed for {artifact} ({path}): {exc}"
            if required:
                raise RuntimeError(msg) from exc
            logger.warning(msg)
            df_to_write = df
            coerced_columns = []

    def _do_write(*, compression_value: str | None) -> None:
        df_to_write.to_parquet(
            path,
            index=index,
            engine=engine,
            compression=compression_value,
        )

    try:
        try:
            _do_write(compression_value=compression)
        except Exception as exc:
            # Fallback for environments where the preferred compression codec isn't available.
            msg_text = str(exc).lower()
            if compression and ("unsupported" in msg_text and "compression" in msg_text):
                logger.warning(
                    "PARQUET_COMPRESSION_FALLBACK: %s parquet write failed with compression=%s; retrying with compression=None | path=%s error=%s",
                    artifact,
                    compression,
                    path,
                    exc,
                )
                _do_write(compression_value=None)
            else:
                raise

        duration_s = float(time.monotonic() - start)
        bytes_written = int(os.path.getsize(path)) if os.path.exists(path) else 0

        stats = ParquetWriteStats(
            artifact=artifact,
            path=path,
            rows=int(len(df_to_write)),
            cols=int(len(df_to_write.columns)),
            bytes=bytes_written,
            duration_s=duration_s,
            coerced_columns=coerced_columns,
        )
        # Prefer structured log fields when available; fall back to normal logger formatting.
        try:
            logger.info(
                "Wrote parquet artifact: %s",
                artifact,
                extra={
                    "artifact": artifact,
                    "path": path,
                    "rows": stats.rows,
                    "cols": stats.cols,
                    "bytes": stats.bytes,
                    "duration_s": stats.duration_s,
                    "coerced_columns": stats.coerced_columns,
                    "parquet_mode": "required" if required else "best_effort",
                },
            )
        except Exception:
            logger.info(
                "Wrote parquet artifact %s path=%s rows=%s cols=%s bytes=%s duration_s=%.3f coerced_columns=%s mode=%s",
                artifact,
                path,
                stats.rows,
                stats.cols,
                stats.bytes,
                stats.duration_s,
                ",".join(stats.coerced_columns),
                "required" if required else "best_effort",
            )
        return stats
    except Exception as exc:
        # Provide extra context for debugging, especially in required mode.
        object_cols = []
        object_col_samples: dict[str, str] = {}
        try:
            object_cols = [c for c in df.columns if str(df[c].dtype) == "object"]
            for c in object_cols[:25]:
                series = df[c]
                sample_val = None
                try:
                    non_null = series.dropna()
                    if not non_null.empty:
                        sample_val = non_null.iloc[0]
                except Exception:
                    sample_val = None
                if sample_val is not None:
                    object_col_samples[c] = f"type={type(sample_val).__name__} value={repr(sample_val)[:200]}"
        except Exception:
            object_cols = []

        details = f"path={path} mode={'required' if required else 'best_effort'} object_columns={object_cols} samples={object_col_samples} error={exc}"
        msg = f"PARQUET_EXPORT_FAILED: {artifact} parquet export failed | {details}"
        try:
            logger.error(
                msg,
                extra={
                    "artifact": artifact,
                    "path": path,
                    "parquet_mode": "required" if required else "best_effort",
                    "object_columns": object_cols,
                    "object_column_samples": object_col_samples,
                },
            )
        except Exception:
            logger.error(
                "PARQUET_EXPORT_FAILED: %s parquet export failed | path=%s mode=%s object_columns=%s samples=%s error=%s",
                artifact,
                path,
                "required" if required else "best_effort",
                ",".join(object_cols),
                str(object_col_samples),
                exc,
            )

        if required:
            raise RuntimeError(msg) from exc

        logger.warning("Parquet export is best-effort; continuing with CSV compatibility layer.")
        # Return a zeroed stats object for best-effort mode.
        return ParquetWriteStats(
            artifact=artifact,
            path=path,
            rows=int(len(df)),
            cols=int(len(df.columns)),
            bytes=0,
            duration_s=float(time.monotonic() - start),
            coerced_columns=coerced_columns,
        )
