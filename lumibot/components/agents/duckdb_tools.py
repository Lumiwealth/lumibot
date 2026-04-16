import hashlib
import re
import time
from collections import defaultdict
from typing import Any

import duckdb
import pandas as pd

from lumibot.tools.helpers import parse_timestep_qty_and_unit

from .asset_resolution import resolve_asset_and_quote


_READ_ONLY_SQL_RE = re.compile(r"^\s*(select|with|show|describe|pragma|explain)\b", re.IGNORECASE)


class DuckDBQueryLayer:
    def __init__(self, strategy: Any) -> None:
        self.strategy = strategy
        self.connection = duckdb.connect(database=":memory:")
        self._frames: dict[str, pd.DataFrame] = {}
        self._table_meta: dict[str, dict[str, Any]] = {}
        self._source_tables: dict[tuple[Any, ...], dict[str, Any]] = {}
        self._history_cache: dict[tuple[Any, ...], dict[str, Any]] = {}
        self._name_counters: defaultdict[str, int] = defaultdict(int)
        self.metrics: dict[str, float] = {
            "history_load_calls": 0.0,
            "history_cache_hits": 0.0,
            "history_bind_calls": 0.0,
            "history_bind_cache_hits": 0.0,
            "history_visible_refresh_calls": 0.0,
            "history_load_ms": 0.0,
            "query_calls": 0.0,
            "query_ms": 0.0,
        }

    @staticmethod
    def _slugify(value: str) -> str:
        slug = re.sub(r"[^a-zA-Z0-9_]+", "_", value).strip("_").lower()
        return slug or "table"

    def _normalize_index(self, df: pd.DataFrame) -> pd.DataFrame:
        normalized = df.copy()
        if normalized.index.name:
            index_name = normalized.index.name
        else:
            index_name = "datetime"
        if index_name in normalized.columns:
            index_name = f"{index_name}_index"
        normalized = normalized.reset_index().rename(columns={"index": index_name})
        return normalized

    @staticmethod
    def _normalize_timestep(value: str) -> str:
        try:
            _, unit = parse_timestep_qty_and_unit(str(value))
        except Exception:
            unit = str(value)
        normalized = str(unit or value).strip().lower()
        if normalized in {"m", "min", "mins", "minute", "minutes"}:
            return "minute"
        if normalized in {"h", "hr", "hrs", "hour", "hours"}:
            return "hour"
        if normalized in {"d", "day", "days"}:
            return "day"
        return normalized

    @staticmethod
    def _safe_identifier(value: str) -> str:
        if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", value):
            raise ValueError(f"Invalid DuckDB table name: {value!r}")
        return value

    @staticmethod
    def _quote_identifier(value: str) -> str:
        return f'"{value}"'

    @staticmethod
    def _timestamp_literal(value: Any) -> str:
        if value is None:
            return "NULL"
        timestamp = pd.Timestamp(value)
        escaped = str(timestamp.isoformat()).replace("'", "''")
        return f"TIMESTAMPTZ '{escaped}'"

    def _current_datetime(self) -> Any:
        if hasattr(self.strategy, "get_datetime"):
            return self.strategy.get_datetime()
        return None

    def _data_source(self) -> Any | None:
        broker = getattr(self.strategy, "broker", None)
        return getattr(broker, "data_source", None)

    def _lookup_source_frame(self, *, asset: Any, quote: Any, timestep: str) -> tuple[tuple[Any, ...], Any, pd.DataFrame] | None:
        data_source = self._data_source()
        store = getattr(data_source, "_data_store", None)
        finder = getattr(data_source, "find_asset_in_data_store", None)
        if not isinstance(store, dict) or not callable(finder):
            return None
        store_key = finder(asset, quote, timestep)
        if store_key not in store:
            return None
        data = store[store_key]
        if data is None:
            return None
        source_timestep = self._normalize_timestep(getattr(data, "timestep", ""))
        requested_timestep = self._normalize_timestep(timestep)
        if source_timestep != requested_timestep:
            return None
        frame = getattr(data, "df", None)
        if not isinstance(frame, pd.DataFrame):
            return None
        return store_key, data, frame

    def _source_table_name(self, *, symbol: str, asset_type: str, timestep: str, store_key: Any) -> str:
        digest = hashlib.sha1(repr(store_key).encode("utf-8")).hexdigest()[:10]
        slug = self._slugify(f"{symbol}_{asset_type}_{timestep}_{digest}")
        return f"source_{slug}"

    def _register_frame(self, table_name: str, frame: pd.DataFrame, meta: dict[str, Any]) -> dict[str, Any]:
        self._frames[table_name] = frame
        self.connection.register(table_name, frame)
        info = {
            "table_name": table_name,
            "row_count": int(len(frame.index)),
            "columns": [str(col) for col in frame.columns],
        }
        info.update(meta)
        self._table_meta[table_name] = info
        return info

    def _ensure_source_table(
        self,
        *,
        symbol: str,
        asset_type: str,
        timestep: str,
        store_key: Any,
        frame: pd.DataFrame,
    ) -> dict[str, Any]:
        cache_key = (store_key, self._normalize_timestep(timestep))
        cached = self._source_tables.get(cache_key)
        if cached is not None:
            self.metrics["history_bind_cache_hits"] += 1.0
            return cached
        normalized = self._normalize_index(frame)
        datetime_column = next((col for col in normalized.columns if "date" in str(col).lower() or "time" in str(col).lower()), None)
        if datetime_column is None:
            datetime_column = normalized.columns[0]
        normalized[datetime_column] = pd.to_datetime(normalized[datetime_column])
        table_name = self._source_table_name(symbol=symbol, asset_type=asset_type, timestep=timestep, store_key=store_key)
        info = self._register_frame(
            table_name,
            normalized,
            {
                "symbol": symbol,
                "asset_type": asset_type,
                "timestep": timestep,
                "datetime_column": str(datetime_column),
                "kind": "source_frame",
            },
        )
        self._source_tables[cache_key] = info
        self.metrics["history_bind_calls"] += 1.0
        return info

    def _create_visible_view(
        self,
        *,
        source_info: dict[str, Any],
        length: int,
        table_name: str | None,
        symbol: str,
        asset_type: str,
        timestep: str,
    ) -> dict[str, Any]:
        datetime_column = self._safe_identifier(str(source_info["datetime_column"]))
        source_table_name = self._safe_identifier(str(source_info["table_name"]))
        if table_name is None:
            base = self._slugify(f"{symbol}_{asset_type}_{timestep}")
            self._name_counters[base] += 1
            table_name = f"{base}_{self._name_counters[base]}"
        table_name = self._safe_identifier(table_name)
        current_dt = self._current_datetime()
        cutoff_literal = self._timestamp_literal(current_dt)
        sql = (
            f"CREATE OR REPLACE TEMP VIEW {self._quote_identifier(table_name)} AS "
            f"WITH visible AS ("
            f"SELECT * FROM {self._quote_identifier(source_table_name)} "
            f"WHERE {self._quote_identifier(datetime_column)} <= {cutoff_literal} "
            f"ORDER BY {self._quote_identifier(datetime_column)} DESC "
            f"LIMIT {int(length)}"
            f") "
            f"SELECT * FROM visible ORDER BY {self._quote_identifier(datetime_column)} ASC"
        )
        self.connection.execute(sql)
        row_count = int(
            self.connection.execute(
                f"SELECT COUNT(*) FROM {self._quote_identifier(table_name)}"
            ).fetchone()[0]
        )
        info = {
            "table_name": table_name,
            "row_count": row_count,
            "columns": list(source_info["columns"]),
            "symbol": symbol,
            "asset_type": asset_type,
            "timestep": timestep,
            "loaded_at": current_dt.isoformat() if hasattr(current_dt, "isoformat") else None,
            "source_table_name": source_table_name,
            "datetime_column": datetime_column,
            "kind": "visible_view",
        }
        self._table_meta[table_name] = info
        self.metrics["history_visible_refresh_calls"] += 1.0
        return info

    def load_history_table(
        self,
        *,
        symbol: str,
        length: int,
        timestep: str = "day",
        table_name: str | None = None,
        asset_type: str = "stock",
        quote_symbol: str | None = None,
        exchange: str | None = None,
        expiration: str | None = None,
        strike: float | None = None,
        right: str | None = None,
        include_after_hours: bool = True,
    ) -> dict[str, Any]:
        started = time.perf_counter()
        self.metrics["history_load_calls"] += 1.0
        cache_key = (
            symbol,
            int(length),
            str(timestep),
            table_name,
            str(asset_type),
            quote_symbol,
            exchange,
            str(expiration) if expiration is not None else None,
            strike,
            right,
            bool(include_after_hours),
            self.strategy.get_datetime().isoformat() if hasattr(self.strategy, "get_datetime") else None,
        )
        cached = self._history_cache.get(cache_key)
        if cached is not None:
            self.metrics["history_cache_hits"] += 1.0
            return dict(cached)
        asset, quote = resolve_asset_and_quote(
            self.strategy,
            symbol=symbol,
            asset_type=asset_type,
            expiration=expiration,
            strike=strike,
            right=right,
            quote_symbol=quote_symbol,
        )
        source_entry = self._lookup_source_frame(asset=asset, quote=quote, timestep=timestep)
        info: dict[str, Any]
        if source_entry is not None:
            store_key, _data, frame = source_entry
            source_info = self._ensure_source_table(
                symbol=symbol,
                asset_type=asset_type,
                timestep=timestep,
                store_key=store_key,
                frame=frame,
            )
            info = self._create_visible_view(
                source_info=source_info,
                length=length,
                table_name=table_name,
                symbol=symbol,
                asset_type=asset_type,
                timestep=timestep,
            )
        else:
            bars = None
            frame = None
            for candidate_length in range(int(length), 0, -1):
                bars = self.strategy.get_historical_prices(
                    asset,
                    length=candidate_length,
                    timestep=timestep,
                    quote=quote,
                    exchange=exchange,
                    include_after_hours=include_after_hours,
                )
                if bars is None:
                    continue
                frame = getattr(bars, "pandas_df", None)
                if frame is not None:
                    break
            if frame is None:
                frame = pd.DataFrame(columns=["datetime", "open", "high", "low", "close", "volume"])
            normalized = self._normalize_index(frame)
            if table_name is None:
                base = self._slugify(f"{symbol}_{asset_type}_{timestep}")
                self._name_counters[base] += 1
                table_name = f"{base}_{self._name_counters[base]}"
            info = self._register_frame(
                self._safe_identifier(table_name),
                normalized,
                {
                    "symbol": symbol,
                    "asset_type": asset_type,
                    "timestep": timestep,
                    "loaded_at": self.strategy.get_datetime().isoformat() if hasattr(self.strategy, "get_datetime") else None,
                    "kind": "slice_frame",
                },
            )
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        self.metrics["history_load_ms"] += float(elapsed_ms)
        info["load_ms"] = round(elapsed_ms, 3)
        self._history_cache[cache_key] = dict(info)
        return info

    def query(self, *, sql: str, limit: int = 200) -> dict[str, Any]:
        if not sql or not _READ_ONLY_SQL_RE.match(sql):
            raise ValueError("DuckDB tool only allows read-only SQL statements.")
        started = time.perf_counter()
        result = self.connection.execute(sql)
        frame = result.fetch_df()
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        self.metrics["query_calls"] += 1.0
        self.metrics["query_ms"] += float(elapsed_ms)
        if len(frame.index) > limit:
            limited = frame.head(limit)
            truncated = True
        else:
            limited = frame
            truncated = False
        rows = limited.to_dict(orient="records")
        return {
            "row_count": int(len(frame.index)),
            "columns": [str(col) for col in frame.columns],
            "rows": rows,
            "truncated": truncated,
            "query_ms": round(elapsed_ms, 3),
        }

    def get_metrics(self) -> dict[str, float]:
        return dict(self.metrics)
