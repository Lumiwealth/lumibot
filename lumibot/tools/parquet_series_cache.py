from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from types import ModuleType
from typing import Any, TypeAlias, cast

from lumibot.constants import LUMIBOT_DEFAULT_PYTZ
from lumibot.tools.backtest_cache import get_backtest_cache

logger = logging.getLogger(__name__)
PandasDataFrame: TypeAlias = Any  # noqa: UP040 - keep Python 3.11 parser compatibility.
IndexCoercer: TypeAlias = Callable[[PandasDataFrame], PandasDataFrame]  # noqa: UP040


class _LazyModule(ModuleType):
    _module_name: str
    _module: ModuleType | None

    __slots__ = ("_module_name", "_module")

    def __init__(self, module_name: str) -> None:
        super().__init__(module_name)
        object.__setattr__(self, "_module_name", module_name)
        object.__setattr__(self, "_module", None)

    def _load(self) -> ModuleType:
        module = cast(ModuleType | None, object.__getattribute__(self, "_module"))
        if module is None:
            module = import_module(cast(str, object.__getattribute__(self, "_module_name")))
            object.__setattr__(self, "_module", module)
        return module

    def __getattr__(self, name: str) -> Any:
        return getattr(self._load(), name)


pd: Any = _LazyModule("pandas")


class _LazyModule:
    __slots__ = ("_module_name", "_module")

    def __init__(self, module_name: str):
        object.__setattr__(self, "_module_name", module_name)
        object.__setattr__(self, "_module", None)

    def _load(self):
        module = object.__getattribute__(self, "_module")
        if module is None:
            module = import_module(object.__getattribute__(self, "_module_name"))
            object.__setattr__(self, "_module", module)
        return module

    def __getattr__(self, name):
        return getattr(self._load(), name)


pd = _LazyModule("pandas")


@dataclass(frozen=True)
class ParquetSeriesCache:
    """Provider-agnostic helper for parquet series caching.

    This standardizes the pattern we implement across data providers:
    - hydrate a local parquet path from remote cache (optional)
    - read and normalize into a tz-aware DatetimeIndex
    - merge updates and persist
    - allow placeholder rows to live in cache (e.g. `missing=True`)
    """

    path: Path
    remote_payload: dict[str, object] | None = None
    index_coercer: IndexCoercer | None = None
    tz: Any = LUMIBOT_DEFAULT_PYTZ

    def hydrate_remote(self) -> None:
        cache_manager = get_backtest_cache()
        try:
            cache_manager.ensure_local_file(self.path, payload=self.remote_payload)
        except Exception:
            return

    def read(self) -> PandasDataFrame:
        if not self.path.exists():
            return pd.DataFrame()
        try:
            df = pd.read_parquet(self.path)
        except Exception:
            return pd.DataFrame()
        if df is None or df.empty:
            return pd.DataFrame()

        if self.index_coercer is not None:
            try:
                df = self.index_coercer(df)
            except Exception:
                return pd.DataFrame()

        if not isinstance(df.index, pd.DatetimeIndex):
            if "datetime" in df.columns:
                df = df.set_index(pd.to_datetime(df["datetime"], utc=True, errors="coerce"))
                df = df.drop(columns=["datetime"], errors="ignore")
            else:
                return pd.DataFrame()

        df.index = pd.to_datetime(df.index, utc=True, errors="coerce")
        df = df[~df.index.isna()]
        df = df.sort_index()
        try:
            df.index = df.index.tz_convert(self.tz)
        except Exception:
            try:
                df.index = df.index.tz_localize(self.tz)
            except Exception:
                pass

        if "missing" in df.columns:
            df["missing"] = df["missing"].fillna(False)
        return df

    def write(self, df: PandasDataFrame, *, remote_payload: dict[str, object] | None = None) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        df_to_save = df.copy()
        if not isinstance(df_to_save.index, pd.DatetimeIndex):
            raise ValueError("ParquetSeriesCache frames must be indexed by datetime")
        df_to_save.to_parquet(self.path)
        cache_manager = get_backtest_cache()
        try:
            cache_manager.on_local_update(self.path, payload=remote_payload or self.remote_payload)
        except Exception:
            logger.debug("Remote cache upload failed for %s", self.path.as_posix(), exc_info=True)

    @staticmethod
    def merge(existing: PandasDataFrame | None, incoming: PandasDataFrame | None) -> PandasDataFrame:
        if existing is None or existing.empty:
            return incoming
        if incoming is None or incoming.empty:
            return existing
        merged = pd.concat([existing, incoming], axis=0).sort_index()
        merged = merged[~merged.index.duplicated(keep="last")]
        if "missing" in merged.columns:
            merged["missing"] = merged["missing"].fillna(False)
        return merged
