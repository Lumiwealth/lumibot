from __future__ import annotations

import gzip
import hashlib
import json
import os
from collections.abc import Callable, Mapping
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, cast

from lumibot.constants import LUMIBOT_CACHE_FOLDER

_backtest_cache_getter: Callable[[], Any] | None = None


def get_backtest_cache() -> Any:
    global _backtest_cache_getter
    if _backtest_cache_getter is None:
        from lumibot.tools.backtest_cache import get_backtest_cache as _get_backtest_cache

        _backtest_cache_getter = _get_backtest_cache
    return _backtest_cache_getter()


def _normalize_json(value: object) -> object:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Mapping):
        mapping = cast(Mapping[object, object], value)
        return {str(key): _normalize_json(val) for key, val in sorted(mapping.items(), key=lambda item: str(item[0]))}
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, set):
        normalized_items = [_normalize_json(item) for item in cast(set[object], value)]
        return sorted(
            normalized_items,
            key=lambda item: json.dumps(item, sort_keys=True, default=str),
        )
    if isinstance(value, (list, tuple)):
        return [_normalize_json(item) for item in cast(list[object] | tuple[object, ...], value)]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    data = getattr(value, "__dict__", None)
    if isinstance(data, Mapping):
        mapping = cast(Mapping[object, object], data)
        return {str(key): _normalize_json(val) for key, val in sorted(mapping.items(), key=lambda item: str(item[0]))}
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        try:
            return _normalize_json(to_dict())
        except Exception:
            return str(value)
    to_minimal_dict = getattr(value, "to_minimal_dict", None)
    if callable(to_minimal_dict):
        try:
            return _normalize_json(to_minimal_dict())
        except Exception:
            return str(value)
    return value


def _cache_root() -> Path:
    return Path(os.environ.get("LUMIBOT_CACHE_FOLDER") or LUMIBOT_CACHE_FOLDER)


class AgentReplayCache:
    def __init__(self) -> None:
        self.root = _cache_root() / "agent_runtime" / "replay"
        self.root.mkdir(parents=True, exist_ok=True)
        self.remote_cache = get_backtest_cache()

    def compute_key(self, payload: dict[str, Any]) -> str:
        normalized = _normalize_json(payload)
        encoded = json.dumps(normalized, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    def _path_for(self, key: str) -> Path:
        return self.root / key[:2] / f"{key}.json.gz"

    def load(self, key: str) -> dict[str, Any] | None:
        path = self._path_for(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.remote_cache.ensure_local_file(path)
        if not path.exists():
            return None
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            payload = json.load(handle)
        return dict(cast(Mapping[str, Any], payload)) if isinstance(payload, Mapping) else None

    def save(self, key: str, payload: dict[str, Any]) -> Path:
        path = self._path_for(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(path, "wt", encoding="utf-8") as handle:
            json.dump(_normalize_json(payload), handle, sort_keys=True)
        self.remote_cache.on_local_update(path)
        return path
