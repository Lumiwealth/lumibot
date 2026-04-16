from __future__ import annotations

import gzip
import hashlib
import json
import os
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any

from lumibot.constants import LUMIBOT_CACHE_FOLDER
from lumibot.tools.backtest_cache import get_backtest_cache


def _normalize_json(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(key): _normalize_json(val) for key, val in sorted(value.items(), key=lambda item: str(item[0]))}
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, set):
        return sorted(_normalize_json(item) for item in value)
    if isinstance(value, (list, tuple)):
        return [_normalize_json(item) for item in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    data = getattr(value, "__dict__", None)
    if isinstance(data, dict):
        return {str(key): _normalize_json(val) for key, val in sorted(data.items(), key=lambda item: str(item[0]))}
    if hasattr(value, "to_dict"):
        try:
            return _normalize_json(value.to_dict())
        except Exception:
            return str(value)
    if hasattr(value, "to_minimal_dict"):
        try:
            return _normalize_json(value.to_minimal_dict())
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
            return json.load(handle)

    def save(self, key: str, payload: dict[str, Any]) -> Path:
        path = self._path_for(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(path, "wt", encoding="utf-8") as handle:
            json.dump(_normalize_json(payload), handle, sort_keys=True)
        self.remote_cache.on_local_update(path)
        return path
