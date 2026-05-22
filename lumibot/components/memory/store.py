import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _safe_name(value: Any) -> str:
    text = str(value or "strategy").strip() or "strategy"
    return re.sub(r"[^A-Za-z0-9_.=-]+", "_", text).strip("_")


def _chmod_private(path: Path, mode: int) -> None:
    try:
        os.chmod(path, mode)
    except OSError:
        pass


class MemoryStore:
    """Local JSONL memory store for agentic strategy decisions and lessons."""

    def __init__(self, strategy: Any, *, root_dir: str | os.PathLike[str] | None = None) -> None:
        self.strategy = strategy
        strategy_name = _safe_name(getattr(strategy, "name", None) or strategy.__class__.__name__)
        self.root_dir = Path(root_dir or os.environ.get("LUMIBOT_MEMORY_DIR") or Path.cwd() / ".lumibot" / "memory")
        self.strategy_dir = self.root_dir / strategy_name
        self.root_dir.mkdir(parents=True, exist_ok=True)
        _chmod_private(self.root_dir, 0o700)
        self.strategy_dir.mkdir(parents=True, exist_ok=True)
        _chmod_private(self.strategy_dir, 0o700)

    def remember(
        self,
        text: str,
        *,
        kind: str = "memory",
        tags: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        entry = self._entry(kind=kind, text=text, tags=tags, metadata=metadata)
        self._append("memories.jsonl", entry)
        return entry

    def remember_decision(
        self,
        text: str,
        *,
        symbol: str | None = None,
        action: str | None = None,
        evidence: dict[str, Any] | None = None,
        tags: list[str] | None = None,
    ) -> dict[str, Any]:
        metadata = {"symbol": symbol, "action": action, "evidence": evidence or {}}
        entry = self._entry(kind="decision", text=text, tags=tags, metadata=metadata)
        self._append("decisions.jsonl", entry)
        return entry

    def remember_lesson(
        self,
        text: str,
        *,
        symbol: str | None = None,
        outcome: dict[str, Any] | None = None,
        tags: list[str] | None = None,
    ) -> dict[str, Any]:
        metadata = {"symbol": symbol, "outcome": outcome or {}}
        entry = self._entry(kind="lesson", text=text, tags=tags, metadata=metadata)
        self._append("lessons.jsonl", entry)
        self._append("memories.jsonl", entry)
        return entry

    def open_thesis(
        self,
        text: str,
        *,
        symbol: str | None = None,
        tags: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        metadata = {"symbol": symbol, **(metadata or {}), "status": "open"}
        entry = self._entry(kind="thesis", text=text, tags=tags, metadata=metadata)
        self._append("theses.jsonl", entry)
        return entry

    def update_thesis(self, thesis_id: str, text: str, *, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
        entry = self._entry(kind="thesis_update", text=text, metadata={"thesis_id": thesis_id, **(metadata or {})})
        self._append("theses.jsonl", entry)
        return entry

    def close_thesis(self, thesis_id: str, text: str, *, outcome: dict[str, Any] | None = None) -> dict[str, Any]:
        entry = self._entry(kind="thesis_close", text=text, metadata={"thesis_id": thesis_id, "outcome": outcome or {}})
        self._append("theses.jsonl", entry)
        return entry

    def search(self, query: str, *, limit: int = 10, kind: str | None = None) -> dict[str, Any]:
        terms = [term.lower() for term in re.split(r"\s+", str(query or "").strip()) if term]
        rows = []
        for path in sorted(self.strategy_dir.glob("*.jsonl")):
            for entry in self._read_jsonl(path):
                if kind and entry.get("kind") != kind:
                    continue
                haystack = json.dumps(entry, sort_keys=True).lower()
                score = sum(1 for term in terms if term in haystack) if terms else 1
                if score > 0:
                    rows.append((score, entry))
        rows.sort(key=lambda item: (item[0], item[1].get("timestamp", "")), reverse=True)
        return {"query": query, "count": len(rows), "results": [entry for _, entry in rows[: max(int(limit), 1)]]}

    def _entry(
        self,
        *,
        kind: str,
        text: str,
        tags: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        now = self._timestamp()
        return {
            "id": f"{kind}_{now.replace(':', '').replace('-', '').replace('.', '')}",
            "timestamp": now,
            "strategy": getattr(self.strategy, "name", None) or self.strategy.__class__.__name__,
            "kind": kind,
            "text": str(text or "").strip(),
            "tags": list(tags or []),
            "metadata": metadata or {},
        }

    def _timestamp(self) -> str:
        if hasattr(self.strategy, "get_datetime"):
            try:
                value = self.strategy.get_datetime()
                if hasattr(value, "isoformat"):
                    return value.isoformat()
            except Exception:
                pass
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    def _append(self, filename: str, entry: dict[str, Any]) -> None:
        path = self.strategy_dir / filename
        line = json.dumps(entry, sort_keys=True) + "\n"
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        with os.fdopen(fd, "a", encoding="utf-8") as fh:
            fh.write(line)
        _chmod_private(path, 0o600)

    def _read_jsonl(self, path: Path) -> list[dict[str, Any]]:
        if not path.exists():
            return []
        rows = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        return rows
