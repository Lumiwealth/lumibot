import hashlib
import json
import os
import re
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SCHEMA_VERSION = 1


def _safe_name(value: Any) -> str:
    text = str(value or "strategy").strip() or "strategy"
    return re.sub(r"[^A-Za-z0-9_.=-]+", "_", text).strip("_")


def _json_dumps(value: Any) -> str:
    try:
        return json.dumps(value if value is not None else {}, sort_keys=True, default=str)
    except Exception:
        return json.dumps({"repr": repr(value)}, sort_keys=True)


def _json_loads(value: Any, default: Any) -> Any:
    if value in (None, ""):
        return default
    try:
        return json.loads(str(value))
    except Exception:
        return default


def _compact_text(value: Any, *, limit: int) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return text[: max(limit - 15, 1)].rstrip() + "... [truncated]"


def _normalize_symbol(value: Any) -> str | None:
    text = str(value or "").strip().upper()
    return text or None


class MemoryStore:
    """SQLite-backed memory store for Lumibot trading agents.

    The SQLite database is the live write path. Events are append-only; the
    `memory_index` table is a projection that makes current state and search
    cheap. Parquet exports are derived artifacts for observability.
    """

    def __init__(self, strategy: Any, *, root_dir: str | os.PathLike[str] | None = None) -> None:
        self.strategy = strategy
        self.strategy_name = getattr(strategy, "name", None) or strategy.__class__.__name__
        safe_strategy_name = _safe_name(self.strategy_name)
        self.root_dir = Path(root_dir or os.environ.get("LUMIBOT_MEMORY_DIR") or Path.cwd() / ".lumibot" / "memory")
        self.strategy_dir = self.root_dir / safe_strategy_name
        self.strategy_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.strategy_dir / "memory.sqlite"
        self._ensure_schema()

    def remember(
        self,
        text: str,
        *,
        kind: str = "memory",
        tags: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        agent_name: str | None = None,
        model_call_id: str | None = None,
        retrieval_id: str | None = None,
    ) -> dict[str, Any]:
        memory_id = f"{_safe_name(kind)}_{uuid.uuid4().hex}"
        return self._record_projection_event(
            event_type="memory.created",
            subject_type=kind,
            subject_id=memory_id,
            kind=kind,
            status="active",
            text=text,
            tags=tags,
            metadata=metadata,
            agent_name=agent_name,
            model_call_id=model_call_id,
            retrieval_id=retrieval_id,
        )

    def remember_decision(
        self,
        text: str,
        *,
        symbol: str | None = None,
        action: str | None = None,
        evidence: dict[str, Any] | None = None,
        tags: list[str] | None = None,
        agent_name: str | None = None,
        model_call_id: str | None = None,
        retrieval_id: str | None = None,
    ) -> dict[str, Any]:
        metadata = {"symbol": symbol, "action": action, "evidence": evidence or {}}
        memory_id = f"decision_{uuid.uuid4().hex}"
        return self._record_projection_event(
            event_type="decision.recorded",
            subject_type="decision",
            subject_id=memory_id,
            kind="decision",
            status="recorded",
            text=text,
            symbol=symbol,
            tags=tags,
            metadata=metadata,
            agent_name=agent_name,
            model_call_id=model_call_id,
            retrieval_id=retrieval_id,
        )

    def remember_proposal(
        self,
        text: str,
        *,
        symbol: str | None = None,
        action: str | None = None,
        tags: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        agent_name: str | None = None,
        model_call_id: str | None = None,
        retrieval_id: str | None = None,
    ) -> dict[str, Any]:
        merged_metadata = {"symbol": symbol, "action": action, **(metadata or {})}
        memory_id = f"proposal_{uuid.uuid4().hex}"
        return self._record_projection_event(
            event_type="proposal.recorded",
            subject_type="proposal",
            subject_id=memory_id,
            kind="proposal",
            status="proposed",
            text=text,
            symbol=symbol,
            tags=tags,
            metadata=merged_metadata,
            agent_name=agent_name,
            model_call_id=model_call_id,
            retrieval_id=retrieval_id,
        )

    def remember_risk_note(
        self,
        text: str,
        *,
        symbol: str | None = None,
        tags: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        agent_name: str | None = None,
        model_call_id: str | None = None,
        retrieval_id: str | None = None,
    ) -> dict[str, Any]:
        merged_metadata = {"symbol": symbol, **(metadata or {})}
        memory_id = f"risk_note_{uuid.uuid4().hex}"
        return self._record_projection_event(
            event_type="risk_note.recorded",
            subject_type="risk_note",
            subject_id=memory_id,
            kind="risk_note",
            status="active",
            text=text,
            symbol=symbol,
            tags=tags,
            metadata=merged_metadata,
            agent_name=agent_name,
            model_call_id=model_call_id,
            retrieval_id=retrieval_id,
        )

    def remember_lesson(
        self,
        text: str,
        *,
        symbol: str | None = None,
        outcome: dict[str, Any] | None = None,
        tags: list[str] | None = None,
        agent_name: str | None = None,
        model_call_id: str | None = None,
        retrieval_id: str | None = None,
    ) -> dict[str, Any]:
        outcome = outcome or {}
        status = "validated" if outcome.get("validated") is True else "proposed"
        metadata = {"symbol": symbol, "outcome": outcome}
        memory_id = f"lesson_{uuid.uuid4().hex}"
        return self._record_projection_event(
            event_type=f"lesson.{status}",
            subject_type="lesson",
            subject_id=memory_id,
            kind="lesson",
            status=status,
            text=text,
            symbol=symbol,
            tags=tags,
            metadata=metadata,
            agent_name=agent_name,
            model_call_id=model_call_id,
            retrieval_id=retrieval_id,
        )

    def open_thesis(
        self,
        text: str,
        *,
        symbol: str | None = None,
        tags: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        agent_name: str | None = None,
        model_call_id: str | None = None,
        retrieval_id: str | None = None,
    ) -> dict[str, Any]:
        metadata = {"symbol": symbol, **(metadata or {}), "status": "open"}
        memory_id = f"thesis_{uuid.uuid4().hex}"
        return self._record_projection_event(
            event_type="thesis.opened",
            subject_type="thesis",
            subject_id=memory_id,
            kind="thesis",
            status="open",
            text=text,
            symbol=symbol,
            tags=tags,
            metadata=metadata,
            agent_name=agent_name,
            model_call_id=model_call_id,
            retrieval_id=retrieval_id,
        )

    def update_thesis(
        self,
        thesis_id: str,
        text: str,
        *,
        metadata: dict[str, Any] | None = None,
        agent_name: str | None = None,
        model_call_id: str | None = None,
        retrieval_id: str | None = None,
    ) -> dict[str, Any]:
        existing = self.get(thesis_id)
        symbol = metadata.get("symbol") if isinstance(metadata, dict) else None
        if not symbol and existing:
            symbol = existing.get("symbol") or (existing.get("metadata") or {}).get("symbol")
        merged_metadata = {"thesis_id": thesis_id, **(metadata or {}), "status": "open"}
        return self._record_projection_event(
            event_type="thesis.updated",
            subject_type="thesis",
            subject_id=thesis_id,
            kind="thesis",
            status="open",
            text=text,
            symbol=symbol,
            tags=None,
            metadata=merged_metadata,
            agent_name=agent_name,
            model_call_id=model_call_id,
            retrieval_id=retrieval_id,
        )

    def close_thesis(
        self,
        thesis_id: str,
        text: str,
        *,
        outcome: dict[str, Any] | None = None,
        agent_name: str | None = None,
        model_call_id: str | None = None,
        retrieval_id: str | None = None,
    ) -> dict[str, Any]:
        existing = self.get(thesis_id)
        symbol = existing.get("symbol") if existing else None
        return self._record_projection_event(
            event_type="thesis.closed",
            subject_type="thesis",
            subject_id=thesis_id,
            kind="thesis",
            status="closed",
            text=text,
            symbol=symbol,
            tags=None,
            metadata={"thesis_id": thesis_id, "outcome": outcome or {}, "status": "closed"},
            agent_name=agent_name,
            model_call_id=model_call_id,
            retrieval_id=retrieval_id,
        )

    def record_warning(
        self,
        text: str,
        *,
        kind: str = "agent_warning",
        symbol: str | None = None,
        metadata: dict[str, Any] | None = None,
        agent_name: str | None = None,
        model_call_id: str | None = None,
        retrieval_id: str | None = None,
    ) -> dict[str, Any]:
        event = self._append_event(
            event_type=kind,
            subject_type="warning",
            subject_id=f"warning_{uuid.uuid4().hex}",
            text=text,
            symbol=symbol,
            metadata=metadata or {},
            payload={"warning": text, "metadata": metadata or {}},
            agent_name=agent_name,
            model_call_id=model_call_id,
            retrieval_id=retrieval_id,
        )
        self._export_artifacts_best_effort()
        return event

    def record_order_submitted(
        self,
        *,
        order: Any = None,
        symbol: str | None = None,
        side: str | None = None,
        quantity: Any = None,
        order_type: str | None = None,
        asset_type: str | None = None,
        order_payload: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        agent_name: str | None = None,
        model_call_id: str | None = None,
        retrieval_id: str | None = None,
        **order_kwargs: Any,
    ) -> dict[str, Any]:
        order_id = None
        if isinstance(order_payload, dict):
            order_id = order_payload.get("identifier")
        order_id = order_id or getattr(order, "identifier", None) or getattr(order, "id", None) or uuid.uuid4().hex
        payload = {
            "kind": "order",
            "status": "submitted",
            "symbol": symbol,
            "side": side,
            "quantity": quantity,
            "order_type": order_type,
            "asset_type": asset_type,
            "order": order_payload or {},
            "arguments": {key: value for key, value in order_kwargs.items() if value is not None},
            "metadata": metadata or {},
        }
        text_parts = ["Submitted order"]
        if side:
            text_parts.append(str(side))
        if quantity is not None:
            text_parts.append(str(quantity))
        if symbol:
            text_parts.append(str(symbol).upper())
        if order_type:
            text_parts.append(f"as {order_type}")
        event = self._append_event(
            event_type="order.submitted",
            subject_type="order",
            subject_id=f"order_{_safe_name(order_id)}",
            text=" ".join(text_parts),
            symbol=symbol,
            metadata=payload,
            payload=payload,
            agent_name=agent_name,
            model_call_id=model_call_id,
            retrieval_id=retrieval_id,
        )
        self._export_artifacts_best_effort()
        return event

    def get(self, memory_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM memory_index WHERE memory_id = ?", (memory_id,)).fetchone()
        return self._row_to_memory(row) if row else None

    def search(
        self,
        query: str,
        *,
        limit: int = 10,
        kind: str | None = None,
        symbol: str | None = None,
        status: str | None = None,
        agent_name: str | None = None,
        model_call_id: str | None = None,
    ) -> dict[str, Any]:
        query_text = str(query or "").strip()
        terms = [term.lower() for term in re.split(r"\s+", query_text) if term]
        normalized_symbol = _normalize_symbol(symbol)
        max_results = max(int(limit or 10), 1)
        with self._connect() as conn:
            index_rows = conn.execute(
                """
                SELECT *
                FROM memory_index
                WHERE (? IS NULL OR kind = ?)
                  AND (? IS NULL OR symbol = ?)
                  AND (? IS NULL OR status = ?)
                """,
                (kind, kind, normalized_symbol, normalized_symbol, status, status),
            ).fetchall()
            event_rows = conn.execute(
                """
                SELECT *
                FROM memory_events
                WHERE subject_id IS NOT NULL
                  AND (? IS NULL OR symbol = ?)
                """,
                (normalized_symbol, normalized_symbol),
            ).fetchall()
        scored: list[tuple[int, str, dict[str, Any]]] = []
        seen_ids: set[str] = set()
        for row in index_rows:
            item = self._row_to_memory(row)
            seen_ids.add(f"index:{item.get('memory_id')}")
            haystack = _json_dumps(item).lower()
            score = sum(1 for term in terms if term in haystack) if terms else 1
            if score > 0:
                scored.append((score, item.get("updated_at") or "", item))
        for row in event_rows:
            item = self._row_to_event_memory(row)
            if kind is not None and item.get("kind") != kind:
                continue
            if status is not None and item.get("status") != status:
                continue
            item_id = f"event:{item.get('event_id')}"
            if item_id in seen_ids:
                continue
            haystack = _json_dumps(item).lower()
            score = sum(1 for term in terms if term in haystack) if terms else 1
            if score > 0:
                scored.append((score, item.get("timestamp") or "", item))
                seen_ids.add(item_id)
        scored.sort(key=lambda item: (item[0], item[1]), reverse=True)
        results = [item for _, _, item in scored[:max_results]]
        retrieval = self.record_retrieval(
            query=query_text,
            kind=kind,
            symbol=normalized_symbol,
            status=status,
            limit=max_results,
            candidates=[item for _, _, item in scored],
            selected=results,
            agent_name=agent_name,
            model_call_id=model_call_id,
        )
        return {
            "query": query_text,
            "kind": kind,
            "symbol": normalized_symbol,
            "status": status,
            "count": len(scored),
            "retrieval_id": retrieval["retrieval_id"],
            "results": results,
        }

    def record_retrieval(
        self,
        *,
        query: str,
        kind: str | None,
        symbol: str | None,
        status: str | None,
        limit: int,
        candidates: list[dict[str, Any]],
        selected: list[dict[str, Any]],
        rendered_text: str | None = None,
        agent_name: str | None = None,
        model_call_id: str | None = None,
    ) -> dict[str, Any]:
        retrieval_id = f"retrieval_{uuid.uuid4().hex}"
        timestamp = self._timestamp()
        candidate_ids = [str(item.get("id") or item.get("memory_id")) for item in candidates if item.get("id") or item.get("memory_id")]
        selected_ids = [str(item.get("id") or item.get("memory_id")) for item in selected if item.get("id") or item.get("memory_id")]
        rendered = rendered_text if rendered_text is not None else "\n\n".join(
            f"{item.get('kind')} {item.get('symbol') or ''} {item.get('status') or ''}: {item.get('text') or ''}".strip()
            for item in selected
        )
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO memory_retrievals (
                    retrieval_id, timestamp, wall_time, strategy, agent_name, model_call_id,
                    query, kind, symbol, status, result_limit, candidate_ids_json,
                    selected_ids_json, rendered_text, schema_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    retrieval_id,
                    timestamp,
                    self._wall_time(),
                    self.strategy_name,
                    agent_name,
                    model_call_id,
                    query,
                    kind,
                    symbol,
                    status,
                    int(limit),
                    _json_dumps(candidate_ids),
                    _json_dumps(selected_ids),
                    rendered,
                    SCHEMA_VERSION,
                ),
            )
        self._export_artifacts_best_effort()
        return {"retrieval_id": retrieval_id, "selected_ids": selected_ids, "candidate_ids": candidate_ids}

    def compact_state(
        self,
        *,
        symbols: list[str] | None = None,
        max_theses: int = 8,
        max_lessons: int = 8,
        max_chars_per_item: int = 900,
        update_open_thesis_outcomes: bool = True,
    ) -> dict[str, Any]:
        normalized_symbols = sorted({symbol for symbol in (_normalize_symbol(item) for item in (symbols or [])) if symbol})
        with self._connect() as conn:
            open_theses_rows = conn.execute(
                """
                SELECT *
                FROM memory_index
                WHERE kind = 'thesis' AND status = 'open'
                ORDER BY updated_at DESC
                LIMIT ?
                """,
                (max(int(max_theses), 1),),
            ).fetchall()
            lesson_rows = conn.execute(
                """
                SELECT *
                FROM memory_index
                WHERE kind = 'lesson' AND status = 'validated'
                ORDER BY updated_at DESC
                LIMIT ?
                """,
                (max(int(max_lessons), 1),),
            ).fetchall()
        if update_open_thesis_outcomes:
            try:
                self._record_open_thesis_outcome_observations(open_theses_rows, normalized_symbols)
            except Exception:
                pass
        open_theses = [self._compact_memory_item(self._row_to_memory(row), max_chars=max_chars_per_item) for row in open_theses_rows]
        lessons = [self._compact_memory_item(self._row_to_memory(row), max_chars=max_chars_per_item) for row in lesson_rows]
        position_rationales = [
            item
            for item in open_theses
            if normalized_symbols and _normalize_symbol(item.get("symbol")) in normalized_symbols
        ]
        return {
            "schema_version": SCHEMA_VERSION,
            "as_of": self._timestamp(),
            "strategy": self.strategy_name,
            "held_symbols": normalized_symbols,
            "open_theses": open_theses,
            "current_position_rationales": position_rationales,
            "validated_lessons": lessons,
            "retrieval_policy": (
                "If an agent is holding a symbol and plans to add, reduce, or sell it, "
                "it should call search_memory for the open thesis first."
            ),
        }

    def export_artifacts(self, output_dir: str | os.PathLike[str] | None = None, *, prefix: str | None = None) -> dict[str, str]:
        import pandas as pd

        output_path = Path(output_dir or self.strategy_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        file_prefix = _safe_name(prefix or "agent_memory")
        table_map = {
            "memory_events": "memory_events",
            "memory_retrievals": "memory_retrievals",
            "memory_state": "memory_index",
        }
        written: dict[str, str] = {}
        with self._connect() as conn:
            for artifact_name, table_name in table_map.items():
                df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
                path = output_path / f"{file_prefix}_{artifact_name}.parquet"
                df.to_parquet(path, index=False, compression="zstd")
                written[artifact_name] = str(path)
        return written

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS memory_events (
                    event_id TEXT PRIMARY KEY,
                    sequence INTEGER NOT NULL UNIQUE,
                    timestamp TEXT NOT NULL,
                    wall_time TEXT NOT NULL,
                    strategy TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    subject_type TEXT,
                    subject_id TEXT,
                    symbol TEXT,
                    agent_name TEXT,
                    model_call_id TEXT,
                    retrieval_id TEXT,
                    text TEXT,
                    tags_json TEXT NOT NULL,
                    metadata_json TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    payload_hash TEXT NOT NULL,
                    schema_version INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_memory_events_subject ON memory_events(subject_type, subject_id);
                CREATE INDEX IF NOT EXISTS idx_memory_events_symbol ON memory_events(symbol);
                CREATE INDEX IF NOT EXISTS idx_memory_events_type ON memory_events(event_type);

                CREATE TABLE IF NOT EXISTS memory_index (
                    memory_id TEXT PRIMARY KEY,
                    latest_event_id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    status TEXT,
                    symbol TEXT,
                    text TEXT NOT NULL,
                    tags_json TEXT NOT NULL,
                    metadata_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    schema_version INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_memory_index_kind ON memory_index(kind);
                CREATE INDEX IF NOT EXISTS idx_memory_index_symbol ON memory_index(symbol);
                CREATE INDEX IF NOT EXISTS idx_memory_index_status ON memory_index(status);

                CREATE TABLE IF NOT EXISTS memory_retrievals (
                    retrieval_id TEXT PRIMARY KEY,
                    timestamp TEXT NOT NULL,
                    wall_time TEXT NOT NULL,
                    strategy TEXT NOT NULL,
                    agent_name TEXT,
                    model_call_id TEXT,
                    query TEXT NOT NULL,
                    kind TEXT,
                    symbol TEXT,
                    status TEXT,
                    result_limit INTEGER NOT NULL,
                    candidate_ids_json TEXT NOT NULL,
                    selected_ids_json TEXT NOT NULL,
                    rendered_text TEXT NOT NULL,
                    schema_version INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_memory_retrievals_symbol ON memory_retrievals(symbol);
                CREATE INDEX IF NOT EXISTS idx_memory_retrievals_agent ON memory_retrievals(agent_name);
                """
            )

    @staticmethod
    def _coerce_float(value: Any) -> float | None:
        try:
            parsed = float(value)
        except Exception:
            return None
        return parsed

    def _held_positions_by_symbol(self) -> dict[str, dict[str, Any]]:
        get_positions = getattr(self.strategy, "get_positions", None)
        if not callable(get_positions):
            return {}
        try:
            positions = get_positions(include_cash_positions=False)
        except TypeError:
            positions = get_positions()
        except Exception:
            return {}
        held: dict[str, dict[str, Any]] = {}
        for position in positions or []:
            asset = getattr(position, "asset", None)
            symbol = _normalize_symbol(getattr(asset, "symbol", None) or getattr(position, "symbol", None))
            if not symbol:
                continue
            quantity = self._coerce_float(getattr(position, "quantity", None))
            if quantity in (None, 0):
                continue
            last_price = None
            get_last_price = getattr(self.strategy, "get_last_price", None)
            if callable(get_last_price):
                try:
                    last_price = self._coerce_float(get_last_price(asset))
                except Exception:
                    last_price = None
            held[symbol] = {
                "symbol": symbol,
                "quantity": quantity,
                "asset_type": getattr(asset, "asset_type", None),
                "last_price": last_price,
                "market_value": quantity * last_price if quantity is not None and last_price is not None else None,
            }
        return held

    def _has_event_for_subject_on_date(self, *, event_type: str, subject_id: str, date_prefix: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT 1
                FROM memory_events
                WHERE event_type = ? AND subject_id = ? AND timestamp LIKE ?
                LIMIT 1
                """,
                (event_type, subject_id, f"{date_prefix}%"),
            ).fetchone()
        return row is not None

    def _record_open_thesis_outcome_observations(
        self,
        open_theses_rows: list[sqlite3.Row],
        normalized_symbols: list[str],
    ) -> None:
        if not open_theses_rows:
            return
        held_positions = self._held_positions_by_symbol()
        if not held_positions:
            return
        current_date = self._timestamp()[:10]
        wrote_event = False
        symbol_filter = set(normalized_symbols)
        for row in open_theses_rows:
            item = self._row_to_memory(row)
            thesis_id = str(item.get("memory_id") or "")
            symbol = _normalize_symbol(item.get("symbol") or (item.get("metadata") or {}).get("symbol"))
            if not thesis_id or not symbol:
                continue
            if symbol_filter and symbol not in symbol_filter:
                continue
            position = held_positions.get(symbol)
            if not position:
                continue
            if self._has_event_for_subject_on_date(
                event_type="thesis.outcome_observed",
                subject_id=thesis_id,
                date_prefix=current_date,
            ):
                continue
            metadata = {
                "thesis_id": thesis_id,
                "symbol": symbol,
                "status": "observed",
                "position": position,
                "observed_date": current_date,
            }
            self._append_event(
                event_type="thesis.outcome_observed",
                subject_type="thesis",
                subject_id=thesis_id,
                text=(
                    f"Observed open thesis for {symbol}: quantity={position.get('quantity')} "
                    f"last_price={position.get('last_price')} market_value={position.get('market_value')}"
                ),
                symbol=symbol,
                metadata=metadata,
                payload={"kind": "thesis_outcome", "status": "observed", **metadata},
            )
            wrote_event = True
        if wrote_event:
            self._export_artifacts_best_effort()

    def _record_projection_event(
        self,
        *,
        event_type: str,
        subject_type: str,
        subject_id: str,
        kind: str,
        status: str,
        text: str,
        symbol: str | None = None,
        tags: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        agent_name: str | None = None,
        model_call_id: str | None = None,
        retrieval_id: str | None = None,
    ) -> dict[str, Any]:
        event = self._append_event(
            event_type=event_type,
            subject_type=subject_type,
            subject_id=subject_id,
            text=text,
            symbol=symbol,
            tags=tags,
            metadata=metadata or {},
            payload={
                "kind": kind,
                "status": status,
                "text": text,
                "symbol": symbol,
                "tags": tags or [],
                "metadata": metadata or {},
            },
            agent_name=agent_name,
            model_call_id=model_call_id,
            retrieval_id=retrieval_id,
        )
        self._upsert_projection(
            memory_id=subject_id,
            event_id=event["event_id"],
            kind=kind,
            status=status,
            text=text,
            symbol=symbol,
            tags=tags,
            metadata=metadata,
            timestamp=event["timestamp"],
        )
        self._export_artifacts_best_effort()
        return self.get(subject_id) or event

    def _append_event(
        self,
        *,
        event_type: str,
        subject_type: str | None,
        subject_id: str | None,
        text: str,
        symbol: str | None = None,
        tags: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
        agent_name: str | None = None,
        model_call_id: str | None = None,
        retrieval_id: str | None = None,
    ) -> dict[str, Any]:
        timestamp = self._timestamp()
        wall_time = self._wall_time()
        event_id = f"event_{uuid.uuid4().hex}"
        tags_json = _json_dumps(list(tags or []))
        metadata_json = _json_dumps(metadata or {})
        payload_json = _json_dumps(payload or {})
        payload_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
        normalized_symbol = _normalize_symbol(symbol or (metadata or {}).get("symbol"))
        with self._connect() as conn:
            sequence = int(conn.execute("SELECT COALESCE(MAX(sequence), 0) + 1 FROM memory_events").fetchone()[0])
            conn.execute(
                """
                INSERT INTO memory_events (
                    event_id, sequence, timestamp, wall_time, strategy, event_type,
                    subject_type, subject_id, symbol, agent_name, model_call_id,
                    retrieval_id, text, tags_json, metadata_json, payload_json,
                    payload_hash, schema_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_id,
                    sequence,
                    timestamp,
                    wall_time,
                    self.strategy_name,
                    event_type,
                    subject_type,
                    subject_id,
                    normalized_symbol,
                    agent_name,
                    model_call_id,
                    retrieval_id,
                    str(text or "").strip(),
                    tags_json,
                    metadata_json,
                    payload_json,
                    payload_hash,
                    SCHEMA_VERSION,
                ),
            )
        return {
            "event_id": event_id,
            "sequence": sequence,
            "timestamp": timestamp,
            "strategy": self.strategy_name,
            "event_type": event_type,
            "subject_type": subject_type,
            "subject_id": subject_id,
            "symbol": normalized_symbol,
            "agent_name": agent_name,
            "model_call_id": model_call_id,
            "retrieval_id": retrieval_id,
            "text": str(text or "").strip(),
            "tags": list(tags or []),
            "metadata": metadata or {},
        }

    def _upsert_projection(
        self,
        *,
        memory_id: str,
        event_id: str,
        kind: str,
        status: str,
        text: str,
        symbol: str | None,
        tags: list[str] | None,
        metadata: dict[str, Any] | None,
        timestamp: str,
    ) -> None:
        normalized_symbol = _normalize_symbol(symbol or (metadata or {}).get("symbol"))
        with self._connect() as conn:
            existing = conn.execute("SELECT created_at FROM memory_index WHERE memory_id = ?", (memory_id,)).fetchone()
            created_at = existing["created_at"] if existing else timestamp
            conn.execute(
                """
                INSERT INTO memory_index (
                    memory_id, latest_event_id, kind, status, symbol, text,
                    tags_json, metadata_json, created_at, updated_at, schema_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(memory_id) DO UPDATE SET
                    latest_event_id = excluded.latest_event_id,
                    kind = excluded.kind,
                    status = excluded.status,
                    symbol = excluded.symbol,
                    text = excluded.text,
                    tags_json = excluded.tags_json,
                    metadata_json = excluded.metadata_json,
                    updated_at = excluded.updated_at,
                    schema_version = excluded.schema_version
                """,
                (
                    memory_id,
                    event_id,
                    kind,
                    status,
                    normalized_symbol,
                    str(text or "").strip(),
                    _json_dumps(list(tags or [])),
                    _json_dumps(metadata or {}),
                    created_at,
                    timestamp,
                    SCHEMA_VERSION,
                ),
            )

    def _compact_memory_item(self, item: dict[str, Any], *, max_chars: int) -> dict[str, Any]:
        return {
            "id": item.get("id") or item.get("memory_id"),
            "kind": item.get("kind"),
            "status": item.get("status"),
            "symbol": item.get("symbol"),
            "updated_at": item.get("updated_at"),
            "text": _compact_text(item.get("text"), limit=max_chars),
            "tags": item.get("tags") or [],
            "metadata": item.get("metadata") or {},
        }

    def _row_to_memory(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": row["memory_id"],
            "memory_id": row["memory_id"],
            "latest_event_id": row["latest_event_id"],
            "timestamp": row["updated_at"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "strategy": self.strategy_name,
            "kind": row["kind"],
            "status": row["status"],
            "symbol": row["symbol"],
            "text": row["text"],
            "tags": _json_loads(row["tags_json"], []),
            "metadata": _json_loads(row["metadata_json"], {}),
        }

    def _row_to_event_memory(self, row: sqlite3.Row) -> dict[str, Any]:
        payload = _json_loads(row["payload_json"], {})
        metadata = _json_loads(row["metadata_json"], {})
        return {
            "id": row["event_id"],
            "event_id": row["event_id"],
            "memory_id": row["subject_id"],
            "timestamp": row["timestamp"],
            "created_at": row["timestamp"],
            "updated_at": row["timestamp"],
            "strategy": self.strategy_name,
            "kind": payload.get("kind") or row["subject_type"],
            "status": payload.get("status"),
            "symbol": row["symbol"],
            "text": row["text"],
            "tags": _json_loads(row["tags_json"], []),
            "metadata": metadata,
            "event_type": row["event_type"],
            "source": "event_history",
        }

    def _timestamp(self) -> str:
        if hasattr(self.strategy, "get_datetime"):
            try:
                value = self.strategy.get_datetime()
                if hasattr(value, "isoformat"):
                    return value.isoformat()
            except Exception:
                pass
        return self._wall_time()

    @staticmethod
    def _wall_time() -> str:
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    def _export_artifacts_best_effort(self) -> None:
        if str(os.environ.get("LUMIBOT_MEMORY_EXPORT_PARQUET", "1")).strip().lower() in {"0", "false", "no", "off"}:
            return
        try:
            self.export_artifacts(self.strategy_dir, prefix="agent_memory")
        except Exception:
            pass
