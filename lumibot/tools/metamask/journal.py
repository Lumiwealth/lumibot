"""SQLite reservations and effect checkpoints shared by native broker operations."""

from __future__ import annotations

import json
import os
import sqlite3
import stat
import time
from contextlib import contextmanager
from decimal import ROUND_CEILING, Context, Decimal, localcontext
from pathlib import Path
from threading import Lock, RLock

from .models import decimal
from .provider import MetaMaskError, address, identifier, json_object

STATES = frozenset(
    {
        "prepared",
        "awaiting_approval",
        "submitted",
        "open",
        "partial_fill",
        "filled",
        "canceled",
        "rejected",
        "unknown",
    }
)
UNCERTAIN = frozenset({"prepared", "awaiting_approval", "unknown"})
OUTSTANDING = frozenset({"submitted", "open", "partial_fill"})
SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS bindings(id TEXT PRIMARY KEY,wallet TEXT UNIQUE NOT NULL,data TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS operations(agent TEXT NOT NULL,id TEXT NOT NULL,data TEXT NOT NULL,PRIMARY KEY(agent,id));
CREATE TABLE IF NOT EXISTS policy_approvals(id TEXT PRIMARY KEY,wallet TEXT NOT NULL,data TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS native_funding(
    agent TEXT NOT NULL,id TEXT NOT NULL,wallet TEXT NOT NULL,data TEXT NOT NULL,PRIMARY KEY(agent,id));
CREATE TABLE IF NOT EXISTS collateral_transfers(
    agent TEXT NOT NULL,id TEXT NOT NULL,wallet TEXT NOT NULL,data TEXT NOT NULL,PRIMARY KEY(agent,id));
CREATE TABLE IF NOT EXISTS native_effects(
    agent TEXT NOT NULL,operation TEXT NOT NULL,step TEXT NOT NULL,
    data TEXT NOT NULL,PRIMARY KEY(agent,operation,step));
CREATE TABLE IF NOT EXISTS native_metadata(name TEXT PRIMARY KEY,value TEXT NOT NULL);
"""


def _json(value: object) -> str:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def sqlite_busy(error: sqlite3.OperationalError) -> bool:
    """Recognize ownership contention on Python 3.10 and newer SQLite APIs."""
    code = getattr(error, "sqlite_errorcode", None)
    if isinstance(code, int):
        return code & 0xFF in (5, 6)  # SQLITE_BUSY and SQLITE_LOCKED.
    message = str(error).lower()
    return any(
        message == text or message.startswith(text + ":") for text in ("database is locked", "database table is locked")
    )


class Journal:
    """One execution owner per journal, with locks released by process death.

    Existing service tables and opaque pending records are retained. Native
    signing evidence lives separately so legacy operation JSON is not erased.
    """

    def __init__(self, path: Path):
        supplied = Path(path).expanduser()
        if supplied.is_symlink():
            raise MetaMaskError("UNSAFE_STATE", "Journal path must not be a symlink")
        self.path = supplied.parent.resolve() / supplied.name
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        parent = self.path.parent.stat()
        if parent.st_uid != os.getuid() or parent.st_mode & 0o022:
            raise MetaMaskError("UNSAFE_STATE", "Journal directory must be writable only by its owner")
        for candidate in (self.path, Path(str(self.path) + ".owner.sqlite")):
            if candidate.exists() or candidate.is_symlink():
                info = candidate.lstat()
                if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.getuid():
                    raise MetaMaskError("UNSAFE_STATE", "Journal files must be owned, unlinked regular files")
        self._lock = RLock()
        self._execution_locks = {}
        self._closed = False
        self._owner = sqlite3.connect(str(self.path) + ".owner.sqlite", timeout=0, check_same_thread=False)
        try:
            os.chmod(str(self.path) + ".owner.sqlite", 0o600)
            self._owner.executescript(
                "PRAGMA journal_mode=DELETE; CREATE TABLE IF NOT EXISTS owner(id INTEGER); BEGIN EXCLUSIVE;"
            )
        except sqlite3.OperationalError as error:
            self._owner.close()
            if sqlite_busy(error):
                raise MetaMaskError(
                    "STATE_IN_USE", "Another execution owner already holds this wallet journal"
                ) from None
            raise
        try:
            self.db = sqlite3.connect(self.path, timeout=5, isolation_level=None, check_same_thread=False)
            os.chmod(self.path, 0o600)
            self.db.executescript(SCHEMA)
        except BaseException:
            self._owner.close()
            if hasattr(self, "db"):
                self.db.close()
            raise

    def execution_lock(self, wallet: str):
        with self._lock:
            return self._execution_locks.setdefault(address(wallet), Lock())

    def bind(self, value: dict[str, object]):
        agent = identifier(value.get("id"), "AGENT_ID")
        wallet = address(value.get("wallet"))
        if value.get("network") not in {"testnet", "mainnet"} or not isinstance(value.get("enabled"), bool):
            raise MetaMaskError("INVALID_BINDING", "Binding requires network and enabled state")
        expiry = value.get("expiresAt")
        if isinstance(expiry, bool) or not isinstance(expiry, (int, float)) or not decimal(expiry, positive=True):
            raise MetaMaskError("INVALID_BINDING", "Binding expiry must be finite Unix milliseconds")
        decimal(value.get("maxOrderUsd"), positive=True)
        decimal(value.get("maxDailyUsd"), positive=True)
        with self._lock:
            prior = self.db.execute("SELECT data FROM bindings WHERE id=?", (agent,)).fetchone()
            current = json_object(json.loads(prior[0]), "binding") if prior else {}
            if current and (
                current.get("wallet") != wallet
                or current.get("network") != value["network"]
                or (current.get("projectId") is not None and current.get("projectId") != value.get("projectId"))
            ):
                raise MetaMaskError("WALLET_IMMUTABLE", "Create a new binding to change wallet, project or network")
            merged = {**current, **value, "wallet": wallet}
            try:
                self.db.execute(
                    "INSERT INTO bindings VALUES (?,?,?) ON CONFLICT(id) DO UPDATE SET data=excluded.data",
                    (agent, wallet, _json(merged)),
                )
            except sqlite3.IntegrityError:
                raise MetaMaskError(
                    "WALLET_ALREADY_BOUND", "A dedicated wallet cannot belong to another agent binding"
                ) from None

    def binding(self, agent: str) -> dict[str, object]:
        with self._lock:
            row = self.db.execute("SELECT data FROM bindings WHERE id=?", (agent,)).fetchone()
            if row is None:
                raise MetaMaskError("NOT_FOUND", "Agent binding was not found")
            return json_object(json.loads(row[0]), "binding")

    @contextmanager
    def submission(self, agent: str, expected_binding: dict[str, object], *, cancellation=False):
        """Serialize owner revocation with the final external-effect handoff."""
        with self._lock:
            current = self.binding(agent)
            if not cancellation and (
                current.get("enabled") is not True or decimal(current.get("expiresAt")) <= int(time.time() * 1000)
            ):
                raise MetaMaskError("REVOKED", "Agent authorization expired or was revoked")
            if current != expected_binding:
                raise MetaMaskError("POLICY_CHANGED", "Binding changed before execution handoff")
            yield

    def operations(self, agent: str) -> list[dict[str, object]]:
        with self._lock:
            rows = self.db.execute("SELECT data FROM operations WHERE agent=? ORDER BY rowid", (agent,)).fetchall()
            return [json_object(json.loads(row[0]), "operation") for row in rows]

    def owner_operations(self, agent: str) -> dict[str, list[dict[str, object]]]:
        """Retain inspectable legacy owner receipts without replaying mutations."""
        wallet = self.binding(agent)["wallet"]
        with self._lock:
            return {
                table: [
                    json_object(json.loads(row[0]), "owner operation")
                    for row in self.db.execute(f"SELECT data FROM {table} WHERE wallet=?", (wallet,)).fetchall()
                ]
                for table in ("policy_approvals", "native_funding", "collateral_transfers")
            }

    def get(self, agent: str, operation_id: str) -> dict[str, object] | None:
        with self._lock:
            row = self.db.execute(
                "SELECT data FROM operations WHERE agent=? AND id=?", (agent, operation_id)
            ).fetchone()
            return json_object(json.loads(row[0]), "operation") if row else None

    def save(self, operation: dict[str, object]):
        agent = identifier(operation.get("agent"), "AGENT_ID")
        operation_id = identifier(operation.get("id"), "operation ID")
        if not isinstance(operation.get("state"), str) or operation.get("state") not in STATES:
            raise MetaMaskError("INVALID_STATE", "Operation state is invalid")
        if decimal(operation.get("filledQuantity", "0")) < 0:
            raise MetaMaskError("INVALID_AMOUNT", "Cumulative fills cannot be negative")
        decimal(operation.get("reservedUsd"), positive=True)
        with self._lock:
            current = self.get(agent, operation_id)
            if current is not None:
                if current.get("request") != operation.get("request") or current.get("createdAt") != operation.get(
                    "createdAt"
                ):
                    raise MetaMaskError("ID_CONFLICT", "Operation identity and original request cannot change")
                if decimal(current.get("reservedUsd"), positive=True) != decimal(
                    operation.get("reservedUsd"), positive=True
                ):
                    raise MetaMaskError("RESERVATION_IMMUTABLE", "Execution snapshots cannot change reserved notional")
                if decimal(operation.get("filledQuantity", "0")) < decimal(current.get("filledQuantity", "0")):
                    raise MetaMaskError("FILL_REGRESSION", "Known cumulative fills cannot decrease")
                if decimal(operation.get("feeAdjustedReservationUsd", operation["reservedUsd"])) < decimal(
                    current.get("feeAdjustedReservationUsd", current["reservedUsd"])
                ):
                    raise MetaMaskError("RESERVATION_REGRESSION", "Recorded chain fee charges cannot decrease")
            if decimal(operation.get("feeAdjustedReservationUsd", operation["reservedUsd"])) < decimal(
                operation["reservedUsd"]
            ):
                raise MetaMaskError("INVALID_AMOUNT", "Fee-adjusted reservation cannot understate original reservation")
            self.db.execute(
                "INSERT INTO operations VALUES (?,?,?) ON CONFLICT(agent,id) DO UPDATE SET data=excluded.data",
                (agent, operation_id, _json(operation)),
            )

    def replace_if_unchanged(self, before: dict[str, object], after: dict[str, object]) -> dict[str, object]:
        agent = identifier(before.get("agent"), "AGENT_ID")
        operation_id = identifier(before.get("id"), "operation ID")
        with self._lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                current = self.get(agent, operation_id)
                if current is None:
                    raise MetaMaskError("NOT_FOUND", "Execution operation disappeared")
                if current == before:
                    self.save(after)
                    current = after
                self.db.execute("COMMIT")
                return current
            except BaseException:
                self.db.execute("ROLLBACK")
                raise

    def reserve(
        self,
        agent: str,
        request: dict[str, object],
        usd: str,
        *,
        expected_binding: dict[str, object],
        now: int | None = None,
    ):
        now = int(time.time() * 1000) if now is None else now
        operation_id = identifier(request.get("id"), "operation ID")
        amount = decimal(usd, positive=True)
        with self._lock, localcontext(Context(prec=160, rounding=ROUND_CEILING)):
            self.db.execute("BEGIN IMMEDIATE")
            try:
                binding = self.binding(agent)
                if binding != expected_binding:
                    raise MetaMaskError("POLICY_CHANGED", "Binding changed while execution price was being prepared")
                if binding.get("enabled") is not True or decimal(binding.get("expiresAt")) <= now:
                    raise MetaMaskError("REVOKED", "Agent authorization expired or was revoked")
                prior = self.get(agent, operation_id)
                if prior is not None:
                    if prior.get("request") != request:
                        raise MetaMaskError("ID_CONFLICT", "Order ID already belongs to different intent")
                    self.db.execute("COMMIT")
                    return prior, False
                pending = self.db.execute(
                    "SELECT data FROM collateral_transfers WHERE wallet=?", (binding["wallet"],)
                ).fetchall()
                if any(json_object(json.loads(row[0])).get("state") in UNCERTAIN for row in pending):
                    raise MetaMaskError(
                        "RECONCILIATION_REQUIRED", "Resolve pending collateral movement before another trade"
                    )
                operations = self.operations(agent)
                if any(item.get("reconciliationErrorCode") for item in operations):
                    raise MetaMaskError(
                        "RECONCILIATION_REQUIRED", "Resolve inconsistent execution evidence before further spending"
                    )
                if any(item.get("budgetReviewRequired") is True for item in operations):
                    raise MetaMaskError(
                        "FEE_BUDGET_REVIEW_REQUIRED", "Review recorded chain fee overrun before further spending"
                    )
                if any(item.get("state") in UNCERTAIN for item in operations):
                    raise MetaMaskError(
                        "RECONCILIATION_REQUIRED", "Resolve pending or uncertain execution before another trade"
                    )
                used = Decimal(0)
                for item in operations:
                    possible_chain_cost = item.get("hashes") or any(
                        effect.get("transaction") for effect in self.effects(agent, str(item["id"])).values()
                    )
                    if (
                        item.get("state") == "rejected"
                        and decimal(item.get("filledQuantity", "0")) == 0
                        and not possible_chain_cost
                    ):
                        continue
                    if decimal(item.get("createdAt")) > now - 86_400_000 or item.get("state") in OUTSTANDING:
                        used += max(
                            decimal(item.get("reservedUsd")),
                            decimal(item.get("feeAdjustedReservationUsd", item["reservedUsd"])),
                        )
                if amount > decimal(binding.get("maxOrderUsd"), positive=True):
                    raise MetaMaskError("ORDER_LIMIT", "Trade exceeds its per-order notional limit")
                if used + amount > decimal(binding.get("maxDailyUsd"), positive=True):
                    raise MetaMaskError("DAILY_LIMIT", "Trade exceeds its rolling daily reservation")
                operation = {
                    "id": operation_id,
                    "agent": agent,
                    "request": request,
                    "createdAt": now,
                    "updatedAt": now,
                    "state": "prepared",
                    "reservedUsd": format(amount, "f"),
                    "filledQuantity": "0",
                    "fees": [],
                    "hashes": [],
                }
                self.save(operation)
                self.db.execute("COMMIT")
                return operation, True
            except BaseException:
                self.db.execute("ROLLBACK")
                raise

    def save_effect(self, operation: dict[str, object], step: str, effect: dict[str, object]):
        agent = identifier(operation.get("agent"), "AGENT_ID")
        operation_id = identifier(operation.get("id"), "operation ID")
        identifier(step, "effect step")
        with self._lock:
            previous = self.effect(agent, operation_id, step)
            if previous is not None:
                for key in (
                    "requestId",
                    "origin",
                    "clientOrigin",
                    "nonce",
                    "action",
                    "typedData",
                    "typedDataHash",
                    "network",
                    "method",
                    "wallet",
                    "projectId",
                    "protocolPayload",
                    "chainId",
                    "expiresAfter",
                    "ownerEpoch",
                    "binding",
                    "clientOrderId",
                    "transaction",
                    "quoteId",
                    "trade",
                    "approvals",
                    "minimumOutput",
                    "expiresAt",
                    "feeEstimates",
                    "nativeUsdPrice",
                    "feeEstimate",
                ):
                    if previous.get(key) != effect.get(key):
                        raise MetaMaskError("EFFECT_IMMUTABLE", "Persisted signing identity and action cannot change")
            self.db.execute(
                "INSERT INTO native_effects VALUES (?,?,?,?) "
                "ON CONFLICT(agent,operation,step) DO UPDATE SET data=excluded.data",
                (agent, operation_id, step, _json(effect)),
            )

    def acknowledge_fee_overrun(self, agent: str, operation_id: str, expected_overrun: dict[str, object]):
        with self._lock:
            operation = self.get(agent, operation_id)
            if operation is None:
                raise MetaMaskError("NOT_FOUND", "Fee overrun operation was not found")
            if not expected_overrun or operation.get("feeBudgetOverrun") != expected_overrun:
                raise MetaMaskError("REVIEW_CHANGED", "Review the exact current fee overrun before acknowledgment")
            if operation.get("feeAdjustedReservationUsd") is None:
                raise MetaMaskError(
                    "FEE_VALUATION_REQUIRED", "Legacy overrun needs verified USD valuation before acknowledgment"
                )
            if operation.get("budgetReviewRequired") is True:
                operation = {**operation, "budgetReviewRequired": False, "feeBudgetReviewedAt": int(time.time() * 1000)}
                self.save(operation)
            return operation

    def effect(self, agent: str, operation_id: str, step: str) -> dict[str, object] | None:
        with self._lock:
            row = self.db.execute(
                "SELECT data FROM native_effects WHERE agent=? AND operation=? AND step=?",
                (agent, operation_id, step),
            ).fetchone()
            return json_object(json.loads(row[0]), "execution effect") if row else None

    def effects(self, agent: str, operation_id: str) -> dict[str, dict[str, object]]:
        with self._lock:
            rows = self.db.execute(
                "SELECT step,data FROM native_effects WHERE agent=? AND operation=? ORDER BY rowid",
                (agent, operation_id),
            ).fetchall()
            return {step: json_object(json.loads(data), "execution effect") for step, data in rows}

    def next_nonce(self, *, now: int | None = None) -> int:
        now = int(time.time() * 1000) if now is None else now
        with self._lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                previous = self.db.execute("SELECT value FROM native_metadata WHERE name='nonce'").fetchone()
                value = max(now, int(previous[0]) + 1 if previous else now)
                self.db.execute(
                    "INSERT INTO native_metadata VALUES ('nonce',?) "
                    "ON CONFLICT(name) DO UPDATE SET value=excluded.value",
                    (str(value),),
                )
                self.db.execute("COMMIT")
                return value
            except BaseException:
                self.db.execute("ROLLBACK")
                raise

    def close(self):
        with self._lock:
            if not self._closed:
                self.db.close()
                self._owner.close()
                self._closed = True
