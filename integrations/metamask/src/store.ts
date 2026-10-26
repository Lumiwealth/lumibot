import { DatabaseSync } from "node:sqlite";
import { createHash, timingSafeEqual } from "node:crypto";
import { chmodSync } from "node:fs";
import { Decimal } from "./numeric.js";
import { z } from "zod";
import {
  assert,
  bindingSchema,
  operationSchema,
  type Binding,
  type Operation,
  type TradeRequest,
} from "./contracts.js";

export function digest(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}
const row = z.object({ data: z.string() });
export class Store {
  readonly db: DatabaseSync;
  constructor(path: string) {
    this.db = new DatabaseSync(path);
    if (path !== ":memory:") chmodSync(path, 0o600);
    this.db.exec(`PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;
      CREATE TABLE IF NOT EXISTS bindings (id TEXT PRIMARY KEY, wallet TEXT UNIQUE NOT NULL, data TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS operations (agent TEXT NOT NULL, id TEXT NOT NULL, data TEXT NOT NULL, PRIMARY KEY(agent,id));`);
  }
  putBinding(binding: Binding): void {
    bindingSchema.parse(binding);
    const previous = this.bindings().find((b) => b.id === binding.id);
    assert(
      !previous || previous.wallet === binding.wallet,
      "WALLET_IMMUTABLE",
      "Create a new agent ID to change wallets",
    );
    this.db
      .prepare(
        "INSERT INTO bindings VALUES (?,?,?) ON CONFLICT(id) DO UPDATE SET wallet=excluded.wallet,data=excluded.data",
      )
      .run(binding.id, binding.wallet, JSON.stringify(binding));
  }
  bindings(): Binding[] {
    return this.db
      .prepare("SELECT data FROM bindings")
      .all()
      .map((v) => bindingSchema.parse(JSON.parse(row.parse(v).data)));
  }
  binding(id: string): Binding {
    const value = this.db
      .prepare("SELECT data FROM bindings WHERE id=?")
      .get(id);
    assert(value !== undefined, "NOT_FOUND", "Agent binding not found");
    return bindingSchema.parse(JSON.parse(row.parse(value).data));
  }
  authorize(token: string): Binding {
    const hash = Buffer.from(digest(token), "hex");
    const binding = this.bindings().find((b) =>
      timingSafeEqual(Buffer.from(b.tokenHash, "hex"), hash),
    );
    assert(
      binding !== undefined &&
        binding.enabled &&
        binding.expiresAt > Date.now(),
      "UNAUTHORIZED",
      "Agent access is missing, disabled, or expired",
    );
    return binding;
  }
  operations(agent: string): Operation[] {
    return this.db
      .prepare("SELECT data FROM operations WHERE agent=?")
      .all(agent)
      .map((v) => operationSchema.parse(JSON.parse(row.parse(v).data)));
  }
  get(agent: string, id: string): Operation | undefined {
    return this.operations(agent).find((op) => op.id === id);
  }
  save(op: Operation): void {
    operationSchema.parse(op);
    this.db
      .prepare(
        "INSERT INTO operations VALUES (?,?,?) ON CONFLICT(agent,id) DO UPDATE SET data=excluded.data",
      )
      .run(op.agent, op.id, JSON.stringify(op));
  }
  reserve(
    binding: Binding,
    request: TradeRequest,
    usd: string,
    now = Date.now(),
  ): { operation: Operation; created: boolean } {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const current = this.binding(binding.id);
      assert(
        current.enabled && current.expiresAt > now,
        "REVOKED",
        "Agent authorization expired or was revoked",
      );
      assert(
        JSON.stringify(current) === JSON.stringify(binding),
        "POLICY_CHANGED",
        "Policy changed during preparation",
      );
      const existing = this.get(binding.id, request.id);
      if (existing) {
        assert(
          JSON.stringify(existing.request) === JSON.stringify(request),
          "ID_CONFLICT",
          "Order ID already belongs to a different request",
        );
        this.db.exec("COMMIT");
        return { operation: existing, created: false };
      }
      const operations = this.operations(binding.id);
      assert(
        !operations.some((op) =>
          ["prepared", "awaiting_approval", "unknown"].includes(op.state),
        ),
        "RECONCILIATION_REQUIRED",
        "Resolve pending or uncertain submission before placing another trade",
      );
      const used = operations
        .filter(
          (op) =>
            !(
              op.state === "rejected" && new Decimal(op.filledQuantity).eq(0)
            ) &&
            (op.createdAt > now - 86400000 ||
              ["submitted", "open", "partial_fill"].includes(op.state)),
        )
        .reduce((sum, op) => sum.plus(op.reservedUsd), new Decimal(0));
      assert(
        new Decimal(usd).gt(0) && new Decimal(usd).lte(current.maxOrderUsd),
        "ORDER_LIMIT",
        "Trade exceeds per-order limit or has no reliable valuation",
      );
      assert(
        used.plus(usd).lte(current.maxDailyUsd),
        "DAILY_LIMIT",
        "Trade exceeds rolling 24-hour limit",
      );
      const op: Operation = {
        id: request.id,
        agent: binding.id,
        request,
        createdAt: now,
        updatedAt: now,
        state: "prepared",
        reservedUsd: usd,
        filledQuantity: "0",
        fees: [],
        hashes: [],
      };
      this.save(op);
      this.db.exec("COMMIT");
      return { operation: op, created: true };
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }
  replaceIfUnchanged(before: Operation, after: Operation): Operation {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const current = this.get(before.agent, before.id);
      assert(current !== undefined, "NOT_FOUND", "Operation disappeared");
      if (JSON.stringify(current) !== JSON.stringify(before)) {
        this.db.exec("COMMIT");
        return current;
      }
      this.save(after);
      this.db.exec("COMMIT");
      return after;
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }
  close(): void {
    this.db.close();
  }
}
