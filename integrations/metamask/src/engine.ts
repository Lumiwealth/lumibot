import { Decimal } from "./numeric.js";
import {
  assert,
  sameAsset,
  type Binding,
  type Operation,
  type Provider,
  type TradeRequest,
} from "./contracts.js";
import { Store } from "./store.js";

export class Engine {
  private get active(): Set<string> {
    return this.store.executingWallets;
  }
  constructor(
    readonly store: Store,
    readonly provider: Provider,
  ) {}
  validate(binding: Binding, request: TradeRequest): void {
    if (request.kind === "perp") {
      assert(
        binding.markets.includes(request.symbol),
        "MARKET_DENIED",
        "Market is outside agent scope",
      );
      assert(
        request.leverage <= binding.maxLeverage,
        "LEVERAGE_LIMIT",
        "Leverage exceeds agent limit",
      );
    } else {
      assert(
        request.input.chainId === request.output.chainId,
        "BRIDGE_DENIED",
        "Cross-chain transfers are outside trading authority",
      );
      assert(
        binding.chains.includes(request.input.chainId),
        "CHAIN_DENIED",
        "Chain is outside agent scope",
      );
      assert(
        binding.assets.some((a) => sameAsset(a, request.input)) &&
          binding.assets.some((a) => sameAsset(a, request.output)),
        "ASSET_DENIED",
        "Token identity or decimals are outside agent scope",
      );
      assert(
        !sameAsset(request.input, request.output),
        "INVALID_PAIR",
        "Input and output token must differ",
      );
      assert(
        request.slippageBps <= binding.maxSlippageBps,
        "SLIPPAGE_LIMIT",
        "Slippage exceeds agent limit",
      );
      assert(
        new Decimal(request.inputAmount).decimalPlaces() <=
          request.input.decimals,
        "PRECISION",
        "Input amount exceeds token precision",
      );
    }
  }
  private persistExecution(binding: Binding, op: Operation): void {
    if (op.state === "unknown") {
      const current = this.store.binding(binding.id);
      assert(
        current.enabled &&
          current.expiresAt > Date.now() &&
          JSON.stringify(current) === JSON.stringify(binding),
        "REVOKED",
        "Authorization changed before the next signing step",
      );
    }
    this.store.save(op);
  }
  async submit(binding: Binding, request: TradeRequest): Promise<Operation> {
    this.validate(binding, request);
    const prior = this.store.get(binding.id, request.id);
    if (prior) {
      assert(
        JSON.stringify(prior.request) === JSON.stringify(request),
        "ID_CONFLICT",
        "Order ID already belongs to a different request",
      );
      return prior;
    }
    assert(
      !this.active.has(binding.wallet),
      "BUSY",
      "Wallet execution in progress",
    );
    this.active.add(binding.wallet);
    try {
      await this.provider.verify(binding);
      const estimate = await this.provider.estimate(binding, request);
      const reserved = this.store.reserve(binding, request, estimate.usd);
      if (!reserved.created) return reserved.operation;
      let op = reserved.operation;
      // Persist uncertainty BEFORE external submission. A process death must never trigger a second trade.
      op = {
        ...op,
        state: "unknown",
        ...(estimate.limitPrice
          ? { executionLimitPrice: estimate.limitPrice }
          : {}),
        updatedAt: Date.now(),
        message: "Submission started; reconcile before retrying",
      };
      this.store.save(op);
      try {
        op = await this.provider.execute(binding, op, (update) =>
          this.persistExecution(binding, update),
        );
      } catch (error) {
        // The provider may have persisted a job/hash before throwing. Preserve that recovery evidence.
        op = this.store.get(binding.id, request.id) ?? op;
        op = {
          ...op,
          message:
            op.state === "awaiting_approval"
              ? (op.message ?? "MetaMask approval pending; do not resubmit")
              : "Provider outcome requires reconciliation; do not resubmit",
          updatedAt: Date.now(),
        };
      }
      this.store.save(op);
      return op;
    } finally {
      this.active.delete(binding.wallet);
    }
  }
  async refresh(binding: Binding, id: string): Promise<Operation> {
    const op = this.store.get(binding.id, id);
    assert(op !== undefined, "NOT_FOUND", "Order not found");
    if (
      this.active.has(binding.wallet) ||
      ["filled", "canceled", "rejected"].includes(op.state)
    )
      return op;
    const refreshed = await this.provider.reconcile(binding, op);
    return this.store.replaceIfUnchanged(op, refreshed);
  }
  async resume(binding: Binding, id: string): Promise<Operation> {
    assert(
      !this.active.has(binding.wallet),
      "BUSY",
      "Wallet execution in progress",
    );
    this.active.add(binding.wallet);
    try {
      const op = this.store.get(binding.id, id);
      assert(
        op !== undefined && op.transactionStage === "allowance_confirmed",
        "RESUME_DENIED",
        "Only confirmed allowance operations can resume",
      );
      await this.provider.verify(binding);
      assert(
        new Decimal(
          (await this.provider.estimate(binding, op.request)).usd,
        ).lte(op.reservedUsd),
        "REPRICE_REQUIRED",
        "New valuation exceeds original reservation",
      );
      // Claim durably before awaiting signing. Unknown outcomes cannot be replayed.
      const claimed = {
        ...op,
        state: "unknown" as const,
        transactionStage: "trade" as const,
      };
      const current = this.store.binding(binding.id);
      assert(
        current.enabled &&
          current.expiresAt > Date.now() &&
          JSON.stringify(current) === JSON.stringify(binding),
        "REVOKED",
        "Binding changed during resume",
      );
      const actual = this.store.replaceIfUnchanged(op, claimed);
      assert(actual === claimed, "BUSY", "Operation changed during resume");
      try {
        const result = await this.provider.execute(binding, claimed, (update) =>
          this.persistExecution(binding, update),
        );
        this.store.save(result);
        return result;
      } catch {
        return this.store.get(binding.id, id) ?? claimed;
      }
    } finally {
      this.active.delete(binding.wallet);
    }
  }
  async recoverPerp(
    binding: Binding,
    id: string,
    providerOrderId: string,
  ): Promise<Operation> {
    assert(
      !this.active.has(binding.wallet),
      "BUSY",
      "Wallet execution in progress",
    );
    this.active.add(binding.wallet);
    try {
      const op = this.store.get(binding.id, id);
      assert(
        op?.request.kind === "perp" &&
          op.state === "unknown" &&
          !op.providerOrderId &&
          !op.signatureOnly,
        "RECOVERY_DENIED",
        "Only uncertain perpetual submissions without a venue ID can be linked",
      );
      assert(
        !this.store
          .operations(binding.id)
          .some((other) => other.providerOrderId === providerOrderId),
        "RECOVERY_DENIED",
        "Venue order is already linked to another operation",
      );
      const result = await this.provider.recoverPerp(
        binding,
        op,
        providerOrderId,
      );
      return this.store.replaceIfUnchanged(op, result);
    } finally {
      this.active.delete(binding.wallet);
    }
  }
  async cancel(binding: Binding, id: string): Promise<Operation> {
    const op = this.store.get(binding.id, id);
    assert(op !== undefined, "NOT_FOUND", "Order not found");
    const result = await this.provider.cancel(binding, op);
    return this.store.replaceIfUnchanged(op, result);
  }
}
