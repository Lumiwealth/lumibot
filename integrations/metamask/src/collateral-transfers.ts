import { assert, type Binding, type CollateralTransfer } from "./contracts.js";
import type { SigningBoundary } from "./perps-wallet.js";
import { Store } from "./store.js";

export interface CollateralProvider {
  verify(binding: Binding): Promise<void>;
  prepareCollateral(binding: Binding, amount: string): Promise<void>;
  submitCollateral(
    binding: Binding,
    amount: string,
    boundary: SigningBoundary,
  ): Promise<void>;
  collateralSignatureStatus(binding: Binding, id: string): Promise<string>;
}

/** Owner-only spot-to-perp funding. Runtime order tokens cannot call this API. */
export class CollateralTransfers {
  constructor(
    private store: Store,
    private provider: CollateralProvider,
  ) {}

  async submit(
    binding: Binding,
    id: string,
    amount: string,
  ): Promise<CollateralTransfer> {
    assert(
      binding.network === "testnet",
      "NETWORK_DENIED",
      "Setup collateral funding supports testnet only",
    );
    const previous = this.store
      .collateralTransfers(binding.wallet)
      .find((value) => value.agent === binding.id && value.id === id);
    if (previous) {
      assert(
        previous.amount === amount,
        "ID_CONFLICT",
        "Funding ID belongs to a different amount",
      );
      return previous;
    }
    assert(
      !this.store.executingWallets.has(binding.wallet),
      "BUSY",
      "Wallet execution in progress",
    );
    assert(
      !this.store
        .collateralTransfers(binding.wallet)
        .some((value) =>
          ["prepared", "awaiting_approval", "unknown"].includes(value.state),
        ) &&
        !this.store
          .operations(binding.id)
          .some((value) =>
            ["prepared", "awaiting_approval", "unknown"].includes(value.state),
          ),
      "RECONCILIATION_REQUIRED",
      "Resolve pending wallet work before funding",
    );
    this.store.executingWallets.add(binding.wallet);
    let op: CollateralTransfer = {
      id,
      agent: binding.id,
      wallet: binding.wallet,
      network: "testnet",
      amount,
      state: "prepared",
      createdAt: Date.now(),
      updatedAt: Date.now(),
    };
    const save = (update: Partial<CollateralTransfer>) => {
      op = { ...op, ...update, updatedAt: Date.now() };
      this.store.saveCollateralTransfer(op);
    };
    const recheck = async () => {
      await this.provider.verify(binding);
      assert(
        JSON.stringify(this.store.binding(binding.id)) ===
          JSON.stringify(binding),
        "POLICY_CHANGED",
        "Agent configuration changed during funding",
      );
      save({ state: "unknown", signatureOnly: false, approvalId: undefined });
    };
    try {
      await this.provider.verify(binding);
      await this.provider.prepareCollateral(binding, amount);
      save({});
      await this.provider.submitCollateral(binding, amount, {
        before: recheck,
        release: recheck,
        pending: (approvalId) =>
          save({ state: "awaiting_approval", approvalId, signatureOnly: true }),
      });
      save({
        state: "submitted",
        signatureOnly: false,
        approvalId: undefined,
        message:
          "Funding submitted. Refresh balances to verify available collateral.",
      });
      return op;
    } catch {
      save({
        state: op.state === "prepared" ? "rejected" : op.state,
        message:
          op.state === "prepared"
            ? "Funding preparation failed; no transfer submitted. Refresh balances and review Guard policy."
            : op.state === "awaiting_approval"
              ? "MetaMask signature pending. Check your registered approval channel; do not repeat this request."
              : "Funding outcome unknown. Inspect the venue before further wallet actions; this request will not be replayed.",
      });
      return op;
    } finally {
      this.store.executingWallets.delete(binding.wallet);
    }
  }

  async list(binding: Binding): Promise<CollateralTransfer[]> {
    const rows = this.store.collateralTransfers(binding.wallet);
    if (this.store.executingWallets.has(binding.wallet)) return rows;
    const results: CollateralTransfer[] = [];
    for (const op of rows) {
      // Unknown is persisted before signing. An inactive prepared request cannot
      // have released a signature, so interrupted preparation needs no replay.
      if (op.state === "prepared") {
        const resolved: CollateralTransfer = {
          ...op,
          state: "rejected",
          updatedAt: Date.now(),
          message:
            "Preparation interrupted before signing; no transfer submitted.",
        };
        this.store.saveCollateralTransfer(resolved);
        results.push(resolved);
        continue;
      }
      if (
        op.state === "awaiting_approval" &&
        op.signatureOnly &&
        op.approvalId
      ) {
        const status = await this.provider.collateralSignatureStatus(
          binding,
          op.approvalId,
        );
        if (["SIGNED", "DENIED", "EXPIRED", "FAILED"].includes(status)) {
          const resolved: CollateralTransfer = {
            ...op,
            state: "rejected",
            updatedAt: Date.now(),
            message: `Interrupted signature ${status.toLowerCase()}; no collateral transfer will be replayed.`,
          };
          this.store.saveCollateralTransfer(resolved);
          results.push(resolved);
          continue;
        }
      }
      results.push(op);
    }
    return results;
  }
}
