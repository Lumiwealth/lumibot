import {
  createPublicClient,
  http,
  TransactionNotFoundError,
  TransactionReceiptNotFoundError,
} from "viem";
import { sepolia, arbitrumSepolia } from "viem/chains";
import {
  assert,
  nativeFundingSchema,
  transactionHash,
  type Binding,
  type NativeFunding,
  BrokerError,
} from "./contracts.js";
import { Store } from "./store.js";

export interface FundingEvidence {
  transaction: {
    hash: string;
    from: string;
    to: string | null;
    value: bigint;
    nonce: bigint;
  } | null;
  receipt?: {
    hash: string;
    from: string;
    to: string | null;
    success: boolean;
    confirmations: number;
    timestamp: bigint;
  };
}
export type FundingReader = (record: NativeFunding) => Promise<FundingEvidence>;

/** Public testnet reads only. No transaction submission or wallet credentials. */
export const readFunding: FundingReader = async (record) => {
  assert(
    record.hash !== undefined,
    "HASH_REQUIRED",
    "Transaction hash required",
  );
  const chain = record.chainId === sepolia.id ? sepolia : arbitrumSepolia;
  const client = createPublicClient({
    chain,
    transport: http(
      record.chainId === sepolia.id
        ? "https://ethereum-sepolia-rpc.publicnode.com"
        : "https://sepolia-rollup.arbitrum.io/rpc",
      { timeout: 15000, retryCount: 0 },
    ),
  });
  assert(
    (await client.getChainId()) === record.chainId,
    "CHAIN_MISMATCH",
    "Funding RPC returned another network",
  );
  let tx;
  try {
    tx = await client.getTransaction({ hash: record.hash });
  } catch (error) {
    if (error instanceof TransactionNotFoundError) return { transaction: null };
    throw error;
  }
  const transaction = {
    hash: tx.hash,
    from: tx.from,
    to: tx.to,
    value: tx.value,
    nonce: BigInt(tx.nonce),
  };
  let receipt;
  try {
    receipt = await client.getTransactionReceipt({ hash: record.hash });
  } catch (error) {
    if (error instanceof TransactionReceiptNotFoundError)
      return { transaction };
    throw error;
  }
  const [head, block] = await Promise.all([
    client.getBlockNumber(),
    client.getBlock({ blockHash: receipt.blockHash }),
  ]);
  assert(
    tx.blockHash === receipt.blockHash &&
      tx.blockNumber === receipt.blockNumber &&
      block.hash === receipt.blockHash,
    "RECEIPT_MISMATCH",
    "Funding transaction and receipt disagree",
  );
  return {
    transaction,
    receipt: {
      hash: receipt.transactionHash,
      from: receipt.from,
      to: receipt.to,
      success: receipt.status === "success",
      confirmations: Number(
        head >= receipt.blockNumber ? head - receipt.blockNumber + 1n : 0n,
      ),
      timestamp: block.timestamp,
    },
  };
};

/** Owner-only funding journal. Unknown outcomes never cause another transfer. */
export class NativeFundings {
  private active = new Set<string>();
  constructor(
    private store: Store,
    private read: FundingReader = readFunding,
  ) {}

  prepare(
    binding: Binding,
    input: Pick<NativeFunding, "id" | "sender" | "chainId" | "amountWei"> & {
      nonce: string;
    },
  ): NativeFunding {
    assert(
      binding.network === "testnet",
      "NETWORK_DENIED",
      "Native setup funding supports testnet only",
    );
    const op = nativeFundingSchema.parse({
      ...input,
      agent: binding.id,
      wallet: binding.wallet,
      network: "testnet",
      state: "prepared",
      createdAt: Date.now(),
      updatedAt: Date.now(),
    });
    assert(
      op.sender !== op.wallet,
      "INVALID_RECIPIENT",
      "Choose a separate agent wallet",
    );
    assert(
      op.nonce !== undefined,
      "NONCE_REQUIRED",
      "Capture the sender nonce before opening wallet confirmation",
    );
    const rows = this.store.nativeFundings();
    const prior = rows.find(
      (value) => value.id === op.id && value.agent === op.agent,
    );
    if (prior) {
      assert(
        prior.sender === op.sender &&
          prior.chainId === op.chainId &&
          prior.amountWei === op.amountWei &&
          prior.nonce === op.nonce,
        "ID_CONFLICT",
        "Funding ID belongs to different transfer details",
      );
      return prior;
    }
    assert(
      !rows.some(
        (value) =>
          ["prepared", "submitted", "unknown"].includes(value.state) &&
          (value.wallet === op.wallet ||
            (value.sender === op.sender && value.chainId === op.chainId)),
      ),
      "RECONCILIATION_REQUIRED",
      "Refresh existing native funding or link its hash before another transfer",
    );
    this.store.saveNativeFunding(op);
    return op;
  }
  private find(binding: Binding, id: string): NativeFunding {
    const op = this.store
      .nativeFundings(binding.wallet)
      .find((row) => row.agent === binding.id && row.id === id);
    assert(op !== undefined, "NOT_FOUND", "Native funding not found");
    return op;
  }
  rejected(binding: Binding, id: string): NativeFunding {
    const op = this.find(binding, id);
    assert(
      !this.active.has(`${binding.id}:${id}`) &&
        op.state === "prepared" &&
        !op.hash,
      "REJECTION_DENIED",
      "A submitted funding request needs receipt reconciliation",
    );
    const result: NativeFunding = {
      ...op,
      state: "wallet_rejected",
      updatedAt: Date.now(),
      message:
        "Wallet rejected the funding request before returning a transaction hash.",
    };
    this.store.saveNativeFunding(result);
    return result;
  }
  async refresh(
    binding: Binding,
    id: string,
    candidate?: string,
  ): Promise<NativeFunding> {
    let op = this.find(binding, id);
    const original = op;
    const key = `${binding.id}:${id}`;
    if (this.active.has(key)) {
      assert(
        candidate === undefined,
        "BUSY",
        "Funding receipt check in progress",
      );
      return op;
    }
    if (
      ["confirmed", "reverted", "wallet_rejected", "replaced"].includes(
        op.state,
      )
    ) {
      assert(
        candidate === undefined ||
          candidate.toLowerCase() === op.hash ||
          candidate.toLowerCase() === op.replacementHash,
        "HASH_CONFLICT",
        "Funding is already resolved",
      );
      return op;
    }
    this.active.add(key);
    try {
      if (candidate !== undefined) {
        const hash = transactionHash.parse(candidate);
        assert(
          !this.store
            .nativeFundings(binding.wallet)
            .some(
              (row) =>
                row.id !== id &&
                (row.hash === hash || row.replacementHash === hash),
            ),
          "HASH_CONFLICT",
          "Transaction is already linked to another funding request",
        );
        if (op.verified && op.hash !== hash) {
          // A replacement can consume the original nonce only once. Do not
          // overwrite the original hash based on a pending or mismatched claim.
          const evidence = await this.read({ ...op, hash });
          const tx = evidence.transaction,
            receipt = evidence.receipt;
          assert(
            tx !== null,
            "REPLACEMENT_UNCONFIRMED",
            "Replacement transaction is not yet visible",
          );
          assert(
            op.nonce !== undefined &&
              tx.hash.toLowerCase() === hash &&
              tx.from.toLowerCase() === op.sender &&
              tx.nonce.toString() === op.nonce,
            "REPLACEMENT_MISMATCH",
            "Replacement must use the same sender and nonce on this testnet",
          );
          assert(
            receipt !== undefined && receipt.confirmations >= 2,
            "REPLACEMENT_UNCONFIRMED",
            "Keep the original record until the replacement has two block confirmations",
          );
          assert(
            receipt.hash.toLowerCase() === hash &&
              receipt.from.toLowerCase() === op.sender &&
              receipt.to?.toLowerCase() === tx.to?.toLowerCase() &&
              receipt.timestamp * 1000n >=
                BigInt(Math.floor(op.createdAt - 5000)),
            "REPLACEMENT_MISMATCH",
            "Replacement receipt does not match the verified transaction",
          );
          const sameFunding =
            tx.to?.toLowerCase() === op.wallet &&
            tx.value.toString() === op.amountWei;
          const result: NativeFunding = {
            ...op,
            replacementHash: hash,
            confirmations: receipt.confirmations,
            state: sameFunding
              ? receipt.success
                ? "confirmed"
                : "reverted"
              : "replaced",
            updatedAt: Date.now(),
            message: sameFunding
              ? receipt.success
                ? "Replacement funding transaction confirmed."
                : "Replacement funding transaction reverted; ETH was not received."
              : "Original funding was replaced by a mined transaction with different transfer details. Expected funding is not confirmed. Nothing will be resent automatically.",
          };
          this.store.saveNativeFunding(result);
          return result;
        }
        op = { ...op, hash, state: "submitted", updatedAt: Date.now() };
        this.store.saveNativeFunding(op);
      }
      if (!op.hash) return op;
      try {
        const evidence = await this.read(op);
        const matches = (value: {
          hash: string;
          from: string;
          to: string | null;
        }) =>
          value.hash.toLowerCase() === op.hash &&
          value.from.toLowerCase() === op.sender &&
          value.to?.toLowerCase() === op.wallet;
        if (!evidence.transaction) {
          op = {
            ...op,
            state: "submitted",
            confirmations: 0,
            message:
              "Transaction not yet visible on the selected testnet. Do not send again.",
          };
        } else {
          const tx = evidence.transaction,
            receipt = evidence.receipt;
          // A nonce persisted before the wallet prompt also proves consumption
          // when its original hash was lost before the first chain observation.
          if (
            !original.verified &&
            original.nonce !== undefined &&
            tx.hash.toLowerCase() === op.hash &&
            tx.from.toLowerCase() === op.sender &&
            tx.nonce.toString() === original.nonce &&
            (tx.to?.toLowerCase() !== op.wallet ||
              tx.value.toString() !== op.amountWei) &&
            receipt !== undefined &&
            receipt.confirmations >= 2 &&
            receipt.hash.toLowerCase() === op.hash &&
            receipt.from.toLowerCase() === op.sender &&
            receipt.to?.toLowerCase() === tx.to?.toLowerCase() &&
            receipt.timestamp * 1000n >= BigInt(Math.floor(op.createdAt - 5000))
          ) {
            const result: NativeFunding = {
              ...original,
              replacementHash: op.hash,
              state: "replaced",
              confirmations: receipt.confirmations,
              updatedAt: Date.now(),
              message:
                "Funding nonce was consumed by a mined transaction with different transfer details. Expected funding is not confirmed. Nothing will be resent automatically.",
            };
            this.store.saveNativeFunding(result);
            return result;
          }
          assert(
            matches(evidence.transaction) &&
              evidence.transaction.value.toString() === op.amountWei &&
              (op.nonce === undefined ||
                evidence.transaction.nonce.toString() === op.nonce),
            "FUNDING_MISMATCH",
            "Transaction sender, recipient or amount differs from the funding request",
          );
          op = {
            ...op,
            verified: true,
            nonce: evidence.transaction.nonce.toString(),
            state: "submitted",
            confirmations: 0,
            message: "Transaction verified; waiting for a mined receipt.",
          };
          if (evidence.receipt) {
            assert(
              matches(evidence.receipt) &&
                evidence.receipt.timestamp * 1000n >=
                  BigInt(Math.floor(op.createdAt - 5000)),
              "FUNDING_MISMATCH",
              "Receipt belongs to another transfer or predates this funding request",
            );
            op = {
              ...op,
              confirmations: evidence.receipt.confirmations,
              state:
                evidence.receipt.confirmations >= 2
                  ? evidence.receipt.success
                    ? "confirmed"
                    : "reverted"
                  : "submitted",
              message:
                evidence.receipt.confirmations >= 2
                  ? evidence.receipt.success
                    ? "Native ETH funding confirmed. Hyperliquid USDC collateral is separate."
                    : "Funding transaction reverted; native ETH was not received."
                  : "Receipt mined; waiting for two block confirmations.",
            };
          }
        }
      } catch (error) {
        op = {
          ...op,
          state: "unknown",
          verified:
            error instanceof BrokerError && error.code === "FUNDING_MISMATCH"
              ? false
              : op.verified,
          message:
            error instanceof BrokerError
              ? `${error.code}: ${error.message}`
              : "Receipt check unavailable. Keep the transaction hash and refresh; do not send again.",
        };
      }
      op = { ...op, updatedAt: Date.now() };
      this.store.saveNativeFunding(op);
      return op;
    } finally {
      this.active.delete(key);
    }
  }
  async list(binding: Binding): Promise<NativeFunding[]> {
    const rows = this.store.nativeFundings(binding.wallet);
    const results: NativeFunding[] = [];
    for (const row of rows) results.push(await this.refresh(binding, row.id));
    return results;
  }
}
