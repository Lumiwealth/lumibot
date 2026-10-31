import { test } from "node:test";
import { strict as assert } from "node:assert";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { NativeFundings, type FundingEvidence } from "../src/native-funding.js";
import { bindingSchema } from "../src/contracts.js";
import { Store, digest } from "../src/store.js";

const recipient = "0x" + "1".repeat(40),
  sender = "0x" + "2".repeat(40);
const hash = `0x${"a".repeat(64)}`,
  otherHash = `0x${"b".repeat(64)}`;
const binding = bindingSchema.parse({
  id: "agent",
  wallet: recipient,
  tokenHash: digest("runtime"),
  enabled: true,
  expiresAt: Date.now() + 60000,
  network: "testnet",
  chains: [11155111],
  assets: [],
  markets: ["BTC"],
  maxOrderUsd: "25",
  maxDailyUsd: "100",
  maxSlippageBps: 50,
  maxLeverage: 1,
  policyDigest: digest("policy"),
});
const input = {
  id: "fund",
  sender,
  chainId: 11155111 as const,
  amountWei: "1000000000000001",
  nonce: "7",
};
function fixture() {
  const store = new Store(":memory:");
  store.putBinding(binding);
  let evidence: FundingEvidence = { transaction: null };
  let reads = 0;
  const funding = new NativeFundings(store, async () => {
    reads++;
    return evidence;
  });
  const mined = (confirmations = 2): FundingEvidence => ({
    transaction: {
      hash,
      from: sender,
      to: recipient,
      value: BigInt(input.amountWei),
      nonce: 7n,
    },
    receipt: {
      hash,
      from: sender,
      to: recipient,
      success: true,
      confirmations,
      timestamp: BigInt(Math.floor(Date.now() / 1000)),
    },
  });
  return {
    store,
    funding,
    mined,
    evidence: (value: FundingEvidence) => {
      evidence = value;
    },
    reads: () => reads,
  };
}

test("funding intent is idempotent, exact in wei and blocks another unresolved transfer", () => {
  const f = fixture();
  try {
    const op = f.funding.prepare(binding, input);
    assert.equal(op.amountWei, "1000000000000001");
    assert.deepEqual(f.funding.prepare(binding, input), op);
    assert.throws(
      () => f.funding.prepare(binding, { ...input, amountWei: "1" }),
      { code: "ID_CONFLICT" },
    );
    assert.throws(
      () => f.funding.prepare(binding, { ...input, id: "another" }),
      { code: "RECONCILIATION_REQUIRED" },
    );
    assert.throws(() => f.funding.prepare(binding, { ...input, nonce: "8" }), {
      code: "ID_CONFLICT",
    });
    const another = { ...binding, id: "other", wallet: `0x${"3".repeat(40)}` };
    f.store.putBinding(another);
    assert.throws(() => f.funding.prepare(another, { ...input, id: "other" }), {
      code: "RECONCILIATION_REQUIRED",
    });
    assert.throws(
      () => f.funding.prepare({ ...binding, network: "mainnet" }, input),
      { code: "NETWORK_DENIED" },
    );
    assert.throws(
      () => f.funding.prepare(binding, { ...input, sender: recipient }),
      { code: "INVALID_RECIPIENT" },
    );
    assert.equal(f.reads(), 0);
  } finally {
    f.store.close();
  }
});

test("a hash and a mined block are insufficient until identity and two confirmations match", async () => {
  const f = fixture();
  try {
    f.funding.prepare(binding, input);
    assert.equal(
      (await f.funding.refresh(binding, input.id, hash)).state,
      "submitted",
    );
    f.evidence(f.mined(1));
    assert.equal(
      (await f.funding.refresh(binding, input.id)).state,
      "submitted",
    );
    assert.throws(() => f.funding.rejected(binding, input.id), {
      code: "REJECTION_DENIED",
    });
    await assert.rejects(
      () => f.funding.refresh(binding, input.id, otherHash),
      { code: "REPLACEMENT_MISMATCH" },
    );
    f.evidence(f.mined());
    const confirmed = await f.funding.refresh(binding, input.id);
    assert.equal(confirmed.state, "confirmed");
    assert.equal(confirmed.confirmations, 2);
    assert.equal(confirmed.verified, true);
    f.funding.prepare(binding, { ...input, id: "another" });
    await assert.rejects(() => f.funding.refresh(binding, "another", hash), {
      code: "HASH_CONFLICT",
    });
  } finally {
    f.store.close();
  }
});

test("wrong sender, recipient, value, hash or old receipt cannot confirm funding", async () => {
  for (const change of [
    "sender",
    "recipient",
    "value",
    "hash",
    "receipt",
    "time",
  ]) {
    const f = fixture();
    try {
      const op = f.funding.prepare(binding, input);
      const evidence = f.mined();
      assert(evidence.transaction && evidence.receipt);
      if (change === "sender") evidence.transaction.from = recipient;
      if (change === "recipient") evidence.transaction.to = sender;
      if (change === "value") evidence.transaction.value -= 1n;
      if (change === "hash") evidence.transaction.hash = otherHash;
      if (change === "receipt") evidence.receipt.to = sender;
      if (change === "time")
        evidence.receipt.timestamp = BigInt(
          Math.floor(op.createdAt / 1000) - 6,
        );
      f.evidence(evidence);
      const result = await f.funding.refresh(binding, input.id, hash);
      if (change === "value") {
        assert.equal(result.state, "replaced");
        assert.equal(result.replacementHash, hash);
        assert.equal(
          f.funding.prepare(binding, { ...input, id: "new", nonce: "8" }).state,
          "prepared",
        );
        continue;
      }
      assert.equal(result.state, "unknown", change);
      assert.equal(result.verified, false, change);
      assert.match(result.message ?? "", /FUNDING_MISMATCH/);
      assert.throws(() => f.funding.prepare(binding, { ...input, id: "new" }), {
        code: "RECONCILIATION_REQUIRED",
      });
    } finally {
      f.store.close();
    }
  }
});

test("wallet rejection is distinct from an onchain revert", async () => {
  const f = fixture();
  try {
    f.funding.prepare(binding, input);
    assert.equal(
      f.funding.rejected(binding, input.id).state,
      "wallet_rejected",
    );
    f.funding.prepare(binding, { ...input, id: "next" });
    const evidence = f.mined();
    assert(evidence.receipt);
    evidence.receipt.success = false;
    f.evidence(evidence);
    assert.equal(
      (await f.funding.refresh(binding, "next", hash)).state,
      "reverted",
    );
  } finally {
    f.store.close();
  }
});

test("a missing unverified hash can be corrected using actual matching evidence", async () => {
  const f = fixture();
  try {
    f.funding.prepare(binding, input);
    await f.funding.refresh(binding, input.id, otherHash);
    f.evidence(f.mined());
    assert.equal(
      (await f.funding.refresh(binding, input.id, hash)).state,
      "confirmed",
    );
  } finally {
    f.store.close();
  }
});
test("a mined same-nonce speed-up resolves pending funding without a second transfer", async () => {
  const f = fixture();
  try {
    f.funding.prepare(binding, input);
    const pending = f.mined();
    delete pending.receipt;
    f.evidence(pending);
    assert.equal(
      (await f.funding.refresh(binding, input.id, hash)).verified,
      true,
    );
    const replacement = f.mined();
    assert(replacement.transaction && replacement.receipt);
    replacement.transaction.hash = otherHash;
    replacement.receipt.hash = otherHash;
    f.evidence(replacement);
    const result = await f.funding.refresh(binding, input.id, otherHash);
    assert.equal(result.state, "confirmed");
    assert.equal(result.hash, hash);
    assert.equal(result.replacementHash, otherHash);
    assert.equal(result.nonce, "7");
  } finally {
    f.store.close();
  }
});
test("unmined and wrong-nonce replacements preserve the original funding record", async () => {
  const f = fixture();
  try {
    f.funding.prepare(binding, input);
    const pending = f.mined();
    delete pending.receipt;
    f.evidence(pending);
    const original = await f.funding.refresh(binding, input.id, hash);
    for (const kind of ["pending", "wrong_nonce"]) {
      const replacement = f.mined(kind === "pending" ? 1 : 2);
      assert(replacement.transaction && replacement.receipt);
      replacement.transaction.hash = otherHash;
      replacement.receipt.hash = otherHash;
      if (kind === "wrong_nonce") replacement.transaction.nonce = 8n;
      f.evidence(replacement);
      await assert.rejects(
        () => f.funding.refresh(binding, input.id, otherHash),
        {
          code:
            kind === "pending"
              ? "REPLACEMENT_UNCONFIRMED"
              : "REPLACEMENT_MISMATCH",
        },
      );
      assert.deepEqual(f.store.nativeFundings(binding.wallet)[0], original);
    }
  } finally {
    f.store.close();
  }
});
test("a mined cancellation consumes the original nonce without claiming ETH receipt", async () => {
  const f = fixture();
  try {
    f.funding.prepare(binding, input);
    const pending = f.mined();
    delete pending.receipt;
    f.evidence(pending);
    await f.funding.refresh(binding, input.id, hash);
    const canceled = f.mined();
    assert(canceled.transaction && canceled.receipt);
    canceled.transaction.hash = otherHash;
    canceled.receipt.hash = otherHash;
    canceled.transaction.to = sender;
    canceled.receipt.to = sender;
    canceled.transaction.value = 0n;
    f.evidence(canceled);
    const result = await f.funding.refresh(binding, input.id, otherHash);
    assert.equal(result.state, "replaced");
    assert.equal(result.hash, hash);
    assert.equal(result.replacementHash, otherHash);
    assert.equal(
      f.funding.prepare(binding, { ...input, id: "new" }).state,
      "prepared",
    );
    await assert.rejects(() => f.funding.refresh(binding, "new", otherHash), {
      code: "HASH_CONFLICT",
    });
  } finally {
    f.store.close();
  }
});

test("a missing original hash can be resolved by its mined same-nonce cancellation", async () => {
  const f = fixture();
  try {
    f.funding.prepare(binding, input);
    const canceled = f.mined();
    assert(canceled.transaction && canceled.receipt);
    canceled.transaction.hash = otherHash;
    canceled.receipt.hash = otherHash;
    canceled.transaction.to = sender;
    canceled.receipt.to = sender;
    canceled.transaction.value = 0n;
    f.evidence(canceled);
    const result = await f.funding.refresh(binding, input.id, otherHash);
    assert.equal(result.state, "replaced");
    assert.equal(result.hash, undefined);
    assert.equal(result.replacementHash, otherHash);
    assert.equal(
      f.funding.prepare(binding, { ...input, id: "next", nonce: "8" }).state,
      "prepared",
    );
  } finally {
    f.store.close();
  }
});

test("RPC failure and process restart preserve uncertainty without resubmitting funds", async () => {
  const dir = mkdtempSync(join(tmpdir(), "native-funding-"));
  const path = join(dir, "journal.sqlite");
  let store = new Store(path);
  try {
    store.putBinding(binding);
    const funding = new NativeFundings(store, async () => {
      throw new Error("offline");
    });
    funding.prepare(binding, input);
    assert.equal(
      (await funding.refresh(binding, input.id, hash)).state,
      "unknown",
    );
    store.close();
    store = new Store(path);
    let reads = 0;
    const restored = new NativeFundings(store, async () => {
      reads++;
      return { transaction: null };
    });
    assert.equal((await restored.list(binding))[0]?.hash, hash);
    assert.equal(reads, 1);
    assert.throws(() => restored.prepare(binding, { ...input, id: "retry" }), {
      code: "RECONCILIATION_REQUIRED",
    });
  } finally {
    store.close();
    rmSync(dir, { recursive: true });
  }
});

test("concurrent receipt checks cannot overwrite the active transaction identity", async () => {
  const f = fixture();
  let finish: (value: FundingEvidence) => void = () => {};
  const pending = new Promise<FundingEvidence>((resolve) => {
    finish = resolve;
  });
  const funding = new NativeFundings(f.store, async () => pending);
  try {
    funding.prepare(binding, input);
    const checking = funding.refresh(binding, input.id, hash);
    await assert.rejects(() => funding.refresh(binding, input.id, otherHash), {
      code: "BUSY",
    });
    assert.equal((await funding.list(binding))[0]?.hash, hash);
    finish(f.mined());
    assert.equal((await checking).state, "confirmed");
  } finally {
    finish({ transaction: null });
    f.store.close();
  }
});
