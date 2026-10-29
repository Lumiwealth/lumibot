import { test } from "node:test";
import { strict as assert } from "node:assert";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  CollateralTransfers,
  type CollateralProvider,
} from "../src/collateral-transfers.js";
import { bindingSchema, perpSchema } from "../src/contracts.js";
import { Store, digest } from "../src/store.js";

const binding = bindingSchema.parse({
  id: "agent",
  wallet: "0x" + "1".repeat(40),
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

function fixture(path = ":memory:") {
  const store = new Store(path);
  store.putBinding(binding);
  let submissions = 0;
  const provider: CollateralProvider = {
    verify: async () => {},
    prepareCollateral: async () => {},
    submitCollateral: async (_binding, _amount, boundary) => {
      await boundary.before();
      await boundary.release();
      submissions++;
    },
    collateralSignatureStatus: async () => "DENIED",
  };
  return {
    store,
    provider,
    service: new CollateralTransfers(store, provider),
    submissions: () => submissions,
  };
}

test("owner collateral request ID is durable and cannot change amount", async () => {
  const f = fixture();
  try {
    assert.equal(
      (await f.service.submit(binding, "topup", "10")).state,
      "submitted",
    );
    assert.equal(
      (await f.service.submit(binding, "topup", "10")).state,
      "submitted",
    );
    await assert.rejects(
      f.service.submit(binding, "topup", "11"),
      /different amount/,
    );
    assert.equal(f.submissions(), 1);
  } finally {
    f.store.close();
  }
});

test("unknown collateral submission blocks replay and new trading", async () => {
  const f = fixture();
  f.provider.submitCollateral = async (_binding, _amount, boundary) => {
    await boundary.before();
    await boundary.release();
    throw new Error("Lost venue response");
  };
  try {
    const op = await f.service.submit(binding, "uncertain", "10");
    assert.equal(op.state, "unknown");
    assert.equal(
      (await f.service.submit(binding, "uncertain", "10")).state,
      "unknown",
    );
    await assert.rejects(
      f.service.submit(binding, "replacement", "10"),
      /pending wallet work/,
    );
    const request = perpSchema.parse({
      kind: "perp",
      id: "order",
      symbol: "BTC",
      side: "buy",
      quantity: "0.001",
      orderType: "market",
    });
    assert.throws(
      () => f.store.reserve(binding, request, "10"),
      /pending collateral funding/,
    );
  } finally {
    f.store.close();
  }
});

test("collateral preparation failure permits a corrected new request", async () => {
  const f = fixture();
  f.provider.prepareCollateral = async () => {
    throw new Error("Insufficient spot funds");
  };
  try {
    assert.equal(
      (await f.service.submit(binding, "too-large", "10")).state,
      "rejected",
    );
    assert.equal(f.submissions(), 0);
    f.provider.prepareCollateral = async () => {};
    assert.equal(
      (await f.service.submit(binding, "corrected", "1")).state,
      "submitted",
    );
  } finally {
    f.store.close();
  }
});

test("binding changes during collateral approval prevent signature release", async () => {
  const f = fixture();
  f.provider.submitCollateral = async (_binding, _amount, boundary) => {
    await boundary.before();
    boundary.pending("signature-job");
    f.store.putBinding({ ...binding, enabled: false });
    await boundary.release();
    assert.fail("Changed authority must stop before signature release");
  };
  try {
    assert.equal(
      (await f.service.submit(binding, "pending", "10")).state,
      "awaiting_approval",
    );
    const [resolved] = await f.service.list(binding);
    assert(resolved);
    assert.equal(resolved.state, "rejected");
    assert.equal(f.submissions(), 0);
  } finally {
    f.store.close();
  }
});

for (const status of ["SIGNED", "DENIED", "EXPIRED", "FAILED"]) {
  test(`interrupted collateral signature ${status} resolves after restart without replay`, async () => {
    const dir = mkdtempSync(join(tmpdir(), "collateral-restart-"));
    const path = join(dir, "broker.sqlite");
    const f = fixture(path);
    f.provider.submitCollateral = async (_binding, _amount, boundary) => {
      await boundary.before();
      boundary.pending("signature-job");
      throw new Error("Interrupted wait");
    };
    try {
      assert.equal(
        (await f.service.submit(binding, "pending", "10")).state,
        "awaiting_approval",
      );
    } finally {
      f.store.close();
    }
    const next = new Store(path);
    f.provider.collateralSignatureStatus = async () => status;
    try {
      const service = new CollateralTransfers(next, f.provider);
      const [resolved] = await service.list(binding);
      assert(resolved);
      assert.equal(resolved.state, "rejected");
      assert.equal(
        (await service.submit(binding, "pending", "10")).state,
        "rejected",
      );
      assert.equal(f.submissions(), 0);
    } finally {
      next.close();
      rmSync(dir, { recursive: true });
    }
  });
}

test("interrupted preparation is rejected without signing or replay", async () => {
  const f = fixture();
  try {
    f.store.saveCollateralTransfer({
      id: "interrupted",
      agent: binding.id,
      wallet: binding.wallet,
      network: "testnet",
      amount: "1",
      state: "prepared",
      createdAt: Date.now(),
      updatedAt: Date.now(),
    });
    const [resolved] = await f.service.list(binding);
    assert(resolved);
    assert.equal(resolved.state, "rejected");
    assert.equal(f.submissions(), 0);
    assert.equal(
      (await f.service.submit(binding, "new-request", "1")).state,
      "submitted",
    );
  } finally {
    f.store.close();
  }
});

test("setup collateral request rejects mainnet before provider access", async () => {
  const f = fixture();
  f.provider.verify = async () =>
    assert.fail("Mainnet must fail before provider access");
  try {
    await assert.rejects(
      f.service.submit({ ...binding, network: "mainnet" }, "mainnet", "10"),
      /testnet only/,
    );
  } finally {
    f.store.close();
  }
});
