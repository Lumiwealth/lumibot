import { test } from "node:test";
import { strict as assert } from "node:assert";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Decimal } from "../src/numeric.js";
import { Engine } from "../src/engine.js";
import { Store, digest } from "../src/store.js";
import {
  bindingSchema,
  perpSchema,
  swapSchema,
  requestSchema,
  type Provider,
  type Operation,
} from "../src/contracts.js";
const wallet = "0x" + "1".repeat(40);
const binding = bindingSchema.parse({
  id: "agent-one",
  wallet,
  tokenHash: digest("runtime-token"),
  enabled: true,
  expiresAt: Date.now() + 3600000,
  network: "testnet",
  chains: [11155111],
  assets: [],
  markets: ["BTC"],
  maxOrderUsd: "10",
  maxDailyUsd: "15",
  maxSlippageBps: 50,
  maxLeverage: 1,
  policyDigest: digest("policy"),
});
function request(id = "order-one") {
  return perpSchema.parse({
    kind: "perp",
    id,
    symbol: "BTC",
    side: "buy",
    quantity: "0.0001",
    orderType: "market",
  });
}
function fixture() {
  const store = new Store(":memory:");
  store.putBinding(binding);
  let calls = 0;
  const provider: Provider = {
    verify: async () => {},
    estimate: async () => ({ usd: "8" }),
    execute: async (_b, op) => {
      calls++;
      return { ...op, state: "submitted", providerOrderId: "123" };
    },
    reconcile: async (_b, op) => op,
    cancel: async (_b, op) => ({ ...op, state: "canceled" }),
    account: async () => ({
      cash: "0",
      equity: "0",
      positionsValue: "0",
      currency: "USDC",
      positions: [],
    }),
    price: async () => "100",
  };
  return {
    store,
    provider,
    engine: new Engine(store, provider),
    calls: () => calls,
  };
}
test("same ID is idempotent and different payload is rejected", async () => {
  const f = fixture();
  try {
    const first = await f.engine.submit(binding, request());
    assert.equal((await f.engine.submit(binding, request())).id, first.id);
    assert.equal(f.calls(), 1);
    await assert.rejects(
      () => f.engine.submit(binding, { ...request(), side: "sell" }),
      /different request/,
    );
  } finally {
    f.store.close();
  }
});
test("parallel engine instances cannot execute same ID twice", async () => {
  const f = fixture();
  try {
    await Promise.all([
      f.engine.submit(binding, request()),
      new Engine(f.store, f.provider).submit(binding, request()),
    ]);
    assert.equal(f.calls(), 1);
  } finally {
    f.store.close();
  }
});
test("daily cap reserves submitted orders", async () => {
  const f = fixture();
  try {
    await f.engine.submit(binding, request());
    await assert.rejects(
      () => f.engine.submit(binding, request("two")),
      /rolling 24-hour/,
    );
    assert.equal(f.calls(), 1);
  } finally {
    f.store.close();
  }
});
test("lost response retains uncertainty and prevents new trades", async () => {
  const f = fixture();
  f.provider.execute = async () => {
    throw new Error("lost after signing");
  };
  try {
    assert.equal((await f.engine.submit(binding, request())).state, "unknown");
    await assert.rejects(
      () => f.engine.submit(binding, request("two")),
      /uncertain submission/,
    );
  } finally {
    f.store.close();
  }
});
test("approval evidence survives provider exception", async () => {
  const f = fixture();
  f.provider.execute = async (_b, op, save) => {
    save({ ...op, state: "awaiting_approval", approvalId: "job-1" });
    throw new Error("awaiting");
  };
  try {
    const result = await f.engine.submit(binding, request());
    assert.equal(result.approvalId, "job-1");
    assert.equal(result.state, "awaiting_approval");
  } finally {
    f.store.close();
  }
});
test("unapproved market and leverage fail before provider execution", async () => {
  const f = fixture();
  try {
    await assert.rejects(
      () => f.engine.submit(binding, { ...request(), symbol: "ETH" }),
      /scope/,
    );
    await assert.rejects(
      () => f.engine.submit(binding, { ...request(), leverage: 2 }),
      /Leverage/,
    );
    assert.equal(f.calls(), 0);
  } finally {
    f.store.close();
  }
});
test("authorization checks expiry, revocation and independent agent tokens", () => {
  const f = fixture();
  try {
    assert.equal(f.store.authorize("runtime-token").id, binding.id);
    assert.throws(() => f.store.authorize("other"), /missing/);
    f.store.putBinding({ ...binding, enabled: false });
    assert.throws(() => f.store.authorize("runtime-token"), /disabled/);
    f.store.putBinding({ ...binding, expiresAt: 1 });
    assert.throws(() => f.store.authorize("runtime-token"), /expired/);
  } finally {
    f.store.close();
  }
});
test("wallet ownership cannot be reassigned or shared between bindings", () => {
  const f = fixture();
  try {
    assert.throws(
      () => f.store.putBinding({ ...binding, wallet: "0x" + "2".repeat(40) }),
      /new agent ID/,
    );
    assert.throws(
      () => f.store.putBinding({ ...binding, id: "other" }),
      /UNIQUE/,
    );
  } finally {
    f.store.close();
  }
});
test("revocation during quote preparation blocks signing", async () => {
  const f = fixture();
  f.provider.estimate = async () => {
    f.store.putBinding({ ...binding, enabled: false });
    return { usd: "8" };
  };
  try {
    await assert.rejects(() => f.engine.submit(binding, request()), /revoked/);
    assert.equal(f.calls(), 0);
  } finally {
    f.store.close();
  }
});
test("limit change during quote preparation blocks signing", async () => {
  const f = fixture();
  f.provider.estimate = async () => {
    f.store.putBinding({ ...binding, maxOrderUsd: "1" });
    return { usd: "8" };
  };
  try {
    await assert.rejects(() => f.engine.submit(binding, request()), /changed/);
    assert.equal(f.calls(), 0);
  } finally {
    f.store.close();
  }
});
test("restart preserves unknown operation and reservation", async () => {
  const dir = mkdtempSync(join(tmpdir(), "metamask-test-"));
  const file = join(dir, "orders.sqlite");
  let store = new Store(file);
  try {
    store.putBinding(binding);
    const op = store.reserve(binding, request(), "8").operation;
    store.save({ ...op, state: "unknown" });
    store.close();
    store = new Store(file);
    assert.equal(store.get(binding.id, op.id)?.state, "unknown");
    assert.throws(
      () => store.reserve(binding, request("second"), "1"),
      /uncertain/,
    );
  } finally {
    store.close();
    rmSync(dir, { recursive: true });
  }
});
test("wrong-chain, changed decimals, extra parameters and precision rejected", async () => {
  const f = fixture();
  const token = {
    symbol: "TOKEN",
    chainId: 11155111,
    address: "0x" + "2".repeat(40),
    decimals: 6,
  };
  const native = {
    symbol: "ETH",
    chainId: 11155111,
    address: "native" as const,
    decimals: 18,
  };
  const b = { ...binding, assets: [token, native] };
  f.store.putBinding(b);
  try {
    const swap = swapSchema.parse({
      kind: "swap",
      id: "swap-one",
      input: token,
      output: native,
      inputAmount: "1",
      slippageBps: 50,
    });
    await assert.rejects(
      () => f.engine.submit(b, { ...swap, input: { ...token, decimals: 18 } }),
      /decimals/,
    );
    await assert.rejects(
      () => f.engine.submit(b, { ...swap, output: { ...native, chainId: 1 } }),
      /Cross-chain/,
    );
    await assert.rejects(
      () => f.engine.submit(b, { ...swap, inputAmount: "1.0000001" }),
      /precision/,
    );
    assert.throws(() => requestSchema.parse({ ...swap, rawTransaction: {} }));
    assert.equal(f.calls(), 0);
  } finally {
    f.store.close();
  }
});
test("signed rebates remain serializable", () => {
  const f = fixture();
  try {
    const op = f.store.reserve(binding, request(), "1").operation;
    f.store.save({ ...op, fees: [{ asset: "USDC", amount: "-0.01" }] });
    assert.equal(f.store.get(binding.id, op.id)?.fees[0]?.amount, "-0.01");
  } finally {
    f.store.close();
  }
});
test("uncertain swap cannot be resumed", async () => {
  const f = fixture();
  try {
    await f.engine.submit(binding, request());
    await assert.rejects(
      () => f.engine.resume(binding, "order-one"),
      /confirmed allowance/,
    );
  } finally {
    f.store.close();
  }
});

test("stale refresh cannot overwrite a resumed execution claim", () => {
  const f = fixture();
  try {
    const original = f.store.reserve(binding, request(), "8").operation;
    const claimed = { ...original, state: "unknown" as const };
    f.store.save(claimed);
    const stale = { ...original, state: "awaiting_approval" as const };
    assert.equal(f.store.replaceIfUnchanged(original, stale).state, "unknown");
  } finally {
    f.store.close();
  }
});

test("revocation between allowance and trade prevents the next signing step", async () => {
  const f = fixture();
  let signedTrade = false;
  f.provider.execute = async (_b, op, save) => {
    save({
      ...op,
      state: "awaiting_approval",
      transactionStage: "allowance",
      approvalId: "job",
    });
    f.store.putBinding({ ...binding, enabled: false });
    save({ ...op, state: "unknown", transactionStage: "trade" });
    signedTrade = true;
    return { ...op, state: "submitted" };
  };
  try {
    const result = await f.engine.submit(binding, request());
    assert.equal(signedTrade, false);
    assert.equal(result.transactionStage, "allowance");
  } finally {
    f.store.close();
  }
});

test("token base units retain precision beyond JavaScript numbers and default decimal precision", () => {
  const units = new Decimal("123456789.123456789123456789").mul(
    new Decimal(10).pow(18),
  );
  assert.equal(units.toFixed(0), "123456789123456789123456789");
});
