import { test, mock } from "node:test";
import { strict as assert } from "node:assert";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { createReadOnlyPerpsService } from "@metamask/agent-sdk";
import { MetaMaskProvider } from "../src/sdk.js";
import { Store, digest } from "../src/store.js";
import { bindingSchema, perpSchema } from "../src/contracts.js";
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
  maxOrderUsd: "10",
  maxDailyUsd: "25",
  maxSlippageBps: 50,
  maxLeverage: 1,
  policyDigest: digest("policy"),
});
function fixture() {
  const dir = mkdtempSync(join(tmpdir(), "metamask-provider-"));
  const store = new Store(":memory:");
  store.putBinding(binding);
  const provider = new MetaMaskProvider(dir);
  const request = perpSchema.parse({
    kind: "perp",
    id: "order",
    symbol: "BTC",
    side: "buy",
    quantity: "2",
    orderType: "market",
  });
  const op = {
    ...store.reserve(binding, request, "10").operation,
    state: "submitted" as const,
    providerOrderId: "123",
  };
  return {
    provider,
    op,
    cleanup: () => {
      store.close();
      rmSync(dir, { recursive: true });
    },
  };
}
test("perpetual reconciliation uses actual fills, deduplicates trade IDs and records rebates", async () => {
  const f = fixture();
  try {
    f.provider.info = async (_b, body) =>
      body.type === "orderStatus"
        ? { status: "order", order: { status: "filled" } }
        : [
            {
              oid: 123,
              tid: 1,
              px: "50",
              sz: "1",
              fee: "0.01",
              feeToken: "USDC",
              time: Date.now(),
            },
            {
              oid: 123,
              tid: 1,
              px: "50",
              sz: "1",
              fee: "0.01",
              feeToken: "USDC",
              time: Date.now(),
            },
            {
              oid: 123,
              tid: 2,
              px: "60",
              sz: "1",
              fee: "-0.01",
              feeToken: "USDC",
              time: Date.now(),
            },
            {
              oid: 321,
              tid: 3,
              px: "500",
              sz: "100",
              fee: "1",
              feeToken: "USDC",
              time: Date.now(),
            },
          ];
    const result = await f.provider.reconcile(binding, f.op);
    assert.equal(result.state, "filled");
    assert.equal(result.filledQuantity, "2");
    assert.equal(result.averagePrice, "55");
    assert.equal(result.fees.length, 2);
  } finally {
    f.cleanup();
  }
});
test("filled status without complete fill evidence remains unresolved", async () => {
  const f = fixture();
  try {
    f.provider.info = async (_b, b) =>
      b.type === "orderStatus"
        ? { status: "order", order: { status: "filled" } }
        : [];
    assert.equal(
      (await f.provider.reconcile(binding, f.op)).state,
      "submitted",
    );
  } finally {
    f.cleanup();
  }
});
test("venue cancellation preserves filled quantity for partial-fill callbacks", async () => {
  const f = fixture();
  try {
    f.provider.info = async (_b, b) =>
      b.type === "orderStatus"
        ? { status: "order", order: { status: "marginCanceled" } }
        : [
            {
              oid: 123,
              tid: 1,
              px: "50",
              sz: "1",
              fee: "0.01",
              feeToken: "USDC",
              time: Date.now(),
            },
          ];
    const result = await f.provider.reconcile(binding, f.op);
    assert.equal(result.state, "canceled");
    assert.equal(result.filledQuantity, "1");
  } finally {
    f.cleanup();
  }
});
test("unknown submission without venue ID cannot infer a fill", async () => {
  const f = fixture();
  try {
    const { providerOrderId, ...unknown } = f.op;
    assert.equal(
      (await f.provider.reconcile(binding, { ...unknown, state: "unknown" }))
        .state,
      "unknown",
    );
  } finally {
    f.cleanup();
  }
});
test(
  "public Hyperliquid testnet schema",
  { skip: process.env.METAMASK_TEST_PUBLIC_TESTNET !== "1" },
  async () => {
    const f = fixture();
    try {
      const response = await f.provider.info(binding, { type: "meta" });
      assert(
        response && typeof response === "object" && "universe" in response,
      );
      assert(Array.isArray(response.universe) && response.universe.length > 0);
      const candles = await f.provider.info(binding, {
        type: "candleSnapshot",
        req: {
          coin: "BTC",
          interval: "1h",
          startTime: Date.now() - 86400000,
          endTime: Date.now(),
        },
      });
      assert(Array.isArray(candles) && candles.length > 0);
      for (const candle of candles) {
        assert.equal(typeof candle.o, "string");
        assert.equal(typeof candle.t, "number");
      }
    } finally {
      f.cleanup();
    }
  },
);

test("market quote becomes a persisted IOC limit and is not repriced during signing", async () => {
  const f = fixture();
  const service = createReadOnlyPerpsService({
    venue: "hyperliquid",
    network: "testnet",
  });
  mock.method(service, "getMarkets", async () => [
    {
      venue: "hyperliquid",
      symbol: "BTC",
      maxLeverage: 20,
      sizeDecimals: 5,
      markPrice: "50000",
    },
  ]);
  mock.method(service, "quote", async () => ({
    venue: "hyperliquid",
    symbol: "BTC",
    side: "long",
    orderType: "market",
    entryPrice: "50000",
    size: "2",
    notional: "100000",
    estimatedFee: "5",
    estimatedLiquidationPrice: "0",
    feeRate: "0.00005",
    warnings: [],
  }));
  mock.method(f.provider, "perps", async () => service);
  const open = mock.method(
    service,
    "open",
    async (
      _venue: Parameters<typeof service.open>[0],
      params: Parameters<typeof service.open>[1],
    ) => {
      assert.equal(params.orderType, "limit");
      assert.equal(params.tif, "ioc");
      assert.equal(params.limitPrice, "50250");
      return {
        venue: "hyperliquid",
        symbol: "BTC",
        status: "resting",
        orderId: "123",
      };
    },
  );
  f.provider.info = async (_b, b) =>
    b.type === "orderStatus"
      ? { status: "order", order: { status: "open" } }
      : [];
  try {
    const estimate = await f.provider.estimate(binding, f.op.request);
    assert.equal(estimate.limitPrice, "50250");
    assert.equal(estimate.usd, "100505");
    const op = { ...f.op, executionLimitPrice: estimate.limitPrice };
    assert.equal(
      (await f.provider.execute(binding, op, () => {})).state,
      "open",
    );
    assert.equal(open.mock.callCount(), 1);
  } finally {
    mock.restoreAll();
    f.cleanup();
  }
});

test("preparation failure is rejected without pretending a transaction was submitted", async () => {
  const f = fixture();
  try {
    const result = await f.provider.execute(binding, f.op, () => {});
    assert.equal(result.state, "rejected");
    assert.match(result.message ?? "", /MISSING_EXECUTION_PLAN/);
  } finally {
    f.cleanup();
  }
});

test("perpetual cash excludes spot buying power outside the equity snapshot", async () => {
  const f = fixture();
  const replacement = mock.method(f.provider, "perps", async () => ({
    getBalance: async () => ({
      totalBalance: "100",
      spendableBalance: "150",
      marginUsed: "20",
    }),
    getPositions: async () => [],
  }));
  try {
    const result = await f.provider.account(binding, "perp");
    assert.equal(result.cash, "80");
    assert.equal(result.positionsValue, "20");
    assert.equal(result.equity, "100");
  } finally {
    replacement.mock.restore();
    f.cleanup();
  }
});
