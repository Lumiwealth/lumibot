import { test, mock } from "node:test";
import { strict as assert } from "node:assert";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { createReadOnlyPerpsService, validateTif } from "@metamask/agent-sdk";
import { MetaMaskProvider } from "../src/sdk.js";
import { Store, digest } from "../src/store.js";
import { bindingSchema, perpSchema, swapSchema } from "../src/contracts.js";
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
test("owner recovery verifies exact venue identity and records a confirmed rejection without signing", async () => {
  const f = fixture();
  const op = {
    ...f.op,
    state: "unknown" as const,
    providerOrderId: undefined,
    request: perpSchema.parse({
      ...f.op.request,
      orderType: "limit",
      limitPrice: "50",
      tif: "alo",
    }),
    executionLimitPrice: "50",
  };
  const order = {
    oid: 123,
    coin: "BTC",
    side: "B",
    origSz: "2",
    limitPx: "50",
    timestamp: op.createdAt,
    reduceOnly: false,
    tif: "Alo",
    orderType: "Limit",
  };
  let evidence = order;
  f.provider.info = async (b, body) => {
    assert.equal(b.wallet, binding.wallet);
    assert.equal(body.user, binding.wallet);
    if (body.type === "userFillsByTime") return [];
    assert.equal(body.oid, 123);
    return {
      status: "order",
      order: { status: "badAloPxRejected", order: evidence },
    };
  };
  const signing = mock.method(f.provider, "perps", async () => {
    throw new Error("Recovery must not sign");
  });
  try {
    const result = await f.provider.recoverPerp(binding, op, "123");
    assert.equal(result.state, "rejected");
    assert.equal(result.providerOrderId, "123");
    assert.equal(result.filledQuantity, "0");
    assert.equal(signing.mock.callCount(), 0);
    for (const patch of [
      { coin: "ETH" },
      { side: "A" },
      { origSz: "1" },
      { limitPx: "51" },
      { timestamp: op.createdAt - 5001 },
      { timestamp: op.updatedAt + 5001 },
      { reduceOnly: true },
      { tif: "Ioc" },
    ]) {
      evidence = { ...order, ...patch };
      await assert.rejects(() => f.provider.recoverPerp(binding, op, "123"), {
        code: "RECOVERY_MISMATCH",
      });
    }
    f.provider.info = async () => ({ status: "unknownOid" });
    await assert.rejects(() => f.provider.recoverPerp(binding, op, "123"));
  } finally {
    signing.mock.restore();
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
  mock.method(
    service,
    "quote",
    async (
      _venue: Parameters<typeof service.quote>[0],
      params: Parameters<typeof service.quote>[1],
    ) => {
      validateTif(params);
      assert.equal(params.tif, "ioc");
      return {
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
      };
    },
  );
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
  mock.method(service, "getBalance", async () => ({
    spendableBalance: "200000",
  }));
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

test("unfunded perpetual opening rejects before any signing attempt", async () => {
  const f = fixture();
  const service = createReadOnlyPerpsService({
    venue: "hyperliquid",
    network: "testnet",
  });
  mock.method(service, "getBalance", async () => ({ spendableBalance: "0" }));
  mock.method(f.provider, "perps", async () => service);
  const open = mock.method(service, "open", async () => {
    throw new Error("must not sign");
  });
  let saved = false;
  try {
    const result = await f.provider.execute(
      binding,
      { ...f.op, executionLimitPrice: "50000" },
      () => {
        saved = true;
      },
    );
    assert.equal(result.state, "rejected");
    assert.match(result.message ?? "", /INSUFFICIENT_COLLATERAL/);
    assert.equal(open.mock.callCount(), 0);
    assert.equal(saved, false);
  } finally {
    mock.restoreAll();
    f.cleanup();
  }
});

test("testnet swap refuses unsupported provider route before token pricing", async () => {
  const f = fixture();
  mock.method(f.provider.networks, "getSnapshot", async () => ({
    refreshedAt: new Date().toISOString(),
    serverWallet: {
      networks: [
        {
          chainId: 11155111,
          name: "Sepolia",
          networkType: "testnet",
          shieldSupported: true,
        },
      ],
    },
    balance: { fullSupport: ["eip155:11155111"], partialSupport: [] },
  }));
  const pricing = mock.method(f.provider, "tokenPrice", async () => {
    throw new Error("must not price");
  });
  try {
    await assert.rejects(
      () =>
        f.provider.estimate(
          binding,
          swapSchema.parse({
            kind: "swap",
            id: "test-route",
            inputAmount: "0.001",
            slippageBps: 50,
            input: {
              symbol: "ETH",
              chainId: 11155111,
              address: "native",
              decimals: 18,
            },
            output: {
              symbol: "TOKEN",
              chainId: 11155111,
              address: "0x" + "2".repeat(40),
              decimals: 6,
            },
          }),
        ),
      /swap routing is unavailable/,
    );
    assert.equal(pricing.mock.callCount(), 0);
  } finally {
    mock.restoreAll();
    f.cleanup();
  }
});

test("nonzero unpriced testnet holdings cannot silently become zero equity", async () => {
  const f = fixture();
  mock.method(f.provider, "walletState", async () =>
    f.provider.state(binding.wallet),
  );
  mock.method(f.provider, "wallet", () => ({
    getBalance: async () => ({
      totalValue: "0",
      unprocessedNetworks: [],
      unpricedAssetIds: ["eip155:11155111/slip44:60"],
      balances: [
        {
          assetId: "eip155:11155111/slip44:60",
          symbol: "ETH",
          balance: "0.001",
          value: null,
        },
      ],
    }),
  }));
  try {
    await assert.rejects(
      () => f.provider.account(binding, "spot"),
      /unpriced assets/,
    );
  } finally {
    mock.restoreAll();
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
    getAccountMode: async () => ({ rawState: "default" }),
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

test("unified account equity comes from spot collateral and includes existing PnL only once", async () => {
  const f = fixture();
  const replacement = mock.method(f.provider, "perps", async () => ({
    // The current SDK reports per-DEX equity even when unified balances live in spot.
    getBalance: async () => ({
      totalBalance: "0",
      spendableBalance: "898",
      marginUsed: "0",
    }),
    getPositions: async () => [],
    getAccountMode: async () => ({ rawState: "unifiedAccount" }),
  }));
  let spotTotal = "898",
    spotHold = "0";
  const info = mock.method(
    f.provider,
    "info",
    async (...[, request]: Parameters<MetaMaskProvider["info"]>) => {
      assert.equal(request.type, "spotClearinghouseState");
      return {
        balances: [
          { coin: "USDC", token: 0, total: spotTotal, hold: spotHold },
        ],
      };
    },
  );
  try {
    assert.deepEqual(await f.provider.account(binding, "perp"), {
      cash: "898",
      equity: "898",
      positionsValue: "0",
      currency: "USDC",
      positions: [],
    });
    spotTotal = "102";
    spotHold = "20";
    // Spot total already includes unified PnL. SDK per-DEX equity must not be added.
    replacement.mock.restore();
    mock.method(f.provider, "perps", async () => ({
      getBalance: async () => ({
        totalBalance: "2",
        spendableBalance: "82",
        marginUsed: "20",
      }),
      getPositions: async () => [],
      getAccountMode: async () => ({ rawState: "unifiedAccount" }),
    }));
    const result = await f.provider.account(binding, "perp");
    assert.equal(result.equity, "102");
    assert.equal(result.cash, "82");
    assert.equal(result.positionsValue, "20");
  } finally {
    info.mock.restore();
    mock.restoreAll();
    f.cleanup();
  }
});

test("limit recovery uses the SDK wire price without loosening the requested limit", async () => {
  const f = fixture();
  const service = createReadOnlyPerpsService({
    venue: "hyperliquid",
    network: "testnet",
  });
  mock.method(service, "getMarkets", async () => [
    { symbol: "ETH", sizeDecimals: 4 },
  ]);
  mock.method(service, "quote", async () => ({
    entryPrice: "2000.1",
    estimatedFee: "0.01",
  }));
  mock.method(f.provider, "perps", async () => service);
  const request = perpSchema.parse({
    kind: "perp",
    id: "rounded",
    symbol: "ETH",
    side: "buy",
    quantity: "0.01",
    orderType: "limit",
    limitPrice: "2000.123456",
    tif: "alo",
  });
  try {
    const plan = await f.provider.estimate(binding, request);
    assert.deepEqual(plan, { usd: "20.011", limitPrice: "2000.1" });
    const op = {
      ...f.op,
      request,
      state: "unknown" as const,
      providerOrderId: undefined,
      executionLimitPrice: plan.limitPrice,
    };
    f.provider.info = async (_b, body) =>
      body.type === "userFillsByTime"
        ? []
        : {
            status: "order",
            order: {
              status: "badAloPxRejected",
              order: {
                oid: 123,
                coin: "ETH",
                side: "B",
                origSz: "0.01",
                limitPx: "2000.1",
                timestamp: op.createdAt,
                reduceOnly: false,
                tif: "Alo",
                orderType: "Limit",
              },
            },
          };
    assert.equal(
      (await f.provider.recoverPerp(binding, op, "123")).state,
      "rejected",
    );
    await assert.rejects(
      () => f.provider.estimate(binding, { ...request, side: "sell" }),
      { code: "PRICE_PRECISION" },
    );
    await assert.rejects(
      () => f.provider.estimate(binding, { ...request, limitPrice: "2000.09" }),
      { code: "PRICE_PRECISION" },
    );
  } finally {
    mock.restoreAll();
    f.cleanup();
  }
});
test("empty testnet assets need no invented USD price", async () => {
  const f = fixture();
  const state = f.provider.state(binding.wallet);
  const stateMock = mock.method(f.provider, "walletState", async () => state);
  const walletMock = mock.method(f.provider, "wallet", () => ({
    getBalance: async () => ({
      totalValue: "0",
      unprocessedNetworks: [],
      unpricedAssetIds: ["eip155:11155111/slip44:60"],
      balances: [
        {
          assetId: "eip155:11155111/slip44:60",
          symbol: "ETH",
          balance: "0",
          value: null,
        },
      ],
    }),
  }));
  try {
    assert.deepEqual(await f.provider.account(binding, "spot"), {
      cash: "0",
      equity: "0",
      positionsValue: "0",
      currency: "USD",
      positions: [],
    });
  } finally {
    stateMock.mock.restore();
    walletMock.mock.restore();
    f.cleanup();
  }
});

for (const status of ["SIGNED", "DENIED", "EXPIRED", "FAILED"]) {
  test(`interrupted signature ${status} resolves without replaying a venue order`, async () => {
    const f = fixture();
    mock.method(f.provider, "walletState", async () =>
      f.provider.state(binding.wallet),
    );
    mock.method(f.provider, "wallet", () => ({
      getJobStatus: async () => ({ kind: "signature", status }),
    }));
    const info = mock.method(f.provider, "info", async () => {
      throw new Error("no venue lookup");
    });
    try {
      const op = {
        ...f.op,
        providerOrderId: undefined,
        state: "awaiting_approval" as const,
        approvalId: "job",
        signatureOnly: true,
      };
      const result = await f.provider.reconcile(binding, op);
      assert.equal(result.state, "rejected");
      assert.match(result.message ?? "", /No order will be replayed/);
      assert.equal(info.mock.callCount(), 0);
    } finally {
      mock.restoreAll();
      f.cleanup();
    }
  });
}
