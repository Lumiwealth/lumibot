import { test } from "node:test";
import { strict as assert } from "node:assert";
import { nativeFundingSchema } from "../src/contracts.js";
import { fundingNonce, nativeTransfer } from "../web/native-transfer.js";
import type { BrowserWallet } from "../web/testnet-network.js";

const sender = `0x${"2".repeat(40)}`;
const record = nativeFundingSchema.parse({
  id: "fund",
  agent: "agent",
  wallet: `0x${"1".repeat(40)}`,
  sender,
  network: "testnet",
  chainId: 421614,
  amountWei: "100000000000000",
  nonce: "7",
  state: "prepared",
  createdAt: Date.now(),
  updatedAt: Date.now(),
});
function wallet(overrides: Record<string, unknown> = {}) {
  const calls: Array<{ method: string; params?: unknown[] }> = [];
  const responses: Record<string, unknown> = {
    eth_chainId: "0x66eee",
    eth_accounts: [sender],
    eth_getTransactionCount: "0x7",
    eth_getBlockByNumber: { baseFeePerGas: `0x${(20042000).toString(16)}` },
    eth_gasPrice: `0x${(20000000).toString(16)}`,
    ...overrides,
  };
  const provider: BrowserWallet = {
    request: async (call) => {
      calls.push(call);
      return responses[call.method] ?? null;
    },
  };
  return { provider, calls };
}

test("a stale gas quote below base fee is buffered and reviewed retries preserve nonce and transfer", async () => {
  const f = wallet();
  const first = await nativeTransfer(f.provider, record);
  const retry = await nativeTransfer(f.provider, record);
  assert.deepEqual(retry, first);
  assert.deepEqual(first, {
    from: sender,
    to: record.wallet,
    value: "0x5af3107a4000",
    chainId: "0x66eee",
    nonce: "0x7",
    maxFeePerGas: `0x${(40084000).toString(16)}`,
    maxPriorityFeePerGas: "0x0",
  });
  assert(!f.calls.some((call) => call.method === "eth_sendTransaction"));
});
test("funding captures the pending nonce before preparing an intent", async () => {
  const f = wallet({ eth_getTransactionCount: "0xa" });
  assert.equal(await fundingNonce(f.provider, 421614, sender), "10");
  assert.deepEqual(f.calls.at(-1), {
    method: "eth_getTransactionCount",
    params: [sender, "pending"],
  });
});
test("consumed nonce, wrong account, mainnet and malformed fee evidence prevent another wallet request", async () => {
  for (const [override, message] of [
    [{ eth_getTransactionCount: "0x8" }, /nonce already mined/],
    [{ eth_accounts: [`0x${"3".repeat(40)}`] }, /account changed/],
    [{ eth_chainId: "0x1" }, /differs from selected testnet/],
    [{ eth_getBlockByNumber: { baseFeePerGas: "invalid" } }, /Invalid/],
  ] as const) {
    const f = wallet(override);
    await assert.rejects(() => nativeTransfer(f.provider, record), message);
    assert(!f.calls.some((call) => call.method === "eth_sendTransaction"));
  }
  const f = wallet();
  await assert.rejects(
    () => nativeTransfer(f.provider, { ...record, nonce: undefined }),
    /nonce unavailable/,
  );
  assert.equal(f.calls.length, 0);
});
