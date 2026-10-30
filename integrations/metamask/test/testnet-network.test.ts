import { test } from "node:test";
import { strict as assert } from "node:assert";
import { requireTestnet } from "../web/testnet-network.js";

test("an unknown testnet is added through the wallet before switching and checking chain identity", async () => {
  const calls: Array<{ method: string; params?: unknown[] }> = [];
  let added = false;
  await requireTestnet(
    {
      request: async (input) => {
        calls.push(input);
        if (input.method === "wallet_switchEthereumChain" && !added)
          throw { code: 4902, message: "Unknown chain" };
        if (input.method === "wallet_addEthereumChain") {
          added = true;
          return null;
        }
        return input.method === "eth_chainId" ? "0x66eee" : null;
      },
    },
    421614,
  );
  assert.deepEqual(
    calls.map((call) => call.method),
    [
      "wallet_switchEthereumChain",
      "wallet_addEthereumChain",
      "wallet_switchEthereumChain",
      "eth_chainId",
    ],
  );
  assert.deepEqual(calls[1]?.params, [
    {
      chainId: "0x66eee",
      chainName: "Arbitrum Sepolia",
      rpcUrls: ["https://sepolia-rollup.arbitrum.io/rpc"],
      blockExplorerUrls: ["https://sepolia.arbiscan.io"],
      nativeCurrency: { name: "Sepolia Ether", symbol: "ETH", decimals: 18 },
    },
  ]);
});
test("network rejection and a wrong active chain prevent funding preparation", async () => {
  const calls: string[] = [];
  await assert.rejects(
    () =>
      requireTestnet(
        {
          request: async (input) => {
            calls.push(input.method);
            if (input.method === "wallet_switchEthereumChain")
              throw { code: 4902 };
            throw new Error("User rejected adding network");
          },
        },
        421614,
      ),
    /User rejected/,
  );
  assert.deepEqual(calls, [
    "wallet_switchEthereumChain",
    "wallet_addEthereumChain",
  ]);
  await assert.rejects(
    () =>
      requireTestnet(
        {
          request: async (input) =>
            input.method === "eth_chainId" ? "0x1" : null,
        },
        11155111,
      ),
    /differs from selected testnet/,
  );
});
test("an existing testnet needs no add-network permission and other errors are not retried", async () => {
  const calls: string[] = [];
  await requireTestnet(
    {
      request: async (input) => {
        calls.push(input.method);
        return input.method === "eth_chainId" ? "0xAA36A7" : null;
      },
    },
    11155111,
  );
  assert.deepEqual(calls, ["wallet_switchEthereumChain", "eth_chainId"]);
  await assert.rejects(
    () =>
      requireTestnet(
        {
          request: async () => {
            throw new Error("Transport unavailable");
          },
        },
        11155111,
      ),
    /Transport unavailable/,
  );
});
