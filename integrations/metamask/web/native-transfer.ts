import { z } from "zod";
import { requireTestnet, type BrowserWallet } from "./testnet-network.js";
import type { NativeFunding } from "../src/contracts.js";

const quantity = z
  .string()
  .regex(/^0x[0-9a-f]+$/i)
  .max(66)
  .transform((value) => BigInt(value));
const hex = (value: bigint) => `0x${value.toString(16)}`;

async function checkSender(
  wallet: BrowserWallet,
  sender: string,
): Promise<void> {
  const accounts = z
    .array(z.string())
    .parse(await wallet.request({ method: "eth_accounts" }));
  if (accounts[0]?.toLowerCase() !== sender.toLowerCase())
    throw new Error("Browser account changed. Reconnect before funding.");
}
export async function fundingNonce(
  wallet: BrowserWallet,
  chain: 11155111 | 421614,
  sender: string,
): Promise<string> {
  await requireTestnet(wallet, chain);
  await checkSender(wallet, sender);
  return quantity
    .parse(
      await wallet.request({
        method: "eth_getTransactionCount",
        params: [sender, "pending"],
      }),
    )
    .toString();
}

/** Reuse the original nonce and payload, while allowing a reviewed fee replacement. */
export async function nativeTransfer(
  wallet: BrowserWallet,
  record: NativeFunding,
) {
  if (record.nonce === undefined)
    throw new Error(
      "Funding nonce unavailable. Link its transaction hash from MetaMask Activity.",
    );
  await requireTestnet(wallet, record.chainId);
  await checkSender(wallet, record.sender);
  const latestNonce = quantity.parse(
    await wallet.request({
      method: "eth_getTransactionCount",
      params: [record.sender, "latest"],
    }),
  );
  if (latestNonce > BigInt(record.nonce))
    throw new Error(
      "Funding nonce already mined. Link or refresh its receipt before another request.",
    );
  const [rawBlock, rawGasPrice] = await Promise.all([
    wallet.request({
      method: "eth_getBlockByNumber",
      params: ["latest", false],
    }),
    wallet.request({ method: "eth_gasPrice" }),
  ]);
  const baseFee = z
    .object({ baseFeePerGas: quantity })
    .parse(rawBlock).baseFeePerGas;
  const gasPrice = quantity.parse(rawGasPrice);
  const priority = gasPrice > baseFee ? gasPrice - baseFee : 0n;
  const maximum = baseFee * 2n + priority;
  if (maximum <= 0n)
    throw new Error(
      "Funding fee estimate unavailable. Refresh before signing.",
    );
  return {
    from: record.sender,
    to: record.wallet,
    value: hex(BigInt(record.amountWei)),
    chainId: hex(BigInt(record.chainId)),
    nonce: hex(BigInt(record.nonce)),
    maxFeePerGas: hex(maximum),
    maxPriorityFeePerGas: hex(priority),
  };
}
