import { z } from "zod";

export interface BrowserWallet {
  request(input: { method: string; params?: unknown[] }): Promise<unknown>;
}
const networks = {
  11155111: {
    chainId: "0xaa36a7",
    chainName: "Sepolia",
    rpcUrls: ["https://ethereum-sepolia-rpc.publicnode.com"],
    blockExplorerUrls: ["https://sepolia.etherscan.io"],
    nativeCurrency: { name: "Sepolia Ether", symbol: "ETH", decimals: 18 },
  },
  421614: {
    chainId: "0x66eee",
    chainName: "Arbitrum Sepolia",
    rpcUrls: ["https://sepolia-rollup.arbitrum.io/rpc"],
    blockExplorerUrls: ["https://sepolia.arbiscan.io"],
    nativeCurrency: { name: "Sepolia Ether", symbol: "ETH", decimals: 18 },
  },
};

export async function requireTestnet(
  wallet: BrowserWallet,
  chain: keyof typeof networks,
): Promise<void> {
  const network = networks[chain];
  try {
    await wallet.request({
      method: "wallet_switchEthereumChain",
      params: [{ chainId: network.chainId }],
    });
  } catch (error) {
    const parsed = z.object({ code: z.number() }).safeParse(error);
    if (!parsed.success || parsed.data.code !== 4902) throw error;
    await wallet.request({
      method: "wallet_addEthereumChain",
      params: [network],
    });
    await wallet.request({
      method: "wallet_switchEthereumChain",
      params: [{ chainId: network.chainId }],
    });
  }
  const actual = z
    .string()
    .regex(/^0x[0-9a-f]+$/i)
    .parse(await wallet.request({ method: "eth_chainId" }));
  if (BigInt(actual) !== BigInt(chain))
    throw new Error("Wallet network differs from selected testnet");
}
