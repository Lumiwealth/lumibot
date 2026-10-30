import { z } from "zod";
import { Decimal } from "./numeric.js";

export const amount = z
  .string()
  .regex(/^(0|[1-9]\d*)(\.\d+)?$/)
  .refine((v) => new Decimal(v).isFinite());
export const signedAmount = z.string().regex(/^-?(0|[1-9]\d*)(\.\d+)?$/);
export const positive = amount.refine(
  (v) => new Decimal(v).gt(0),
  "Amount must be positive",
);
export const address = z
  .string()
  .regex(/^0x[0-9a-fA-F]{40}$/)
  .transform((v) => v.toLowerCase());
export const identifier = z.string().regex(/^[a-zA-Z0-9_-]{1,100}$/);
export const assetSchema = z
  .object({
    symbol: z.string().min(1).max(30),
    chainId: z.number().int().positive(),
    address: z.union([address, z.literal("native")]),
    decimals: z.number().int().min(0).max(36),
  })
  .strict();
export type TokenAsset = z.infer<typeof assetSchema>;
export const bindingSchema = z
  .object({
    id: identifier,
    wallet: address,
    tokenHash: z.string().regex(/^[a-f0-9]{64}$/),
    enabled: z.boolean(),
    expiresAt: z.number().int().positive(),
    network: z.enum(["testnet", "mainnet"]),
    chains: z.array(z.number().int().positive()).min(1),
    assets: z.array(assetSchema),
    markets: z.array(z.string().min(1)),
    maxOrderUsd: positive.max(80),
    maxDailyUsd: positive.max(80),
    maxSlippageBps: z.number().int().min(1).max(500),
    maxLeverage: z.number().int().min(1).max(50),
    policyDigest: z.string().regex(/^[a-f0-9]{64}$/),
  })
  .strict();
export type Binding = z.infer<typeof bindingSchema>;
export const transactionHash = z
  .string()
  .regex(/^0x[\da-fA-F]{64}$/)
  .transform((value): `0x${string}` => `0x${value.slice(2).toLowerCase()}`);
export const nativeFundingSchema = z.object({
  id: identifier,
  agent: identifier,
  wallet: address,
  sender: address,
  network: z.literal("testnet"),
  chainId: z.union([z.literal(11155111), z.literal(421614)]),
  amountWei: z
    .string()
    .regex(/^[1-9]\d{0,77}$/)
    .refine((value) => BigInt(value) < 2n ** 256n),
  state: z.enum([
    "prepared",
    "submitted",
    "confirmed",
    "reverted",
    "unknown",
    "wallet_rejected",
    "replaced",
  ]),
  hash: transactionHash.optional(),
  replacementHash: transactionHash.optional(),
  nonce: z
    .string()
    .regex(/^(0|[1-9]\d*)$/)
    .max(20)
    .refine((value) => BigInt(value) < 2n ** 64n)
    .optional(),
  verified: z.boolean().default(false),
  confirmations: z.number().int().nonnegative().default(0),
  createdAt: z.number(),
  updatedAt: z.number(),
  message: z.string().optional(),
});
export type NativeFunding = z.infer<typeof nativeFundingSchema>;
export const collateralTransferSchema = z.object({
  id: identifier,
  agent: identifier,
  wallet: address,
  network: z.literal("testnet"),
  amount: positive.max(80),
  state: z.enum([
    "prepared",
    "awaiting_approval",
    "submitted",
    "rejected",
    "unknown",
  ]),
  createdAt: z.number(),
  updatedAt: z.number(),
  approvalId: z.string().optional(),
  signatureOnly: z.boolean().optional(),
  message: z.string().optional(),
});
export type CollateralTransfer = z.infer<typeof collateralTransferSchema>;
export const collateralBalanceSchema = z.object({
  wallet: address,
  network: z.literal("testnet"),
  spotUsdc: amount,
  perpsEquity: signedAmount,
  unifiedAccount: z.boolean(),
  transferAvailable: z.boolean(),
});
export const perpSchema = z
  .object({
    kind: z.literal("perp"),
    id: identifier,
    symbol: z.string().min(1).max(60),
    side: z.enum(["buy", "sell"]),
    quantity: positive.max(80),
    orderType: z.enum(["market", "limit"]),
    limitPrice: positive.max(80).optional(),
    leverage: z.number().int().positive().default(1),
    reduceOnly: z.boolean().default(false),
    tif: z.enum(["gtc", "ioc", "alo"]).default("gtc"),
  })
  .strict()
  .refine(
    (v) => v.orderType !== "limit" || v.limitPrice !== undefined,
    "Limit price required",
  );
export const swapSchema = z
  .object({
    kind: z.literal("swap"),
    id: identifier,
    input: assetSchema,
    output: assetSchema,
    inputAmount: positive.max(80),
    slippageBps: z.number().int().positive(),
  })
  .strict();
export const requestSchema = z.discriminatedUnion("kind", [
  perpSchema,
  swapSchema,
]);
export type TradeRequest = z.infer<typeof requestSchema>;
export const operationSchema = z
  .object({
    id: identifier,
    agent: identifier,
    request: requestSchema,
    createdAt: z.number(),
    updatedAt: z.number(),
    state: z.enum([
      "prepared",
      "awaiting_approval",
      "submitted",
      "open",
      "partial_fill",
      "filled",
      "canceled",
      "rejected",
      "unknown",
    ]),
    reservedUsd: amount,
    executionLimitPrice: positive.optional(),
    filledQuantity: amount.default("0"),
    averagePrice: positive.optional(),
    fees: z
      .array(z.object({ asset: z.string(), amount: signedAmount }))
      .default([]),
    providerOrderId: z.string().optional(),
    quoteId: z.string().optional(),
    hashes: z.array(z.string()).default([]),
    approvalId: z.string().optional(),
    signatureOnly: z.boolean().optional(),
    message: z.string().optional(),
    transactionStage: z
      .enum(["allowance", "allowance_confirmed", "trade"])
      .optional(),
    tradeHash: z.string().optional(),
  })
  .strict();
export type Operation = z.infer<typeof operationSchema>;
export const policyApprovalSchema = z
  .object({
    id: identifier,
    wallet: address,
    policyDigest: z.string().regex(/^[a-f0-9]{64}$/),
    state: z.enum([
      "unknown",
      "pending",
      "approved",
      "denied",
      "expired",
      "applied",
    ]),
    requestId: z.string().optional(),
    createdAt: z.number().int(),
    updatedAt: z.number().int(),
    expiresAt: z.string().optional(),
  })
  .strict();
export type PolicyApproval = z.infer<typeof policyApprovalSchema>;

export const positionSchema = z.object({
  symbol: z.string(),
  quantity: amount.or(z.string().regex(/^-\d+(\.\d+)?$/)),
  entryPrice: amount.optional(),
  marketValue: z.string(),
  unrealizedPnl: z.string().optional(),
  asset: assetSchema.optional(),
});
export type Position = z.infer<typeof positionSchema>;
export interface Account {
  cash: string;
  equity: string;
  positionsValue: string;
  currency: string;
  positions: Position[];
}
export class BrokerError extends Error {
  constructor(
    public readonly code: string,
    message: string,
  ) {
    super(message);
  }
}
export function assert(
  condition: boolean,
  code: string,
  message: string,
): asserts condition {
  if (!condition) throw new BrokerError(code, message);
}
export function assetId(asset: TokenAsset): string {
  return `eip155:${asset.chainId}/${asset.address === "native" ? "slip44:60" : `erc20:${asset.address}`}`;
}
export function sameAsset(a: TokenAsset, b: TokenAsset): boolean {
  return (
    a.chainId === b.chainId &&
    a.address === b.address &&
    a.decimals === b.decimals
  );
}
export interface Provider {
  verify(binding: Binding): Promise<void>;
  estimate(
    binding: Binding,
    request: TradeRequest,
  ): Promise<{ usd: string; limitPrice?: string }>;
  execute(
    binding: Binding,
    operation: Operation,
    save: (op: Operation) => void,
  ): Promise<Operation>;
  reconcile(binding: Binding, operation: Operation): Promise<Operation>;
  recoverPerp(
    binding: Binding,
    operation: Operation,
    providerOrderId: string,
  ): Promise<Operation>;
  cancel(binding: Binding, operation: Operation): Promise<Operation>;
  account(binding: Binding, kind: "perp" | "spot"): Promise<Account>;
  price(binding: Binding, symbol: string): Promise<string>;
}
