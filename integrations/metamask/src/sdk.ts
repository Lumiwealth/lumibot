import {
  AccountService,
  AuthService,
  NetworkRegistry,
  MfaApprovalService,
  HyperliquidVenueProvider,
  PerpsService,
  WalletRuntimeMode,
  isBridgeChainId,
  PriceService,
  SessionManager,
  SwapService,
  TradingModeService,
  TradingPolicyService,
  createPerpsServiceFromSession,
  createWalletServiceFromSession,
  disableAnalytics,
  isSwapQuoteUnavailable,
  loadConfig,
  type Logger,
  type PersistedSwapQuote,
  type StoredSession,
  type SupportedNetworksCache,
  type WalletStateSnapshot,
} from "@metamask/agent-sdk";
import {
  mkdirSync,
  readFileSync,
  renameSync,
  rmSync,
  writeFileSync,
  existsSync,
} from "node:fs";
import { join } from "node:path";
import { decodeFunctionData, encodeFunctionData, parseAbi } from "viem";
import { z } from "zod";
import { Decimal } from "./numeric.js";
import { acquireStateLock } from "./state-lock.js";
import {
  assert,
  assetId,
  BrokerError,
  type Account,
  type Binding,
  type Operation,
  type Provider,
  type TradeRequest,
} from "./contracts.js";
import { digest } from "./store.js";
import { PerpsWallet, type SigningBoundary } from "./perps-wallet.js";

const approvalAbi = parseAbi([
  "function approve(address spender, uint256 amount) returns (bool)",
]);
const logger: Logger = {
  debug() {},
  info() {},
  warn() {},
  error() {},
  reset() {},
  enable() {},
};
const sessionSchema = z.object({
  schemaVersion: z.string(),
  data: z
    .object({
      cliToken: z.string().nullable(),
      cliRefreshToken: z.string().nullable(),
      projectId: z.string().nullable(),
      chain: z.string().nullable(),
      walletMode: z.enum(["server-wallet", "byok"]).nullable(),
      tradingMode: z.enum(["guard", "beast"]).nullable(),
      authMethod: z.enum(["qr", "social"]).nullable(),
      loginMethod: z
        .enum(["google", "email", "qr", "other"])
        .nullable()
        .default(null),
      consent: z.number().nullable().default(null),
    })
    .passthrough(),
});
const orderStatus = z.object({
  status: z.string(),
  order: z.object({ status: z.string() }).passthrough().optional(),
});
const fillsSchema = z.array(
  z.object({
    oid: z.number(),
    tid: z.number(),
    px: z.string(),
    sz: z.string(),
    fee: z.string(),
    feeToken: z.string(),
    time: z.number(),
  }),
);
const tokenFromAddress = (value: string) =>
  value === "native" ? "0x0000000000000000000000000000000000000000" : value;

export class MetaMaskProvider implements Provider {
  readonly config = loadConfig("prod", {});
  readonly session: SessionManager;
  readonly auth: AuthService;
  readonly prices: PriceService;
  readonly networks: NetworkRegistry;
  readonly accounts: AccountService;
  readonly policy: TradingPolicyService;
  readonly approvals: MfaApprovalService;
  readonly modes: TradingModeService;
  private networkCache: SupportedNetworksCache | null = null;
  private readonly quotes = new Map<string, PersistedSwapQuote>();
  constructor(directory: string) {
    disableAnalytics();
    mkdirSync(directory, { recursive: true, mode: 0o700 });
    const path = join(directory, "session.json");
    const acquire = () =>
      acquireStateLock(
        join(directory, "session.owner.sqlite"),
        "Session refresh already in use by another process",
      );
    this.session = new SessionManager({
      logger,
      locker: {
        acquire,
        async acquireAsync() {
          const release = acquire();
          return async () => release();
        },
      },
      storage: {
        read: () =>
          existsSync(path)
            ? sessionSchema.parse(JSON.parse(readFileSync(path, "utf8")))
            : null,
        write: (data: StoredSession) => {
          const temp = path + ".tmp";
          writeFileSync(temp, JSON.stringify(data), { mode: 0o600 });
          renameSync(temp, path);
        },
        delete: () => rmSync(path, { force: true }),
      },
    });
    this.session.load();
    this.auth = new AuthService(this.session, "prod");
    this.prices = new PriceService("prod", () => this.token());
    this.accounts = new AccountService("prod", () => this.token());
    this.networks = new NetworkRegistry({
      env: "prod",
      logger,
      getAuthTokenFn: () => this.token(),
      storage: {
        read: () => this.networkCache,
        write: (data) => {
          this.networkCache = data;
        },
      },
    });
    this.policy = new TradingPolicyService(this.session, "prod", logger);
    this.approvals = new MfaApprovalService(this.session, "prod");
    this.modes = new TradingModeService(this.session, "prod");
  }
  token(): string {
    const token = this.session.get("cliToken");
    assert(
      typeof token === "string",
      "AUTH_REQUIRED",
      "Sign in to MetaMask Agent Wallet in setup",
    );
    return token;
  }
  async authToken(): Promise<string> {
    assert(
      this.session.get("cliToken") !== null,
      "AUTH_REQUIRED",
      "Sign in to MetaMask Agent Wallet in setup",
    );
    if (this.auth.isCliTokenExpiringSoon()) await this.auth.refreshCliToken();
    const token = this.session.get("cliToken");
    assert(
      typeof token === "string",
      "AUTH_REQUIRED",
      "MetaMask session unavailable",
    );
    return token;
  }
  async authenticationStatus(): Promise<{
    authenticated: boolean;
    loginMethod: string | null;
    authenticationError?: string;
  }> {
    const loginMethod = this.session.get("loginMethod") ?? null;
    if (this.session.get("cliToken") === null)
      return { authenticated: false, loginMethod };
    try {
      await this.authToken();
      return { authenticated: true, loginMethod };
    } catch {
      return {
        authenticated: false,
        loginMethod,
        authenticationError:
          "MetaMask session could not refresh. Reconnect using the original sign-in method.",
      };
    }
  }
  async login(token: string): Promise<void> {
    await this.auth.importCliToken(token);
    this.session.update({ walletMode: "server-wallet" });
  }
  state(wallet?: string): WalletStateSnapshot {
    return {
      byokWallets: [],
      remoteWallets: [],
      customEvmChains: [],
      customSolanaChains: [],
      pendingJobs: [],
      ...(wallet
        ? {
            selectedWallet: {
              mode: "server" as const,
              namespace: "evm" as const,
              ref: { address: wallet },
            },
          }
        : {}),
    };
  }
  wallet(state: WalletStateSnapshot) {
    return createWalletServiceFromSession({
      session: this.session.load(),
      walletState: state,
      resolveMnemonic: async () => {
        throw new BrokerError(
          "MODE_DENIED",
          "Main wallet keys are not accepted",
        );
      },
      serverWalletBaseUrl: this.config.serverWalletBaseUrl,
      accountsApiBaseUrl: this.config.accountsApiBaseUrl,
      infuraRpcBaseUrl: this.config.infuraRpcBaseUrl,
      priceService: this.prices,
      networkRegistry: this.networks,
      logger,
      refreshAuthToken: () => this.authToken(),
    });
  }
  async walletState(wallet?: string): Promise<WalletStateSnapshot> {
    await this.authToken();
    const state = this.state(wallet);
    state.remoteWallets = (
      await this.wallet(state).fetchRemoteWallets("evm")
    ).filter((w) => "kind" in w);
    if (wallet)
      assert(
        state.remoteWallets.some(
          (w) => w.address.toLowerCase() === wallet && w.kind === "server",
        ),
        "WALLET_DENIED",
        "Wallet does not belong to this server-wallet session",
      );
    return state;
  }
  async listWallets() {
    return (await this.walletState()).remoteWallets.map((w) => ({
      address: w.address,
      name: w.name ?? "",
      kind: w.kind,
    }));
  }
  async createWallet(name: string) {
    const state = await this.walletState();
    const result = await this.wallet(state).createWallet({
      namespace: "evm",
      name,
    });
    await this.modes.setMode({
      mode: "guard",
      walletAddress: result.wallet.address,
    });
    return { address: result.wallet.address };
  }
  async verify(binding: Binding): Promise<void> {
    await this.walletState(binding.wallet);
    const mode = await this.modes.getMode(binding.wallet);
    assert(
      mode.mode === "guard",
      "GUARD_REQUIRED",
      "Agent wallet must remain in Guard Mode",
    );
    assert(
      digest(await this.policy.getPolicy(binding.wallet)) ===
        binding.policyDigest,
      "POLICY_CHANGED",
      "Provider policy changed; owner must review binding",
    );
    const networks = await this.networks.getSnapshot();
    for (const chain of binding.chains) {
      const match = networks.serverWallet.networks.find(
        (n) => n.chainId === chain,
      );
      assert(
        match !== undefined && match.networkType === binding.network,
        "NETWORK_MISMATCH",
        "Chain does not match bound testnet/mainnet environment",
      );
    }
  }
  async perps(binding: Binding, boundary?: SigningBoundary) {
    if (boundary) {
      const state = await this.walletState(binding.wallet);
      const projectId = this.session.get("projectId");
      assert(
        typeof projectId === "string",
        "AUTH_REQUIRED",
        "Missing MetaMask project",
      );
      const wallet = new PerpsWallet(
        {
          mode: WalletRuntimeMode.Server,
          authToken: await this.authToken(),
          projectId,
          wallets: state.remoteWallets,
          serverWalletBaseUrl: this.config.serverWalletBaseUrl,
          accountsApiBaseUrl: this.config.accountsApiBaseUrl,
          infuraRpcBaseUrl: this.config.infuraRpcBaseUrl,
          priceService: this.prices,
          networkRegistry: this.networks,
          logger,
          refreshAuthToken: () => this.authToken(),
        },
        boundary,
      );
      return new PerpsService([
        new HyperliquidVenueProvider({
          walletService: wallet,
          walletState: state,
          network: binding.network,
        }),
      ]);
    }
    return createPerpsServiceFromSession({
      session: this.session.load(),
      walletState: await this.walletState(binding.wallet),
      resolveMnemonic: async () => {
        throw new BrokerError(
          "MODE_DENIED",
          "Local wallet keys are not accepted",
        );
      },
      serverWalletBaseUrl: this.config.serverWalletBaseUrl,
      accountsApiBaseUrl: this.config.accountsApiBaseUrl,
      infuraRpcBaseUrl: this.config.infuraRpcBaseUrl,
      priceService: this.prices,
      networkRegistry: this.networks,
      logger,
      venue: "hyperliquid",
      network: binding.network,
    });
  }
  async price(binding: Binding, symbol: string): Promise<string> {
    const market = (
      await (await this.perps(binding)).getMarkets("hyperliquid")
    ).find((m) => m.symbol === symbol);
    assert(market !== undefined, "MARKET_NOT_FOUND", "Market not available");
    return market.markPrice;
  }
  async collateralBalance(binding: Binding) {
    assert(
      binding.network === "testnet",
      "NETWORK_DENIED",
      "Setup collateral funding supports testnet only",
    );
    const service = await this.perps(binding);
    const [balance, mode, rawSpot] = await Promise.all([
      service.getBalance("hyperliquid"),
      service.getAccountMode("hyperliquid"),
      this.info(binding, {
        type: "spotClearinghouseState",
        user: binding.wallet,
      }),
    ]);
    const spot = z
      .object({
        balances: z.array(z.object({ coin: z.string(), total: z.string() })),
      })
      .parse(rawSpot);
    const spotUsdc = spot.balances
      .filter((row) => row.coin === "USDC")
      .reduce((sum, row) => sum.plus(row.total), new Decimal(0))
      .toFixed();
    return {
      wallet: binding.wallet,
      network: "testnet" as const,
      spotUsdc,
      perpsEquity: balance.totalBalance,
      unifiedAccount: mode.unifiedAccount,
      transferAvailable: mode.usdClassTransferAvailable,
    };
  }
  async prepareCollateral(binding: Binding, amount: string): Promise<void> {
    assert(
      binding.network === "testnet",
      "NETWORK_DENIED",
      "Setup collateral funding supports testnet only",
    );
    await (
      await this.perps(binding)
    ).prepareTransferCollateral("hyperliquid", {
      amount,
      asset: "USDC",
      direction: "spotToPerp",
    });
  }
  async submitCollateral(
    binding: Binding,
    amount: string,
    boundary: SigningBoundary,
  ): Promise<void> {
    const result = await (
      await this.perps(binding, boundary)
    ).transferCollateral("hyperliquid", {
      amount,
      asset: "USDC",
      direction: "spotToPerp",
    });
    assert(
      ["submitted", "confirmed"].includes(result.status),
      "FUNDING_UNCONFIRMED",
      "Collateral transfer was not acknowledged by the venue",
    );
  }
  async collateralSignatureStatus(
    binding: Binding,
    id: string,
  ): Promise<string> {
    const wallet = this.wallet(await this.walletState(binding.wallet));
    return (
      await wallet.getJobStatus("evm", { kind: "signature", pollingId: id })
    ).status;
  }
  async tokenPrice(
    binding: Binding,
    asset: import("./contracts.js").TokenAsset,
  ): Promise<string> {
    await this.authToken();
    const value = (
      await this.prices.getSpotPrices({ assetIds: [assetId(asset)] })
    )[assetId(asset)]?.usd;
    assert(
      value !== undefined && Number.isFinite(value) && value > 0,
      "UNPRICED",
      "No reliable USD token valuation",
    );
    return String(value);
  }
  async estimate(
    binding: Binding,
    request: TradeRequest,
  ): Promise<{ usd: string; limitPrice?: string }> {
    if (request.kind === "perp") {
      const service = await this.perps(binding);
      const market = (await service.getMarkets("hyperliquid")).find(
        (m) => m.symbol === request.symbol,
      );
      assert(market !== undefined, "MARKET_NOT_FOUND", "Market not available");
      assert(
        new Decimal(request.quantity).decimalPlaces() <= market.sizeDecimals,
        "PRECISION",
        "Quantity exceeds venue precision",
      );
      const quote = await service.quote("hyperliquid", {
        symbol: request.symbol,
        side: request.side === "buy" ? "long" : "short",
        size: request.quantity,
        leverage: request.leverage,
        orderType: request.orderType,
        tif: request.orderType === "market" ? "ioc" : request.tif,
        ...(request.limitPrice ? { limitPrice: request.limitPrice } : {}),
      });
      if (request.orderType === "limit")
        return {
          usd: new Decimal(request.quantity)
            .mul(request.limitPrice ?? quote.entryPrice)
            .plus(quote.estimatedFee)
            .toFixed(),
          limitPrice: request.limitPrice ?? quote.entryPrice,
        };
      const slippage = new Decimal(binding.maxSlippageBps).div(10000);
      const entry = new Decimal(quote.entryPrice);
      const rounding =
        request.side === "buy" ? Decimal.ROUND_DOWN : Decimal.ROUND_UP;
      const limit = entry
        .mul(
          request.side === "buy"
            ? new Decimal(1).plus(slippage)
            : new Decimal(1).minus(slippage),
        )
        .toSignificantDigits(5, rounding)
        .toDecimalPlaces(Math.max(0, 6 - market.sizeDecimals), rounding);
      assert(
        limit.gt(0),
        "INVALID_PRICE",
        "Bounded execution price rounds to zero",
      );
      return {
        usd: entry
          .mul(new Decimal(1).plus(slippage))
          .mul(request.quantity)
          .plus(quote.estimatedFee)
          .toFixed(),
        limitPrice: limit.toFixed(),
      };
    }
    assert(
      isBridgeChainId(await this.networks.getSnapshot(), request.input.chainId),
      "UNSUPPORTED_ROUTE",
      "MetaMask swap routing is unavailable on this chain; testnet wallets support balances and signing, not testnet swaps",
    );
    return {
      usd: new Decimal(request.inputAmount)
        .mul(await this.tokenPrice(binding, request.input))
        .toFixed(),
    };
  }
  async execute(
    binding: Binding,
    operation: Operation,
    save: (op: Operation) => void,
  ): Promise<Operation> {
    const attempt = { started: false };
    try {
      return await this.executePrepared(binding, operation, save, attempt);
    } catch (error) {
      if (attempt.started) throw error;
      return {
        ...operation,
        state: "rejected",
        updatedAt: Date.now(),
        message:
          error instanceof BrokerError
            ? `${error.code}: ${error.message}`
            : "Provider preparation failed before signing; no trade was submitted",
      };
    }
  }
  private async executePrepared(
    binding: Binding,
    operation: Operation,
    save: (op: Operation) => void,
    attempt: { started: boolean },
  ): Promise<Operation> {
    const request = operation.request;
    if (request.kind === "perp") {
      assert(
        operation.executionLimitPrice !== undefined,
        "MISSING_EXECUTION_PLAN",
        "No persisted bounded execution price",
      );
      let signing = operation;
      const recheck = async () => {
        await this.verify(binding);
        signing = {
          ...signing,
          state: "unknown",
          signatureOnly: false,
          approvalId: undefined,
          updatedAt: Date.now(),
        };
        save(signing);
      };
      const service = await this.perps(binding, {
        before: recheck,
        release: recheck,
        pending: (id) => {
          signing = {
            ...signing,
            state: "awaiting_approval",
            approvalId: id,
            signatureOnly: true,
            updatedAt: Date.now(),
            message:
              "MetaMask signature pending. If interrupted, completion will not automatically place an order.",
          };
          save(signing);
        },
      });
      if (!request.reduceOnly) {
        const balance = await service.getBalance("hyperliquid");
        assert(
          new Decimal(balance.spendableBalance).gt(0),
          "INSUFFICIENT_COLLATERAL",
          "Fund the selected venue account before opening a perpetual position",
        );
      }
      save(operation);
      attempt.started = true;
      const result = await service.open("hyperliquid", {
        symbol: request.symbol,
        side: request.side === "buy" ? "long" : "short",
        size: request.quantity,
        leverage: request.leverage,
        orderType: "limit",
        tif: request.orderType === "market" ? "ioc" : request.tif,
        reduceOnly: request.reduceOnly,
        maxSlippageBps: binding.maxSlippageBps,
        limitPrice: operation.executionLimitPrice,
      });
      const op: Operation = {
        ...operation,
        state:
          result.status === "rejected"
            ? "rejected"
            : result.status === "resting"
              ? "open"
              : "submitted",
        updatedAt: Date.now(),
        ...(result.orderId ? { providerOrderId: result.orderId } : {}),
        message: result.error,
      };
      save(op);
      return this.reconcile(binding, op);
    }
    let op = operation;
    const state = await this.walletState(binding.wallet);
    const wallet = this.wallet(state);
    const projectId = this.session.get("projectId");
    assert(
      typeof projectId === "string",
      "AUTH_REQUIRED",
      "Missing MetaMask project",
    );
    const service = new SwapService({
      env: "prod",
      walletAddress: binding.wallet,
      bridgeApiBaseUrl: this.config.bridgeApiBaseUrl,
      projectId,
      infuraRpcBaseUrl: this.config.infuraRpcBaseUrl,
      getAuthTokenFn: () => this.token(),
      networkRegistry: this.networks,
      quoteStore: {
        save: (q) => {
          this.quotes.set(q.quoteId, q);
        },
        load: (id) => {
          const q = this.quotes.get(id);
          assert(q !== undefined, "QUOTE_EXPIRED", "Request a new quote");
          return q;
        },
        clearAll: () => this.quotes.clear(),
      },
      submitTransaction: async (input) => {
        const stage = input.bridgeQuoteId ? "trade" : "allowance";
        let transaction = input.transaction;
        assert(
          input.chainId === request.input.chainId,
          "CHAIN_DENIED",
          "Transaction chain differs from quote",
        );
        if (stage === "allowance") {
          assert(
            request.input.address !== "native" &&
              transaction.to.toLowerCase() === request.input.address &&
              transaction.data !== undefined,
            "ALLOWANCE_DENIED",
            "Approval must target the authorized source token",
          );
          const decoded = decodeFunctionData({
            abi: approvalAbi,
            data: transaction.data,
          });
          const maximum = BigInt(
            new Decimal(request.inputAmount)
              .mul(new Decimal(10).pow(request.input.decimals))
              .toFixed(0),
          );
          // Reset approvals stay zero. Every positive approval is capped to this trade.
          transaction = {
            ...transaction,
            data: encodeFunctionData({
              abi: approvalAbi,
              functionName: "approve",
              args: [decoded.args[0], decoded.args[1] === 0n ? 0n : maximum],
            }),
          };
          assert(
            (transaction.value ?? 0n) === 0n,
            "ALLOWANCE_DENIED",
            "Token approval cannot transfer native value",
          );
        } else {
          const value = transaction.value ?? 0n;
          if (request.input.address === "native")
            assert(
              value <=
                BigInt(
                  new Decimal(request.inputAmount)
                    .mul(new Decimal(10).pow(request.input.decimals))
                    .toFixed(0),
                ),
              "VALUE_LIMIT",
              "Native value exceeds authorized input",
            );
          else
            assert(
              value === 0n,
              "VALUE_LIMIT",
              "Token swaps cannot attach unbudgeted native value",
            );
        }

        op = {
          ...op,
          state: "unknown",
          transactionStage: stage,
          updatedAt: Date.now(),
        };
        delete op.approvalId;
        save(op);
        attempt.started = true;
        const result = await wallet.submitEvmTransaction({
          state,
          chainId: input.chainId,
          transaction,
          wait: false,
          waitForReceipt: false,
          ...(input.intent ? { intent: input.intent } : {}),
          ...(input.bridgeQuoteId
            ? { bridgeQuoteId: input.bridgeQuoteId }
            : {}),
        });
        op = {
          ...op,
          state:
            result.status === "AWAITING_MFA"
              ? "awaiting_approval"
              : "submitted",
          updatedAt: Date.now(),
          ...(result.pendingJob
            ? { approvalId: result.pendingJob.pollingId }
            : {}),
          ...(result.status === "AWAITING_MFA"
            ? {
                message:
                  this.session.get("authMethod") === "qr"
                    ? "Approve in MetaMask Mobile notifications. Do not resubmit."
                    : "Approve using the MetaMask email link. Do not resubmit.",
              }
            : {}),
          hashes: result.hash ? [...op.hashes, result.hash] : op.hashes,
          ...(stage === "trade" && result.hash
            ? { tradeHash: result.hash }
            : {}),
        };
        save(op);
        // Keep ordinary sequential approvals unattended. Persist every job update
        // before waiting; MFA/timeouts remain resumable without replaying a trade.
        let hash = result.hash;
        if (!hash && result.pendingJob) {
          const job = await wallet.awaitJob(
            "evm",
            { kind: "transaction", pollingId: result.pendingJob.pollingId },
            {
              timeoutMs: 30000,
              onUpdate: async (job) => {
                if (job.kind !== "transaction") return;
                op = {
                  ...op,
                  state:
                    job.status === "AWAITING_MFA"
                      ? "awaiting_approval"
                      : "submitted",
                  updatedAt: Date.now(),
                  message:
                    job.status === "AWAITING_MFA"
                      ? this.session.get("authMethod") === "qr"
                        ? "Approve in MetaMask Mobile notifications. Do not resubmit."
                        : "Approve using the MetaMask email link. Do not resubmit."
                      : undefined,
                  hashes: job.txHash
                    ? [...new Set([...op.hashes, job.txHash])]
                    : op.hashes,
                  ...(stage === "trade" && job.txHash
                    ? { tradeHash: job.txHash }
                    : {}),
                };
                save(op);
              },
            },
          );
          if (job.kind === "transaction") hash = job.txHash;
        }
        assert(
          hash !== undefined,
          "APPROVAL_PENDING",
          "MetaMask signing job pending; operation retained",
        );
        op = {
          ...op,
          hashes: [...new Set([...op.hashes, hash])],
          ...(stage === "trade" ? { tradeHash: hash } : {}),
          updatedAt: Date.now(),
        };
        save(op);
        if (input.waitForReceipt)
          await wallet.waitForEvmReceipt({
            state,
            chainId: input.chainId,
            hash,
          });
        return {
          hash,
          ...(result.pendingJob ? { pendingJob: result.pendingJob } : {}),
        };
      },
    });
    const quote = await service.fetchQuote({
      from: tokenFromAddress(request.input.address),
      to: tokenFromAddress(request.output.address),
      amount: request.inputAmount,
      fromChain: request.input.chainId,
      toChain: request.output.chainId,
      slippage: new Decimal(request.slippageBps).div(100).toFixed(),
    });
    if (isSwapQuoteUnavailable(quote))
      return {
        ...op,
        state: "rejected",
        message: quote.message,
        updatedAt: Date.now(),
      };
    if (quote.quote.gasIncluded7702)
      return {
        ...op,
        state: "rejected",
        message:
          "Gasless delegation routes are not supported by this broker; no transaction submitted",
        updatedAt: Date.now(),
      };
    assert(
      quote.request.walletAddress.toLowerCase() === binding.wallet &&
        quote.request.srcChainId === request.input.chainId &&
        quote.request.destChainId === request.input.chainId,
      "QUOTE_MISMATCH",
      "Quote account/network differs from authorized request",
    );
    assert(
      quote.request.srcAsset.address.toLowerCase() ===
        tokenFromAddress(request.input.address) &&
        quote.request.destAsset.address.toLowerCase() ===
          tokenFromAddress(request.output.address),
      "QUOTE_MISMATCH",
      "Quote tokens differ from authorized request",
    );
    assert(
      new Decimal(quote.request.srcAssetAmount).eq(
        new Decimal(request.inputAmount).mul(
          new Decimal(10).pow(request.input.decimals),
        ),
      ),
      "QUOTE_MISMATCH",
      "Quote input amount differs from authorized request",
    );
    assert(
      !quote.request.recipientAddress ||
        quote.request.recipientAddress.toLowerCase() === binding.wallet,
      "RECIPIENT_DENIED",
      "Swap output must return to agent wallet",
    );
    assert(
      quote.request.srcAsset.decimals === request.input.decimals &&
        quote.request.destAsset.decimals === request.output.decimals,
      "PRECISION",
      "Provider token metadata differs from approved registry",
    );
    assert(
      quote.request.slippage <= request.slippageBps / 100,
      "SLIPPAGE_LIMIT",
      "Quote exceeds authorized slippage",
    );
    op = { ...op, quoteId: quote.quoteId };
    save(op);
    try {
      await service.executeQuote({
        quoteId: quote.quoteId,
        state,
        wait: false,
      });
    } finally {
      this.quotes.delete(quote.quoteId);
    }
    return this.reconcile(binding, op);
  }
  async info(
    binding: Binding,
    body: Record<string, unknown>,
  ): Promise<unknown> {
    const host =
      binding.network === "testnet"
        ? "https://api.hyperliquid-testnet.xyz"
        : "https://api.hyperliquid.xyz";
    const response = await fetch(host + "/info", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(15000),
    });
    assert(response.ok, "PROVIDER_UNAVAILABLE", "Hyperliquid read failed");
    return response.json();
  }
  async reconcile(binding: Binding, op: Operation): Promise<Operation> {
    if (op.request.kind === "perp") {
      if (op.signatureOnly && op.approvalId && !op.providerOrderId) {
        const wallet = this.wallet(await this.walletState(binding.wallet));
        const job = await wallet.getJobStatus("evm", {
          kind: "signature",
          pollingId: op.approvalId,
        });
        if (["SIGNED", "FAILED", "DENIED", "EXPIRED"].includes(job.status)) {
          return {
            ...op,
            state: "rejected",
            updatedAt: Date.now(),
            message:
              "Interrupted signature request resolved; signature was never released to the venue. No order will be replayed. Earlier leverage changes may have applied.",
          };
        }
        return op;
      }
      if (!op.providerOrderId) return op;
      const response = orderStatus.parse(
        await this.info(binding, {
          type: "orderStatus",
          user: binding.wallet,
          oid: Number(op.providerOrderId),
        }),
      );
      const fills: z.infer<typeof fillsSchema> = [];
      let cursor = op.createdAt - 5000;
      const end = Date.now();
      for (let page = 0; page < 100; page++) {
        const rows = fillsSchema.parse(
          await this.info(binding, {
            type: "userFillsByTime",
            user: binding.wallet,
            startTime: cursor,
            endTime: end,
            aggregateByTime: false,
          }),
        );
        fills.push(...rows);
        if (rows.length < 2000) break;
        const next = Math.max(...rows.map((f) => f.time));
        assert(
          next > cursor && page < 99,
          "FILL_HISTORY_INCOMPLETE",
          "Fill pagination cannot establish complete history",
        );
        cursor = next;
      }
      const unique = [
        ...new Map(
          fills
            .filter((f) => String(f.oid) === op.providerOrderId)
            .map((f) => [f.tid, f]),
        ).values(),
      ];
      const quantity = unique.reduce((a, f) => a.plus(f.sz), new Decimal(0));
      const value = unique.reduce(
        (a, f) => a.plus(new Decimal(f.sz).mul(f.px)),
        new Decimal(0),
      );
      const status = response.order?.status;
      const state: Operation["state"] =
        status === "filled" && quantity.eq(op.request.quantity)
          ? "filled"
          : status?.toLowerCase().includes("cancel")
            ? "canceled"
            : status === "open"
              ? quantity.gt(0)
                ? "partial_fill"
                : "open"
              : status?.toLowerCase().includes("reject")
                ? "rejected"
                : op.state;
      return {
        ...op,
        state,
        updatedAt: Date.now(),
        filledQuantity: quantity.toFixed(),
        message:
          state === "filled" ||
          state === "canceled" ||
          state === "open" ||
          state === "partial_fill"
            ? undefined
            : op.message,
        ...(quantity.gt(0)
          ? { averagePrice: value.div(quantity).toFixed() }
          : {}),
        fees: unique.map((f) => ({
          asset: f.feeToken,
          amount: new Decimal(f.fee).toFixed(),
        })),
      };
    }
    const request = op.request;
    await this.authToken();
    if (op.approvalId && !op.tradeHash) {
      const wallet = this.wallet(await this.walletState(binding.wallet));
      const job = await wallet.getJobStatus("evm", {
        kind: "transaction",
        pollingId: op.approvalId,
      });
      if (job.kind === "transaction" && job.txHash) {
        op = {
          ...op,
          hashes: [...new Set([...op.hashes, job.txHash])],
          state: "submitted",
          updatedAt: Date.now(),
          ...(op.transactionStage === "trade" ? { tradeHash: job.txHash } : {}),
        };
      } else if (["FAILED", "DENIED", "EXPIRED"].includes(job.status)) {
        return {
          ...op,
          state: "rejected",
          message: "MetaMask signing job failed or was declined",
          updatedAt: Date.now(),
        };
      }
    }
    if (op.transactionStage === "allowance" && op.hashes.length) {
      const hash = op.hashes.at(-1);
      if (hash) {
        const result = await this.accounts.getTransactionByHash({
          chainId: request.input.chainId,
          txHash: hash,
          includeValueTransfers: true,
        });
        if (result.data.blockNumber > 0) {
          return {
            ...op,
            state: result.data.isError ? "rejected" : "awaiting_approval",
            transactionStage: result.data.isError
              ? "allowance"
              : "allowance_confirmed",
            updatedAt: Date.now(),
            message: result.data.isError
              ? "Allowance reverted"
              : "Allowance confirmed. Resume this operation in setup to obtain a fresh swap quote.",
          };
        }
      }
      return op;
    }
    // Account API returns mined transaction value transfers, never quote estimates as fills.
    for (const hash of op.tradeHash ? [op.tradeHash] : []) {
      const result = await this.accounts.getTransactionByHash({
        chainId: request.input.chainId,
        txHash: hash,
        includeValueTransfers: true,
      });
      assert(
        result.data.hash.toLowerCase() === hash.toLowerCase() &&
          result.data.chainId === request.input.chainId &&
          result.data.from.toLowerCase() === binding.wallet,
        "RECEIPT_MISMATCH",
        "Receipt identity mismatch",
      );
      if (result.data.blockNumber <= 0) return op;
      if (result.data.isError)
        return {
          ...op,
          state: "rejected",
          message: "Onchain transaction reverted",
          updatedAt: Date.now(),
        };
      const transfers = result.data.valueTransfers;
      const received = transfers
        .filter(
          (t) =>
            t.to.toLowerCase() === binding.wallet &&
            t.contractAddress.toLowerCase() ===
              tokenFromAddress(request.output.address),
        )
        .reduce(
          (a, t) =>
            a.plus(new Decimal(t.amount).div(new Decimal(10).pow(t.decimal))),
          new Decimal(0),
        );
      if (received.gt(0))
        return {
          ...op,
          state: "filled",
          message: undefined,
          filledQuantity: received.toFixed(),
          averagePrice: new Decimal(request.inputAmount)
            .div(received)
            .toFixed(),
          updatedAt: Date.now(),
          fees: [
            {
              asset: `eip155:${request.input.chainId}/slip44:60`,
              amount: new Decimal(result.data.gasUsed)
                .mul(result.data.effectiveGasPrice)
                .div("1e18")
                .toFixed(),
            },
          ],
        };
    }
    return op;
  }
  async cancel(binding: Binding, op: Operation): Promise<Operation> {
    assert(
      op.request.kind === "perp" && op.providerOrderId !== undefined,
      "CANCEL_UNSUPPORTED",
      "Onchain swaps and uncertain submissions cannot be canceled as exchange orders",
    );
    await (
      await this.perps(binding)
    ).cancel("hyperliquid", {
      orderId: op.providerOrderId,
      symbol: op.request.symbol,
    });
    return this.reconcile(binding, op);
  }
  async account(binding: Binding, kind: "perp" | "spot"): Promise<Account> {
    if (kind === "perp") {
      const service = await this.perps(binding);
      const [balance, positions] = await Promise.all([
        service.getBalance("hyperliquid"),
        service.getPositions("hyperliquid"),
      ]);
      // Spendable collateral can include spot funds outside this equity snapshot.
      const cash = Decimal.max(
        0,
        Decimal.min(
          balance.spendableBalance,
          new Decimal(balance.totalBalance).minus(balance.marginUsed),
        ),
      );
      return {
        cash: cash.toFixed(),
        equity: balance.totalBalance,
        positionsValue: new Decimal(balance.totalBalance).minus(cash).toFixed(),
        currency: "USDC",
        positions: positions.map((p) => ({
          symbol: p.symbol,
          quantity: new Decimal(p.size)
            .mul(p.side === "short" ? -1 : 1)
            .toFixed(),
          entryPrice: p.entryPrice,
          marketValue: p.positionValue,
          unrealizedPnl: p.unrealizedPnl,
        })),
      };
    }
    const state = await this.walletState(binding.wallet);
    const balances = await this.wallet(state).getBalance({
      state,
      address: binding.wallet,
      chains: binding.chains.map((id) => `eip155:${id}`),
      testnetChainIds: binding.network === "testnet" ? binding.chains : [],
      tokenContracts: binding.assets.flatMap((a) =>
        a.address === "native"
          ? []
          : [z.templateLiteral(["0x", z.string()]).parse(a.address)],
      ),
    });
    const holdings = balances.balances.filter(
      (b) => !new Decimal(b.balance).isZero(),
    );
    assert(
      holdings.every(
        (b) =>
          b.value !== null &&
          b.value !== undefined &&
          !balances.unpricedAssetIds.includes(b.assetId),
      ) && balances.unprocessedNetworks.length === 0,
      "INCOMPLETE_VALUATION",
      "Wallet contains unpriced assets or unavailable networks",
    );
    return {
      cash: "0",
      equity: balances.totalValue,
      positionsValue: balances.totalValue,
      currency: "USD",
      positions: holdings.map((b) => ({
        symbol: b.symbol,
        quantity: b.balance,
        marketValue: b.value ?? "0",
        ...(binding.assets.find((a) => assetId(a) === b.assetId)
          ? { asset: binding.assets.find((a) => assetId(a) === b.assetId) }
          : {}),
      })),
    };
  }
}
