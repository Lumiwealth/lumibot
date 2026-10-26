import {
  createServer,
  type IncomingMessage,
  type ServerResponse,
} from "node:http";
import { randomBytes, timingSafeEqual } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { resolve, join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { Decimal } from "./numeric.js";
import { z } from "zod";
import {
  address,
  assetSchema,
  sameAsset,
  assert,
  bindingSchema,
  BrokerError,
  identifier,
  requestSchema,
} from "./contracts.js";
import { Store, digest } from "./store.js";
import { Engine } from "./engine.js";
import { MetaMaskProvider } from "./sdk.js";

const bodyLimit = 65536;
async function body(req: IncomingMessage): Promise<unknown> {
  let size = 0;
  const chunks: Buffer[] = [];
  for await (const chunk of req) {
    const data = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    size += data.length;
    assert(size <= bodyLimit, "TOO_LARGE", "Request too large");
    chunks.push(data);
  }
  return JSON.parse(Buffer.concat(chunks).toString("utf8"));
}
function json(res: ServerResponse, value: unknown, status = 200): void {
  res.writeHead(status, {
    "Content-Type": "application/json",
    "Cache-Control": "no-store",
    "X-Content-Type-Options": "nosniff",
  });
  res.end(JSON.stringify(value));
}
export function application(
  store: Store,
  provider: MetaMaskProvider,
  adminToken: string,
  port: number,
  allowMainnet = false,
) {
  const engine = new Engine(store, provider);
  const parent = resolve(dirname(fileURLToPath(import.meta.url)), "..");
  const root = existsSync(join(parent, "package.json"))
    ? parent
    : dirname(parent);
  return createServer(async (req, res) => {
    try {
      const boundPort = req.socket.localPort ?? port;
      assert(
        req.headers.host === `127.0.0.1:${boundPort}` ||
          req.headers.host === `localhost:${boundPort}`,
        "HOST_DENIED",
        "Unexpected Host",
      );
      if (req.headers.origin)
        assert(
          req.headers.origin === `http://${req.headers.host}`,
          "ORIGIN_DENIED",
          "Cross-origin requests denied",
        );
      const url = new URL(req.url ?? "/", "http://127.0.0.1");
      if (req.method === "GET" && ["/", "/main.js"].includes(url.pathname)) {
        const path =
          url.pathname === "/"
            ? join(root, "web/index.html")
            : join(root, "dist/web/setup.bundle.js");
        res.writeHead(200, {
          "Content-Type":
            url.pathname === "/" ? "text/html" : "text/javascript",
          "Cache-Control": "no-store",
          "X-Content-Type-Options": "nosniff",
          "Referrer-Policy": "no-referrer",
          "X-Frame-Options": "DENY",
        });
        res.end(readFileSync(path));
        return;
      }
      const bearer = req.headers.authorization?.replace(/^Bearer /, "") ?? "";
      if (url.pathname.startsWith("/admin/")) {
        assert(
          timingSafeEqual(
            Buffer.from(digest(bearer), "hex"),
            Buffer.from(digest(adminToken), "hex"),
          ),
          "UNAUTHORIZED",
          "Unlock setup with the local admin token",
        );
        if (req.method === "GET" && url.pathname === "/admin/status") {
          json(res, {
            authenticated: provider.session.get("cliToken") !== null,
            loginUrl: new URL(
              "/agentic/login",
              provider.config.dashboardUrl,
            ).toString(),
            bindings: store
              .bindings()
              .map(({ tokenHash, ...binding }) => binding),
          });
          return;
        }
        if (req.method === "POST" && url.pathname === "/admin/login") {
          const input = z
            .object({ token: z.string().min(10) })
            .strict()
            .parse(await body(req));
          await provider.login(input.token);
          json(res, { authenticated: true });
          return;
        }
        if (req.method === "GET" && url.pathname === "/admin/wallets") {
          json(res, await provider.listWallets());
          return;
        }
        if (req.method === "POST" && url.pathname === "/admin/wallets") {
          const input = z
            .object({ name: identifier })
            .strict()
            .parse(await body(req));
          json(res, await provider.createWallet(input.name));
          return;
        }
        if (req.method === "GET" && url.pathname === "/admin/policy") {
          const wallet = address.parse(url.searchParams.get("wallet"));
          await provider.walletState(wallet);
          json(res, {
            policy: await provider.policy.getPolicy(wallet),
            mode: await provider.modes.getMode(wallet),
            template: await provider.policy.getPolicyTemplate(),
          });
          return;
        }
        if (req.method === "POST" && url.pathname === "/admin/guard") {
          const input = z
            .object({ wallet: address })
            .strict()
            .parse(await body(req));
          await provider.walletState(input.wallet);
          json(
            res,
            await provider.modes.setMode({
              mode: "guard",
              walletAddress: input.wallet,
            }),
          );
          return;
        }
        if (req.method === "POST" && url.pathname === "/admin/policy") {
          const input = z
            .object({ wallet: address, policy: z.string().min(1).max(20000) })
            .strict()
            .parse(await body(req));
          await provider.walletState(input.wallet);
          json(
            res,
            await provider.policy.updatePolicy(input.wallet, input.policy),
          );
          return;
        }
        if (req.method === "POST" && url.pathname === "/admin/bindings") {
          const input = bindingSchema
            .omit({ tokenHash: true, policyDigest: true, enabled: true })
            .parse(await body(req));
          assert(
            allowMainnet || input.network === "testnet",
            "MAINNET_DISABLED",
            "Service was started for testnets only",
          );
          const token = randomBytes(32).toString("hex");
          const binding = bindingSchema.parse({
            ...input,
            enabled: true,
            tokenHash: digest(token),
            policyDigest: digest(await provider.policy.getPolicy(input.wallet)),
          });
          await provider.verify(binding);
          store.putBinding(binding);
          json(res, {
            agent: binding.id,
            token,
            endpoint: `http://127.0.0.1:${port}`,
            wallet: binding.wallet,
            network: binding.network,
          });
          return;
        }
        if (req.method === "POST" && url.pathname === "/admin/revoke") {
          const input = z
            .object({ id: identifier })
            .strict()
            .parse(await body(req));
          const binding = store.binding(input.id);
          store.putBinding({ ...binding, enabled: false });
          json(res, {
            revoked: true,
            scope:
              "LumiBot runtime access; onchain grants remain separately revocable",
          });
          return;
        }
        if (req.method === "POST" && url.pathname === "/admin/cancel") {
          const input = z
            .object({ agent: identifier, id: identifier })
            .strict()
            .parse(await body(req));
          json(res, await engine.cancel(store.binding(input.agent), input.id));
          return;
        }
        if (req.method === "POST" && url.pathname === "/admin/resume") {
          const input = z
            .object({ agent: identifier, id: identifier })
            .strict()
            .parse(await body(req));
          const binding = store.binding(input.agent);
          assert(
            binding.enabled && binding.expiresAt > Date.now(),
            "REVOKED",
            "Agent authorization expired or was revoked",
          );
          json(res, await engine.resume(binding, input.id));
          return;
        }
        if (req.method === "GET" && url.pathname === "/admin/operations") {
          const id = identifier.parse(url.searchParams.get("agent"));
          const binding = store.binding(id);
          const operations = [];
          for (const op of store.operations(id))
            operations.push(await engine.refresh(binding, op.id));
          json(res, operations);
          return;
        }
        throw new BrokerError("NOT_FOUND", "Unknown owner action");
      }
      const binding = store.authorize(bearer);
      if (req.method === "GET" && url.pathname === "/v1/identity") {
        json(res, {
          agent: binding.id,
          wallet: binding.wallet,
          network: binding.network,
          expiresAt: binding.expiresAt,
        });
        return;
      }
      if (req.method === "POST" && url.pathname === "/v1/orders") {
        json(
          res,
          await engine.submit(binding, requestSchema.parse(await body(req))),
        );
        return;
      }
      if (req.method === "GET" && url.pathname === "/v1/orders") {
        const results = [];
        for (const op of store.operations(binding.id))
          results.push(await engine.refresh(binding, op.id));
        json(res, results);
        return;
      }
      if (req.method === "POST" && url.pathname === "/v1/cancel") {
        const input = z
          .object({ id: identifier })
          .strict()
          .parse(await body(req));
        json(res, await engine.cancel(binding, input.id));
        return;
      }
      if (req.method === "GET" && url.pathname === "/v1/account") {
        const kind = z
          .enum(["perp", "spot"])
          .parse(url.searchParams.get("kind"));
        json(res, await provider.account(binding, kind));
        return;
      }
      if (req.method === "POST" && url.pathname === "/v1/token-price") {
        const input = z
          .object({ asset: assetSchema, quote: assetSchema.optional() })
          .strict()
          .parse(await body(req));
        for (const asset of [
          input.asset,
          ...(input.quote ? [input.quote] : []),
        ])
          assert(
            binding.assets.some((a) => sameAsset(a, asset)),
            "ASSET_DENIED",
            "Asset outside binding",
          );
        const base = await provider.tokenPrice(binding, input.asset);
        const quote = input.quote
          ? await provider.tokenPrice(binding, input.quote)
          : "1";
        json(res, { price: new Decimal(base).div(quote).toFixed() });
        return;
      }
      if (req.method === "GET" && url.pathname === "/v1/funding") {
        const input = z
          .object({
            startTime: z.coerce.number().int().positive(),
            endTime: z.coerce.number().int().positive(),
            symbol: z.string().optional(),
          })
          .parse(Object.fromEntries(url.searchParams));
        assert(
          input.startTime < input.endTime &&
            input.endTime - input.startTime <= 365 * 86400000,
          "RANGE_DENIED",
          "Funding window must be at most 365 days",
        );
        if (input.symbol)
          assert(
            binding.markets.includes(input.symbol),
            "MARKET_DENIED",
            "Market outside binding",
          );
        const result = await (
          await provider.perps(binding)
        ).getFundingPayments("hyperliquid", {
          startTime: input.startTime,
          endTime: input.endTime,
          ...(input.symbol ? { symbol: input.symbol } : {}),
        });
        json(res, {
          ...result,
          payments: result.payments.filter((p) =>
            binding.markets.includes(p.symbol),
          ),
        });
        return;
      }
      if (req.method === "GET" && url.pathname === "/v1/candles") {
        const input = z
          .object({
            symbol: z.string(),
            interval: z.enum(["1m", "1h", "1d"]),
            start: z.coerce.number().int().positive(),
            end: z.coerce.number().int().positive(),
          })
          .parse(Object.fromEntries(url.searchParams));
        assert(
          binding.markets.includes(input.symbol),
          "MARKET_DENIED",
          "Market outside binding",
        );
        const seconds = { "1m": 60, "1h": 3600, "1d": 86400 }[input.interval];
        assert(
          input.start < input.end &&
            input.end - input.start <= seconds * 1000 * 5001,
          "RANGE_DENIED",
          "Candle window too large",
        );
        const rows = z
          .array(
            z.object({
              t: z.number(),
              T: z.number(),
              o: z.string(),
              h: z.string(),
              l: z.string(),
              c: z.string(),
              v: z.string(),
            }),
          )
          .parse(
            await provider.info(binding, {
              type: "candleSnapshot",
              req: {
                coin: input.symbol,
                interval: input.interval,
                startTime: input.start,
                endTime: input.end,
              },
            }),
          );
        json(
          res,
          rows
            .filter((r) => r.T <= input.end)
            .map((r) => ({
              timestamp: r.t,
              open: r.o,
              high: r.h,
              low: r.l,
              close: r.c,
              volume: r.v,
            })),
        );
        return;
      }
      if (req.method === "GET" && url.pathname === "/v1/price") {
        const symbol = z.string().min(1).parse(url.searchParams.get("symbol"));
        assert(
          binding.markets.includes(symbol),
          "MARKET_DENIED",
          "Market outside agent scope",
        );
        json(res, { price: await provider.price(binding, symbol) });
        return;
      }
      throw new BrokerError("NOT_FOUND", "Unknown broker operation");
    } catch (error) {
      if (error instanceof BrokerError)
        json(
          res,
          { error: { code: error.code, message: error.message } },
          error.code === "UNAUTHORIZED" ? 401 : 400,
        );
      else if (error instanceof z.ZodError || error instanceof SyntaxError)
        json(
          res,
          {
            error: {
              code: "INVALID_INPUT",
              message: "Invalid request structure",
            },
          },
          400,
        );
      else
        json(
          res,
          {
            error: {
              code: "PROVIDER_ERROR",
              message:
                "MetaMask request failed. Check authentication, wallet approvals, and provider availability.",
            },
          },
          502,
        );
    }
  });
}

if (
  process.argv[1] &&
  resolve(process.argv[1]) === fileURLToPath(import.meta.url)
) {
  const value = (flag: string, fallback: string) => {
    const index = process.argv.indexOf(flag);
    return index < 0 ? fallback : (process.argv[index + 1] ?? fallback);
  };
  const directory = resolve(value("--state-dir", ".state"));
  const port = z.coerce
    .number()
    .int()
    .min(1024)
    .max(65535)
    .parse(value("--port", "8766"));
  mkdirSync(directory, { recursive: true, mode: 0o700 });
  const tokenPath = join(directory, "admin-token");
  if (!existsSync(tokenPath))
    writeFileSync(tokenPath, randomBytes(32).toString("hex"), { mode: 0o600 });
  const store = new Store(join(directory, "broker.sqlite"));
  const server = application(
    store,
    new MetaMaskProvider(directory),
    readFileSync(tokenPath, "utf8").trim(),
    port,
    process.argv.includes("--allow-mainnet"),
  );
  server.listen(port, "127.0.0.1", () =>
    console.log(
      `MetaMask setup: http://127.0.0.1:${port}\nUnlock using token file: ${tokenPath}`,
    ),
  );
  const stop = () =>
    server.close(() => {
      store.close();
      process.exit(0);
    });
  process.on("SIGTERM", stop);
  process.on("SIGINT", stop);
}
