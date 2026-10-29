import { test } from "node:test";
import { strict as assert } from "node:assert";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { request as httpRequest } from "node:http";
import { once } from "node:events";
import { application } from "../src/server.js";
import { MetaMaskProvider } from "../src/sdk.js";
import { Store, digest } from "../src/store.js";
import { bindingSchema } from "../src/contracts.js";

test("real HTTP server isolates admin and agent access, checks origins and serves setup", async () => {
  const directory = mkdtempSync(join(tmpdir(), "metamask-http-"));
  const store = new Store(":memory:");
  store.putBinding(
    bindingSchema.parse({
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
    }),
  );
  const provider = new MetaMaskProvider(directory);
  const server = application(store, provider, "owner", 8766);
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address();
  assert(address && typeof address === "object");
  const url = `http://127.0.0.1:${address.port}`;
  const call = (
    path: string,
    token: string,
    extra: Record<string, string> = {},
  ) =>
    fetch(url + path, {
      headers: { Authorization: `Bearer ${token}`, ...extra },
    });
  try {
    assert.equal((await call("/admin/status", "runtime")).status, 401);
    assert.equal(
      (await call("/admin/collateral?agent=agent", "runtime")).status,
      401,
    );
    assert.equal(
      (
        await fetch(url + "/admin/collateral", {
          method: "POST",
          headers: { Authorization: "Bearer runtime" },
          body: JSON.stringify({ agent: "agent", id: "fund", amount: "1" }),
        })
      ).status,
      401,
    );
    assert.equal(
      (
        await fetch(url + "/admin/collateral", {
          method: "POST",
          headers: { Authorization: "Bearer owner" },
          body: JSON.stringify({
            agent: "agent",
            id: "fund",
            amount: "1",
            recipient: "0x" + "2".repeat(40),
          }),
        })
      ).status,
      400,
    );
    assert.equal((await call("/v1/identity", "owner")).status, 401);
    assert.equal(
      (await call("/admin/status", "owner", { Origin: "https://evil.invalid" }))
        .status,
      400,
    );
    const invalidHost = await new Promise<number | undefined>(
      (resolve, reject) => {
        const req = httpRequest(
          url + "/admin/status",
          { headers: { Host: "evil.invalid", Authorization: "Bearer owner" } },
          (res) => {
            res.resume();
            resolve(res.statusCode);
          },
        );
        req.on("error", reject);
        req.end();
      },
    );
    assert.equal(invalidHost, 400);
    const state = await (await call("/admin/status", "owner")).json();
    assert.equal(state.authenticated, false);
    assert.equal(state.loginUrl, "https://developer.metamask.io/agentic/login");
    assert.equal(JSON.stringify(state).includes("tokenHash"), false);
    provider.session.update({
      cliToken: "expired-session",
      loginMethod: "google",
    });
    provider.auth.isCliTokenExpiringSoon = () => true;
    provider.auth.refreshCliToken = async () => {
      throw new Error("Refresh unavailable");
    };
    const expired = await (await call("/admin/status", "owner")).json();
    assert.equal(expired.authenticated, false);
    assert.equal(expired.loginMethod, "google");
    assert.match(expired.authenticationError, /original sign-in method/);
    assert.equal(provider.session.get("cliToken"), "expired-session");
    const identity = await (await call("/v1/identity", "runtime")).json();
    assert.equal(identity.network, "testnet");
    const page = await call("/", "");
    assert.equal(page.status, 200);
    assert.match(await page.text(), /MetaMask broker/);
    assert.equal((await call("/v1/orders", "runtime")).status, 200);
    assert.equal(
      (
        await fetch(url + "/v1/orders", {
          method: "POST",
          headers: { Authorization: "Bearer runtime" },
          body: '{"kind":"rawTransaction"}',
        })
      ).status,
      400,
    );
  } finally {
    server.close();
    await once(server, "close");
    store.close();
    rmSync(directory, { recursive: true });
  }
});
