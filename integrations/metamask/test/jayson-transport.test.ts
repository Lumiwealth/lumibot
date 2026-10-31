import { test } from "node:test";
import { strict as assert } from "node:assert";
import { createServer } from "node:http";
import { createRequire } from "node:module";
import { Connection } from "@solana/web3.js";
import { z } from "zod";

test("the SDK's Solana HTTP client preserves JSON-RPC success, errors and malformed-response rejection", async () => {
  const seen: string[] = [];
  const server = createServer(async (req, res) => {
    let body = "";
    for await (const part of req) body += part;
    const request = z
      .object({ method: z.string(), id: z.string() })
      .parse(JSON.parse(body));
    seen.push(request.method);
    res.setHeader("content-type", "application/json");
    if (request.method === "getBlockHeight") return res.end("invalid-json");
    res.end(
      JSON.stringify({
        jsonrpc: "2.0",
        id: request.id,
        ...(request.method === "getSlot"
          ? { error: { code: -32000, message: "Test RPC rejection" } }
          : { result: { "solana-core": "2.1.0", "feature-set": 123 } }),
      }),
    );
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  try {
    const address = server.address();
    assert(address && typeof address !== "string");
    const connection = new Connection(`http://127.0.0.1:${address.port}`);
    assert.deepEqual(await connection.getVersion(), {
      "solana-core": "2.1.0",
      "feature-set": 123,
    });
    await assert.rejects(() => connection.getSlot(), /Test RPC rejection/);
    await assert.rejects(() => connection.getBlockHeight(), /JSON/);
    assert.deepEqual(seen, ["getVersion", "getSlot", "getBlockHeight"]);
  } finally {
    await new Promise<void>((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve())),
    );
  }
});

test("the vendor's exact browser entry point preserves notification behavior", async () => {
  const require = createRequire(import.meta.url);
  const vendorRequire = createRequire(
    require.resolve("@solana/web3.js/package.json"),
  );
  const Client: new (
    transport: (
      body: string,
      callback: (error?: Error | null, response?: string) => void,
    ) => void,
  ) => {
    request(
      method: string,
      params: unknown[],
      id: null,
      callback: (error?: Error | null, response?: unknown) => void,
    ): void;
  } = vendorRequire("jayson/lib/client/browser");
  const sent: unknown[] = [];
  const client = new Client((body, callback) => {
    sent.push(JSON.parse(body));
    callback(null, "");
  });
  await new Promise<void>((resolve, reject) =>
    client.request("testNotification", [], null, (error, response) => {
      if (error) return reject(error);
      assert.equal(response, undefined);
      resolve();
    }),
  );
  assert.deepEqual(sent, [
    { jsonrpc: "2.0", method: "testNotification", params: [] },
  ]);
});
