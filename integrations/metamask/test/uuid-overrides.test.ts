import { test } from "node:test";
import { strict as assert } from "node:assert";
import { createRequire } from "node:module";
import { z } from "zod";

test("vendor JSON-RPC request IDs retain CommonJS behavior with patched UUID", () => {
  const require = createRequire(import.meta.url);
  const request: (
    method: string,
    params: unknown[],
    id?: string | null,
  ) => unknown = require("jayson/lib/generateRequest");
  const schema = z.object({
    jsonrpc: z.literal("2.0"),
    method: z.literal("eth_chainId"),
    params: z.array(z.never()),
    id: z.string().uuid(),
  });
  const ids = new Set<string>();
  for (let i = 0; i < 100; i++)
    ids.add(schema.parse(request("eth_chainId", [])).id);
  assert.equal(ids.size, 100);
  assert.deepEqual(request("eth_chainId", [], "caller-id"), {
    jsonrpc: "2.0",
    method: "eth_chainId",
    params: [],
    id: "caller-id",
  });
  assert.deepEqual(request("eth_chainId", [], null), {
    jsonrpc: "2.0",
    method: "eth_chainId",
    params: [],
  });
});
