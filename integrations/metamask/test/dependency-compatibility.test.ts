import { test } from "node:test";
import { strict as assert } from "node:assert";
import { createServer } from "node:http";
import { once } from "node:events";
import { HttpClient } from "@polymarket/builder-relayer-client/dist/http-helpers/index.js";

test("vendor HTTP wrapper retains request and error behavior with Axios security backport", async (t) => {
  const server = createServer(async (req, res) => {
    const chunks: Buffer[] = [];
    for await (const chunk of req) chunks.push(Buffer.from(chunk));
    res.writeHead(req.url === "/fail" ? 503 : 200, {
      "Content-Type": "application/json",
    });
    res.end(
      JSON.stringify({
        method: req.method,
        url: req.url,
        authorization: req.headers.authorization,
        body: Buffer.concat(chunks).toString(),
      }),
    );
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address();
  assert(address && typeof address === "object");
  const url = `http://127.0.0.1:${address.port}`;
  const client = new HttpClient();
  client.instance.defaults.proxy = false;
  try {
    const result = await client.send(url + "/submit", "POST", {
      headers: { Authorization: "Bearer fixture" },
      params: { network: "testnet" },
      data: { id: "fixture" },
    });
    assert.equal(result.status, 200);
    assert.deepEqual(result.data, {
      method: "POST",
      url: "/submit?network=testnet",
      authorization: "Bearer fixture",
      body: '{"id":"fixture"}',
    });
    t.mock.method(console, "error", () => {});
    await assert.rejects(client.send(url + "/fail", "GET"), /"status":503/);
  } finally {
    server.close();
    await once(server, "close");
  }
});
