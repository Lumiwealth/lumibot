import { test, mock } from "node:test";
import { strict as assert } from "node:assert";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  FoxWalletService,
  WalletRuntimeMode,
  type EvmSignTypedDataInput,
} from "@metamask/agent-sdk";
import { MetaMaskProvider } from "../src/sdk.js";
import { PerpsWallet, type SigningBoundary } from "../src/perps-wallet.js";

function fixture(boundary: SigningBoundary) {
  const dir = mkdtempSync(join(tmpdir(), "metamask-signer-"));
  const provider = new MetaMaskProvider(dir);
  const wallet = new PerpsWallet(
    {
      mode: WalletRuntimeMode.Server,
      authToken: "test",
      projectId: "test",
      wallets: [],
      serverWalletBaseUrl: provider.config.serverWalletBaseUrl,
      accountsApiBaseUrl: provider.config.accountsApiBaseUrl,
      infuraRpcBaseUrl: provider.config.infuraRpcBaseUrl,
      priceService: provider.prices,
      networkRegistry: provider.networks,
    },
    boundary,
  );
  const input: EvmSignTypedDataInput = {
    state: provider.state(),
    chainId: 421614,
    typedData: {
      domain: {},
      types: { Message: [{ name: "value", type: "string" }] },
      primaryType: "Message",
      message: { value: "test" },
    },
  };
  return {
    wallet,
    input,
    cleanup: () => {
      mock.restoreAll();
      rmSync(dir, { recursive: true });
    },
  };
}

test("revocation between SDK leverage and order signatures blocks the next signature", async () => {
  let enabled = true;
  const check = async () => {
    assert.ok(enabled, "revoked");
  };
  const f = fixture({ before: check, release: check, pending() {} });
  const signed = mock.method(
    FoxWalletService.prototype,
    "signEvmTypedData",
    async () => ({ signature: "0x1234" }),
  );
  try {
    await f.wallet.signEvmTypedData(f.input);
    enabled = false;
    await assert.rejects(() => f.wallet.signEvmTypedData(f.input), /revoked/);
    assert.equal(signed.mock.callCount(), 1);
  } finally {
    f.cleanup();
  }
});

test("pending signature is journaled before waiting and rechecked before release", async () => {
  const events: string[] = [];
  const f = fixture({
    before: async () => {
      events.push("check");
    },
    pending: (id) => {
      events.push(id);
    },
    release: async () => {
      events.push("release");
    },
  });
  mock.method(
    FoxWalletService.prototype,
    "signEvmTypedData",
    async (input: EvmSignTypedDataInput) => {
      assert.equal(input.wait, false);
      return { status: "AWAITING_MFA", pendingJob: { pollingId: "job" } };
    },
  );
  mock.method(f.wallet, "awaitJob", async () => {
    events.push("wait");
    return { kind: "signature", status: "SIGNED", signature: "0x1234" };
  });
  try {
    assert.equal(
      (await f.wallet.signEvmTypedData(f.input)).signature,
      "0x1234",
    );
    assert.deepEqual(events, ["check", "job", "wait", "release"]);
  } finally {
    f.cleanup();
  }
});

test("revocation during approval prevents releasing the completed signature", async () => {
  let enabled = true;
  const check = async () => {
    assert.ok(enabled, "revoked");
  };
  const f = fixture({ before: check, pending() {}, release: check });
  mock.method(FoxWalletService.prototype, "signEvmTypedData", async () => ({
    pendingJob: { pollingId: "job" },
  }));
  mock.method(f.wallet, "awaitJob", async () => {
    enabled = false;
    return { kind: "signature", status: "SIGNED", signature: "0x1234" };
  });
  try {
    await assert.rejects(() => f.wallet.signEvmTypedData(f.input), /revoked/);
  } finally {
    f.cleanup();
  }
});

test("signature timeout retains recovery evidence and never releases a signature", async () => {
  let jobId: string | undefined;
  let released = false;
  const f = fixture({
    before: async () => {},
    pending: (id) => {
      jobId = id;
    },
    release: async () => {
      released = true;
    },
  });
  mock.method(FoxWalletService.prototype, "signEvmTypedData", async () => ({
    pendingJob: { pollingId: "job" },
  }));
  mock.method(f.wallet, "awaitJob", async () => {
    throw new Error("timeout");
  });
  try {
    await assert.rejects(() => f.wallet.signEvmTypedData(f.input), /timeout/);
    assert.equal(jobId, "job");
    assert.equal(released, false);
  } finally {
    f.cleanup();
  }
});
