import { test, mock } from "node:test";
import { strict as assert } from "node:assert";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { MetaMaskProvider } from "../src/sdk.js";
import { Store } from "../src/store.js";
import { PolicyApprovals } from "../src/policy-approvals.js";
const wallet = "0x" + "1".repeat(40);

for (const outcome of ["approved", "denied", "expired"] as const) {
  test(`policy approval survives restart and records ${outcome} without resubmission`, async () => {
    const dir = mkdtempSync(join(tmpdir(), "metamask-policy-"));
    let store = new Store(join(dir, "broker.sqlite"));
    const provider = new MetaMaskProvider(dir);
    const ownership = mock.method(provider, "walletState", async () =>
      provider.state(wallet),
    );
    const submit = mock.method(provider.policy, "updatePolicy", async () => ({
      applied: false,
      mfaRequired: true,
      requestId: "remote-request",
    }));
    // Captured live policy response shape, deliberately different from SDK declarations.
    const status = mock.method(provider.approvals, "getApproval", async () => ({
      requestId: "remote-request",
      status: outcome,
    }));
    try {
      let approvals = new PolicyApprovals(store, provider);
      const pending = await approvals.submit("request", wallet, "policy");
      assert.equal(pending.state, "pending");
      assert.deepEqual(
        await approvals.submit("request", wallet, "policy"),
        pending,
      );
      assert.equal(submit.mock.callCount(), 1);
      await assert.rejects(
        approvals.submit("request", wallet, "different"),
        /different content/,
      );
      await assert.rejects(
        approvals.submit("other", wallet, "policy"),
        /Resolve the existing/,
      );
      store.close();
      store = new Store(join(dir, "broker.sqlite"));
      approvals = new PolicyApprovals(store, provider);
      const rows = await approvals.list(wallet);
      assert.equal(rows[0]?.state, outcome);
      assert.equal(submit.mock.callCount(), 1);
      assert.equal(status.mock.callCount(), 1);
      await approvals.list(wallet);
      assert.equal(status.mock.callCount(), 1);
    } finally {
      ownership.mock.restore();
      submit.mock.restore();
      status.mock.restore();
      store.close();
      rmSync(dir, { recursive: true });
    }
  });
}

test("lost policy response remains unknown and cannot duplicate a widening request", async () => {
  const dir = mkdtempSync(join(tmpdir(), "metamask-policy-"));
  const store = new Store(":memory:");
  const provider = new MetaMaskProvider(dir);
  const ownership = mock.method(provider, "walletState", async () =>
    provider.state(wallet),
  );
  const submit = mock.method(provider.policy, "updatePolicy", async () => {
    throw new Error("connection lost");
  });
  try {
    const approvals = new PolicyApprovals(store, provider);
    await assert.rejects(
      approvals.submit("request", wallet, "policy"),
      /connection lost/,
    );
    assert.equal(
      (await approvals.submit("request", wallet, "policy")).state,
      "unknown",
    );
    assert.equal(submit.mock.callCount(), 1);
  } finally {
    ownership.mock.restore();
    submit.mock.restore();
    store.close();
    rmSync(dir, { recursive: true });
  }
});

test("mismatched approval evidence cannot change a pending policy request", async () => {
  const dir = mkdtempSync(join(tmpdir(), "metamask-policy-"));
  const store = new Store(":memory:");
  const provider = new MetaMaskProvider(dir);
  const ownership = mock.method(provider, "walletState", async () =>
    provider.state(wallet),
  );
  const submit = mock.method(provider.policy, "updatePolicy", async () => ({
    applied: false,
    mfaRequired: true,
    requestId: "expected",
  }));
  const status = mock.method(provider.approvals, "getApproval", async () => ({
    requestId: "other",
    status: "approved",
  }));
  try {
    const approvals = new PolicyApprovals(store, provider);
    await approvals.submit("request", wallet, "policy");
    await assert.rejects(approvals.list(wallet), /does not match/);
    assert.equal(store.policyApprovals(wallet)[0]?.state, "pending");
  } finally {
    ownership.mock.restore();
    submit.mock.restore();
    status.mock.restore();
    store.close();
    rmSync(dir, { recursive: true });
  }
});
