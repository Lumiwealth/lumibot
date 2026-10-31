import { test } from "node:test";
import { strict as assert } from "node:assert";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { Store } from "../src/store.js";

test("one journal has one execution owner, including across processes", async () => {
  const directory = mkdtempSync(join(tmpdir(), "metamask-owner-"));
  const path = join(directory, "broker.sqlite");
  const child = spawn(
    process.execPath,
    [
      "--input-type=module",
      "-e",
      `import { Store } from ${JSON.stringify(new URL("../src/store.js", import.meta.url).href)};
       const store = new Store(process.argv[1]);
       process.stdout.write('ready');
       setInterval(() => { store.bindings(); }, 1000);`,
      path,
    ],
    { stdio: ["ignore", "pipe", "pipe"] },
  );
  const exited = once(child, "exit");
  try {
    const [data] = await once(child.stdout, "data", {
      signal: AbortSignal.timeout(5000),
    });
    assert.equal(String(data), "ready");
    assert.throws(() => new Store(path), /already in use/);
    // A crashed owner must release without TTLs, PID guessing or deleting files.
    child.kill("SIGKILL");
    await exited;
    const recovered = new Store(path);
    try {
      assert.deepEqual(recovered.bindings(), []);
      assert.throws(() => new Store(path), /already in use/);
      const independent = new Store(join(directory, "other.sqlite"));
      independent.close();
    } finally {
      recovered.close();
    }
    const reopened = new Store(path);
    reopened.close();
  } finally {
    if (child.exitCode === null && child.signalCode === null) {
      child.kill("SIGKILL");
      await exited;
    }
    rmSync(directory, { recursive: true });
  }
});
