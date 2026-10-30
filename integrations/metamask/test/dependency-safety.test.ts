import { test } from "node:test";
import { strict as assert } from "node:assert";
import { assertDependencySafety } from "../src/dependency-safety.js";

test("the supported clean install has no loadable vulnerable native addon", () => {
  assert.doesNotThrow(() => assertDependencySafety());
});
test("a native addon or an indeterminate loader failure blocks managed-wallet startup", () => {
  assert.throws(
    () => assertDependencySafety(() => "/native/bigint_buffer.node"),
    /Unsupported bigint-buffer native addon/,
  );
  assert.throws(
    () =>
      assertDependencySafety(() => {
        throw new Error("EACCES");
      }),
    /Cannot verify/,
  );
  assert.throws(
    () =>
      assertDependencySafety(() => {
        throw Object.assign(new Error("missing"), { tries: [] });
      }),
    /Cannot verify/,
  );
  assert.doesNotThrow(() =>
    assertDependencySafety(() => {
      throw Object.assign(new Error("missing"), {
        tries: ["/missing/bigint_buffer.node"],
      });
    }),
  );
});
