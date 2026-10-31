import { createRequire } from "node:module";
import { dirname } from "node:path";
import { z } from "zod";

const require = createRequire(import.meta.url);
function locateNativeBigint(): string {
  const locate: (options: {
    bindings: string;
    module_root: string;
    path: true;
  }) => string = require("bindings");
  return locate({
    bindings: "bigint_buffer",
    module_root: dirname(require.resolve("bigint-buffer/package.json")),
    path: true,
  });
}

/** The unpatched bigint-buffer advisory affects its optional native addon. */
export function assertDependencySafety(
  locate: () => string = locateNativeBigint,
): void {
  try {
    locate();
  } catch (error) {
    // bindings reports attempted paths only when no native candidate exists.
    if (
      z.object({ tries: z.array(z.string()).nonempty() }).safeParse(error)
        .success
    )
      return;
    throw new Error(
      "Cannot verify bigint-buffer native addon safety. Reinstall with npm ci --ignore-scripts.",
    );
  }
  throw new Error(
    "Unsupported bigint-buffer native addon found. Reinstall with npm ci --ignore-scripts before starting the broker.",
  );
}
