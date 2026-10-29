import { DatabaseSync } from "node:sqlite";
import { chmodSync } from "node:fs";
import { BrokerError } from "./contracts.js";

/** Local-filesystem lock released by the OS even after abrupt process death. */
export function acquireStateLock(path: string, message: string): () => void {
  const owner = new DatabaseSync(path);
  try {
    chmodSync(path, 0o600);
    owner.exec(
      "PRAGMA journal_mode=DELETE; CREATE TABLE IF NOT EXISTS owner (id INTEGER); BEGIN EXCLUSIVE",
    );
  } catch (error) {
    owner.close();
    if (
      error instanceof Error &&
      "errcode" in error &&
      (error.errcode === 5 || error.errcode === 6)
    )
      throw new BrokerError("STATE_IN_USE", message);
    throw error;
  }
  let released = false;
  return () => {
    if (!released) {
      owner.close();
      released = true;
    }
  };
}
