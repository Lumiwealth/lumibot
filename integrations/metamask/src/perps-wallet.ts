import {
  FoxWalletService,
  type EvmSignTypedDataInput,
  type EvmSignatureResult,
  type WalletRuntimeConfig,
} from "@metamask/agent-sdk";
import { assert } from "./contracts.js";

export interface SigningBoundary {
  before(): Promise<void>;
  pending(id: string): void;
  release(): Promise<void>;
}

/** Public SDK wallet extension: journal each signature before venue submission. */
export class PerpsWallet extends FoxWalletService {
  constructor(
    config: WalletRuntimeConfig,
    private boundary: SigningBoundary,
  ) {
    super(config);
  }

  override async signEvmTypedData(
    input: EvmSignTypedDataInput,
  ): Promise<EvmSignatureResult> {
    await this.boundary.before();
    let result = await super.signEvmTypedData({ ...input, wait: false });
    if (!result.signature && result.pendingJob) {
      this.boundary.pending(result.pendingJob.pollingId);
      const job = await this.awaitJob(
        "evm",
        {
          kind: "signature",
          pollingId: result.pendingJob.pollingId,
        },
        { timeoutMs: 30000, ...(input.signal ? { signal: input.signal } : {}) },
      );
      if (job.kind === "signature")
        result = {
          ...result,
          status: job.status,
          ...(job.signature ? { signature: job.signature } : {}),
          job,
        };
    }
    assert(
      typeof result.signature === "string",
      "SIGNATURE_PENDING",
      "Signature unavailable; no venue order submitted by this signing step",
    );
    // Recheck after any provider approval wait, before giving the SDK a usable signature.
    // Persist unknown first: a crash after this point may have reached the venue.
    await this.boundary.release();
    return result;
  }
}
