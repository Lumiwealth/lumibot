import { z } from "zod";
import { assert, type PolicyApproval } from "./contracts.js";
import { Store, digest } from "./store.js";
import { MetaMaskProvider } from "./sdk.js";

const status = z.enum(["pending", "approved", "denied", "expired"]);
// The current policy API returns a compact request/status response. Transaction
// approvals may use the expanded SDK shape; normalize both at this boundary.
const approvalResponse = z.union([
  z.object({ requestId: z.string(), status }),
  z.object({
    subjectId: z.string(),
    status,
    operationType: z.literal("wallet_policy_update"),
    expiresAt: z.string(),
    subject: z.object({ walletAddress: z.string() }),
  }),
]);

export class PolicyApprovals {
  constructor(
    private store: Store,
    private provider: MetaMaskProvider,
  ) {}

  async submit(
    id: string,
    wallet: string,
    policy: string,
  ): Promise<PolicyApproval> {
    await this.provider.walletState(wallet);
    const claimed = this.store.beginPolicyApproval({
      id,
      wallet,
      policyDigest: digest(policy),
      state: "unknown",
      createdAt: Date.now(),
      updatedAt: Date.now(),
    });
    if (!claimed.created) return claimed.approval;
    // A lost response must not cause a second policy-widening request.
    const result = await this.provider.policy.updatePolicy(wallet, policy);
    return this.store.updatePolicyApproval(claimed.approval, {
      ...claimed.approval,
      state: result.applied
        ? "applied"
        : result.mfaRequired && result.requestId
          ? "pending"
          : "unknown",
      ...(result.requestId ? { requestId: result.requestId } : {}),
      updatedAt: Date.now(),
    });
  }

  async list(wallet: string): Promise<PolicyApproval[]> {
    await this.provider.walletState(wallet);
    const results: PolicyApproval[] = [];
    for (const previous of this.store.policyApprovals(wallet)) {
      if (previous.state !== "pending" || !previous.requestId) {
        results.push(previous);
        continue;
      }
      const approval = approvalResponse.parse(
        await this.provider.approvals.getApproval(previous.requestId),
      );
      const requestId =
        "requestId" in approval ? approval.requestId : approval.subjectId;
      assert(
        requestId === previous.requestId &&
          ("requestId" in approval ||
            approval.subject.walletAddress.toLowerCase() === wallet),
        "APPROVAL_MISMATCH",
        "Provider approval does not match the recorded wallet policy request",
      );
      results.push(
        this.store.updatePolicyApproval(previous, {
          ...previous,
          state: approval.status,
          ...("expiresAt" in approval ? { expiresAt: approval.expiresAt } : {}),
          updatedAt: Date.now(),
        }),
      );
    }
    return results;
  }
}
