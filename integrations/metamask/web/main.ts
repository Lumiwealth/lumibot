import { createEVMClient } from "@metamask/connect-evm";
import {
  createWalletClient,
  custom,
  parseEther,
  getAddress,
  formatEther,
} from "viem";
import { erc7715ProviderActions } from "@metamask/smart-accounts-kit/actions";
import { fundingNonce, nativeTransfer } from "./native-transfer.js";
import { z } from "zod";
import {
  assetSchema,
  bindingSchema,
  operationSchema,
  policyApprovalSchema,
  collateralBalanceSchema,
  collateralTransferSchema,
  nativeFundingSchema,
  type NativeFunding,
} from "../src/contracts.js";

function element(id: string): HTMLElement {
  const value = document.getElementById(id);
  if (!value) throw new Error(`Missing element: ${id}`);
  return value;
}
function input(id: string): HTMLInputElement {
  const value = element(id);
  if (!(value instanceof HTMLInputElement)) throw new Error(id);
  return value;
}
function select(id: string): HTMLSelectElement {
  const value = element(id);
  if (!(value instanceof HTMLSelectElement)) throw new Error(id);
  return value;
}
function textarea(id: string): HTMLTextAreaElement {
  const value = element(id);
  if (!(value instanceof HTMLTextAreaElement)) throw new Error(id);
  return value;
}
function notice(message: string, error = false) {
  const node = element("notice");
  node.textContent = message;
  node.hidden = false;
  node.classList.toggle("error", error);
}
let admin = "";
let connected: Awaited<ReturnType<typeof createEVMClient>> | undefined;
let browserAccount: `0x${string}` | undefined;
let boundWallets = new Set<string>();
let boundAgents = new Map<string, string>();
let nativeFundingPoll: ReturnType<typeof setTimeout> | undefined;
const nativeSigning = new Set<string>();
let approvalChannel = "your MetaMask approval channel";
const statusSchema = z.object({
  authenticated: z.boolean(),
  loginMethod: z.string().nullable(),
  authenticationError: z.string().optional(),
  loginUrl: z.string().url(),
  bindings: z.array(bindingSchema.omit({ tokenHash: true })),
});
async function api(path: string, payload?: unknown): Promise<unknown> {
  const result = await fetch(`/admin/${path}`, {
    method: payload === undefined ? "GET" : "POST",
    headers: {
      Authorization: `Bearer ${admin}`,
      "Content-Type": "application/json",
    },
    ...(payload !== undefined ? { body: JSON.stringify(payload) } : {}),
  });
  const data: unknown = await result.json();
  if (!result.ok) {
    const error = z
      .object({ error: z.object({ code: z.string(), message: z.string() }) })
      .parse(data);
    throw new Error(`${error.error.code}: ${error.error.message}`);
  }
  return data;
}
function action(id: string, callback: () => Promise<void>, event = "click") {
  element(id).addEventListener(event, async (e) => {
    e.preventDefault();
    const controls = element(id).querySelectorAll("button");
    if (element(id) instanceof HTMLButtonElement)
      element(id).setAttribute("disabled", "");
    controls.forEach((b) => (b.disabled = true));
    try {
      await callback();
    } catch (error) {
      notice(error instanceof Error ? error.message : "Request failed", true);
    } finally {
      element(id).removeAttribute("disabled");
      controls.forEach((b) => (b.disabled = false));
    }
  });
}
async function refresh() {
  const state = statusSchema.parse(await api("status"));
  element("unlock").hidden = true;
  element("setup").hidden = false;
  element("scope").hidden = !state.authenticated;
  element("login").hidden = state.authenticated;
  element("wallets").hidden = !state.authenticated;
  element("identity-warning").hidden = true;
  element("reconnect-session").hidden = !state.authenticated;
  element("session-state").textContent =
    state.authenticationError ??
    (state.authenticated
      ? `MetaMask session ready${state.loginMethod ? ` · ${state.loginMethod} sign-in` : ""}.`
      : `Sign in${state.loginMethod ? ` using your original ${state.loginMethod} method` : ""}.`);
  boundWallets = new Set(state.bindings.map((b) => b.wallet.toLowerCase()));
  boundAgents = new Map(
    state.bindings.map((b) => [b.wallet.toLowerCase(), b.id]),
  );
  approvalChannel =
    state.loginMethod === "qr"
      ? "MetaMask Mobile"
      : ["google", "email"].includes(state.loginMethod ?? "")
        ? "your registered email"
        : "your MetaMask approval channel";
  const link = element("login-link");
  if (link instanceof HTMLAnchorElement) link.href = state.loginUrl;
  const list = element("bindings");
  list.replaceChildren();
  const operationAgent = select("operation-agent").value;
  const collateralAgent = select("collateral-agent").value;
  select("operation-agent").replaceChildren();
  select("collateral-agent").replaceChildren();
  for (const binding of state.bindings) {
    const div = document.createElement("div");
    div.className = "binding";
    const title = document.createElement("strong");
    const access = !binding.enabled
      ? "Revoked"
      : binding.expiresAt <= Date.now()
        ? "Expired"
        : "Enabled";
    title.textContent = `${binding.id} · ${access} · ${binding.network}`;
    const wallet = document.createElement("p");
    wallet.textContent = binding.wallet;
    const limits = document.createElement("p");
    limits.textContent = `$${binding.maxOrderUsd}/order · $${binding.maxDailyUsd}/24h · expires ${new Date(binding.expiresAt).toLocaleString()}`;
    div.append(title, wallet, limits);
    if (binding.enabled) {
      const button = document.createElement("button");
      button.className = "secondary";
      button.textContent = "Revoke bot access";
      button.onclick = async () => {
        button.disabled = true;
        try {
          await api("revoke", { id: binding.id });
          await refresh();
          notice(
            "Bot access revoked. Existing orders and onchain permissions remain; review them separately.",
          );
        } catch (e) {
          notice(String(e), true);
        } finally {
          button.disabled = false;
        }
      };
      div.append(button);
    }
    list.append(div);
    select("operation-agent").add(new Option(binding.id, binding.id));
    if (binding.network === "testnet")
      select("collateral-agent").add(new Option(binding.id, binding.id));
  }
  if (state.bindings.some((b) => b.id === operationAgent))
    select("operation-agent").value = operationAgent;
  if (
    state.bindings.some(
      (b) => b.id === collateralAgent && b.network === "testnet",
    )
  )
    select("collateral-agent").value = collateralAgent;
  if (!state.bindings.length) list.textContent = "No agent bindings yet.";
  if (state.authenticated) await wallets();
}
async function wallets() {
  const rows = z
    .array(
      z.object({ address: z.string(), name: z.string(), kind: z.string() }),
    )
    .parse(await api("wallets"));
  const addresses = new Set(rows.map((row) => row.address.toLowerCase()));
  const missing = [...boundWallets].some((address) => !addresses.has(address));
  element("identity-warning").hidden = !missing;
  element("identity-warning").textContent = missing
    ? "Existing agent wallets are missing from this session. Reconnect using the sign-in method that created them before funding or renewing access."
    : "";
  const menu = select("wallet-select"),
    previous = menu.value;
  menu.replaceChildren(new Option("Select wallet", ""));
  for (const wallet of rows.filter((w) => w.kind === "server"))
    menu.add(
      new Option(
        `${wallet.name || "Agent wallet"} · ${wallet.address}`,
        wallet.address.toLowerCase(),
      ),
    );
  if (rows.some((w) => w.address.toLowerCase() === previous))
    menu.value = previous;
  if (menu.value !== previous) clearPolicy();
}
function wallet() {
  const value = select("wallet-select").value;
  if (!value) throw new Error("Select an agent wallet first");
  return getAddress(value);
}
let policyWallet: string | undefined;
let policyRevision = 0;
function clearPolicy() {
  policyWallet = undefined;
  policyRevision++;
  textarea("policy").value = "";
  element("policy-area").hidden = true;
  element("policy-approvals").replaceChildren();
}
select("wallet-select").addEventListener("change", clearPolicy);
action(
  "unlock-form",
  async () => {
    admin = input("admin-token").value.trim();
    await refresh();
    input("admin-token").value = "";
    notice("Local setup unlocked.");
  },
  "submit",
);
action(
  "login-form",
  async () => {
    const token = input("cli-token").value.trim();
    await api("login", { token });
    input("cli-token").value = "";
    await refresh();
    notice("MetaMask session connected.");
  },
  "submit",
);
action("refresh-wallets", wallets);
action("refresh-status", refresh);
action("reconnect-session", async () => {
  element("login").hidden = false;
  input("cli-token").focus();
});
action(
  "wallet-form",
  async () => {
    const name = input("wallet-name").value;
    const result = z
      .object({ address: z.string() })
      .parse(await api("wallets", { name }));
    await wallets();
    select("wallet-select").value = result.address.toLowerCase();
    clearPolicy();
    input("agent-id").value = name;
    notice("Agent wallet created. Review its Guard policy next.");
  },
  "submit",
);
action("load-policy", async () => {
  const target = wallet();
  const revision = ++policyRevision;
  const response = z
    .object({ policy: z.string(), mode: z.object({ mode: z.string() }) })
    .passthrough()
    .parse(await api(`policy?wallet=${target}`));
  if (revision !== policyRevision || wallet() !== target) return;
  policyWallet = target;
  textarea("policy").value = response.policy;
  element("guard-state").textContent = `Provider mode: ${response.mode.mode}`;
  element("policy-area").hidden = false;
});
async function policyApprovals() {
  const target = wallet();
  const revision = policyRevision;
  const rows = z
    .array(policyApprovalSchema)
    .parse(await api(`approvals?wallet=${target}`));
  if (revision !== policyRevision || wallet() !== target) return;
  const list = element("policy-approvals");
  list.replaceChildren();
  for (const row of rows) {
    const item = document.createElement("p");
    item.textContent = `Policy ${row.state}${row.expiresAt ? ` · deadline ${new Date(row.expiresAt).toLocaleString()}` : ""}`;
    if (row.state === "unknown")
      item.textContent +=
        " · Outcome unknown; inspect MetaMask before another request.";
    if (row.state === "approved")
      item.textContent +=
        " · Approval recorded. Review current policy before renewing bot access.";
    list.append(item);
  }
  if (!rows.length)
    list.textContent = "No recorded policy requests for this wallet.";
}
action("refresh-policy-approvals", policyApprovals);
action("save-policy", async () => {
  const target = wallet();
  if (policyWallet !== target)
    throw new Error("Review this wallet's policy before submitting changes.");
  const result = policyApprovalSchema.parse(
    await api("policy", {
      id: crypto.randomUUID(),
      wallet: target,
      policy: textarea("policy").value,
    }),
  );
  if (wallet() !== target) return;
  notice(
    result.state === "pending"
      ? "Policy approval pending in MetaMask. Request saved across restarts."
      : `Policy ${result.state}. Review current policy before renewing bot access.`,
  );
  await policyApprovals();
});
action("guard-mode", async () => {
  notice(
    `Guard request: ${JSON.stringify(await api("guard", { wallet: wallet() }))}. Refresh policy to verify.`,
  );
});
action(
  "binding-form",
  async () => {
    const result = z
      .object({
        agent: z.string(),
        token: z.string(),
        endpoint: z.string(),
        wallet: z.string(),
        network: z.string(),
      })
      .parse(
        await api("bindings", {
          id: input("agent-id").value,
          wallet: wallet(),
          network: "testnet",
          expiresAt: Date.now() + Number(input("expiry").value) * 3600000,
          chains: input("chains")
            .value.split(",")
            .map((v) => Number(v.trim())),
          assets: z
            .array(assetSchema)
            .parse(JSON.parse(textarea("assets").value)),
          markets: input("markets")
            .value.split(",")
            .map((v) => v.trim())
            .filter(Boolean),
          maxOrderUsd: input("order-limit").value,
          maxDailyUsd: input("daily-limit").value,
          maxSlippageBps: Number(input("slippage").value),
          maxLeverage: Number(input("leverage").value),
        }),
      );
    element("credential-output").textContent = JSON.stringify(
      {
        BROKER_URL: result.endpoint,
        BROKER_TOKEN: result.token,
        WALLET_ADDRESS: result.wallet,
        NETWORK: result.network,
        MODE: input("markets").value.trim() ? "perp" : "spot",
      },
      null,
      2,
    );
    element("credentials").hidden = false;
    await refresh();
    notice("Scoped token created. Save it before hiding or leaving this page.");
  },
  "submit",
);
action("hide-token", async () => {
  element("credential-output").textContent = "";
  element("credentials").hidden = true;
});
action("connect", async () => {
  connected ??= await createEVMClient({
    dapp: { name: "LumiBot MetaMask setup", url: window.location.origin },
    api: {
      supportedNetworks: {
        "0xaa36a7": "https://ethereum-sepolia-rpc.publicnode.com",
        "0x66eee": "https://sepolia-rollup.arbitrum.io/rpc",
      },
    },
    analytics: { enabled: false },
  });
  const connection = await connected.connect({
    chainIds: ["0xaa36a7", "0x66eee"],
  });
  const account = connection.accounts[0];
  if (!account) throw new Error("MetaMask returned no account");
  browserAccount = getAddress(account);
  element("browser-wallet").textContent = browserAccount;
  element("fund-form").hidden = false;
  element("disconnect").hidden = false;
  connected.getProvider().on("accountsChanged", () => {
    browserAccount = undefined;
    element("fund-form").hidden = true;
    element("browser-wallet").textContent =
      "Account changed. Reconnect before funding.";
  });
  notice("Browser wallet connected. Agent signing keys remain separate.");
});
action("disconnect", async () => {
  await connected?.disconnect();
  connected = undefined;
  browserAccount = undefined;
  element("fund-form").hidden = true;
  element("disconnect").hidden = true;
  element("browser-wallet").textContent = "No browser wallet connected.";
});
async function gasFunding(agent: string, automatic = false): Promise<void> {
  clearTimeout(nativeFundingPoll);
  if (automatic && element("native-funding").contains(document.activeElement)) {
    nativeFundingPoll = setTimeout(() => {
      void gasFunding(agent, true).catch((error: unknown) =>
        notice(String(error), true),
      );
    }, 5000);
    return;
  }
  const records = z
    .array(nativeFundingSchema)
    .parse(await api(`native-funding?agent=${encodeURIComponent(agent)}`));
  if (boundAgents.get(select("wallet-select").value.toLowerCase()) !== agent)
    return;
  const list = element("native-funding");
  list.replaceChildren();
  if (!records.length)
    list.textContent = "No native gas funding recorded for this agent.";
  for (const record of records) {
    const row = document.createElement("div");
    row.className = "binding";
    const title = document.createElement("strong");
    title.textContent = `${formatEther(BigInt(record.amountWei))} ETH · ${record.chainId === 11155111 ? "Sepolia" : "Arbitrum Sepolia"} · ${record.state}`;
    const detail = document.createElement("p");
    detail.textContent =
      record.message ??
      "Check MetaMask Activity. This request will not be sent again automatically.";
    row.append(title, detail);
    if (record.hash || record.replacementHash) {
      const link = document.createElement("a");
      link.href = `https://${record.chainId === 11155111 ? "sepolia.etherscan.io" : "sepolia.arbiscan.io"}/tx/${record.replacementHash ?? record.hash}`;
      link.target = "_blank";
      link.rel = "noreferrer";
      link.textContent = `View ${record.replacementHash ? "replacement" : "transaction"} · ${record.confirmations} confirmations`;
      row.append(link);
    }
    if (["prepared", "submitted", "unknown"].includes(record.state)) {
      const form = document.createElement("form"),
        label = document.createElement("label"),
        hash = document.createElement("input"),
        button = document.createElement("button");
      label.textContent = "Funding transaction hash from MetaMask Activity";
      hash.required = true;
      hash.pattern = "0x[0-9a-fA-F]{64}";
      hash.value = record.hash ?? "";
      label.append(hash);
      button.textContent = "Check funding receipt";
      form.append(label, button);
      form.onsubmit = async (event) => {
        event.preventDefault();
        button.disabled = true;
        try {
          await api("native-funding/hash", {
            agent,
            id: record.id,
            hash: hash.value.trim(),
          });
          await gasFunding(agent);
        } catch (error) {
          notice(String(error), true);
        } finally {
          button.disabled = false;
        }
      };
      row.append(form);
      if (!record.hash && record.nonce !== undefined) {
        const review = document.createElement("button");
        review.textContent = "Review pending funding";
        review.disabled = nativeSigning.has(record.id);
        review.onclick = async () => {
          review.disabled = true;
          try {
            await requestFunding(record);
          } catch (error) {
            notice(String(error), true);
          } finally {
            review.disabled = false;
          }
        };
        const hint = document.createElement("p");
        hint.textContent =
          "Reuses the original nonce and transfer details. MetaMask may replace a pending request; funds cannot be sent twice with this nonce.";
        row.append(hint, review);
      }
    }
    list.append(row);
  }
  if (
    records.some(
      (record) =>
        record.hash && ["submitted", "unknown"].includes(record.state),
    )
  ) {
    nativeFundingPoll = setTimeout(() => {
      void gasFunding(agent, true).catch((error: unknown) =>
        notice(String(error), true),
      );
    }, 5000);
  }
}
select("wallet-select").addEventListener("change", () => {
  clearTimeout(nativeFundingPoll);
  element("native-funding").replaceChildren();
  const agent = boundAgents.get(select("wallet-select").value.toLowerCase());
  if (agent)
    void gasFunding(agent).catch((error: unknown) =>
      notice(String(error), true),
    );
});
window.addEventListener("pagehide", () => clearTimeout(nativeFundingPoll));
action("refresh-native-funding", async () => {
  const agent = boundAgents.get(wallet().toLowerCase());
  if (!agent) throw new Error("Create an agent binding first");
  await gasFunding(agent);
});
async function requestFunding(
  record: NativeFunding,
  firstAttempt = false,
): Promise<void> {
  if (!connected || browserAccount?.toLowerCase() !== record.sender)
    throw new Error(
      "Connect the original sending wallet before reviewing funding.",
    );
  if (nativeSigning.has(record.id))
    throw new Error("Funding confirmation is already open.");
  nativeSigning.add(record.id);
  try {
    const provider = connected.getProvider();
    const transaction = await nativeTransfer(provider, record);
    let hash: unknown;
    try {
      hash = await provider.request({
        method: "eth_sendTransaction",
        params: [transaction],
      });
    } catch (error) {
      if (
        firstAttempt &&
        typeof error === "object" &&
        error !== null &&
        "code" in error &&
        error.code === 4001
      )
        await api("native-funding/rejected", {
          agent: record.agent,
          id: record.id,
        });
      throw error;
    }
    const fundingHash = z
      .string()
      .regex(/^0x[0-9a-fA-F]{64}$/)
      .parse(hash);
    notice(
      `Funding submitted: ${fundingHash}. Checking mined receipt; do not send again.`,
    );
    try {
      await api("native-funding/hash", {
        agent: record.agent,
        id: record.id,
        hash: fundingHash,
      });
    } catch (error) {
      notice(
        `Keep funding hash ${fundingHash}. Receipt tracking unavailable: ${String(error)}`,
        true,
      );
    }
  } finally {
    nativeSigning.delete(record.id);
    try {
      await gasFunding(record.agent);
    } catch {
      element("native-funding").textContent =
        "Funding history unavailable. Refresh when the service returns; do not create another transfer.";
    }
  }
}
action(
  "fund-form",
  async () => {
    if (!connected || !browserAccount)
      throw new Error("Connect browser wallet first");
    const chain = nativeFundingSchema.shape.chainId.parse(
      Number(select("fund-chain").value),
    );
    const recipient = wallet();
    if (recipient.toLowerCase() === browserAccount.toLowerCase())
      throw new Error("Choose a separate agent wallet");
    const value = parseEther(input("fund-amount").value);
    if (value <= 0n) throw new Error("Funding amount must be positive");
    const provider = connected.getProvider();
    const sender = browserAccount;
    const agent = boundAgents.get(recipient.toLowerCase());
    if (!agent) throw new Error("Create an agent binding first");
    const nonce = await fundingNonce(provider, chain, sender);
    const prepared = nativeFundingSchema.parse(
      await api("native-funding", {
        id: crypto.randomUUID(),
        agent,
        sender,
        chainId: chain,
        amountWei: value.toString(),
        nonce,
      }),
    );
    if (prepared.state !== "prepared" || prepared.hash)
      throw new Error("Funding already started. Refresh its receipt.");
    await requestFunding(prepared, true);
  },
  "submit",
);
action("permissions", async () => {
  if (!connected) throw new Error("Connect browser wallet first");
  const client = createWalletClient({
    transport: custom(connected.getProvider()),
  }).extend(erc7715ProviderActions());
  const supported = await client.getSupportedExecutionPermissions();
  const granted = await client.getGrantedExecutionPermissions();
  element("permission-output").textContent = JSON.stringify(
    { supported, granted },
    (_, v: unknown) => (typeof v === "bigint" ? v.toString() : v),
    2,
  );
  element("permission-output").hidden = false;
});
action("refresh-operations", async () => {
  const agent = select("operation-agent").value;
  if (!agent) throw new Error("Create an agent binding first");
  const rows = z
    .array(operationSchema)
    .parse(await api(`operations?agent=${encodeURIComponent(agent)}`));
  const list = element("operations");
  list.replaceChildren();
  if (!rows.length) list.textContent = "No orders for this agent.";
  for (const op of rows) {
    const div = document.createElement("div");
    div.className = "binding";
    const title = document.createElement("strong");
    title.textContent = `${op.id} · ${op.state}`;
    const detail = document.createElement("p");
    detail.textContent =
      (op.message ?? `Filled: ${op.filledQuantity}`) +
      (op.state === "awaiting_approval"
        ? ` Approval: ${approvalChannel}.`
        : "") +
      (op.approvalId ? ` Signing job: ${op.approvalId}` : "");
    div.append(title, detail);
    if (
      op.request.kind === "perp" &&
      op.state === "unknown" &&
      !op.providerOrderId &&
      !op.signatureOnly
    ) {
      const form = document.createElement("form");
      const label = document.createElement("label");
      label.textContent = "Hyperliquid order ID";
      const input = document.createElement("input");
      input.required = true;
      input.inputMode = "numeric";
      input.pattern = "[1-9][0-9]*";
      label.append(input);
      const help = document.createElement("p");
      help.textContent =
        "Find this order in the same wallet's venue history. Linking checks its details and refreshes status without placing another order.";
      const button = document.createElement("button");
      button.textContent = "Link venue order";
      form.append(label, help, button);
      form.onsubmit = async (event) => {
        event.preventDefault();
        button.disabled = true;
        try {
          const result = operationSchema.parse(
            await api("recover-perp", {
              agent,
              id: op.id,
              providerOrderId: input.value.trim(),
            }),
          );
          title.textContent = `${op.id} · ${result.state}`;
          detail.textContent =
            result.message ?? `Filled: ${result.filledQuantity}`;
          form.remove();
          notice(
            "Venue order linked. Refresh orders for current status and available actions.",
          );
        } catch (error) {
          notice(String(error), true);
        } finally {
          button.disabled = false;
        }
      };
      div.append(form);
    }
    if (
      op.request.kind === "perp" &&
      op.providerOrderId &&
      !["filled", "canceled", "rejected"].includes(op.state)
    ) {
      const cancel = document.createElement("button");
      cancel.className = "secondary";
      cancel.textContent = "Cancel order";
      cancel.onclick = async () => {
        cancel.disabled = true;
        try {
          await api("cancel", { agent, id: op.id });
          notice("Cancellation sent. Refresh to confirm venue status.");
        } catch (e) {
          notice(String(e), true);
        } finally {
          cancel.disabled = false;
        }
      };
      div.append(cancel);
    }
    if (op.transactionStage === "allowance_confirmed") {
      const button = document.createElement("button");
      button.textContent = "Resume swap with fresh quote";
      button.onclick = async () => {
        button.disabled = true;
        try {
          await api("resume", { agent, id: op.id });
          notice("Swap resumed. Refresh orders for status.");
        } catch (e) {
          notice(String(e), true);
        } finally {
          button.disabled = false;
        }
      };
      div.append(button);
    }
    list.append(div);
  }
});

let collateralRevision = 0;
let collateralRequest:
  | { id: string; agent: string; amount: string }
  | undefined;
function renderCollateralTransfer(
  op: z.infer<typeof collateralTransferSchema>,
): HTMLElement {
  const row = document.createElement("p");
  const detail = op.state === "submitted" ? "" : op.message;
  row.textContent = `${op.amount} USDC · ${op.state}${detail ? ` · ${detail}` : ""}${op.approvalId ? ` · Signing job: ${op.approvalId}` : ""}`;
  return row;
}
select("collateral-agent").addEventListener("change", () => {
  collateralRevision++;
  element("collateral-balance").textContent =
    "Refresh balances for this agent.";
  element("collateral-transfers").replaceChildren();
  element("collateral-amount-field").hidden = false;
  element("collateral-submit").hidden = false;
});
async function refreshCollateral() {
  const agent = select("collateral-agent").value;
  if (!agent) throw new Error("Create a testnet agent binding first");
  const revision = ++collateralRevision;
  element("collateral-balance").textContent = "Loading collateral balances…";
  const schema = collateralBalanceSchema.extend({
    transfers: z.array(collateralTransferSchema),
  });
  let state: z.infer<typeof schema>;
  try {
    state = schema.parse(
      await api(`collateral?agent=${encodeURIComponent(agent)}`),
    );
  } catch (error) {
    if (
      revision === collateralRevision &&
      agent === select("collateral-agent").value
    )
      element("collateral-balance").textContent =
        "Balances unavailable. Refresh to retry; no values assumed.";
    throw error;
  }
  if (
    revision !== collateralRevision ||
    agent !== select("collateral-agent").value
  )
    return;
  element("collateral-balance").textContent =
    `Spot: ${state.spotUsdc} USDC · Perps equity: ${state.perpsEquity} USDC${state.unifiedAccount ? " · Unified account; no collateral move needed." : ""}`;
  element("collateral-amount-field").hidden = !state.transferAvailable;
  element("collateral-submit").hidden = !state.transferAvailable;
  element("collateral-transfers").replaceChildren(
    ...state.transfers.map(renderCollateralTransfer),
  );
}
action("refresh-collateral", async () => {
  await refreshCollateral();
  notice("Collateral balances refreshed.");
});
action(
  "collateral-form",
  async () => {
    const agent = select("collateral-agent").value;
    if (!agent) throw new Error("Create a testnet agent binding first");
    const amount = input("collateral-amount").value.trim();
    if (
      !collateralRequest ||
      collateralRequest.agent !== agent ||
      collateralRequest.amount !== amount
    )
      collateralRequest = { id: crypto.randomUUID(), agent, amount };
    notice(
      `Requesting testnet collateral move for ${agent}. Approval may be required via ${approvalChannel}.`,
    );
    const result = collateralTransferSchema.parse(
      await api("collateral", collateralRequest),
    );
    if (["submitted", "rejected"].includes(result.state))
      collateralRequest = undefined;
    if (select("collateral-agent").value !== agent) return;
    element("collateral-transfers").prepend(renderCollateralTransfer(result));
    if (result.state === "submitted") {
      input("collateral-amount").value = "";
      try {
        await refreshCollateral();
        notice("Funding submitted. Spot and perps balances refreshed.");
      } catch {
        notice(
          "Funding submitted, but balance refresh failed. Refresh balances before another transfer.",
          true,
        );
      }
      return;
    }
    notice(
      `${result.message ?? `Collateral ${result.state}.`}${result.state === "awaiting_approval" ? ` Check ${approvalChannel}.` : ""}`,
      ["unknown", "rejected"].includes(result.state),
    );
  },
  "submit",
);
