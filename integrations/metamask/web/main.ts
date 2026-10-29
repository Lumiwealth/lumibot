import { createEVMClient } from "@metamask/connect-evm";
import { createWalletClient, custom, parseEther, getAddress } from "viem";
import { erc7715ProviderActions } from "@metamask/smart-accounts-kit/actions";
import { z } from "zod";
import {
  assetSchema,
  bindingSchema,
  operationSchema,
  policyApprovalSchema,
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
const statusSchema = z.object({
  authenticated: z.boolean(),
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
  const link = element("login-link");
  if (link instanceof HTMLAnchorElement) link.href = state.loginUrl;
  const list = element("bindings");
  list.replaceChildren();
  select("operation-agent").replaceChildren();
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
  }
  if (!state.bindings.length) list.textContent = "No agent bindings yet.";
  if (state.authenticated) await wallets();
}
async function wallets() {
  const rows = z
    .array(
      z.object({ address: z.string(), name: z.string(), kind: z.string() }),
    )
    .parse(await api("wallets"));
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
action(
  "fund-form",
  async () => {
    if (!connected || !browserAccount)
      throw new Error("Connect browser wallet first");
    const chain = Number(select("fund-chain").value);
    if (![11155111, 421614].includes(chain))
      throw new Error("Only testnets supported");
    const recipient = wallet();
    if (recipient.toLowerCase() === browserAccount.toLowerCase())
      throw new Error("Choose a separate agent wallet");
    const value = parseEther(input("fund-amount").value);
    if (value <= 0n) throw new Error("Funding amount must be positive");
    const provider = connected.getProvider();
    const chainId = `0x${chain.toString(16)}`;
    await provider.request({
      method: "wallet_switchEthereumChain",
      params: [{ chainId }],
    });
    const actual = await provider.request({ method: "eth_chainId" });
    if (actual !== chainId)
      throw new Error("Wallet network differs from selected testnet");
    const hash = await provider.request({
      method: "eth_sendTransaction",
      params: [
        {
          from: browserAccount,
          to: recipient,
          value: `0x${value.toString(16)}`,
          chainId,
        },
      ],
    });
    notice(
      `Funding submitted: ${String(hash)}. Receipt confirmation still pending.`,
    );
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
      (op.approvalId ? ` Signing job: ${op.approvalId}` : "");
    div.append(title, detail);
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
