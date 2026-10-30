import type { PerpsAccountBalance } from "@metamask/agent-sdk";
import { z } from "zod";
import { amount, signedAmount, assert, type Account } from "./contracts.js";
import { Decimal } from "./numeric.js";
type DecimalValue = InstanceType<typeof Decimal>;

export function hyperCoreUsdc(raw: unknown): {
  total: DecimalValue;
  hold: DecimalValue;
} {
  const state = z
    .object({
      balances: z.array(
        z.object({
          coin: z.string(),
          token: z.number().int().nonnegative().optional(),
          total: signedAmount,
          hold: amount,
        }),
      ),
    })
    .parse(raw);
  const rows = state.balances.filter((row) => row.coin === "USDC");
  assert(
    rows.length <= 1 && rows.every((row) => row.token === 0),
    "ACCOUNT_BALANCE_MISMATCH",
    "HyperCore USDC identity is ambiguous",
  );
  return {
    total: new Decimal(rows[0]?.total ?? "0"),
    hold: new Decimal(rows[0]?.hold ?? "0"),
  };
}

/** Normalize the venue's actual collateral ledger, without adding unified PnL twice. */
export function perpFunds(
  balance: Pick<
    PerpsAccountBalance,
    "totalBalance" | "spendableBalance" | "marginUsed"
  >,
  rawMode: string,
  rawSpot?: unknown,
): Pick<Account, "cash" | "equity" | "positionsValue"> {
  assert(
    ["default", "disabled", "unifiedAccount"].includes(rawMode),
    "ACCOUNT_MODE_UNSUPPORTED",
    "This broker values standard and USDC unified accounts. Portfolio margin requires separate multi-asset valuation.",
  );
  let equity: DecimalValue, cash: DecimalValue;
  if (rawMode === "unifiedAccount") {
    const usdc = hyperCoreUsdc(rawSpot);
    equity = usdc.total;
    cash = Decimal.max(0, equity.minus(usdc.hold));
  } else {
    equity = new Decimal(signedAmount.parse(balance.totalBalance));
    cash = Decimal.max(
      0,
      Decimal.min(
        signedAmount.parse(balance.spendableBalance),
        equity.minus(amount.parse(balance.marginUsed)),
      ),
    );
  }
  return {
    cash: cash.toFixed(),
    equity: equity.toFixed(),
    positionsValue: equity.minus(cash).toFixed(),
  };
}
