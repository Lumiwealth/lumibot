import { test } from "node:test";
import { strict as assert } from "node:assert";
import { hyperCoreUsdc, perpFunds } from "../src/perps-account.js";

const legacy = {
  totalBalance: "0",
  spendableBalance: "0",
  marginUsed: "0",
};
const spot = (total: string, hold: string) => ({
  balances: [{ coin: "USDC", token: 0, total, hold }],
});

test("unified collateral preserves precision and deducts holds from spendable cash", () => {
  assert.deepEqual(
    perpFunds(legacy, "unifiedAccount", spot("79.954442", "34.557072")),
    { equity: "79.954442", cash: "45.39737", positionsValue: "34.557072" },
  );
});

test("unified cash never becomes negative while a deficit remains visible in equity", () => {
  assert.deepEqual(perpFunds(legacy, "unifiedAccount", spot("2", "3")), {
    equity: "2",
    cash: "0",
    positionsValue: "2",
  });
  assert.deepEqual(perpFunds(legacy, "unifiedAccount", spot("-1.25", "0")), {
    equity: "-1.25",
    cash: "0",
    positionsValue: "-1.25",
  });
});

test("missing USDC means zero collateral without assigning other assets a dollar peg", () => {
  for (const balances of [
    [],
    [{ coin: "OTHER", token: 1, total: "100", hold: "0" }],
  ]) {
    assert.deepEqual(perpFunds(legacy, "unifiedAccount", { balances }), {
      equity: "0",
      cash: "0",
      positionsValue: "0",
    });
  }
});

test("USDC collateral requires its unique venue token identity", () => {
  const usdc = spot("10", "0").balances[0];
  for (const balances of [
    [{ ...usdc, token: 1 }],
    [{ coin: "USDC", total: "10", hold: "0" }],
    [usdc, usdc],
  ]) {
    assert.throws(() => hyperCoreUsdc({ balances }), {
      code: "ACCOUNT_BALANCE_MISMATCH",
    });
  }
});

test("unsupported account modes cannot report a USDC-only portfolio value", () => {
  for (const mode of ["portfolioMargin", "future-mode"]) {
    assert.throws(() => perpFunds(legacy, mode, spot("10", "0")), {
      code: "ACCOUNT_MODE_UNSUPPORTED",
    });
  }
});

test("standard account cash excludes unrelated spot buying power", () => {
  for (const mode of ["default", "disabled"]) {
    assert.deepEqual(
      perpFunds(
        { totalBalance: "100", spendableBalance: "300", marginUsed: "20" },
        mode,
      ),
      { equity: "100", cash: "80", positionsValue: "20" },
    );
  }
});
