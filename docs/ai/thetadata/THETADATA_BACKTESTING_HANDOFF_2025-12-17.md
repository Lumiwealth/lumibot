# ThetaData Backtesting + `get_last_price()` Purity — Handoff + Recovery Plan (2025-12-17)

This document is a **handoff** for a fresh session to fix ThetaData-backed backtests in LumiBot **without changing any strategy files**.

It also documents a critical API-contract violation that was introduced: **`get_last_price()` returning a quote-derived “mark” (NBBO mid)** for options in backtests. That violates the live-broker reality for the brokers we use (stale last trades are common for options), and it can cause strategies to backtest well and then fail live. This doc lays out how to fix it correctly (Option A: keep `get_last_price()` trade-based; use quotes/mark via dedicated APIs).

---

## 0) Non‑Negotiables / Guardrails

### 0.1 Contract: **Option A is mandatory**
- **`get_last_price()` must remain “last trade / last traded price only”.**
  - It must **never** return `bid`, `ask`, or `mid` (quote-derived prices).
  - If last trade is unavailable (common for illiquid options), it should return `None` (or a well-defined last-trade fallback policy that still remains trade-based).
- **Quotes/mark pricing must be explicit**, via:
  - `get_quote()` (already exists and returns `Quote` with `bid`, `ask`, `mid_price`), and/or
  - a new explicit helper like `get_mark_price()` / `get_mid_price()` / `get_price(price_type="mark")` (recommended).

### 0.2 Scope
- **Do not modify strategy files.** Fix LumiBot and the prompt system so strategies work as-is.
- **Never run `git checkout`** (hard ban).
- **Long-running commands must be wrapped with** `/Users/robertgrzesik/bin/safe-timeout …`.
- For ThetaData:
  - **Do not start ThetaTerminal locally with production credentials** (see `AGENTS.md`).
  - Backtests should use the **remote data-downloader** endpoint.

### 0.3 Why this is safety-critical
If backtests implicitly use NBBO-mid for options while live uses stale last trades, you can get:
- false fills/sizing in backtests,
- incorrect P&L curves,
- strategies that “work” historically but fail in production,
- and real-money risk.

---

## 1) Workspace / Key Paths

### 1.1 LumiBot repo (the library being fixed)
- Repo root:
  - `/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot`
- Source:
  - `/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot/lumibot`
- Tests:
  - `/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot/tests`
- Docs:
  - `/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot/docs`
  - `/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot/docsrc`

### 1.2 Strategy Library repo (strategies must remain unchanged)
- Demos folder:
  - `/Users/robertgrzesik/Documents/Development/Strategy Library/Demos`
- Logs (Strategy Library runs):
  - `/Users/robertgrzesik/Documents/Development/Strategy Library/logs`

### 1.3 Prompt system (used to generate strategies)
- Prompts:
  - `/Users/robertgrzesik/Documents/Development/ai_bot_builder/src/prompts`
  - In particular:
    - `/Users/robertgrzesik/Documents/Development/ai_bot_builder/src/prompts/shared/shared_methods_properties.py`
    - `/Users/robertgrzesik/Documents/Development/ai_bot_builder/src/prompts/shared/shared_notes_reminders.py`

---

## 2) `.env` + Environment Variable Loading (CRITICAL)

### 2.1 How LumiBot loads `.env`
File: `/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot/lumibot/credentials.py`

Behavior:
1. Determine `script_dir = dirname(sys.argv[0])`.
2. Walk (`os.walk`) `script_dir` recursively; load the **first** `.env` found.
3. If none, walk the **current working directory** recursively; load first `.env` found.

Implication:
- Running a backtest from different folders can load a different `.env` **without you realizing**.
- If you run from LumiBot repo root, it tends to load:
  - `/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot/.env`
- If you run a strategy from Strategy Library Demos, it tends to load:
  - `/Users/robertgrzesik/Documents/Development/Strategy Library/Demos/.env`

### 2.2 Key env vars used in backtesting (redact secrets)
- `IS_BACKTESTING=true|false`
- `BACKTESTING_START=YYYY-MM-DD`
- `BACKTESTING_END=YYYY-MM-DD`
- `BACKTESTING_DATA_SOURCE=thetadata|ThetaData|Polygon|Yahoo|...` (various casing exists in files)
- `DATADOWNLOADER_BASE_URL=http://data-downloader.lumiwealth.com:8080` (or IP)
- `DATADOWNLOADER_API_KEY=<redacted>`
- `DATADOWNLOADER_API_KEY_HEADER=X-Downloader-Key` (default)
- Optional quality-of-life:
  - `BACKTESTING_SHOW_PROGRESS_BAR=false`
  - `BACKTESTING_QUIET_LOGS=true`
  - `SHOW_PLOT=False`
  - `SHOW_INDICATORS=False`
  - `SHOW_TEARSHEET=False`

### 2.3 Two `.env` files involved in this incident
1. Strategy Library Demos:
   - `/Users/robertgrzesik/Documents/Development/Strategy Library/Demos/.env`
   - Example (sanitized): had `BACKTESTING_START=2025-10-01`, `BACKTESTING_END=2025-12-01`, `DATADOWNLOADER_BASE_URL=http://data-downloader.lumiwealth.com:8080`
2. LumiBot repo:
   - `/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot/.env`
   - Contains downloader settings and Theta dev creds (do not paste here).

---

## 3) Data Downloader (ThetaData Remote)

### 3.1 Correct domain spelling
- Correct: `lumiwealth.com`
- Observed confusion: `lumywealth.com` (wrong)

### 3.2 Verified working endpoint
- `DATADOWNLOADER_BASE_URL=http://data-downloader.lumiwealth.com:8080`
  - DNS: `data-downloader.lumiwealth.com` → `34.232.207.152`
  - `/version` returns JSON like:
    - `version` (git SHA)
    - `request_timeout` (e.g. 30s)
    - `theta_*_base_url` (usually loopback inside downloader container)
    - `concurrency` settings

### 3.3 Deprecated endpoint notes
- Avoid hard-coded downloader IPs (they can change on redeploy). Use `http://data-downloader.lumiwealth.com:8080`.

### 3.4 How LumiBot decides “remote downloader”
File: `/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot/lumibot/tools/thetadata_helper.py`
- If `DATADOWNLOADER_BASE_URL` is set and is **not loopback**, it enables remote mode (and should not start local ThetaTerminal).

### 3.5 AWS / Route53 note
- Route53 likely controls `data-downloader.lumiwealth.com`.
- If needed, use AWS CLI read-only commands in a future session (do not mutate DNS):
  - `aws route53 list-hosted-zones`
  - `aws route53 list-resource-record-sets --hosted-zone-id ZONEID`
  - Confirm A/ALIAS records for the downloader hostname.

---

## 4) Caching (ThetaData)

### 4.1 Cache root
File: `/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot/lumibot/constants.py`
- `LUMIBOT_CACHE_FOLDER = appdirs.user_cache_dir(appauthor="LumiWealth", appname="lumibot", version="1.0")`
  - On macOS this is typically under:
    - `~/Library/Caches/LumiWealth/lumibot/1.0`

### 4.2 ThetaData cache subtree
File: `/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot/lumibot/tools/thetadata_helper.py`
- `CACHE_SUBFOLDER = "thetadata"`
- The helper caches OHLC/quote data and also option chains:
  - `.../thetadata/<asset-type>/...`
  - Option chains:
    - `LUMIBOT_CACHE_FOLDER/thetadata/<asset-type>/option_chains/{symbol}_{date}.parquet`

### 4.3 Placeholder rows / missing coverage
- The Theta pipeline can add “placeholder” rows when downloader returns missing ranges.
- Many downstream operations assume a unique datetime index and consistent schema.
- There is an internal `missing` flag column in some paths.

---

## 5) Strategies Under Test (No Strategy Modifications Allowed)

### 5.1 Deep Dip Calls
- File:
  - `/Users/robertgrzesik/Documents/Development/Strategy Library/Demos/AAPL Deep Dip Calls (Copy 4).py`
- NOTE: Despite the filename, the strategy is configured for **GOOG** (`parameters["symbol"] = "GOOG"`). Strategy name remains `AAPLDeepDipCalls`.
- Strategy uses:
  - `self.get_last_price(underlying)` for underlying price checks (fine).
  - `self.get_quote(option)` for mid pricing with a fallback to `self.get_last_price(option)` if quote missing (`_get_option_mid`).

Reported issues (user):
- Missing trade in 2020 (expected buys in 2020/2022/2025 for 25% dips).
- Equity curve cliffs, including a suspicious crash around corporate action dates.

Confirmed contributing factors:
- Theta option EOD rows can have `close=0` (no trades) while NBBO bid/ask exist.
- If day-mode option frames drop NBBO columns, `get_quote()` returns empty and strategy falls back to `get_last_price()` which returns `None` → buy skipped.

### 5.2 Leaps Buy & Hold (Alpha Picks)
- File:
  - `/Users/robertgrzesik/Documents/Development/Strategy Library/Demos/Leaps Buy Hold (Alpha Picks).py`
- Symbols: `["UBER", "STRL", "CLS", "MFC", "APP"]`
- Reported issue: only buying UBER (or failing to price the others).

Likely contributors:
- Thinly traded options have no prints (close=0) but may have quotes.
- If the data path or cache drops quote columns, `get_quote()` is empty and downstream logic treats the option as unpriceable.

### 5.3 TQQQ Strategies
Multiple exist; at minimum these files reference TQQQ and may need validation:
- `/Users/robertgrzesik/Documents/Development/Strategy Library/Demos/TQQQ.py`
- `/Users/robertgrzesik/Documents/Development/Strategy Library/Demos/TQQQ 200-Day MA.py`
- `/Users/robertgrzesik/Documents/Development/Strategy Library/Demos/TQQQ Day Trader (failing).py`
- `/Users/robertgrzesik/Documents/Development/Strategy Library/Demos/tqqq_trader.py`

---

## 6) Core Root Cause: Semantic Mismatch

### 6.1 What we need (Option A)
- `get_last_price()` = **last traded price** (or last close derived from trades only).
- `get_quote()` / `get_mark_price()` = **bid/ask/mid** used for:
  - options valuation,
  - spread checks,
  - sizing on illiquid contracts,
  - live trading parity.

### 6.2 What exists today (even before the recent mistake)
Live data sources are already inconsistent:
- Tradier: `/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot/lumibot/data_sources/tradier_data.py`
  - `get_last_price()` calls `tradier.market.get_last_price(...)` (likely last trade).
- Alpaca: `/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot/lumibot/data_sources/alpaca_data.py:525`
  - `get_last_price()` explicitly calls `get_quote()` and returns quote `.price` (and even bid/ask fallbacks).
- Schwab: `/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot/lumibot/data_sources/schwab_data.py:498`
  - `get_last_price()` calls `get_quote()` and returns quote `.price`.
- CCXT (crypto): `/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot/lumibot/data_sources/ccxt_data.py:216`
  - `get_last_price()` returns `fetch_ticker(symbol)["last"]` (usually last trade).

So the “contract” is already unclear. The fix must **standardize behavior** across sources and docs.

---

## 7) The Specific Mistake Introduced (Must Be Removed)

### 7.1 The problematic behavior
`get_last_price()` for options in backtests was modified to:
- return NBBO-mid (or bid/ask) when `close <= 0`.

This is a hidden “mark” fallback and violates Option A.

### 7.2 Why it happened
It was a band-aid to “fix” Theta EOD option bars where:
- `close=0` (no prints), but
- `bid/ask` exist (valid market).

But the correct fix is:
- **preserve / provide quotes** for option instruments, and
- make strategies/components explicitly use quote-based mark pricing, not `get_last_price`.

---

## 8) Current Uncommitted Working Tree State (What Was Touched)

Repo: `/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot`

### 8.1 `git status -sb` (captured)
```
## theta-bug-fixes...origin/theta-bug-fixes
 M lumibot/backtesting/backtesting_broker.py
 M lumibot/backtesting/thetadata_backtesting_pandas.py
 M lumibot/brokers/broker.py
 M lumibot/brokers/tradier.py
 M lumibot/components/options_helper.py
 M lumibot/data_sources/pandas_data.py
 M lumibot/entities/chains.py
 M lumibot/entities/data.py
 M lumibot/strategies/_strategy.py
 M lumibot/tools/thetadata_helper.py
 M lumibot/tools/thetadata_queue_client.py
 M setup.py
 M tests/test_options_helper.py
 M tests/test_pandas_data.py
 M tests/test_thetadata_helper.py
 M tests/test_thetadata_queue_client.py
 M tests/test_tradier.py
?? THETADATA_INVESTIGATION_2025-12-11.md
?? tests/test_option_strike_conversion.py
?? tests/test_split_adjustment.py
?? tests/test_thetadata_last_price_nbbo_fallback.py
?? tests/test_thetadata_yahoo_parity.py
```

### 8.2 `git diff --stat` (captured)
```
17 files changed, 1537 insertions(+), 420 deletions(-)
```

### 8.3 Key files with the dangerous `get_last_price()` behavior
- `/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot/lumibot/data_sources/pandas_data.py`
  - Added NBBO-mid fallback when price ≤ 0 for options (must be removed under Option A).

---

## 9) Correct Fix Plan (Option A) — Detailed Implementation Sequence

This is the plan the next session should execute. The first steps are **safety rails** (tests + docs) before refactors.

### Phase 0 — Safety rails first (prevent recurrence)
1. **Document the contract** in LumiBot docs + docstrings:
   - `Strategy.get_last_price`: last trade only
   - `Strategy.get_quote`: bid/ask/mid
   - Add/introduce `Strategy.get_mark_price` (explicit mark) and document it as the correct option valuation input.
2. **Add hard unit tests** that enforce:
   - `get_last_price()` never returns `bid/ask/mid` even when bid/ask exist.
   - `get_mark_price()` returns mid when bid/ask exist (and has a clear fallback policy).
3. Add CI/test naming and a regression checklist so future changes don’t silently violate contract.

### Phase 1 — Introduce explicit “mark” API and migrate internal users
1. Add `get_mark_price()` (Strategy → Broker → DataSource).
   - Default Strategy implementation should call `get_quote()` and return `.mid_price` (or other policy).
2. Update **internal valuation** paths to use mark where appropriate:
   - Portfolio valuation (`/lumibot/strategies/_strategy.py`) should use mark for options, not last trade.
   - Order cash reservation (`/lumibot/entities/order.py:987`) should use mark for options while pending (or a strict policy).
3. Update `OptionsHelper`:
   - Eliminate fallback to `get_last_price()` for option validation/pricing.
   - Replace with `get_mark_price()` or `get_quote()` mid.
4. Keep strategies unchanged; their existing `get_quote()` usage should now work reliably in backtests.

### Phase 2 — ThetaData day-mode: guarantee option quotes are present
1. Ensure day-mode option history uses Theta EOD endpoints **with NBBO columns preserved** (bid/ask).
2. Ensure caching preserves quote columns and schema consistently (no “missing quote columns” warnings).
3. Ensure `get_quote()` for options works in day-mode:
   - For days with no trades: quote exists; mid exists; last trade may be missing → correct.

### Phase 3 — Splits and corporate actions (avoid cliffs without lying about price type)
1. Keep stock prices split-adjusted (standard for backtests).
2. For options:
   - Ensure option **contract adjustments** are handled without turning marks into trades.
   - Decide whether to:
     - normalize option OHLC/quotes into post-split terms consistently, OR
     - keep raw contract prices but then adjust position quantities/cost basis in broker simulation.
3. Add regression tests around known split events (GOOG 20:1 in July 2022) to prevent equity cliffs.

### Phase 4 — Prompt system fix (prevents new “bad” strategies)
Update `/Users/robertgrzesik/Documents/Development/ai_bot_builder/src/prompts` so generated strategies:
- use `get_quote(option).mid_price` (or `get_mark_price`) for any option pricing,
- do **not** use `get_last_price(option)` for option valuation, spreads, or sizing.

Concrete prompt edits:
- `/prompts/shared/shared_methods_properties.py`: clarify contract and recommend mark APIs for options.
- `/prompts/shared/shared_notes_reminders.py`: add a “never use get_last_price for option pricing” rule.

### Phase 5 — Regression validation (must pass before release)
Run a validation matrix (all without strategy edits):
1. Deep Dip Calls:
   - 2020 window: confirm 2020 buy triggers on dip with quote-mark pricing.
   - 2022 window: confirm no artificial cliff around split.
   - Full run: confirm expected trades (user expects 2020/2022/2025 dips).
2. Alpha Picks:
   - short 1–2 month window: confirm it can price and trade more than UBER; if symbols are illiquid, it should skip with explicit quote-based reasoning.
3. TQQQ strategy suite:
   - validate there are no cash/portfolio valuation anomalies.

---

## 10) Documentation Updates Required (explicit)

### 10.1 LumiBot docs
Update docs to explicitly define:
- `get_last_price()` = **last traded price** (or last trade-derived bar close), not a mark.
- `get_quote()` = bid/ask (+mid) and is the only correct source for mark.
- Add `get_mark_price()` docs and strongly recommend for options.

Likely doc targets:
- `docs/` and/or `docsrc/` (exact file depends on current doc structure).
- Also update code docstrings:
  - `/lumibot/strategies/strategy.py`
  - `/lumibot/brokers/broker.py`

### 10.2 Prompt docs (ai_bot_builder)
Update:
- `/Users/robertgrzesik/Documents/Development/ai_bot_builder/src/prompts/shared/shared_methods_properties.py`
- `/Users/robertgrzesik/Documents/Development/ai_bot_builder/src/prompts/shared/shared_notes_reminders.py`

---

## 11) Backtest Runs Performed During Investigation (for reference)

Note: Some runs were executed by importing strategy files and calling `.backtest()` directly (so the strategy’s `__main__` datasource choice did not matter).

### 11.1 Generated artifacts in LumiBot repo logs
Directory:
- `/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot/logs`

Artifacts created:
- `AAPLDeepDipCalls_2025-12-17_00-41_oSX8KL_*` (2020-01-01 → 2020-06-01)
  - Trades show a buy on `2020-03-12` (GOOG calls).
- `AAPLDeepDipCalls_2025-12-17_00-43_rcZd75_*` (2022-01-01 → 2022-08-31)
  - Trades show buy/sell around July 2022 (needs split correctness validation).
- `LeapsCallDebitSpread_2025-12-17_00-58_vgzJNE_*` (2025-10-01 → 2025-10-15)
  - Trades show spreads opened for UBER, CLS, MFC (still needs STRL/APP validation).

### 11.2 Strategy Library artifacts inspected
Directory:
- `/Users/robertgrzesik/Documents/Development/Strategy Library/logs`
Examples referenced earlier by user:
- `AAPLDeepDipCalls_2025-12-16_15-38_Yxoim1_*`
- `LeapsCallDebitSpread_2025-12-16_22-57_FeFJZ0_*`

---

## 12) Immediate Next Session Checklist (Do This First)

1. **Stop making functional changes until contract tests exist.**
2. Add “Option A contract tests”:
   - `get_last_price` must never use bid/ask/mid fallback.
   - `get_mark_price` (new) must use bid/ask mid when available.
3. Update docs + prompt rules to prevent new strategies from using `get_last_price` for option pricing.
4. Refactor internal valuation/users (`_strategy.py`, `order.py`, `OptionsHelper`) to use mark, not last trade.
5. Only after that, re-validate:
   - DeepDip (2020/2022/full)
   - Alpha Picks
   - TQQQ suite

---

## Appendix A — Commands Used (pattern)

All long commands must use safe-timeout, e.g.:
```
/Users/robertgrzesik/bin/safe-timeout 600s env \
  DATADOWNLOADER_BASE_URL=http://data-downloader.lumiwealth.com:8080 \
  IS_BACKTESTING=True \
  BACKTESTING_START=2020-01-01 \
  BACKTESTING_END=2020-06-01 \
  BACKTESTING_SHOW_PROGRESS_BAR=false \
  BACKTESTING_QUIET_LOGS=true \
  SHOW_PLOT=False \
  SHOW_INDICATORS=False \
  SHOW_TEARSHEET=False \
  python3 - <<'PY'
  # importlib load strategy file then call Strategy.backtest(ThetaDataBacktesting, ...)
  PY
```

---

## Appendix B — Critical Reminder

Do **not** “fix” missing option prints by silently converting `get_last_price()` into a mark price. That is exactly how you get “it backtests but doesn’t trade live.”
