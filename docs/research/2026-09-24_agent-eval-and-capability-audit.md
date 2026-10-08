# LumiBot agent audit: evals, cost, capability gaps, and what would make people love this library

Date: 2026-09-24. Branch `version/4.6.0`. Every number below was read out of the
repo or measured, not estimated from memory. Where something is a projection it
says so.

## 1. The eval suite is smaller and slower than we thought

**There are 16 eval cases, not 30.**

```
agent_eval_cases/  -> 16 json files
  options   5   credit_spread_close_signed_quantities, expiration_with_data,
                iron_condor_atomic_open, iron_condor_limit_between_bid_ask,
                single_leg_chain_and_quote
  stock     3   orb_completed_bars, pending_exit_no_duplicate, price_before_order
  research  3   macro_point_in_time, sec_prompt_injection, unavailable_safe_fallback
  crypto    1   instrument_identity
  congress  1   public_filings_only
  rules     1   active_override_strategy_prompt
  handoff   1   researcher_trader_evidence_handoff
```

**Why it takes so long, exactly.** From `scripts/run_agent_evals.py`:

```python
REQUIRED_CONSECUTIVE_PASSES = 3            # line 35
parser.add_argument("--max-workers", default=3)   # line 1057
if args.repeat < REQUIRED_CONSECUTIVE_PASSES:
    raise RuntimeError(f"--repeat must be at least {REQUIRED_CONSECUTIVE_PASSES}")
```

Three repeats is a **hard floor you cannot lower from the command line**. Every
case runs three times, and each repetition runs an acting model and then a judge
model. So 16 cases is really **48 acting runs plus 48 judge runs, at 3 workers**,
which is 32 sequential batches of agent conversations. That is the whole
explanation for the wall-clock time. It is not the number of tests.

**BotSpot Agent does it differently**, and Rob remembered right:

```
botspot_agent/scripts/run_fresh_strategy_runtime_live_eval_local.sh
repeat="${AGENT_LIVE_EVAL_REPEAT:-1}"
```

35 eval case folders, **one repetition by default**, configurable upward. So
BotSpot Agent has roughly twice the coverage at a third of the runs.

### Recommendation

Make repeat a risk decision instead of a constant:

- Default `--repeat 1` for the ordinary local loop, matching BotSpot Agent.
- Keep 3 only for the cases that have earned it: new, recently changed,
  historically flaky, and anything that can place an order. Mark that per case
  in the case JSON rather than globally.
- Raise `--max-workers` from 3. These are network-bound LLM calls, not CPU work.
  8 is reasonable on this machine and cuts the batch count by more than half.

Those two changes together take the common case from 96 model conversations to
about 20 and from 32 sequential batches to about 3.

## 2. Cost: Luna is already the default and it is cheap

The eval runner already uses Luna for both roles:

```python
DEFAULT_ACTING_MODEL = "openai/gpt-6-luna"
DEFAULT_JUDGE_MODEL  = "openai/gpt-6-luna"
```

Prices in the runner's own table, per million tokens:

| Model | Input | Cached input | Output |
|---|---:|---:|---:|
| openai/gpt-6-luna | $0.10 | $0.01 | $0.50 |
| gemini-3.5-flash-lite | $0.30 | $0.03 | $2.50 |
| gemini-3.1-flash-lite | $0.25 | $0.025 | $1.50 |

Luna is 3x cheaper on input and **5x cheaper on output** than the Gemini
flash-lite it replaced. Rob's read is correct.

The runner already has real cost control: `reserve_budget_batch`,
`initial_repetition_reservation_usd`, a `--max-cost-usd` preflight, and a
per-scope `model_call_budget` for acting and judging separately. So the machinery
to cap spend exists and is wired in.

**What is missing is the receipt.** There is no committed ledger of what a full
run actually cost. `agent_eval_baselines/` holds red baselines, not spend. Until
one run is recorded, any figure I give is arithmetic, not measurement. The next
full run should write total cost, wall time, and tokens to a committed file, the
same way the Playwright receipt works. That is a small change and it turns this
whole question into a number we can look up.

Projection only, to be replaced by that receipt: at Luna prices, a trading-agent
conversation with tool calls is plausibly a few cents, so 96 conversations lands
in the low single-digit dollars. Comfortably inside Rob's $10 to $30 ceiling even
today, and well under it after the repeat change.

## 3. The real problem: 78 tools, 16 evals

The repo grew a lot in the last day. `BuiltinTools.all()` now returns **78 tools**,
up from 51. Here is coverage against the eval suite.

| Tool family | Tools | Evals | Gap |
|---|---:|---:|---|
| browser (`browser_*`) | 11 | **0** | includes `browser_login`, which handles passwords |
| options | 8 | 5 | best covered area |
| orders | 7 | ~2 indirect | no modify, cancel, or wait-for-terminal eval |
| SEC filings / fundamentals | 10 | 2 | |
| memory and thesis | 9 | 0 | |
| indicators | 3 | **0** | |
| market data | 4 | 1 indirect | |
| email, Slack, notify | 4 | **0** | these send things to real people |
| `http_request` | 1 | **0** | brand new |
| `rss_fetch` | 1 | **0** | brand new |
| `duckdb_query` | 1 | 0 | |
| `house_public_disclosures` | 1 | 1 | newest tool, already has one |

**Twenty-seven tools shipped in a day and one of them got an eval.** The browser
family is the sharpest edge: eleven tools, zero evals, and one of them logs into
websites with credentials.

### Asset classes with zero coverage

Futures, forex, and prediction markets have no eval at all. Crypto has one, and
it only checks instrument identity, not a trade.

## 4. Proposed eval families, ordered by what can hurt a customer

Roughly 45 new cases, taking the suite from 16 to about 60. Every one of these
is a thing that can lose someone money or leak something.

**A. Order lifecycle (10 cases).** The highest-value block, because every asset
class flows through it.
- Market vs limit vs bracket: agent is told which and uses that one
- Limit walking: place, check, reprice toward the touch, stop at a stated cap
- `orders_wait_for_terminal` is actually used instead of assuming a fill
- A timeout is reconciled, not retried blindly, so no duplicate order
- Partial fill handled as partial, not as filled
- Cancel and modify hit the right order id
- A rejected order is reported as rejected, never as success
- Position and open-order reread after every mutation

**B. Indicators (8 cases).** Rob named these. One case per family, checking the
agent picks the right indicator and reads it the right way round: moving averages,
RSI, MACD, Bollinger, ATR, Fibonacci retracement, SuperTrend, Ichimoku. The
failure mode worth catching is confident misuse, for example reading a cloud
signal inverted.

**C. Browser (7 cases).** New surface, zero coverage.
- Session open, navigate, extract, close without leaking a session
- A page that needs JavaScript actually needs the browser, and a page that does
  not is fetched with `http_request` instead
- **Prompt injection from page content is treated as data, never instruction.**
  We already have `research_sec_prompt_injection`, so extend that pattern to the
  open web, where it matters far more
- `browser_login` never writes a credential into a log, a transcript, or memory
- Recovery after a dead session
- A blocked or challenged page is reported as blocked, not hallucinated around

**D. Options depth (6 cases).** Multi-leg with correct signed quantities per leg,
strike selection by delta, expiration selection with real data, spread priced
between bid and ask, assignment and early-exercise awareness, and a
close-the-position case that does not accidentally open a new one.

**E. Futures and forex (5 cases).** Contract and expiry selection, rollover,
multiplier and tick-size arithmetic, margin awareness, and one forex pair
convention case. Currently zero.

**F. Backtesting integrity (5 cases).** This is the one that protects the
product's core claim. Point-in-time discipline: the agent must not read a price,
a filing, a news article, or an indicator value dated after the simulated clock.
We have one macro case; this should be a family, because look-ahead is the
failure that makes every backtest a lie.

**G. Data and tools (5 cases).** `http_request` against a JSON API,
`rss_fetch`, `duckdb_query` on a real table, FRED series selection, and one case
where the tool is unavailable and the agent says so instead of inventing a number.

**H. Outbound (3 cases).** Email, Slack, and `notify_user`: the agent sends when
asked, does not send when not asked, and never puts a credential or a customer's
personal data in the body.

**I. Portfolio (3 cases).** Rebalance to target weights, respect a stated risk
limit, and refuse to exceed a position cap even when instructed to.

## 5. What the field says we are missing, and what we already have

I read the 2026 literature rather than guessing.

**The universal weak spot in agentic trading is the gate between decision and
execution.** A review of 20+ DeFi trading agents found the same architecture
everywhere and the same hole: "the most common failure mode was this: the
strategy looks responsible at the LLM step, and then nothing checks anything
between 'model said yes' and 'transaction broadcasted'." Their second-largest
gap was exits being treated as an afterthought.

The arXiv survey *Agentic Quantitative Trading* (2608.31041, Aug 2026) reaches
the same conclusion from the academic side: systems "remain concentrated on
signal discovery, while complete integration with portfolio construction,
execution, and risk control is still uncommon," and "strong model or forecasting
capability does not reliably translate into trading performance under live market
conditions."

**This is very good news for LumiBot.** Everyone is building the easy half. The
part we are unusual for having, a real broker execution path with a deterministic
risk gate and an inspectable record, is the part the field says is missing. We
should say that out loud, and we should prove it with evals rather than claims.

**The regulatory direction points the same way.** Singapore's IMDA framework
(January 2026) and the NIST AI Agent Standards Initiative (February 2026) both
converge on identity, traceability, and stoppability: an agent needs its own
credential, an audit trail of who authorised what, and a tested kill switch. A
framework that ships those as primitives has a real enterprise story.

## 6. Ranked by return: what to build for 4.6.1

Ordered by what most grows adoption and most reduces the chance of hurting a
customer.

**1. Order lifecycle evals (family A).** Every asset class flows through orders.
This is where a bug costs real money, and it is the cheapest block to write
because the tools already exist.

**2. A committed eval receipt, plus repeat 1 and more workers.** Turns "how much
does this cost and how long does it take" from an argument into a file. Unblocks
running evals often enough to matter, which is the actual reason coverage is thin.

**3. Browser evals, prompt injection first.** Eleven tools and a login surface
with no tests. The injection case is the one that matters: an agent that reads
the open web and then trades is exactly the attack the literature warns about.

**4. Backtest look-ahead evals (family F).** Backtesting accuracy is the product.
A look-ahead bug does not crash, it just quietly makes every result wrong.

**5. Indicators evals (family B).** Rob is right that this is what people
actually ask for, and it is the cheapest family to write.

**6. Exit and risk-gate primitives, not just evals.** The literature's two named
gaps. Attach-at-placement brackets, a background exit watcher that reports when
it dies, and a deterministic policy check the model cannot talk its way past.
This is a feature, and it is the one most aligned with what the field says is
missing.

**7. More built-in data tools, fewer API keys.** Rob's point about key fatigue is
the real adoption blocker: Gemini key plus Alpaca key plus Exa key plus FRED key
is a wall in front of the first run. Priorities: a search tool that works with a
key the user already has, more free public sources wired in out of the box (SEC
already done, Treasury, BLS, Congress already done), and a clear statement of
which tools need no key at all. Data vendor marketplaces such as FactSet are
worth exploring, but they need a real test account before we promise anything.

**8. Evals for the remaining families (C through I).** Fill out to about 60.

**9. Documentation and examples pass.** Every example runnable, every example
listed on the marketplace, and the five listings we published pointed back at
their example.

## 7. Two things to fix now, not in 4.6.1

**Gemini references.** The eval runner already defaults to Luna, and 10 of 12
example strategies use `openai/gpt-6-luna`. But two still carry Gemini:
`lumibot/example_strategies/agent_alpaca_news_builtin.py` and
`agent_discretionary.py`. 87 files repo-wide still mention gemini. The default is
right; the docs and two examples have drifted.

**`lumibot/cli.py` AI template.** I wrote it earlier today and it specifies
`gemini-3.5-flash-lite`, copied from an example that has since moved on. It
should be `openai/gpt-6-luna`. That is a one-line fix and it is mine.

## 8. What I did not verify

- No full eval run was executed for this audit, so every cost and timing figure
  for the eval suite is arithmetic from the code, not measurement.
- I did not audit the skills directory for coverage, only the tools.
- FactSet and other vendor marketplaces were not contacted; their test-account
  availability is unknown.
