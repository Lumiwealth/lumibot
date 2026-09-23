# Agent Strategy Execution Proof

One-line description: Real-model and deterministic end-to-end evidence for the Congress, SEC Form 4, and authenticated-browser Strategy examples.

Last Updated: 2026-09-22

Status: The September 20 Congress and Form 4 runs below used sample rows that were deleted on 2026-09-22. Those two runs are retired. They are not current proof that a congressional or Form 4 example works. The browser section used an owned test site and remains a mechanics receipt only. No live broker, customer account, release, or deployment was used.

Audience: LumiBot maintainers, BotSpot integrators, and strategy authors.

## Overview

This receipt distinguishes three different claims:

1. The public examples are ordinary LumiBot `Strategy` subclasses.
2. Their availability, browser, order, and fill wiring passes deterministic
   regression tests through `Strategy.run_backtest()` and
   `PandasDataBacktesting`.
3. The Congress and Form 4 examples also completed fresh Gemini
   `gemini-3.5-flash-lite` runs through the normal Google ADK runtime and placed
   simulated orders through LumiBot's agent order tools.

The Congress and Form 4 inputs in the two runs below were invented sample rows, including a made-up Nancy Pelosi NVDA purchase. Those JSON files were deleted. The examples now stop unless the caller passes official filings. Do not cite the fills below as a working Pelosi bot, a working insider bot, or a tear sheet.

## Retired sample Congress run (not current proof)

- Strategy: `AICongressDisclosuresStrategy`
- Backtest window: 2026-01-01 through 2026-04-01
- Transaction date: 2026-01-05
- Public disclosure date: 2026-02-14
- First researcher/trader decision: 2026-02-17 09:30 America/New_York, the
  first session in the backtest after publication
- Order: `bt_1`, buy 27 NVDA market
- Fill: 27 NVDA at 181.77 on 2026-02-17 09:30 America/New_York
- Later disclosed AAPL sale: evaluated on 2026-03-16 and held because the
  strategy had no long AAPL position and shorting is prohibited

Local raw evidence:

- `logs/AICongressDisclosuresStrategy_2026-09-20_16-22_6mzysf_trades.csv`
  SHA-256 `769c5c2a9c1c5fd8148200828cd161098545e8c8cd00526e45512d0c66aae68d`
- `logs/AICongressDisclosuresStrategy_2026-09-20_16-22_6mzysf_agent_detail.parquet`
  SHA-256 `8833cff068df676217f306ec6a89e6cacab8e68fe71e11a49745ff0c412e44a7`
- `logs/AICongressDisclosuresStrategy_2026-09-20_16-22_6mzysf_tearsheet.html`

## Retired sample SEC Form 4 run (not current proof)

- Strategy: `AISECInsiderFilingsStrategy`
- Backtest window: 2026-01-01 through 2026-04-01
- Transaction date: 2026-02-10
- SEC acceptance/publication: 2026-02-11 18:30 UTC, after the US cash close
- First researcher/trader decision: 2026-02-12 09:30 America/New_York
- Classification: the open-market purchase was eligible; the grant/award was
  filtered and never presented as an open-market trade signal
- Order: `bt_1`, buy 18 AAPL market
- Fill: 18 AAPL at 275.59 on 2026-02-12 09:30 America/New_York

Local raw evidence:

- `logs/AISECInsiderFilingsStrategy_2026-09-20_16-24_7sEX8j_trades.csv`
  SHA-256 `ac3e888034594be748a41ec748f41785f35647c9c780e53f073188dd75d92a71`
- `logs/AISECInsiderFilingsStrategy_2026-09-20_16-24_7sEX8j_agent_detail.parquet`
  SHA-256 `393377d11c2f36d74b62d69f18534a85f15d23c3b7bcd5fdc06e2cfb1b77084c`
- `logs/AISECInsiderFilingsStrategy_2026-09-20_16-24_7sEX8j_tearsheet.html`

## Browser Strategy Proof

`AIBrowserResearchShowcaseStrategy` ran through the same backtest engine. Its
research agent used the real built-in browser tools to open a persistent
Patchright session, navigate to an owned JavaScript fixture, inject a
host-scoped login, observe the authenticated dashboard, capture a screenshot,
and close with a trace receipt. The trading/risk agent then submitted `bt_1`,
which filled one SHOW share at 102.00. The publisher opened a separate browser
profile and posted an idempotent receipt keyed by `bt_1` to the owned fixture.

Local screenshot evidence:

- `docs/research/artifacts/2026-09-20-strategy-proof/test_browser_showcase_research0/browser/artifacts/7486734966bd4c7996f921d27b2ea3b4/strategy-research.png`
  SHA-256 `bf4a4a943eacf1ef9a34107e779807be5c04053eb4eb60f69fc6bd11d14db944`
- `docs/research/artifacts/2026-09-20-strategy-proof/test_browser_showcase_research0/browser/artifacts/cba4e397c31448b4861b5b5b4e33b290/strategy-published-receipt.png`
  SHA-256 `4ba417a388c92b075e5bd64036a089eb64f0050ef607faf23848b660cfe5078d`

This proves the strategy-to-browser-to-trade-to-publisher contract on an owned
site. It does not claim that TipRanks, Instagram, X, Reddit, or another
third-party account was automated.

### Fresh real-model browser run

A fresh Gemini ``gemini-3.5-flash-lite`` run then exercised the same public
Strategy through Google ADK. The first attempt exposed two real defects that the
scripted regression had not caught: Patchright's synchronous runtime was being
started inside ADK's asyncio loop, and stateful browser tools marked with the
string ``cache_scope=none`` were accidentally cached. Both failures received
red regression tests before repair.

The corrected run completed the whole flow:

- Authenticated browser research at the owned JavaScript fixture.
- Screenshot and action-trace receipts from the research session.
- Risk sizing with ``max_position_pct=1`` interpreted as 1%, not 100%.
- Order ``bt_1``: buy one SPY market; filled at 602.00 in the backtest.
- Idempotent publication keyed by ``bt_1`` to the owned fixture.
- A second screenshot and action trace showing the confirmed ``published``
  response.

Local raw evidence:

- ``docs/research/artifacts/2026-09-20-strategy-proof/real-model-browser-green/result.json``
  SHA-256 ``60fae68664f30952bb6f825d9c659303082f0e56cefb8c61733e0708471075c9``
- Research screenshot SHA-256
  ``bf4a4a943eacf1ef9a34107e779807be5c04053eb4eb60f69fc6bd11d14db944``
- Publication screenshot SHA-256
  ``4ba417a388c92b075e5bd64036a089eb64f0050ef607faf23848b660cfe5078d``
- Research trace SHA-256
  ``d3a50f5e6ecabe9bccf273fb3049d06b026da0d080e1cd9ab5461979cefdda0c``
- Publication trace SHA-256
  ``7ab4149ba93a4903e67ba51cd765566ebd843d14ef9d8e4ccefa9f362c67ff4e``

The model currently reasons from the rendered page URL, title, visible text,
extracted DOM values, and action results. Screenshots are preserved as audit
artifacts, but they are not yet reattached to the model as native multimodal
image parts. Native screenshot vision remains a separate, explicit gap.

## Deterministic Gate

Command:

```bash
LUMIBOT_DISABLE_DOTENV=1 LUMIBOT_DISABLE_DOTENV_LOCAL=1 \
  .venv/bin/pytest -q \
  tests/backtest/test_disclosure_strategy_backtests.py \
  tests/backtest/test_browser_showcase_backtest.py
```

Result: `3 passed`.

The tests assert that no disclosure agent runs before `published_at`, that Form
4 non-open-market grants are filtered, that only the trading/risk role receives
order tools, that actual backtest fills occur, and that browser screenshots,
action traces, and the idempotent publication receipt exist.

The complete affected browser, disclosure-strategy, and documentation group
also passed after the original implementation: `28 passed`. The browser gate
was expanded and rerun after the async/cache and Bitunix repairs: `75 passed`
in 47.88 seconds. The group includes the real Patchright fixture,
authenticated profile persistence, multi-tab behavior, upload/download,
screenshots, the 100-restart soak, and the new `wait_text` synchronization
action. `wait_text` was added after a captured red run proved that waiting only
for a locator's attached state could return before an IndexedDB write became
observable.

## Repository-Wide Verification

The current deterministic inventory was selected with dotenv disabled and with
credential/network acceptance groups kept separate. A single-process attempt
hit its 15-minute safety cutoff at 56%, so it is not counted as a result. The
same file inventory was then split once across four terminal shards:

```text
3012 passed
42 skipped
2 xfailed
4 xpassed
83 subtests passed
0 failed
```

The shard logs are under
``logs/full-deterministic-shards-20260920-c82f77e5/``. An isolated follow-up
correctly revealed that
``tests/test_broker_bitunix.py::TestBitunixBroker::test_parse_source_timestep``
was order-dependent: it failed alone because a mocked data-source parser
returned a ``MagicMock``. The broker now validates delegated parser output and
uses its existing fallback mapping for invalid values. The complete Bitunix
broker file then passed: `15 passed`.

The acceptance inventory remains a separate red gate. Three acceptance
backtests passed before Backdoor Butterfly repeatedly received
`ThetaTerminal session invalid (status=403)` from the shared downloader. That
run was interrupted rather than continuing to send known-invalid requests.
Neither that acceptance gate nor the full repository is described as green.

## BotSpot Source Parity

The four current approved BotSpot revisions are now the source of truth for
the checked-in Ray Dalio and Citadel examples. Their local `main.py` content is
byte-for-byte identical to the read-only production revision:

- Ray Dalio regular: `a2a02db9ad0db1b8ce8d9e339fe0f0cd8b0698b1ce36281c077291fa077e2914`
- Ray Dalio leveraged: `7f8f2d4ef5363669926080d86504f68bdbd7ab30618fbac94dc2f0e469a304f1`
- Citadel regular: `50e78b923a9548994ba593f91a792c34f2d3ed384cc405ca3d2abecf5166a758`
- Citadel leveraged: `3e9bc4330b0d8bab021f7844fa41541bcd86a7b24b67f371c4967bcf8e36f915`

Each documentation page now puts its regular and leveraged BotSpot tracking
links directly below the workflow image. Congress, Form 4, and the browser
showcase cannot receive equivalent links until those strategies exist as
BotSpot listings after the separately authorized LumiBot release.

## Communication Delivery Receipts

- Slack DM to Rob (link kept in private notes).
- Gmail from the BotSpot sender mailbox to Rob's test inbox (message ID kept in
  private notes).

The MetaMask external channel was read successfully through the connected
Slack application. There was no newer MetaMask reply after Rob's 2026-09-19
messages, and no external MetaMask message was sent.

## Point-in-Time Boundary

These examples are backtestable only when the historical source preserves the
date the market could first know each record. Congress uses report/publication
time rather than transaction date. Form 4 uses EDGAR acceptance/publication
time rather than transaction date. A present-day API snapshot without those
historical availability timestamps is not sufficient for an honest backtest.

The deleted sample files made this boundary easy to test, and they also made the example look like a real member portfolio. That was the wrong trade. Clock tests may still pass invented rows into the date gate. The shipped example must not.

## Data boundary

House Clerk periodic transaction reports and Senate eFD filings are public. SEC Form 4 filings on EDGAR are public. This example does not need a paid license to read those official PDFs and XML files. A backtest is honest only when each row stays hidden until its report or acceptance time.
