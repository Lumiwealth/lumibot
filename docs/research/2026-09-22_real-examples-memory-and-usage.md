# Real examples, memory, and usage

Date: 2026-09-22

This note records what was checked before the next implementation slice. It is not a deploy plan and it does not change AWS.

## Fake sample trades removed

Deleted:

- `/Users/robertgrzesik/Development/lumibot/lumibot/example_strategies/fixtures/congress_disclosures.json`
- `/Users/robertgrzesik/Development/lumibot/lumibot/example_strategies/fixtures/sec_form4_transactions.json`

`AICongressDisclosuresStrategy` and `AISECInsiderFilingsStrategy` now stop unless the caller passes official filings. The public pages no longer describe a synthetic Pelosi NVDA fill or a synthetic AAPL Form 4 fill as current proof. The September 20 receipt at `/Users/robertgrzesik/Development/lumibot/docs/research/2026-09-20_AGENT_STRATEGY_EXECUTION_PROOF.md` marks those two runs retired.

Clock tests still pass invented rows into the date gate. Those rows use the name `Clock Test Member`. They are not a member portfolio and they are not shipped sample trades.

Local proof on 2026-09-22:

```text
.venv/bin/pytest -q tests/test_disclosure_strategies.py tests/test_disclosure_replay.py tests/backtest/test_disclosure_strategy_backtests.py tests/test_agent_capability_docs.py tests/test_ai_two_agent_strategy_examples.py tests/test_ai_trading_team_example.py
37 passed
```

No real House PDF, Senate PDF, or EDGAR backtest has been run yet. The examples are honest. They are not yet working bots.

## Official filings do contain option fields

House Clerk periodic transaction reports and Senate eFD filings are public. Some Pelosi option rows name the underlying, call or put, strike, and expiration in the description. Examples already read: House filing `20026590` (50 call options, strike 150, expiration 2026-01-16, on GOOGL, AMZN, and NVDA) and filing `20033725` (2026-01-23 exercises of those calls, plus new 20-call purchases expiring 2027-01-15). Older stock-only bots dropped that description. They did not fail because the official PDF lacked the fields.

A stock example can trade from ticker plus buy or sell. An option example must skip a row that lacks underlying, put or call, strike, and expiration. Amounts are ranges, not exact share counts. Hide every row until `ReportDate`.

## Who to build after Pelosi

Google Keyword Planner, United States, English, 2026-09-22:

- `congress stock tracker`: 2,400 average monthly searches, low competition
- `congress stock trades`: 1,900 average monthly searches, low competition
- Named seeds not returned, so treat them as unreported, not as zero: `nancy pelosi stock trades`, `pelosi stock tracker`, `ro khanna stock trades`, `tommy tuberville stock trades`

X search, 2026-06-01 through 2026-09-22, 34 posts, 4 x_search calls. After Pelosi, the chatter ranked Ro Khanna, Lisa McClain, Michael McCaul, Thomas Suozzi, and Josh Gottheimer. The posts were about stocks, not options. Do not repeat return claims from those posts.

SEO parent is the category (`congress stock tracker`), not a second personal name. First member page: Pelosi. Second member page: Ro Khanna, because that is the X crowd after Pelosi, and only after his filings parse. A third or fourth member waits on the same proof. Add one category page for the search phrase people actually type.

## What the other examples actually do

Checked by reading code. No fresh tear sheet.

- Ackman GitHub example hardcodes a stock list and asks the model about quality and catalysts. It does not fetch news or 13F filings. The public BotSpot Marketplace page points at a different 573-byte SPY file, strategy `ab216282-0ff0-4aa1-948c-c0daa1eed5a9`.
- Buffett GitHub example asks for filings and cash flow in the prompt. The strategy file has no EDGAR client. The Marketplace copy is an older, shorter file.
- Large-cap means the big-stock bull and bear team (names such as AAPL, MSFT, NVDA). Leveraged ETF means paired funds such as TQQQ with SQQQ. Both Marketplace copies are older than GitHub.
- Iron condor and credit spread rules say one atomic multi-leg order. Their prompts do not name `orders_submit_multileg`. The SPX zero-DTE example does name that tool. A real options backtest has to prove one package for entry and one package for exit.
- Ray Dalio and Citadel Marketplace code still matches GitHub. Leave them alone, including their four paper deployments.

Options history for the new tear sheets should use Alpaca or Interactive Brokers. ThetaData is not the path.

A short backtest that finishes slightly red can go on a page if it is not an account blow-up. Prefer examples that make money. Most of them should. Do not publish a tear sheet that destroys the account.

## Memory and what a bot costs

Always-on live bots are not one Fargate box each. They share EC2 `t4g.small` hosts in `/Users/robertgrzesik/Development/bot_manager/terraform/main.tf`: 2 vCPU, 2 GiB, comment says about two bots per instance, `asg_max_size` 500. Current task size in `broker_configs.json` is about 682 CPU units and 596 MiB. A 3 GB or 4 GB bot cannot fit on that host.

us-east-1 Linux on-demand, third-party calculators checked 2026-09-22, not an AWS bill:

- `t4g.small`: $0.0168 per hour, about $12.26 for 730 hours, for the whole host
- `t4g.medium`: $0.0336 per hour, about $24.53 for 730 hours, 4 GiB, still tight once the operating system is subtracted
- `t4g.large`: 8 GiB, not priced in this note

Scheduled custom bots use one global Fargate size, not a per-bot setting. Terraform default is 512 CPU and 1024 MiB. At 0.5 vCPU, Fargate allows 1, 2, 3, or 4 GB.

Linux ARM Fargate us-east-1 public rates from the AWS Fargate pricing page: $0.0000089944 per vCPU-second and $0.0000009889 per GB-second. At 730 hours and 0.5 vCPU:

| Memory | Compute only |
| --- | --- |
| 1 GB | about $14.42 |
| 2 GB | about $17.02 |
| 3 GB | about $19.62 |
| 4 GB | about $22.21 |

The extra gigabyte is about $2.60. That is compute only. A public IPv4 address is $0.005 per hour, about $3.65 per month, and only if one is assigned. The scheduled path defaults public IP off. Production always-on bots use the shared host network, not one public IP per bot. NAT already exists for production. Do not add another NAT.

CPU stays fixed. Customers and the agent should set memory when a schedule or an always-on bot starts. Allowed steps: 2, 3, and 4 GB. Hard cap: 4 GB. Browser is one reason to raise memory. It is not a separate product. An Agent eval should teach the agent to raise memory when the strategy needs it. A blanket "every AI bot is 2 GB" rule is optional and was not chosen.

A 4 GB always-on bot needs a larger host than `t4g.small`. That host change is spending. It stays out until Rob approves the exact size and monthly cost.

## Usage tracking today

Production `deployment_runtime_session` columns: id, deploymentId, ownerId, runId, status, startedAt, endedAt, runtimeSeconds, observedAt, createdAt, updatedAt. No memory, gigabytes, or dollar columns.

September 2026 is the only month with rows: 597 runs, 44 deployments, 25.16 runtime hours, first start 2026-09-04, last start 2026-09-22 19:30 UTC, longest run 540 seconds. This is short scheduled duration, not always-on gigabyte-hours. The writer is live.

`ai_usage_event` is model token cost. August 2026: 4,752 events, 0 with a deployment id, estimated cost $490.26. September 2026: 15,745 events, 594 with a deployment id, estimated cost $181.57.

`GET /account/usage` shows deployment runtime minutes. Backtest minute limits can stop a new backtest. Deployment runtime minutes do not stop a bot. There is no gigabyte bill yet.

Next usage work, after approval: store the memory setting and gigabyte-hours on the runtime session. Keep it as a record. Do not block starts and do not change plan caps until a later decision.
