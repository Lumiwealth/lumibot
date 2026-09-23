# Real examples, memory, and usage

Date: 2026-09-22

## Correction, 2026-09-23

The HTML files in `docs/research/tearsheets/2026-09-22-*.html` are invalid as AI results. The marker is `docs/research/tearsheets/2026-09-22-INVALID.md`. Do not delete the HTML.

`AICongressDisclosuresStrategy` and `AISECInsiderFilingsStrategy` do not take a filings parameter, and they do not stop when filings are missing. The research agent fetches the public source. There is no execution mode. Each in-scope example is `initialize` plus `on_trading_iteration`. Bull and bear run together, then an interpreter, then the trading agent. The rest of this note still describes the old `filing_rule` and `price_rule` runs. That is the record of the invalid sheets, not the current code.

This note records what was checked before the next implementation slice. It is not a deploy plan and it does not change AWS.

## Fake sample trades removed

Deleted:

- `/Users/robertgrzesik/Development/lumibot/lumibot/example_strategies/fixtures/congress_disclosures.json`
- `/Users/robertgrzesik/Development/lumibot/lumibot/example_strategies/fixtures/sec_form4_transactions.json`

`AICongressDisclosuresStrategy` and `AISECInsiderFilingsStrategy` no longer stop unless the caller passes official filings. That behavior is gone. The public pages no longer describe a synthetic Pelosi NVDA fill or a synthetic AAPL Form 4 fill as current proof. The September 20 receipt at `/Users/robertgrzesik/Development/lumibot/docs/research/2026-09-20_AGENT_STRATEGY_EXECUTION_PROOF.md` marks those two runs retired.

Clock tests still pass invented rows into the date gate. Those rows use the name `Clock Test Member`. They are not a member portfolio and they are not shipped sample trades.

Local proof on 2026-09-22 did run. The tear sheets are in `docs/research/tearsheets/2026-09-22-*.html`. Those runs used `filing_rule` and `price_rule`. They did not call a model, so they spent no AI tokens.

They are not finished bots. `proof_modes.price_rule_once` buys one share and stops. The Congress filing path also submits quantity 1. On a $100,000 account that is why the stock tear sheets look flat. Option proofs submit one contract. One contract is 100 shares of premium, so the Pelosi options curve moves. That sheet is 22 Jan 2026 to 4 Feb 2026. The strategy card is about 3% total return and 124% annualized. SPY on the same short window is about -1%. The 124% figure is two weeks stretched into a year. The trade file has four buys and no sells. Those four contracts cost roughly $56,000 of premium, which is why the account moves. Spread proofs open one package, wait a fixed number of days, and close. They do not use the 50% profit take or the 21-day time stop written in the prompt.

Buffett and Ackman proofs read a real filing and then hold. There is no trade, so there is no QuantStats curve. Ro Khanna was only a search-name idea. His House PDF is a scan, the text parser got nothing, and no Khanna bot was built.

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

## What the tear sheets actually proved

Checked against the 2026-09-22 HTML files and `proof_modes.py`.

- Congress stock pages (Pelosi, Josh Gottheimer, Lisa McClain) trade the real House PDF after the filing date. They buy one share per row. They do not rebuild the member's portfolio or scale it to the account.
- Large-cap bought 1 AAPL. Leveraged ETF bought 1 TQQQ. In the real agent code, bull runs, then bear reads the bull case, then the trader decides. That is sequential. It is not bull and bear in parallel.
- Iron condor, credit spread, and SPXW each opened and closed one package. The close was a timer in the proof, not the profit target or stop in the prompt.
- The live web proof downloaded one Pelosi PDF and posted the id to postman-echo.com, a site that repeats what you send it. It then bought 1 SPY. That proved GET and POST. It is not a Pelosi strategy. Unit tests cover PUT, PATCH, DELETE, and credential headers. Those methods were not part of this tear sheet.
- The SEC Form 4 proof fetched the live Atom feed. A backtest's clock is the simulated day. The feed is "latest right now," so those filings were in the future of the simulated day and were hidden. No trade. No Marketplace page.
- Ray Dalio and Citadel stay untouched, including their four paper bots. Do not put the new examples on the Marketplace until sizing and a real exit cycle are in the tear sheets.

Options history for the new tear sheets should use Alpaca or Interactive Brokers. ThetaData is not the path.

A short backtest that finishes slightly red can go on a page if it is not an account blow-up. Prefer examples that make money. Most of them should. Do not publish a tear sheet that destroys the account.

## Memory and what a bot costs

Always-on live bots are not one Fargate box each. They share EC2 `t4g.small` hosts in `/Users/robertgrzesik/Development/bot_manager/terraform/main.tf`: 2 vCPU, 2 GiB, comment says about two bots per instance, `asg_max_size` 500. Current task size in `broker_configs.json` is about 682 CPU units and 596 MiB. A 3 GB or 4 GB bot cannot fit on that host.

us-east-1 Linux on-demand, third-party calculators checked 2026-09-22, not an AWS bill:

- `t4g.small`: $0.0168 per hour, about $12.26 for 730 hours, for the whole host
- `t4g.medium`: $0.0336 per hour, about $24.53 for 730 hours, 4 GiB, still tight once the operating system is subtracted
- `t4g.large`: 8 GiB, not priced in this note

Scheduled starts can now ask for 2, 3, or 4 GB on that one start. CPU stays at the requested value. Omit the setting and the task stays 1024 MiB. An always-on start that asks for 2, 3, or 4 GB is rejected, because those bots share a 2 GB host and a bigger host was not approved. That rejection is only the always-on path.

Linux ARM Fargate us-east-1 public rates from the AWS Fargate pricing page: $0.0000089944 per vCPU-second and $0.0000009889 per GB-second. At 730 hours and 0.5 vCPU:

| Memory | Compute only |
| --- | --- |
| 1 GB | about $14.42 |
| 2 GB | about $17.02 |
| 3 GB | about $19.62 |
| 4 GB | about $22.21 |

The extra gigabyte is about $2.60. That is compute only. A public IPv4 address is $0.005 per hour, about $3.65 per month, and only if one is assigned. The scheduled path defaults public IP off. Production always-on bots use the shared host network, not one public IP per bot. NAT already exists for production. Do not add another NAT.

CPU stays fixed. A scheduled start may set 2, 3, or 4 GB. Allowed steps are only those three. Hard cap is 4 GB. Leave it unset and the task stays 1024 MiB. An always-on start that asks for 2, 3, or 4 GB is rejected in plain language and is not placed. Browser use is one reason a scheduled bot may need more memory. It is not a separate product. The Agent eval already teaches a scheduled start to pass `memoryGb` when the user asks for 2, 3, or 4 GB. A blanket "every AI bot is 2 GB" rule was not chosen.

A 4 GB always-on bot needs a larger host than `t4g.small`. That host change is spending. It stays out until Rob approves the exact size and monthly cost.

## Usage tracking today

The code now stores memory on the runtime session. Node migration `1796000000000-AddDeploymentRuntimeMemory` adds `memoryMib` and `estimatedCostUsd`. Allowed stored sizes are 2048, 3072, and 4096. The cost estimate is the monthly Fargate rate times seconds run, divided by 2,592,000. A 1024 MiB run and an unknown size store null. The account usage query still sums `runtimeSeconds` only. Memory does not feed the minute quota and does not stop a bot.

That migration is in the repo. It is not on the live database, because nothing has been deployed. A read of production on 2026-09-22 still showed the old columns only: id, deploymentId, ownerId, runId, status, startedAt, endedAt, runtimeSeconds, observedAt, createdAt, updatedAt.

September 2026 production rows at that read: 597 runs, 44 deployments, 25.16 runtime hours, first start 2026-09-04, last start 2026-09-22 19:30 UTC, longest run 540 seconds. This is short scheduled duration. No live bot was started at 2, 3, or 4 GB. The memory eval used a fixture start.

`ai_usage_event` is model token cost. August 2026: 4,752 events, 0 with a deployment id, estimated cost $490.26. September 2026: 15,745 events, 594 with a deployment id, estimated cost $181.57. The 2026-09-22 tear sheets are not in that number. They used `filing_rule` and `price_rule` and called no model.

`GET /account/usage` shows deployment runtime minutes. Backtest minute limits can stop a new backtest. Deployment runtime minutes do not stop a bot. There is no live gigabyte bill until a release Rob asks for.
