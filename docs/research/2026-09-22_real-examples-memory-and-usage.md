# Real examples, memory, and usage

Date: 2026-09-22

## Correction, 2026-09-23

The HTML files in `docs/research/tearsheets/2026-09-22-*.html` are invalid as AI results. The marker is `docs/research/tearsheets/2026-09-22-INVALID.md`. Do not delete the HTML.

`AICongressDisclosuresStrategy` and `AISECInsiderFilingsStrategy` do not take a filings parameter, and they do not stop when filings are missing. The research agent fetches the public source. There is no execution mode. Each in-scope example is `initialize` plus `on_trading_iteration`. Bull and bear run together, then an interpreter, then the trading agent. The rest of this note still describes the old `filing_rule` and `price_rule` runs. That is the record of the invalid sheets, not the current code.

This note records what was checked before the next implementation slice. It is not a deploy plan and it does not change AWS.

## Fake sample trades removed

Deleted:

- `lumibot/example_strategies/fixtures/congress_disclosures.json`
- `lumibot/example_strategies/fixtures/sec_form4_transactions.json`

`AICongressDisclosuresStrategy` and `AISECInsiderFilingsStrategy` no longer stop unless the caller passes official filings. That behavior is gone. The public pages no longer describe a synthetic Pelosi NVDA fill or a synthetic AAPL Form 4 fill as current proof. The September 20 receipt at `docs/research/2026-09-20_AGENT_STRATEGY_EXECUTION_PROOF.md` marks those two runs retired.

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

Always-on hosted bots share small hosts, so a 3 GB or 4 GB bot needs a larger
host shape. That is a spending change and stays out until Rob approves the
exact size and monthly cost. Host sizes, current task shapes, production usage
counts, and model-cost figures are kept in private BotSpot operations notes.
