# Lumibot Docs Search Console Snapshot

> Search Console findings for `lumibot.lumiwealth.com` before the AI committee documentation launch.

**Last Updated:** 2026-05-07
**Status:** Active
**Audience:** Both

---

## Overview

Google Search Console is configured at the `lumiwealth.com` domain property, which covers `lumibot.lumiwealth.com`. On 2026-05-07 we submitted `https://lumibot.lumiwealth.com/sitemap.xml`. Google read it successfully, but the live sitemap only listed 8 pages, so only 8 pages were discovered from that sitemap.

## Search Console Data

Filter used: page URL contains `lumibot.lumiwealth.com`.

Last 3 months:

- Clicks: 957
- Impressions: 252K
- CTR: 0.4%
- Average position: 6.5

Top 3-month queries:

| Query | Clicks | Impressions |
| --- | ---: | ---: |
| lumibot | 250 | 7,698 |
| polygon backtesting | 30 | 159 |
| lumiwealth | 24 | 485 |
| lumibot trading | 18 | 265 |
| lumibot python | 11 | 297 |
| lumibot documentation | 5 | 73 |
| databento promo code | 3 | 203 |
| lumibot backtesting | 3 | 135 |
| polygon backtest | 3 | 7 |
| thetadata | 2 | 456 |

Last 28 days:

- Clicks: 309
- Impressions: 86.7K
- CTR: 0.4%
- Average position: 6.1

Top 28-day queries:

| Query | Clicks | Impressions |
| --- | ---: | ---: |
| lumibot | 85 | 3,193 |
| lumibot trading | 8 | 103 |
| lumiwealth | 5 | 201 |
| polygon backtesting | 3 | 19 |
| databento promo code | 2 | 101 |
| lumibot python | 2 | 57 |
| polygon.io | 2 | 44 |
| lumibot ai | 1 | 56 |
| thetadata promo code | 1 | 49 |
| lumibot backtesting | 1 | 46 |

Top 28-day pages:

| Page | Clicks | Impressions |
| --- | ---: | ---: |
| `/` | 136 | 9,082 |
| `/brokers.schwab.html` | 28 | 12,757 |
| `/backtesting.polygon.html` | 21 | 2,735 |
| `/getting_started.html` | 20 | 4,805 |
| `/backtesting.thetadata.html` | 12 | 4,530 |
| `/backtesting.html` | 10 | 1,958 |
| `/strategy_methods.data/lumibot.strategies.strategy.Strategy.get_historical_prices.html` | 7 | 941 |
| `/backtesting.tearsheet_html.html` | 7 | 517 |
| `/brokers.alpaca.html` | 6 | 4,605 |
| `/brokers.html` | 6 | 885 |

## Positioning Read

Current SEO demand is mostly brand, Python trading, backtesting, and provider-specific traffic. AI-specific demand is present but early: `lumibot ai` appeared in the last 28 days with 56 impressions and 1 click.

Recommended public positioning:

- Keep deterministic Python trading and backtesting prominent.
- Make AI trading agents a first-viewport hook because it is differentiated and rising.
- Keep backtest/live parity central because it matches existing search intent and is the biggest product difference versus agent-only frameworks.

## Sitemap Fix

The docs build now generates `docsrc/_extra/sitemap.xml` from all `.rst` source pages. The generated sitemap contains 132 URLs instead of 8. After the docs are deployed, Search Console should be allowed to re-read the sitemap or the sitemap should be resubmitted.
