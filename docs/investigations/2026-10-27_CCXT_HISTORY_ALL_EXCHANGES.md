# CCXT HISTORY ON EVERY EXCHANGE

One-line description: public candle history checked on every CCXT exchange with `scripts/check_ccxt_history.py --all`.

Last Updated: 2026-10-27
Status: Snapshot
Audience: Contributors

## Overview

Run on 2026-10-27 with CCXT 4.5.64, public data only (no API keys), 300 one-minute and 200 daily bars per pair, after the page-size negotiation fix in `CcxtData.get_barset_from_api`.

| Result | Count |
| --- | --- |
| Exchanges checked | 105 |
| Checks that loaded the full history | 219 |
| Checks that failed | 39 |
| Exchanges whose markets did not load (network, region, maintenance) | 9 |
| Exchanges that publish no candle history (`fetchOHLCV` false) | 16 |

What the failures are:

- Thin or new markets return fewer bars than asked (for example 1 to 47 one-minute bars). LumiBot reports this honestly instead of inventing bars.
- Exchanges that need an API key even for candles (luno, modetrade, woofipro).
- Exchange limits: daily resolution not offered (paradex), a range cap (exmo), only the latest page returned (apex, bithumb), required extra parameters (cex `dataType`, zebpay `until`), a CCXT parsing error (tokocrypto).

Rerun with `python scripts/check_ccxt_history.py --all` after CCXT upgrades.

## Failed checks

```text
aftermath USDKRW/USD:USDC 1d: FAILED ExchangeNotAvailable: aftermath POST https://aftermath.finance/api/ccxt/OHLCV 400 Bad Request "Error 2026: time period 7257
apex BTC/USDT:USDT 1m: FAILED ValueError: CCXT returned only 200 of 300 requested 1m bars for BTC/USDT:USDT
bequant BTC/USD 1m: FAILED ValueError: CCXT returned only 1 of 300 requested 1m bars for BTC/USD
bigone BTC/USDC 1d: FAILED ValueError: CCXT returned only 1 of 200 requested 1d bars for BTC/USDC
bingx BTC/USDT 1m: FAILED BadRequest: bingx {"code":100204,"msg":"No K-line data found for BTC_USDT with 1min interval.","timestamp":1793138770226}
bingx BTC/USDC 1m: FAILED BadRequest: bingx {"code":100204,"msg":"No K-line data found for BTC_USDC with 1min interval.","timestamp":1793138772933}
bitbank BTC/JPY 1d: FAILED ExchangeError: bitbank {"success":0,"data":{"code":10000}}
bithumb ETH/BTC 1m: FAILED ValueError: CCXT returned only 200 of 300 requested 1m bars for ETH/BTC
btcmarkets BTC/USDT 1m: FAILED ValueError: CCXT returned only 2 of 300 requested 1m bars for BTC/USDT
bybit BTC/USD 1d: FAILED ValueError: CCXT returned only 195 of 200 requested 1d bars for BTC/USD
cex BTC/USDT 1m: FAILED ArgumentsRequired: cex fetchOHLCV requires a parameter "dataType" to be either "bestBid" or "bestAsk"
cex BTC/USDT 1d: FAILED ArgumentsRequired: cex fetchOHLCV requires a parameter "dataType" to be either "bestBid" or "bestAsk"
cex BTC/USD 1m: FAILED ArgumentsRequired: cex fetchOHLCV requires a parameter "dataType" to be either "bestBid" or "bestAsk"
cex BTC/USD 1d: FAILED ArgumentsRequired: cex fetchOHLCV requires a parameter "dataType" to be either "bestBid" or "bestAsk"
delta BTC/USDT 1m: FAILED ValueError: CCXT returned only 10 of 300 requested 1m bars for BTC/USDT
delta BTC/USDT 1d: FAILED ValueError: CCXT returned only 1 of 200 requested 1d bars for BTC/USDT
exmo BTC/USDC 1m: FAILED BadRequest: exmo {"s":"error","errmsg":"range period is too long: maximum 3000 candles allowed"}
extended BTC/USDC 1d: FAILED ValueError: CCXT returned only 173 of 200 requested 1d bars for BTC/USDC
kucoineu BTC/USDT 1m: FAILED ValueError: CCXT returned only 2 of 300 requested 1m bars for BTC/USDT
kucoineu BTC/USDC 1m: FAILED ValueError: CCXT returned no 1m bars for BTC/USDC; 300 were requested
luno BTC/USDT 1m: FAILED AuthenticationError: luno requires "apiKey" credential
luno BTC/USDT 1d: FAILED AuthenticationError: luno requires "apiKey" credential
luno BTC/USDC 1m: FAILED AuthenticationError: luno requires "apiKey" credential
luno BTC/USDC 1d: FAILED AuthenticationError: luno requires "apiKey" credential
mercado HNT/BRL 1m: FAILED ValueError: CCXT returned only 15 of 300 requested 1m bars for HNT/BRL
modetrade 1000BONK/USDC:USDC 1m: FAILED AuthenticationError: modetrade requires "apiKey" credential
modetrade 1000BONK/USDC:USDC 1d: FAILED AuthenticationError: modetrade requires "apiKey" credential
ndax BTC/USDT 1m: FAILED ValueError: CCXT returned no 1m bars for BTC/USDT; 300 were requested
ndax BTC/USD 1m: FAILED ValueError: CCXT returned no 1m bars for BTC/USD; 300 were requested
paradex AAVE/USD:USDC 1m: FAILED ValueError: CCXT returned only 47 of 300 requested 1m bars for AAVE/USD:USDC
paradex AAVE/USD:USDC 1d: FAILED BadRequest: paradex {"error":"INVALID_REQUEST_PARAMETER","message":"resolution: must be one of [1 3 5 15 30 60].","data":{"reso
tokocrypto BTC/USDC 1m: FAILED KeyError: 0
tokocrypto BTC/USDC 1d: FAILED KeyError: 0
woofipro 1000BONK/USDC:USDC 1m: FAILED AuthenticationError: woofipro requires "apiKey" credential
woofipro 1000BONK/USDC:USDC 1d: FAILED AuthenticationError: woofipro requires "apiKey" credential
zebpay BTC/USDT 1m: FAILED ArgumentsRequired: zebpay fetchOHLCV() requires a both a since and until/endtime parameter for spot markets
zebpay BTC/USDT 1d: FAILED ArgumentsRequired: zebpay fetchOHLCV() requires a both a since and until/endtime parameter for spot markets
zebpay BTC/USDT:USDT 1m: FAILED BadRequest: zebpay property interval should not exist
zebpay BTC/USDT:USDT 1d: FAILED BadRequest: zebpay property interval should not exist
```
