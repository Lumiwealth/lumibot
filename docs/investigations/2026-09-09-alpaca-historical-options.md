# Alpaca historical-options integration boundary

## Verified facts

- Alpaca documents historical option bars and trades, with availability from
  February 2024. Its current option-chain and snapshot endpoints are latest
  snapshots; they are not a historical chain or historical-Greeks endpoint.
- Alpaca's contract endpoint can include inactive contracts and filter an exact
  expiration. That can provide the expired-contract universe, but a backtest
  still needs as-of quotes/trades and must calculate any historical Greeks from
  as-of inputs rather than reuse today's snapshot.
- `AlpacaData` has a live `OptionHistoricalDataClient`, but
  `AlpacaBacktesting` currently constructs only stock and crypto historical
  clients and has no historical option-chain implementation. Live options
  support therefore does not establish backtesting support.
- Alpaca's public support answer says API market data may not be redistributed,
  and its customer agreement prohibits reproducing, distributing, selling, or
  commercially exploiting market data without written consent. A user's OAuth
  grant authenticates that user; it does not by itself grant BotSpot a hosted
  redistribution right.

## Required product decision

Obtain written confirmation for the intended user-bound hosted backtesting
flow. The request must distinguish calculations executed for the authenticated
user from storage, display, sharing, marketplace publication, and redistribution
to other users. Do not use Rob's personal market-data entitlement for another
user.

## Engineering path after permission is confirmed

1. Bind Alpaca data access to the requesting user's approved credential and
   subscription, independently of the broker chosen for order execution.
2. Add an option historical client to `AlpacaBacktesting` and route option bars,
   quotes, and trades by the full OCC contract identity.
3. Discover the as-of contract universe with inactive contracts and exact
   expirations; paginate deterministically and retain source/completeness
   metadata.
4. Calculate as-of Greeks only from as-of underlying price, option price,
   expiration, rate, and volatility inputs. Never substitute a latest snapshot.
5. Price multi-leg packages from contemporaneous bid/ask observations and fail
   closed when a required leg lacks a usable quote.
6. Add unit, adapter-contract, historical look-ahead, and multi-leg integration
   tests before advertising Alpaca historical options as supported.

## Non-goals

- No provider bake-off.
- No shared credential or entitlement.
- No fallback from historical data to a current option chain.
- No purchase, subscription change, cloud deployment, or production mutation in
  this code change.
