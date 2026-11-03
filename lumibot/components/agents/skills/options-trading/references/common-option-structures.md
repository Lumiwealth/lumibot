# Common Option Structures

All descriptions are per one package. Scale every leg by the same package quantity
unless a ratio is stated explicitly.

## Vertical spreads

- Bull put credit spread: sell a higher-strike put and buy a lower-strike put with
  the same expiration.
- Bear call credit spread: sell a lower-strike call and buy a higher-strike call
  with the same expiration.
- Bull call debit spread: buy a lower-strike call and sell a higher-strike call.
- Bear put debit spread: buy a higher-strike put and sell a lower-strike put.

For equal-width credit verticals, opening credit must be greater than zero and less
than the strike width.

## Iron condor

Use one expiration and four ordered strikes:

1. Buy the lower put wing.
2. Sell the higher put.
3. Sell the lower call.
4. Buy the higher call wing.

The put wing must be below the short put. The call wing must be above the short
call. The short put must be below the short call. A standard iron condor opens for
a net credit smaller than either wing width.

## Butterfly

A long call butterfly buys one lower-strike call, sells two middle-strike calls,
and buys one higher-strike call with the same expiration and normally equal wing
widths. A long put butterfly uses the analogous put legs. Preserve the 1:-2:1
ratio.

Compute each wing width from its adjacent strikes. Shifting all three strikes
up or down changes the center and moneyness, not the width when those differences
remain equal. Compare the actual center, debit, payoff bounds and signed net
Greeks. For any raw Greek g, the one-package contribution is
`g_lower - 2*g_middle + g_upper`, scaled by the actual multiplier when reporting
share-equivalent exposure. Small net delta alone does not remove gamma, time-decay
or volatility risk as the underlying approaches or leaves the center.

## Two-long/one-short call stock replacement

Buy two lower-strike calls and sell one higher-strike call at one expiration.
Preserve the 2:-1 ratio; one long and two shorts is a different structure with
unbounded upside loss. Evaluate the actual debit, intrinsic/extrinsic premium,
signed net Greeks and downside loss. Offsetting extrinsic cost does not make
the position free. A physically settled short call can require delivering the
underlying before either long is exercised: inspect shares, account capacity,
dividends and remaining obligations independently of the expiration payoff.

## Straddle and strangle

- Long straddle: buy one call and one put at the same strike and expiration.
- Short straddle: sell those two legs.
- Long strangle: buy an out-of-the-money put and call with the same expiration.
- Short strangle: sell those two legs.

Short uncovered structures can have very large or unlimited loss. Do not use them
unless the user's rules explicitly permit the exposure and the broker supports it.

## Calendar spread

Use the same strike and right across different expirations, normally selling the
nearer contract and buying the farther contract. Calendar valuation is sensitive
to term structure and volatility. Verify both expirations and both exact quotes.
