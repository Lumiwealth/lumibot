# Legacy IB Gateway Order Semantics

Legacy Interactive Brokers Gateway order-duration behavior and backtesting parity.

Last Updated: 2026-08-29

Status: Active

Audience: LumiBot contributors and maintainers

## Overview

The deprecated socket-based Interactive Brokers adapter must preserve an
order's ``time_in_force`` and ``good_till_date`` on every native leg it creates
for bracket, OTO, and OCO orders. Automatically generated advanced-order
children inherit the same values so BacktestingBroker's existing DAY and GTD
expiry rules model the same intent. This is intentionally centralized in the
order entity and the legacy adapter; provider-generic strategy APIs remain
unchanged.

When a live last-price tick is unavailable, the legacy adapter may use the
previous-close tick only when no last price has already been received.
