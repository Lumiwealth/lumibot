# Options Package Price Integrity

Complete quote evidence for multi-leg prices.

Last Updated: 2026-09-08
Status: Implemented; deterministic regression covered
Audience: Contributors and strategy authors

## Overview

`OptionsHelper.calculate_multileg_limit_price` previously skipped unavailable
leg quotes. A two-leg spread could consequently receive a one-leg price. This
was reproduced at the real helper boundary before repair.

Every leg must now be an option with finite nonnegative bid/ask and bid <= ask.
A missing quote, provider failure, unsupported leg or invalid quote returns None
for the entire package. Unknown pricing styles raise ValueError. Valid quotes
retain signed best/mid/fastest per-unit pricing. This does not guarantee a fill
or replace strategy-specific liquidity, risk or size checks.

`tests/test_agent_package_price_integrity.py` fixtures only provider quotes and
exercises the actual helper, not a replacement pricing function. Agent package
pricing reports unavailable and cannot calculate a partial limit automatically.
