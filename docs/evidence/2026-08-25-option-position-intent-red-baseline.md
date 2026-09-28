# Option position-intent red baseline

Command:

```text
gtimeout 120s .venv/bin/python -m pytest tests/test_option_position_intent.py -q
```

Pre-fix result: **5 failed** on branch `version/4.5.85`.

The failures reproduced the intended production paths:

1. Alpaca's single-leg `OrderData` had no `position_intent` attribute after submitting an explicit `sell_to_close`.
2. Alpaca parsing reconstructed broker-returned `sell_to_close` as generic `sell`.
3. `Broker.close_position()` created a generic option `sell` rather than `sell_to_close`.
4. A generic option sell was still submitted when an active `sell_to_close` already reserved the full long position.
5. Alpaca submission rejection marked the order as errored but returned normally instead of raising the rejection.

The complete pytest failure output was observed locally on 2026-08-25 before any product-code change. The preserved regression tests are `tests/test_option_position_intent.py`.
