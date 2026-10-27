# Bitunix account-selected position modes

Position mode is an account choice. Order submission reads the documented
`GET /api/v1/futures/account/position_mode` response instead of changing the
whole account to HEDGE. `set_position_mode(hedged)` is the explicit operation
for a caller that intends to change the account setting.

In HEDGE mode, `tradeSide` distinguishes opening and closing, and a close uses
the position's native side and position ID. In ONE_WAY mode, `side` remains the
execution direction, `reduceOnly` prevents a close from increasing exposure,
and hedge-specific wire selectors are omitted. Net positions retain their
venue position IDs without inventing long/short hedge-leg identities.

Read the mode for each submission so a manual account-mode change cannot leave
a stale translator. If that read fails, a reduce-only close can use a position
snapshot that explicitly confirms either supported mode. An unidentified
protocol still fails visibly rather than sending an ambiguous order.

This is a shared broker capability for all strategies. Existing base-coin
quantities, precision rules, hedge-leg selection and close leverage preservation
remain covered. Regression cases first reproduced unwanted account mutation,
incorrect one-way close sides and incorrect net-position identity. Earlier
expectations that required implicit HEDGE mutation were updated intentionally
to preserve user-selected modes; genuine failed-mode reads remain tested.

Provider specifications:
- [Account mode read](https://www.bitunix.com/api-docs/futures/account/get_position_mode.html)
- [Explicit account mode change](https://www.bitunix.com/api-docs/futures/account/change_position_mode.html)
- [Order parameters](https://www.bitunix.com/api-docs/futures/trade/place_order.html)
