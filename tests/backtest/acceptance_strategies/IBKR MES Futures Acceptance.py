from __future__ import annotations

import os
from datetime import date, timedelta
from decimal import Decimal

from lumibot.entities import Asset, Order, SmartLimitConfig, SmartLimitPreset
from lumibot.strategies.strategy import Strategy
from lumibot.traders import Trader

IS_BACKTESTING = os.environ.get("IS_BACKTESTING", "").lower() == "true"
_BENCHMARK_FUTURE = Asset("MES", asset_type=Asset.AssetType.FUTURE, expiration=date(2025, 12, 19))


class IbkrMesFuturesAcceptance(Strategy):
    """Deterministic IBKR futures acceptance strategy.

    Goal: exercise futures data + fills across a variety of order types without relying on
    non-deterministic signals. The acceptance harness asserts tearsheet metrics + queue-free
    invariant; this script intentionally places a fixed sequence of orders.
    """

    def initialize(self, parameters=None):
        self.set_market("us_futures")
        self.sleeptime = "1M"
        self.include_cash_positions = True

        # Deterministic contract (late 2025). Keep explicit futures for acceptance determinism.
        self.future = Asset("MES", asset_type=Asset.AssetType.FUTURE, expiration=date(2025, 12, 19))

        self.vars.phase = 0
        self.vars.active_order_ids = set()
        self.vars.phase_started_at = None

        # Exercise minute/hour/day data paths early.
        try:
            self.get_historical_prices(self.future, 120, "minute")
            self.get_historical_prices(self.future, 24, "hour")
            self.get_historical_prices(self.future, 3, "day")
        except Exception:
            pass

    def _position_qty(self) -> float:
        pos = self.get_position(self.future)
        if pos is None:
            return 0.0
        try:
            return float(pos.quantity)
        except Exception:
            return 0.0

    def _has_active_orders(self) -> bool:
        for oid in list(self.vars.active_order_ids):
            o = self.get_order(oid)
            if o is None or not o.is_active():
                self.vars.active_order_ids.discard(oid)
        return bool(self.vars.active_order_ids)

    def _submit(self, order: Order) -> None:
        submitted = self.submit_order(order)
        try:
            self.vars.active_order_ids.add(submitted.identifier)
        except Exception:
            pass

    def _cancel_all_orders(self) -> None:
        for oid in list(self.vars.active_order_ids):
            try:
                o = self.get_order(oid)
                if o is not None and o.is_active():
                    self.cancel_order(o)
            except Exception:
                pass
        self.vars.active_order_ids = set()

    def on_trading_iteration(self):
        now = self.get_datetime()
        if self.vars.phase_started_at is None:
            self.vars.phase_started_at = now

        # Safety: prevent indefinite waiting.
        if self._has_active_orders() and (now - self.vars.phase_started_at) > timedelta(minutes=60):
            self._cancel_all_orders()

        if self._has_active_orders():
            return

        qty = self._position_qty()

        # Phase 0: Market long entry
        if self.vars.phase == 0:
            self.vars.phase_started_at = now
            self._submit(
                self.create_order(self.future, Decimal("1"), Order.OrderSide.BUY, order_type=Order.OrderType.MARKET)
            )
            self.vars.phase += 1
            return

        # Phase 1: Hold exposure for a while to ensure non-degenerate return series, then marketable LIMIT exit.
        if self.vars.phase == 1 and qty > 0 and (now - self.vars.phase_started_at) >= timedelta(minutes=120):
            self.vars.phase_started_at = now
            self._submit(
                self.create_order(
                    self.future,
                    Decimal(str(abs(qty))),
                    Order.OrderSide.SELL,
                    order_type=Order.OrderType.LIMIT,
                    limit_price=Decimal("0"),
                )
            )
            self.vars.phase += 1
            return

        # Phase 2: Stop-limit long entry (configured to trigger immediately)
        if self.vars.phase == 2 and qty == 0:
            self.vars.phase_started_at = now
            # Buy STOP_LIMIT triggers when price >= stop_price; setting stop below market should be immediately eligible.
            self._submit(
                self.create_order(
                    self.future,
                    Decimal("1"),
                    Order.OrderSide.BUY,
                    order_type=Order.OrderType.STOP_LIMIT,
                    stop_price=Decimal("0.25"),
                    limit_price=Decimal("999999"),
                )
            )
            self.vars.phase += 1
            return

        # Phase 3: Hold exposure briefly, then place a trailing stop exit (may fill or get canceled).
        if self.vars.phase == 3 and qty > 0 and (now - self.vars.phase_started_at) >= timedelta(minutes=60):
            self.vars.phase_started_at = now
            self._submit(
                self.create_order(
                    self.future,
                    Decimal(str(abs(qty))),
                    Order.OrderSide.SELL,
                    order_type=Order.OrderType.TRAIL,
                    trail_price=Decimal("0.25"),
                )
            )
            self.vars.phase += 1
            return

        # If trailing stop didn't trigger within a reasonable time, force-flatten and continue.
        if self.vars.phase == 4 and qty != 0 and (now - self.vars.phase_started_at) >= timedelta(minutes=120):
            self.vars.phase_started_at = now
            self._submit(
                self.create_order(
                    self.future, Decimal(str(abs(qty))), Order.OrderSide.SELL, order_type=Order.OrderType.MARKET
                )
            )
            self.vars.phase += 1
            return
        if self.vars.phase == 4 and qty == 0:
            self.vars.phase += 1
            self.vars.phase_started_at = now
            return

        # Phase 5: SMART_LIMIT long entry (exercise tick rounding + quote-based fills)
        if self.vars.phase == 5 and qty == 0:
            self.vars.phase_started_at = now
            cfg = SmartLimitConfig(preset=SmartLimitPreset.FAST, final_price_pct=1.0, slippage=0.0)
            self._submit(
                self.create_order(
                    self.future,
                    Decimal("1"),
                    Order.OrderSide.BUY,
                    order_type=Order.OrderType.SMART_LIMIT,
                    smart_limit=cfg,
                )
            )
            self.vars.phase += 1
            return

        # Phase 6: OCO exit (limit+stop; one should fill and cancel the other)
        if self.vars.phase == 6 and qty > 0 and (now - self.vars.phase_started_at) >= timedelta(minutes=60):
            self.vars.phase_started_at = now
            self._submit(
                self.create_order(
                    self.future,
                    Decimal(str(abs(qty))),
                    Order.OrderSide.SELL,
                    order_class=Order.OrderClass.OCO,
                    limit_price=Decimal("0"),  # marketable limit
                    stop_price=Decimal("0.25"),  # should not trigger before the limit
                )
            )
            self.vars.phase += 1
            return

        # Phase 7: Short market entry
        if self.vars.phase == 7 and qty == 0:
            self.vars.phase_started_at = now
            self._submit(
                self.create_order(self.future, Decimal("1"), Order.OrderSide.SELL, order_type=Order.OrderType.MARKET)
            )
            self.vars.phase += 1
            return

        # Phase 8: Market cover
        if self.vars.phase == 8 and qty < 0 and (now - self.vars.phase_started_at) >= timedelta(minutes=60):
            self.vars.phase_started_at = now
            self._submit(
                self.create_order(
                    self.future, Decimal(str(abs(qty))), Order.OrderSide.BUY, order_type=Order.OrderType.MARKET
                )
            )
            self.vars.phase += 1
            return

        # Phase 9: OTO order (entry + exactly 1 child)
        if self.vars.phase == 9 and qty == 0:
            self.vars.phase_started_at = now
            self._submit(
                self.create_order(
                    self.future,
                    Decimal("1"),
                    Order.OrderSide.BUY,
                    order_type=Order.OrderType.MARKET,
                    order_class=Order.OrderClass.OTO,
                    secondary_limit_price=Decimal("0"),  # marketable child exit once parent fills
                )
            )
            self.vars.phase += 1
            return

        # Phase 10: Bracket order (entry + children)
        if self.vars.phase == 10 and qty == 0:
            self.vars.phase_started_at = now
            self._submit(
                self.create_order(
                    self.future,
                    Decimal("1"),
                    Order.OrderSide.BUY,
                    order_type=Order.OrderType.LIMIT,
                    limit_price=Decimal("999999"),  # marketable buy limit
                    order_class=Order.OrderClass.BRACKET,
                    secondary_limit_price=Decimal("0"),  # marketable take-profit sell limit (immediate)
                    secondary_stop_price=Decimal("0.25"),  # stop-loss placeholder (non-triggering)
                    secondary_stop_limit_price=Decimal("0.25"),
                )
            )
            self.vars.phase += 1
            return

        # Final: ensure flat.
        if self.vars.phase >= 11 and qty != 0:
            self._submit(
                self.create_order(
                    self.future,
                    Decimal(str(abs(qty))),
                    Order.OrderSide.SELL if qty > 0 else Order.OrderSide.BUY,
                    order_type=Order.OrderType.MARKET,
                )
            )
            return

        return


if __name__ == "__main__":
    if IS_BACKTESTING:
        from lumibot.backtesting.interactive_brokers_rest_backtesting import InteractiveBrokersRESTBacktesting

        IbkrMesFuturesAcceptance.backtest(
            InteractiveBrokersRESTBacktesting,
            benchmark_asset=_BENCHMARK_FUTURE,
            quote_asset=Asset("USD", Asset.AssetType.FOREX),
            budget=20_000,
            parameters={},
        )
    else:
        trader = Trader()
        strategy = IbkrMesFuturesAcceptance()
        trader.add_strategy(strategy)
        trader.run_all()
