from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal

from lumibot.entities import Asset, Order
from lumibot.strategies.strategy import Strategy


@dataclass(frozen=True)
class MesParityConfig:
    asset_type: str  # "future" | "cont_future"
    expiration: date | None = None


class MesOrderMatrixParity(Strategy):
    """Order-matrix strategy for provider parity (OHLC-only fills).

    This intentionally places a fixed sequence of orders to validate:
    - fills for market/limit/stop/stop-limit/trailing/bracket/OCO/OTO
    - long + short flows
    - deterministic behavior across providers when using Trades OHLC bars

    NOTE: Do not rely on SMART_LIMIT here because DataBento backtests are OHLCV-only.
    """

    def initialize(self, parameters=None):
        self.set_market("us_futures")
        self.sleeptime = "1M"
        self.include_cash_positions = True

        cfg = parameters.get("cfg") if isinstance(parameters, dict) else None
        if not isinstance(cfg, MesParityConfig):
            cfg = MesParityConfig(asset_type="cont_future")

        if cfg.asset_type == "future":
            if cfg.expiration is None:
                raise ValueError("MesOrderMatrixParity: explicit FUTURE requires expiration")
            self.future = Asset("MES", asset_type=Asset.AssetType.FUTURE, expiration=cfg.expiration)
        else:
            self.future = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)

        self.vars.phase = 0
        self.vars.phase_started_at = None
        self.vars.active = set()

        # Prime the data path.
        try:
            self.get_historical_prices(self.future, 120, "minute")
            self.get_historical_prices(self.future, 24, "hour")
            self.get_historical_prices(self.future, 3, "day")
        except Exception:
            pass

    def _pos_qty(self) -> float:
        pos = self.get_position(self.future)
        if pos is None:
            return 0.0
        try:
            return float(pos.quantity)
        except Exception:
            return 0.0

    def _has_active(self) -> bool:
        for oid in list(self.vars.active):
            o = self.get_order(oid)
            if o is None or not o.is_active():
                self.vars.active.discard(oid)
        return bool(self.vars.active)

    def _submit(self, order: Order) -> None:
        submitted = self.submit_order(order)
        try:
            self.vars.active.add(submitted.identifier)
        except Exception:
            pass

    def on_trading_iteration(self):
        now = self.get_datetime()
        if self.vars.phase_started_at is None:
            self.vars.phase_started_at = now

        if self._has_active():
            # Hard safety: never hang forever.
            if (now - self.vars.phase_started_at) > timedelta(minutes=90):
                for oid in list(self.vars.active):
                    o = self.get_order(oid)
                    if o is not None and o.is_active():
                        try:
                            self.cancel_order(o)
                        except Exception:
                            pass
                self.vars.active = set()
            return

        qty = self._pos_qty()

        # Phase 0: Market long entry
        if self.vars.phase == 0:
            self.vars.phase_started_at = now
            self._submit(
                self.create_order(self.future, Decimal("1"), Order.OrderSide.BUY, order_type=Order.OrderType.MARKET)
            )
            self.vars.phase += 1
            return

        # Phase 1: Marketable LIMIT exit after some runtime
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

        # Phase 2: STOP_LIMIT long entry (configured to be eligible quickly)
        if self.vars.phase == 2 and qty == 0:
            self.vars.phase_started_at = now
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

        # Phase 3: trailing stop exit
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

        # Phase 4: OCO exit (limit should fill; stop should cancel)
        if self.vars.phase == 4 and qty > 0 and (now - self.vars.phase_started_at) >= timedelta(minutes=60):
            self.vars.phase_started_at = now
            self._submit(
                self.create_order(
                    self.future,
                    Decimal(str(abs(qty))),
                    Order.OrderSide.SELL,
                    order_class=Order.OrderClass.OCO,
                    limit_price=Decimal("0"),
                    stop_price=Decimal("0.25"),
                )
            )
            self.vars.phase += 1
            return

        # Phase 5: Short entry
        if self.vars.phase == 5 and qty == 0:
            self.vars.phase_started_at = now
            self._submit(
                self.create_order(self.future, Decimal("1"), Order.OrderSide.SELL, order_type=Order.OrderType.MARKET)
            )
            self.vars.phase += 1
            return

        # Phase 6: OTO cover (entry filled immediately, then child filled next bar)
        if self.vars.phase == 6 and qty < 0 and (now - self.vars.phase_started_at) >= timedelta(minutes=30):
            self.vars.phase_started_at = now
            self._submit(
                self.create_order(
                    self.future,
                    Decimal("1"),
                    Order.OrderSide.BUY,
                    order_type=Order.OrderType.LIMIT,
                    limit_price=Decimal("999999"),
                    order_class=Order.OrderClass.OTO,
                    secondary_limit_price=Decimal("0"),
                )
            )
            self.vars.phase += 1
            return

        # Final: enforce flat.
        if self.vars.phase >= 7 and qty != 0:
            self._submit(
                self.create_order(
                    self.future,
                    Decimal(str(abs(qty))),
                    Order.OrderSide.SELL if qty > 0 else Order.OrderSide.BUY,
                    order_type=Order.OrderType.MARKET,
                )
            )
            return
