from __future__ import annotations

import os
from datetime import timedelta
from decimal import Decimal

from lumibot.entities import Asset, Order, SmartLimitConfig, SmartLimitPreset
from lumibot.strategies.strategy import Strategy
from lumibot.traders import Trader

IS_BACKTESTING = os.environ.get("IS_BACKTESTING", "").lower() == "true"


class IbkrCryptoAcceptance(Strategy):
    """Deterministic IBKR crypto acceptance strategy (BTC/USD).

    Goal: validate IBKR crypto minute bars + quote-based fills (bid/ask derived) in a stable window.
    """

    def initialize(self, parameters=None):
        self.set_market("24/7")
        self.sleeptime = "1M"
        self.include_cash_positions = True

        self.base = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
        self.quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

        self.vars.phase = 0
        self.vars.phase_started_at = None
        self.vars.active_order_id = None

        # Prefetch the small windows that the core runtime path relies on:
        # - minute bars for order fills and iteration cadence
        # - daily bars (30D) because the quote path can consult daily series (see `data.py:checker` logs)
        try:
            self.get_historical_prices(self.base, 120, "minute", quote=self.quote)
            self.get_historical_prices(self.base, 30, "day", quote=self.quote)
        except Exception:
            pass

    def _active(self):
        if not self.vars.active_order_id:
            return None
        o = self.get_order(self.vars.active_order_id)
        if o is None or not o.is_active():
            self.vars.active_order_id = None
            return None
        return o

    def _submit(self, order: Order) -> None:
        submitted = self.submit_order(order)
        try:
            self.vars.active_order_id = submitted.identifier
        except Exception:
            self.vars.active_order_id = None

    def on_trading_iteration(self):
        now = self.get_datetime()
        if self.vars.phase_started_at is None:
            self.vars.phase_started_at = now

        active = self._active()
        if active is not None:
            # Safety: cancel anything stuck.
            if (now - self.vars.phase_started_at) > timedelta(minutes=60):
                try:
                    self.cancel_order(active)
                except Exception:
                    pass
                self.vars.active_order_id = None
            return

        # Phase 0: market buy
        if self.vars.phase == 0:
            self.vars.phase_started_at = now
            self._submit(
                self.create_order(
                    self.base, Decimal("0.01"), Order.OrderSide.BUY, order_type=Order.OrderType.MARKET, quote=self.quote
                )
            )
            self.vars.phase += 1
            return

        # Phase 1: hold exposure across day boundaries (QuantStats is daily-resampled) then sell.
        if self.vars.phase == 1 and (now - self.vars.phase_started_at) >= timedelta(hours=36):
            self.vars.phase_started_at = now
            self._submit(
                self.create_order(
                    self.base,
                    Decimal("0.01"),
                    Order.OrderSide.SELL,
                    order_type=Order.OrderType.MARKET,
                    quote=self.quote,
                )
            )
            self.vars.phase += 1
            return

        # Phase 2: SMART_LIMIT buy (tests bid/ask quote path)
        if self.vars.phase == 2:
            self.vars.phase_started_at = now
            cfg = SmartLimitConfig(preset=SmartLimitPreset.FAST, final_price_pct=1.0, slippage=0.0)
            self._submit(
                self.create_order(
                    self.base,
                    Decimal("0.01"),
                    Order.OrderSide.BUY,
                    order_type=Order.OrderType.SMART_LIMIT,
                    smart_limit=cfg,
                    quote=self.quote,
                )
            )
            self.vars.phase += 1
            return

        # Phase 3: SMART_LIMIT sell (hold long enough to cross another day boundary)
        if self.vars.phase == 3 and (now - self.vars.phase_started_at) >= timedelta(hours=24):
            self.vars.phase_started_at = now
            cfg = SmartLimitConfig(preset=SmartLimitPreset.FAST, final_price_pct=1.0, slippage=0.0)
            self._submit(
                self.create_order(
                    self.base,
                    Decimal("0.01"),
                    Order.OrderSide.SELL,
                    order_type=Order.OrderType.SMART_LIMIT,
                    smart_limit=cfg,
                    quote=self.quote,
                )
            )
            self.vars.phase += 1
            return

        return


if __name__ == "__main__":
    if IS_BACKTESTING:
        from lumibot.backtesting.interactive_brokers_rest_backtesting import InteractiveBrokersRESTBacktesting

        IbkrCryptoAcceptance.backtest(
            InteractiveBrokersRESTBacktesting,
            benchmark_asset=Asset("BTC", Asset.AssetType.CRYPTO),
            quote_asset=Asset("USD", Asset.AssetType.FOREX),
            budget=50_000,
            parameters={},
        )
    else:
        trader = Trader()
        strategy = IbkrCryptoAcceptance()
        trader.add_strategy(strategy)
        trader.run_all()
