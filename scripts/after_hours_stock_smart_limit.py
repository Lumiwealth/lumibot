"""After-hours SMART_LIMIT experiment for stocks (paper).

This is an ops script: it places real paper orders and logs repricing + fills.
Use this to sanity-check after-hours behavior (e.g. Tradier `time_in_force='post'`).
"""

from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from lumibot.brokers.alpaca import Alpaca
from lumibot.brokers.tradier import Tradier
from lumibot.credentials import ALPACA_TEST_CONFIG, TRADIER_TEST_CONFIG
from lumibot.entities import Asset, Order, SmartLimitConfig, SmartLimitPreset
from lumibot.strategies.strategy import Strategy


class _Harness(Strategy):
    def initialize(self, parameters=None):
        self.sleeptime = "1S"

    def on_trading_iteration(self):
        return


@dataclass(frozen=True)
class _Snapshot:
    ts: float
    status: str
    limit_price: float | None
    avg_fill_price: float | None


def _make_broker(name: str):
    name = name.lower().strip()
    if name == "tradier":
        if not TRADIER_TEST_CONFIG.get("ACCOUNT_NUMBER") or not TRADIER_TEST_CONFIG.get("ACCESS_TOKEN"):
            raise RuntimeError("Missing TRADIER_TEST_ACCOUNT_NUMBER / TRADIER_TEST_ACCESS_TOKEN in .env")
        return Tradier(
            account_number=TRADIER_TEST_CONFIG["ACCOUNT_NUMBER"],
            access_token=TRADIER_TEST_CONFIG["ACCESS_TOKEN"],
            paper=True,
            connect_stream=True,
        )
    if name == "alpaca":
        return Alpaca(ALPACA_TEST_CONFIG, connect_stream=False)
    raise ValueError(f"Unsupported broker: {name}")


def _poll(broker, order: Order) -> _Snapshot:
    now = time.time()
    broker_name = str(getattr(broker, "name", "")).lower()

    if broker_name == "tradier":
        record = broker._pull_broker_order(order.identifier)  # noqa: SLF001 (ops script)
        status = str(record.get("status", "")).lower()
        limit_price = record.get("price")
        avg_fill = record.get("avg_fill_price")
        return _Snapshot(
            ts=now,
            status=status,
            limit_price=float(limit_price) if limit_price is not None else None,
            avg_fill_price=float(avg_fill) if avg_fill is not None else None,
        )

    if broker_name == "alpaca":
        raw = broker.api.get_order_by_id(order.identifier)
        raw_status = getattr(raw, "status", "")
        if hasattr(raw_status, "value"):
            raw_status = raw_status.value
        status = str(raw_status).lower()
        limit_price = getattr(raw, "limit_price", None)
        avg_fill = getattr(raw, "filled_avg_price", None) or getattr(raw, "avg_fill_price", None)
        return _Snapshot(
            ts=now,
            status=status,
            limit_price=float(limit_price) if limit_price is not None else None,
            avg_fill_price=float(avg_fill) if avg_fill is not None else None,
        )

    raise ValueError(f"Unsupported broker for polling: {broker_name}")


def _drive_until_done(strategy: _Harness, order: Order, *, timeout_seconds: int) -> tuple[bool, list[_Snapshot]]:
    start = time.time()
    snapshots: list[_Snapshot] = []
    last_limit = None

    while time.time() - start < timeout_seconds:
        snap = _poll(strategy.broker, order)
        snapshots.append(snap)

        order.status = snap.status
        if snap.limit_price is not None:
            order.limit_price = float(snap.limit_price)

        if last_limit is None and snap.limit_price is not None:
            last_limit = snap.limit_price
        elif last_limit is not None and snap.limit_price is not None and abs(snap.limit_price - last_limit) > 1e-9:
            print(f"{datetime.now().isoformat()} limit moved: {last_limit} -> {snap.limit_price}")
            last_limit = snap.limit_price

        if snap.status in {"filled", "fill"}:
            return True, snapshots
        if snap.status in {"canceled", "cancelled", "rejected", "expired", "error"}:
            return False, snapshots

        try:
            strategy._executor._process_smart_limit_orders()  # noqa: SLF001 (ops script)
        except Exception as exc:
            print(f"{datetime.now().isoformat()} SMART_LIMIT tick error: {exc}")

        time.sleep(1.0)

    # Timeout: cancel the order so we don't leave a dangling after-hours paper order behind.
    try:
        strategy.broker.cancel_order(order)
    except Exception:
        pass
    return False, snapshots


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker", choices=["tradier", "alpaca"], required=True)
    parser.add_argument("--symbol", default="SPY")
    parser.add_argument("--timeout-seconds", type=int, default=180)
    parser.add_argument("--tif", default="post", help="Tradier duration (post/pre/day/gtc). Alpaca ignores this.")
    args = parser.parse_args()

    broker = _make_broker(args.broker)
    strategy = _Harness(broker=broker)
    try:
        strategy.initialize()
    except TypeError:
        strategy.initialize(parameters=None)

    cfg = SmartLimitConfig(preset=SmartLimitPreset.FAST, final_price_pct=1.0, final_hold_seconds=30)
    asset = Asset(args.symbol.upper(), asset_type=Asset.AssetType.STOCK)

    try:
        # Best-effort: clear any previously-open orders for this strategy before we start.
        try:
            strategy.cancel_open_orders()
        except Exception:
            pass

        print(f"{datetime.now().isoformat()} submitting BUY SMART_LIMIT {asset.symbol} after-hours tif={args.tif}")
        buy = strategy.create_order(
            asset,
            1,
            Order.OrderSide.BUY,
            order_type=Order.OrderType.SMART_LIMIT,
            smart_limit=cfg,
            time_in_force=args.tif,
        )
        submitted_buy = strategy.submit_order(buy)
        ok_buy, buy_snaps = _drive_until_done(strategy, submitted_buy, timeout_seconds=args.timeout_seconds)
        buy_fill = next((s.avg_fill_price for s in reversed(buy_snaps) if s.avg_fill_price is not None), None)
        print(f"{datetime.now().isoformat()} BUY done ok={ok_buy} fill={buy_fill}")

        if not ok_buy:
            return 2

        print(f"{datetime.now().isoformat()} submitting SELL SMART_LIMIT {asset.symbol} after-hours tif={args.tif}")
        sell = strategy.create_order(
            asset,
            1,
            Order.OrderSide.SELL,
            order_type=Order.OrderType.SMART_LIMIT,
            smart_limit=cfg,
            time_in_force=args.tif,
        )
        submitted_sell = strategy.submit_order(sell)
        ok_sell, sell_snaps = _drive_until_done(strategy, submitted_sell, timeout_seconds=args.timeout_seconds)
        sell_fill = next((s.avg_fill_price for s in reversed(sell_snaps) if s.avg_fill_price is not None), None)
        print(f"{datetime.now().isoformat()} SELL done ok={ok_sell} fill={sell_fill}")

        return 0 if ok_sell else 3
    finally:
        # Best-effort cleanup so we don't leave paper orders hanging around.
        try:
            strategy.cancel_open_orders()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
