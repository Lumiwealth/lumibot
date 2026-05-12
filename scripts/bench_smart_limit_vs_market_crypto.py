"""Benchmark SMART_LIMIT vs MARKET fills for Alpaca crypto (paper).

Crypto is 24/7, so this script is useful on weekends/after-hours to validate SMART_LIMIT behavior
without waiting for the equity/options session.

This is an ops script: it places real paper orders.
"""

from __future__ import annotations

import argparse
import csv
import random
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from lumibot.brokers.alpaca import Alpaca
from lumibot.credentials import ALPACA_TEST_CONFIG
from lumibot.entities import Asset, Order, SmartLimitConfig, SmartLimitPreset
from lumibot.strategies.strategy import Strategy


class _BenchStrategy(Strategy):
    def initialize(self, parameters=None):
        self.sleeptime = "1S"

    def on_trading_iteration(self):
        return


@dataclass(frozen=True)
class _FillResult:
    ok: bool
    seconds: float
    reprices: int
    submit_bid: float | None
    submit_ask: float | None
    submit_mid: float | None
    fill_price: float | None


def _alpaca() -> Alpaca:
    api_key = ALPACA_TEST_CONFIG.get("API_KEY")
    api_secret = ALPACA_TEST_CONFIG.get("API_SECRET")
    if not api_key or not api_secret or api_key == "<your key here>" or api_secret == "<your key here>":
        raise RuntimeError("Missing ALPACA_TEST_API_KEY / ALPACA_TEST_API_SECRET in .env")
    return Alpaca(ALPACA_TEST_CONFIG, connect_stream=False)


def _poll_alpaca_order(broker: Alpaca, order: Order):
    return broker.api.get_order_by_id(order.identifier)


def _cancel_alpaca_open_orders_for_symbol(broker: Alpaca, symbol: str) -> None:
    """Best-effort cleanup to avoid Alpaca crypto wash-trade rejections in paper."""
    try:
        from alpaca.trading.enums import QueryOrderStatus
        from alpaca.trading.requests import GetOrdersRequest
    except Exception:
        return

    target = str(symbol).upper().replace("/", "")
    try:
        request = GetOrdersRequest(status=QueryOrderStatus.OPEN, limit=500)
        open_orders = broker.api.get_orders(filter=request) or []
    except Exception:
        return

    for raw in open_orders:
        raw_symbol = getattr(raw, "symbol", None)
        if raw_symbol is None and hasattr(raw, "_raw") and isinstance(raw._raw, dict):
            raw_symbol = raw._raw.get("symbol")
        if not raw_symbol:
            continue
        raw_norm = str(raw_symbol).upper().replace("/", "")
        if raw_norm != target:
            continue
        try:
            broker.api.cancel_order_by_id(getattr(raw, "id", None) or raw._raw.get("id"))  # noqa: SLF001
        except Exception:
            pass


def _get_crypto_position_qty(broker: Alpaca, base_symbol: str) -> float:
    base_symbol = str(base_symbol).upper()
    try:
        positions = broker.api.get_all_positions() or []
    except Exception:
        return 0.0
    for p in positions:
        sym = str(getattr(p, "symbol", "")).upper().replace("/", "")
        if sym.startswith(base_symbol):
            try:
                return float(getattr(p, "qty", 0.0))
            except Exception:
                return 0.0
    return 0.0


def _quote_snapshot(strategy: _BenchStrategy, base: Asset, quote: Asset) -> tuple[float | None, float | None, float | None]:
    q = strategy.get_quote(base, quote=quote)
    bid = getattr(q, "bid", None)
    ask = getattr(q, "ask", None)
    try:
        bid_f = float(bid) if bid is not None else None
    except Exception:
        bid_f = None
    try:
        ask_f = float(ask) if ask is not None else None
    except Exception:
        ask_f = None
    mid_f = None
    if bid_f is not None and ask_f is not None and ask_f > 0 and bid_f >= 0:
        mid_f = (bid_f + ask_f) / 2.0
    return bid_f, ask_f, mid_f


def _wait_fill(
    strategy: _BenchStrategy,
    order: Order,
    *,
    timeout_seconds: int,
    drive_smart_limit: bool,
) -> tuple[bool, int, float, float | None]:
    start = time.time()
    last_limit = None
    reprices = 0

    while time.time() - start < timeout_seconds:
        if drive_smart_limit:
            try:
                strategy._executor._process_smart_limit_orders()  # noqa: SLF001 (ops script)
            except Exception:
                pass

        try:
            raw = _poll_alpaca_order(strategy.broker, order)
        except Exception:
            # If submission failed, the order identifier might be a local UUID that the broker doesn't know about.
            return False, reprices, time.time() - start, None
        raw_status = getattr(raw, "status", "")
        if hasattr(raw_status, "value"):
            raw_status = raw_status.value
        status = str(raw_status).lower()

        limit_price = getattr(raw, "limit_price", None)
        if limit_price is not None:
            try:
                limit_f = float(limit_price)
                if last_limit is None:
                    last_limit = limit_f
                elif abs(limit_f - last_limit) > 1e-9:
                    reprices += 1
                    last_limit = limit_f
            except Exception:
                pass

        if status in {"filled", "fill"}:
            avg_fill = getattr(raw, "filled_avg_price", None) or getattr(raw, "avg_fill_price", None)
            fill_price = float(avg_fill) if avg_fill is not None else None
            return True, reprices, time.time() - start, fill_price
        if status in {"canceled", "cancelled", "rejected", "expired", "error"}:
            avg_fill = getattr(raw, "filled_avg_price", None) or getattr(raw, "avg_fill_price", None)
            fill_price = float(avg_fill) if avg_fill is not None else None
            return False, reprices, time.time() - start, fill_price

        time.sleep(1.0)

    try:
        strategy.broker.cancel_order(order)
    except Exception:
        pass
    return False, reprices, time.time() - start, None


def _slippage_vs_mid(side: str, fill_price: float | None, mid: float | None) -> float | None:
    if fill_price is None or mid is None:
        return None
    side = side.lower()
    if side == "buy":
        return float(fill_price) - float(mid)
    return float(mid) - float(fill_price)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="BTC", help="Crypto base (e.g. BTC, ETH)")
    parser.add_argument("--quote", default="USD", help="Quote currency (default USD)")
    parser.add_argument("--qty", type=float, default=0.001)
    parser.add_argument("--trials", type=int, default=5)
    parser.add_argument("--preset", choices=["fast", "normal", "patient"], default="fast")
    parser.add_argument("--final-price-pct", type=float, default=1.0)
    parser.add_argument("--step-seconds", type=int, default=1)
    parser.add_argument("--final-hold-seconds", type=int, default=30)
    parser.add_argument("--timeout-seconds", type=int, default=120)
    parser.add_argument("--output", default="logs/bench_smart_limit_vs_market_crypto.csv")
    parser.add_argument("--market-first", action="store_true")
    args = parser.parse_args()

    broker = _alpaca()
    strategy = _BenchStrategy(broker=broker)
    try:
        strategy.initialize()
    except TypeError:
        strategy.initialize(parameters=None)

    preset = SmartLimitPreset(args.preset)
    smart_cfg = SmartLimitConfig(
        preset=preset,
        final_price_pct=float(args.final_price_pct),
        step_seconds=int(args.step_seconds),
        final_hold_seconds=int(args.final_hold_seconds),
    )

    base = Asset(args.symbol.upper(), asset_type=Asset.AssetType.CRYPTO)
    quote = Asset(args.quote.upper(), asset_type=Asset.AssetType.FOREX)

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    headers = [
        "trial",
        "mode",
        "side",
        "symbol",
        "qty",
        "submit_bid",
        "submit_ask",
        "submit_mid",
        "fill_price",
        "slippage_vs_mid",
        "reprices",
        "seconds",
        "timestamp",
    ]

    write_header = not out_path.exists()
    with out_path.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        if write_header:
            writer.writeheader()

        for trial in range(1, args.trials + 1):
            _cancel_alpaca_open_orders_for_symbol(broker, f"{base.symbol}/{quote.symbol}")

            # Best-effort flatten (paper can charge fees/rounding; keep it simple and sell any existing base qty).
            existing_qty = _get_crypto_position_qty(broker, base.symbol)
            if existing_qty > 0:
                flatten = strategy.create_order(
                    base,
                    existing_qty,
                    Order.OrderSide.SELL,
                    order_type=Order.OrderType.MARKET,
                    quote=quote,
                )
                submitted_flatten = strategy.submit_order(flatten)
                _wait_fill(strategy, submitted_flatten, timeout_seconds=30, drive_smart_limit=False)

            modes = ["market", "smart"]
            if not args.market_first:
                random.shuffle(modes)

            for mode in modes:
                if mode == "market":
                    order_type = Order.OrderType.MARKET
                    smart_limit = None
                    drive = False
                else:
                    order_type = Order.OrderType.SMART_LIMIT
                    smart_limit = smart_cfg
                    drive = True

                # Round-trip: buy then sell. Sell size must reflect what we actually received after fees/rounding.
                for side in ("buy", "sell"):
                    bid, ask, mid = _quote_snapshot(strategy, base, quote)
                    order_side = Order.OrderSide.BUY if side == "buy" else Order.OrderSide.SELL
                    qty = float(args.qty)
                    if side == "sell":
                        # After the buy, Alpaca can apply crypto fees/rounding such that base qty is slightly less
                        # than requested. Sell the available position size to avoid "insufficient balance".
                        for _ in range(10):
                            qty = _get_crypto_position_qty(broker, base.symbol)
                            if qty > 0:
                                break
                            time.sleep(0.5)
                        if qty <= 0:
                            break
                    order = strategy.create_order(
                        base,
                        qty,
                        order_side,
                        order_type=order_type,
                        smart_limit=smart_limit,
                        quote=quote,
                    )

                    submitted = strategy.submit_order(order)
                    ok, reprices, seconds, fill_price = _wait_fill(
                        strategy,
                        submitted,
                        timeout_seconds=int(args.timeout_seconds),
                        drive_smart_limit=drive,
                    )

                    writer.writerow(
                        {
                            "trial": trial,
                            "mode": mode,
                            "side": side,
                            "symbol": f"{base.symbol}/{quote.symbol}",
                            "qty": float(args.qty),
                            "submit_bid": bid,
                            "submit_ask": ask,
                            "submit_mid": mid,
                            "fill_price": fill_price,
                            "slippage_vs_mid": _slippage_vs_mid(side, fill_price, mid),
                            "reprices": reprices,
                            "seconds": round(seconds, 2),
                            "timestamp": datetime.now().isoformat(),
                        }
                    )
                    f.flush()

                    if not ok:
                        try:
                            strategy.cancel_open_orders()
                        except Exception:
                            pass
                        # If the buy succeeded but sell failed, the next iteration will attempt sell again.
                        break

            # Best-effort cleanup between trials.
            try:
                strategy.cancel_open_orders()
            except Exception:
                pass

    print(f"Wrote results to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
