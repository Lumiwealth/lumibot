"""Benchmark SMART_LIMIT vs MARKET fills (paper trading).

This script is intentionally operational (not a unit test). It places *real* paper trades and compares:
- Market order fills
- SMART_LIMIT fills (with repricing)

It is designed to be run manually during market hours.
"""

from __future__ import annotations

import argparse
import csv
import random
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

# Ensure the repo root is importable when running as a script (sys.path[0] == scripts/).
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from lumibot.brokers.alpaca import Alpaca
from lumibot.brokers.tradier import Tradier
from lumibot.components.options_helper import OptionsHelper
from lumibot.credentials import ALPACA_TEST_CONFIG, TRADIER_TEST_CONFIG
from lumibot.entities import Asset, Order, SmartLimitConfig, SmartLimitPreset
from lumibot.strategies.strategy import Strategy


def _is_finite_positive(value: Any) -> bool:
    try:
        return value is not None and float(value) > 0 and float(value) == float(value)
    except Exception:
        return False


def _quote_snapshot(strategy: Strategy, asset: Asset, *, quote: Asset | None = None, exchange: str | None = None):
    q = strategy.get_quote(asset, quote=quote, exchange=exchange)
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


def _multileg_net_snapshot(strategy: Strategy, legs: list[Order]):
    quote_data: list[tuple[Order, float | None, float | None]] = []
    for leg in legs:
        q = strategy.get_quote(leg.asset, quote=leg.quote, exchange=leg.exchange)
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
        quote_data.append((leg, bid_f, ask_f))

    if any(b is None or a is None or b < 0 or a <= 0 for _, b, a in quote_data):
        return None, None, None

    net_best = 0.0
    net_fastest = 0.0
    for leg, bid_f, ask_f in quote_data:
        if leg.is_buy_order():
            net_best += float(bid_f)
            net_fastest += float(ask_f)
        else:
            net_best -= float(ask_f)
            net_fastest -= float(bid_f)

    return net_best, net_fastest, (net_best + net_fastest) / 2.0


class _BenchStrategy(Strategy):
    def initialize(self, parameters=None):
        self.sleeptime = "1S"
        self.options_helper = OptionsHelper(self)

    def on_trading_iteration(self):
        return


@dataclass(frozen=True)
class _FillSnapshot:
    ts: float
    status: str
    limit_price: float | None
    avg_fill_price: float | None
    filled_qty: float | None


def _make_broker(name: str):
    name = name.lower().strip()
    if name == "alpaca":
        return Alpaca(ALPACA_TEST_CONFIG, connect_stream=False)
    if name == "tradier":
        if not TRADIER_TEST_CONFIG.get("ACCOUNT_NUMBER") or not TRADIER_TEST_CONFIG.get("ACCESS_TOKEN"):
            raise RuntimeError("Missing TRADIER_TEST_ACCOUNT_NUMBER / TRADIER_TEST_ACCESS_TOKEN in .env")
        return Tradier(
            account_number=TRADIER_TEST_CONFIG["ACCOUNT_NUMBER"],
            access_token=TRADIER_TEST_CONFIG["ACCESS_TOKEN"],
            paper=True,
            connect_stream=True,
        )
    raise ValueError(f"Unsupported broker: {name}")


def _poll_order(broker, order: Order) -> _FillSnapshot:
    now = time.time()
    broker_name = getattr(broker, "name", "").lower()

    if broker_name == "alpaca":
        raw = broker.api.get_order_by_id(order.identifier)
        raw_status = getattr(raw, "status", "")
        if hasattr(raw_status, "value"):
            raw_status = raw_status.value
        status = str(raw_status).lower()
        limit_price = getattr(raw, "limit_price", None)
        avg_fill = getattr(raw, "filled_avg_price", None) or getattr(raw, "avg_fill_price", None)
        filled_qty = getattr(raw, "filled_qty", None) or getattr(raw, "qty", None)
        return _FillSnapshot(
            ts=now,
            status=status,
            limit_price=float(limit_price) if limit_price is not None else None,
            avg_fill_price=float(avg_fill) if avg_fill is not None else None,
            filled_qty=float(filled_qty) if filled_qty is not None else None,
        )

    if broker_name == "tradier":
        record = broker._pull_broker_order(order.identifier)  # noqa: SLF001 (ops script)
        status = str(record.get("status", "")).lower()
        limit_price = record.get("price")
        avg_fill = record.get("avg_fill_price")
        filled_qty = record.get("exec_quantity") or record.get("filled_quantity")
        return _FillSnapshot(
            ts=now,
            status=status,
            limit_price=float(limit_price) if limit_price is not None else None,
            avg_fill_price=float(avg_fill) if avg_fill is not None else None,
            filled_qty=float(filled_qty) if filled_qty is not None else None,
        )

    raise ValueError(f"Unsupported broker: {broker_name}")


def _wait_for_fill(
    strategy: _BenchStrategy,
    order: Order,
    *,
    timeout_seconds: int,
    drive_smart_limit: bool,
) -> tuple[bool, list[_FillSnapshot]]:
    snapshots: list[_FillSnapshot] = []
    start = time.time()
    last_limit: float | None = None

    while time.time() - start < timeout_seconds:
        snap = _poll_order(strategy.broker, order)
        snapshots.append(snap)

        # Keep the LumiBot order object roughly in sync so SMART_LIMIT repricing stops once filled/canceled.
        try:
            order.status = snap.status
            if snap.limit_price is not None:
                order.limit_price = float(snap.limit_price)
        except Exception:
            pass

        if snap.limit_price is not None and last_limit is None:
            last_limit = snap.limit_price
        elif snap.limit_price is not None and last_limit is not None and abs(snap.limit_price - last_limit) > 1e-9:
            last_limit = snap.limit_price

        if snap.status in {"filled", "fill"}:
            return True, snapshots
        if snap.status in {"canceled", "cancelled", "rejected", "expired", "error"}:
            return False, snapshots

        if drive_smart_limit:
            try:
                strategy._executor._process_smart_limit_orders()  # noqa: SLF001 (ops script)
            except Exception as exc:
                strategy.log_message(f"[bench] SMART_LIMIT tick error: {exc}", color="red")

        time.sleep(1.0)

    try:
        strategy.broker.cancel_order(order)
    except Exception:
        pass
    return False, snapshots


def _pick_underlying(symbol: str) -> Asset:
    symbol = symbol.upper().strip()
    if symbol in {"SPX", "SPXW", "NDX", "RUT", "VIX"}:
        return Asset(symbol, asset_type=Asset.AssetType.INDEX)
    return Asset(symbol, asset_type=Asset.AssetType.STOCK)


def _pick_expiry(strategy: _BenchStrategy, underlying: Asset, days_out: int) -> date:
    chains = strategy.get_chains(underlying)
    if not chains and underlying.symbol.upper() == "SPX":
        chains = strategy.get_chains(Asset("SPXW", asset_type=Asset.AssetType.INDEX))
    if not chains and underlying.symbol.upper() == "SPXW":
        chains = strategy.get_chains(Asset("SPX", asset_type=Asset.AssetType.INDEX))
    if not chains:
        raise RuntimeError(f"No option chains for {underlying.symbol}")

    target_date = datetime.now().astimezone().date() + timedelta(days=days_out)
    expiry = strategy.options_helper.get_expiration_on_or_after_date(
        target_date,
        chains,
        "call",
        underlying_asset=underlying,
    )
    if expiry is None:
        raise RuntimeError(f"Could not find expiry for {underlying.symbol} on/after {target_date}")
    return expiry


def _pick_atm_strike(strategy: _BenchStrategy, underlying: Asset, expiry: date) -> float:
    """Pick an approximate ATM strike.

    Some brokers/data sources don't provide a reliable index last price (e.g. Tradier + SPX),
    but still provide option chains + strikes. Fall back to the chain median strike.
    """

    price = strategy.get_last_price(underlying)
    if _is_finite_positive(price):
        return float(price)

    chains = strategy.get_chains(underlying)
    if not chains and underlying.symbol.upper() == "SPX":
        chains = strategy.get_chains(Asset("SPXW", asset_type=Asset.AssetType.INDEX))
    if not chains and underlying.symbol.upper() == "SPXW":
        chains = strategy.get_chains(Asset("SPX", asset_type=Asset.AssetType.INDEX))
    if not chains:
        raise RuntimeError(f"Underlying price unavailable and no chains for {underlying.symbol}")

    strikes_raw = chains.strikes(expiry, "CALL") or []
    strikes = sorted(float(s) for s in strikes_raw if s is not None)
    if not strikes:
        raise RuntimeError(f"No strikes found for {underlying.symbol} expiry={expiry}")
    return float(strikes[len(strikes) // 2])


def _strike_step(symbol: str) -> float:
    symbol = symbol.upper().strip()
    if symbol in {"SPX", "SPXW", "RUT", "NDX"}:
        return 5.0
    if symbol in {"TSLA"}:
        return 5.0
    return 1.0


def _build_single_call(
    strategy: _BenchStrategy,
    underlying_symbol: str,
    *,
    days_out: int,
    otm_points: float,
    order_type: str,
    smart_limit: SmartLimitConfig | None,
) -> tuple[Order, Order]:
    underlying = _pick_underlying(underlying_symbol)
    expiry = _pick_expiry(strategy, underlying, days_out)
    atm = _pick_atm_strike(strategy, underlying, expiry)
    step = _strike_step(underlying.symbol)
    atm_rounded = round(atm / step) * step
    strike = atm_rounded + otm_points

    call_asset = strategy.options_helper.find_next_valid_option(underlying, strike, expiry, put_or_call="call")
    if call_asset is None:
        raise RuntimeError("Could not find a valid call option asset")

    open_order = strategy.create_order(
        call_asset,
        1,
        Order.OrderSide.BUY_TO_OPEN,
        order_type=order_type,
        smart_limit=smart_limit,
    )
    close_order = strategy.create_order(
        call_asset,
        1,
        Order.OrderSide.SELL_TO_CLOSE,
        order_type=order_type,
        smart_limit=smart_limit,
    )
    return open_order, close_order


def _build_iron_condor(
    strategy: _BenchStrategy,
    underlying_symbol: str,
    *,
    days_out: int,
    short_distance: float,
    wing_width: float,
    order_type: str,
    smart_limit: SmartLimitConfig | None,
) -> tuple[list[Order], list[Order]]:
    underlying = _pick_underlying(underlying_symbol)
    expiry = _pick_expiry(strategy, underlying, days_out)
    atm = _pick_atm_strike(strategy, underlying, expiry)
    step = _strike_step(underlying.symbol)
    atm_rounded = round(atm / step) * step

    put_short = atm_rounded - short_distance
    put_long = put_short - wing_width
    call_short = atm_rounded + short_distance
    call_long = call_short + wing_width

    put_short_asset = strategy.options_helper.find_next_valid_option(underlying, put_short, expiry, put_or_call="put")
    put_long_asset = strategy.options_helper.find_next_valid_option(underlying, put_long, expiry, put_or_call="put")
    call_short_asset = strategy.options_helper.find_next_valid_option(underlying, call_short, expiry, put_or_call="call")
    call_long_asset = strategy.options_helper.find_next_valid_option(underlying, call_long, expiry, put_or_call="call")

    if not all([put_short_asset, put_long_asset, call_short_asset, call_long_asset]):
        raise RuntimeError("Failed to resolve all iron condor legs")

    open_legs = [
        strategy.create_order(put_long_asset, 1, Order.OrderSide.BUY_TO_OPEN, order_type=order_type, smart_limit=smart_limit),
        strategy.create_order(put_short_asset, 1, Order.OrderSide.SELL_TO_OPEN, order_type=order_type, smart_limit=smart_limit),
        strategy.create_order(call_short_asset, 1, Order.OrderSide.SELL_TO_OPEN, order_type=order_type, smart_limit=smart_limit),
        strategy.create_order(call_long_asset, 1, Order.OrderSide.BUY_TO_OPEN, order_type=order_type, smart_limit=smart_limit),
    ]

    close_legs = [
        strategy.create_order(put_long_asset, 1, Order.OrderSide.SELL_TO_CLOSE, order_type=order_type, smart_limit=smart_limit),
        strategy.create_order(put_short_asset, 1, Order.OrderSide.BUY_TO_CLOSE, order_type=order_type, smart_limit=smart_limit),
        strategy.create_order(call_short_asset, 1, Order.OrderSide.BUY_TO_CLOSE, order_type=order_type, smart_limit=smart_limit),
        strategy.create_order(call_long_asset, 1, Order.OrderSide.SELL_TO_CLOSE, order_type=order_type, smart_limit=smart_limit),
    ]

    return open_legs, close_legs


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker", required=True, choices=["alpaca", "tradier"])
    parser.add_argument("--symbol", required=True, help="Underlying (e.g. SPY, TSLA, SPX, SPXW)")
    parser.add_argument("--structure", required=True, choices=["single_call", "iron_condor"])
    parser.add_argument("--days-out", type=int, default=7)
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--preset", choices=["fast", "normal", "patient"], default="fast")
    parser.add_argument("--final-price-pct", type=float, default=1.0)
    parser.add_argument("--final-hold-seconds", type=int, default=120)
    parser.add_argument("--timeout-seconds", type=int, default=240)
    parser.add_argument("--output", default="logs/bench_smart_limit_vs_market_v2.csv")
    parser.add_argument("--market-first", action="store_true")
    args = parser.parse_args()

    broker = _make_broker(args.broker)
    strategy = _BenchStrategy(broker=broker)
    # In live mode, Strategy.initialize() is normally invoked by the Trader/Executor.
    # This script drives the strategy manually, so call initialize explicitly.
    try:
        strategy.initialize()
    except TypeError:
        # Legacy initialize signatures may accept parameters; ignore for this ops script.
        strategy.initialize(parameters=None)

    preset = SmartLimitPreset(args.preset)
    smart_cfg = SmartLimitConfig(
        preset=preset,
        final_price_pct=float(args.final_price_pct),
        final_hold_seconds=int(args.final_hold_seconds),
    )

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    headers = [
        "trial",
        "broker",
        "symbol",
        "structure",
        "mode",
        "open_submit_bid",
        "open_submit_ask",
        "open_submit_mid",
        "open_net_best",
        "open_net_fastest",
        "open_net_mid",
        "open_status",
        "open_fill_price",
        "close_submit_bid",
        "close_submit_ask",
        "close_submit_mid",
        "close_net_best",
        "close_net_fastest",
        "close_net_mid",
        "close_status",
        "close_fill_price",
        "open_reprices",
        "close_reprices",
        "open_seconds",
        "close_seconds",
        "timestamp",
    ]

    write_header = not output_path.exists()
    with output_path.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        if write_header:
            writer.writeheader()

        modes = ["market", "smart"]
        for trial in range(1, args.trials + 1):
            if args.market_first:
                run_order = modes
            else:
                run_order = modes[:]
                random.shuffle(run_order)

            for mode in run_order:
                if mode == "market":
                    order_type = Order.OrderType.MARKET
                    smart_limit = None
                    drive = False
                else:
                    order_type = Order.OrderType.SMART_LIMIT
                    smart_limit = smart_cfg
                    drive = True

                open_submit_bid = open_submit_ask = open_submit_mid = None
                close_submit_bid = close_submit_ask = close_submit_mid = None
                open_net_best = open_net_fastest = open_net_mid = None
                close_net_best = close_net_fastest = close_net_mid = None

                if args.structure == "single_call":
                    open_order, close_order = _build_single_call(
                        strategy,
                        args.symbol,
                        days_out=args.days_out,
                        otm_points=_strike_step(args.symbol),
                        order_type=order_type,
                        smart_limit=smart_limit,
                    )

                    open_submit_bid, open_submit_ask, open_submit_mid = _quote_snapshot(
                        strategy, open_order.asset, quote=open_order.quote, exchange=open_order.exchange
                    )
                    submitted = strategy.submit_order(open_order)
                    open_parent = submitted
                    ok_open, open_snaps = _wait_for_fill(strategy, open_parent, timeout_seconds=args.timeout_seconds, drive_smart_limit=drive)

                    ok_close = False
                    close_snaps: list[_FillSnapshot] = []
                    if ok_open:
                        close_submit_bid, close_submit_ask, close_submit_mid = _quote_snapshot(
                            strategy, close_order.asset, quote=close_order.quote, exchange=close_order.exchange
                        )
                        submitted_close = strategy.submit_order(close_order)
                        close_parent = submitted_close
                        ok_close, close_snaps = _wait_for_fill(strategy, close_parent, timeout_seconds=args.timeout_seconds, drive_smart_limit=drive)
                        if not ok_close:
                            # Best-effort flatten to avoid leaving positions behind.
                            mkt_close = strategy.create_order(
                                close_order.asset,
                                close_order.quantity,
                                close_order.side,
                                order_type=Order.OrderType.MARKET,
                                quote=close_order.quote,
                                exchange=close_order.exchange,
                            )
                            submitted_mkt = strategy.submit_order(mkt_close)
                            _wait_for_fill(strategy, submitted_mkt, timeout_seconds=60, drive_smart_limit=False)

                else:
                    open_legs, close_legs = _build_iron_condor(
                        strategy,
                        args.symbol,
                        days_out=args.days_out,
                        short_distance=_strike_step(args.symbol) * 10,
                        wing_width=_strike_step(args.symbol) * 5,
                        order_type=order_type,
                        smart_limit=smart_limit,
                    )

                    submit_kwargs = {}
                    if mode == "market" and args.broker == "alpaca":
                        submit_kwargs = {"is_multileg": True, "order_type": "market"}

                    open_net_best, open_net_fastest, open_net_mid = _multileg_net_snapshot(strategy, open_legs)
                    submitted = strategy.submit_order(open_legs, **submit_kwargs)
                    open_parent = submitted[0] if isinstance(submitted, list) else submitted
                    ok_open, open_snaps = _wait_for_fill(strategy, open_parent, timeout_seconds=args.timeout_seconds, drive_smart_limit=drive)

                    ok_close = False
                    close_snaps = []
                    if ok_open:
                        close_net_best, close_net_fastest, close_net_mid = _multileg_net_snapshot(strategy, close_legs)
                        submitted_close = strategy.submit_order(close_legs, **submit_kwargs)
                        close_parent = submitted_close[0] if isinstance(submitted_close, list) else submitted_close
                        ok_close, close_snaps = _wait_for_fill(strategy, close_parent, timeout_seconds=args.timeout_seconds, drive_smart_limit=drive)
                        if not ok_close:
                            # Best-effort flatten: market close legs as a package.
                            close_mkt_legs = [
                                strategy.create_order(leg.asset, leg.quantity, leg.side, order_type=Order.OrderType.MARKET, smart_limit=None)
                                for leg in close_legs
                            ]
                            mkt_kwargs = {}
                            if args.broker == "alpaca":
                                mkt_kwargs = {"is_multileg": True, "order_type": "market"}
                            submitted_mkt = strategy.submit_order(close_mkt_legs, **mkt_kwargs)
                            mkt_parent = submitted_mkt[0] if isinstance(submitted_mkt, list) else submitted_mkt
                            _wait_for_fill(strategy, mkt_parent, timeout_seconds=60, drive_smart_limit=False)

                def _count_reprices(snaps: list[_FillSnapshot]) -> int:
                    prices = [s.limit_price for s in snaps if s.limit_price is not None]
                    uniq = []
                    for p in prices:
                        if not uniq or abs(p - uniq[-1]) > 1e-9:
                            uniq.append(p)
                    return max(0, len(uniq) - 1)

                open_fill = next((s.avg_fill_price for s in reversed(open_snaps) if s.avg_fill_price is not None), None)
                close_fill = next((s.avg_fill_price for s in reversed(close_snaps) if s.avg_fill_price is not None), None)

                writer.writerow(
                    {
                        "trial": trial,
                        "broker": args.broker,
                        "symbol": args.symbol,
                        "structure": args.structure,
                        "mode": mode,
                        "open_submit_bid": open_submit_bid,
                        "open_submit_ask": open_submit_ask,
                        "open_submit_mid": open_submit_mid,
                        "open_net_best": open_net_best,
                        "open_net_fastest": open_net_fastest,
                        "open_net_mid": open_net_mid,
                        "open_status": ok_open,
                        "open_fill_price": open_fill,
                        "close_submit_bid": close_submit_bid,
                        "close_submit_ask": close_submit_ask,
                        "close_submit_mid": close_submit_mid,
                        "close_net_best": close_net_best,
                        "close_net_fastest": close_net_fastest,
                        "close_net_mid": close_net_mid,
                        "close_status": ok_close,
                        "close_fill_price": close_fill,
                        "open_reprices": _count_reprices(open_snaps),
                        "close_reprices": _count_reprices(close_snaps),
                        "open_seconds": round(open_snaps[-1].ts - open_snaps[0].ts, 2) if open_snaps else None,
                        "close_seconds": round(close_snaps[-1].ts - close_snaps[0].ts, 2) if close_snaps else None,
                        "timestamp": datetime.now().isoformat(),
                    }
                )
                f.flush()

                try:
                    strategy.cancel_open_orders()
                except Exception:
                    pass

    print(f"Wrote results to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
