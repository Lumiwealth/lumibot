#!/usr/bin/env python3
"""
One-time IBKR futures conid backfill via the TWS / Gateway API (includeExpired=True).

Why this exists:
- IBKR Client Portal (REST) cannot reliably discover expired futures conids.
- Backtesting explicit futures (and cont_future stitching) requires stable conid lookup.

Output (local cache layout; mirrors LumiBot runtime cache):
- <cache_root>/ibkr/conids.json
- <cache_root>/ibkr/future/contracts/CONID_<conid>.json

This script is designed to be resumable and safe to re-run. It only appends/updates mappings.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import queue
import re
import sys
import time
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ibapi.client import EClient  # type: ignore
from ibapi.common import TickerId  # type: ignore
from ibapi.contract import Contract  # type: ignore
from ibapi.wrapper import EWrapper  # type: ignore

_EIGHT_DIGIT = re.compile(r"^\d{8}$")
_SIX_DIGIT = re.compile(r"^\d{6}$")


@dataclass(frozen=True)
class RootSpec:
    symbol: str
    exchange: str
    currency: str


@dataclass
class ContractRow:
    conid: int
    symbol: str
    local_symbol: str
    exchange: str
    currency: str
    last_trade_date: str
    trading_class: str | None
    multiplier: float | None
    min_tick: float | None


def _parse_last_trade_date(raw: str) -> str | None:
    txt = (raw or "").strip()
    if not txt:
        return None
    if _EIGHT_DIGIT.match(txt):
        return txt
    if _SIX_DIGIT.match(txt):
        # Some contracts report YYYYMM. This is ambiguous for keying; keep it out of the registry
        # until we have a consistent policy. (US futures usually include YYYYMMDD.)
        return None
    # Sometimes it comes with suffixes like "20251219;..." – take first token.
    token = re.split(r"[^0-9]", txt)[0]
    if _EIGHT_DIGIT.match(token):
        return token
    return None


def _conid_key(*, symbol: str, exchange: str, expiration_yyyymmdd: str) -> str:
    # Matches lumibot.tools.ibkr_helper.IbkrConidKey.to_key() for futures
    return "|".join(["future", symbol, "", exchange, expiration_yyyymmdd])


def _conid_key_usd(*, symbol: str, exchange: str, expiration_yyyymmdd: str) -> str:
    # Historical caches sometimes keyed futures with quote_symbol="USD".
    return "|".join(["future", symbol, "USD", exchange, expiration_yyyymmdd])


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, indent=2), encoding="utf-8")


class _ContractDetailsApp(EWrapper, EClient):
    def __init__(self) -> None:
        EClient.__init__(self, wrapper=self)
        self._q: queue.Queue[tuple[str, Any]] = queue.Queue()
        self._connected = False

    def connectAck(self) -> None:  # noqa: N802 (ibapi naming)
        self._connected = True
        self._q.put(("connectAck", None))

    def nextValidId(self, orderId: int) -> None:  # noqa: N802
        self._q.put(("nextValidId", int(orderId)))

    def error(self, reqId: TickerId, errorCode: int, errorString: str, advancedOrderRejectJson="") -> None:  # noqa: N802
        # Surface errors; reqId may be -1 for global.
        self._q.put(("error", {"reqId": int(reqId), "code": int(errorCode), "message": str(errorString)}))

    def contractDetails(self, reqId: int, contractDetails) -> None:  # noqa: N802
        cd = contractDetails
        c = cd.contract
        payload = {
            "reqId": int(reqId),
            "conid": int(getattr(c, "conId", 0) or 0),
            "symbol": str(getattr(c, "symbol", "") or ""),
            "localSymbol": str(getattr(c, "localSymbol", "") or ""),
            "exchange": str(getattr(c, "exchange", "") or ""),
            "currency": str(getattr(c, "currency", "") or ""),
            "lastTradeDateOrContractMonth": str(getattr(c, "lastTradeDateOrContractMonth", "") or ""),
            "tradingClass": str(getattr(c, "tradingClass", "") or "") or None,
            "multiplier": getattr(cd, "mdSizeMultiplier", None),  # not always populated; keep for reference
            "minTick": getattr(cd, "minTick", None),
            # Some details fields are useful for later debugging (kept small).
            "longName": str(getattr(cd, "longName", "") or "") or None,
        }
        # NOTE: multiplier is more reliably present on the contract in some IB builds.
        raw_mult = getattr(c, "multiplier", None)
        if raw_mult not in (None, ""):
            try:
                payload["multiplier"] = float(raw_mult)
            except Exception:
                payload["multiplier"] = raw_mult
        self._q.put(("contractDetails", payload))

    def contractDetailsEnd(self, reqId: int) -> None:  # noqa: N802
        self._q.put(("contractDetailsEnd", int(reqId)))


def _preflight_contract_details(app: _ContractDetailsApp, *, timeout_s: float = 10.0, req_id: int | None = None) -> bool:
    """Return True when TWS appears to have a working secdef connection.

    If TWS is running but not logged in / not connected to IB servers, IB emits farm-broken
    messages (2110/2157) and contract details requests typically fail with error 200.
    """
    # Drain any stale messages so we don't confuse an old `contractDetailsEnd` with this preflight.
    drained = 0
    while True:
        try:
            app._q.get_nowait()
            drained += 1
        except queue.Empty:
            break

    test = RootSpec(symbol="MES", exchange="CME", currency="USD")
    if req_id is None:
        # Use a per-run id to avoid collisions with outstanding requests.
        req_id = int(time.time()) % 1_000_000 + 1000
    app.reqContractDetails(req_id, _contract_details_request(test))
    deadline = time.time() + float(timeout_s)
    saw_any_row = False
    while time.time() < deadline:
        try:
            kind, payload = app._q.get(timeout=0.5)
        except queue.Empty:
            continue
        if kind == "contractDetails" and isinstance(payload, dict) and int(payload.get("reqId") or -1) == req_id:
            row = _normalize_contract_row(payload)
            if row is not None:
                saw_any_row = True
        if kind == "contractDetailsEnd" and int(payload) == req_id:
            return saw_any_row
    return False


def _iter_roots(path: Path) -> Iterable[RootSpec]:
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sym = (row.get("symbol") or "").strip().upper()
            exch = (row.get("exchange") or "").strip().upper()
            ccy = (row.get("currency") or "").strip().upper()
            if not sym or not exch or not ccy:
                continue
            yield RootSpec(symbol=sym, exchange=exch, currency=ccy)


def _normalize_contract_row(raw: dict[str, Any]) -> ContractRow | None:
    try:
        conid = int(raw.get("conid") or 0)
    except Exception:
        return None
    if conid <= 0:
        return None

    symbol = str(raw.get("symbol") or "").strip().upper()
    local_symbol = str(raw.get("localSymbol") or "").strip().upper()
    exchange = str(raw.get("exchange") or "").strip().upper()
    currency = str(raw.get("currency") or "").strip().upper()
    ltd_raw = str(raw.get("lastTradeDateOrContractMonth") or "").strip()
    ltd = _parse_last_trade_date(ltd_raw)
    if not symbol or not exchange or not currency or not ltd:
        return None

    trading_class = raw.get("tradingClass")
    trading_class = str(trading_class).strip().upper() if trading_class else None

    raw_tick = raw.get("minTick")
    min_tick: float | None
    try:
        min_tick = float(raw_tick) if raw_tick not in (None, "") else None
    except Exception:
        min_tick = None

    raw_mult = raw.get("multiplier")
    multiplier: float | None
    try:
        multiplier = float(raw_mult) if raw_mult not in (None, "") else None
    except Exception:
        multiplier = None

    return ContractRow(
        conid=conid,
        symbol=symbol,
        local_symbol=local_symbol,
        exchange=exchange,
        currency=currency,
        last_trade_date=ltd,
        trading_class=trading_class,
        multiplier=multiplier,
        min_tick=min_tick,
    )


def _contract_details_request(root: RootSpec) -> Contract:
    c = Contract()
    c.secType = "FUT"
    c.symbol = root.symbol
    c.exchange = root.exchange
    c.currency = root.currency
    c.includeExpired = True
    return c


def _write_contract_info(cache_root: Path, row: ContractRow) -> None:
    path = cache_root / "ibkr" / "future" / "contracts" / f"CONID_{int(row.conid)}.json"
    payload: dict[str, Any] = {
        "conid": int(row.conid),
        "symbol": row.symbol,
        "localSymbol": row.local_symbol,
        "exchange": row.exchange,
        "currency": row.currency,
        "lastTradeDateOrContractMonth": row.last_trade_date,
    }
    if row.trading_class:
        payload["tradingClass"] = row.trading_class
    if row.multiplier is not None:
        payload["multiplier"] = row.multiplier
    if row.min_tick is not None:
        payload["minTick"] = row.min_tick
    _write_json(path, payload)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Backfill IBKR futures conids via TWS API (includeExpired=True).")
    parser.add_argument(
        "--roots-csv",
        default="data/ibkr_symbol_lookup/us_futures_roots.csv",
        help="CSV of futures roots (symbol,exchange,currency).",
    )
    parser.add_argument(
        "--cache-root",
        default="data/ibkr_tws_backfill_cache",
        help="Cache root directory to write (will contain ibkr/conids.json).",
    )
    parser.add_argument("--host", default=os.environ.get("INTERACTIVE_BROKERS_IP", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("INTERACTIVE_BROKERS_PORT") or "7497"))
    parser.add_argument("--client-id", type=int, default=int(os.environ.get("INTERACTIVE_BROKERS_CLIENT_ID") or "991"))
    parser.add_argument("--sleep-s", type=float, default=0.15, help="Sleep between root requests to avoid pacing.")
    parser.add_argument("--max-roots", type=int, default=0, help="Optional cap for testing (0 = no cap).")
    parser.add_argument("--timeout-s", type=float, default=20.0, help="Per-root timeout waiting for TWS responses.")
    parser.add_argument(
        "--wait-ready-s",
        type=float,
        default=0.0,
        help="If contract discovery isn't ready, keep retrying preflight until this many seconds elapse (0 = no wait).",
    )
    args = parser.parse_args(argv)

    roots_csv = Path(args.roots_csv)
    cache_root = Path(args.cache_root)
    conids_path = cache_root / "ibkr" / "conids.json"

    roots = list(_iter_roots(roots_csv))
    if args.max_roots and args.max_roots > 0:
        roots = roots[: int(args.max_roots)]
    if not roots:
        print(f"No roots found in {roots_csv}", file=sys.stderr)
        return 2

    mapping: dict[str, int] = {}
    existing = _read_json(conids_path)
    if isinstance(existing, dict):
        for k, v in existing.items():
            try:
                iv = int(v)
            except Exception:
                continue
            if iv > 0:
                mapping[str(k)] = iv

    app = _ContractDetailsApp()
    app.connect(args.host, int(args.port), clientId=int(args.client_id))

    try:
        import threading

        t = threading.Thread(target=app.run, daemon=True)
        t.start()
    except Exception:
        try:
            if app.isConnected():
                app.disconnect()
        except Exception:
            pass
        return 3

    try:
        # Wait for a basic handshake.
        deadline = time.time() + 10.0
        saw_next_id = False
        while time.time() < deadline:
            try:
                kind, _payload = app._q.get(timeout=0.5)
            except queue.Empty:
                continue
            if kind == "nextValidId":
                saw_next_id = True
                break
        if not saw_next_id:
            print(
                f"Failed to handshake with TWS on {args.host}:{args.port}. "
                "Ensure TWS/Gateway is running and API access is enabled.",
                file=sys.stderr,
            )
            return 3

        ready_deadline = time.time() + float(args.wait_ready_s or 0.0)
        while True:
            if _preflight_contract_details(app, timeout_s=12.0, req_id=None):
                break
            if args.wait_ready_s and time.time() < ready_deadline:
                print(
                    json.dumps(
                        {
                            "status": "waiting_for_tws_ready",
                            "detail": "TWS API reachable but secdef/contract discovery not ready yet; retrying soon.",
                            "retry_in_s": 10,
                        }
                    ),
                    file=sys.stderr,
                    flush=True,
                )
                time.sleep(10)
                continue
            print(
                "TWS API is reachable, but contract discovery failed. This usually means TWS is not connected "
                "to IB servers (or API access is not fully enabled). Please verify:\n"
                "- TWS is fully logged in (not sitting on the login screen)\n"
                "- Lower-right status is GREEN (connected)\n"
                "- Global Configuration → API → Settings → 'Enable ActiveX and Socket Clients' is ON\n"
                "- Port matches (commonly 7497 for paper, 7496 for live)\n",
                file=sys.stderr,
            )
            return 4

        # Use a req_id range that won't collide with preflight.
        req_id = 1000
        roots_done = 0
        contracts_written = 0
        keys_written = 0
        errors: list[dict[str, Any]] = []

        for root in roots:
            req_id += 1
            contract = _contract_details_request(root)
            app.reqContractDetails(req_id, contract)

            per_root: list[ContractRow] = []
            end_seen = False
            timeout_at = time.time() + float(args.timeout_s)
            while time.time() < timeout_at:
                try:
                    kind, payload = app._q.get(timeout=0.5)
                except queue.Empty:
                    continue
                if kind == "error":
                    # Keep going; some roots will be entitlement-locked.
                    if isinstance(payload, dict):
                        errors.append(payload)
                    continue
                if kind == "contractDetailsEnd" and int(payload) == int(req_id):
                    end_seen = True
                    break
                if kind == "contractDetails" and isinstance(payload, dict) and int(payload.get("reqId") or -1) == int(req_id):
                    row = _normalize_contract_row(payload)
                    if row is not None:
                        per_root.append(row)

            if not end_seen:
                errors.append({"reqId": int(req_id), "code": -1, "message": f"timeout waiting for contractDetailsEnd for {root}"})

            # Persist per-root rows.
            for row in per_root:
                exch = (root.exchange or row.exchange).strip().upper()
                key = _conid_key(symbol=row.symbol, exchange=exch, expiration_yyyymmdd=row.last_trade_date)
                key_usd = _conid_key_usd(symbol=row.symbol, exchange=exch, expiration_yyyymmdd=row.last_trade_date)
                for k in (key, key_usd):
                    prev = mapping.get(k)
                    mapping[k] = int(row.conid)
                    if prev != int(row.conid):
                        keys_written += 1
                _write_contract_info(cache_root, row)
                contracts_written += 1

            roots_done += 1
            if roots_done % 10 == 0:
                _write_json(conids_path, {k: int(v) for k, v in sorted(mapping.items())})
                print(
                    json.dumps(
                        {
                            "roots_done": roots_done,
                            "roots_total": len(roots),
                            "conid_keys": len(mapping),
                            "new_or_changed_keys": keys_written,
                            "contracts_written": contracts_written,
                            "last_root": {"symbol": root.symbol, "exchange": root.exchange, "currency": root.currency},
                        }
                    )
                )

            time.sleep(float(args.sleep_s))

        _write_json(conids_path, {k: int(v) for k, v in sorted(mapping.items())})
        print(
            json.dumps(
                {
                    "cache_root": str(cache_root.resolve()),
                    "conids_json": str(conids_path.resolve()),
                    "roots_total": len(roots),
                    "conid_keys_total": len(mapping),
                    "contracts_written": contracts_written,
                    "keys_new_or_changed": keys_written,
                    "errors_count": len(errors),
                    "sample_errors": errors[:10],
                },
                indent=2,
            )
        )
        return 0
    finally:
        try:
            if app.isConnected():
                app.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
