"""Normalized USDC history with explicit retrieval and classification coverage."""

import datetime as dt
import math

import requests

from lumibot.entities import CashEvent, CashEventSync

from .models import decimal
from .provider import MetaMaskError, address
from .venue import ledger_identity

_EPOCH = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
_MILLISECOND = dt.timedelta(milliseconds=1)


def _cash_event(row, *, wallet, network, ledger):
    delta = row["delta"]
    kind = delta["type"]
    amount = decimal(delta.get("usdc"))
    if "token" in delta and delta["token"] != "USDC":
        raise ValueError("Non-USDC ledger assets need separate valuation")
    if "nativeTokenFee" in delta and decimal(delta["nativeTokenFee"]) != 0:
        raise ValueError("Native-token transfer fees need separate valuation")
    external, event_type = True, "deposit"
    if ledger == "userFunding":
        coin = delta.get("coin")
        if kind != "funding" or not isinstance(coin, str) or not coin or ":" in coin:
            raise ValueError("Only default-DEX USDC funding is qualified")
        external, event_type = False, "other_cash"
    else:
        if amount < 0:
            raise ValueError("Capital ledger amounts must be nonnegative")
        if "fee" in delta and decimal(delta["fee"]) != 0:
            raise ValueError("Fee-bearing capital net/gross classification is unqualified")
        if kind in {"withdraw", "internalTransfer"} and "fee" not in delta:
            raise ValueError("Withdrawal or transfer fee evidence is missing")
        if kind == "withdraw":
            amount, event_type = amount.copy_negate(), "withdrawal"
        elif kind in {"internalTransfer", "subAccountTransfer"}:
            source, destination = address(delta.get("user")), address(delta.get("destination"))
            if (source == wallet) == (destination == wallet):
                raise ValueError("Transfer must identify exactly one selected-wallet leg")
            if source == wallet:
                amount, event_type = amount.copy_negate(), "withdrawal"
        elif kind != "deposit":
            # shortcut: historical account-mode changes need boundary-aware capital classification.
            raise ValueError("Ledger category needs separately qualified capital classification")
    number = float(amount)
    if not math.isfinite(number) or (amount != 0 and number == 0):
        raise ValueError("Cash amount cannot be represented by the standard event contract")
    return CashEvent(
        broker_name="metamask",
        broker_event_id=f"{network}:{wallet}:perp:{ledger}:{ledger_identity(row)}",
        event_type=event_type,
        amount=number,
        occurred_at=_EPOCH + row["time"] * _MILLISECOND,
        currency="USDC",
        raw_type=kind,
        raw_subtype=delta.get("coin") if ledger == "userFunding" else None,
        description=f"Hyperliquid {kind}",
        is_external_cash_flow=external,
    )


def cash_event_sync(client, mode, *, since=None, limit=5000):
    if limit is not None and (type(limit) is not int or limit < 1):
        raise ValueError("Cash-event limit must be a positive integer or None")
    now = dt.datetime.now(dt.timezone.utc)
    end = now.replace(microsecond=now.microsecond // 1000 * 1000)
    start = since if since is not None else end - dt.timedelta(days=60)
    if not isinstance(start, dt.datetime) or start.tzinfo is None or start.utcoffset() is None:
        raise ValueError("Cash history requires an aware datetime")
    start = start.astimezone(dt.timezone.utc)
    if not _EPOCH <= start < end:
        raise ValueError("Cash history requires a past, increasing window")
    wallet = address(client.config.get("WALLET_ADDRESS"))
    network = client.network
    arguments = {
        "events": (),
        "broker_name": "metamask",
        "account_id": f"{network}:{wallet}:{mode}",
        "mode": "paper" if network == "testnet" else "live",
        "start": start,
        "end": end,
    }
    state, reason, events = "unsupported", "onchain_cash_history_unsupported", []
    if mode == "perp":
        try:
            # Venue uses inclusive milliseconds; the receipt uses [start, end).
            rows = client.cash_history((start - _EPOCH) // _MILLISECOND, (end - _EPOCH) // _MILLISECOND - 1)
        except (MetaMaskError, ValueError, requests.RequestException):
            state, reason = "unavailable", "wallet_ledger_unavailable"
        else:
            state, reason = "complete", None
            for ledger, entries in rows.items():
                for row in entries:
                    try:
                        event = _cash_event(row, wallet=wallet, network=network, ledger=ledger)
                    except (MetaMaskError, ValueError, OverflowError):
                        state, reason = "partial", "unresolved_ledger_transaction"
                        continue
                    if start <= event.occurred_at < end:
                        events.append(event)
            events.sort(key=lambda event: (event.occurred_at, event.event_id))
            if limit is not None and len(events) > limit:
                events = events[-limit:]
                state, reason = "partial", "cash_event_limit"
    return CashEventSync(
        **{**arguments, "events": tuple(events)},
        state=state,
        reason_code=reason,
        fetched_at=dt.datetime.now(dt.timezone.utc),
    )
