"""Real-price proof paths for the public examples.

These paths do not call a model. The default agent path stays in place when
execution_mode is omitted.
"""

from __future__ import annotations

import json
from datetime import timedelta
from typing import Any

from lumibot.components.agents.builtins import _bind_submit_multileg_order
from lumibot.components.agents.web_tools import WebClient
from lumibot.fundamentals.sec import DEFAULT_SEC_USER_AGENT


def price_rule_once(strategy: Any, symbol: str) -> None:
    """Buy one share of a named symbol, then stop. One share cannot wreck the account."""
    if getattr(strategy, "_price_rule_done", False):
        return
    price = strategy.get_last_price(symbol)
    if price is None or float(price) <= 0:
        strategy.log_message(f"Price rule skip, no price: {symbol}")
        return
    strategy.log_message(f"Price rule buy {symbol} at {price}")
    strategy.submit_order(strategy.create_order(symbol, 1, "buy"))
    strategy._price_rule_done = True


def _minute_bar_close(strategy: Any, symbol: str):
    """Read the latest completed minute bar. Daily last-price is not this proof."""
    bars = strategy.get_historical_prices(symbol, 2, "minute", include_after_hours=False)
    frame = getattr(bars, "df", None)
    if frame is None or getattr(frame, "empty", True):
        return None
    try:
        price = float(frame["close"].iloc[-1])
    except (TypeError, ValueError, KeyError, IndexError):
        return None
    if price != price or price <= 0:
        return None
    return price


def minute_proof_round_trip(strategy: Any, symbol: str) -> None:
    """Buy one share on a minute bar, then sell it after a later minute bar."""
    state = getattr(strategy, "_minute_proof_state", "buy")
    if state == "done":
        return
    price = _minute_bar_close(strategy, symbol)
    if price is None:
        strategy.log_message(f"Minute proof skip, no price: {symbol}")
        return
    if state == "buy":
        strategy.log_message(f"Minute proof buy {symbol} at {price}")
        strategy.submit_order(strategy.create_order(symbol, 1, "buy"))
        strategy._minute_proof_state = "sell"
        return
    position = strategy.get_position(symbol)
    quantity = getattr(position, "quantity", 0) or 0
    if quantity <= 0:
        return
    strategy.log_message(f"Minute proof sell {symbol} at {price}")
    strategy.submit_order(strategy.create_order(symbol, 1, "sell"))
    strategy._minute_proof_state = "done"


def edgar_hold(strategy: Any, symbol: str = "AAPL") -> None:
    """Read a real EDGAR filing, then hold."""
    if getattr(strategy, "_source_proof_logged", False):
        return
    filings = strategy.fundamentals.get_filings(symbol, form="10-K", limit=1)
    rows = filings.get("filings") or []
    accession = rows[0].get("accession_number") if rows else None
    section_ok = False
    if accession:
        section = strategy.fundamentals.get_filing_section(
            symbol,
            accession_number=accession,
            section="1",
            primary_document=rows[0].get("primary_document"),
        )
        section_ok = bool(section.get("ok") or section.get("text"))
    strategy.log_message(
        f"EDGAR filing read {symbol} 10-K accession {accession} "
        f"section_ok {section_ok} action hold"
    )
    strategy._source_proof_logged = True


def ackman_page_hold(strategy: Any) -> None:
    """Fetch a real Pershing Square source, then hold."""
    if getattr(strategy, "_source_proof_logged", False):
        return
    url = (
        "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany"
        "&CIK=0001336528&type=13F-HR&count=10&output=atom"
    )
    client = WebClient(timeout_seconds=30, max_response_bytes=2_000_000)
    try:
        response = client.request(
            "GET",
            url,
            headers={"User-Agent": DEFAULT_SEC_USER_AGENT, "Accept": "application/atom+xml"},
        )
        text = str(response.get("text") or "")
        matched = "PERSHING" in text.upper() or "ACKMAN" in text.upper()
        if not response.get("ok") or not matched:
            url = "https://pershingsquareholdings.com/"
            response = client.request("GET", url, headers={"User-Agent": DEFAULT_SEC_USER_AGENT})
            text = str(response.get("text") or "")
            matched = response.get("ok") and ("PERSHING" in text.upper() or "ACKMAN" in text.upper())
    finally:
        client.close()
    strategy.log_message(
        f"Ackman source fetch {url} matched {matched} "
        f"bytes {response.get('content_length')} action hold"
    )
    strategy._source_proof_logged = True


def submit_multileg_proof(
    strategy: Any,
    open_legs: list[dict[str, Any]],
    close_legs: list[dict[str, Any]],
    hold_days: int = 5,
) -> None:
    """Open with orders_submit_multileg, hold so prices can move, then close.

    Zero-day proofs pass hold_days=0 so the close happens on the next bar,
    before that expiration is gone.
    """
    state = getattr(strategy, "_multileg_proof_state", "open")
    if state == "done":
        return
    if state == "hold":
        opened_at = getattr(strategy, "_multileg_proof_opened_at", None)
        now = strategy.get_datetime()
        if opened_at is not None and now < opened_at + timedelta(days=hold_days):
            return
        state = "close"
    legs = open_legs if state == "open" else close_legs
    tool = _bind_submit_multileg_order(strategy, strategy.agents)
    strategy.log_message(f"orders_submit_multileg {state}")
    result = tool.function(legs_json=json.dumps(legs), price_style="market")
    strategy.log_message(f"orders_submit_multileg {state} submitted {len(result.get('submitted') or [])}")
    if state == "open":
        strategy._multileg_proof_opened_at = strategy.get_datetime()
        strategy._multileg_proof_state = "hold"
    else:
        strategy._multileg_proof_state = "done"
