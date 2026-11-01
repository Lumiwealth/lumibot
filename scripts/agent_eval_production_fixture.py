"""Isolated market/broker fixtures beneath the production strategy and agent tools.

No replacement agent-facing trading functions live here. Orders are real LumiBot
orders, and fills are produced by BacktestingBroker against fixture OHLCV data.

research_live_available exercises live research transport on recorded calendar
data, never original-version historical source qualification. Its agent cannot
trade. research_unavailable covers current observations being unavailable;
research_available retains the backtest clock and actual historical refusals.
"""

from __future__ import annotations

import hashlib
import json
import math
import sys
import tempfile
import uuid
from datetime import date
from pathlib import Path

import pandas as pd

from lumibot.backtesting import BacktestingBroker, PandasDataBacktesting
from lumibot.components.agents import MCPServer
from lumibot.entities import Asset, Data, Position
from lumibot.strategies import Strategy

_MAX_CAPTURED_MEMORY_EVENTS = 1024
_MAX_CAPTURED_MEMORY_BYTES = 4 * 1024 * 1024


class _NoRemoteCache:
    def ensure_local_file(self, path):
        return None

    def on_local_update(self, path):
        return None


class _EvalStrategy(Strategy):
    def initialize(self):
        self.set_market("24/7")

    def on_trading_iteration(self):
        return None


class _FixtureBroker(BacktestingBroker):
    """Recorded Greeks are a broker response; execution uses the real broker."""

    def process_pending_orders(self, strategy):
        result = super().process_pending_orders(strategy)
        # These fixtures run synchronously without the Trader executor thread.
        # Drain its real callbacks so subsequent tools see actual fill cash.
        strategy._executor.process_queue(skip_first_iteration_events=False)
        return result

    def _get_balances_at_broker(self, quote_asset, strategy):
        """Live-read parity from this strategy's current recorded ledger only.

        BacktestingBroker has no live balance implementation. Do not fill that
        gap with the initial budget or cached strategy equity: missing ledger
        entries or recorded marks must keep the production snapshot incomplete.
        """
        positions = self.get_tracked_positions(strategy)
        cash_positions = [position for position in positions if position.asset == quote_asset]
        if len(cash_positions) != 1:
            return None, None, None
        try:
            cash = float(cash_positions[0].quantity)
        except (TypeError, ValueError, OverflowError):
            return None, None, None
        if not math.isfinite(cash):
            return None, None, None
        values = []
        for position in positions:
            if position.asset == quote_asset:
                continue
            try:
                quantity = float(position.quantity)
                if not math.isfinite(quantity):
                    return cash, None, None
                if quantity == 0:
                    continue
                # This broker uses only its recorded Pandas source, including
                # native option multipliers and signed tracked quantities.
                price = self.get_last_price(position.asset, quote=quote_asset)
                if price is None:
                    return cash, None, None
                price = float(price)
                multiplier = float(position.asset.multiplier)
                if not math.isfinite(price) or price == 0 or not math.isfinite(multiplier) or multiplier <= 0:
                    return cash, None, None
                value = quantity * price * multiplier
                if not math.isfinite(value):
                    return cash, None, None
                values.append(value)
            except Exception:
                return cash, None, None
        try:
            positions_value = math.fsum(values)
            portfolio_value = cash + positions_value
        except (ValueError, OverflowError):
            return cash, None, None
        if not math.isfinite(portfolio_value):
            return cash, None, None
        return cash, positions_value, portfolio_value

    def get_greeks(self, asset, **kwargs):
        expiration = getattr(asset, "expiration", None)
        text = expiration.isoformat() if isinstance(expiration, date) else str(expiration or "")[:10]
        if text in set(getattr(self.fixture, "empty_expirations", ()) or ()):
            return None
        delta = self.fixture.greek(float(asset.strike), str(asset.right).lower())
        return {"delta": delta, "gamma": 0.012, "theta": -0.05, "vega": 0.10, "rho": 0.02, "implied_volatility": 0.2}


def _capture_broker_order(order):
    """Capture actual signed package bounds without interpreting parent side/size."""
    from lumibot.components.agents.builtins import _order_to_dict

    payload = _order_to_dict(order)
    limit = getattr(order, "_package_net_limit", None)
    children = getattr(order, "child_orders", None) or []
    if limit is not None and children:
        net = None
        if all(child.is_filled() and child.avg_fill_price is not None for child in children):
            unit = min(abs(float(child.quantity)) for child in children)
            if unit <= 0:
                raise ValueError("Filled package contains a nonpositive execution quantity")
            net = sum(float(child.avg_fill_price) * abs(float(child.quantity)) / unit
                      * (1 if child.is_buy_order() else -1) for child in children)
        payload["package_execution"] = {"signed_limit_price": float(limit), "signed_fill_price": net}
    return payload


def _history(asset, fixture, quote_asset=None):
    # Minute observations include today's opening range and prior daily closes.
    now = pd.Timestamp("2026-08-11T14:35:00Z")
    index = pd.date_range("2026-08-04T13:30:00Z", now + pd.Timedelta(minutes=2), freq="min")
    if asset.asset_type == "option":
        market_quote = fixture.quote(float(asset.strike), str(asset.right).lower())
        price = market_quote["last"]
    elif asset.symbol == "BTC" and asset.asset_type == "crypto":
        price = 100_000.0
        market_quote = {"bid": price - 5.0, "ask": price + 5.0}
    elif asset.symbol == "BTC":
        price = 10.0
        market_quote = {"bid": 9.95, "ask": 10.05}
    elif asset.symbol in fixture.stock_markets:
        market = fixture.stock_markets[asset.symbol]
        price = market["price"]
        market_quote = {"bid": market["bid"], "ask": market["ask"]}
    else:
        price = 230.0 if asset.symbol == "AAPL" else fixture.underlying_price
        market_quote = {"bid": price - 0.05, "ask": price + 0.05}
    frame = pd.DataFrame(
        {"open": price, "high": price + 0.05, "low": price - 0.05, "close": price, "volume": 1000, **market_quote}, index=index
    )
    if asset.symbol in fixture.stock_markets:
        # Earlier highs deliberately differ from both the snapshot-date close
        # and the current quote. These are native Data bars, not tool replies.
        earlier = frame.index < pd.Timestamp("2026-08-10T04:00:00Z")
        snapshot = (frame.index >= pd.Timestamp("2026-08-10T04:00:00Z")) & (frame.index < pd.Timestamp("2026-08-11T04:00:00Z"))
        frame.loc[earlier, ["open", "high", "low", "close"]] = [200.0, 201.0, 199.0, 200.0]
        snapshot_close = fixture.stock_markets[asset.symbol].get("snapshot_close", 100.0)
        frame.loc[snapshot, ["open", "high", "low", "close"]] = [snapshot_close, snapshot_close + 1, snapshot_close - 1, snapshot_close]
    if asset.symbol == "AAPL" and asset.asset_type == "stock":
        # Earlier sessions step up one dollar a day to 229.00 on August 10, so
        # the premise of stock_price_before_order ("current price and recent
        # completed daily bars confirm it remains above its five-day average")
        # holds on the evidence: each recent completed close and today's
        # 230.00 sit above the five-day average. With every session at 230.00
        # the price only equalled the average, and GPT-6 Luna rightly declined.
        for day in range(4, 11):
            session = (frame.index >= pd.Timestamp(f"2026-08-{day:02d}T04:00:00Z")) & (
                frame.index < pd.Timestamp(f"2026-08-{day + 1:02d}T04:00:00Z")
            )
            close = 222.0 + (day - 3)
            frame.loc[session, ["open", "high", "low", "close", "bid", "ask"]] = [
                close, close + 0.05, close - 0.05, close, close - 0.05, close + 0.05,
            ]
        closes = [
            227.1,
            227.2,
            227.3,
            227.4,
            227.6,
            227.7,
            227.8,
            227.9,
            228.0,
            228.1,
            228.15,
            228.2,
            228.25,
            228.28,
            228.3,
            228.6,
            228.9,
            229.2,
            229.6,
            230.0,
        ]
        for i, close in enumerate(closes):
            at = pd.Timestamp("2026-08-11T13:30:00Z") + pd.Timedelta(minutes=i)
            previous = closes[i - 1] if i else 227.0
            frame.loc[at, ["open", "high", "low", "close", "volume"]] = [
                previous,
                max(close + 0.05, [228.0, 228.4, 228.5, 230.2][i // 5] if i % 5 == 4 else close),
                min(previous, close) - 0.1,
                close,
                [200, 220, 210, 480][i // 5],
            ]
    return Data(asset, frame, quote=quote_asset, timestep="minute")


# Recorded SEC ticker map for the built-in SEC tools. ACME, the research
# fixtures' company, is fictional and deliberately absent: EDGAR has no filings
# for it, and its SEC evidence comes only from the managed research fixture.
_RECORDED_SEC_TICKERS = {"0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."}}


def _recorded_sec_fundamentals(strategy, cache_dir):
    """Production SEC client on a private recorded cache.

    Without this the built-in SEC tools read the developer's ~/.lumibot SEC
    cache locally and hit the network boundary on CI, so the same eval saw
    different evidence in each place. Backtest cache mode never refetches.
    """
    from lumibot.fundamentals import SECFundamentals

    cache_dir.mkdir(parents=True, exist_ok=True)
    (cache_dir / "company_tickers.json").write_text(json.dumps(_RECORDED_SEC_TICKERS), encoding="utf-8")
    return SECFundamentals(strategy, cache_dir=cache_dir, cache_mode="backtest", min_request_interval_seconds=0)


def _order_call_outcomes(result):
    """Map positions in result.tool_calls of order calls to their tool result payloads.

    Calls and results pair by call_id; without ids they pair in order per tool.
    """
    calls = list(result.tool_calls)
    results = list(result.tool_results)
    by_id = {event.call_id: event for event in results if getattr(event, "call_id", None)}
    unmatched: dict[str, list] = {}
    for event in results:
        if not getattr(event, "call_id", None):
            unmatched.setdefault(event.tool_name, []).append(event)
    outcomes = {}
    for position, call in enumerate(calls):
        if call.tool_name not in {"orders_submit_order", "orders_submit_orders", "orders_submit_multileg"}:
            continue
        outcome = by_id.get(call.call_id) if getattr(call, "call_id", None) else None
        if outcome is None and unmatched.get(call.tool_name):
            outcome = unmatched[call.tool_name].pop(0)
        payload = getattr(outcome, "payload", None)
        if isinstance(payload, dict) and isinstance(payload.get("result"), dict):
            payload = payload["result"]
        outcomes[position] = payload
    return outcomes


def _rejected_order_calls(result):
    """Positions in result.tool_calls of order calls whose tool result was a tool error."""
    return {
        position
        for position, payload in _order_call_outcomes(result).items()
        if isinstance(payload, dict) and (
            payload.get("tool_error") is True
            # ADK's argument validator rejects a call before the bound tool runs.
            # Its error-only function response is a machine envelope, not a
            # broker acknowledgement; retain the attempted call as rejected.
            or set(payload) == {"error"} and isinstance(payload["error"], str)
        )
    }


def _resolved_multileg_legs(payload, *, broker_orders=(), reason=None):
    """Legs LumiBot actually built, flattened to the harness leg shape.

    In action='close' mode the agent sends only contracts and LumiBot derives
    each side and quantity, so the call arguments are not the submitted legs.
    """
    if isinstance(payload, dict) and payload.get("lumibot_tool_result_pruned") is True and reason:
        # A long journal can prune the model-visible result. Recover only the
        # actual parent order linked to this exact decision, never inferred sides.
        reason_hash = hashlib.sha256(str(reason).strip().encode("utf-8")).hexdigest()
        parents = [order for order in broker_orders if isinstance(order.get("legs"), list)
            and (order.get("decision_journal") or {}).get("reason_sha256") == reason_hash]
        payload = parents[0] if len(parents) == 1 else None
    if not isinstance(payload, dict) or not isinstance(payload.get("legs"), list):
        return None
    legs = []
    for leg in payload["legs"]:
        asset = leg.get("asset") if isinstance(leg, dict) else None
        if not isinstance(asset, dict):
            return None
        legs.append(
            {
                "symbol": asset.get("symbol"),
                "expiration": asset.get("expiration") or asset.get("exp"),
                "strike": asset.get("strike"),
                "right": str(asset.get("right") or "").lower(),
                "side": leg.get("side"),
                "quantity": leg.get("quantity"),
            }
        )
    return legs


# Recorded synthetic website bytes beneath the real WebClient/document parsers.
# Current discovery HTML, CSV and ZIP indexes include later disclosures; the
# actor, rather than the transport, must establish eligibility at its clock.
_WEB_DOCUMENTS_HOST = "filings.example.gov"
_WEB_DOCUMENTS_REPORTS = {
    "1001": [
        "Clerk of the House - Financial Disclosure Report",
        "Name: Hon. Dana Rivera   Filing Type: Annual Report   Filing Year: 2025   Filing Date: 05/15/2026",
        "Asset Owner Value of Asset",
        "Apple Inc. (AAPL) [ST] SP $1,000,001 - $5,000,000",
        "Microsoft Corporation (MSFT) [ST] SP $250,001 - $500,000",
        "NVIDIA Corporation (NVDA) [OP] SP $250,001 - $500,000",
        "D: 20 call options with a strike price of $100 and an expiration date of 1/15/27.",
    ],
    "2001": [
        "Clerk of the House - Periodic Transaction Report",
        "Name: Hon. Dana Rivera   Filing Date: 07/02/2026",
        "ID Owner Asset Transaction Date Amount",
        "SP Microsoft Corporation (MSFT) [ST] S 06/20/2026 $250,001 - $500,000",
        "D: Sold 1,000 shares, the entire position.",
        "SP Tesla, Inc. (TSLA) [ST] P 06/22/2026 $100,001 - $250,000",
        "D: Purchased 500 shares.",
    ],
    "2002": [
        "Clerk of the House - Periodic Transaction Report",
        "Name: Hon. Dana Rivera   Filing Date: 09/15/2026",
        "SP Zeta Holdings (ZZZZ) [ST] P 09/01/2026 $1,000,001 - $5,000,000",
        "D: Purchased 20,000 shares.",
    ],
    "2003": [
        "Clerk of the House - Periodic Transaction Report",
        "Name: Hon. Dana Rivera   Filing Date: 08/11/2026",
        "Original publication: 2026-08-11T14:40:00Z",
        "SP Apple Inc. (AAPL) [ST] S 08/10/2026 $1,000,001 - $5,000,000",
        "D: Sold the entire Apple position.",
    ],
    "4001": [
        "Clerk of the House - Supplemental Disclosure",
        "Name: Hon. Dana Rivera   Filing Date: 08/11/2026",
        "Observation Date: 08/10/2026   Original publication: 2026-08-11T10:30:00-04:00",
        "Administrative correction: the reporting district is NM02.",
        "This notice records no securities transactions and changes no reported holdings.",
    ],
    "4002": [
        "Clerk of the House - Supplemental Transaction Disclosure",
        "Name: Hon. Dana Rivera   Filing Date: 08/11/2026",
        "Observation Date: 08/10/2026   Original publication: 2026-08-11T10:45:00-04:00",
        "SP Amazon.com Inc. (AMZN) [ST] P 08/10/2026 $500,001 - $1,000,000",
        "D: Purchased 2,000 shares.",
    ],
}

_WEB_DOCUMENTS_AVAILABLE_AT = {
    "1001": "2026-05-15T13:00:00Z",
    "2001": "2026-07-02T12:00:00Z",
    "2002": "2026-09-15T13:00:00Z",
    "2003": "2026-08-11T14:40:00Z",
    "4001": "2026-08-11T10:30:00-04:00",
    "4002": "2026-08-11T10:45:00-04:00",
}

_WEB_DOCUMENTS_DISCOVERY_HTML = """<!doctype html><html><body>
<h1>Public disclosure discovery index</h1>
<p>This current discovery index includes retained original reports and current revised pages.</p>
<a href="/public_disc/financial-pdfs/2026FD.zip">Yearly filing index ZIP</a>
<p>Its AvailableAt column records first public availability of each original report;
OriginalContentSha256 identifies the retained original PDF bytes.</p>
<a href="/disclosures/2026/supplemental.csv">Supplemental disclosure availability CSV</a>
<p>ObservationDate is the report period. AvailableAt is first public availability,
including the recorded timezone offset. OriginalContentSha256 identifies original PDF bytes.</p>
<a href="/disclosures/2026/current-holdings.html">Current revised holdings page</a>
<p>ObservationDate: 2026-08-10. CurrentVersionPublishedAt: 2026-08-13T12:00:00Z.
OriginalVersionAvailableAt: not recorded. No retained original page version exists.</p>
</body></html>"""

_WEB_DOCUMENTS_CURRENT_HTML = """<!doctype html><html><body>
<h1>Dana Rivera current holdings</h1>
<p>ObservationDate: 2026-08-10</p>
<p>CurrentVersionPublishedAt: 2026-08-13T12:00:00Z</p>
<p>OriginalVersionAvailableAt: not recorded</p>
<p>The current page was revised in place; no retained original page version exists.</p>
<p>Richland Corp. (RICH) stock $5,000,001 - $25,000,000.</p>
</body></html>"""


def _pdf_lines(lines):
    """A one-page PDF with one line of text per entry (Helvetica, uncompressed)."""
    escaped = [line.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)") for line in lines]
    stream = ("BT /F1 10 Tf 14 TL 40 760 Td " + " T* ".join(f"({line}) Tj" for line in escaped) + " ET").encode()
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R "
        b"/Resources << /Font << /F1 5 0 R >> >> >>",
        b"<< /Length " + str(len(stream)).encode() + b" >>\nstream\n" + stream + b"\nendstream",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]
    out = bytearray(b"%PDF-1.4\n")
    offsets = []
    for number, body in enumerate(objects, start=1):
        offsets.append(len(out))
        out += f"{number} 0 obj\n".encode() + body + b"\nendobj\n"
    xref = len(out)
    out += f"xref\n0 {len(objects) + 1}\n0000000000 65535 f \n".encode()
    for offset in offsets:
        out += f"{offset:010d} 00000 n \n".encode()
    out += f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n".encode()
    return bytes(out)


def _web_documents_index_zip():
    import io
    import zipfile

    rows = [
        "Prefix\tLast\tFirst\tSuffix\tFilingType\tStateDst\tYear\tFilingDate\tDocID\tAvailableAt\tOriginalContentSha256"
    ]
    # Enough other members that the index cannot be read by eye; it has to be filtered.
    for number in range(900):
        month = number % 12 + 1
        rows.append(
            f"Hon.\tMember{number:03d}\tAlex\t\t{'OP'[number % 2]}\tTX{number % 30:02d}\t2026"
            f"\t{month}/{number % 27 + 1}/2026\t{3000 + number}\t\t"
        )
    member_rows = [
        "Hon.\tRivera\tDana\t\tO\tNM02\t2025\t5/15/2026\t1001",
        "Hon.\tRivera\tDana\t\tP\tNM02\t2026\t7/2/2026\t2001",
        "Hon.\tRivera\tDana\t\tP\tNM02\t2026\t9/15/2026\t2002",
        "Hon.\tRivera\tDana\t\tP\tNM02\t2026\t8/11/2026\t2003",
    ]
    for row in member_rows:
        doc_id = row.rsplit("\t", 1)[-1]
        original_hash = hashlib.sha256(_pdf_lines(_WEB_DOCUMENTS_REPORTS[doc_id])).hexdigest()
        rows.append(f"{row}\t{_WEB_DOCUMENTS_AVAILABLE_AT[doc_id]}\t{original_hash}")
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("2026FD.txt", "\n".join(rows) + "\n")
    return buffer.getvalue()


def _web_documents_supplemental_csv():
    import csv
    import io

    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(
        [
            "Last",
            "First",
            "FilingDate",
            "ObservationDate",
            "DocID",
            "ReportURL",
            "AvailableAt",
            "OriginalContentSha256",
        ]
    )
    for doc_id in ("4001", "4002"):
        writer.writerow(
            [
                "Rivera",
                "Dana",
                "8/11/2026",
                "2026-08-10",
                doc_id,
                f"https://{_WEB_DOCUMENTS_HOST}/reports/{doc_id}.pdf",
                _WEB_DOCUMENTS_AVAILABLE_AT[doc_id],
                hashlib.sha256(_pdf_lines(_WEB_DOCUMENTS_REPORTS[doc_id])).hexdigest(),
            ]
        )
    return buffer.getvalue().encode("utf-8")


def _web_documents_client(*, request_log=None):
    import httpx

    from lumibot.components.agents.web_tools import WebClient

    index = _web_documents_index_zip()
    requests = request_log if request_log is not None else []

    def record_request(request):
        # Capture the validated original URL before WebClient pins its address.
        # Every redirect gets its own hook. Never record headers or bodies.
        if len(requests) >= 1024:
            raise RuntimeError("Synthetic web request capture is full; request was not sent.")
        requests.append({"method": request.method, "url": str(request.url)})

    def handler(request):
        path = request.url.path
        if path == "/disclosures/2026/index.html":
            return httpx.Response(200, text=_WEB_DOCUMENTS_DISCOVERY_HTML, headers={"content-type": "text/html"})
        if path == "/disclosures/2026/supplemental.csv":
            return httpx.Response(302, headers={"location": "/disclosures/2026/supplemental-index.csv"})
        if path == "/disclosures/2026/supplemental-index.csv":
            return httpx.Response(200, content=_web_documents_supplemental_csv(), headers={"content-type": "text/csv"})
        if path == "/disclosures/2026/current-holdings.html":
            return httpx.Response(200, text=_WEB_DOCUMENTS_CURRENT_HTML, headers={"content-type": "text/html"})
        if path.lower().endswith("/2026fd.zip"):
            return httpx.Response(200, content=index, headers={"content-type": "application/x-zip-compressed"})
        doc_id = path.rsplit("/", 1)[-1].removesuffix(".pdf")
        if path.startswith("/reports/") and doc_id in _WEB_DOCUMENTS_REPORTS:
            return httpx.Response(
                200, content=_pdf_lines(_WEB_DOCUMENTS_REPORTS[doc_id]), headers={"content-type": "application/pdf"}
            )
        return httpx.Response(404, text="Not found")

    client = WebClient(transport=httpx.MockTransport(handler), resolver=lambda host: ["93.184.216.34"])
    client._client.event_hooks["request"].append(record_request)
    client.web_requests = requests
    return client


# Many real public sources list only the day a report was published, with no
# time or version history. A generic investor's report index shaped that way.
_DATE_ONLY_HOST = "records.example.org"
_DATE_ONLY_REPORTS = {
    "H1": [
        "Example Capital - Holdings Report",
        "Published: 05/15/2026   Holdings as of: 03/31/2026",
        "Apple Inc. (AAPL) common stock $1,000,001 - $5,000,000",
        "Microsoft Corporation (MSFT) common stock $250,001 - $500,000",
    ],
    "C1": [
        "Example Capital - Change Notice",
        "Published: 07/02/2026",
        "Microsoft Corporation (MSFT) sold 06/20/2026 $250,001 - $500,000: the entire position.",
        "Tesla, Inc. (TSLA) bought 06/22/2026 $100,001 - $250,000.",
    ],
    "C2": [
        "Example Capital - Change Notice",
        "Published: 08/10/2026",
        "Amazon.com Inc. (AMZN) bought 08/05/2026 $500,001 - $1,000,000.",
    ],
    "C3": [
        "Example Capital - Change Notice",
        "Published: 08/11/2026",
        "Apple Inc. (AAPL) sold 08/07/2026 $1,000,001 - $5,000,000: the entire position.",
    ],
    "C4": [
        "Example Capital - Change Notice",
        "Published: 09/15/2026",
        "Zeta Holdings (ZZZZ) bought 09/01/2026 $1,000,001 - $5,000,000.",
    ],
}
_DATE_ONLY_ROWS = [
    ("H1", "Holdings Report", "5/15/2026"),
    ("C1", "Change Notice", "7/2/2026"),
    ("C2", "Change Notice", "8/10/2026"),
    ("C3", "Change Notice", "8/11/2026"),
    ("C4", "Change Notice", "9/15/2026"),
]


def _date_only_index_csv():
    rows = ["ReportID,Investor,ReportType,PublishedDate,URL"]
    for number in range(300):
        rows.append(f"X{number:03d},Other Fund {number:03d},Holdings Report,{number % 12 + 1}/{number % 27 + 1}/2026,")
    for report_id, report_type, published in _DATE_ONLY_ROWS:
        rows.append(
            f"{report_id},Example Capital,{report_type},{published},https://{_DATE_ONLY_HOST}/reports/{report_id}.pdf"
        )
    return ("\n".join(rows) + "\n").encode("utf-8")


def _date_only_client(*, request_log=None):
    import httpx

    from lumibot.components.agents.web_tools import WebClient

    requests = request_log if request_log is not None else []

    def record_request(request):
        if len(requests) >= 1024:
            raise RuntimeError("Synthetic web request capture is full; request was not sent.")
        requests.append({"method": request.method, "url": str(request.url)})

    def handler(request):
        path = request.url.path
        if path == "/reports/index.csv":
            return httpx.Response(200, content=_date_only_index_csv(), headers={"content-type": "text/csv"})
        report_id = path.rsplit("/", 1)[-1].removesuffix(".pdf")
        if path.startswith("/reports/") and report_id in _DATE_ONLY_REPORTS:
            return httpx.Response(
                200, content=_pdf_lines(_DATE_ONLY_REPORTS[report_id]), headers={"content-type": "application/pdf"}
            )
        return httpx.Response(404, text="Not found")

    client = WebClient(transport=httpx.MockTransport(handler), resolver=lambda host: ["93.184.216.34"])
    client._client.event_hooks["request"].append(record_request)
    client.web_requests = requests
    return client


# A generic research site whose main note carries no publication date at all,
# plus a revision whose listing declares a date after the backtest clock. The
# undated note is usable with a disclosed estimate; the declared-future revision
# must stay unopened in a backtest.
_UNDATED_HOST = "notes.example.net"
_UNDATED_PAGES = {
    "/research/": (
        "<html><body><h1>Widget Industry Research Notes</h1><ul>"
        '<li><a href="/research/widget-demand.html">Widget Industry Demand Note</a></li>'
        '<li><a href="/research/widget-demand-revised.html">Widget Industry Demand Note: Revised</a>'
        " (published September 2, 2026)</li>"
        "</ul></body></html>"
    ),
    "/research/live/": (
        "<html><body><h1>Widget Industry Research Notes</h1><ul>"
        '<li><a href="/research/widget-demand.html">Widget Industry Demand Note</a></li>'
        "</ul></body></html>"
    ),
    "/research/widget-demand.html": (
        "<html><body><h1>Widget Industry Demand Note</h1>"
        "<p>Survey responses were collected from widget buyers through June 30, 2026.</p>"
        "<p>Second-quarter 2026 widget demand index: 112, up from 104 in the first quarter of 2026.</p>"
        "<p>Buyers expect steady orders for the rest of the year.</p>"
        "</body></html>"
    ),
    "/research/widget-demand-revised.html": (
        "<html><body><h1>Widget Industry Demand Note: Revised</h1>"
        "<p>Published September 2, 2026.</p>"
        "<p>Revised second-quarter 2026 widget demand index: 97, down from the earlier 112.</p>"
        "</body></html>"
    ),
}


_UNDATED_FIXTURES = frozenset({"web_documents_undated", "web_documents_undated_live"})

# Ordinary HTML reports with imperfect coverage, date-only publication and no
# retained-version hashes. Exercise estimates without relaxing future exclusion.
_APPROXIMATE_PORTFOLIO_PAGES = {
    "/portfolio/": (
        "<html><body><h1>Public stock basket reports</h1>"
        "<p>The archive is incomplete; no certified complete manifest or version hashes are available.</p>"
        '<p><a href="/portfolio/snapshot.html">March 31 snapshot</a>, published April 15, 2026.</p>'
        '<p><a href="/portfolio/update.html">Transaction update</a>, published June 15, 2026.</p>'
        '<p><a href="/portfolio/marks.html">August 10 closing marks</a>, published August 10, 2026.</p>'
        '<p><a href="/portfolio/future.html">New transaction update</a>, published September 2, 2026.</p>'
        "</body></html>"
    ),
    "/portfolio/snapshot.html": (
        "<html><body><h1>Stock basket at March 31, 2026</h1>"
        "<p>Published April 15, 2026. Common shares only. AAPL: 1,000 shares. "
        "MSFT: value $50,000 to $100,000; quantity not disclosed; March 31 price $300. "
        "These are the only reported holdings at the snapshot.</p></body></html>"
    ),
    "/portfolio/update.html": (
        "<html><body><h1>Transaction update, published June 15, 2026</h1>"
        "<p>February 10: sold 1,000 AAPL shares. This historical transaction is "
        "repeated here and is already reflected in the March 31 snapshot.</p>"
        "<p>June 10: sold 300 AAPL shares (partial sale).</p>"
        "<p>June 10: TSLA common stock [ST], purchase, 200 shares.</p>"
        "<p>June 10: TSLA call options [OP], purchase, 2 contracts, strike $350, "
        "expiration December 18, 2026. These calls remain unexercised.</p>"
        "<p>No other transactions in this report.</p></body></html>"
    ),
    "/portfolio/marks.html": (
        "<html><body><h1>August 10, 2026 closing prices</h1>"
        "<p>Published August 10, 2026. AAPL $230; MSFT $320; TSLA $300.</p></body></html>"
    ),
    "/portfolio/future.html": (
        "<html><body><h1>Published September 2, 2026</h1>"
        "<p>August 28: sold the entire AAPL holding.</p></body></html>"
    ),
}


def _undated_client(*, request_log=None, pages=None):
    import httpx

    from lumibot.components.agents.web_tools import WebClient

    requests = request_log if request_log is not None else []

    def record_request(request):
        if len(requests) >= 1024:
            raise RuntimeError("Synthetic web request capture is full; request was not sent.")
        requests.append({"method": request.method, "url": str(request.url)})

    def handler(request):
        page = (pages if pages is not None else _UNDATED_PAGES).get(request.url.path)
        if page is None:
            return httpx.Response(404, text="Not found")
        return httpx.Response(200, text=page, headers={"content-type": "text/html"})

    client = WebClient(transport=httpx.MockTransport(handler), resolver=lambda host: ["93.184.216.34"])
    client._client.event_hooks["request"].append(record_request)
    client.web_requests = requests
    return client


_WRAPPED_RANGES = (
    ("QZX01", 250001, 500000), ("QZX02", 1000001, 5000000),
    ("QZX03", 500001, 1000000), ("QZX04", 15001, 50000),
    ("QZX05", 5000001, 25000000), ("QZX06", 100001, 250000),
    ("QZX07", 1001, 15000), ("QZX08", 25000001, 50000000),
    ("QZX09", 50001, 100000), ("QZX10", 1000001, 5000000),
    ("QZX11", 250001, 500000), ("QZX12", 500001, 1000000),
    ("QZX13", 100001, 250000),
)


def _wrapped_ranges_text():
    # This reproduces the observed extracted-table shape: an instrument and
    # lower bound on a matching line, its upper bound on the continuation, and
    # a different income field. No real person's report or holdings are copied.
    lines = ["Operating investment portfolio, reporting date March 31, 2026.",
             "Published June 1, 2026. All monetary values are USD.",
             "Asset / owner / asset value / income type / income amount"]
    for number, (symbol, lower, upper) in enumerate(_WRAPPED_RANGES, 1):
        income = ("None; income amount: None" if number % 2 else
                  "Dividends; income amount: $15,001 - $50,000")
        lines += [f"Industrial Equipment and Distribution Group {number:02d}",
                  f"Ordinary common shares ({symbol}) [EQ] Owner A ${lower:,} -",
                  f"${upper:,}",
                  f"Income type: {income}. This income field is separate from asset value.",
                  "Instrument classification: listed common equity; reporting currency: US dollars."]
    lines += ["Dormant Equipment Group (QZXZERO) [EQ] Owner A asset value None; income None.",
              "Cash reserve [CASH] Owner A asset value $50,001 - $100,000; income $1 - $200."]
    return "\n".join(lines)


def _wrapped_ranges_client():
    import httpx
    from lumibot.components.agents.web_tools import WebClient

    requests = []

    def handler(request):
        requests.append({"method": request.method, "url": str(request.url)})
        if request.url.path == "/reports/portfolio.txt":
            return httpx.Response(200, text=_wrapped_ranges_text(), headers={"content-type": "text/plain"})
        return httpx.Response(404, text="Not found")

    client = WebClient(transport=httpx.MockTransport(handler), resolver=lambda host: ["93.184.216.34"])
    client.web_requests = requests
    return client


def _recorded_fred(strategy, cache_dir):
    """Real FRED client and point-in-time filtering, fed recorded VIX closes instead of the network.

    The price data has no VIX (like Alpaca), so the agent must find the VIX close on FRED.
    """
    from lumibot.macro.fred import FREDMacroData

    # The same-day close is deliberately contradictory: a date-only vintage
    # must not let a morning decision use an after-close observation.
    closes = {"2026-08-05": 22.10, "2026-08-06": 23.45, "2026-08-07": 24.90,
              "2026-08-10": 27.30, "2026-08-11": 11.10, "2026-08-12": 31.00}

    class _RecordedFRED(FREDMacroData):
        def _get_json(self, url, params, cache_path):
            if params.get("series_id") != "VIXCLS":
                raise ValueError(f"Recorded FRED fixture has no series {params.get('series_id')!r}.")
            return {"observations": [
                {"date": day, "value": f"{value:.2f}", "realtime_start": day, "realtime_end": "9999-12-31"}
                for day, value in closes.items()
            ]}

    return _RecordedFRED(strategy, cache_dir=cache_dir, api_key="recorded-fixture")


class ProductionFixture:
    def __init__(self, fixture):
        self.fixture = fixture
        self.directory = tempfile.TemporaryDirectory(prefix="lumibot-agent-eval-")
        self.root = Path(self.directory.name)
        self.closed = False
        assets = [Asset("AAPL"), Asset("SPY")]
        assets.extend(Asset(symbol) for symbol, market in fixture.stock_markets.items() if market is not None)
        for right, strikes in (("put", (592, 594, 596, 598)), ("call", (602, 604, 606, 608))):
            assets.extend(
                Asset(
                    "SPY",
                    asset_type="option",
                    expiration=date.fromisoformat(fixture.expiration),
                    strike=strike,
                    right=right,
                )
                for strike in strikes
            )
        data = [_history(asset, fixture) for asset in assets]
        if fixture.name == "crypto_instrument_identity":
            stock_btc = Asset("BTC", asset_type="stock")
            crypto_btc = Asset("BTC", asset_type="crypto")
            crypto_usd = Asset("USD", asset_type="crypto")
            data.extend(
                [
                    _history(stock_btc, fixture),
                    _history(crypto_btc, fixture, quote_asset=crypto_usd),
                ]
            )
        source = PandasDataBacktesting(
            pandas_data=data,
            datetime_start=pd.Timestamp("2026-08-11T14:35:00Z"),
            datetime_end=pd.Timestamp("2026-08-11T14:37:00Z"),
            show_progress_bar=False,
            market="24/7",
        )
        source.load_data()
        self.broker = _FixtureBroker(data_source=source)
        self.broker.fixture = fixture
        self.broker.initialize_market_calendars(source.get_trading_days_pandas())
        self.broker._first_iteration = False
        self.strategy = _EvalStrategy(
            broker=self.broker,
            budget=fixture.initial_cash,
            name=f"eval_{uuid.uuid4().hex}",
            risk_free_rate=0.04,
            analyze_backtest=False,
            parameters={},
        )
        self.strategy._first_iteration = False
        from lumibot.components.memory import MemoryStore
        # The fixture owns this exact synthetic SQLite store and derived exports.
        # ProductionFixture.close removes them with the fixture's finally owner.
        self.strategy.memory = MemoryStore(self.strategy, root_dir=self.root / "memory")
        for position in fixture.positions:
            value = dict(position)
            quantity = value.pop("quantity")
            if value.get("expiration"):
                value["expiration"] = date.fromisoformat(value["expiration"])
            self.broker._filled_positions.append(Position(self.strategy.name, Asset(**value), quantity))
        if fixture.name == "stock_pending_exit":
            order = self.strategy.create_order(Asset("AAPL"), 40, "sell", order_type="limit", limit_price=250.0)
            order.identifier = "bt_pending_exit"
            self.strategy.submit_order(order)
        if fixture.empty_expirations:
            original_get_chains = self.strategy.get_chains
            empty = tuple(fixture.empty_expirations)
            extra_strikes = {"CALL": [602.0, 604.0, 606.0, 608.0], "PUT": [592.0, 594.0, 596.0, 598.0]}

            def get_chains(asset, *args, **kwargs):
                chains = original_get_chains(asset, *args, **kwargs)
                if str(getattr(asset, "symbol", "")).upper() != "SPY":
                    return chains
                root = chains.setdefault("Chains", {})
                for side, strikes in extra_strikes.items():
                    side_map = root.get(side) or {}
                    normalized: dict[str, list] = {}
                    for key, listed in list(side_map.items()):
                        text = key.isoformat() if isinstance(key, date) else str(key)[:10]
                        normalized[text] = [float(value) for value in listed]
                    for expiration in empty:
                        # Listed, with no bars in the data store, so quotes and last trades miss.
                        normalized[expiration] = list(strikes)
                    root[side] = normalized
                return chains

            self.strategy.get_chains = get_chains
        if fixture.name == "web_documents":
            self.strategy._agent_web_client = _web_documents_client()
            self.fixture.web_requests = self.strategy._agent_web_client.web_requests
        if fixture.name == "web_documents_date_only":
            self.strategy._agent_web_client = _date_only_client()
            self.fixture.web_requests = self.strategy._agent_web_client.web_requests
        if fixture.name in _UNDATED_FIXTURES:
            self.strategy._agent_web_client = _undated_client()
            self.fixture.web_requests = self.strategy._agent_web_client.web_requests
        if fixture.name == "web_documents_approximate_portfolio":
            self.strategy._agent_web_client = _undated_client(pages=_APPROXIMATE_PORTFOLIO_PAGES)
            self.fixture.web_requests = self.strategy._agent_web_client.web_requests
        if fixture.name == "web_documents_wrapped_ranges":
            self.strategy._agent_web_client = _wrapped_ranges_client()
            self.fixture.web_requests = self.strategy._agent_web_client.web_requests
        if fixture.name == "vix_from_fred":
            self.strategy.macro = _recorded_fred(self.strategy, self.root / "fred")
        if fixture.name == "news_source_recovery":
            self.strategy._agent_web_client = _undated_client(pages={
                "/news/archive.html": (
                    "<html><body><h1>Industry newsroom archive</h1>"
                    '<a href="/news/prior.html">Supply update, published August 10, 2026 at 20:00 UTC</a>'
                    '<a href="/news/future.html">Revised supply update, published August 12, 2026</a>'
                    "</body></html>"
                ),
                "/news/prior.html": (
                    '<html><head><meta property="article:published_time" content="2026-08-10T20:00:00Z"></head>'
                    "<body><h1>Widget supply update</h1><p>Published August 10, 2026 at 20:00 UTC.</p>"
                    "<p>The widget supply index rose from 104 to 108. Temporary freight disruption "
                    "remains a risk. This is an industry survey, not a company earnings release.</p></body></html>"
                ),
                "/news/future.html": (
                    "<html><body><h1>Revised supply update</h1><p>Published August 12, 2026.</p>"
                    "<p>The revised widget supply index is 80.</p></body></html>"
                ),
            })
            self.fixture.web_requests = self.strategy._agent_web_client.web_requests
        self.strategy.fundamentals = _recorded_sec_fundamentals(self.strategy, self.root / "sec")
        if fixture.name in {"research_live_available", "research_unavailable", "web_documents_undated_live"}:
            # Use the actual Strategy mode setter after synthetic backtest setup.
            # Keep its bounded recorded clock and isolated broker: calendar data
            # is live research evidence, not a qualified historical replay.
            self.strategy.is_backtesting = False
        self.manager = self.strategy.agents
        self.manager.replay_cache.root = self.root / "replay"
        self.manager.replay_cache.remote_cache = _NoRemoteCache()

    def create_agent(self, case, runtime, *, name="trader", allow_trading=True, system_prompt=None):
        from scripts.run_agent_evals import EVAL_REASONING_EFFORT

        rules = self.root / "rules.json"
        rules.write_text(json.dumps(case.get("rules") or {"version": 1, "rules": []}), encoding="utf-8")
        servers = []
        if str(case.get("fixture", "")).startswith("research"):
            servers = [
                MCPServer(
                    name="botspot_research",
                    command=sys.executable,
                    args=[str(Path(__file__).with_name("agent_eval_research_server.py")), self.fixture.name],
                    exposed_tools=["search_data_catalog", "query_data", "search_documents", "get_document"],
                )
            ]
        return self.manager.create(
            name=name,
            model=case["model"],
            system_prompt=system_prompt or case["systemPrompt"],
            rules_path=rules,
            _runtime=runtime,
            mcp_servers=servers,
            allow_trading=False if self.fixture.name == "research_live_available" else allow_trading,
            allow_network=bool(case.get("allowNetwork")),
            reasoning_effort=None if str(case["model"]).startswith("gemini") else EVAL_REASONING_EFFORT,
        )

    def tools(self):
        from lumibot.components.agents import BuiltinTools

        return [definition.binder(self.strategy, self.manager) for definition in BuiltinTools.all()]

    def settle(self):
        self.broker.process_pending_orders(self.strategy)

    def _capture_memory_events(self):
        """Read committed synthetic audit rows, preserving original bytes and linkage.

        This is evidence from the existing MemoryStore, not a second journal or
        generated tool trajectory. Capture before settlement and store teardown;
        neither stage can establish when the actor observed a broker fill.
        """
        captured = []
        size = 2  # JSON array delimiters; include separators in the byte bound.
        with self.strategy.memory._connect() as connection:
            rows = connection.execute(
                """SELECT event_id, sequence, timestamp, wall_time, strategy, event_type,
                          subject_type, subject_id, agent_name, model_call_id, text,
                          metadata_json, payload_json, payload_hash, schema_version
                   FROM memory_events
                   WHERE event_type IN (
                       'agent.tool_started', 'agent.tool_outcome',
                       'order.intent', 'order.action_outcome'
                   )
                   ORDER BY sequence LIMIT ?""",
                (_MAX_CAPTURED_MEMORY_EVENTS + 1,),
            )
            for source in rows:
                if len(captured) >= _MAX_CAPTURED_MEMORY_EVENTS:
                    raise ValueError("Fixture journal row bound exceeded; audit evidence was not truncated.")
                row = dict(source)
                row["metadata"] = json.loads(row.pop("metadata_json"))
                size += len(json.dumps(row, ensure_ascii=True).encode("utf-8")) + (2 if captured else 0)
                if size > _MAX_CAPTURED_MEMORY_BYTES:
                    raise ValueError("Fixture journal byte bound exceeded; audit evidence was not truncated.")
                captured.append(row)
        self.fixture.memory_events = captured

    def capture(self, result):
        from lumibot.components.agents.builtins import _position_to_dict

        self._capture_memory_events()
        self.orders_before_settlement = [_capture_broker_order(order) for order in self.strategy.get_orders()]
        self.settle()
        self.fixture.final_account = {
            "cash": float(self.strategy.get_cash()),
            "portfolio_value": float(self.strategy.get_portfolio_value()),
        }
        self.fixture.broker_trade_events = json.loads(self.broker._trade_event_log_df.to_json(orient="records", date_format="iso"))
        self.fixture.calls = [{"name": event.tool_name, "arguments": event.payload} for event in result.tool_calls]
        self.fixture.positions = [
            _position_to_dict(p) for p in self.strategy.get_positions() if p.asset.symbol != "USD"
        ]
        self.fixture.submissions = []
        self.fixture.rejected_submissions = []
        outcomes = _order_call_outcomes(result)
        rejected = _rejected_order_calls(result)
        for position, event in enumerate(result.tool_calls):
            if event.tool_name not in {"orders_submit_order", "orders_submit_orders", "orders_submit_multileg"}:
                continue
            args = event.payload or {}
            if event.tool_name == "orders_submit_orders":
                try:
                    specifications = json.loads(args.get("orders_json", "[]"))
                except (TypeError, json.JSONDecodeError):
                    specifications = []
                target = self.fixture.rejected_submissions if position in rejected else self.fixture.submissions
                if isinstance(specifications, list):
                    target.extend({"tool": event.tool_name, **row} for row in specifications if isinstance(row, dict))
                continue
            if event.tool_name == "orders_submit_multileg":
                legs = args.get("legs_json", "[]")
                try:
                    legs = json.loads(legs) if isinstance(legs, str) else legs
                except json.JSONDecodeError:
                    legs = []
                if args.get("action") == "close" and position not in rejected:
                    legs = _resolved_multileg_legs(outcomes.get(position),
                        broker_orders=self.orders_before_settlement, reason=args.get("reason")) or legs
                record = {
                    "tool": event.tool_name,
                    "legs": legs,
                    "net_limit_price": args.get("net_limit_price"),
                    "price_style": args.get("price_style"),
                }
            else:
                record = {"tool": event.tool_name, **args}
            # Only orders the tool accepted reached the broker.
            target = self.fixture.rejected_submissions if position in rejected else self.fixture.submissions
            target.append(record)
        return [_capture_broker_order(order) for order in self.strategy.get_orders()]

    def close(self):
        if self.closed:
            return
        self.closed = True
        self.broker.stream.stop()
        try:
            if self.fixture.name in {"web_documents", "web_documents_date_only", "web_documents_approximate_portfolio", "web_documents_wrapped_ranges", *_UNDATED_FIXTURES}:
                self.strategy._agent_web_client.close()
        finally:
            self.directory.cleanup()
