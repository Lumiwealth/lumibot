"""SEC Form 4 public-insider-filings AI strategy example.

This strategy analyzes public filings. It is not based on material non-public
information and should not be described as illegal insider trading.
"""

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from lumibot.components.agents.web_tools import WebClient
from lumibot.components.disclosure_signals import visible_insider_transactions
from lumibot.fundamentals.sec import DEFAULT_SEC_USER_AGENT
from lumibot.strategies import Strategy

_MISSING_FILINGS = (
    "SEC Form 4 example requires official EDGAR filings. Pass transactions or "
    "transactions_path, or set load_live_feed. This example does not include sample trades."
)
FORM4_ATOM_URL = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&output=atom"
_WEBB_ACCESSION = "0001193125-26-397981"
_ACCESSION_RE = re.compile(r"(\d{10}-\d{2}-\d{6})")


def _records(parameters: dict[str, Any]) -> list[dict[str, Any]]:
    supplied = parameters.get("transactions")
    if supplied is not None:
        return list(supplied)
    path_value = parameters.get("transactions_path")
    if not path_value:
        raise ValueError(_MISSING_FILINGS)
    path = Path(path_value)
    if not path.is_file():
        raise ValueError(f"{_MISSING_FILINGS} Missing file: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("SEC Form 4 file must contain a JSON list of official filings.")
    return payload


def _parse_timestamp(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    text = str(value).replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def form4_rows_from_feed(feed: dict[str, Any]) -> list[dict[str, Any]]:
    """Turn a live Form 4 Atom payload into rows the strategy can clock-gate."""
    rows = []
    for entry in feed.get("entries") or []:
        blob = " ".join(str(entry.get(key) or "") for key in ("title", "summary", "id", "link"))
        match = _ACCESSION_RE.search(blob)
        accession = match.group(1) if match else None
        title = str(entry.get("title") or "")
        rows.append(
            {
                "id": accession or entry.get("id") or title,
                "accession_number": accession,
                "title": title,
                "link": entry.get("link"),
                "published_at": entry.get("published_at"),
                "summary": entry.get("summary"),
                "source": "sec_edgar_form4_atom",
                "ticker": None,
                "open_market": None,
            }
        )
    return rows


def split_form4_feed(rows: list[dict[str, Any]], *, as_of: Any) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Keep only rows whose published time is already visible at the clock."""
    ceiling = _parse_timestamp(as_of)
    if ceiling is None:
        raise ValueError("as_of must be a valid datetime.")
    visible = []
    hidden = []
    for row in rows:
        published = _parse_timestamp(row.get("published_at"))
        if published is None or published > ceiling:
            hidden.append(row)
        else:
            visible.append(row)
    return visible, hidden


def fetch_live_form4_feed() -> dict[str, Any]:
    """rss_fetch the live SEC Form 4 Atom feed."""
    client = WebClient(timeout_seconds=30, max_response_bytes=2_000_000)
    try:
        return client.fetch_feed(
            FORM4_ATOM_URL,
            max_entries=40,
            max_response_bytes=2_000_000,
        )
    finally:
        client.close()


def dry_run_form4_feed(as_of: datetime | None = None) -> str:
    """Live-mode read. Prints whether today's feed can show the Webb accession."""
    clock = as_of or datetime.now(timezone.utc)
    # SEC rejects a bare client. Use the same declared user agent the strategy uses.
    client = WebClient(timeout_seconds=30, max_response_bytes=2_000_000)
    try:
        response = client.request(
            "GET",
            FORM4_ATOM_URL,
            headers={"User-Agent": DEFAULT_SEC_USER_AGENT, "Accept": "application/atom+xml"},
        )
    finally:
        client.close()
    text = response.get("text") or ""
    if "<feed" not in text:
        raise RuntimeError(f"SEC Form 4 feed was not Atom. Status {response.get('status_code')}.")
    parsed = WebClient._parse_feed(
        text,
        max_entries=40,
        source=FORM4_ATOM_URL,
        fetched_at=response.get("fetched_at") or clock.isoformat(),
    )
    rows = form4_rows_from_feed(parsed)
    visible, hidden = split_form4_feed(rows, as_of=clock)
    webb_visible = [row for row in visible if row.get("accession_number") == _WEBB_ACCESSION]
    lines = [
        f"Form 4 feed fetched {len(rows)} visible {len(visible)} hidden_future {len(hidden)}",
        f"Form 4 feed Webb visible {len(webb_visible)} accession {_WEBB_ACCESSION}",
    ]
    for row in webb_visible:
        lines.append(f"Form 4 feed visible Webb {row.get('title')} published {row.get('published_at')}")
    text = "\n".join(lines)
    print(text)
    return text


class AISECInsiderFilingsStrategy(Strategy):
    parameters = {
        "transactions": None,
        "transactions_path": None,
        "load_live_feed": False,
        "execution_mode": "agent",
        "open_market_only": True,
        "include_amendments": False,
        "max_position_pct": 5,
    }

    def initialize(self):
        self.sleeptime = "1D"
        self._processed_transaction_ids = set()
        self.agents.create(
            name="form4_researcher",
            default_model="gemini-3.5-flash-lite",
            allow_trading=False,
            system_prompt=(
                "Analyze supplied public SEC Form 4 rows. Distinguish open-market codes from grants, gifts, option "
                "exercises, automatic plans, derivatives, and amendments. Treat published_at as the first available "
                "moment. Identify issuer, owner, direct/indirect ownership, value, contradictions, and missing "
                "facts. Never use transaction_date as the availability boundary and do not submit orders."
            ),
        )
        self.agents.create(
            name="trading_risk_manager",
            default_model="gemini-3.5-flash-lite",
            allow_trading=True,
            system_prompt=(
                "You are the only trading agent and own risk management. Verify the account, positions, open orders, "
                "and market price. Trade only exact tickers supported by supplied open-market Form 4 evidence. "
                "Never short, never trade grants/gifts/options as if they were open-market purchases, and cap each new "
                "position at max_position_pct of portfolio value and available cash. Submit each intent once, inspect "
                "status, and reread account state. Hold when evidence is amended, conflicting, stale, or incomplete."
            ),
        )

    def _act_on_live_form4_feed(self, as_of: Any) -> None:
        client = WebClient(timeout_seconds=30, max_response_bytes=2_000_000)
        try:
            parsed = client.fetch_feed(FORM4_ATOM_URL, max_entries=40, max_response_bytes=2_000_000)
        finally:
            client.close()
        if not parsed.get("ok", True) and not parsed.get("entries"):
            self.log_message(f"Form 4 rss_fetch failed status {parsed.get('status_code')}")
            return
        rows = form4_rows_from_feed(parsed)
        visible, hidden = split_form4_feed(rows, as_of=as_of)
        self.log_message(
            f"Form 4 rss_fetch {FORM4_ATOM_URL} entries {len(rows)} "
            f"visible {len(visible)} hidden_future {len(hidden)}"
        )
        for row in visible:
            if row.get("accession_number") == _WEBB_ACCESSION or "webb" in str(row.get("title") or "").lower():
                self.log_message(
                    f"Form 4 feed visible Webb accession {row.get('accession_number')} "
                    f"published {row.get('published_at')}"
                )
        for row in hidden:
            if row.get("accession_number") == _WEBB_ACCESSION:
                self.log_message(
                    f"Form 4 feed hidden future accession {row.get('accession_number')} "
                    f"published {row.get('published_at')}"
                )
        if self.parameters.get("execution_mode") != "filing_rule":
            return
        for row in visible:
            ticker = row.get("ticker")
            if not ticker:
                self.log_message(f"Form 4 feed skip, no ticker: {row.get('accession_number')}")
                continue
            price = self.get_last_price(ticker)
            if price is None or float(price) <= 0:
                self.log_message(f"Form 4 feed skip, no price: {ticker}")
                continue
            self.submit_order(self.create_order(ticker, 1, "buy"))
            self.log_message(f"Form 4 feed order buy {ticker} after {row.get('published_at')}")

    def on_trading_iteration(self):
        as_of = self.get_datetime()
        if (
            self.parameters.get("transactions") is None
            and not self.parameters.get("transactions_path")
            and self.parameters.get("load_live_feed")
        ):
            self._act_on_live_form4_feed(as_of)
            return
        visible = visible_insider_transactions(_records(self.parameters), as_of=as_of)
        if self.parameters.get("open_market_only", True):
            visible = [record for record in visible if record.get("open_market") is True]
        if not self.parameters.get("include_amendments", False):
            visible = [record for record in visible if record.get("amendment") is not True]
        current = [record for record in visible if record["id"] not in self._processed_transaction_ids]
        if not current:
            return
        context = {
            "as_of": as_of.isoformat(),
            "transactions": current,
            "max_position_pct": self.parameters["max_position_pct"],
            "availability_rule": "Records become visible at SEC acceptance/published_at.",
        }
        research = self.agents["form4_researcher"].run(
            task_prompt="Evaluate the newly public Form 4 rows and produce a sourced evidence packet.",
            context=context,
        )
        decision = self.agents["trading_risk_manager"].run(
            task_prompt="Review the evidence, enforce risk, and take at most one justified trading action.",
            context={**context, "research_evidence": research.summary},
        )
        self.log_message(f"Form 4 research: {research.summary}")
        self.log_message(f"Form 4 trader: {decision.summary}")
        self._processed_transaction_ids.update(record["id"] for record in current)


if __name__ == "__main__":
    dry_run_form4_feed()
