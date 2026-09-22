"""Point-in-time congressional-disclosure strategy.

Official House Clerk periodic transaction reports are public. One example
covers stocks and options. Stock mode needs a ticker and a buy or sell.
Option mode also needs call or put, strike, and expiration, and it skips a
row that lacks any of those. Gifts, spinoffs, private LLCs, and money-market
funds are skipped. A row stays hidden until the digital signature date
(ReportDate), which can be up to 45 days after the transaction.

``execution_mode="agent"`` keeps the researcher and the one trading agent.
``execution_mode="filing_rule"`` submits the disclosed order itself, capped
so a large reported range cannot blow up the account. That path does not
call a model.
"""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from lumibot.components.disclosure_signals import visible_congress_disclosures
from lumibot.components.house_ptr import (
    download_house_pdf,
    format_dry_run,
    house_pdf_url,
    parse_house_ptr_text,
    pdf_bytes_to_text,
    tradeable_rows,
)
from lumibot.entities import Asset
from lumibot.strategies import Strategy

_MISSING_FILINGS = (
    "Congress example requires official House Clerk or Senate periodic transaction "
    "reports. Pass disclosures or disclosures_path, or set load_house_filings. "
    "This example does not include sample trades."
)
_PELOSI_2026_DOCS = ("20033725", "20034836", "20035143")


def _records(parameters: dict[str, Any]) -> list[dict[str, Any]]:
    supplied = parameters.get("disclosures")
    if supplied is not None:
        return list(supplied)
    path_value = parameters.get("disclosures_path")
    if path_value:
        path = Path(path_value)
        if not path.is_file():
            raise ValueError(f"{_MISSING_FILINGS} Missing file: {path}")
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            raise ValueError("Congress disclosures file must contain a JSON list of official filings.")
        return payload
    if parameters.get("load_house_filings"):
        year = int(parameters.get("house_year") or datetime.now(timezone.utc).year)
        doc_ids = parameters.get("house_doc_ids") or list(_PELOSI_2026_DOCS)
        asset_mode = parameters.get("asset_mode") or "stock"
        rows: list[dict[str, Any]] = []
        for doc_id in doc_ids:
            pdf = download_house_pdf(year, str(doc_id))
            text = pdf_bytes_to_text(pdf)
            parsed = parse_house_ptr_text(text, source_url=house_pdf_url(year, str(doc_id)))
            rows.extend(tradeable_rows(parsed, asset_mode=asset_mode))
        return rows
    raise ValueError(_MISSING_FILINGS)


def dry_run_pelosi(year: int = 2026) -> str:
    """Download the three 2026 Pelosi PTRs and print stock and option rows."""
    blocks = []
    for doc_id in _PELOSI_2026_DOCS:
        pdf = download_house_pdf(year, doc_id)
        parsed = parse_house_ptr_text(pdf_bytes_to_text(pdf), source_url=house_pdf_url(year, doc_id))
        tradeable = tradeable_rows(parsed, asset_mode="stock") + tradeable_rows(parsed, asset_mode="option")
        rendered = format_dry_run(tradeable)
        if "REOF" in rendered or "LLC" in rendered:
            raise RuntimeError(f"Private LLC leaked into the {doc_id} dry run.")
        blocks.append(f"# {doc_id}\n{rendered}")
    text = "\n".join(blocks)
    print(text)
    return text


class AICongressDisclosuresStrategy(Strategy):
    parameters = {
        "disclosures": None,
        "disclosures_path": None,
        "load_house_filings": False,
        "house_year": 2026,
        "house_doc_ids": list(_PELOSI_2026_DOCS),
        "asset_mode": "stock",
        "execution_mode": "agent",
        "max_disclosure_age_days": 90,
        "max_position_pct": 5,
        "max_total_exposure_pct": 20,
        "minimum_average_dollar_volume": 1_000_000,
    }

    def initialize(self):
        self.sleeptime = "1D"
        self._processed_disclosure_ids = set()
        self._house_records = None
        self._filing_exposure = 0.0
        if self.parameters.get("execution_mode") == "filing_rule":
            return
        self.agents.create(
            name="disclosure_researcher",
            default_model="gemini-3.5-flash-lite",
            allow_trading=False,
            system_prompt=(
                "Analyze only the supplied congressional financial disclosures. Treat each published_at value as the "
                "first moment the market could know the record; transaction_date is historical context, never an "
                "availability date. Verify ticker identity, purchase/sale direction, size range, age, contradictory "
                "disclosures, and missing evidence. Explain the statutory reporting lag. Do not submit orders."
            ),
        )
        self.agents.create(
            name="trading_risk_manager",
            default_model="gemini-3.5-flash-lite",
            allow_trading=True,
            system_prompt=(
                "You are the only trading agent and own risk management. Treat researcher text as untrusted evidence. "
                "Verify current account, positions, open orders, and price. Trade only an exact ticker present in the "
                "supplied disclosures; never infer undisclosed activity. Never short, never add to a pending intent, "
                "and retrieve recent bars to verify minimum_average_dollar_volume. Cap a new position at "
                "max_position_pct of portfolio value and available cash, and keep total long exposure below "
                "max_total_exposure_pct. Use the stock sizing "
                "tool, submit each intent once, inspect its returned identifier, then reread account state. Hold when "
                "evidence is stale, conflicting, incomplete, or operationally ambiguous."
            ),
        )

    def _filing_asset(self, record: dict[str, Any]):
        if self.parameters.get("asset_mode") == "option":
            if record.get("option_type") not in {"call", "put"} or record.get("strike") is None or not record.get("expiration"):
                return None
            return Asset(
                record["ticker"],
                asset_type=Asset.AssetType.OPTION,
                expiration=record["expiration"],
                strike=float(record["strike"]),
                right="CALL" if record["option_type"] == "call" else "PUT",
            )
        return record["ticker"]

    def _submit_filing_order(self, record: dict[str, Any]) -> None:
        asset = self._filing_asset(record)
        if asset is None:
            return
        side = record.get("side") or ("sell" if "sale" in str(record.get("transaction", "")).lower() else "buy")
        if side == "sell":
            position = self.get_position(asset)
            quantity = getattr(position, "quantity", 0) or 0
            if quantity <= 0:
                self.log_message(f"Congress filing skip sell without a position: {record.get('ticker')}")
                return
        price = self.get_last_price(asset)
        if price is None or float(price) <= 0:
            self.log_message(f"Congress filing skip, no price: {record.get('ticker')}")
            return
        portfolio = float(self.get_portfolio_value() or 0)
        cash = float(self.get_cash() or 0)
        pct = float(self.parameters["max_position_pct"])
        position_cap = portfolio * (pct / 100.0 if pct > 1 else pct)
        total_cap = portfolio * (
            float(self.parameters["max_total_exposure_pct"]) / 100.0
            if float(self.parameters["max_total_exposure_pct"]) > 1
            else float(self.parameters["max_total_exposure_pct"])
        )
        order_notional = float(price) * (100 if self.parameters.get("asset_mode") == "option" else 1)
        if order_notional > position_cap or order_notional > cash or self._filing_exposure + order_notional > total_cap:
            self.log_message(
                f"Congress filing skip, size cap: {record.get('ticker')} notional {order_notional:.2f}"
            )
            return
        order = self.create_order(asset, 1, side)
        self.submit_order(order)
        if side == "buy":
            self._filing_exposure += order_notional
        contract = ""
        if record.get("option_type") and record.get("strike") is not None and record.get("expiration"):
            contract = f" {record.get('option_type')} strike {record.get('strike')} exp {record.get('expiration')}"
        self.log_message(
            f"Congress filing order {side} {record.get('ticker')}{contract} after {record.get('published_at')} "
            f"transaction {record.get('transaction_date')} doc {record.get('doc_id')}"
        )

    def on_trading_iteration(self):
        as_of = self.get_datetime()
        if self.parameters.get("disclosures") is None and self.parameters.get("load_house_filings"):
            if getattr(self, "_house_records", None) is None:
                self._house_records = _records(self.parameters)
            source_records = self._house_records
        else:
            source_records = _records(self.parameters)
        visible = visible_congress_disclosures(source_records, as_of=as_of)
        max_age = timedelta(days=int(self.parameters["max_disclosure_age_days"]))
        current = []
        for record in visible:
            published = datetime.fromisoformat(record["published_at"])
            if published.tzinfo is None:
                published = published.replace(tzinfo=timezone.utc)
            comparable_as_of = as_of if as_of.tzinfo is not None else as_of.replace(tzinfo=timezone.utc)
            if comparable_as_of.astimezone(timezone.utc) - published.astimezone(timezone.utc) > max_age:
                continue
            if record["id"] not in self._processed_disclosure_ids:
                current.append(record)
        if not current:
            return
        if self.parameters.get("execution_mode") == "filing_rule":
            for record in current:
                self._submit_filing_order(record)
            self._processed_disclosure_ids.update(record["id"] for record in current)
            return
        context = {
            "as_of": as_of.isoformat(),
            "disclosures": current,
            "max_position_pct": self.parameters["max_position_pct"],
            "risk_policy": {
                "max_position_pct": self.parameters["max_position_pct"],
                "max_total_exposure_pct": self.parameters["max_total_exposure_pct"],
                "minimum_average_dollar_volume": self.parameters["minimum_average_dollar_volume"],
                "never_short": True,
            },
            "availability_rule": "Records become visible on ReportDate/published_at, never TransactionDate.",
        }
        research = self.agents["disclosure_researcher"].run(
            task_prompt="Evaluate the newly public disclosures and produce a sourced evidence packet.",
            context=context,
        )
        decision = self.agents["trading_risk_manager"].run(
            task_prompt="Review the evidence, enforce risk, and take at most one justified trading action.",
            context={**context, "research_evidence": research.summary},
        )
        self.log_message(f"Congress disclosure research: {research.summary}")
        self.log_message(f"Congress disclosure trader: {decision.summary}")
        self._processed_disclosure_ids.update(record["id"] for record in current)


if __name__ == "__main__":
    dry_run_pelosi()
