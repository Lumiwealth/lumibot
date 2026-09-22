"""Live GET plus POST proof. The buy happens only when the echo returns the filing id."""

from lumibot.components.agents.web_tools import WebClient
from lumibot.strategies.strategy import Strategy

_PDF_URL = "https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/20033725.pdf"
_ECHO_URL = "https://postman-echo.com/post"
_EXPECTED_DOC_ID = "20033725"
_USER_AGENT = "Lumiwealth research botspot.trade"


class AIPublicWebFetchStrategy(Strategy):
    parameters = {
        "pdf_url": _PDF_URL,
        "echo_url": _ECHO_URL,
        "expected_doc_id": _EXPECTED_DOC_ID,
        "trade_symbol": "SPY",
    }

    def initialize(self):
        self.sleeptime = "1D"

    def on_trading_iteration(self):
        if getattr(self, "_fetch_done", False):
            return
        self._fetch_done = True
        expected = str(self.parameters["expected_doc_id"])
        client = WebClient(timeout_seconds=60, max_response_bytes=8_000_000)
        try:
            downloaded = client.request(
                "GET",
                str(self.parameters["pdf_url"]),
                headers={"User-Agent": _USER_AGENT},
                max_response_bytes=8_000_000,
            )
            posted = client.request(
                "POST",
                str(self.parameters["echo_url"]),
                json_body={
                    "doc_id": expected,
                    "content_sha256": downloaded.get("content_sha256"),
                    "ok": bool(downloaded.get("ok")),
                },
                headers={"User-Agent": _USER_AGENT, "Accept": "application/json"},
            )
        finally:
            client.close()
        echoed = ((posted.get("json") or {}).get("json") or {}).get("doc_id")
        self.log_message(
            f"Public fetch GET ok {downloaded.get('ok')} bytes {downloaded.get('content_length')} "
            f"POST status {posted.get('status_code')} echoed_doc_id {echoed}"
        )
        if downloaded.get("ok") and echoed == expected:
            symbol = str(self.parameters["trade_symbol"])
            price = self.get_last_price(symbol)
            if price is None or float(price) <= 0:
                self.log_message(f"Public fetch decision skip, no price: {symbol}")
                return
            self.submit_order(self.create_order(symbol, 1, "buy"))
            self.log_message(f"Public fetch decision buy {symbol} because echoed doc_id {echoed}")
            return
        self.log_message("Public fetch decision skip")
