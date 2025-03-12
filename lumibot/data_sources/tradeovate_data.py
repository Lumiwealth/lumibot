import logging
import json
import threading
import time
from decimal import Decimal
from typing import Union
import requests
import websocket  # pip install websocket-client

from termcolor import colored
from lumibot.entities import Asset, Bars
from lumibot.data_sources import DataSource

class TradeovateData(DataSource):
    """
    Data source that connects to the Tradovate Market Data API.
    Note: Tradovate market data is delivered via WebSocket.
    """
    MIN_TIMESTEP = "minute"
    SOURCE = "Tradeovate"

    def __init__(self, config, get_headers_func=None):
        super().__init__()
        self.config = config
        # Use the market data WebSocket URL from config or default.
        self.ws_url = config.get("MD_WS_URL", "wss://md.tradovateapi.com/v1/websocket")
        # REST endpoint for market data.
        self.market_data_url = config.get("MD_URL", "https://md.tradovateapi.com/v1")
        # The get_headers_func is used for REST calls.
        self.get_headers_func = get_headers_func

    def get_chains(self, asset: Asset, quote: Asset = None) -> dict:
        logging.error(colored("Method 'get_chains' does not work with Tradovate.", "red"))
        return {}

    def get_historical_prices(
        self, asset, length, timestep="", timeshift=None, quote=None, exchange=None, include_after_hours=True
    ) -> Bars:
        """
        Retrieve historical chart data for the given asset via WebSocket using the md/getChart command.
        This method sends a WebSocket request to retrieve 'length' bars of historical data.
        
        Note: Tradovate provides historical chart data via WebSocket, not via a REST GET.
        """
        ws_url = self.ws_url  # Use the WebSocket URL from configuration.
        
        # Extract token from get_headers_func.
        token = None
        if self.get_headers_func:
            headers = self.get_headers_func(with_auth=True)
            auth = headers.get("Authorization", "")
            if auth.startswith("Bearer "):
                token = auth.split(" ")[1]
        if not token:
            logging.error(colored("No token available for WebSocket authorization.", "red"))
            return None

        # Build the chart request. Adjust chartDescription as needed.
        # For daily data, change "underlyingType" to "DailyBar"
        chart_description = {
            "underlyingType": "DailyBar",  # Adjust as needed: "MinuteBar" for minute bars, "DailyBar" for day bars, etc.
            "elementSize": 1,
            "elementSizeUnit": "UnderlyingUnits"
        }
        time_range = {
            "asMuchAsElements": length
        }
        request_body = {
            "symbol": asset.symbol,
            "chartDescription": chart_description,
            "timeRange": time_range
        }
        request_message = f"md/getChart\n1\n\n" + json.dumps(request_body)

        last_bars = None
        finished = threading.Event()

        def on_message(ws, message):
            nonlocal last_bars
            logging.info(colored(f"Raw message: {repr(message)}", "blue"))
            if not message.strip():
                return
            # Ignore open and heartbeat frames.
            if message.strip() in ["o", "h"]:
                if message.strip() == "h":
                    ws.send("[]")  # respond to heartbeat if needed.
                return
            # Remove leading protocol letter if present.
            if message[0] in ['a', 'o', 'h', 'c']:
                payload_str = message[1:]
            else:
                payload_str = message
            if not payload_str.strip():
                return
            try:
                data = json.loads(payload_str)
            except json.JSONDecodeError as e:
                logging.error(colored(f"JSONDecodeError: {e} for payload: {repr(payload_str)}", "red"))
                return
            # Expect a message with event "chart"
            if isinstance(data, dict) and data.get("e") == "chart" and "d" in data:
                charts = data["d"].get("charts", [])
                if charts and isinstance(charts, list):
                    bars_data = charts[0].get("bars", [])
                    if bars_data:
                        last_bars = bars_data
                        logging.info(colored(f"Received historical bars for {asset.symbol}", "green"))
                        ws.close()
                        finished.set()

        def on_error(ws, error):
            logging.error(colored(f"WebSocket error: {error}", "red"))
            finished.set()

        def on_close(ws, close_status_code, close_msg):
            logging.info(colored("WebSocket connection closed.", "yellow"))
            finished.set()

        def on_open(ws):
            logging.info(colored("WebSocket connection opened.", "green"))
            # Send authorization message.
            auth_message = f"authorize\n0\n\n{token}"
            ws.send(auth_message)
            time.sleep(1)  # Allow time for authorization.
            logging.info(colored(f"Subscribing to historical chart data for symbol: {asset.symbol}", "blue"))
            logging.info(colored(f"Sending subscription: {request_message}", "blue"))
            ws.send(request_message)

        ws_app = websocket.WebSocketApp(
            ws_url,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
            on_open=on_open
        )
        ws_thread = threading.Thread(target=ws_app.run_forever)
        ws_thread.daemon = True
        ws_thread.start()

        finished.wait(timeout=10)
        if not finished.is_set():
            logging.error(colored("Timeout waiting for historical chart data via WebSocket.", "red"))
            ws_app.close()

        if last_bars is not None:
            return Bars(last_bars)
        else:
            return None

    def get_contract_id(self, symbol: str):
        """
        Retrieve the contract ID for a given symbol using the Trading REST API.
        Uses the /contract/find endpoint, and if no result is returned as a list,
        checks if the response is a dict.
        """
        trading_api_url = self.config.get("TRADING_API_URL", "https://demo.tradovateapi.com/v1")
        url = f"{trading_api_url}/contract/find?name={symbol}"
        headers = self.get_headers_func(with_auth=True) if self.get_headers_func else {"Accept": "application/json"}
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            contract = None
            if isinstance(data, list):
                if data:
                    contract = data[0]
            elif isinstance(data, dict):
                contract = data
            if contract and "id" in contract:
                contract_id = contract.get("id")
                logging.info(colored(f"Found contract id {contract_id} for symbol {symbol}", "green"))
                return contract_id
            else:
                logging.error(colored(f"No contract found for symbol {symbol} via find endpoint.", "red"))
                # Optionally, try the suggest endpoint as fallback.
                suggest_url = f"{trading_api_url}/contract/suggest?name={symbol}"
                response = requests.get(suggest_url, headers=headers)
                response.raise_for_status()
                data = response.json()
                if isinstance(data, list) and data:
                    contract = data[0]
                    contract_id = contract.get("id")
                    logging.info(colored(f"(Suggest) Found contract id {contract_id} for symbol {symbol}", "green"))
                    return contract_id
                logging.error(colored(f"No contract found for symbol {symbol} via suggest endpoint.", "red"))
                return None
        except Exception as e:
            logging.error(colored(f"Error retrieving contract id for symbol {symbol}: {e}", "red"))
            return None

    def get_last_price(self, asset, quote=None, exchange=None) -> Union[float, Decimal, None]:
        """
        Retrieve the most recent price for the given asset via WebSocket.
        This method first retrieves the contract ID for the asset's symbol, then subscribes
        to market data using that contract ID.
        """
        ws_url = self.ws_url
        # Extract token from get_headers_func.
        token = None
        if self.get_headers_func:
            headers = self.get_headers_func(with_auth=True)
            auth = headers.get("Authorization", "")
            if auth.startswith("Bearer "):
                token = auth.split(" ")[1]
        if not token:
            logging.error(colored("No token available for WebSocket authorization.", "red"))
            return None

        # Retrieve contract id for asset.symbol.
        contract_id = self.get_contract_id(asset.symbol)
        if contract_id is None:
            logging.error(colored(f"Failed to retrieve contract id for symbol {asset.symbol}", "red"))
            return None

        last_price = None
        finished = threading.Event()

        def on_message(ws, message):
            nonlocal last_price
            logging.info(colored(f"Raw message: {repr(message)}", "blue"))
            if not message.strip():
                return
            trimmed = message.strip()
            if trimmed in ["o", "h"]:
                if trimmed == "h":
                    ws.send("[]")
                return
            # Strip the leading protocol letter if present.
            if message[0] in ['a', 'o', 'h', 'c']:
                payload_str = message[1:]
            else:
                payload_str = message
            if not payload_str.strip():
                return
            try:
                data = json.loads(payload_str)
            except json.JSONDecodeError as e:
                logging.error(colored(f"JSONDecodeError: {e} for payload: {repr(payload_str)}", "red"))
                return
            if isinstance(data, list) and data and isinstance(data[0], dict):
                if data[0].get("e") == "md" and "d" in data[0]:
                    quotes = data[0]["d"].get("quotes", [])
                    if quotes:
                        trade_entry = quotes[0]["entries"].get("Trade", {})
                        if "price" in trade_entry:
                            last_price = trade_entry["price"]
                            logging.info(colored(f"Received last price for {asset.symbol}: {last_price}", "green"))
                            ws.close()
                            finished.set()

        def on_error(ws, error):
            logging.error(colored(f"WebSocket error: {error}", "red"))
            finished.set()

        def on_close(ws, close_status_code, close_msg):
            logging.info(colored("WebSocket connection closed.", "yellow"))
            finished.set()

        def on_open(ws):
            logging.info(colored("WebSocket connection opened.", "green"))
            # Send authorization message.
            auth_message = f"authorize\n0\n\n{token}"
            ws.send(auth_message)
            time.sleep(1)
            logging.info(colored(f"Subscribing to market data for contract id: {contract_id}", "blue"))
            # Build subscription message using contract id.
            subscribe_body = json.dumps({"symbol": contract_id})
            subscribe_message = f"md/subscribeQuote\n1\n\n{subscribe_body}"
            logging.info(colored(f"Sending subscription: {subscribe_message}", "blue"))
            ws.send(subscribe_message)

        ws_app = websocket.WebSocketApp(
            ws_url,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
            on_open=on_open
        )
        ws_thread = threading.Thread(target=ws_app.run_forever)
        ws_thread.daemon = True
        ws_thread.start()
        finished.wait(timeout=5)
        if not finished.is_set():
            logging.error(colored("Timeout waiting for last price via WebSocket.", "red"))
            ws_app.close()
        return float(last_price) if last_price is not None else None