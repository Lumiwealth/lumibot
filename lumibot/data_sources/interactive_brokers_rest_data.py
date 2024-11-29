import logging
from termcolor import colored
from lumibot.entities import Asset, Bars

from .data_source import DataSource
import subprocess
import os
import time
import requests
import urllib3
from datetime import datetime, timedelta
import pandas as pd

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

TYPE_MAP = dict(
    stock="STK",
    option="OPT",
    future="FUT",
    forex="CASH",
    index="IND",
    multileg="BAG",
)


class InteractiveBrokersRESTData(DataSource):
    """
    Data source that connects to the Interactive Brokers REST API.
    """

    MIN_TIMESTEP = "minute"
    SOURCE = "InteractiveBrokersREST"

    def __init__(self, config):
        if config["API_URL"] is None:
            self.port = "4234"
            self.base_url = f"https://localhost:{self.port}/v1/api"
        else:
            self.api_url = config["API_URL"]
            self.base_url = f"{self.api_url}/v1/api"

        self.account_id = config["ACCOUNT_ID"] if "ACCOUNT_ID" in config else None

        # Check if we are running on a server
        running_on_server = (
            config["RUNNING_ON_SERVER"]
            if config["RUNNING_ON_SERVER"] is not None
            else ""
        )
        if running_on_server.lower() == "true" or hasattr(self, "api_url"):
            self.running_on_server = True
        else:
            self.running_on_server = False

        self.start(config["IB_USERNAME"], config["IB_PASSWORD"])

    def start(self, ib_username, ib_password):
        if not self.running_on_server:
            # Run the Docker image with the specified environment variables and port mapping
            if (
                not subprocess.run(
                    ["docker", "--version"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                ).returncode
                == 0
            ):
                logging.error(colored("Docker is not installed.", "red"))
                return
            # Color the text green
            logging.info(
                colored("Connecting to Interactive Brokers REST API...", "green")
            )

            inputs_dir = "/srv/clientportal.gw/root/conf.yaml"
            env_variables = {
                "IBEAM_ACCOUNT": ib_username,
                "IBEAM_PASSWORD": ib_password,
                "IBEAM_GATEWAY_BASE_URL": f"https://localhost:{self.port}",
                "IBEAM_LOG_TO_FILE": False,
                "IBEAM_REQUEST_RETRIES": 1,
                "IBEAM_PAGE_LOAD_TIMEOUT": 30,
                "IBEAM_INPUTS_DIR": inputs_dir,
            }

            env_args = [f"--env={key}={value}" for key, value in env_variables.items()]
            conf_path = os.path.join(
                os.path.dirname(os.path.dirname(__file__)), "resources", "conf.yaml"
            )
            volume_mount = f"{conf_path}:{inputs_dir}"

            subprocess.run(
                ["docker", "rm", "-f", "lumibot-client-portal"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            subprocess.run(
                [
                    "docker",
                    "run",
                    "-d",
                    "--name",
                    "lumibot-client-portal",
                    *env_args,
                    "-p",
                    f"{self.port}:{self.port}",
                    "-v",
                    volume_mount,
                    "voyz/ibeam",
                ],
                stdout=subprocess.DEVNULL,
                text=True,
            )

            # check if authenticated
            time.sleep(15)

        while not self.is_authenticated():
            logging.info(
                colored(
                    "Not connected to API server yet. Waiting for Interactive Brokers API Portal to start...",
                    "yellow",
                )
            )
            logging.info(
                colored(
                    "Waiting for another 10 seconds before checking again...", "yellow"
                )
            )
            time.sleep(10)

        # Set self.account_id
        self.fetch_account_id()

        logging.info(colored("Connected to the Interactive Brokers API", "green"))
        self.suppress_warnings()

    def suppress_warnings(self):
        # Suppress weird server warnings
        url = f"{self.base_url}/iserver/questions/suppress"
        json = {"messageIds": ["o451", "o383", "o354", "o163"]}

        self.post_to_endpoint(url, json=json, description="Suppressing server warnings", allow_fail=False)
    
    def fetch_account_id(self):
        if self.account_id is not None:
            return  # Account ID already set

        url = f"{self.base_url}/portfolio/accounts"

        response = self.get_from_endpoint(
            url, "Fetching Account ID", allow_fail=False
        )
        self.last_portfolio_ping = datetime.now()
        self.account_id = response[0]["id"]

    def is_authenticated(self):
        url = f"{self.base_url}/iserver/accounts"
        response = self.get_from_endpoint(
            url, "Auth Check", silent=True, allow_fail=False
        )
        if response is None or 'error' in response:
            return False
        else:
            return True

    def ping_iserver(self):
        url = f"{self.base_url}/iserver/accounts"
        response = self.get_from_endpoint(
            url, "Auth Check", silent=True, allow_fail=False
        )

        if response is None or 'error' in response:
            return False
        else:
            return True

    def ping_portfolio(self):
        url = f"{self.base_url}/portfolio/accounts"
        response = self.get_from_endpoint(
            url, "Auth Check", silent=True
        )
        if response is None or 'error' in response:
            return False
        else:
            return True

    def get_contract_details(self, conId):
        self.ping_iserver()

        url = f"{self.base_url}/iserver/contract/{conId}/info"
        response = self.get_from_endpoint(url, "Getting contract details")
        return response

    def get_contract_rules(self, conid):
        """
        Get the contract rules for a given contract ID (conid) and whether it is a buy or sell.

        Parameters
        ----------
        conid : int
            The contract ID.
        isBuy : bool
            True if it is a buy order, False if it is a sell order.

        Returns
        -------
        dict
            The contract rules if the request is successful, None otherwise.
        """
        self.ping_iserver()

        url = f"{self.base_url}/iserver/contract/{conid}/info-and-rules"

        response = self.get_from_endpoint(url, "Getting Contract Rules")

        if response is not None and "error" in response:
            logging.error(
                colored(f"Failed to get contract rules: {response['error']}", "red")
            )
            return None

        return response

    def get_account_balances(self):
        """
        Retrieves the account balances for a given account ID.
        """
        self.ping_portfolio()

        # Define the endpoint URL for fetching account balances
        url = f"{self.base_url}/portfolio/{self.account_id}/ledger"
        response = self.get_from_endpoint(
            url, "Getting account balances", allow_fail=False
        )

        # Error handle
        if response is not None and "error" in response:
            logging.error(
                colored(
                    f"Couldn't get account balances. Error: {response['error']}",
                    "red",
                )
            )
            return None

        return response

    def handle_http_errors(self, response, description):
        to_return = None
        re_msg = None
        is_error = False

        if response.text:
            try:
                response_json = response.json()
            except ValueError:
                logging.error(
                    colored(f"Invalid JSON response", "red")
                )
                response_json = {}
        else:
            response_json = {}

        status_code = response.status_code

        if isinstance(response_json, dict): 
            error_message = response_json.get("error", "") or response_json.get("message", "")
        else:
            error_message = ""
        
        # Check if this is an order confirmation request
        if "Are you sure you want to submit this order?" in response.text:
            response_json = response.json()
            orders = []
            for order in response_json:
                if isinstance(order, dict) and 'id' in order:
                    confirm_url = f"{self.base_url}/iserver/reply/{order['id']}"
                    confirm_response = self.post_to_endpoint(
                        confirm_url, 
                        {"confirmed": True},
                        description="Confirming Order",
                        silent=True,
                        allow_fail=True
                    )
                    if confirm_response:
                        orders.extend(confirm_response)
                        status_code = 200
            response_json = orders
        
        if 'xcredserv comm failed during getEvents due to Connection refused':
            retrying = True
            re_msg = f"Task {description} failed: The server is undergoing maintenance. Should fix itself soon"

        elif 'Please query /accounts first' in error_message:
            self.ping_iserver()
            retrying = True
            re_msg = f"Task {description} failed: Lumibot got Deauthenticated"

        elif "no bridge" in error_message.lower() or "not authenticated" in error_message.lower():
            retrying = True
            re_msg = f"Task {description} failed: Not Authenticated"

        elif 200 <= status_code < 300:
            to_return = response_json
            retrying = False

        elif status_code == 429:
            retrying = True
            re_msg = f"Task {description} failed: You got rate limited"

        elif status_code == 503:
            re_msg = f"Task {description} failed: Internal server error. Should fix itself soon"
            retrying = True

        elif status_code == 500:
            to_return = response_json
            is_error = True
            retrying = False

        elif status_code == 410:
            retrying = True
            re_msg = f"Task {description} failed: The bridge blew up"

        elif 400 <= status_code < 500:
            to_return = response_json
            is_error = True
            retrying = False
        
        else: 
            retrying = False

        return (retrying, re_msg, is_error, to_return)

    def get_from_endpoint(self, url, description="", silent=False, allow_fail=True):
        to_return = None
        retries = 0
        retrying = True

        try:
            while retrying or not allow_fail:
                response = requests.get(url, verify=False)
                retrying, re_msg, is_error, to_return = self.handle_http_errors(response, description)
                
                if re_msg is not None:
                    if not silent and retries == 0:
                        logging.warning(colored(f'{re_msg}. Retrying...', "yellow"))

                elif is_error:
                    if not silent and retries == 0:
                        logging.error(colored(f"Task {description} failed: {to_return}", "red"))

                else:
                    allow_fail = True
                
                retries+=1

        except requests.exceptions.RequestException as e:
            message = f"Error: {description}. Exception: {e}"
            if not silent:
                logging.error(colored(message, "red"))
            to_return = {"error": message}

        return to_return

    def post_to_endpoint(self, url, json: dict, description="", silent=False, allow_fail=True):
        to_return = None
        retries = 0
        retrying = True

        try:
            while retrying or not allow_fail:
                response = requests.post(url, json=json, verify=False)
                retrying, re_msg, is_error, to_return = self.handle_http_errors(response, description)
                
                if re_msg is not None:
                    if not silent and retries == 0:
                        logging.warning(colored(f'{re_msg}. Retrying...', "yellow"))

                elif is_error:
                    if not silent and retries == 0:
                        logging.error(colored(f"Task {description} failed: {to_return}", "red"))

                else:
                    allow_fail = True

                retries += 1

        except requests.exceptions.RequestException as e:
            message = f"Error: {description}. Exception: {e}"
            if not silent:
                logging.error(colored(message, "red"))
            to_return = {"error": message}

        return to_return

    def delete_to_endpoint(self, url, description="", silent=False, allow_fail=True):
        to_return = None
        retries = 0
        retrying = True

        try:
            while retrying or not allow_fail:
                response = requests.delete(url, verify=False)
                retrying, re_msg, is_error, to_return = self.handle_http_errors(response, description)
                
                if re_msg is not None:
                    if not silent and retries == 0:
                        logging.warning(colored(f'{re_msg}. Retrying...', "yellow"))

                elif is_error:
                    if not silent and retries == 0:
                        logging.error(colored(f"Task {description} failed: {to_return}", "red"))

                else:
                    allow_fail = True

                retries += 1

        except requests.exceptions.RequestException as e:
            message = f"Error: {description}. Exception: {e}"
            if not silent:
                logging.error(colored(message, "red"))
            to_return = {"error": message}

        return to_return

    def get_open_orders(self):
        self.ping_iserver()

        # Clear cache with force=true
        url = f"{self.base_url}/iserver/account/orders?force=true"
        response = self.get_from_endpoint(url, "Getting open orders", allow_fail=False)
        
        # Fetch
        url = f"{self.base_url}/iserver/account/orders?&accountId={self.account_id}&filters=Submitted,PreSubmitted"
        response = self.get_from_endpoint(
            url, "Getting open orders", allow_fail=False
        )

        # Filters don't work, we'll filter on our own
        filtered_orders = []
        if (
            isinstance(response, dict)
            and "orders" in response
            and isinstance(response["orders"], list)
        ):
            for order in response["orders"]:
                if isinstance(order, dict) and order.get("status") not in [
                    "Cancelled",
                    "Filled",
                ]:
                    filtered_orders.append(order)

        return filtered_orders

    def get_broker_all_orders(self):
        self.ping_iserver()

        # Clear cache with force=true
        url = f"{self.base_url}/iserver/account/orders?force=true"
        response = self.get_from_endpoint(url, "Getting open orders", allow_fail=False)

        # Fetch
        url = f"{self.base_url}/iserver/account/orders?&accountId={self.account_id}"
        response = self.get_from_endpoint(
            url, "Getting open orders", allow_fail=False
        )

        if 'orders' in response and isinstance(response['orders'], list):
            return [order for order in response['orders'] if order.get('totalSize', 0) != 0]
        
        return []

    def get_order_info(self, orderid):
        self.ping_iserver()

        url = f"{self.base_url}/iserver/account/order/status/{orderid}"
        response = self.get_from_endpoint(url, "Getting Order Info", allow_fail=False, silent=True)
        return response

    def execute_order(self, order_data):
        if order_data is None:
            logging.debug(colored("Failed to get order data.", "red"))
            return None

        self.ping_iserver()

        url = f"{self.base_url}/iserver/account/{self.account_id}/orders"
        response = self.post_to_endpoint(url, order_data, description="Executing order")
                
        if isinstance(response, list) and "order_id" in response[0]:
            # success
            return response

        elif response is not None and "error" in response:
            logging.error(
                colored(f"Failed to execute order: {response['error']}", "red")
            )
            return None
        elif response is not None and "message" in response:
            logging.error(
                colored(f"Failed to execute order: {response['message']}", "red")
            )
            return None
        elif response is not None:
            logging.error(colored(f"Failed to execute order: {response}", "red"))
        else:
            logging.error(colored(f"Failed to execute order: {order_data}", "red"))

    def delete_order(self, order):
        self.ping_iserver()
        orderId = order.identifier
        url = f"{self.base_url}/iserver/account/{self.account_id}/order/{orderId}"
        status = self.delete_to_endpoint(url, description=f"Deleting order {orderId}")
        if status:
            logging.info(
                colored(f"Order with ID {orderId} canceled successfully.", "green")
            )
        else:
            logging.error(colored(f"Failed to delete order with ID {orderId}.", "red"))

    def get_positions(self):
        """
        Retrieves the current positions for a given account ID.
        """
        # invalidate cache
        """
        url = f'{self.base_url}/portfolio/{self.account_id}/positions/invalidate'
        response = self.post_to_endpoint(url, {})
        """
        self.ping_portfolio()

        url = f"{self.base_url}/portfolio/{self.account_id}/positions"
        response = self.get_from_endpoint(
            url, "Getting account positions", allow_fail=False
        )

        # Error handle
        if response is not None and "error" in response:
            logging.error(
                colored(
                    f"Couldn't get account positions. Error: {response['error']}",
                    "red",
                )
            )
            return None

        return response

    def stop(self):
        # Check if the Docker image is already running
        if self.running_on_server:
            return

        subprocess.run(
            ["docker", "rm", "-f", "lumibot-client-portal"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    def get_chains(self, asset: Asset, quote=None) -> dict:
        """
        - `Multiplier` (str) eg: `100`
        - 'Chains' - paired Expiration/Strike info to guarentee that the strikes are valid for the specific
                     expiration date.
                     Format:
                       chains['Chains']['CALL'][exp_date] = [strike1, strike2, ...]
                     Expiration Date Format: 2023-07-31
        """

        chains = {
            "Multiplier": asset.multiplier,
            "Exchange": "unknown",
            "Chains": {"CALL": {}, "PUT": {}},
        }
        logging.info(
            "This task is extremely slow. If you still wish to use it, prepare yourself for a long wait."
        )
        self.ping_iserver()

        url_for_dates = f"{self.base_url}/iserver/secdef/search?symbol={asset.symbol}"
        response = self.get_from_endpoint(url_for_dates, "Getting Option Dates")

        if response and isinstance(response, list) and "conid" in response[0]:
            conid = response[0]["conid"]
        else:
            logging.error("Failed to get conid from response")
            return {}

        option_dates = None
        if response and isinstance(response, list) and "sections" in response[0]:
            for section in response[0]["sections"]:
                if "secType" in section and section["secType"] == "OPT":
                    option_dates = section["months"]
                    break
        else:
            logging.error("Failed to get sections from response")
            return {}

        # Array of options dates for asset
        if option_dates:
            months = option_dates.split(";")  # in MMMYY
        else:
            logging.error("Option dates are None")
            return {}

        for month in months:
            # TODO &exchange could be added
            url_for_strikes = f"{self.base_url}/iserver/secdef/strikes?sectype=OPT&conid={conid}&month={month}"
            strikes = self.get_from_endpoint(url_for_strikes, "Getting Strikes")

            if strikes and "call" in strikes:
                for strike in strikes["call"]:
                    url_for_expiry = f"{self.base_url}/iserver/secdef/info?conid={conid}&sectype=OPT&month={month}&right=C&strike={strike}"
                    contract_info = self.get_from_endpoint(
                        url_for_expiry, "Getting expiration Date"
                    )
                    if (
                        contract_info
                        and isinstance(contract_info, list)
                        and len(contract_info) > 0
                        and "maturityDate" in contract_info[0]
                    ):
                        expiry_date = contract_info[0]["maturityDate"]
                        expiry_date = datetime.strptime(expiry_date, "%Y%m%d").strftime(
                            "%Y-%m-%d"
                        )  # convert to yyyy-mm-dd
                        if expiry_date not in chains["Chains"]["CALL"]:
                            chains["Chains"]["CALL"][expiry_date] = []
                        chains["Chains"]["CALL"][expiry_date].append(strike)
                    else:
                        logging.error("Invalid contract_info format")
                        return {}

            if strikes and "put" in strikes:
                for strike in strikes["put"]:
                    url_for_expiry = f"{self.base_url}/iserver/secdef/info?conid={conid}&sectype=OPT&month={month}&right=P&strike={strike}"
                    contract_info = self.get_from_endpoint(
                        url_for_expiry, "Getting expiration Date"
                    )
                    if (
                        contract_info
                        and isinstance(contract_info, list)
                        and len(contract_info) > 0
                        and "maturityDate" in contract_info[0]
                    ):
                        expiry_date = contract_info[0]["maturityDate"]
                        expiry_date = datetime.strptime(expiry_date, "%Y%m%d").strftime(
                            "%Y-%m-%d"
                        )  # convert to yyyy-mm-dd
                        if expiry_date not in chains["Chains"]["PUT"]:
                            chains["Chains"]["PUT"][expiry_date] = []
                        chains["Chains"]["PUT"][expiry_date].append(strike)
                    else:
                        logging.error("Invalid contract_info format")
                        return {}

        return chains

    def get_historical_prices(
        self,
        asset,
        length,
        timestep="",
        timeshift=None,
        quote=None,
        exchange=None,
        include_after_hours=True,
    ) -> Bars:
        """
        Get bars for a given asset

        Parameters
        ----------
        asset : Asset
            The asset to get the bars for.
        length : int
            The number of bars to get.
        timestep : str
            The timestep to get the bars at. For example, "minute" or "day".
        timeshift : datetime.timedelta
            The amount of time to shift the bars by. For example, if you want the bars from 1 hour ago to now,
            you would set timeshift to 1 hour.
        quote : Asset
            The quote asset to get the bars for.
        exchange : str
            The exchange to get the bars for.
        include_after_hours : bool
            Whether to include after hours data.
        """
        self.ping_iserver()

        if isinstance(asset, str):
            asset = Asset(symbol=asset)

        if not timestep:
            timestep = self.get_timestep()

        if timeshift:
            start_time = (datetime.now() - timeshift).strftime("%Y%m%d-%H:%M:%S")
        else:
            start_time = datetime.now().strftime("%Y%m%d-%H:%M:%S")

        conid = self.get_conid_from_asset(asset=asset)

        # Determine the period based on the timestep and length
        # TODO fix wtvr this is
        try:
            timestep_value = int(timestep.split()[0])
        except ValueError:
            timestep_value = 1

        if "minute" in timestep:
            period = f"{length * timestep_value}mins"
            timestep = f"{timestep_value}mins"
        elif "hour" in timestep:
            period = f"{length * timestep_value}h"
            timestep = f"{timestep_value}h"
        elif "day" in timestep:
            period = f"{length * timestep_value}d"
            timestep = f"{timestep_value}d"
        elif "week" in timestep:
            period = f"{length * timestep_value}w"
            timestep = f"{timestep_value}w"
        elif "month" in timestep:
            period = f"{length * timestep_value}m"
            timestep = f"{timestep_value}m"
        elif "year" in timestep:
            period = f"{length * timestep_value}y"
            timestep = f"{timestep_value}y"
        else:
            logging.error(colored(f"Unsupported timestep: {timestep}", "red"))
            return Bars(
                pd.DataFrame(
                    columns=["timestamp", "open", "high", "low", "close", "volume"]
                ),
                self.SOURCE,
                asset,
                raw=pd.DataFrame(
                    columns=["timestamp", "open", "high", "low", "close", "volume"]
                ),
                quote=quote,
            )

        url = f"{self.base_url}/iserver/marketdata/history?conid={conid}&period={period}&bar={timestep}&outsideRth={include_after_hours}&startTime={start_time}"

        if exchange:
            url += f"&exchange={exchange}"

        result = self.get_from_endpoint(url, "Getting Historical Prices")

        if result and "error" in result:
            logging.error(
                colored(f"Error getting historical prices: {result['error']}", "red")
            )
            return Bars(
                pd.DataFrame(
                    columns=["timestamp", "open", "high", "low", "close", "volume"]
                ),
                self.SOURCE,
                asset,
                raw=pd.DataFrame(
                    columns=["timestamp", "open", "high", "low", "close", "volume"]
                ),
                quote=quote,
            )

        if not result or not result["data"]:
            logging.error(
                colored(
                    f"Failed to get historical prices for {asset.symbol}, result was: {result}",
                    "red",
                )
            )
            return Bars(
                pd.DataFrame(
                    columns=["timestamp", "open", "high", "low", "close", "volume"]
                ),
                self.SOURCE,
                asset,
                raw=pd.DataFrame(
                    columns=["timestamp", "open", "high", "low", "close", "volume"]
                ),
                quote=quote,
            )

        # Create a DataFrame from the data
        df = pd.DataFrame(result["data"], columns=["t", "o", "h", "l", "c", "v"])

        # Rename columns to match the expected format
        df.rename(
            columns={
                "t": "timestamp",
                "o": "open",
                "h": "high",
                "l": "low",
                "c": "close",
                "v": "volume",
            },
            inplace=True,
        )

        # Convert timestamp to datetime and set as index
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        df["timestamp"] = (
            df["timestamp"].dt.tz_localize("UTC").dt.tz_convert("America/New_York")
        )
        df.set_index("timestamp", inplace=True)

        """
        # Add dividend and stock_splits columns with default values
        df['dividend'] = 0.0
        df['stock_splits'] = 0.0
        """

        bars = Bars(df, self.SOURCE, asset, raw=df, quote=quote)

        return bars

    def get_last_price(self, asset, quote=None, exchange=None) -> float | None:
        field = "last_price"
        response = self.get_market_snapshot(asset, [field])  # TODO add exchange

        if response is None or field not in response:
            if asset.asset_type in ["option", "future"]:
                logging.debug(
                    f"Failed to get {field} for asset {asset.symbol} with strike {asset.strike} and expiration date {asset.expiration}"
                )
            else:
                logging.debug(
                    f"Failed to get {field} for asset {asset.symbol} of type {asset.asset_type}"
                )
            return None

        price = response[field]

        # Remove the 'C' prefix if it exists
        if isinstance(price, str) and price.startswith("C"):
            price = float(price[1:])

        return float(price)

    def get_conid_from_asset(self, asset: Asset):
        self.ping_iserver()
        # Get conid of underlying
        url = f"{self.base_url}/iserver/secdef/search?symbol={asset.symbol}"
        response = self.get_from_endpoint(url, "Getting Underlying conid")

        if (
            isinstance(response, list)
            and len(response) > 0
            and isinstance(response[0], dict)
            and "conid" in response[0]
        ):
            underlying_conid = int(response[0]["conid"])
        else:
            logging.error(
                colored(
                    f"Failed to get conid of asset: {asset.symbol} of type {asset.asset_type}",
                    "red",
                )
            )
            logging.error(colored(f"Response: {response}", "red"))
            return None

        if asset.asset_type == "option":
            exchange = next(
                (section["exchange"] for section in response[0]["sections"] if section["secType"] == "OPT"),
                None,
            )
            return self._get_conid_for_derivative(
                underlying_conid,
                asset,
                sec_type="OPT",
                exchange=exchange,
                additional_params={
                    "right": asset.right,
                    "strike": asset.strike,
                },
            )
        elif asset.asset_type == "future":
            exchange = next(
                (section["exchange"] for section in response[0]["sections"] if section["secType"] == "FUT"),
                None,
            )
            return self._get_conid_for_derivative(
                underlying_conid,
                asset,
                exchange=exchange,
                sec_type="FUT",
                additional_params={
                    "multiplier": asset.multiplier,
                },
            )
        elif asset.asset_type in ["stock", "forex", "index"]:
            return underlying_conid

    def _get_conid_for_derivative(
        self,
        underlying_conid: int,
        asset: Asset,
        sec_type: str,
        additional_params: dict,
        exchange: str | None,
    ):
        expiration_date = asset.expiration.strftime("%Y%m%d")
        expiration_month = asset.expiration.strftime("%b%y").upper()  # in MMMYY

        params = {
            "conid": underlying_conid,
            "sectype": sec_type,
            "month": expiration_month,
            "exchange": exchange
        }
        params.update(additional_params)
        query_string = "&".join(f"{key}={value}" for key, value in params.items() if value is not None)

        url_for_expiry = f"{self.base_url}/iserver/secdef/info?{query_string}"
        contract_info = self.get_from_endpoint(
            url_for_expiry, f"Getting {sec_type} Contract Info", silent=True
        )

        matching_contract = None
        if contract_info:
            matching_contract = next(
                (
                    contract
                    for contract in contract_info
                    if isinstance(contract, dict)
                    and contract.get("maturityDate") == expiration_date
                ),
                None,
            )

        if matching_contract is None:
            logging.debug(
                colored(
                    f"No matching contract found for asset: {asset.symbol} with expiration date {expiration_date}",
                    "red",
                )
            )
            return None

        return matching_contract["conid"]

    def query_greeks(self, asset: Asset) -> dict:
        greeks = self.get_market_snapshot(asset, ["vega", "theta", "gamma", "delta"])
        return greeks if greeks is not None else {}

    def get_market_snapshot(self, asset: Asset, fields: list):
        all_fields = {
            "84": "bid",
            "85": "ask_size",
            "86": "ask",
            "88": "bid_size",
            "31": "last_price",
            "7283": "implied_volatility",
            "7311": "vega",
            "7310": "theta",
            "7308": "delta",
            "7309": "gamma",
            # https://www.interactivebrokers.com/campus/ibkr-api-page/webapi-ref/#tag/Trading-Market-Data/paths/~1iserver~1marketdata~1snapshot/get
        }
        self.ping_iserver()

        conId = self.get_conid_from_asset(asset)
        if conId is None:
            return None

        fields_to_get = []
        for identifier, name in all_fields.items():
            if name in fields:
                fields_to_get.append(identifier)

        fields_str = ",".join(str(field) for field in fields_to_get)

        url = f"{self.base_url}/iserver/marketdata/snapshot?conids={conId}&fields={fields_str}"

        # If fields are missing, fetch again
        max_retries = 500
        retries = 0
        missing_fields = True

        response = None
        while missing_fields and retries < max_retries:
            if retries >= 3:
                time.sleep(5)
            response = self.get_from_endpoint(url, "Getting Market Snapshot")
            retries += 1
            missing_fields = False
            for field in fields_to_get:
                if (
                    response
                    and isinstance(response, list)
                    and len(response) > 0
                    and not field in response[0]
                ):
                    missing_fields = True
                    break

        # return only what was requested
        output = {}

        if (
            response
            and isinstance(response, list)
            and len(response) > 0
            and isinstance(response[0], dict)
        ):
            for key, value in response[0].items():
                if key in fields_to_get:
                    # Convert the value to a float if it is a number
                    try:
                        value = float(value)
                    except ValueError:
                        pass

                    # Map the field to the name
                    output[all_fields[key]] = value

        return output

    def get_quote(self, asset, quote=None, exchange=None):
        """
        This function returns the quote of an asset. The quote includes the bid and ask price.

        Parameters
        ----------
        asset : Asset
            The asset to get the quote for.
        quote : Asset, optional
            The quote asset to get the quote for (currently not used for Tradier).
        exchange : str, optional
            The exchange to get the quote for (currently not used for Tradier).
            Quote of the asset, including the bid and ask price.

        Returns
        -------
        dict
           Quote of the asset, including the bid, and ask price.
        """
        result = self.get_market_snapshot(
            asset, ["last_price", "bid", "ask", "bid_size", "ask_size"]
        )
        if not result:
            return None

        result["price"] = result.pop("last_price")

        if isinstance(result["price"], str) and result["price"].startswith("C "):
            logging.warning(
                colored(
                    f"Ticker {asset.symbol} of type {asset.asset_type} with strike price {asset.strike} and expiry date {asset.expiration} is not trading currently. Got the last close price instead.",
                    "yellow",
                )
            )
            result["price"] = float(result["price"][1:])

        if "bid" in result:
            if result["bid"] == -1:
                result["bid"] = None
        else:
            result["bid"] = None

        if "ask" in result:
            if result["ask"] == -1:
                result["ask"] = None
        else:
            result["ask"] = None
        
        return result
