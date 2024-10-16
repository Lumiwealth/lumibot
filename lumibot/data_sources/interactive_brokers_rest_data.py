import logging
from termcolor import colored
from lumibot.entities import Asset, Bars

from .data_source import DataSource
import subprocess
import os
import time
import requests
import urllib3
from datetime import datetime
import pandas as pd

# Disable warnings for insecure HTTP requests
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Mapping of asset types to Interactive Brokers' type codes
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

    This class provides methods to interact with the Interactive Brokers REST API,
    including authentication, fetching account information, executing orders,
    retrieving market data, and more.

    Attributes:
        MIN_TIMESTEP (str): The minimum timestep for historical data.
        SOURCE (str): The source identifier for the data.
    """

    MIN_TIMESTEP = "minute"
    SOURCE = "InteractiveBrokersREST"

    def __init__(self, config):
        """
        Initializes the InteractiveBrokersRESTData instance.

        Args:
            config (dict): Configuration dictionary containing the following keys:
                - API_URL (str): The base URL for the Interactive Brokers API. If None, defaults to localhost.
                - ACCOUNT_ID (str, optional): The account ID to use. If not provided, it will be fetched.
                - IB_USERNAME (str): The Interactive Brokers username.
                - IB_PASSWORD (str): The Interactive Brokers password.
                - RUNNING_ON_SERVER (str): Indicates if the code is running on a server ("true" or "false").
        """
        # Determine the base URL based on the provided API_URL
        if config["API_URL"] is None:
            self.port = "4234"
            self.base_url = f"https://localhost:{self.port}/v1/api"
        else:
            self.api_url = config["API_URL"]
            self.base_url = f"{self.api_url}/v1/api"

        # Set account ID if provided
        self.account_id = config["ACCOUNT_ID"] if "ACCOUNT_ID" in config else None
        self.ib_username = config["IB_USERNAME"]
        self.ib_password = config["IB_PASSWORD"]

        # Determine if running on a server
        running_on_server = (
            config["RUNNING_ON_SERVER"]
            if config["RUNNING_ON_SERVER"] is not None
            else ""
        )
        if running_on_server.lower() == "true" or hasattr(self, "api_url"):
            self.running_on_server = True
        else:
            self.running_on_server = False

        # Start the connection process
        self.start()

    def start(self):
        """
        Starts the connection to the Interactive Brokers REST API.

        If not running on a server, it attempts to start the required Docker container.
        Waits until authentication is successful and retrieves the account ID if not provided.
        Suppresses specific server warnings after successful connection.
        """
        if not self.running_on_server:
            # Check if Docker is installed
            if (
                subprocess.run(
                    ["docker", "--version"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                ).returncode
                != 0
            ):
                logging.error(colored("Docker is not installed.", "red"))
                return

            # Log connection attempt
            logging.info(
                colored("Connecting to Interactive Brokers REST API...", "green")
            )

            # Define environment variables for Docker container
            inputs_dir = "/srv/clientportal.gw/root/conf.yaml"
            env_variables = {
                "IBEAM_ACCOUNT": self.ib_username,
                "IBEAM_PASSWORD": self.ib_password,
                "IBEAM_GATEWAY_BASE_URL": f"https://localhost:{self.port}",
                "IBEAM_LOG_TO_FILE": False,
                "IBEAM_REQUEST_RETRIES": 1,
                "IBEAM_PAGE_LOAD_TIMEOUT": 30,
                "IBEAM_INPUTS_DIR": inputs_dir,
            }

            # Prepare environment arguments for Docker run command
            env_args = [f"--env={key}={value}" for key, value in env_variables.items()]
            conf_path = os.path.join(
                os.path.dirname(os.path.dirname(__file__)), "resources", "conf.yaml"
            )
            volume_mount = f"{conf_path}:{inputs_dir}"

            # Remove any existing Docker container named 'lumibot-client-portal'
            subprocess.run(
                ["docker", "rm", "-f", "lumibot-client-portal"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )

            # Run the Docker container in detached mode
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

            # Wait for the Docker container to initialize
            time.sleep(10)

        # Continuously check for authentication until successful
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

        # Retrieve account ID if not provided
        if self.account_id is None:
            url = f"{self.base_url}/portfolio/accounts"
            response = self.get_from_endpoint(url, "Fetching Account ID")
            if response is not None:
                if (
                    isinstance(response, list)
                    and len(response) > 0
                    and isinstance(response[0], dict)
                ):
                    self.account_id = response[0]["id"]
                else:
                    logging.error(colored("Failed to get Account ID.", "red"))
            else:
                logging.error(colored("Failed to get Account ID.", "red"))

        logging.info(colored("Connected to Client Portal", "green"))

        # Suppress specific server warnings
        suppress_url = f"{self.base_url}/iserver/questions/suppress"
        suppress_payload = {"messageIds": ["o451", "o383", "o354", "o163"]}
        self.post_to_endpoint(suppress_url, json=suppress_payload)

    def is_authenticated(self):
        """
        Checks if the client is authenticated with the Interactive Brokers API.

        Returns:
            bool: True if authenticated, False otherwise.
        """
        url = f"{self.base_url}/iserver/accounts"
        response = self.get_from_endpoint(
            url, "Auth Check", silent=True, return_errors=False
        )
        return response is not None

    def ping_portfolio(self):
        """
        Pings the portfolio endpoint to verify connectivity.

        Returns:
            bool: True if the portfolio endpoint is reachable, False otherwise.
        """
        url = f"{self.base_url}/portfolio/accounts"
        response = self.get_from_endpoint(
            url, "Auth Check", silent=True, return_errors=False
        )
        return response is not None

    def get_contract_details(self, conId):
        """
        Retrieves contract details for a given contract ID.

        Args:
            conId (int): The contract ID.

        Returns:
            dict or None: Contract details if successful, None otherwise.
        """
        url = f"{self.base_url}/iserver/contract/{conId}/info"
        response = self.get_from_endpoint(url, "Getting contract details")
        return response

    def get_account_info(self):
        """
        Retrieves account summary information.

        Returns:
            dict or None: Account summary information if successful, None otherwise.
        """
        url = f"{self.base_url}/portal/account/summary?accountId={self.account_id}"
        response = self.get_from_endpoint(url, "Getting account info")
        return response

    def get_account_balances(self, silent=True):
        """
        Retrieves the account balances for the configured account ID.

        Args:
            silent (bool): If True, suppresses certain error messages.

        Returns:
            dict or None: Account balances if successful, None otherwise.
        """
        # Define the endpoint URL for fetching account balances
        url = f"{self.base_url}/portfolio/{self.account_id}/ledger"
        response = self.get_from_endpoint(
            url, "Getting account balances", silent=silent
        )

        # Handle "Please query /accounts first" error
        if response is not None and "error" in response:
            if silent:
                if self.ping_portfolio():
                    return self.get_account_balances(silent=False)
                else:
                    logging.error(
                        colored(
                            f"Couldn't get account balances. Not authenticated. Error: {response['error']}",
                            "red",
                        )
                    )
                    return None
            else:
                logging.error(
                    colored(
                        f"Couldn't get account balances. Error: {response['error']}",
                        "red",
                    )
                )
                return None

        return response

    def get_from_endpoint(
        self, endpoint, description, silent=False, return_errors=True
    ):
        """
        Makes a GET request to the specified API endpoint.

        Args:
            endpoint (str): The full URL of the endpoint.
            description (str): Description of the task for logging purposes.
            silent (bool): If True, suppresses certain log messages.
            return_errors (bool): If True, returns error messages in the response.

        Returns:
            dict or None: JSON response from the API if successful, error dict or None otherwise.
        """
        try:
            # Make the GET request to the endpoint
            response = requests.get(endpoint, verify=False)

            # Check if the request was successful
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                error_message = f"error: {description} endpoint not found."
                if not silent:
                    logging.warning(colored(error_message, "yellow"))
                if return_errors:
                    return {"error": error_message}
                return None
            elif response.status_code == 429:
                logging.info(
                    f"You got rate limited {description}. Waiting for 1 second..."
                )
                time.sleep(1)
                return self.get_from_endpoint(endpoint, description, silent)
            else:
                error_message = (
                    f"error: Task '{description}' Failed. "
                    f"Status code: {response.status_code}, Response: {response.text}"
                )
                if not silent:
                    logging.error(colored(error_message, "red"))
                if return_errors:
                    return {"error": error_message}
                return None

        except requests.exceptions.RequestException as e:
            # Log an error message if there was a problem with the request
            error_message = f"error: {description}: {e}"
            if not silent:
                logging.error(colored(error_message, "red"))
            if return_errors:
                return {"error": error_message}
            return None

    def post_to_endpoint(self, url, json: dict):
        """
        Makes a POST request to the specified API endpoint with a JSON payload.

        Args:
            url (str): The full URL of the endpoint.
            json (dict): The JSON payload to send in the POST request.

        Returns:
            dict or None: JSON response from the API if successful, None otherwise.
        """
        try:
            # Make the POST request to the endpoint
            response = requests.post(url, json=json, verify=False)

            # Check if the request was successful
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                logging.error(colored(f"{url} endpoint not found.", "red"))
                return None
            elif response.status_code == 429:
                logging.info(f"You got rate limited {url}. Waiting for 5 seconds...")
                time.sleep(5)
                return self.post_to_endpoint(url, json)
            else:
                if "error" in response.json():
                    logging.error(
                        colored(
                            f"Task '{url}' Failed. Error: {response.json()['error']}",
                            "red",
                        )
                    )
                else:
                    logging.error(
                        colored(
                            f"Task '{url}' Failed. Status code: {response.status_code}, "
                            f"Response: {response.text}",
                            "red",
                        )
                    )
                return None

        except requests.exceptions.RequestException as e:
            # Log an error message if there was a problem with the request
            logging.error(colored(f"Error {url}: {e}", "red"))

    def delete_to_endpoint(self, url):
        """
        Makes a DELETE request to the specified API endpoint.

        Args:
            url (str): The full URL of the endpoint.

        Returns:
            dict or None: JSON response from the API if successful, None otherwise.
        """
        try:
            # Make the DELETE request to the endpoint
            response = requests.delete(url, verify=False)

            # Check if the request was successful
            if response.status_code == 200:
                # Handle specific error messages in the response
                if (
                    "error" in response.json()
                    and "doesn't exist" in response.json()["error"]
                ):
                    logging.warning(
                        colored(
                            f"Order ID doesn't exist: {response.json()['error']}",
                            "yellow",
                        )
                    )
                    return None
                return response.json()
            elif response.status_code == 404:
                logging.error(colored(f"{url} endpoint not found.", "red"))
                return None
            elif response.status_code == 429:
                logging.info(f"You got rate limited {url}. Waiting for 5 seconds...")
                time.sleep(5)
                return self.delete_to_endpoint(url)
            else:
                logging.error(
                    colored(
                        f"Task '{url}' Failed. Status code: {response.status_code}, Response: {response.text}",
                        "red",
                    )
                )
                return None
        except requests.exceptions.RequestException as e:
            # Log an error message if there was a problem with the request
            logging.error(colored(f"Error {url}: {e}", "red"))

    def get_open_orders(self, silent=True):
        """
        Retrieves all open orders for the configured account.

        Args:
            silent (bool): If True, suppresses certain error messages.

        Returns:
            list or None: List of open orders if available, None otherwise.
        """
        # Initial request to clear cache with force=true (may be unnecessary)
        initial_url = f"{self.base_url}/iserver/account/orders?force=true"
        self.get_from_endpoint(initial_url, "Getting open orders")

        # Fetch open orders with specific filters
        url = f"{self.base_url}/iserver/account/orders?&accountId={self.account_id}&filters=Submitted,PreSubmitted"
        response = self.get_from_endpoint(url, "Getting open orders", silent=silent)

        # Handle "Please query /accounts first" error
        if response is not None and "error" in response:
            if silent:
                if self.is_authenticated():
                    return self.get_open_orders(silent=False)
                else:
                    logging.error(
                        colored(
                            f"Couldn't retrieve open orders. Not authenticated. Error: {response['error']}",
                            "red",
                        )
                    )
                    return None
            else:
                logging.error(
                    colored(
                        f"Couldn't retrieve open orders. Error: {response['error']}",
                        "red",
                    )
                )
                return None

        if response is None or response == []:
            return None

        # Filter out orders that are Cancelled or Filled
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

    def get_order_info(self, orderid):
        """
        Retrieves information for a specific order.

        Args:
            orderid (str): The unique identifier of the order.

        Returns:
            dict or None: Order information if successful, None otherwise.
        """
        url = f"{self.base_url}/iserver/account/order/status/{orderid}"
        response = self.get_from_endpoint(url, "Getting Order Info")
        return response

    def execute_order(self, order_data):
        """
        Executes an order based on the provided order data.

        Args:
            order_data (dict): The order data to be executed.

        Returns:
            list or None: List of executed orders if successful, None otherwise.
        """
        if order_data is None:
            logging.debug(colored("Failed to get order data.", "red"))
            return None

        url = f"{self.base_url}/iserver/account/{self.account_id}/orders"
        response = self.post_to_endpoint(url, order_data)

        if isinstance(response, list) and "order_id" in response[0]:
            # Order executed successfully
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
        else:
            logging.error(colored(f"Failed to execute order: {order_data}", "red"))

    def delete_order(self, order):
        """
        Deletes (cancels) a specific order.

        Args:
            order (Order): The order object to be deleted.

        Returns:
            None
        """
        orderId = order.identifier
        url = f"{self.base_url}/iserver/account/{self.account_id}/order/{orderId}"
        status = self.delete_to_endpoint(url)
        if status:
            logging.info(
                colored(f"Order with ID {orderId} canceled successfully.", "green")
            )
        else:
            logging.error(colored(f"Failed to delete order with ID {orderId}.", "red"))

    def get_positions(self, silent=True):
        """
        Retrieves the current positions for the configured account.

        Args:
            silent (bool): If True, suppresses certain error messages.

        Returns:
            dict or None: Current positions if successful, None otherwise.
        """
        # Define the endpoint URL for fetching positions
        url = f"{self.base_url}/portfolio/{self.account_id}/positions"
        response = self.get_from_endpoint(
            url, "Getting account positions", silent=silent
        )

        # Handle "Please query /accounts first" error
        if response is not None and "error" in response:
            if silent:
                if self.ping_portfolio():
                    return self.get_positions(silent=False)
                else:
                    logging.error(
                        colored(
                            f"Couldn't get account positions. Not authenticated. Error: {response['error']}",
                            "red",
                        )
                    )
                    return None
            else:
                logging.error(
                    colored(
                        f"Couldn't get account positions. Error: {response['error']}",
                        "red",
                    )
                )
                return None

        return response

    def stop(self):
        """
        Stops the connection to the Interactive Brokers REST API.

        If not running on a server, it removes the Docker container.
        """
        # If running on a server, no action is needed
        if self.running_on_server:
            return

        # Remove the Docker container if it's running
        subprocess.run(
            ["docker", "rm", "-f", "lumibot-client-portal"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    def get_chains(self, asset: Asset, quote=None) -> dict:
        """
        Retrieves option chains for a given asset.

        Args:
            asset (Asset): The asset for which to retrieve option chains.
            quote (Asset, optional): The quote asset (currently not used).

        Returns:
            dict: A dictionary containing multiplier, exchange, and chains for CALL and PUT options.
                  Format:
                      {
                          "Multiplier": "100",
                          "Exchange": "unknown",
                          "Chains": {
                              "CALL": {
                                  "2023-07-31": [strike1, strike2, ...],
                                  ...
                              },
                              "PUT": {
                                  "2023-07-31": [strike1, strike2, ...],
                                  ...
                              }
                          }
                      }
                  Returns an empty dictionary if an error occurs.
        """
        chains = {
            "Multiplier": asset.multiplier,
            "Exchange": "unknown",
            "Chains": {"CALL": {}, "PUT": {}},
        }
        logging.info(
            "This task is extremely slow. If you still wish to use it, prepare yourself for a long wait."
        )

        # Search for the contract ID (conid) based on the asset symbol
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

        # Parse the option months
        if option_dates:
            months = option_dates.split(";")  # in MMMYY format
        else:
            logging.error("Option dates are None")
            return {}

        # Iterate through each month to retrieve strikes and expiration dates
        for month in months:
            # Define the endpoint URL for fetching strikes
            url_for_strikes = f"{self.base_url}/iserver/secdef/strikes?sectype=OPT&conid={conid}&month={month}"
            strikes = self.get_from_endpoint(url_for_strikes, "Getting Strikes")

            # Process CALL options
            if strikes and "call" in strikes:
                for strike in strikes["call"]:
                    # Define the endpoint URL for fetching contract info
                    url_for_expiry = (
                        f"{self.base_url}/iserver/secdef/info?conid={conid}&sectype=OPT&month={month}&right=C&strike={strike}"
                    )
                    contract_info = self.get_from_endpoint(
                        url_for_expiry, "Getting expiration Date"
                    )
                    if (
                        contract_info
                        and isinstance(contract_info, list)
                        and len(contract_info) > 0
                        and "maturityDate" in contract_info[0]
                    ):
                        # Parse and format the expiration date
                        expiry_date = contract_info[0]["maturityDate"]
                        expiry_date = datetime.strptime(expiry_date, "%Y%m%d").strftime(
                            "%Y-%m-%d"
                        )
                        if expiry_date not in chains["Chains"]["CALL"]:
                            chains["Chains"]["CALL"][expiry_date] = []
                        chains["Chains"]["CALL"][expiry_date].append(strike)
                    else:
                        logging.error("Invalid contract_info format")
                        return {}

            # Process PUT options
            if strikes and "put" in strikes:
                for strike in strikes["put"]:
                    # Define the endpoint URL for fetching contract info
                    url_for_expiry = (
                        f"{self.base_url}/iserver/secdef/info?conid={conid}&sectype=OPT&month={month}&right=P&strike={strike}"
                    )
                    contract_info = self.get_from_endpoint(
                        url_for_expiry, "Getting expiration Date"
                    )
                    if (
                        contract_info
                        and isinstance(contract_info, list)
                        and len(contract_info) > 0
                        and "maturityDate" in contract_info[0]
                    ):
                        # Parse and format the expiration date
                        expiry_date = contract_info[0]["maturityDate"]
                        expiry_date = datetime.strptime(expiry_date, "%Y%m%d").strftime(
                            "%Y-%m-%d"
                        )
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
        Retrieves historical price data (bars) for a given asset.

        Args:
            asset (Asset or str): The asset to retrieve bars for. If a string is provided, it will be converted to an Asset.
            length (int): The number of bars to retrieve.
            timestep (str, optional): The timestep for each bar (e.g., "minute", "day"). Defaults to the minimum timestep.
            timeshift (datetime.timedelta, optional): The time shift for the start time of the data. Defaults to current time.
            quote (Asset, optional): The quote asset for the bars.
            exchange (str, optional): The exchange to filter the data by.
            include_after_hours (bool): Whether to include after-hours data.

        Returns:
            Bars: An instance of the Bars class containing the historical data.
        """
        # Convert asset to Asset object if it's a string
        if isinstance(asset, str):
            asset = Asset(symbol=asset)

        # Use default timestep if not provided
        if not timestep:
            timestep = self.get_timestep()

        # Calculate the start time based on timeshift
        if timeshift:
            start_time = (datetime.now() - timeshift).strftime("%Y%m%d-%H:%M:%S")
        else:
            start_time = datetime.now().strftime("%Y%m%d-%H:%M:%S")

        # Retrieve the contract ID (conid) for the asset
        conid = self.get_conid_from_asset(asset=asset)

        # Determine the period based on timestep and length
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
            raise ValueError(f"Unsupported timestep: {timestep}")

        # Construct the historical data endpoint URL
        url = (
            f"{self.base_url}/iserver/marketdata/history?conid={conid}&period={period}"
            f"&bar={timestep}&outsideRth={include_after_hours}&startTime={start_time}"
        )

        # Append exchange filter if provided
        if exchange:
            url += f"&exchange={exchange}"

        # Fetch historical data from the endpoint
        result = self.get_from_endpoint(url, "Getting Historical Prices")

        # Handle errors in the response
        if result and "error" in result:
            logging.error(
                colored(f"Error getting historical prices: {result['error']}", "red")
            )
            raise Exception("Error getting historical prices")

        if not result or not result["data"]:
            logging.error(
                colored(
                    f"Failed to get historical prices for {asset.symbol}, result was: {result}",
                    "red",
                )
            )
            return Bars(
                pd.DataFrame(), self.SOURCE, asset, raw=pd.DataFrame(), quote=quote
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

        # Convert timestamp to datetime and set as index with timezone localization
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        df["timestamp"] = (
            df["timestamp"].dt.tz_localize("UTC").dt.tz_convert("America/New_York")
        )
        df.set_index("timestamp", inplace=True)

        # Initialize Bars object with the DataFrame
        bars = Bars(df, self.SOURCE, asset, raw=df, quote=quote)

        return bars

    def get_last_price(self, asset, quote=None, exchange=None) -> float:
        """
        Retrieves the last traded price for a given asset.

        Args:
            asset (Asset): The asset to retrieve the last price for.
            quote (Asset, optional): The quote asset (currently not used).
            exchange (str, optional): The exchange to filter the data by.

        Returns:
            float: The last traded price. Returns -1 if retrieval fails.
        """
        field = "last_price"
        response = self.get_market_snapshot(asset, [field])

        if response is None or field not in response:
            if asset.asset_type in ["option", "future"]:
                logging.error(
                    f"Failed to get {field} for asset {asset.symbol} with strike {asset.strike} and expiration date {asset.expiration}"
                )
            else:
                logging.error(f"Failed to get {field} for asset {asset.symbol}")
            return -1

        price = response[field]

        # Remove the 'C' prefix if it exists and convert to float
        if isinstance(price, str) and price.startswith("C"):
            price = float(price[1:])

        return float(price)

    def get_conid_from_asset(self, asset: Asset):
        """
        Retrieves the contract ID (conid) for a given asset.

        Args:
            asset (Asset): The asset for which to retrieve the conid.

        Returns:
            int or None: The contract ID if found, None otherwise.
        """
        # Search for the contract definition based on the asset symbol
        url = f"{self.base_url}/iserver/secdef/search?symbol={asset.symbol}"
        response = self.get_from_endpoint(url, "Getting Underlying conid")

        if (
            isinstance(response, list)
            and len(response) > 0
            and isinstance(response[0], dict)
            and "conid" in response[0]
        ):
            conid = int(response[0]["conid"])
        else:
            logging.error(
                colored(
                    f"Failed to get conid of asset: {asset.symbol} of type {asset.asset_type}",
                    "red",
                )
            )
            logging.error(colored(f"Response: {response}", "red"))
            return None

        # Handle option assets by retrieving the specific conid for the option contract
        if asset.asset_type == "option":
            expiration_date = asset.expiration.strftime("%Y%m%d")
            expiration_month = asset.expiration.strftime("%b%y").upper()  # MMMYY format
            strike = asset.strike
            right = asset.right

            # Define the endpoint URL for fetching contract info
            url_for_expiry = (
                f"{self.base_url}/iserver/secdef/info?conid={conid}&sectype=OPT&month={expiration_month}"
                f"&right={right}&strike={strike}"
            )
            contract_info = self.get_from_endpoint(
                url_for_expiry, "Getting expiration Date"
            )

            matching_contract = None
            if contract_info:
                # Find the contract with the matching maturity date
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
                        f"No matching contract found for asset: {asset.symbol} with expiration date {expiration_date} and strike {strike}",
                        "red",
                    )
                )
                return None

            return matching_contract["conid"]

        # For stock and forex assets, return the underlying conid
        elif asset.asset_type in ["stock", "forex"]:
            return conid

    def query_greeks(self, asset: Asset) -> dict:
        """
        Queries the Greeks (vega, theta, gamma, delta) for a given option asset.

        Args:
            asset (Asset): The option asset to query Greeks for.

        Returns:
            dict: A dictionary containing the requested Greeks. Returns an empty dict if unavailable.
        """
        greeks = self.get_market_snapshot(asset, ["vega", "theta", "gamma", "delta"])
        return greeks if greeks is not None else {}

    def get_market_snapshot(self, asset: Asset, fields: list):
        """
        Retrieves a market snapshot for the specified asset and fields.

        Args:
            asset (Asset): The asset to retrieve the snapshot for.
            fields (list): List of fields to retrieve (e.g., ["vega", "theta"]).

        Returns:
            dict or None: A dictionary containing the requested fields and their values, or None if unsuccessful.
        """
        # Mapping of field identifiers to their names as per Interactive Brokers API
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
            "7309": "gamma"
            # Reference: https://www.interactivebrokers.com/campus/ibkr-api-page/webapi-ref/#tag/Trading-Market-Data/paths/~1iserver~1marketdata~1snapshot/get
        }

        # Retrieve the contract ID for the asset
        conId = self.get_conid_from_asset(asset)
        if conId is None:
            return None

        # Determine which fields to retrieve based on the requested field names
        fields_to_get = []
        for identifier, name in all_fields.items():
            if name in fields:
                fields_to_get.append(identifier)

        # Create a comma-separated string of field identifiers
        fields_str = ",".join(str(field) for field in fields_to_get)

        # Check authentication status before making the request
        if not self.is_authenticated():
            logging.error(
                colored("Unable to retrieve market snapshot. Not Authenticated.", "red")
            )
            return None

        # Define the endpoint URL for the market snapshot
        url = f"{self.base_url}/iserver/marketdata/snapshot?conids={conId}&fields={fields_str}"

        # Initialize retry parameters
        max_retries = 500
        retries = 0
        missing_fields = True

        response = None
        # Retry fetching until all requested fields are present or max retries reached
        while missing_fields and retries < max_retries:
            if retries >= 3:
                time.sleep(5)  # Introduce delay after initial retries

            response = self.get_from_endpoint(url, "Getting Market Snapshot")
            retries += 1

            # Check if all requested fields are present in the response
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

        # Prepare the output dictionary with the requested fields
        output = {}

        if (
            response
            and isinstance(response, list)
            and len(response) > 0
            and isinstance(response[0], dict)
        ):
            for key, value in response[0].items():
                if key in fields_to_get:
                    # Attempt to convert numeric values to float
                    try:
                        value = float(value)
                    except ValueError:
                        pass

                    # Map the field identifier to the field name
                    output[all_fields[key]] = value

        return output

    def get_quote(self, asset, quote=None, exchange=None):
        """
        Retrieves the quote for a given asset, including bid and ask prices.

        Args:
            asset (Asset): The asset to retrieve the quote for.
            quote (Asset, optional): The quote asset (currently not used).
            exchange (str, optional): The exchange to filter the data by.

        Returns:
            dict or None: A dictionary containing the price, bid, ask, bid_size, ask_size, and trading status.
                          Returns None if retrieval fails.
        """
        # Retrieve the market snapshot for specified fields
        result = self.get_market_snapshot(
            asset, ["last_price", "bid", "ask", "bid_size", "ask_size"]
        )
        if not result:
            return None

        # Rename 'last_price' to 'price'
        result["price"] = result.pop("last_price")

        # Handle cases where the price is prefixed with 'C' indicating the asset is not trading
        if isinstance(result["price"], str) and result["price"].startswith("C"):
            logging.warning(
                colored(
                    f"Ticker {asset.symbol} of type {asset.asset_type} with strike price {asset.strike} and expiry date {asset.expiration} is not trading currently. Got the last close price instead.",
                    "yellow",
                )
            )
            result["price"] = float(result["price"][1:])
            result["trading"] = False
        else:
            result["trading"] = True

        # Replace invalid bid and ask prices with None
        if result["bid"] == -1:
            result["bid"] = None
        if result["ask"] == -1:
            result["ask"] = None

        return result
