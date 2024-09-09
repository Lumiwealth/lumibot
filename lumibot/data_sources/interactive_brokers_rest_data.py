import logging
from termcolor import colored
from lumibot.entities import Asset, Bars

from .data_source import DataSource
import subprocess
import os
import time
import requests
import urllib3
from lumibot.tools.helpers import create_options_symbol

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

    def __init__(self, config, api_url):
        if api_url is None:
            self.port = "4234"
            self.base_url = f'https://localhost:{self.port}/v1/api'
        else:
            self.api_url = api_url
            self.base_url = f'{api_url}/v1/api'
        
        self.account_id = config["ACCOUNT_ID"] if "ACCOUNT_ID" in config else None
        self.ib_username = config["IB_USERNAME"]
        self.ib_password = config["IB_PASSWORD"]

        self.start()

    def start(self):
        if not hasattr(self, "api_url"):
            # Run the Docker image with the specified environment variables and port mapping
            if not subprocess.run(['docker', '--version'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0:
                logging.error("Docker is not installed.")
                return
            logging.info("Connecting to IBKR Client Portal...")

            inputs_dir = '/srv/clientportal.gw/root/conf.yaml'
            env_variables = {
                'IBEAM_ACCOUNT': self.ib_username,
                'IBEAM_PASSWORD': self.ib_password,
                'IBEAM_GATEWAY_BASE_URL': f'https://localhost:{self.port}',
                'IBEAM_LOG_TO_FILE': False,
                'IBEAM_REQUEST_RETRIES': 10,
                'IBEAM_PAGE_LOAD_TIMEOUT': 30,
                'IBEAM_INPUTS_DIR': inputs_dir
            }

            env_args = [f'--env={key}={value}' for key, value in env_variables.items()]
            conf_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "resources", "conf.yaml")
            volume_mount = f'{conf_path}:{inputs_dir}'

            subprocess.run(['docker', 'rm', '-f', 'lumibot-client-portal'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            subprocess.run(['docker', 'run', '-d', '--name', 'lumibot-client-portal', *env_args, '-p', f'{self.port}:{self.port}', '-v', volume_mount, 'voyz/ibeam'], stdout=subprocess.DEVNULL, text=True)

            # check if authenticated
            time.sleep(10)

        while not self.is_authenticated():
            logging.info("Not connected to API server yet.")
            logging.info("Waiting for another 10 seconds before checking again...")
            time.sleep(10)
        
        # Set self.account_id
        if self.account_id is None:
            url = f'{self.base_url}/portfolio/accounts'
            response = self.get_from_endpoint(url, "Fetching Account ID")
            if response is not None:
                self.account_id = response[0]['id']
            else:
                logging.error(f"Failed to get Account ID.")
        
        logging.info("Connected to Client Portal")

    def is_authenticated(self):
        url = f'{self.base_url}/iserver/accounts'
        response = self.get_from_endpoint(url, "Auth Check", silent=True)
        if response is not None:
            return True
        else:
            return False

    def get_contract_details(self, conId):
        url = f"{self.base_url}/iserver/contract/{conId}/info"
        response = self.get_from_endpoint(url, "Getting contract details")
        return response
    
    def get_account_info(self):
        url = f"{self.base_url}/portal/account/summary?accountId={self.account_id}"
        response = self.get_from_endpoint(url, "Getting account info")
        return response
        
    def get_account_balances(self):
        """
        Retrieves the account balances for a given account ID.
        """
        # Define the endpoint URL for fetching account balances
        url = f"{self.base_url}/portfolio/{self.account_id}/ledger"
        response = self.get_from_endpoint(url, "Getting account balances")
        return response
            
    def get_from_endpoint(self, endpoint, description, silent=False):
        try:
            # Make the request to the endpoint
            response = requests.get(endpoint, verify=False)

            # Check if the request was successful
            if response.status_code == 200:
                # Return the JSON response containing the account balances
                return response.json()
            elif response.status_code == 404:
                if not silent:
                    logging.warning(f"{description} endpoint not found.")
                return None
            elif response.status_code == 429:
                logging.info(f"You got rate limited {description}. Waiting for 5 seconds...")
                time.sleep(5)
                return self.get_from_endpoint(endpoint, description, silent)
            else:
                # Log an error message if the request failed
                if not silent:
                    logging.error(f"Task '{description}' Failed. Status code: {response.status_code}")
                return None

        except requests.exceptions.RequestException as e:
            # Log an error message if there was a problem with the request
            if not silent:
                logging.error(f"Error {description}: {e}")
            return None
    
    def get_ticker_from_conid(self, conid):
        url = f'{self.base_url}/iserver/contract/{conid}/info'
        response = self.get_from_endpoint(url, "TESTTT")
        logging.info(response)
        return response['symbol']
    
    def post_to_endpoint(self, url, json:dict):
        try:
            ticker = self.get_ticker_from_conid(json['conid'])
            logging.info(ticker)
            response = requests.post(url, json=json, verify=False)
            # Check if the request was successful
            if response.status_code == 200:
                # Return the JSON response containing the account balances
                return response.json()
            elif response.status_code == 404:
                logging.warning(f"{url} endpoint not found.")
                return None
            elif response.status_code == 429:
                logging.info(f"You got rate limited {url}. Waiting for 5 seconds...")
                time.sleep(5)
                return self.get_from_endpoint(url, json)
            else:
                logging.error(f"Task '{url}' Failed. Status code: {response.status_code}")
                return None
            
        except requests.exceptions.RequestException as e:
            # Log an error message if there was a problem with the request
            logging.error(f"Error {url}: {e}")
    
    def delete_to_endpoint(self, url):
        try:
            response = requests.delete(url, verify=False)
            # Check if the request was successful
            if response.status_code == 200:
                # Return the JSON response containing the account balances
                return response.json()
            elif response.status_code == 404:
                logging.warning(f"{url} endpoint not found.")
                return None
            elif response.status_code == 429:
                logging.info(f"You got rate limited {url}. Waiting for 5 seconds...")
                time.sleep(5)
                return self.delete_to_endpoint(url)

            else:
                logging.error(f"Task '{url}' Failed. Status code: {response.status_code}")
                return None
            
        except requests.exceptions.RequestException as e:
            # Log an error message if there was a problem with the request
            logging.error(f"Error {url}: {e}")
    
    def get_open_orders(self):
        # Clear cache with force=true
        url = f'{self.base_url}/iserver/account/orders?force=true'
        response = self.get_from_endpoint(url, "Getting open orders")

        # Fetch
        url = f'{self.base_url}/iserver/account/orders?&accountId={self.account_id}&filters=Submitted,PreSubmitted'
        response = self.get_from_endpoint(url, "Getting open orders")

        if response is None or response == []:
            return None

        ## Filters don't work, we'll filter on our own
        filtered_orders = []
        for order in response['orders']:
            if order['status'] != "Cancelled":
                filtered_orders.append(order)

        return filtered_orders

    def execute_order(self, order_data):
        url = f'{self.base_url}/iserver/account/{self.account_id}/orders'
        response = self.post_to_endpoint(url, order_data)
        logging.info(order_data)
        return response[0]["order_id"]
    
    def delete_order(self, order):
        orderId = order.identifier
        url = f'{self.base_url}/iserver/account/{self.account_id}/order/{orderId}'
        status = self.delete_to_endpoint(url)
        if status:
            logging.info(f"Order with ID {orderId} canceled successfully.")
        else:
            logging.error(f"Failed to delete order with ID {orderId}.")

    def get_positions(self):
        """
        Retrieves the current positions for a given account ID.
        """
        url = f"{self.base_url}/portfolio/{self.account_id}/positions"
        response = self.get_from_endpoint(url, "Getting account positions")
        return response
    
    def stop(self):        
        # Check if the Docker image is already running
        if hasattr(self, "api_url"):
            return

        subprocess.run(['docker', 'rm', '-f', 'lumibot-client-portal'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def get_chains(self, asset: Asset, quote: Asset = None) -> dict: ## options chains
        url_for_dates = f'{self.base_url}/iserver/secdef/search?symbol={asset.symbol}'
        response = self.get_from_endpoint(url_for_dates, "Getting Option Dates")
        option_dates = response[0]['opt'] # separated by semicolons
        option_dates_array = response[0]['opt'].split(';') # in YYYYMMDD

        month="JAN24" #MMMYY
        url_for_strikes = f'{self.base_url}/iserver/secdef/strikes?conid={conid}&sectype={TYPE_MAP[asset.asset_type]}&month={month}'
        logging.error(colored("Method 'get_chains' is not yet implemented.", "red"))
        return {}  # Return an empty dictionary as a placeholder

    def get_historical_prices(
        self, asset, length, timestep="", timeshift=None, quote=None, exchange=None, include_after_hours=True
    ) -> Bars:
        logging.error(colored("Method 'get_historical_prices' is not yet implemented.", "red"))
        return None  # Return None as a placeholder

    def get_last_price(self, asset, quote=None, exchange=None) -> float:
        field = "last_price"
        response = self.get_market_snapshot(asset, [field])
        return response[field]
    
    def get_conid_from_asset(self, asset: Asset):
        url = f'{self.base_url}/iserver/secdef/search?symbol={asset.symbol}'
        response = self.get_from_endpoint(url, "Getting Asset ConId")
        return int(response[0]["conid"])
    
    def get_sectype_from_conid(self, conId):
        url = f'{self.base_url}/iserver/contract/{conId}/info'
        response = self.get_from_endpoint(url, "Getting SecType")
        return response["instrument_type"]

    def get_market_snapshot(self, asset: Asset, fields: list):
        all_fields = {
            "84": "bid",
            "86": "ask",
            "31": "last_price",
            ## add greeks, implied vol
            # https://www.interactivebrokers.com/campus/ibkr-api-page/webapi-ref/#tag/Trading-Market-Data/paths/~1iserver~1marketdata~1snapshot/get
        }

        conId = self.get_conid_from_asset(asset) ## does this work for options? should this work for options?

        fields_to_get = []
        for identifier, name in all_fields.items():
            if name in fields:
                fields_to_get.append(identifier)
        
        fields_str = ",".join(str(field) for field in fields_to_get)
        
        url = f'{self.base_url}/iserver/marketdata/snapshot?conids={conId}&fields={fields_str}'
        
        # First time will only return conid and conidEx
        response = self.get_from_endpoint(url, "Getting Market Snapshot")
        
        # If fields are missing, then its first time, fetch again
        missing_fields = False
        for field in fields_to_get:
            if not field in response[0]:
                missing_fields = True
                break

        if missing_fields:
            # This should be alright
            response = self.get_from_endpoint(url, "Getting Market Snapshot")

        # return only what was requested
        output = {}
        for key, value in response[0].items():
            if key in fields_to_get:
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
        
        result = self.get_market_snapshot(asset, ["bid", "ask"])
        if result is None:
            return None
        
        if result["bid"] == -1:
            result["bid"] = None
        if result["ask"] == -1:
            result["ask"] = None

        return result
    