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
from datetime import datetime

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
        
        # Ensure the Docker process is running
        docker_ps = subprocess.run(['docker', 'ps', '--filter', 'name=lumibot-client-portal', '--format', '{{.Names}}'], capture_output=True, text=True)
        if 'lumibot-client-portal' not in docker_ps.stdout:
            logging.error("Docker container 'lumibot-client-portal' is not running.")
            logging.error("Waiting for 5 seconds and retrying...")
            time.sleep(5)
            self.start()
            return
        
        # Set self.account_id
        if self.account_id is None:
            url = f'{self.base_url}/portfolio/accounts'
            response = self.get_from_endpoint(url, "Fetching Account ID")
            if response is not None:
                self.account_id = response[0]['id']
            else:
                logging.error(f"Failed to get Account ID.")
        
        logging.info("Connected to Client Portal")

        # Suppress weird server warnings
        url = f'{self.base_url}/iserver/questions/suppress'
        json = {'messageIds': ['o451', 'o383']}

        self.post_to_endpoint(url, json=json)

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
                    logging.error(f"Task '{description}' Failed. Status code: {response.status_code}, Response: {response.text}")
                return None

        except requests.exceptions.RequestException as e:
            # Log an error message if there was a problem with the request
            if not silent:
                logging.error(f"Error {description}: {e}")
            return None
    
    def post_to_endpoint(self, url, json: dict):
        try:
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
                logging.error(f"Task '{url}' Failed. Status code: {response.status_code}, Response: {response.text}")
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
                if "error" in response.json() and "doesn't exist" in response.json()["error"]:
                    logging.warning(f"Order ID doesn't exist: {response.json()['error']}")
                    return None
            
                return response.json()
            elif response.status_code == 404:
                logging.warning(f"{url} endpoint not found.")
                return None
            elif response.status_code == 429:
                logging.info(f"You got rate limited {url}. Waiting for 5 seconds...")
                time.sleep(5)
                return self.delete_to_endpoint(url)
            else:
                logging.error(f"Task '{url}' Failed. Status code: {response.status_code}, Response: {response.text}")
                return None
        except requests.exceptions.RequestException as e:
            # Log an error message if there was a problem with the request
            logging.error(f"Error {url}: {e}")
            
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
            if order['status'] not in ["Cancelled", "Filled"]:
                filtered_orders.append(order)

        return filtered_orders

    def get_order_info(self, orderid):
        url = f'{self.base_url}/iserver/account/order/status/{orderid}'
        response = self.get_from_endpoint(url, "Getting Order Info")
        return response
        
    def execute_order(self, order_data): ## cooldown?
        url = f'{self.base_url}/iserver/account/{self.account_id}/orders'
        response = self.post_to_endpoint(url, order_data)
        
        if response is not None:
            ## urgent fix
            try:
                if isinstance(response, list):
                    return response
                else:
                    return response.get('orders')
            except Exception as e:
                error_message = f"An error occurred while retrieving the error message: {str(e)}"
                logging.error(f"Failed to execute order: {error_message}")
                return None
        else:
            logging.error("Failed to execute order: No response from endpoint.")
            return None
    
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
        ## invalidate cache
        url = f'{self.base_url}/portfolio/{self.account_id}/positions/invalidate'
        response = self.post_to_endpoint(url, {})

        url = f"{self.base_url}/portfolio/{self.account_id}/positions"
        response = self.get_from_endpoint(url, "Getting account positions")

        return response
    
    def stop(self):        
        # Check if the Docker image is already running
        if hasattr(self, "api_url"):
            return

        subprocess.run(['docker', 'rm', '-f', 'lumibot-client-portal'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def get_chains(self, asset: Asset, quote: Asset = None) -> dict:
        '''
            - `Multiplier` (str) eg: `100`
            - 'Chains' - paired Expiration/Strike info to guarentee that the strikes are valid for the specific
                         expiration date.
                         Format:
                           chains['Chains']['CALL'][exp_date] = [strike1, strike2, ...]
                         Expiration Date Format: 2023-07-31
        '''
        
        chains = {
            "Multiplier": asset.multiplier,
            "Exchange": "unknown",
            "Chains": {
            "CALL": {},
            "PUT": {}
            }
        }
        logging.info("This task is extremely slow. If you still wish to use it, prepare yourself for a long wait.")

        url_for_dates = f'{self.base_url}/iserver/secdef/search?symbol={asset.symbol}'
        response = self.get_from_endpoint(url_for_dates, "Getting Option Dates")
        
        conid = response[0]['conid']

        option_dates = None 
        for section in response[0]['sections']:
            if section['secType'] == "OPT":
                option_dates = section['months'] 
                break

        # Array of options dates for asset
        months = option_dates.split(';') # in MMMYY

        for month in months:
            url_for_strikes = f'{self.base_url}/iserver/secdef/strikes?sectype=OPT&conid={conid}&month={month}' ## &exchange could be added
            strikes = self.get_from_endpoint(url_for_strikes, "Getting Strikes")

            for strike in strikes['call']:
                url_for_expiry = f'{self.base_url}/iserver/secdef/info?conid={conid}&sectype=OPT&month={month}&right=C&strike={strike}'
                contract_info = self.get_from_endpoint(url_for_expiry, "Getting expiration Date")
                if contract_info is not None:
                    expiry_date = contract_info[0]['maturityDate']
                    expiry_date = datetime.strptime(expiry_date, "%Y%m%d").strftime("%Y-%m-%d") # convert to yyyy-mm-dd
                    if expiry_date not in chains['Chains']['CALL']:
                        chains['Chains']['CALL'][expiry_date] = []
                    chains['Chains']['CALL'][expiry_date].append(strike)
            
            for strike in strikes['put']:
                url_for_expiry = f'{self.base_url}/iserver/secdef/info?conid={conid}&sectype=OPT&month={month}&right=P&strike={strike}'
                contract_info = self.get_from_endpoint(url_for_expiry, "Getting expiration Date")
                if contract_info is not None:
                    expiry_date = contract_info[0]['maturityDate']
                    expiry_date = datetime.strptime(expiry_date, "%Y%m%d").strftime("%Y-%m-%d") # convert to yyyy-mm-dd
                    if expiry_date not in chains['Chains']['CALL']:
                        chains['Chains']['PUT'][expiry_date] = []
                    chains['Chains']['PUT'][expiry_date].append(strike)

        return chains

    def get_historical_prices(
        self, asset, length, timestep="", timeshift=None, quote=None, exchange=None, include_after_hours=True
    ) -> Bars:
        logging.error(colored("Method 'get_historical_prices' is not yet implemented.", "red"))
        return None  # Return None as a placeholder

    def get_last_price(self, asset, quote=None, exchange=None) -> float:
        field = "last_price"
        response = self.get_market_snapshot(asset, [field])
        return response[field]
    
    def get_spread_conid(self, conid):
        url = f'{self.base_url}/iserver/secdef/info?conid={conid}'
        response = self.get_from_endpoint(url, "Getting Asset Currency")
        return SPREAD_CONID_MAP.get(response['currency'], None)

    def get_conid_from_asset(self, asset: Asset):
        url = f'{self.base_url}/iserver/secdef/search?symbol={asset.symbol}'
        response = self.get_from_endpoint(url, "Getting Asset conid")

        conid = int(response[0]["conid"])

        expiration_date = asset.expiration.strftime("%Y%m%d")
        expiration_month = asset.expiration.strftime("%b%y").upper()  # in MMMYY
        strike = asset.strike

        if asset.asset_type == "option":
            url_for_expiry = f'{self.base_url}/iserver/secdef/info?conid={conid}&sectype=OPT&month={expiration_month}&right=C&strike={strike}'
            contract_info = self.get_from_endpoint(url_for_expiry, "Getting expiration Date")

            matching_contract = next((contract for contract in contract_info if contract["maturityDate"] == expiration_date), None)

            if matching_contract:
                return matching_contract['conid']
            else:
                logging.error(f"No matching contract found for expiration date {expiration_date}")
            return None

        elif asset.asset_type == "stock":
            return conid
    
    def get_sectype_from_conid(self, conId):
        url = f'{self.base_url}/iserver/contract/{conId}/info'
        response = self.get_from_endpoint(url, "Getting SecType")
        return response["instrument_type"]

    def query_greeks(self, asset: Asset):
        greeks = self.get_market_snapshot(asset, ["vega", "theta", "gamma", "delta"])
        return greeks

    def get_market_snapshot(self, asset: Asset, fields: list):
        all_fields = {
            "84": "bid",
            "86": "ask",
            "31": "last_price",
            "7283": "implied_volatility", ## could be the wrong iv, there are a lot
            "7311": "vega",
            "7310": "theta",
            "7308": "delta",
            "7309": "gamma"
            # https://www.interactivebrokers.com/campus/ibkr-api-page/webapi-ref/#tag/Trading-Market-Data/paths/~1iserver~1marketdata~1snapshot/get
        }

        conId = self.get_conid_from_asset(asset)

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
        if not result:
            return None
        
        if result["bid"] == -1:
            result["bid"] = None
        if result["ask"] == -1:
            result["ask"] = None

        return result
    