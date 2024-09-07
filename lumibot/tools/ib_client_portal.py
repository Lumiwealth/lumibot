import logging
import subprocess
import time
import requests
import os
import urllib3
import requests

urllib3.disable_warnings()

class IBClientPortal:
    def __init__(self, config, api_url):
        if api_url is None:
            self.port = "4234"
            self.base_url = f'https://localhost:{self.port}/v1/api'
        else:
            self.api_url = api_url
            self.base_url = f'{api_url}/v1/api'

        self.account_info_endpoint = f'{self.base_url}/portal/account/summary'
        
        self.ib_username = config["IB_USERNAME"]
        self.ib_password = config["IB_PASSWORD"]

        self.start_ibeam()
        
        # Set self.account_id
        if config["ACCOUNT_ID"]:
            self.account_id = config["ACCOUNT_ID"]
        else:
            url = f'{self.base_url}/portfolio/accounts'
            response = self.get_from_endpoint(url, "Fetching Account ID")
            if response is not None:
                self.account_id = response[0]['id']
            else:
                logging.error(f"Failed to get Account ID.")
        
    def start_ibeam(self):
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
            conf_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "conf.yaml"))
            volume_mount = f'{conf_path}:{inputs_dir}'

            subprocess.run(['docker', 'rm', '-f', 'lumibot-client-portal'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            subprocess.run(['docker', 'run', '-d', '--name', 'lumibot-client-portal', *env_args, '-p', f'{self.port}:{self.port}', '-v', volume_mount, 'voyz/ibeam'], stdout=subprocess.DEVNULL, text=True)

            # check if authenticated
            time.sleep(10)

        while not self.is_authenticated():
            logging.info("Not connected to API server yet.")
            logging.info("Waiting for another 10 seconds before checking again...")
            time.sleep(10)
        
        logging.info("Connected to Client Portal")

    def is_authenticated(self):
        url = f'{self.base_url}/iserver/accounts'
        response = self.get_from_endpoint(url, "Auth Check", silent=True)
        if response is not None:
            return True
        else:
            return False
        
    def getConID(self, symbol, asset_type):
        url = f"{self.base_url}/iserver/secdef/search"

        body = {
            "symbol": symbol,
            "name": True,
            "sectype": asset_type
        }

        response = self.post_to_endpoint(url, body)

        if response is not None and len(response) > 0:
            return response[0]["conid"]
        else:
            return None
    
    def is_client_portal_running(self):
        url = f'{self.base_url}/iserver/accounts'
        response = self.get_from_endpoint(url, "Getting Accounts")
        if response:
            return True
        else:
            return False

    def get_contract_details(self, conId):
        url = f"{self.base_url}/iserver/account/{account_id}/order/{order_id}"
        response = self.get_from_endpoint(url, "Getting contract details")
        logging.info(response)

        return response
    
    def get_account_info(self):
        url = f"{self.account_info_endpoint}?accountId={self.account_id}"
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
    
    def post_to_endpoint(self, url, json):
        try:
            response = requests.post(url, json=json, verify=False)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"POST Request failed: {e}")
            return None
    
    def delete_to_endpoint(self, url):
        try:
            response = requests.delete(url, verify=False)
            response.raise_for_status()
            return True
        except requests.exceptions.RequestException as e:
            logging.error(f"DELETE Request failed: {e}")
            return False
    
    def get_open_orders(self):
        # Define the endpoint URL for fetching account balances
        url = f'{self.base_url}/iserver/account/orders?&accountId={self.account_id}&filters=Submitted,PreSubmitted' ## force=true doesn't work?
        response = self.get_from_endpoint(url, "Getting open orders")
        if response is None:
            return
        
        return response['orders']
    
    def execute_order(self, order_data):
        url = f'{self.base_url}/iserver/account/{self.account_id}/orders'
        response = self.post_to_endpoint(url, order_data)
        return response["id"]
    
    def delete_order(self, order):
        orderId = order.identifier
        url = f'{self.base_url}/iserver/account/{self.account_id}/order/{orderId}'
        status = self.delete_to_endpoint(url)
        if status:
            logging.info(f"Order with conid {orderId} canceled successfully.")
        else:
            logging.error(f"Failed to delete order with conid {orderId}.")

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