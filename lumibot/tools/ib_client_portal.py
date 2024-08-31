import logging
import subprocess
import time
import requests
import os
import urllib3

urllib3.disable_warnings()

class IBClientPortal:
    def __init__(self, config, api_url):
        if api_url is None:
            self.port = "3000"
            self.base_url = f'https://localhost:{self.port}/v1'
        else:
            self.api_url = api_url
            self.base_url = api_url

        self.account_info_endpoint = f'{self.base_url}/portal/account/summary'
        
        self.ib_username = config["IB_USERNAME"]
        self.ib_password = config["IB_PASSWORD"]

        self.start_ibeam()

        # Set self.account_id
        if config["ACCOUNT_ID"]:
            self.account_id = config["ACCOUNT_ID"]
        else:
            url = f'{self.base_url}/api/portfolio/accounts'
            response = self.get_json_from_endpoint(url, "Fetching Account ID")
            if response is not None:
                self.account_id = response[0]['id']
            else:
                logging.error(f"Failed to get account id.")
        
    def start_ibeam(self):
        if not hasattr(self, "api_url"):
            # Run the Docker image with the specified environment variables and port mapping        
            logging.info("Starting IBeam...")
            existing_container = subprocess.run(['docker', 'ps', '-q', '-f', f'expose={self.port}'], capture_output=True, text=True)
            if existing_container.stdout:
                # Kill the existing container
                subprocess.run(['docker', 'kill', existing_container.stdout.strip()], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            
            inputs_dir = '/srv/clientportal.gw/root/conf.yaml'
            env_variables = {
                'IBEAM_ACCOUNT': self.ib_username,
                'IBEAM_PASSWORD': self.ib_password,
                'IBEAM_GATEWAY_BASE_URL': self.base_url,
                'IBEAM_LOG_TO_FILE': False,
                'IBEAM_REQUEST_RETRIES': 3,
                'IBEAM_INPUTS_DIR': inputs_dir
            }
            env_args = [f'--env={key}={value}' for key, value in env_variables.items()]
            conf_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "ib-rest-conf.yaml"))
            volume_mount = f'{conf_path}:{inputs_dir}'

            subprocess.run(['docker', 'run', '-d', *env_args, '-p', f'{self.port}:{self.port}', '-v', volume_mount, 'voyz/ibeam'], stdout=subprocess.DEVNULL, text=True)

        while not self.is_client_portal_running():
            time.sleep(10)
            
        logging.info("Started IBeam")

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
        url = f'{self.base_url}/portal/iserver/auth/status'
        response = self.get_json_from_endpoint(url, "Status Check", silent=True)
        if response is None:
            return False
        else:
            return True

    def get_contract_details_for_contract(self, contract):
        conid=contract.conId
        url = f"{self.base_url}/iserver/contract/{conid}/info"
        response = self.get_json_from_endpoint(url, "Getting contract details")
        return response
    
    def get_account_info(self):
        url = f"{self.account_info_endpoint}?accountId={self.account_id}"
        response = self.get_json_from_endpoint(url, "Getting account info")
        return response
        
    def get_account_balances(self):
        """
        Retrieves the account balances for a given account ID.
        """
        # Define the endpoint URL for fetching account balances
        url = f"{self.base_url}/portal/portfolio/{self.account_id}/ledger"
        response = self.get_json_from_endpoint(url, "Getting account balances")
        return response
        
    def get_json_from_endpoint(self, endpoint, description, silent=False):
        try:
            # Make the request to the endpoint
            logging.info(endpoint)
            response = requests.get(endpoint, verify=False)

            # Check if the request was successful
            if response.status_code == 200:
                # Return the JSON response containing the account balances
                return response.json()
            elif response.status_code == 404:
                if not silent:
                    logging.warning(f"{description} endpoint not found.")
                return None
            else:
                # Log an error message if the request failed
                if not silent:
                    logging.error(f"Task '{description}' Failed. Status code: {response.status_code}")
                return None

        except Exception as e:
            # Log an error message if there was a problem with the request
            if not silent:
                logging.error(f"Error {description}: {e}")
            return None
    
    def post_to_endpoint(self, url, json):
        try:
            response = requests.post(url, json=json, verify=False)
            response.raise_for_status()
            return response.json()  # Or process the response as needed
        except requests.exceptions.RequestException as e:
            logging.error(f"POST Request failed: {e}")
            return None
    
    def get_open_orders(self):
        # Define the endpoint URL for fetching account balances
        url = f'{self.base_url}/iserver/account/orders'
        response = self.get_json_from_endpoint(url, "Getting open orders")
        return response
    
    def execute_order(self, order_data):
        url = f'{self.base_url}/iserver/account/{self.account_id}/orders'
        response = self.post_to_endpoint(url, order_data)
        return response["id"]
            
    def get_positions(self):
        """
        Retrieves the current positions for a given account ID.
        """
        url = f"{self.base_url}/portal/portfolio/{self.account_id}/positions"
        response = self.get_json_from_endpoint(url, "Getting account positions")
        return response

    def stop(self):        
        # Check if the Docker image is already running
        if hasattr(self, "api_url"):
            return

        existing_container = subprocess.run(['docker', 'ps', '-q', '-f', f'expose={self.port}'], capture_output=True, text=True)
        if existing_container.stdout:
            # Kill the existing container
            subprocess.run(['docker', 'kill', existing_container.stdout.strip()])
            logging.info("Interactive Brokers Client Portal process terminated.")
        else:
            logging.info("No Client Portal process to terminate.")