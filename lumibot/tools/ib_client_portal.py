import logging
import subprocess
import time
import requests
import os

class IBClientPortal:
    def __init__(self, config):
        self.port = "3000"
        self.base_url = f'https://localhost:{self.port}'

        self.status_check_endpoint = f'{self.base_url}/v1/portal/iserver/auth/status'
        self.account_list_endpoint = f'{self.base_url}/v1/api/portfolio/accounts'
        self.account_info_endpoint = f'{self.base_url}/v1/portal/account/summary'
        self.post_ready_delay = 2

        self.account_id = config["ACCOUNT_ID"]
        self.ib_username = config["IB_USERNAME"]
        self.ib_password = config["IB_PASSWORD"]

    def start_ibeam(self):
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
            'IBEAM_INPUTS_DIR': inputs_dir
        }
        env_args = [f'--env={key}={value}' for key, value in env_variables.items()]
        conf_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "conf.yaml"))
        volume_mount = f'{conf_path}:{inputs_dir}'

        subprocess.run(['docker', 'run', '-d', *env_args, '-p', f'{self.port}:{self.port}', '-v', volume_mount, 'voyz/ibeam'], stdout=subprocess.DEVNULL, text=True)

        while not self.is_client_portal_running():
            time.sleep(10)
            
        logging.info("Started IBeam")

    def is_client_portal_running(self):
        try:
            response = requests.get(self.account_list_endpoint, verify=False)
            if response.status_code == 200:
                return True
            else:
                logging.debug(f"Failed to get account list. Status code: {response.status_code}")
                return False
        except Exception:
            return False


    def get_contract_details_for_contract(self, contract):
        conid=contract.conId
        url = f"{self.base_url}/v1/iserver/contract/{conid}/info"
        response = self.get_json_from_endpoint(url, "getting account details")
        return response
    
    def get_account_info(self):
        try:
            logging.info("Attempting to retrieve account information.")
            response = requests.get(self.account_list_endpoint, verify=False)
            if response.status_code == 200:
                accounts_data = response.json()
                if accounts_data:
                    if not self.account_id:
                        self.account_id = accounts_data[0]['id']

                    response = requests.get(f"{self.account_info_endpoint}?accountId={self.account_id}", verify=False)
                    if response.status_code == 200:
                        return response.json()
                    elif response.status_code == 404:
                        logging.warning("Account information endpoint not found. Returning list of accounts.")
                        return {'accounts': accounts_data}
                    else:
                        logging.error(f"Failed to get detailed account info. Status code: {response.status_code}")
                else:
                    logging.warning("No accounts found in the response.")
            else:
                logging.error(f"Failed to list accounts. Status code: {response.status_code}")
            return None
        except requests.exceptions.RequestException as e:
            logging.error(f"Error getting account info: {e}")
            return None
        
    def get_account_balances(self):
        """
        Retrieves the account balances for a given account ID.
        """
        # Define the endpoint URL for fetching account balances
        url = f"{self.base_url}/v1/portal/portfolio/{self.account_id}/ledger"
        response = self.get_json_from_endpoint(url, "getting account balances")
        return response
        
    def get_json_from_endpoint(self, endpoint, description):
        """
        Retrieves the account balances for a given account ID.
        """
        try:
            # Make the request to the endpoint
            response = requests.get(endpoint, verify=False)

            # Check if the request was successful
            if response.status_code == 200:
                # Return the JSON response containing the account balances
                return response.json()
            else:
                # Log an error message if the request failed
                logging.error(f"Failed {description}. Status code: {response.status_code}")
                return None

        except requests.exceptions.RequestException as e:
            # Log an error message if there was a problem with the request
            logging.error(f"Error {description}: {e}")
            return None
        
    def get_open_orders(self):
        # Define the endpoint URL for fetching account balances
        url = f'{self.base_url}/v1/iserver/account/orders?filters=accountId={self.account_id}'
        response = self.get_json_from_endpoint(url, "getting open orders")
        return response
    
    def get_positions(self):
        """
        Retrieves the current positions for a given account ID.
        """
        url = f"{self.base_url}/v1/portal/portfolio/{self.account_id}/positions"
        response = self.get_json_from_endpoint(url, "getting account positions")
        return response

    def run(self):
        if self.start_ibeam():
            time.sleep(3)
            if self.is_client_portal_running():
                time.sleep(self.post_ready_delay)
                return self.get_account_info()
        return None

    def stop(self):        
        # Check if the Docker image is already running
        existing_container = subprocess.run(['docker', 'ps', '-q', '-f', f'expose={self.port}'], capture_output=True, text=True)
        if existing_container.stdout:
            # Kill the existing container
            subprocess.run(['docker', 'kill', existing_container.stdout.strip()])
            logging.info("Interactive Brokers Client Portal process terminated.")
        else:
            logging.info("No Client Portal process to terminate.")