import datetime
import json
import logging

import requests
from dateutil import tz
from lumibot.data_sources import TradovateData

from .broker import Broker


class Tradovate(TradovateData, Broker):
    NAME = "tradovate"

    def __init__(self, config, max_workers=20, chunk_size=100, connect_stream=False):
        # Calling init methods
        TradovateData.__init__(
            self, config, max_workers=max_workers, chunk_size=chunk_size
        )
        Broker.__init__(self, name=self.NAME, connect_stream=connect_stream)
        self.market = "us_futures"

        self.url = config["URL"]
        self.username = config["USERNAME"]
        self.password = config["PASSWORD"]
        self.cid = config["CID"]
        self.sec = config["SEC"]
        self.account_name = config["ACCOUNT_NAME"]

        # TODO: Must renew access token every 24 hours
        self.get_access_token(
            self.username,
            self.password,
            self.cid,
            self.sec,
            self.url,
        )

        self.get_account_id(self.access_token, self.url, self.account_name)

    def get_account_id(self, access_token, url, account_name):
        url = f"{url}/account/list"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        response = requests.get(url, headers=headers)

        account_info = response.json()

        if len(account_info) == 0:
            raise Exception("Tradovate did not return any accounts!")
        else:
            for account in account_info:
                if (
                    account["name"] == account_name
                    or account["nickname"] == account_name
                ):
                    self.account_id = account["id"]
                    self.account_name = account["name"]
                    break

            if self.account_id is None:
                raise Exception(
                    f"Could not find an account named '{account_name}' at Tradovate! Please check that your ACCOUNT_NAME is correct."
                )

            return self.account_id

    def get_access_token(self, username, password, cid, sec, url):
        body = {
            "name": username,
            "password": password,
            # "appId": "lumibot 1",
            # "appVersion": "1.0",
            "cid": cid,
            "sec": sec,
            "deviceId": "123e4567-e89b-12d3-a456-426614174000",
        }

        response = requests.post(
            f"{url}/auth/accesstokenrequest",
            # method: 'POST',
            # mode: 'cors',
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            json=body,
        )

        if response.status_code == 200:
            login_response = response.json()

            if "accessToken" in login_response:
                access_token = login_response["accessToken"]
                print("Login successful!")

                self.access_token = access_token

                return access_token
            else:
                errorText = ""
                if "errorText" in login_response:
                    errorText = login_response["errorText"]

                raise Exception(f"Login to Tradovate failed! {errorText}")
        else:
            raise Exception(
                f"Login to Tradovate failed! Status code: {response.status_code}. Please check that your credentials are correct."
            )

    # =========Clock functions=====================

    def get_timestamp(self):
        """return current timestamp"""
        logging.error("get_timestamp() not implemented for Tradovate")
        return None

    # =========Positions functions==================

    def _get_balances_at_broker(self, quote_asset):
        """Get's the current actual cash, positions value, and total
        liquidation value from Alpaca.

        This method will get the current actual values from Alpaca
        for the actual cash, positions value, and total liquidation.

        Returns
        -------
        tuple of float
            (cash, positions_value, total_liquidation_value)
        """

        total_cash_value = 0
        gross_positions_value = 0
        net_liquidation_value = 0

        return (total_cash_value, gross_positions_value, net_liquidation_value)

    def _pull_positions(self, strategy):
        return []

    def get_historical_account_value(self):
        """Get the historical account value of the account."""

        return {
            "minute": None,
            "hour": None,
            "day": None,
        }