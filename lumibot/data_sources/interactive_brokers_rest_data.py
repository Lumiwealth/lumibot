from __future__ import annotations

import time

from lumibot._lazy_imports import LazyLogger, LazyModule, LazyPytzTimezoneRef, lazy_class
from lumibot.tools.ibkr_secdef import (
    IbkrFuturesExchangeAmbiguousError,
    select_futures_exchange_from_secdef_search_payload,
)

from .data_source import DataSource

logger = LazyLogger(__name__)
TYPE_CHECKING = False

datetime = lazy_class("datetime", "datetime")
timezone = lazy_class("datetime", "timezone")
Decimal = lazy_class("decimal", "Decimal")
IbkrGateway = lazy_class("lumibot.data_sources.ibkr_gateway", "IbkrGateway")
Asset = lazy_class("lumibot.entities", "Asset")
_json = LazyModule("json")
pd = LazyModule("pandas")
requests = LazyModule("requests")
_DEFAULT_PYTZ = LazyPytzTimezoneRef("America/New_York")

if TYPE_CHECKING:
    from ..entities import Bars


def _default_pytz():
    return _DEFAULT_PYTZ._load()


def __getattr__(name):
    if name == "LUMIBOT_DEFAULT_PYTZ":
        return _default_pytz()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def _disable_urllib3_warnings():
    import urllib3

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def colored(*args, **kwargs):
    from termcolor import colored as _colored

    return _colored(*args, **kwargs)


def _ibkr_gateway_module():
    from . import ibkr_gateway

    return ibkr_gateway


def _url_hostname(value):
    from urllib.parse import urlparse

    return (urlparse(value).hostname or "").lower()


def _conf_yaml_text():
    import importlib.resources

    return importlib.resources.files('lumibot.resources').joinpath('conf.yaml').read_text(encoding='utf-8')


def _as_bool(value, *, default=False):
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"Expected a boolean value, received {value!r}")


def _positive_float(value, *, name, default):
    if value is None or value == "":
        return float(default)
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a number") from exc
    if parsed <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return parsed


def _rest_api_base_url(value):
    base_url = str(value or "").strip().rstrip("/")
    if not base_url:
        raise ValueError("IBKR API URL cannot be empty")
    if base_url.endswith("/v1/api"):
        return base_url
    return f"{base_url}/v1/api"


def _get_bars_class():
    from ..entities import Bars

    return Bars

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

    def __init__(
        self,
        config,
        *,
        gateway: IbkrGateway | None = None,
        http_client=None,
        sleep_fn=None,
        monotonic_fn=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        _disable_urllib3_warnings()
        ibkr_gateway = _ibkr_gateway_module()

        config = dict(config or {})
        self._sleep = sleep_fn or time.sleep
        self._monotonic = monotonic_fn or time.monotonic
        self.http_client = http_client or requests
        self.request_timeout = _positive_float(
            config.get("REQUEST_TIMEOUT"), name="IB_REQUEST_TIMEOUT", default=30
        )
        self.auth_timeout = _positive_float(
            config.get("AUTH_TIMEOUT"), name="IB_AUTH_TIMEOUT", default=300
        )
        self.auth_poll_interval = _positive_float(
            config.get("AUTH_POLL_INTERVAL"),
            name="IB_AUTH_POLL_INTERVAL",
            default=5,
        )

        try:
            gateway_port = int(
                config.get("GATEWAY_PORT") or ibkr_gateway.DEFAULT_IBEAM_HOST_PORT
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("IB_GATEWAY_PORT must be an integer") from exc
        self.port = str(gateway_port)

        api_url = config.get("API_URL")
        running_on_server = _as_bool(config.get("RUNNING_ON_SERVER"), default=False)
        if gateway is None:
            if api_url:
                self.api_url = str(api_url).strip().rstrip("/")
                gateway = ibkr_gateway.ExternalIbkrGateway(
                    _rest_api_base_url(self.api_url)
                )
            elif running_on_server:
                gateway = ibkr_gateway.ExternalIbkrGateway(
                    f"https://localhost:{gateway_port}/v1/api"
                )
            else:
                gateway = ibkr_gateway.IBeamGateway(
                    username=config.get("IB_USERNAME"),
                    password=config.get("IB_PASSWORD"),
                    conf_text=_conf_yaml_text(),
                    host_port=gateway_port,
                    paper=_as_bool(config.get("USE_PAPER_ACCOUNT"), default=True),
                    image_tag=(
                        config.get("IBEAM_DOCKER_TAG")
                        or ibkr_gateway.DEFAULT_IBEAM_TAG
                    ),
                    instance_id=config.get("GATEWAY_INSTANCE_ID"),
                )

        self.gateway = gateway
        self.base_url = gateway.base_url.rstrip("/")
        self.running_on_server = not isinstance(gateway, ibkr_gateway.IBeamGateway)
        verify_ssl = config.get("VERIFY_SSL")
        if verify_ssl is None or verify_ssl == "":
            hostname = _url_hostname(self.base_url)
            self.verify_ssl = hostname not in {"localhost", "127.0.0.1", "::1"}
        else:
            self.verify_ssl = _as_bool(verify_ssl)

        self.account_id = config.get("IB_ACCOUNT_ID")
        self._futures_exchange_cache: dict[str, str] = {}
        self.start()

    def start(self):
        try:
            self.gateway.start()
            deadline = self._monotonic() + self.auth_timeout
            while not self.is_authenticated():
                remaining = deadline - self._monotonic()
                if remaining <= 0:
                    raise TimeoutError(
                        "IBKR gateway did not authenticate before IB_AUTH_TIMEOUT. "
                        "Complete required browser/2FA authentication and retry."
                    )
                logger.info(
                    colored(
                        "Waiting for Interactive Brokers REST authentication...",
                        "yellow",
                    )
                )
                self._sleep(min(self.auth_poll_interval, remaining))

            self.fetch_account_id()
            logger.info(colored("Connected to the Interactive Brokers API", "green"))
            self.suppress_warnings()
        except Exception:
            self.gateway.stop()
            raise

    def suppress_warnings(self):
        # Suppress weird server warnings
        url = f"{self.base_url}/iserver/questions/suppress"
        json = {"messageIds": ["o451", "o383", "o354", "o163"]}

        self.post_to_endpoint(
            url,
            json=json,
            description="Suppressing server warnings",
            max_retries=3,
        )

    def fetch_account_id(self):
        if self.account_id is not None:
            return  # Account ID already set

        url = f"{self.base_url}/portfolio/accounts"

        response = self.get_from_endpoint(url, "Fetching Account ID", max_retries=3)
        if not isinstance(response, list) or not response or not response[0].get("id"):
            raise RuntimeError("IBKR did not return an account identifier")
        self.last_portfolio_ping = datetime.now()
        self.account_id = response[0]["id"]

    def is_authenticated(self):
        url = f"{self.base_url}/iserver/accounts"
        response = self.get_from_endpoint(
            url, "Auth Check", silent=True, max_retries=0
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
            logger.error(
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
            logger.error(
                colored(
                    f"Couldn't get account balances. Error: {response['error']}",
                    "red",
                )
            )
            return None

        return response

    def handle_http_errors(self, response, silent, retries, description, allow_fail):
        def show_error(retries, allow_fail):
            if not allow_fail:
                if retries%60 == 0:
                    return True
            else:
                return True

            return False

        to_return = None
        re_msg = None
        is_error = False

        if response.text:
            try:
                response_json = response.json()
            except ValueError:
                logger.error(
                    colored("Invalid JSON response", "red")
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

        if 'xcredserv comm failed during getEvents due to Connection refused' in error_message:
            retrying = True
            re_msg = "The server is undergoing maintenance. Should fix itself soon"

        elif 'Please query /accounts first' in error_message:
            self.ping_iserver()
            retrying = True
            re_msg = "Lumibot got Deauthenticated"

        elif 'There was an error processing the request. Please try again.' in error_message:
            retrying = True
            re_msg = "Something went wrong."

        elif "no bridge" in error_message.lower() or "not authenticated" in error_message.lower():
            retrying = True
            re_msg = "Not Authenticated"

        elif 200 <= status_code < 300:
            to_return = response_json
            retrying = False

        elif status_code == 429:
            retrying = True
            re_msg = "You got rate limited"

        elif status_code == 503:
            re_msg = "Internal server error. Should fix itself soon"
            retrying = True

        elif status_code == 500:
            to_return = response_json
            is_error = True
            retrying = False

        elif status_code == 410:
            retrying = True
            re_msg = "The bridge blew up"

        elif 400 <= status_code < 500:
            to_return = response_json
            is_error = True
            retrying = False

        else:
            retrying = False

        if re_msg is not None:
            if not silent and retries%60 == 0:
                logger.warning(colored(f"Task {description} failed: {re_msg}. Retrying...", "yellow"))
            else:
                logger.debug(colored(f"Task {description} failed: {re_msg}. Retrying...", "yellow"))

        elif is_error:
            if not silent and show_error(retries, allow_fail):
                logger.error(colored(f"Task {description} failed: {to_return}", "red"))
            else:
                logger.debug(colored(f"Task {description} failed: {to_return}", "red"))

        return (retrying, re_msg, is_error, to_return)

    def get_from_endpoint(self, url, description="", silent=False, allow_fail=True, max_retries=None):
        to_return = None
        retries = 0
        retrying = True

        while retrying or not allow_fail:
            try:
                response = self.http_client.get(
                    url,
                    verify=self.verify_ssl,
                    timeout=self.request_timeout,
                )
            except requests.exceptions.RequestException as e:
                response = requests.Response()
                response.status_code = 503
                response._content = _json.dumps({"error": str(e)}).encode("utf-8")

            # Check if the status code is 401
            if response.status_code == 401:
                logger.error(
                    colored(
                        "401 Unauthorized. Check Interactive Brokers credentials "
                        "and complete required two-factor authentication.",
                        "red",
                    )
                )
                return None

            retrying, re_msg, is_error, to_return = self.handle_http_errors(
                response, silent, retries, description, allow_fail
            )

            if re_msg is None and not is_error:
                break

            if max_retries is not None and retries >= max_retries:
                break

            retries+=1
            if retrying or not allow_fail:
                self._sleep(1)

        return to_return

    def post_to_endpoint(self, url, json: dict, description="", silent=False, allow_fail=True, max_retries=None):
        to_return = None
        retries = 0
        retrying = True

        while retrying or not allow_fail:
            try:
                response = self.http_client.post(
                    url,
                    json=json,
                    verify=self.verify_ssl,
                    timeout=self.request_timeout,
                )
            except requests.exceptions.RequestException as e:
                response = requests.Response()
                response.status_code = 503
                response._content = _json.dumps({"error": str(e)}).encode("utf-8")

            retrying, re_msg, is_error, to_return = self.handle_http_errors(
                response, silent, retries, description, allow_fail
            )

            if re_msg is None and not is_error:
                break

            if max_retries is not None and retries >= max_retries:
                break

            retries+=1
            if retrying or not allow_fail:
                self._sleep(1)

        return to_return

    def delete_to_endpoint(self, url, description="", silent=False, allow_fail=True, max_retries=None):
        to_return = None
        retries = 0
        retrying = True

        while retrying or not allow_fail:
            try:
                response = self.http_client.delete(
                    url,
                    verify=self.verify_ssl,
                    timeout=self.request_timeout,
                )
            except requests.exceptions.RequestException as e:
                response = requests.Response()
                response.status_code = 503
                response._content = _json.dumps({"error": str(e)}).encode("utf-8")

            retrying, re_msg, is_error, to_return = self.handle_http_errors(
                response, silent, retries, description, allow_fail
            )

            if re_msg is None and not is_error:
                break

            if max_retries is not None and retries >= max_retries:
                break

            retries+=1
            if retrying or not allow_fail:
                self._sleep(1)

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

    def execute_order(self, order_data, return_raw_response=False):
        if order_data is None:
            logger.debug(colored("Failed to get order data.", "red"))
            return None

        self.ping_iserver()

        url = f"{self.base_url}/iserver/account/{self.account_id}/orders"
        response = self.post_to_endpoint(url, order_data, description="Executing order")

        # Atomic order packages need the complete broker response so the
        # broker adapter can validate every acknowledgement and compensate for
        # partial acceptance. Confirmation handling remains in post_to_endpoint.
        if return_raw_response:
            return response

        if isinstance(response, list) and "order_id" in response[0]:
            # success
            return response

        elif response is not None and "error" in response:
            logger.error(
                colored(f"Failed to execute order: {response['error']}", "red")
            )
            return None
        elif response is not None and "message" in response:
            logger.error(
                colored(f"Failed to execute order: {response['message']}", "red")
            )
            return None
        elif response is not None:
            logger.error(colored(f"Failed to execute order: {response}", "red"))
        else:
            logger.error(colored(f"Failed to execute order: {order_data}", "red"))

    def delete_order(self, order):
        self.ping_iserver()
        orderId = order.identifier
        url = f"{self.base_url}/iserver/account/{self.account_id}/order/{orderId}"
        status = self.delete_to_endpoint(url, description=f"Deleting order {orderId}")
        if status:
            logger.info(
                colored(f"Order with ID {orderId} canceled successfully.", "green")
            )
        else:
            logger.error(colored(f"Failed to delete order with ID {orderId}.", "red"))

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
            logger.error(
                colored(
                    f"Couldn't get account positions. Error: {response['error']}",
                    "red",
                )
            )
            return None

        return response

    def stop(self):
        self.gateway.stop()

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
        logger.info(
            "This task is extremely slow. If you still wish to use it, prepare yourself for a long wait."
        )
        self.ping_iserver()

        url_for_dates = f"{self.base_url}/iserver/secdef/search?symbol={asset.symbol}"
        response = self.get_from_endpoint(url_for_dates, "Getting Option Dates")

        if response and isinstance(response, list) and "conid" in response[0]:
            conid = response[0]["conid"]
        else:
            logger.error("Failed to get conid from response")
            return {}

        option_dates = None
        if response and isinstance(response, list) and "sections" in response[0]:
            for section in response[0]["sections"]:
                if "secType" in section and section["secType"] == "OPT":
                    option_dates = section["months"]
                    break
        else:
            logger.error("Failed to get sections from response")
            return {}

        # Array of options dates for asset
        if option_dates:
            months = option_dates.split(";")  # in MMMYY
        else:
            logger.error("Option dates are None")
            return {}

        for month in months:
            # TODO &exchange could be added
            url_for_strikes = f"{self.base_url}/iserver/secdef/strikes?sectype=OPT&conid={conid}&month={month}"
            strikes = self.get_from_endpoint(url_for_strikes, "Getting Strikes")

            if strikes and "call" in strikes:
                for strike in strikes["call"]:
                    url_for_expiry = (
                        f"{self.base_url}/iserver/secdef/info?conid={conid}"
                        f"&sectype=OPT&month={month}&right=C&strike={strike}"
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
                        expiry_date = contract_info[0]["maturityDate"]
                        expiry_date = datetime.strptime(expiry_date, "%Y%m%d").strftime(
                            "%Y-%m-%d"
                        )  # convert to yyyy-mm-dd
                        if expiry_date not in chains["Chains"]["CALL"]:
                            chains["Chains"]["CALL"][expiry_date] = []
                        chains["Chains"]["CALL"][expiry_date].append(strike)
                    else:
                        logger.error("Invalid contract_info format")
                        return {}

            if strikes and "put" in strikes:
                for strike in strikes["put"]:
                    url_for_expiry = (
                        f"{self.base_url}/iserver/secdef/info?conid={conid}"
                        f"&sectype=OPT&month={month}&right=P&strike={strike}"
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
                        expiry_date = contract_info[0]["maturityDate"]
                        expiry_date = datetime.strptime(expiry_date, "%Y%m%d").strftime(
                            "%Y-%m-%d"
                        )  # convert to yyyy-mm-dd
                        if expiry_date not in chains["Chains"]["PUT"]:
                            chains["Chains"]["PUT"][expiry_date] = []
                        chains["Chains"]["PUT"][expiry_date].append(strike)
                    else:
                        logger.error("Invalid contract_info format")
                        return {}

        return chains

    def _resolve_futures_exchange(self, symbol: str) -> str:
        """Resolve the best IBKR futures exchange for a root symbol.

        Uses `iserver/secdef/search` and applies the same tie-break rules as IBKR REST backtesting:
        - prefer USD + US venues (CME/CBOT/COMEX/NYMEX) when ambiguous
        - require explicit `exchange=` when still ambiguous
        """
        sym = str(symbol or "").strip().upper()
        if not sym:
            raise ValueError("Futures exchange resolution requires a non-empty symbol")
        cached = self._futures_exchange_cache.get(sym)
        if cached:
            return cached
        self.ping_iserver()
        url = f"{self.base_url}/iserver/secdef/search?symbol={sym}&secType=FUT"
        response = self.get_from_endpoint(url, "Resolving futures exchange")
        exchange = select_futures_exchange_from_secdef_search_payload(sym, response)
        self._futures_exchange_cache[sym] = exchange
        return exchange

    def _get_earliest_future_conid(self, symbol: str, exchange: str = None):
        """
        Fetch the conid for the earliest-expiring continuous future for a given symbol and exchange.
        """
        url = f"{self.base_url}/trsrv/futures"
        exchange_val = str(exchange or "").strip().upper() or self._resolve_futures_exchange(symbol)
        params = {"symbols": symbol, "secType": "CONTFUT", "exchange": exchange_val}
        try:
            response = self.http_client.get(
                url,
                params=params,
                verify=self.verify_ssl,
                timeout=self.request_timeout,
            )
            if response.status_code != 200:
                logger.error(colored(f"Failed to retrieve security definition for {symbol}: {response.text}", "red"))
                return None
            contracts = response.json().get(symbol, [])
            if not contracts:
                logger.error(colored(f"No contracts found for {symbol} on {exchange_val}", "red"))
                return None
            # Pick the earliest expiration
            earliest = min(contracts, key=lambda d: int(d["expirationDate"]))
            return earliest["conid"]
        except Exception as e:
            logger.error(colored(f"Error fetching continuous future conid: {e}", "red"))
            return None

    def _get_futures_conid(self, asset: Asset, exchange: str = None):
        """
        Returns the correct conid for a futures asset.
        If expiration is set, returns the specific contract conid.
        If expiration is None, returns the continuous/earliest contract conid.
        """
        if getattr(asset, "asset_type", None) in {
            Asset.AssetType.FUTURE,
            Asset.AssetType.CONT_FUTURE
        }:
            if not exchange:
                exchange = self._resolve_futures_exchange(asset.symbol)
            if getattr(asset, "expiration", None) is None:
                return self._get_earliest_future_conid(asset.symbol, exchange)
            else:
                return self._get_specific_future_conid(asset, exchange)
        return None

    def _get_specific_future_conid(self, asset: Asset, exchange: str = None):
        """
        Returns the conid for a specific futures contract (with expiration).
        """
        self.ping_iserver()
        url = f"{self.base_url}/iserver/secdef/search?symbol={asset.symbol}&secType=FUT"
        response = self.get_from_endpoint(url, "Getting Underlying conid")
        if (
            isinstance(response, list)
            and len(response) > 0
            and isinstance(response[0], dict)
            and "conid" in response[0]
        ):
            underlying_conid = int(response[0]["conid"])
        else:
            logger.error(
                colored(
                    f"Failed to get conid of asset: {asset.symbol} of type {asset.asset_type}",
                    "red",
                )
            )
            logger.error(colored(f"Response: {response}", "red"))
            return None
        try:
            exchange_val = select_futures_exchange_from_secdef_search_payload(asset.symbol, response)
        except IbkrFuturesExchangeAmbiguousError as exc:
            if exchange:
                exchange_val = exchange
            else:
                raise ValueError(
                    f"Ambiguous IBKR FUT exchange for {asset.symbol}; pass exchange=... explicitly."
                ) from exc
        return self._get_conid_for_derivative(
            underlying_conid,
            asset,
            exchange=exchange_val,
            sec_type="FUT",
            additional_params={
                "multiplier": asset.multiplier,
            },
        )

    def get_historical_prices(
        self,
        asset,
        length,
        timestep="",
        timeshift=None,
        quote=None,
        exchange=None,
        include_after_hours=True,
        return_polars: bool = False,
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
            start_time = (datetime.now(timezone.utc) - timeshift).strftime("%Y%m%d-%H:%M:%S")
        else:
            start_time = datetime.now(timezone.utc).strftime("%Y%m%d-%H:%M:%S")

        # --- Use helper for futures conid ---
        conid = None
        if getattr(asset, "asset_type", None) in {
                Asset.AssetType.FUTURE,
                Asset.AssetType.CONT_FUTURE,
        }:
            if not exchange:
                exchange = self._resolve_futures_exchange(asset.symbol)
            conid = self._get_futures_conid(asset, exchange)
        else:
            conid = self.get_conid_from_asset(asset=asset)

        # Determine the period based on the timestep and length
        # TODO fix wtvr this is
        try:
            timestep_value = int(timestep.split()[0])
        except ValueError:
            timestep_value = 1

        if "minute" in timestep:
            period = f"{length * timestep_value}min"
            timestep = f"{timestep_value}min"
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
            logger.error(colored(f"Unsupported timestep: {timestep}", "red"))
            return _get_bars_class()(
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

        url = (
            f"{self.base_url}/iserver/marketdata/history?conid={conid}"
            f"&period={period}&bar={timestep}&outsideRth={include_after_hours}"
            f"&startTime={start_time}"
        )
        if getattr(asset, "asset_type", None) == Asset.AssetType.FUTURE and getattr(asset, "expiration", None) is None:
            url += "&continuous=true"
        if exchange:
            url += f"&exchange={exchange}"

        result = self.get_from_endpoint(url, "Getting Historical Prices")

        if result and "error" in result:
            logger.error(
                colored(f"Error getting historical prices: {result['error']}", "red")
            )
            return _get_bars_class()(
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
            logger.error(
                colored(
                    f"Failed to get historical prices for {asset.symbol}, result was: {result}",
                    "red",
                )
            )
            return _get_bars_class()(
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
            df["timestamp"].dt.tz_localize("UTC").dt.tz_convert(_default_pytz())
        )
        df.set_index("timestamp", inplace=True)

        """
        # Add dividend and stock_splits columns with default values
        df['dividend'] = 0.0
        df['stock_splits'] = 0.0
        """

        bars = _get_bars_class()(df, self.SOURCE, asset, raw=df, quote=quote)

        return bars

    def get_last_price(self, asset, quote=None, exchange=None) -> float | Decimal | None:
        """
        Get the last price for an asset.
        For futures, always use get_market_snapshot (the official IBKR endpoint for all asset types).
        """
        field = "last_price"
        response = self.get_market_snapshot(asset, [field], exchange=exchange)  # Always use this for all asset types

        if response is None or field not in response:
            if getattr(asset, "asset_type", None) in ["option", "future"]:
                logger.debug(
                    f"Failed to get {field} for asset {getattr(asset, 'symbol', None)} "
                    f"with strike {getattr(asset, 'strike', None)} and expiration "
                    f"date {getattr(asset, 'expiration', None)}"
                )
            else:
                logger.debug(
                    f"Failed to get {field} for asset {getattr(asset, 'symbol', None)} "
                    f"of type {getattr(asset, 'asset_type', None)}"
                )
            return None

        price = response[field]

        # Remove the 'C' prefix if it exists
        if isinstance(price, str) and price.startswith("C"):
            price = float(price[1:])

        return float(price)

    def get_conid_from_asset(self, asset: Asset, exchange: str = None):
        # --- Use helper for futures conid ---
        if getattr(asset, "asset_type", None) in {Asset.AssetType.FUTURE, Asset.AssetType.CONT_FUTURE}:
            return self._get_futures_conid(asset, exchange)
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
            logger.error(
                colored(
                    f"Failed to get conid of asset: {asset.symbol} of type {asset.asset_type}",
                    "red",
                )
            )
            logger.error(colored(f"Response: {response}", "red"))
            return None

        if asset.asset_type == Asset.AssetType.OPTION:
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
        elif asset.asset_type == Asset.AssetType.FUTURE:
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
        elif asset.asset_type == Asset.AssetType.CONT_FUTURE:
            return underlying_conid
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
            logger.debug(
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

    def get_market_snapshot(self, asset: Asset, fields: list, exchange: str = None):
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

        conId = self.get_conid_from_asset(asset, exchange=exchange)
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
                    and field not in response[0]
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
            The quote asset to get the quote for (currently not used for Interactive Brokers).
        exchange : str, optional
            The exchange to get the quote for (currently not used for Interactive Brokers).

        Returns
        -------
        Quote
           Quote object containing bid, ask, price and other information.
        """
        result = self.get_market_snapshot(
            asset,
            ["last_price", "bid", "ask", "bid_size", "ask_size"],
            exchange=exchange,
        )
        if not result:
            return None

        result["price"] = result.pop("last_price")

        if isinstance(result["price"], str) and result["price"].startswith("C "):
            logger.warning(
                colored(
                    f"Ticker {asset.symbol} of type {asset.asset_type} with strike "
                    f"price {asset.strike} and expiry date {asset.expiration} is not "
                    "trading currently. Got the last close price instead.",
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

        # Create and return a Quote object instead of a dictionary
        from lumibot.entities import Quote
        return Quote(
            asset=asset,
            price=result.get("price"),
            bid=result.get("bid"),
            ask=result.get("ask"),
            bid_size=result.get("bid_size"),
            ask_size=result.get("ask_size"),
            raw_data=result
        )
