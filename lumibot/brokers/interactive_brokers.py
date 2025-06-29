import datetime
import logging
import os
import random
import time
from collections import deque
from decimal import Decimal
from threading import Thread
import math
from sys import exit
from functools import reduce
from termcolor import colored
from typing import Union

from dateutil import tz
from ibapi.client import *
from ibapi.contract import *
from ibapi.order import *
from ibapi.wrapper import *

from lumibot.data_sources import InteractiveBrokersData

# Naming conflict on Order between IB and Lumibot.
from lumibot.entities import Asset, Position
from lumibot.entities import Order as OrderLum

from .broker import Broker

TYPE_MAP = dict(
    stock="STK",
    option="OPT",
    future="FUT",
    forex="CASH",
    index="IND",
    multileg="BAG",
)

DATE_MAP = dict(
    future="%Y%m%d",
    option="%Y%m%d",
)

ORDERTYPE_MAPPING = dict(
    market="MKT",
    limit="LMT",
    stop="STP",
    stop_limit="STP LMT",
    trailing_stop="TRAIL",
)


class InteractiveBrokers(Broker):
    """Inherit InteractiveBrokerData first and all the price market
    methods than inherits broker"""

    def __init__(self, config, max_workers=20, chunk_size=100, data_source=None, **kwargs):
        if data_source is None:
            data_source = InteractiveBrokersData(config, max_workers=max_workers, chunk_size=chunk_size)

        super().__init__(
            name="interactive_brokers", 
            config=config, 
            data_source=data_source, 
            max_workers=max_workers, 
            **kwargs
            )
        if not self.name:
            self.name = "interactive_brokers"

        # For checking duplicate order status events from IB.
        self.order_status_duplicates = []
        # The default market is NYSE.
        self.market = (config.get("MARKET") if config else None) or os.environ.get("MARKET") or "NYSE"

        # Connection to interactive brokers
        self.ib = None

        # check if the config is a dict
        if isinstance(config, dict):
            ip = config["IP"]
            socket_port = config["SOCKET_PORT"]
            client_id = config["CLIENT_ID"]
            subaccount = config.get("IB_SUBACCOUNT")

        else:
            ip = config.IP
            socket_port = config.SOCKET_PORT
            client_id = config.CLIENT_ID
            subaccount = config.IB_SUBACCOUNT

        self.subaccount = subaccount
        self.ip = ip
        self.socket_port = socket_port
        self.client_id = client_id

        # Ensure we have a unique and non-changing client_id
        if not self.client_id:
            if self.subaccount is None:
                # Set the client_id to a random  number up to 4 digits.
                self.client_id = random.randint(1, 9999)

                # Log that a random client_id was generated.
                logging.info(f"No client_id was set. A random client_id of {client_id} was generated.")
            else:
                logging.error("No client_id was set. A unique and non-changing client_id is necessary when a subaccount is used. Consider setting one as an environment variable.")
                exit()

        self.start_ib()

    def start_ib(self):
        # Connect to interactive brokers.
        if not self.ib:
            self.ib = IBApp(ip_address=self.ip, socket_port=self.socket_port, client_id=self.client_id, subaccount=self.subaccount, ib_broker=self)

        if isinstance(self.data_source, InteractiveBrokersData):
            if not self.data_source.ib:
                self.data_source.ib = self.ib

    # =========Clock functions=====================

    def get_timestamp(self):
        """return current timestamp"""
        clock = self.ib.get_timestamp()
        return clock

    # =========Positions functions==================

    def _parse_broker_position(self, broker_position, strategy, orders=None):
        """Parse a broker position representation
        into a position object"""
        if broker_position["asset_type"] == "stock":
            asset = Asset(
                symbol=broker_position["symbol"],
            )
        elif broker_position["asset_type"] == "future":
            asset = Asset(
                symbol=broker_position["symbol"],
                asset_type="future",
                expiration=broker_position["expiration"],
                multiplier=broker_position["multiplier"],
            )
        elif broker_position["asset_type"] == "option":
            asset = Asset(
                symbol=broker_position["symbol"],
                asset_type="option",
                expiration=broker_position["expiration"],
                strike=broker_position["strike"],
                right=broker_position["right"],
                multiplier=broker_position["multiplier"],
            )
        elif broker_position["asset_type"] == "forex":
            asset = Asset(
                symbol=broker_position["symbol"],
                asset_type="forex",
            )
        else:  # Unreachable code.
            raise ValueError(
                f"From Interactive Brokers, asset type can only be `stock`, "
                f"`future`, or `option`. A value of {broker_position['asset_type']} "
                f"was received."
            )

        quantity = broker_position["position"]
        position = Position(strategy, asset, quantity, orders=orders)
        return position

    def _pull_broker_position(self, asset):
        """Given an asset, get the broker representation
        of the corresponding asset"""
        result = self._pull_broker_positions()
        result = result[result["Symbol"] == asset].squeeze()
        return result

    def _pull_broker_positions(self, strategy=None):
        """Get the broker representation of all positions"""
        positions = []
        ib_positions = self.ib.get_positions()
        if ib_positions:
            for position in ib_positions:
                if position["position"] != 0:
                    positions.append(position)
        else:
            logging.debug("No positions found at interactive brokers.")

        return positions

    def _parse_broker_positions(self, broker_positions, strategy):
        """parse a list of broker positions into a
        list of position objects"""
        result = []
        for broker_position in broker_positions:
            result.append(self._parse_broker_position(broker_position, strategy))

        return result

    def _pull_positions(self, strategy):
        """Get the account positions. return a list of
        position objects"""
        response = self._pull_broker_positions(strategy)
        result = self._parse_broker_positions(response, strategy.name)
        return result

    def _pull_position(self, strategy, asset):
        """
        Pull a single position from the broker that matches the asset and strategy. If no position is found, None is
        returned.

        Parameters
        ----------
        strategy: Strategy
            The strategy object that placed the order to pull
        asset: Asset
            The asset to pull the position for

        Returns
        -------
        Position
            The position object for the asset and strategy if found, otherwise None
        """
        response = self._pull_broker_position(asset)
        result = self._parse_broker_position(response, strategy)
        return result

    # =======Orders and assets functions=========

    def _parse_broker_order(self, response, strategy_name, strategy_object=None):
        """Parse a broker order representation
        to an order object"""

        asset_type = [k for k, v in TYPE_MAP.items() if v == response.contract.secType][0]
        totalQuantity = response.totalQuantity
        limit_price = response.lmtPrice
        stop_price = response.auxPrice
        time_in_force = response.tif
        good_till_date = response.goodTillDate

        if asset_type == "multileg":
            # Create a multileg order.
            order = OrderLum(strategy_name)
            order.order_class = OrderLum.OrderClass.MULTILEG
            order.child_orders = []

            # details = self.ib.get_contract_details_for_contract(response.contract)

            # Parse the legs of the combo order.
            for leg in response.contract.comboLegs:
                # Create the contract object with just the conId
                contract = Contract()
                contract.conId = leg.conId

                # Get the contract details for the leg.
                res = self.ib.get_contract_details_for_contract(contract)
                contract = res[0].contract

                action = leg.action
                child_order = self._parse_order_object(strategy_name, contract, leg.ratio * totalQuantity, action, limit_price, stop_price, time_in_force, good_till_date)
                child_order.parent_identifier = order.identifier
                order.add_child_order(child_order)

        else:
            action = response.action
            order = self._parse_order_object(strategy_name, response.contract, totalQuantity, action, limit_price, stop_price, time_in_force, good_till_date)
        
        order._transmitted = True
        order.set_identifier(response.orderId)
        order.status = response.orderState.status
        order.update_raw(response)
        return order
    
    def _parse_order_object(self, strategy_name, contract, quantity, action, limit_price = None, stop_price = None, time_in_force = None, good_till_date = None):
        expiration = None
        multiplier = 1
        if contract.secType in ["OPT", "FUT"]:
            expiration = datetime.datetime.strptime(
                contract.lastTradeDateOrContractMonth,
                DATE_MAP[[d for d, v in TYPE_MAP.items() if v == contract.secType][0]],
            )
            multiplier = contract.multiplier

        right = None
        strike = None
        if contract.secType == "OPT":
            right = "CALL" if contract.right == "C" else "PUT"
            strike = contract.strike

        order = OrderLum(
            strategy_name,
            Asset(
                symbol=contract.localSymbol,
                asset_type=[k for k, v in TYPE_MAP.items() if v == contract.secType][0],
                expiration=expiration,
                strike=strike,
                right=right,
                multiplier=multiplier,
            ),
            quantity = Decimal(quantity),
            side = action.lower(),
            limit_price = limit_price if limit_price != 0 else None,
            stop_price = stop_price if stop_price != 0 else None,
            time_in_force = time_in_force,
            good_till_date = good_till_date,
            quote = Asset(symbol=contract.currency, asset_type="forex"),
        )   

        return order

    def _pull_broker_order(self, order_id):
        """Get a broker order representation by its id"""
        pull_order = [order for order in self.ib.get_open_orders() if order.orderId == order_id]
        response = pull_order[0] if len(pull_order) > 0 else None
        return response

    def _pull_broker_all_orders(self):
        """Get the broker open orders"""
        orders = self.ib.get_open_orders()
        return orders

    def _flatten_order(self, orders): # implement for stop loss. 
        """Not used for Interactive Brokers. Just returns the orders."""
        return orders
    
    def _submit_orders(self, orders, is_multileg=False, duration="day", price=None, **kwargs):
        if is_multileg:
            multileg_order = OrderLum(orders[0].strategy)
            multileg_order.order_class = OrderLum.OrderClass.MULTILEG
            multileg_order.child_orders = orders

            #If price is not None, then set the limit price for for the parent order and set the type to limit.
            if price is not None:
                multileg_order.limit_price = price
                multileg_order.order_type = OrderLum.OrderType.LIMIT
            else:
                multileg_order.order_type = OrderLum.OrderType.MARKET

            # Submit the multileg order.
            self._orders_queue.put(multileg_order)
            return multileg_order
        else:
            self._orders_queue.put(orders)
            return orders

    def _submit_order(self, order):
        """Submit an order for an asset"""
        # Initial order
        order.identifier = self.ib.nextOrderId()
        kwargs = {
            "type": order.order_type,
            "order_class": order.order_class,
            "time_in_force": order.time_in_force,
            "good_till_date": order.good_till_date,
            "limit_price": order.limit_price,
            "stop_price": order.stop_price,
            "trail_price": order.trail_price,
            "trail_percent": order.trail_percent,
        }
        # Remove items with None values
        kwargs = {k: v for k, v in kwargs.items() if v}

        if order.child_orders:
            for child_order in order.child_orders:
                if child_order.order_type == OrderLum.OrderType.LIMIT:
                    kwargs["take_profit"] = {"limit_price": child_order.limit_price}
                elif child_order.order_type in [OrderLum.OrderType.STOP, OrderLum.OrderType.STOP_LIMIT]:
                    kwargs["stop_loss"] = {"stop_price": child_order.stop_price}
                    if child_order.order_type == OrderLum.OrderType.STOP_LIMIT:
                        kwargs["stop_loss"]["limit_price"] = child_order.stop_limit_price

        if self.subaccount is not None:
            order.account = self.subaccount # to be tested

        self._unprocessed_orders.append(order)
        self.ib.execute_order(order)
        order.status = "submitted"
        return order

    def cancel_order(self, order):
        """Cancel an order"""
        self.ib.cancel_order(order)

    def _modify_order(self, order: OrderLum, limit_price: Union[float, None] = None,
                      stop_price: Union[float, None] = None):
        """
        Modify an order at the broker. Nothing will be done for orders that are already cancelled or filled. You are
        only allowed to change the limit price and/or stop price. If you want to change the quantity,
        you must cancel the order and submit a new one.
        """
        raise NotImplementedError("InteractiveBroker modify order is not implemented.")

    # =========Market functions=======================
    def _close_connection(self):
        self.ib.disconnect()

    def _reconnect_if_not_connected(self):
        # Check if ib is connected
        is_connected = self.ib.isConnected()
        if not is_connected:
            # Delete the ib object and create a new one
            del self.ib
            self.ib = None
            del self.data_source.ib 
            self.data_source.ib = None
            self.start_ib()

            return True

        return False

    def _get_balances_at_broker(self, quote_asset, strategy):
        """Gets the current actual cash, positions value, and total
        liquidation value from interactive Brokers.

        This method will get the current actual values from Interactive
        Brokers for the actual cash, positions value, and total liquidation.

        Returns
        -------
        tuple of float
            (cash, positions_value, total_liquidation_value)
        """

        try:
            # First make sure that we are connected to the broker.
            needed_reconnect = self._reconnect_if_not_connected()

            # If we needed a reconnect, then sleep for a bit to make sure that the connection is established.
            if needed_reconnect:
                # Log that we needed to reconnect to the broker and sleep to make sure the connection is established.
                sleeplen = 5
                logging.warning(
                    f"Had to reconnect to the broker. Sleeping for {sleeplen} seconds to make sure the connection is established."
                )
                # Sleep to make sure the connection is established.
                time.sleep(sleeplen)

            # Get the account summary from the broker.
            summary = self.ib.get_account_summary()

        except Exception as e:
            logger.error(
                "Could not get broker balances. Please check your broker "
                "configuration and make sure that TWS is running with the "
                "correct configuration. For more information, please "
                "see the documentation here: https://lumibot.lumiwealth.com/brokers.interactive_brokers.html"
                f"Error: {e}"
            )

            return None

        if summary is None:
            return None
        
        total_cash_value = [float(c["Value"]) for c in summary if c["Tag"] == "TotalCashBalance" and c["Currency"] == 'BASE'][0]
        gross_position_value = [float(c["Value"]) for c in summary if c["Tag"] == "NetLiquidationByCurrency" and c["Currency"] == 'BASE'][0]
        net_liquidation_value = [float(c["Value"]) for c in summary if c["Tag"] == "NetLiquidationByCurrency" and c["Currency"] == 'BASE'][0]

        return (total_cash_value, gross_position_value, net_liquidation_value)

    def get_contract_details(self, asset):
        # Used for Interactive Brokers. Convert an asset into a IB Contract.
        return self.ib.get_contract_details(asset=asset)

    def option_params(self, asset, exchange="", underlyingConId=""):
        # Returns option chain data, list of strikes and list of expiry dates.
        return self.ib.option_params(asset=asset, exchange=exchange, underlyingConId=underlyingConId)

    def get_chains(self, asset: Asset):
        """
        Returns option chain. IBKR chain data is weird because it returns a list of expirations and a separate list of
        strikes, but no way of coorelating the two. This method returns a dictionary with the expirations and strikes
        listed separately as well as attempting to combine them together under:
            [Chains][right][expiration_date] = [strike1, strike2, ...]

        """
        contract_details = self.get_contract_details(asset=asset)
        contract_id = contract_details[0].contract.conId
        chains = self.option_params(asset, underlyingConId=contract_id)
        if len(chains) == 0:
            raise AssertionError(f"No option chain for {asset}")

        for exchange in chains:
            all_expr = sorted(set(chains[exchange]["Expirations"]))
            # IB format is "20230818", Lumibot/Polygon/Tradier is "2023-08-18"
            formatted_expr = [x if "-" in x else x[:4] + "-" + x[4:6] + "-" + x[6:] for x in all_expr]
            all_strikes = sorted(set(chains[exchange]["Strikes"]))
            chains[exchange]["Chains"] = {"CALL": {}, "PUT": {}}
            for expiration in formatted_expr:
                chains[exchange]["Chains"]["CALL"][expiration] = all_strikes.copy()
                chains[exchange]["Chains"]["PUT"][expiration] = all_strikes.copy()

        if "SMART" in chains:
            return chains["SMART"]
        else:
            # Return the 1st exchange if SMART is not available.
            return chains[list(chains.keys())[0]]

    # =======Stream functions=========
    def on_status_event(
        self,
        orderId,
        status,
        filled,
        remaining,
        avgFullPrice,
        permId,
        parentId,
        lastFillPrice,
        clientId,
        whyHeld,
        mktCapPrice,
    ):
        """
        Information received from IBWrapper.orderStatus(). This can sometimes fire
        duplicates so a list must be kept for checking.

        The following are possible for status:
        - PendingSubmit
        - PendingCancel
        - PreSubmitted
        - Submitted
        - ApiCancelled
        - Cancelled
        - Filled
        - Inactive

        Filled is problematic. - Filled indicates that the order has been completely
        filled. Market orders executions  will not always trigger a Filled status.
        Therefore this must also be checked using ExecDetails
        """
        if status in [
            "PendingSubmit",
            "PendingCancel",
            "PreSubmitted",
            "Filled",
        ]:
            return

        order_status = [
            orderId,
            status,
            filled,
            remaining,
            avgFullPrice,
            permId,
            parentId,
            lastFillPrice,
            clientId,
            whyHeld,
            mktCapPrice,
        ]
        if order_status in self.order_status_duplicates:
            logging.debug(f"Duplicate order status event ignored. Order id {orderId} " f"and status {status} ")
            return
        else:
            self.order_status_duplicates.append(order_status)

        stored_order = self.get_tracked_order(orderId)
        if stored_order is None:
            logging.info(f"Untracked order {orderId} was logged by broker {self.name}")
            return

        # Check the order status submit changes.
        if status == "Submitted":
            type_event = self.NEW_ORDER
        elif status in ["ApiCancelled", "Cancelled", "Inactive"]:
            type_event = self.CANCELED_ORDER
        else:
            logging.error(
                f"A status event with an order of unknown order type of {status}. Should only be: "
                "`Submitted`, `ApiCancelled`, `Cancelled`, `Inactive`"
            )
            return

        self._process_trade_event(
            stored_order,
            type_event,
            price=None,
            filled_quantity=None,
        )

    def on_trade_event(self, reqId, contract, execution):
        orderId = execution.orderId
        stored_order = self.get_tracked_order(orderId)

        if stored_order is None:
            logging.info("Untracked order %s was logged by broker %s" % (orderId, self.name))
            return False
            # Check the order status submit changes.
        if execution.cumQty < stored_order.quantity:
            type_event = self.PARTIALLY_FILLED_ORDER
        elif execution.cumQty == stored_order.quantity:
            type_event = self.FILLED_ORDER
        else:
            raise ValueError(f"An order type should not have made it this far. " f"{execution}")

        price = execution.price
        filled_quantity = execution.shares
        multiplier = stored_order.asset.multiplier if stored_order.asset.multiplier else 1

        self._process_trade_event(
            stored_order,
            type_event,
            price=price,
            filled_quantity=filled_quantity,
            multiplier=multiplier,
        )

        return True

    def get_historical_account_value(self):
        pass

    def _get_stream_object(self):
        pass

    def _register_stream_events(self):
        pass

    def _run_stream(self):
        pass


# ===================INTERACTIVE BROKERS CLASSES===================

class IBWrapper(EWrapper):
    """Listens and collects data from IB."""

    # Error handling code.
    def init_error(self):
        error_queue = queue.Queue()
        self.my_errors_queue = error_queue

    def is_error(self):
        error_exist = not self.my_errors_queue.empty()
        return error_exist

    def get_error(self, timeout=6):
        if self.is_error():
            try:
                return self.my_errors_queue.get(timeout=timeout)
            except queue.Empty:
                return None
        return None

    def error(self, id, error_code, error_string):
        if not hasattr(self, "my_errors_queue"):
            self.init_error()

        error_message = "IBWrapper returned an error with %d error code %d that says %s" % (
            id,
            error_code,
            error_string,
        )

        # Color the error message red.
        colored_error_message = colored(error_message, "red")

        # Make sure we don't lose the error, but we only print it if asked for
        logging.debug(colored_error_message)

        self.my_errors_queue.put(error_message)

    # Time.
    def init_time(self):
        time_queue = queue.Queue()
        self.my_time_queue = time_queue
        return time_queue

    def currentTime(self, server_time):
        if not hasattr(self, "my_time_queue"):
            self.init_time()
        self.my_time_queue.put(server_time)

    # Single tick
    def init_tick(self):
        self.tick = None
        self.tick_type_used = None
        self.tick_request_id = None
        self.tick_asset = None
        self.price = None
        self.bid = None
        self.ask = None
        self.bid_size = None
        self.ask_size = None
        tick_queue = queue.Queue()
        self.my_tick_queue = tick_queue
        return tick_queue

    def tickPrice(self, reqId, tickType, price, attrib):
        if not hasattr(self, "tick"):
            self.init_tick()
            self.tick_request_id = reqId

        if tickType == 1:  # Bid price
            self.bid = price

        elif tickType == 2:  # Ask price
            self.ask = price

        # tickType == 4 is last price, tickType == 9 is last close (from previous day)
        # See details here: https://interactivebrokers.github.io/tws-api/tick_types.html
        if tickType == 4:
            self.price = price
            self.tick_type_used = tickType

        # If the last price is not available, then use yesterday's closing price
        # This can happen if the market is closed
        if tickType == 9 and self.tick is None and self.should_use_last_close:
            self.price = price
            self.tick_type_used = tickType

    def tickSize(self, reqId, tickType, size):
        if tickType == 0:  # Bid size
            self.bid_size = size

        elif tickType == 3:  # Ask size
            self.ask_size = size

    def tickSnapshotEnd(self, reqId):
        super().tickSnapshotEnd(reqId)
        if hasattr(self, "my_tick_queue"):
            self.my_tick_queue.put({
                "price": self.price,
                "bid": self.bid,
                "ask": self.ask,
                "bid_size": self.bid_size,
                "ask_size": self.ask_size,
            })
            if self.tick_type_used == 9:
                logging.warning(
                    f"Last price for {self.tick_asset} not found. Using yesterday's closing price of {self.price} instead. reqId = {reqId}"
                )
        if hasattr(self, "my_greek_queue"):
            self.my_greek_queue.put(self.greek)

    # Greeks
    def init_greek(self):
        self.greek = list()
        greek_queue = queue.Queue()
        self.my_greek_queue = greek_queue
        return greek_queue

    def tickOptionComputation(
        self,
        reqId: TickerId,
        tickType: TickType,
        tickAttrib: int,
        impliedVol: float,
        delta: float,
        optPrice: float,
        pvDividend: float,
        gamma: float,
        vega: float,
        theta: float,
        undPrice: float,
    ):
        super().tickOptionComputation(
            reqId,
            tickType,
            tickAttrib,
            impliedVol,
            delta,
            optPrice,
            pvDividend,
            gamma,
            vega,
            theta,
            undPrice,
        )

        if not hasattr(self, "greek"):
            self.init_greek()
        if tickType == 13:
            self.greek.append(
                [
                    impliedVol,
                    delta,
                    optPrice,
                    pvDividend,
                    gamma,
                    vega,
                    theta,
                    undPrice,
                ]
            )

    # Historical Data.
    def init_historical(self):
        self.historical = list()
        historical_queue = queue.Queue()
        self.my_historical_queue = historical_queue
        return historical_queue

    def historicalData(self, reqId, bar):
        if not hasattr(self, "historical"):
            self.init_historical()
        self.historical.append(vars(bar))

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        self.my_historical_queue.put(self.historical)

    # Realtime Bars (5 sec)
    def realtimeBar(
        self,
        reqId: TickerId,
        time: int,
        open_: float,
        high: float,
        low: float,
        close: float,
        volume: int,
        wap: float,
        count: int,
    ):
        super().realtimeBar(reqId, time, open_, high, low, close, volume, wap, count)
        if not hasattr(self, "realtimeBar"):
            self.init_realtimeBar()
        rtb = dict(
            datetime=datetime.datetime.fromtimestamp(time).astimezone(tz=tz.tzlocal()),
            open=open_,
            high=high,
            low=low,
            close=close,
            volume=volume,
            vwap=wap,
            count=count,
        )

        self.realtime_bars[self.map_reqid_asset[reqId]].append(rtb)

    # Positions.
    def init_positions(self):
        self.positions = list()
        positions_queue = queue.Queue()
        self.my_positions_queue = positions_queue
        return positions_queue

    def position(self, account, contract, pos, avgCost):
        if not hasattr(self, "positions"):
            self.init_positions()

        positionsdict = {
            "account": account,
            "symbol": contract.symbol,
            "asset_type": contract.secType,
            "expiration": contract.lastTradeDateOrContractMonth,
            "strike": contract.strike,
            "right": contract.right,
            "multiplier": contract.multiplier,
            "currency": contract.currency,
            "position": pos,
            "cost": avgCost,
            "type": contract.secType,
        }
        for k, v in TYPE_MAP.items():
            if positionsdict["asset_type"] == v:
                positionsdict["asset_type"] = k

        if positionsdict["asset_type"] in DATE_MAP:
            positionsdict["expiration"] = datetime.datetime.strptime(positionsdict["expiration"], "%Y%m%d").date()

        if positionsdict["right"] == "C":
            positionsdict["right"] = "CALL"
        elif positionsdict["right"] == "P":
            positionsdict["right"] = "PUT"

        self.positions.append(positionsdict)

        positionstxt = ", ".join(f"{k}: {v}" for k, v in positionsdict.items())

        logging.debug(positionstxt)

    def positionEnd(self):
        self.my_positions_queue.put(self.positions)

    # Account summary
    def init_accounts(self):
        self.accounts = list()
        accounts_queue = queue.Queue()
        self.my_accounts_queue = accounts_queue
        return accounts_queue

    def accountSummary(self, reqId: int, account: str, tag: str, value: str, currency: str):
        if not hasattr(self, "accounts"):
            self.init_accounts()

        accountSummarydict = {
            "ReqId": reqId,
            "Account": account,
            "Tag": tag,
            "Value": value,
            "Currency": currency,
        }

        self.accounts.append(accountSummarydict)

        accountSummarytxt = ", ".join([f"{k}: {v}" for k, v in accountSummarydict.items()])

        # Keep the logs, but only show if asked for
        logging.debug(accountSummarytxt)

    def accountSummaryEnd(self, reqId):
        super().accountSummaryEnd(reqId)
        self.my_accounts_queue.put(self.accounts)

    # Order IDs
    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)

        logging.debug("setting nextValidOrderId: %d", orderId)
        self.nextValidOrderId = orderId

    def nextOrderId(self):
        while not hasattr(self, "nextValidOrderId"):
            print("Waiting for next order id")
            time.sleep(0.1)
        oid = self.nextValidOrderId
        self.nextValidOrderId += 1
        return oid

    def init_orders(self):
        self.orders = list()
        orders_queue = queue.Queue()
        self.my_orders_queue = orders_queue
        return orders_queue

    def init_new_orders(self):
        new_orders_queue = queue.Queue()
        self.my_new_orders_queue = new_orders_queue
        return new_orders_queue

    def openOrder(self, orderId: OrderId, contract: Contract, order: Order, orderState: OrderState):
        if not hasattr(self, "orders"):
            self.init_orders()
        openOrdertxt = (
            f"openOrder - "
            f"PermId:  {order.permId}, "
            f"ClientId: {order.clientId}, "
            f"OrderId: {orderId}, "
            f"Account: {order.account},"
            f"Symbol: {contract.symbol}, "
            f"SecType: {contract.secType}, "
            f"Exchange: {contract.exchange}, "
            f"Action: {order.action}, "
            f"OrderType: {order.orderType}, "
            f"TotalQty: {order.totalQuantity},"
            f"CashQty: {order.cashQty}, "
            f"LmtPrice: {order.lmtPrice}, "
            f"AuxPrice: {order.auxPrice}, "
            f"Status: {orderState.status}) "
        )

        logging.debug(openOrdertxt)

        order.contract = contract
        order.orderState = orderState
        self.orders.append(order)

        # Capture new orders.
        if not hasattr(self, "my_new_orders_queue"):
            self.init_new_orders()
        if orderState.status == "PreSubmitted":
            self.my_new_orders_queue.put(order)

    def openOrderEnd(self):
        super().openOrderEnd()
        self.my_orders_queue.put(self.orders)

    def orderStatus(
        self,
        orderId,
        status,
        filled,
        remaining,
        avgFullPrice,
        permId,
        parentId,
        lastFillPrice,
        clientId,
        whyHeld,
        mktCapPrice,
    ):
        orderStatustxt = (
            f"orderStatus - "
            f"orderid: {orderId}, "
            f"status: {status}, "
            f"filled: {filled}, "
            f"remaining: {remaining}, "
            f"lastFillPrice: {lastFillPrice}, "
        )
        logging.debug(orderStatustxt)
        self.ib_broker.on_status_event(
            orderId,
            status,
            filled,
            remaining,
            avgFullPrice,
            permId,
            parentId,
            lastFillPrice,
            clientId,
            whyHeld,
            mktCapPrice,
        )

    def execDetails(self, reqId, contract, execution):
        execDetailstxt = (
            f"execDetails - "
            f"{reqId}, "
            f"{contract.symbol}, "
            f"{contract.secType}, "
            f"{contract.currency}, "
            f"{execution.execId}, "
            f"{execution.orderId}, "
            f"{execution.shares}, "
            f"{execution.lastLiquidity} "
        )
        logging.debug(execDetailstxt)

        return self.ib_broker.on_trade_event(reqId, contract, execution)

    def init_contract_details(self):
        self.contract_details = list()
        contract_details_queue = queue.Queue()
        self.my_contract_details_queue = contract_details_queue
        return contract_details_queue

    def contractDetails(self, reqId, contractDetails):
        if not hasattr(self, "my_contract_details_queue"):
            self.init_contract_details()
        self.contract_details.append(contractDetails)

    def contractDetailsEnd(self, reqId):
        super().contractDetailsEnd(reqId)
        self.my_contract_details_queue.put(self.contract_details)

    def init_option_params(self):
        self.option_params_dict = dict()
        option_params_queue = queue.Queue()
        self.my_option_params_queue = option_params_queue
        return option_params_queue

    def securityDefinitionOptionParameter(
        self,
        reqId: int,
        exchange: str,
        underlyingConId: int,
        tradingClass: str,
        multiplier: str,
        expirations: SetOfString,
        strikes: SetOfFloat,
    ):
        super().securityDefinitionOptionParameter(
            reqId,
            exchange,
            underlyingConId,
            tradingClass,
            multiplier,
            expirations,
            strikes,
        )
        if not hasattr(self, "my_option_params_queue"):
            self.init_option_params()
        self.option_params_dict[exchange] = {
            "Underlying conId": underlyingConId,
            "TradingClass": tradingClass,
            "Multiplier": multiplier,
            "Expirations": expirations,
            "Strikes": strikes,
        }

    def securityDefinitionOptionParameterEnd(self, reqId):
        super().securityDefinitionOptionParameterEnd(reqId)
        self.my_option_params_queue.put(self.option_params_dict)


class IBClient(EClient):
    """Sends data to IB"""

    def __init__(self, wrapper):
        ## Set up with a wrapper inside
        EClient.__init__(self, wrapper)
        self.max_wait_time = 13
        self.reqId = 10000

    def get_reqid(self):
        self.reqId += 1
        return self.reqId

    def get_timestamp(self):
        print("Asking server for Unix time")

        # Creates a queue to store the time
        time_storage = self.wrapper.init_time()

        # Sets up a request for unix time from the Eclient
        self.reqCurrentTime()

        try:
            requested_time = time_storage.get(timeout=self.max_wait_time)
        except queue.Empty:
            logging.info("The Interactive Brokers queue was empty or max time reached for timestamp.")
            requested_time = None

        while self.wrapper.is_error():
            logging.error("Interactive Brokers Error:", self.get_error(timeout=5))

        return requested_time

    def get_tick(self, asset="", greek=False, exchange="SMART", should_use_last_close=True, only_price=True):
        """
        Get the current price and other information for a given asset.

        Parameters
        ----------
        asset: Asset
            The asset to get the current price for.
        greek: bool
            If True, then get the greeks for the option.
        exchange: str
            The exchange to get the data from.
        should_use_last_close: bool
            If True, then use the last close price if the current price is not available.
        only_price: bool
            If True, then only return the price, otherwise return the full tick data.

        Returns
        -------
        dict or float
            The current price and other information for the asset.
        """
        self.should_use_last_close = should_use_last_close

        if not greek:
            tick_storage = self.wrapper.init_tick()
            self.tick_asset = asset
        elif greek:
            greek_storage = self.wrapper.init_greek()

        contract = self.create_contract(
            asset,
            currency="USD",
            exchange=exchange,
        )
        reqId = self.get_reqid()
        self.tick_request_id = reqId

        if not greek:
            self.reqMktData(reqId, contract, "", True, False, [])
        else:
            self.reqMktData(reqId, contract, "13", True, False, [])

        try:
            if not greek:
                requested_tick = tick_storage.get(timeout=self.max_wait_time)
            elif greek:
                requested_greek = greek_storage.get(timeout=self.max_wait_time)
        except queue.Empty:
            data_type = f"{'tick' if not greek else 'greek'}"
            logging.error(
                f"Unable to get data for {self.tick_asset}. The Interactive Brokers queue was empty or max time "
                f"reached for {data_type} data. reqId: {reqId}"
            )
            requested_tick = None
            requested_greek = None

        while self.wrapper.is_error():
            logging.error(f"Error: {self.get_error(timeout=5)}")

        if greek:
            keys = [
                "implied_volatility",
                "delta",
                "option_price",
                "pv_dividend",
                "gamma",
                "vega",
                "theta",
                "underlying_price",
            ]
            greeks = dict(zip(keys, requested_greek[0]))
            return greeks
        elif only_price:
            return requested_tick["price"]
        else:
            return requested_tick       

    def get_historical_data(
        self,
        reqId=0,
        symbol=[],
        end_date_time="",
        parsed_duration="1 D",
        parsed_timestep="1 day",
        type="TRADES",
        useRTH=0,
        formatDate=2,
        keepUpToDate=False,
        chartOptions=[],
        exchange="SMART",
    ):
        historical_storage = self.wrapper.init_historical()

        contract = self.create_contract(symbol, exchange=exchange)
        # Call the historical data.
        self.reqHistoricalData(
            self.get_reqid(),
            contract,
            end_date_time,
            parsed_duration,
            parsed_timestep,  # barSizeSetting
            type,
            useRTH,
            formatDate,
            keepUpToDate,
            chartOptions,
        )

        try:
            requested_historical = historical_storage.get(timeout=self.max_wait_time)
        except queue.Empty:
            print("The queue was empty or max time reached for historical data.")
            requested_historical = None

        while self.wrapper.is_error():
            print(f"Error: {self.get_error(timeout=5)}")

        return requested_historical

    def start_realtime_bars(
        self,
        asset=None,
        bar_size=5,
        what_to_show="TRADES",
        useRTH=True,
        keep_bars=10,
    ):
        reqid = self.get_reqid()
        self.map_reqid_asset[reqid] = asset
        self.realtime_bars[asset] = deque(maxlen=keep_bars)

        contract = self.create_contract(asset)
        # Call the realtime bars data.
        self.reqRealTimeBars(
            reqid,
            contract,
            bar_size,
            what_to_show,
            useRTH,
            ["XYZ"],
        )

    def cancel_realtime_bars(self, asset):
        self.realtime_bars.pop(asset, None)
        reqid = [rid for rid, ast in self.map_reqid_asset.items() if ast == asset][0]
        self.cancelRealTimeBars(reqid)
        logging.info(f"No longer streaming data for {asset.symbol}.")

    def get_positions(self):
        positions_storage = self.wrapper.init_positions()

        # Call the positions data.
        if self.subaccount is not None:
            # reqid = self.get_reqid()
            # self.reqPositionsMulti(reqid, self.subaccount, "") # not working idk why
            self.reqPositions()
        else:
            self.reqPositions()

        try:
            requested_positions = positions_storage.get(timeout=self.max_wait_time)
        except queue.Empty:
            logging.error("The queue was empty or max time reached for positions")
            requested_positions = None

        while self.wrapper.is_error():
            logging.error(f"Error: {self.get_error(timeout=5)}")

        if requested_positions is not None and self.subaccount is not None:
            requested_positions = [pos for pos in requested_positions if pos.get('account') == self.subaccount]

        return requested_positions

    def get_historical_account_value(self):
        logging.error("The function get_historical_account_value is not implemented yet for Interactive Brokers.")
        return {"hourly": None, "daily": None}

    def get_account_summary(self):
        accounts_storage = self.wrapper.init_accounts()

        as_reqid = self.get_reqid()

        self.reqAccountSummary(as_reqid, "All", "$LEDGER") # You could probably just set a subaccount, couldn't get it to work
        try:
            requested_accounts = accounts_storage.get(timeout=self.max_wait_time)
        except queue.Empty:
            logging.info("The Interactive Brokers queue was empty or max time reached for account summary")
            requested_accounts = None

        self.cancelAccountSummary(as_reqid)

        while self.wrapper.is_error():
            logging.debug(f"Error: {self.get_error(timeout=5)}")

        if requested_accounts is not None and self.subaccount is not None:
            requested_accounts = [pos for pos in requested_accounts if pos.get('Account') == self.subaccount]

        return requested_accounts

    def get_open_orders(self):
        orders_storage = self.wrapper.init_orders()

        # Call the orders data.
        if self.subaccount is None:
            self.reqAllOpenOrders()
        else:
            self.reqOpenOrders() # to be tested, gets only orders opened by your specific client id

        try:
            requested_orders = orders_storage.get(timeout=self.max_wait_time)
        except queue.Empty:
            print("The queue was empty or max time reached for orders.")
            requested_orders = None
        
        while self.wrapper.is_error():
            print(f"Error: {self.get_error(timeout=5)}")

        if isinstance(requested_orders, Order):
            requested_orders = [requested_orders]
        
        return requested_orders

    def cancel_order(self, order):
        order_id = order.identifier
        if not order_id or not isinstance(order_id, int):
            logging.info(
                f"An attempt to cancel an order without supplying a proper "
                f"`order_id` was made. This was your `order_id`: {order_id}. "
                f"An integer is required. No action was taken."
            )
            return

        self.cancelOrder(order_id)

        while self.wrapper.is_error():
            print(f"Error: {self.get_error(timeout=5)}")

        return 0
    
    def get_contract_details_for_contract(self, contract):
        contract_details_storage = self.wrapper.init_contract_details()

        # Call the contract details.
        self.reqContractDetails(self.get_reqid(), contract)

        try:
            requested_contract_details = contract_details_storage.get(timeout=self.max_wait_time)
        except queue.Empty:
            print("The queue was empty or max time reached for contract details")
            requested_contract_details = None

        while self.wrapper.is_error():
            print(f"Error: {self.get_error(timeout=5)}")

        return requested_contract_details

    def get_contract_details(self, asset=None):
        contract_details_storage = self.wrapper.init_contract_details()

        # Call the contract details.
        contract = self.create_contract(asset)

        self.reqContractDetails(self.get_reqid(), contract)

        try:
            requested_contract_details = contract_details_storage.get(timeout=self.max_wait_time)
        except queue.Empty:
            print("The queue was empty or max time reached for contract details")
            requested_contract_details = None

        while self.wrapper.is_error():
            print(f"Error: {self.get_error(timeout=5)}")

        return requested_contract_details

    def option_params(self, asset=None, exchange="", underlyingConId=""):
        options_params_storage = self.wrapper.init_option_params()

        # Call the orders data.
        self.reqSecDefOptParams(
            self.get_reqid(),
            asset.symbol,
            exchange,
            TYPE_MAP[asset.asset_type],
            underlyingConId,
        )

        try:
            requested_option_params = options_params_storage.get(timeout=self.max_wait_time)
        except queue.Empty:
            print("The queue was empty or max time reached for option contract " "details.")
            requested_option_params = None

        while self.wrapper.is_error():
            print(f"Error: {self.get_error(timeout=5)}")

        return requested_option_params


class IBApp(IBWrapper, IBClient):

    def __init__(self, ip_address, socket_port, client_id, subaccount=None, ib_broker=None):
        IBWrapper.__init__(self)
        IBClient.__init__(self, wrapper=self)

        self.ip_address = ip_address
        self.socket_port = socket_port
        self.client_id = client_id
        self.ib_broker = ib_broker
        self.subaccount = subaccount

        self.reqAutoOpenOrders(True)

        # Ensure a connection before running
        self.connect(self.ip_address, self.socket_port, client_id)

        
        thread = Thread(target=self.run)
        thread.start()
        self._thread = thread

        self.init_error()
        self.map_reqid_asset = dict()
        self.realtime_bars = dict()

    def get_safe_action(self, action):
        """Convert complex action types to simple buy/sell actions"""
        if action.lower() in [
            OrderLum.OrderSide.BUY, 
            OrderLum.OrderSide.BUY_TO_OPEN, 
            OrderLum.OrderSide.BUY_TO_CLOSE
            ]:
            return OrderLum.OrderSide.BUY.upper()
        elif action.lower() in [
            OrderLum.OrderSide.SELL, 
            OrderLum.OrderSide.SELL_SHORT, 
            OrderLum.OrderSide.SELL_TO_OPEN, 
            OrderLum.OrderSide.SELL_TO_CLOSE
            ]:
            return OrderLum.OrderSide.SELL.upper()
        else:
            raise ValueError(f"Unknown order action: {action}")

    def create_contract(
        self,
        asset,
        exchange=None,
        currency="USD",
        primaryExchange="ISLAND",
    ):
        """Creates new contract objects."""
        contract = Contract()

        contract.symbol = str(asset.symbol).upper()
        contract.secType = TYPE_MAP[asset.asset_type]
        if exchange is None:
            contract.exchange = "SMART"
        else:
            contract.exchange = exchange
        contract.currency = currency

        if asset.asset_type == "stock":
            contract.primaryExchange = primaryExchange
        elif asset.asset_type == "option":
            contract.lastTradeDateOrContractMonth = asset.expiration.strftime("%Y%m%d")
            contract.strike = str(asset.strike)
            contract.right = asset.right
            contract.multiplier = asset.multiplier
            contract.primaryExchange = "CBOE"
        elif asset.asset_type == "future":
            if exchange is None:
                contract.exchange = "CME"
            contract.includeExpired = True
            contract.lastTradeDateOrContractMonth = asset.expiration.strftime("%Y%m%d")
        elif asset.asset_type == "forex":
            contract.exchange = "IDEALPRO"
        elif asset.asset_type == "index":
            pass
        else:
            valid_types = [a.value for a in Asset.AssetType]
            raise ValueError(
                f"The asset {asset.symbol} has a type of {asset.asset_type}. " f"It must be one of {valid_types}"
            )

        return contract

    def create_order(self, order):
        ib_order = Order()
        if order.order_class == "bracket":
            if not order.limit_price:
                logging.info(
                    f"All bracket orders must have limit price for the originating "
                    f"order. The bracket order for {order.symbol} is cancelled."
                )
                return []
            parent = Order()
            parent.orderId = order.identifier if order.identifier else self.nextOrderId()
            parent.action = self.get_safe_action(order.side)
            parent.orderType = "LMT"
            parent.totalQuantity = order.quantity
            parent.lmtPrice = order.limit_price
            parent.transmit = False

            takeProfit = Order()
            takeProfit.orderId = self.nextOrderId()
            takeProfit.action = "SELL" if self.get_safe_action(parent.action) == "BUY" else "BUY"
            takeProfit.orderType = "LMT"
            takeProfit.totalQuantity = order.quantity
            takeProfit.lmtPrice = order.limit_price
            takeProfit.parentId = parent.orderId
            takeProfit.transmit = False

            stopLoss = Order()
            stopLoss.orderId = self.nextOrderId()
            stopLoss.action = "SELL" if self.get_safe_action(parent.action) == "BUY" else "BUY"
            stopLoss.orderType = "STP"
            stopLoss.auxPrice = order.stop_price
            stopLoss.totalQuantity = order.quantity
            stopLoss.parentId = parent.orderId
            stopLoss.transmit = True

            bracketOrder = [parent, takeProfit, stopLoss]

            return bracketOrder

        elif order.order_class == "oto":
            if not order.limit_price:
                logging.info(
                    f"All OTO orders must have limit price for the originating order. "
                    f"The one triggers other order for {order.symbol} is cancelled."
                )
                return []

            parent = Order()
            parent.orderId = order.identifier if order.identifier else self.nextOrderId()
            parent.action = self.get_safe_action(order.side)
            parent.orderType = "LMT"
            parent.totalQuantity = order.quantity
            parent.lmtPrice = order.limit_price
            parent.transmit = False

            if order.limit_price:
                takeProfit = Order()
                takeProfit.orderId = self.nextOrderId()
                takeProfit.action = "SELL" if self.get_safe_action(parent.action) == "BUY" else "BUY"
                takeProfit.orderType = "LMT"
                takeProfit.totalQuantity = order.quantity
                takeProfit.lmtPrice = order.limit_price
                takeProfit.parentId = parent.orderId
                takeProfit.transmit = True
                return [parent, takeProfit]

            elif order.stop_price:
                stopLoss = Order()
                stopLoss.orderId = self.nextOrderId()
                stopLoss.action = "SELL" if self.get_safe_action(parent.action) == "BUY" else "BUY"
                stopLoss.orderType = "STP"
                stopLoss.auxPrice = order.stop_price
                stopLoss.totalQuantity = order.quantity
                stopLoss.parentId = parent.orderId
                stopLoss.transmit = True
                return [parent, stopLoss]

        elif order.order_class == "oco":
            takeProfit = Order()
            takeProfit.orderId = order.identifier if order.identifier else self.nextOrderId()
            takeProfit.action = self.get_safe_action(order.side)
            takeProfit.orderType = "LMT"
            takeProfit.totalQuantity = order.quantity
            takeProfit.lmtPrice = order.limit_price
            takeProfit.transmit = False

            oco_Group = f"oco_{takeProfit.orderId}"
            takeProfit.ocaGroup = oco_Group
            takeProfit.ocaType = 1

            stopLoss = Order()
            stopLoss.orderId = self.nextOrderId()
            stopLoss.action = self.get_safe_action(order.side)
            stopLoss.orderType = "STP"
            stopLoss.totalQuantity = order.quantity
            stopLoss.auxPrice = order.stop_price
            stopLoss.transmit = True

            stopLoss.ocaGroup = oco_Group
            stopLoss.ocaType = 1

            return [takeProfit, stopLoss]
        else:
            ib_order.action = self.get_safe_action(order.side)
            ib_order.orderType = ORDERTYPE_MAPPING[order.order_type]
            ib_order.totalQuantity = order.quantity
            ib_order.lmtPrice = order.limit_price if order.limit_price else 0
            ib_order.auxPrice = order.stop_price if order.stop_price else ""
            ib_order.trailingPercent = order.trail_percent if order.trail_percent else ""
            if order.trail_price:
                ib_order.auxPrice = order.trail_price
            ib_order.orderId = order.identifier if order.identifier else self.nextOrderId()
            ib_order.tif = order.time_in_force.upper()
            ib_order.goodTillDate = order.good_till_date.strftime("%Y%m%d %H:%M:%S") if order.good_till_date else ""
            return [ib_order]
        
    def _create_multileg_order(self, order, exchange=None, **kwargs):
        """Submit a list of orders as a single multileg order"""
        # Initialize the combo contract
        combo_contract = Contract()
        # Construct the symbol with commas if the symbols are different
        if len(set([child_order.asset.symbol for child_order in order.child_orders])) > 1:
            combo_contract.symbol = ",".join([child_order.asset.symbol for child_order in order.child_orders])
        else:
            combo_contract.symbol = order.child_orders[0].asset.symbol
        combo_contract.secType = "BAG"
        combo_contract.exchange = exchange if exchange else "SMART"
        combo_contract.currency = order.child_orders[0].quote.symbol  # Assuming all child orders have the same currency

        # Create a new order ID for the combo order
        combo_order_id = self.nextOrderId()

        # Initialize the combo legs
        combo_contract.comboLegs = []

        # Prepare the legs for the combo order
        for child_order in order.child_orders:
            # Initialize the combo leg
            leg = ComboLeg()

            # Get the conid from the contract details
            contract_details = self.get_contract_details(child_order.asset)
            leg.conId = contract_details[0].contract.conId

            # Set the leg details
            leg.ratio = child_order.quantity
            leg.action = self.get_safe_action(child_order.side)
            leg.exchange = exchange if exchange else "SMART"

            # Append the leg to the combo contract
            combo_contract.comboLegs.append(leg)

        # Reeduce the leg ratios to the smallest integer for each leg
        ratios = [leg.ratio for leg in combo_contract.comboLegs]
        gcd = reduce(math.gcd, ratios)
        for leg in combo_contract.comboLegs:
            leg.ratio = leg.ratio // gcd

        # Initialize the combo order
        combo_order = Order()
        combo_order.action = "BUY" # TODO: This is a placeholder. This should be set based on the order side
        combo_order.orderId = combo_order_id
        combo_order.orderType = ORDERTYPE_MAPPING[order.order_type]
        combo_order.tif = order.time_in_force.upper()
        combo_order.goodTillDate = order.good_till_date if order.good_till_date else ""
        combo_order.totalQuantity = min([child_order.quantity for child_order in order.child_orders])

        # Set the limit price if this is a limit order and a price is provided
        if order.order_type == OrderLum.OrderType.LIMIT and order.limit_price:
            combo_order.lmtPrice = order.limit_price

        # Return the combo contract and order        
        return combo_contract, combo_order

    def execute_order(self, orders):
        # Create a queue to store the new order.
        self.wrapper.init_new_orders()

        if not isinstance(orders, list):
            orders = [orders]

        ib_orders = []
        for order in orders:
            # Check if the order is a multileg order
            if order.order_class == OrderLum.OrderClass.MULTILEG:
                contract_object, order_object = self._create_multileg_order(order)

                order_objects = [order_object]

                # nextID = order_object.orderId if order_object.orderId else self.nextOrderId()
                # ib_orders.append((nextID, contract_object, order_object))
                # continue
            else:
                # Places the order with the returned contract and order objects
                contract_object = self.create_contract(
                    order.asset,
                    exchange=order.exchange,
                    currency=order.quote.symbol,
                )
                order_objects = self.create_order(order)

            if len(order_objects) == 0:
                continue

            # There was an update by IB mid april/2021 that ceased support for
            # `eTradeOnly` and `firmQuoteOnly` attributes. However the defaults
            # where left `True` causing errors. Set to False
            for order_object in order_objects:
                order_object.eTradeOnly = False
                order_object.firmQuoteOnly = False

                nextID = order_object.orderId if order_object.orderId else self.nextOrderId()
                ib_orders.append((nextID, contract_object, order_object))

        for ib_order in ib_orders:
            if len(ib_order) == 0:
                continue

            self.placeOrder(*ib_order)
