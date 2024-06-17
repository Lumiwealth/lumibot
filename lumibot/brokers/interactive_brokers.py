import datetime
import logging
import time
from collections import defaultdict, deque
from decimal import Decimal
from threading import Thread

from dateutil import tz
from ibapi.client import *
from ibapi.contract import *
from ibapi.order import *
from ibapi.wrapper import *
from lumibot.data_sources import InteractiveBrokersData

# Naming conflict on Order between IB and Lumibot.
from lumibot.entities import Asset
from lumibot.entities import Order as OrderLum
from lumibot.entities import Position

from .broker import Broker


class InteractiveBrokers(Broker):
    """Inherit InteractiveBrokerData first and all the price market
    methods than inherits broker"""

    def __init__(self, config, max_workers=20, chunk_size=100, data_source=None, max_connection_retries=0, **kwargs):
        if data_source is None:
            data_source = InteractiveBrokersData(config, max_workers=max_workers, chunk_size=chunk_size)

        super().__init__(self, config=config, data_source=data_source, max_workers=max_workers, **kwargs)
        if not self.name:
            self.name = "interactive_brokers"

        # For checking duplicate order status events from IB.
        self.order_status_duplicates = []
        self.market = "NYSE"  # The default market is NYSE.

        # Connection to interactive brokers
        self.ib = None

        # check if the config is a dict
        if isinstance(config, dict):
            ip = config["IP"]
            socket_port = config["SOCKET_PORT"]
            client_id = config["CLIENT_ID"]
        else:
            ip = config.IP
            socket_port = config.SOCKET_PORT
            client_id = config.CLIENT_ID

        self.start_ib(ip, socket_port, client_id, max_connection_retries)

    def start_ib(self, ip, socket_port, client_id, max_connection_retries):
        # Connect to interactive brokers.
        if not self.ib:
            self.ib = IBApp(ip, socket_port, client_id, ib_broker=self, max_connection_retries=max_connection_retries)

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

        expiration = None
        multiplier = 1
        if response.contract.secType in ["OPT", "FUT"]:
            expiration = datetime.datetime.strptime(
                response.contract.lastTradeDateOrContractMonth,
                DATE_MAP[[d for d, v in TYPE_MAP.items() if v == response.contract.secType][0]],
            )
            multiplier = response.contract.multiplier

        right = None
        strike = None
        if response.contract.secType == "OPT":
            right = "CALL" if response.contract.right == "C" else "PUT"
            strike = response.contract.strike

        order = OrderLum(
            strategy_name,
            Asset(
                symbol=response.contract.localSymbol,
                asset_type=[k for k, v in TYPE_MAP.items() if v == response.contract.secType][0],
                expiration=expiration,
                strike=strike,
                right=right,
                multiplier=multiplier,
            ),
            Decimal(response.totalQuantity),
            response.action.lower(),
            limit_price=response.lmtPrice if response.lmtPrice != 0 else None,
            stop_price=response.auxPrice if response.auxPrice != 0 else None,
            time_in_force=response.tif,
            good_till_date=response.goodTillDate,
            quote=Asset(symbol=response.contract.currency, asset_type="forex"),
        )
        order._transmitted = True
        order.set_identifier(response.orderId)
        order.status = response.orderState.status
        order.update_raw(response)
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

    def _flatten_order(self, orders):  # implement for stop loss.
        """Not used for Interactive Brokers. Just returns the orders."""
        return orders

    def _submit_order(self, order):
        """Submit an order for an asset"""
        # Initial order
        order.identifier = self.ib.nextOrderId()
        kwargs = {
            "type": order.type,
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

        if order.take_profit_price:
            kwargs["take_profit"] = {"limit_price": order.take_profit_price}

        if order.stop_loss_price:
            kwargs["stop_loss"] = {"stop_price": order.stop_loss_price}
            if order.stop_loss_limit_price:
                kwargs["stop_loss"]["limit_price"] = order.stop_loss_limit_price

        self._unprocessed_orders.append(order)
        self.ib.execute_order(order)
        order.status = "submitted"
        return order

    def cancel_order(self, order):
        """Cancel an order"""
        self.ib.cancel_order(order)

    # =========Market functions=======================
    def _close_connection(self):
        self.ib.disconnect()

    def _get_balances_at_broker(self, quote_asset):
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
            summary = self.ib.get_account_summary()
        except:
            logger.error(
                "Could not get broker balances. Please check your broker "
                "configuration and make sure that TWS is running with the "
                "correct configuration. For more information, please "
                "see the documentation here: https://lumibot.lumiwealth.com/brokers.interactive_brokers.html"
            )

            return None
        finally:
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
TYPE_MAP = dict(
    stock="STK",
    option="OPT",
    future="FUT",
    forex="CASH",
    index="IND",
)

DATE_MAP = dict(
    future="%Y%m%d",
    option="%Y%m%d",
)


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
        # Make sure we don't lose the error, but we only print it if asked for
        logging.debug(error_message)

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
        tick_queue = queue.Queue()
        self.my_tick_queue = tick_queue
        return tick_queue

    def tickPrice(self, reqId, tickType, price, attrib):
        if not hasattr(self, "tick"):
            self.init_tick()
            self.tick_request_id = reqId

        # tickType == 4 is last price, tickType == 9 is last close (from previous day)
        # See details here: https://interactivebrokers.github.io/tws-api/tick_types.html
        if tickType == 4:
            self.tick = price
            self.tick_type_used = tickType

        # If the last price is not available, then use yesterday's closing price
        # This can happen if the market is closed
        if tickType == 9 and self.tick is None and self.should_use_last_close:
            self.tick = price
            self.tick_type_used = tickType

    def tickSnapshotEnd(self, reqId):
        super().tickSnapshotEnd(reqId)
        if hasattr(self, "my_tick_queue"):
            self.my_tick_queue.put([self.tick])
            if self.tick_type_used == 9:
                logging.warning(
                    f"Last price for {self.tick_asset} not found. Using yesterday's closing price of {self.tick} instead. reqId = {reqId}"
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

    def get_tick(self, asset="", greek=False, exchange="SMART", should_use_last_close=True):
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

        if not greek:
            return requested_tick
        else:
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
        self.reqPositions()

        try:
            requested_positions = positions_storage.get(timeout=self.max_wait_time)
        except queue.Empty:
            logging.error("The queue was empty or max time reached for positions")
            requested_positions = None

        while self.wrapper.is_error():
            logging.error(f"Error: {self.get_error(timeout=5)}")

        return requested_positions

    def get_historical_account_value(self):
        logging.error("The function get_historical_account_value is not implemented yet for Interactive Brokers.")
        return {"hourly": None, "daily": None}

    def get_account_summary(self):
        accounts_storage = self.wrapper.init_accounts()
        
        as_reqid = self.get_reqid()
        self.reqAccountSummary(as_reqid, "All", "$LEDGER")

        try:
            requested_accounts = accounts_storage.get(timeout=self.max_wait_time)
        except queue.Empty:
            logging.info("The Interactive Brokers queue was empty or max time reached for account summary")
            requested_accounts = None

        self.cancelAccountSummary(as_reqid)

        while self.wrapper.is_error():
            logging.debug(f"Error: {self.get_error(timeout=5)}")

        return requested_accounts

    def get_open_orders(self):
        orders_storage = self.wrapper.init_orders()

        # Call the orders data.
        self.reqAllOpenOrders()

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
    ORDERTYPE_MAPPING = dict(
        market="MKT",
        limit="LMT",
        stop="STP",
        stop_limit="STP LMT",
        trailing_stop="TRAIL",
    )

    def __init__(self, ipaddress, portid, clientid, ib_broker=None, max_connection_retries=0):
        IBWrapper.__init__(self)
        IBClient.__init__(self, wrapper=self)
        self.ib_broker = ib_broker

        # Ensure a connection before running
        connected = False
        retries = 0

        while (not connected) and (retries<=max_connection_retries):
            self.connect(ipaddress, portid, clientid)
            connected = self.isConnected()
            if not connected:
                time.sleep(2)
            retries+=1

        thread = Thread(target=self.run)
        thread.start()
        self._thread = thread

        self.init_error()
        self.map_reqid_asset = dict()
        self.realtime_bars = dict()

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
            raise ValueError(
                f"The asset {asset.symbol} has a type of {asset.asset_type}. " f"It must be one of {asset._asset_types}"
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
            parent.action = order.side.upper()
            parent.orderType = "LMT"
            parent.totalQuantity = order.quantity
            parent.lmtPrice = order.limit_price
            parent.transmit = False

            takeProfit = Order()
            takeProfit.orderId = self.nextOrderId()
            takeProfit.action = "SELL" if parent.action == "BUY" else "BUY"
            takeProfit.orderType = "LMT"
            takeProfit.totalQuantity = order.quantity
            takeProfit.lmtPrice = order.take_profit_price
            takeProfit.parentId = parent.orderId
            takeProfit.transmit = False

            stopLoss = Order()
            stopLoss.orderId = self.nextOrderId()
            stopLoss.action = "SELL" if parent.action == "BUY" else "BUY"
            stopLoss.orderType = "STP"
            stopLoss.auxPrice = order.stop_loss_price
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
            parent.action = order.side.upper()
            parent.orderType = "LMT"
            parent.totalQuantity = order.quantity
            parent.lmtPrice = order.limit_price
            parent.transmit = False

            if order.take_profit_price:
                takeProfit = Order()
                takeProfit.orderId = self.nextOrderId()
                takeProfit.action = "SELL" if parent.action == "BUY" else "BUY"
                takeProfit.orderType = "LMT"
                takeProfit.totalQuantity = order.quantity
                takeProfit.lmtPrice = order.take_profit_price
                takeProfit.parentId = parent.orderId
                takeProfit.transmit = True
                return [parent, takeProfit]

            elif order.stop_loss_price:
                stopLoss = Order()
                stopLoss.orderId = self.nextOrderId()
                stopLoss.action = "SELL" if parent.action == "BUY" else "BUY"
                stopLoss.orderType = "STP"
                stopLoss.auxPrice = order.stop_loss_price
                stopLoss.totalQuantity = order.quantity
                stopLoss.parentId = parent.orderId
                stopLoss.transmit = True
                return [parent, stopLoss]

        elif order.order_class == "oco":
            takeProfit = Order()
            takeProfit.orderId = order.identifier if order.identifier else self.nextOrderId()
            takeProfit.action = order.side.upper()
            takeProfit.orderType = "LMT"
            takeProfit.totalQuantity = order.quantity
            takeProfit.lmtPrice = order.take_profit_price
            takeProfit.transmit = False

            oco_Group = f"oco_{takeProfit.orderId}"
            takeProfit.ocaGroup = oco_Group
            takeProfit.ocaType = 1

            stopLoss = Order()
            stopLoss.orderId = self.nextOrderId()
            stopLoss.action = order.side.upper()
            stopLoss.orderType = "STP"
            stopLoss.totalQuantity = order.quantity
            stopLoss.auxPrice = order.stop_loss_price
            stopLoss.transmit = True

            stopLoss.ocaGroup = oco_Group
            stopLoss.ocaType = 1

            return [takeProfit, stopLoss]
        else:
            ib_order.action = order.side.upper()
            ib_order.orderType = self.ORDERTYPE_MAPPING[order.type]
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

    def execute_order(self, orders):
        # Create a queue to store the new order.
        new_order_storage = self.wrapper.init_new_orders()

        if not isinstance(orders, list):
            orders = [orders]

        ib_orders = []
        for order in orders:
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
