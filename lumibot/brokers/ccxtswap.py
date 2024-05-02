import datetime
import logging
from decimal import ROUND_DOWN, Decimal, getcontext

from lumibot.data_sources import CcxtData
from lumibot.entities import Asset, Order, Position
from termcolor import colored

from .ccxt import Ccxt


class CcxtSwap(Ccxt):
    def __init__(self, config, data_source: CcxtData = None, max_workers=20, chunk_size=100, **kwargs):
        super().__init__(self,
                         config=config,
                         data_source=data_source,
                         max_workers=max_workers,
                         chunk_size=chunk_size,
                         **kwargs)

    # =========Clock functions=====================
    # (inherited from Ccxt)

    # =========Positions functions==================
    def _parse_broker_position(self, position, strategy, orders=None):
        # TODO: Implement this parent method, adapted for Perpetual Futures (swap).
        pass

    def _pull_broker_position(self, asset):
        # TODO: Implement this parent method, adapted for Perpetual Futures (swap).
        pass

    def _pull_broker_positions(self, strategy=None):
        # TODO: Implement this parent method, adapted for Perpetual Futures (swap).
        pass

    # =======Orders and assets functions=========
    def _parse_broker_order(self, response, strategy_name, strategy_object=None):
        # TODO: Implement this parent method, adapted for Perpetual Futures (swap).
        pass

    def _pull_broker_all_orders(self):
        # TODO: Implement this parent method, adapted for Perpetual Futures (swap).
        # Maybe the only addition is a new "if" for exchange_id == "binancecoinm"
        pass

    def _flatten_order(self, order):
        # TODO: Implement this parent method, adapted for Perpetual Futures (swap).
        # Maybe the only addition is a new "if" for exchange_id == "binancecoinm"
        pass

    def _submit_order(self, order):
        # TODO: Implement this parent method, adapted for Perpetual Futures (swap).
        # One of the most important methods probably.
        pass

    def create_order_args(self, order):
        # TODO: Implement this parent method, adapted for Perpetual Futures (swap).
        # One of the most important methods probably too.
        pass

    def cancel_order(self, order):
        # TODO: Implement this parent method, adapted for Perpetual Futures (swap).
        # One of the most important methods probably as well.
        pass
