import datetime
import logging
from decimal import ROUND_DOWN, Decimal, getcontext

from lumibot.data_sources import CcxtData
from lumibot.entities import Asset, Order, Position
from termcolor import colored

from .ccxt import Ccxt


class CcxtPerp(Ccxt):
    pass
