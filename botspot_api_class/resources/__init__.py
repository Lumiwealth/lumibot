"""
BotSpot API Client - Resource Classes

Each resource class provides methods for interacting with a specific API endpoint.
"""

from .backtests import BacktestsResource
from .strategies import StrategiesResource
from .users import UsersResource

__all__ = ["UsersResource", "StrategiesResource", "BacktestsResource"]
