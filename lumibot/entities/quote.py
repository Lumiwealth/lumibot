from __future__ import annotations

import datetime as dt
from typing import Any

from lumibot.entities.asset import Asset


class Quote:
    """
    Quote entity class.

    Attributes:
    -----------
    asset : Asset
        The asset for which the quote is being provided.
    price : float
        The price of the asset.
    bid : float
        The bid price for the asset.
    ask : float
        The ask price for the asset.
    mid_price : float
        The mid price (automatically calculated from bid/ask if not provided).
    volume : float
        The volume of the asset.
    timestamp : datetime.datetime
        The timestamp of the quote.
    bid_size : float
        The size of the bid.
    ask_size : float
        The size of the ask.
    change : float
        The change in price from previous close.
    percent_change : float
        The percent change in price from previous close.
    quote_time : datetime.datetime
        The time of the quote.
    bid_time : datetime.datetime
        The time of the bid.
    ask_time : datetime.datetime
        The time of the ask.
    raw_data : dict
        The raw data from the data source.
    """

    asset: Asset
    price: float | None
    bid: float | None
    ask: float | None
    volume: float | None
    timestamp: dt.datetime
    bid_size: float | None
    ask_size: float | None
    change: float | None
    percent_change: float | None
    quote_time: dt.datetime | None
    bid_time: dt.datetime | None
    ask_time: dt.datetime | None
    raw_data: dict[str, Any] | None
    _mid_price: float | None

    def __init__(
        self,
        asset: Asset,
        price: float | None = None,
        bid: float | None = None,
        ask: float | None = None,
        volume: float | None = None,
        timestamp: dt.datetime | None = None,
        bid_size: float | None = None,
        ask_size: float | None = None,
        change: float | None = None,
        percent_change: float | None = None,
        quote_time: dt.datetime | None = None,
        bid_time: dt.datetime | None = None,
        ask_time: dt.datetime | None = None,
        raw_data: dict[str, Any] | None = None,
        mid_price: float | None = None,
        **kwargs: Any,
    ) -> None:
        self.asset = asset
        self.price = price
        self.bid = bid
        self.ask = ask
        self.volume = volume
        self.timestamp = timestamp or dt.datetime.now(dt.UTC)
        self.bid_size = bid_size
        self.ask_size = ask_size
        self.change = change
        self.percent_change = percent_change
        self.quote_time = quote_time
        self.bid_time = bid_time
        self.ask_time = ask_time
        self.raw_data = raw_data

        if mid_price is not None:
            self._mid_price = mid_price
        elif self.bid is not None and self.ask is not None:
            self._mid_price = (self.bid + self.ask) / 2
        else:
            self._mid_price = None

        for key, value in kwargs.items():
            setattr(self, key, value)

    @property
    def mid_price(self) -> float | None:
        """Calculate the mid price between bid and ask."""
        if self._mid_price is not None:
            return self._mid_price
        if self.bid is not None and self.ask is not None:
            return (self.bid + self.ask) / 2
        return self.price

    def __getitem__(self, key: str) -> Any:
        """
        Allow dictionary-style access to Quote attributes for backward compatibility.
        Tries to get the attribute first, then falls back to raw_data if available.
        """
        if hasattr(self, key):
            return getattr(self, key)
        if self.raw_data and key in self.raw_data:
            return self.raw_data[key]
        raise KeyError(f"'{key}' not found in Quote object or raw_data")

    def __str__(self) -> str:
        return (
            f"Quote(asset={self.asset}, price={self.price}, bid={self.bid}, ask={self.ask}, "
            f"volume={self.volume}, timestamp={self.timestamp})"
        )

    def __repr__(self) -> str:
        return str(self)
