import datetime

from lumibot.entities import Asset


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
    def __init__(self, asset: Asset, price: float = None, bid: float = None, ask: float = None,
                 volume: float = None, timestamp: datetime.datetime = None, bid_size: float = None,
                 ask_size: float = None, change: float = None, percent_change: float = None,
                 quote_time: datetime.datetime = None, bid_time: datetime.datetime = None,
                 ask_time: datetime.datetime = None, raw_data: dict = None, mid_price: float = None, **kwargs):
        self.asset = asset
        self.price = price
        self.bid = bid
        self.ask = ask
        self.volume = volume
        self.timestamp = timestamp or datetime.datetime.now(datetime.timezone.utc)
        self.bid_size = bid_size
        self.ask_size = ask_size
        self.change = change
        self.percent_change = percent_change
        self.quote_time = quote_time
        self.bid_time = bid_time
        self.ask_time = ask_time
        self.raw_data = raw_data

        # Calculate mid_price if not provided
        if mid_price is not None:
            self._mid_price = mid_price
        elif self.bid is not None and self.ask is not None:
            self._mid_price = (self.bid + self.ask) / 2
        else:
            self._mid_price = None

        # Store any additional attributes passed in kwargs
        for key, value in kwargs.items():
            setattr(self, key, value)

    @property
    def mid_price(self):
        """Calculate the mid price between bid and ask"""
        # Return cached value if available
        if hasattr(self, '_mid_price') and self._mid_price is not None:
            return self._mid_price
        # Calculate on the fly if bid/ask are available
        if self.bid is not None and self.ask is not None:
            return (self.bid + self.ask) / 2
        return self.price

    def __getitem__(self, key):
        """
        Allow dictionary-style access to Quote attributes for backward compatibility.
        Tries to get the attribute first, then falls back to raw_data if available.
        """
        # Try to get as an attribute first
        if hasattr(self, key):
            return getattr(self, key)
        # Fall back to raw_data if it exists
        elif self.raw_data and key in self.raw_data:
            return self.raw_data[key]
        else:
            raise KeyError(f"'{key}' not found in Quote object or raw_data")

    def __str__(self):
        return (f"Quote(asset={self.asset}, price={self.price}, bid={self.bid}, ask={self.ask}, "
                f"volume={self.volume}, timestamp={self.timestamp})")

    def __repr__(self):
        return str(self)
