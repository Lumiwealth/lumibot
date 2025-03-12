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
                 ask_time: datetime.datetime = None, raw_data: dict = None, **kwargs):
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
        
        # Store any additional attributes passed in kwargs
        for key, value in kwargs.items():
            setattr(self, key, value)

    @property
    def mid_price(self):
        """Calculate the mid price between bid and ask"""
        if self.bid is not None and self.ask is not None:
            return (self.bid + self.ask) / 2
        return self.price

    def __str__(self):
        return (f"Quote(asset={self.asset}, price={self.price}, bid={self.bid}, ask={self.ask}, "
                f"volume={self.volume}, timestamp={self.timestamp})")

    def __repr__(self):
        return str(self)