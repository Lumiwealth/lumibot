from lumibot.data_sources import AlpacaData

from .broker import Broker


class Tradovate(AlpacaData, Broker):
    
    def __init__(self, config, max_workers=20, chunk_size=100, connect_stream=True):
        pass
    
    def is_market_open(self):
        pass