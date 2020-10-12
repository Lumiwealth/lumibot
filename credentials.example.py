class AlpacaConfig:
    API_KEY = "YOUR ALPACA API KEY"
    API_SECRET = "YOUR ALPACA API SECRET"

    """ If no endpoint is specified, the following paper trading
    endpoint will be used by default"""
    ENDPOINT = "https://paper-api.alpaca.markets"

    VERSION = 'v2' #By default v2
    USE_POLYGON = False #By dfault set to False

class AlphaVantageConfig:
    API_KEY = "YOUR ALPHA VANTAGE API KEY"