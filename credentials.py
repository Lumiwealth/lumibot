class InteractiveBrokersConfig:
    SOCKET_PORT = 7497
    CLIENT_ID = 888
    IP = "127.0.0.1"

class AlpacaConfig:
    API_KEY = "PKK3PDX6TCZQ1QPU7W7G"
    API_SECRET = "j5CRf8WoC7o5RJWRUqbS7t93O1Os4OApeKP7r0pB"

    """ If no endpoint is specified, the following paper trading
    endpoint will be used by default"""
    ENDPOINT = "https://paper-api.alpaca.markets"

    VERSION = "v2"  # By default v2
    USE_POLYGON = False  # By dfault set to False
