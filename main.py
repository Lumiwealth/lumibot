from MomentumBot import MomentumBot

if __name__ == '__main__':
    API_KEY = "PK32PYE8WKLY4M87H2BM"
    API_SECRET = "mCA670lJ5xx1Wbb9VTz7GBnRCRuM3LjMfPEpIhGo"
    ENDPOINT = "https://paper-api.alpaca.markets"

    myBot = MomentumBot(API_KEY, API_SECRET, logfile='test.log', debug=True)
    myBot.run()