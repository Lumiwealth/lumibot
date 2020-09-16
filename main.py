from MomentumBot import MomentumBot

if __name__ == '__main__':
    API_KEY = "PKU9FX5X7WFMY0VSIWEN"
    API_SECRET = "0qjYhx77sedw49YFCAICjqA4i6UE6gu1oRW21gHZ"
    ENDPOINT = "https://paper-api.alpaca.markets"

    myBot = MomentumBot(API_KEY, API_SECRET, logfile='test.log')
    myBot.run()