from MomentumBot import MomentumBot

if __name__ == '__main__':
    API_KEY = "PKH3TWXNFF5EX3EDT6QT"
    API_SECRET = "igIiQg2QV9okPwjEM2pF92c0gjZUjqxV9meYDouv"
    ENDPOINT = "https://paper-api.alpaca.markets"

    myBot = MomentumBot(API_KEY, API_SECRET, logfile='test.log')
    myBot.run()