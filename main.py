from MomentumBot import MomentumBot

if __name__ == '__main__':
    API_KEY = "PK29V27BVCTOM85QDEXO"
    API_SECRET = "Ea/CAy1i3r6UW3S4ChFfqA4OTAtDZnGLNrf8aSMH"
    ENDPOINT = "https://paper-api.alpaca.markets"

    myBot = MomentumBot(API_KEY, API_SECRET, logfile='test.log')
    myBot.run()