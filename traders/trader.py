from threading import Thread

import logging

class Trader:
    def __init__(
        self, logfile=None, debug=False, strategies=None
    ):
        #Setting Logging to both console and a file if logfile is specified
        logging.getLogger("urllib3").setLevel(logging.ERROR)
        logging.getLogger("requests").setLevel(logging.ERROR)
        self.logfile = logfile
        logger = logging.getLogger()
        if debug:
            logger.setLevel(logging.INFO)
        else:
            logger.setLevel(logging.DEBUG)

        logFormater = logging.Formatter("%(asctime)s: %(levelname)s: %(message)s")
        consoleHandler = logging.StreamHandler()
        consoleHandler.setFormatter(logFormater)
        logger.addHandler(consoleHandler)
        if logfile:
            fileHandler = logging.FileHandler(logfile, mode='w')
            fileHandler.setFormatter(logFormater)
            logger.addHandler(fileHandler)

        #Setting the list of strategies if defined
        self.strategies = strategies if strategies else []

    def add_strategy(self, strategy):
        """Adds a strategy to the trader"""
        self.strategies.append(strategy)

    def run_all(self):
        """run all strategies"""
        threads = []
        for strategy in self.strategies:
            t = Thread(target=strategy.run)
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        return