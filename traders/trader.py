import logging
import os
import signal
import sys
from threading import Thread


class Trader:
    def __init__(self, logfile=None, debug=False, strategies=None):
        # Setting Logging to both console and a file if logfile is specified
        logging.getLogger("urllib3").setLevel(logging.ERROR)
        logging.getLogger("requests").setLevel(logging.ERROR)
        self.logfile = logfile
        logger = logging.getLogger()
        if debug:
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)

        logFormater = logging.Formatter("%(asctime)s: %(levelname)s: %(message)s")
        consoleHandler = logging.StreamHandler()
        consoleHandler.setFormatter(logFormater)
        logger.addHandler(consoleHandler)
        if logfile:
            dir = os.path.dirname(os.path.abspath(logfile))
            if not os.path.exists(dir):
                os.mkdir(dir)

            fileHandler = logging.FileHandler(logfile, mode="w")
            fileHandler.setFormatter(logFormater)
            logger.addHandler(fileHandler)

        # Setting the list of strategies if defined
        self.strategies = strategies if strategies else []

        # Initializing the list of threads
        self.threads = []

    def add_strategy(self, strategy):
        """Adds a strategy to the trader"""
        self.strategies.append(strategy)

    def join_threads(self):
        """Joining all the threads"""
        for t in self.threads:
            if t.is_alive():
                t.join()

        return

    def abrupt_closing(self, sig, frame):
        """Run all strategies on_abrupt_closing
        lifecycle method. python signal handlers
        needs two positional arguments, the signal
        and the frame"""

        logging.debug("Received signal number %d." % sig)
        logging.debug("Executing Trader.abrupt_closing in %s frame." % frame)

        for strategy in self.strategies:
            logging.info(
                strategy.format_log_message(
                    "Executing the on_abrupt_closing lifecycle method"
                )
            )
            strategy.on_abrupt_closing()

        # self.join_threads()
        logging.info("Trading finished")
        sys.exit(0)

    def run_all(self):
        """run all strategies"""
        self.threads = []
        signal.signal(signal.SIGINT, self.abrupt_closing)
        for strategy in self.strategies:
            t = Thread(target=strategy.run, daemon=True)
            t.start()
            self.threads.append(t)

        self.join_threads()
