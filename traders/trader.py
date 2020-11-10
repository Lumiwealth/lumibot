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
        self._logfile = logfile
        logger = logging.getLogger()
        if debug:
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)

        logFormater = logging.Formatter("%(asctime)s: %(levelname)s: %(message)s")

        # Setting file logging
        if logfile:
            dir = os.path.dirname(os.path.abspath(logfile))
            if not os.path.exists(dir):
                os.mkdir(dir)

            fileHandler = logging.FileHandler(logfile, mode="w")
            fileHandler.setFormatter(logFormater)
            logger.addHandler(fileHandler)

        # Setting console logger
        consoleHandler = logging.StreamHandler()
        consoleHandler.setFormatter(logFormater)
        logger.addHandler(consoleHandler)

        # Setting the list of strategies if defined
        self._strategies = strategies if strategies else []

        # Initializing the list of threads
        self._threads = []

    def _join_threads(self):
        """Joining all the threads"""
        for t in self._threads:
            t.join()
        return

    def _abrupt_closing(self, sig, frame):
        """Run all strategies on_abrupt_closing
        lifecycle method. python signal handlers
        needs two positional arguments, the signal
        and the frame"""

        logging.debug("Received signal number %d." % sig)
        logging.debug("Executing Trader.abrupt_closing in %s frame." % frame)

        for strategy in self._strategies:
            logging.info(
                strategy.format_log_message(
                    "Executing the on_abrupt_closing lifecycle method"
                )
            )
            strategy.on_abrupt_closing()
            strategy.set_ready_to_close()

        self.wait_for_strategies_to_close()
        logging.info("Trading finished")
        sys.exit(0)

    def add_strategy(self, strategy):
        """Adds a strategy to the trader"""
        self._strategies.append(strategy)

    def wait_for_strategies_to_close(self):
        """Wait for all strategies to finish executing.
        keeping the instance open until daemon threads
        finish executing"""
        while True:
            if all([s.get_ready_to_close() for s in self._strategies]):
                break

    def run_all(self):
        """run all strategies"""
        signal.signal(signal.SIGINT, self._abrupt_closing)
        for strategy in self._strategies:
            t = Thread(target=strategy.run, daemon=True)
            t.start()
            self._threads.append(t)

        self._join_threads()
