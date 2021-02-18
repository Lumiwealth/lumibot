import logging
import os
import signal
from concurrent.futures import ThreadPoolExecutor, as_completed


class Trader:
    def __init__(self, logfile="logs/test.log", debug=False, strategies=None):
        # Setting debug and _logfile parameters and setting global log format
        self._logfile = logfile
        self.debug = debug
        self.log_format = logging.Formatter("%(asctime)s: %(levelname)s: %(message)s")

        # Setting the list of strategies if defined
        self._strategies = strategies if strategies else []

    @property
    def is_backtest(self):
        result = False
        if any([s.broker.IS_BACKTESTING_BROKER for s in self._strategies]):
            result = True
        return result

    def _set_logger(self):
        """Setting Logging to both console and a file if logfile is specified"""
        logging.getLogger("urllib3").setLevel(logging.ERROR)
        logging.getLogger("requests").setLevel(logging.ERROR)

        logger = logging.getLogger()
        if not logger.handlers:
            logger.addHandler(logging.StreamHandler())
        if self.debug:
            logger.setLevel(logging.DEBUG)
        elif self.is_backtest:
            logger.setLevel(logging.ERROR)
        else:
            logger.setLevel(logging.INFO)

        # Setting file logging
        if self._logfile:
            dir = os.path.dirname(os.path.abspath(self._logfile))
            if not os.path.exists(dir):
                os.mkdir(dir)
            fileHandler = logging.FileHandler(self._logfile, mode="w")
            logger.addHandler(fileHandler)

        for handler in logger.handlers:
            handler.setFormatter(self.log_format)

        logger.propagate = True

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
        os._exit(0)

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
        if self.is_backtest:
            logging.info("Backtesting starting...")

        self._set_logger()
        signal.signal(signal.SIGINT, self._abrupt_closing)
        with ThreadPoolExecutor(thread_name_prefix="strategy") as executor:
            tasks = []
            for strategy in self._strategies:
                tasks.append(executor.submit(strategy.run))

            results = []
            for task in as_completed(tasks):
                results.append(task.result())

        return all(results)
