import logging
import os
import signal
import sys
from pathlib import Path

# Overloading time.sleep to warn users against using it

logger = logging.getLogger(__name__)


class Trader:
    def __init__(self, logfile="", backtest=False, debug=False, strategies=None, quiet_logs=False):
        """

        Parameters
        ----------
        logfile: str
            The path to the logfile. If not specified, the logfile will be saved in the user's log directory.
        backtest: bool
            Whether to run the strategies in backtest mode or not. This is used as a safety check to make sure you
            don't mix backtesting and live strategies.
        debug: bool
            Whether to run the strategies in debug mode or not. This will set the log level to DEBUG.
        strategies: list
            A list of strategies to run. If not specified, you must add strategies using trader.add_strategy(strategy)
        quiet_logs: bool
            Whether to quiet backtest logs by setting the log level to ERROR. Defaults to False.
        """
        # Check if the logfile is a valid path
        if logfile:
            if not isinstance(logfile, str):
                raise ValueError("logfile must be a string")

        # Setting debug and _logfile parameters and setting global log format
        self.debug = debug
        self.backtest = backtest
        std_format = "%(asctime)s: %(levelname)s: %(message)s"
        debug_format = "%(asctime)s: %(name)s: %(levelname)s: %(message)s"
        log_format = std_format if not self.debug else debug_format
        self.log_format = logging.Formatter(log_format)
        self.quiet_logs = quiet_logs  # Turns off all logging execpt for error messages in backtesting

        if logfile:
            self.logfile = Path(logfile)
            self.logfile.parent.mkdir(parents=True, exist_ok=True)
            self.logdir = self.logfile.parent
        else:
            self.logfile = None
            # default_logdir = appdirs.user_log_dir(appauthor="Lumiwealth", appname="lumibot", version="1.0")
            self.logdir = Path("logs")

        # Setting the list of strategies if defined
        self._strategies = strategies if strategies else []
        self._pool = []

    @property
    def is_backtest_broker(self):
        result = False
        if any([s.broker.is_backtesting_broker() for s in self._strategies]):
            result = True
        return result

    def add_strategy(self, strategy):
        """Adds a strategy to the trader"""
        self._strategies.append(strategy)

    def run_all(
            self, 
            async_=False, 
            show_plot=True, 
            show_tearsheet=True, 
            save_tearsheet=True, 
            show_indicators=True, 
            tearsheet_file=None,
            base_filename=None,
            ):
        """
        run all strategies

        Parameters
        ----------
        async_: bool
            Whether to run the strategies asynchronously or not. This is not implemented yet.

        show_plot: bool
            Whether to disply the plot in the user's web browser. This is only used for backtesting.

        show_tearsheet: bool
            Whether to display the tearsheet in user's web browser. This is only used for backtesting.

        save_tearsheet: bool
            Whether to save the tearsheet or not. This is only used for backtesting.

        show_indicators: bool
            Whether to display the indicators (markers and lines) in the user's web browser. This is only used for backtesting.

        tearsheet_file: str
            The path to save the tearsheet. This is only used for backtesting.

        base_filename: str
            The base filename to save the tearsheet, plot, indicators, etc. This is only used for backtesting.

        Returns
        -------
        dict
            A dictionary with the keys being the strategy names and the values being the strategy analysis.
        """
        if not self._strategies:
            raise RuntimeError(
                "No strategies to run. You must call trader.add_strategy(strategy) " "before trader.run_all()."
            )

        if self.is_backtest_broker != self.backtest:
            raise RuntimeError(
                f"You cannot mix backtesting and live strategies. You passed in "
                f"Trader(backtest={self.backtest}) but the strategies are configured with "
                f"broker_backtesting={self.is_backtest_broker}."
            )

        if len(self._strategies) != 1:
            if self.is_backtest_broker:
                raise Exception(
                    f"Received {len(self._strategies)} strategies for backtesting."
                    f"You can only backtest one at a time."
                )
            else:
                raise NotImplementedError(
                    f"Running multiple live strategies is not implemented yet. You passed "
                    f"in {len(self._strategies)} strategies."
                )

        strat = self._strategies[0]
        if self.is_backtest_broker:
            strat.verify_backtest_inputs(strat.backtesting_start, strat.backtesting_end)
            logger.info("Backtesting starting...")

        signal.signal(signal.SIGINT, self._stop_pool)
        self._set_logger()
        self._init_pool()
        self._start_pool()
        if not async_:
            self._join_pool()
        result = self._collect_analysis()

        if self.is_backtest_broker:
            logger.setLevel(logging.INFO)
            logger.info("Backtesting finished")

            if strat._analyze_backtest:
                strat.backtest_analysis(
                    logdir=self.logdir,
                    show_plot=show_plot,
                    show_tearsheet=show_tearsheet,
                    save_tearsheet=save_tearsheet,
                    show_indicators=show_indicators,
                    tearsheet_file=tearsheet_file,
                    base_filename=base_filename,
                )

        return result

    # Async version of run_all
    def run_all_async(self):
        """run all strategies"""
        self.run_all(async_=True)
        return self._strategies

    def stop_all(self):
        logging.info("Stopping all strategies for this trader")
        self._stop_pool()

    def _set_logger(self):
        """Setting Logging to both console and a file if logfile is specified"""
        logging.getLogger("urllib3").setLevel(logging.ERROR)
        logging.getLogger("requests").setLevel(logging.ERROR)
        logging.getLogger("apscheduler.scheduler").setLevel(logging.ERROR)
        logging.getLogger("apscheduler.executors.default").setLevel(logging.ERROR)
        logging.getLogger("lumibot.data_sources.yahoo_data").setLevel(logging.ERROR)
        logger = logging.getLogger()

        for handler in logger.handlers:
            if handler.__class__.__name__ == "StreamHandler":
                logger.removeHandler(handler)

        stream_handler = logging.StreamHandler(stream=sys.stdout)
        stream_handler.setLevel(logging.INFO)
        logger.addHandler(stream_handler)

        if self.debug:
            logger.setLevel(logging.DEBUG)
        elif self.is_backtest_broker:
            logger.setLevel(logging.INFO)

            # Quiet logs turns off all backtesting logging except for error messages
            if self.quiet_logs:
                logger.setLevel(logging.ERROR)

                # Ensure console has minimal logging to keep things clean during backtesting
                stream_handler.setLevel(logging.ERROR)

        else:
            # Live trades should always have full logging.
            logger.setLevel(logging.INFO)

        # Setting file logging
        if self.logfile:
            dir = os.path.dirname(os.path.abspath(self.logfile))
            if not os.path.exists(dir):
                os.mkdir(dir)
            fileHandler = logging.FileHandler(self.logfile, mode="w", encoding="utf-8")
            logger.addHandler(fileHandler)

        for handler in logger.handlers:
            handler.setFormatter(self.log_format)

        logger.propagate = True

        # Disable Interactive Brokers logs
        for log_name, log_obj in logging.Logger.manager.loggerDict.items():
            if log_name.startswith("ibapi"):
                iblogger = logging.getLogger(log_name)
                iblogger.setLevel(logging.CRITICAL)
                iblogger.disabled = True

    def _init_pool(self):
        self._pool = [strategy._executor for strategy in self._strategies]

    def _start_pool(self):
        for strategy_thread in self._pool:
            strategy_thread.start()

    def _join_pool(self):
        for strategy_thread in self._pool:
            strategy_thread.join()

    def _stop_pool(self, sig=None, frame=None):
        """Run all strategies on_abrupt_closing
        lifecycle method. python signal handlers
        needs two positional arguments, the signal
        and the frame"""

        logging.debug(f"Received signal number {sig}.")
        logging.debug(f"Closing Trader in {frame} frame.")
        for strategy_thread in self._pool:
            if not strategy_thread.abrupt_closing:
                strategy_thread.stop()
                logging.info(f"Trading finished for {strategy_thread.strategy._name}")

    def _collect_analysis(self):
        result = {}
        for strategy_thread in self._pool:
            result[strategy_thread.name] = strategy_thread.result
        return result
