import logging  # Needed for logging infrastructure setup
import os
import signal
import threading
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from lumibot.tools.lumibot_logger import get_logger, set_console_log_level

# Overloading time.sleep to warn users against using it

logger = get_logger(__name__)

@dataclass
class _BacktestProfilingConfig:
    enabled: bool
    tool: str
    format: str
    clock: str
    output_path: Path


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

        # Setting debug and _logfile parameters
        self.debug = debug
        self.backtest = backtest
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
            plot_file_html=None,
            trades_file=None,
            trade_events_file=None,
            settings_file=None,
            indicators_file=None,
            tearsheet_csv_file=None,
            tearsheet_file=None,
            tearsheet_metrics_file=None,
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

        plot_file_html: str
            The path to save the trades plot HTML. This is only used for backtesting.

        trades_file: str
            The path to save the simplified trades CSV/parquet artifact. This is only used for backtesting.

        trade_events_file: str
            The path to save the full trade-events CSV/parquet artifact. This is only used for backtesting.

        settings_file: str
            The path to save backtest settings JSON. This is only used for backtesting.

        indicators_file: str
            The path to save indicators HTML. This is only used for backtesting.

        tearsheet_csv_file: str
            The path to save tearsheet CSV. This is only used for backtesting.

        tearsheet_file: str
            The path to save the tearsheet. This is only used for backtesting.

        tearsheet_metrics_file: str
            The path to save machine-readable tearsheet summary metrics JSON. This is only used for backtesting.

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
        # NOTE: Market auto-detection now happens inside Broker.__init__.
        # This previous redundancy has been removed to ensure a single
        # source of truth for market inference (futures / crypto / 24-7).
        if self.is_backtest_broker:
            strat.verify_backtest_inputs(strat.backtesting_start, strat.backtesting_end)
            logger.info("Backtesting starting...")

        profiling = self._get_backtest_profiling_config(strat=strat, base_filename=base_filename)

        # When running tests in parallel, this signal line causes tests to fail
        # if they don't run in the main thread. Since real strategies only run in the main thread
        # its safe to check for that before calling.
        try:
            if threading.current_thread() is threading.main_thread():
                signal.signal(signal.SIGINT, self._stop_pool)
        except ValueError:
            # Not in main thread: skip custom SIGINT handler
            pass

        self._set_logger()
        self._init_pool()

        _yappi = None
        if profiling and profiling.enabled and profiling.tool == "yappi":
            try:
                import yappi as _yappi  # type: ignore

                _yappi.set_clock_type(profiling.clock)
                _yappi.clear_stats()
                _yappi.start()
                logger.info(
                    "Backtest profiling enabled: tool=%s clock=%s artifact=%s",
                    profiling.tool,
                    profiling.clock,
                    profiling.output_path.name,
                )
            except Exception as exc:
                _yappi = None
                logger.warning("Failed to enable yappi profiling: %s", exc)

        try:
            self._start_pool()
            if not async_:
                self._join_pool()
            result = self._collect_analysis()

            if self.is_backtest_broker:
                # Don't override the logger level - respect the quiet logs setting
                logger.info("Backtesting finished")

                if strat._analyze_backtest:
                    strat.backtest_analysis(
                        logdir=self.logdir,
                        show_plot=show_plot,
                        show_tearsheet=show_tearsheet,
                        save_tearsheet=save_tearsheet,
                        show_indicators=show_indicators,
                        plot_file_html=plot_file_html,
                        trades_file=trades_file,
                        trade_events_file=trade_events_file,
                        settings_file=settings_file,
                        indicators_file=indicators_file,
                        tearsheet_csv_file=tearsheet_csv_file,
                        tearsheet_file=tearsheet_file,
                        tearsheet_metrics_file=tearsheet_metrics_file,
                        base_filename=base_filename,
                    )

                # Emit a single cache summary line so production runs can quantify S3 hydration
                # cost without guessing or relying solely on profiler artifacts.
                try:
                    from lumibot.tools.backtest_cache import get_backtest_cache

                    get_backtest_cache().log_summary()
                except Exception:
                    pass

            return result
        finally:
            if _yappi is not None and profiling is not None and profiling.enabled:
                try:
                    _yappi.stop()
                    stats = _yappi.get_func_stats()
                    stats.sort("ttot", "desc")

                    profiling.output_path.parent.mkdir(parents=True, exist_ok=True)
                    import csv

                    # Write a text CSV artifact so existing backtest artifact download paths
                    # (Bot Manager → BotSpot "View Files") can serve it without binary handling.
                    with open(profiling.output_path, "w", newline="") as handle:
                        writer = csv.writer(handle)
                        writer.writerow(
                            [
                                "full_name",
                                "module",
                                "lineno",
                                "name",
                                "ncall",
                                "nactualcall",
                                "ttot_s",
                                "tsub_s",
                                "tavg_s",
                                "ctx_name",
                            ]
                        )
                        for entry in stats:
                            writer.writerow(
                                [
                                    getattr(entry, "full_name", ""),
                                    getattr(entry, "module", ""),
                                    getattr(entry, "lineno", ""),
                                    getattr(entry, "name", ""),
                                    getattr(entry, "ncall", ""),
                                    getattr(entry, "nactualcall", ""),
                                    getattr(entry, "ttot", ""),
                                    getattr(entry, "tsub", ""),
                                    getattr(entry, "tavg", ""),
                                    getattr(entry, "ctx_name", ""),
                                ]
                            )
                    logger.info("Wrote backtest profile artifact: %s", profiling.output_path)
                except Exception as exc:
                    logger.warning("Failed to write yappi profile artifact: %s", exc)
                finally:
                    try:
                        _yappi.clear_stats()
                    except Exception:
                        pass

    def _get_backtest_profiling_config(
        self,
        *,
        strat,
        base_filename: Optional[str],
    ) -> Optional[_BacktestProfilingConfig]:
        if not self.is_backtest_broker:
            return None

        profile_mode = os.environ.get("BACKTESTING_PROFILE", "").strip().lower()
        if profile_mode != "yappi":
            return None

        strategy_name = getattr(strat, "_name", None) or getattr(strat, "name", None) or "strategy"
        resolved_base = base_filename or strategy_name
        output_path = (self.logdir / f"{resolved_base}_profile_yappi.csv").resolve()

        # Make settings.json aware of the profiling artifact (best-effort; should never crash).
        try:
            setattr(strat, "_backtest_profiling_enabled", True)
            setattr(strat, "_backtest_profiling_tool", "yappi")
            setattr(strat, "_backtest_profiling_format", "csv")
            setattr(strat, "_backtest_profiling_clock", "wall")
            setattr(strat, "_backtest_profiling_artifact", output_path.name)
        except Exception:
            pass

        return _BacktestProfilingConfig(
            enabled=True,
            tool="yappi",
            format="csv",
            clock="wall",
            output_path=output_path,
        )

    # Async version of run_all
    def run_all_async(self):
        """run all strategies"""
        self.run_all(async_=True)
        return self._strategies

    def stop_all(self):
        logger.info("Stopping all strategies for this trader")
        self._stop_pool()

    def _set_logger(self):
        """Setting Logging to both console and a file if logfile is specified"""
        # Import here to avoid circular imports
        from lumibot.tools.lumibot_logger import add_file_handler, set_log_level
        
        # Set external library log levels to reduce noise
        # NOTE: lumilogger.get_logger doesn't work with non-lumibot loggers, so we use logging.getLogger directly
        logging.getLogger("urllib3").setLevel(logging.ERROR)
        logging.getLogger("requests").setLevel(logging.ERROR)
        logging.getLogger("apscheduler.scheduler").setLevel(logging.ERROR)
        logging.getLogger("apscheduler.executors.default").setLevel(logging.ERROR)
        logging.getLogger("lumibot.data_sources.yahoo_data").setLevel(logging.ERROR)

        # Configure global log level based on trader settings
        if self.debug:
            set_log_level("DEBUG")
        elif self.is_backtest_broker:
            # Quiet logs turns off all backtesting logging except for error messages
            if self.quiet_logs:
                set_log_level("ERROR")
            else:
                set_log_level("INFO")
                # When quiet_logs=False, show INFO logs on console too
        else:
            # Live trades should always have full logging for both console and file
            set_log_level("INFO")

        # PERFORMANCE: avoid spamming identical FutureWarnings thousands of times during backtests
        # (e.g., pandas chained-assignment warnings inside strategy indicator code).
        if self.is_backtest_broker:
            warnings.simplefilter("once", FutureWarning)
            warnings.filterwarnings(
                "ignore",
                category=FutureWarning,
                message=r"A value is trying to be set on a copy of a DataFrame or Series through chained assignment using an inplace method.*",
            )

        # Setting file logging if specified
        if self.logfile:
            add_file_handler(str(self.logfile), level="DEBUG" if self.debug else "INFO",
                             is_backtest=self.is_backtest_broker)

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
            
        # For backtesting, check if any strategy failed and raise exception
        if self.is_backtest_broker:
            for strategy_thread in self._pool:
                # Check if the thread stored an exception
                if hasattr(strategy_thread, 'exception') and strategy_thread.exception is not None:
                    raise strategy_thread.exception

    def _stop_pool(self, sig=None, frame=None):
        """Run all strategies on_abrupt_closing
        lifecycle method. python signal handlers
        needs two positional arguments, the signal
        and the frame"""

        logger.debug(f"Received signal number {sig}.")
        logger.debug(f"Closing Trader in {frame} frame.")
        for strategy_thread in self._pool:
            if not strategy_thread.abrupt_closing:
                strategy_thread.stop()
                logger.info(f"Trading finished for {strategy_thread.strategy._name}")

    def _collect_analysis(self):
        result = {}
        for strategy_thread in self._pool:
            result[strategy_thread.name] = strategy_thread.result
        return result
