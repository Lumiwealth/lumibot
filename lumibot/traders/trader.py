# pyright: reportPrivateUsage=false

from __future__ import annotations

import csv
import logging  # Needed for logging infrastructure setup
import os
import signal
import threading
import warnings
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from types import FrameType
from typing import Any, Protocol, TypeAlias, cast

from lumibot.tools.lumibot_logger import get_logger

# Overloading time.sleep to warn users against using it

logger = get_logger(__name__)

ArtifactPath: TypeAlias = str | None  # noqa: UP040 - keep Python 3.11 parser compatibility.
AnalysisResult: TypeAlias = dict[str, Any]  # noqa: UP040 - keep Python 3.11 parser compatibility.
StrategyResults: TypeAlias = dict[str, AnalysisResult]  # noqa: UP040 - keep Python 3.11 parser compatibility.

_NOISY_EXTERNAL_LOGGERS = (
    "urllib3",
    "requests",
    "apscheduler.scheduler",
    "apscheduler.executors.default",
    "lumibot.data_sources.yahoo_data",
)


class _BrokerProtocol(Protocol):
    def is_backtesting_broker(self) -> bool: ...


class _StrategyExecutorProtocol(Protocol):
    name: str
    result: AnalysisResult
    exception: BaseException | None
    abrupt_closing: bool
    strategy: Any

    def start(self) -> None: ...

    def join(self) -> None: ...

    def stop(self) -> None: ...


class _TraderStrategyProtocol(Protocol):
    broker: _BrokerProtocol
    _executor: _StrategyExecutorProtocol
    _analyze_backtest: bool
    backtesting_start: Any
    backtesting_end: Any
    _backtest_profiling_enabled: bool
    _backtest_profiling_tool: str
    _backtest_profiling_format: str
    _backtest_profiling_clock: str
    _backtest_profiling_artifact: str

    def verify_backtest_inputs(self, start: Any, end: Any) -> Any: ...

    def backtest_analysis(
        self,
        *,
        logdir: Path,
        show_plot: bool,
        show_tearsheet: bool,
        save_tearsheet: bool,
        show_indicators: bool,
        plot_file_html: ArtifactPath,
        trades_file: ArtifactPath,
        trade_events_file: ArtifactPath,
        settings_file: ArtifactPath,
        indicators_file: ArtifactPath,
        tearsheet_csv_file: ArtifactPath,
        tearsheet_file: ArtifactPath,
        tearsheet_metrics_file: ArtifactPath,
        base_filename: ArtifactPath,
    ) -> Any: ...


def _strategy_executor(strategy: _TraderStrategyProtocol) -> _StrategyExecutorProtocol:
    return strategy._executor


def _strategy_name(strategy: Any, default: str = "strategy") -> str:
    return str(getattr(strategy, "_name", getattr(strategy, "name", default)) or default)


@dataclass
class _BacktestProfilingConfig:
    enabled: bool
    tool: str
    format: str
    clock: str
    output_path: Path


class Trader:
    debug: bool
    backtest: bool
    quiet_logs: bool
    logfile: Path | None
    logdir: Path
    _strategies: list[_TraderStrategyProtocol]
    _pool: list[_StrategyExecutorProtocol]

    def __init__(
        self,
        logfile: object = "",
        backtest: bool = False,
        debug: bool = False,
        strategies: Sequence[_TraderStrategyProtocol] | None = None,
        quiet_logs: bool = False,
    ) -> None:
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
        if logfile and not isinstance(logfile, str):
            raise ValueError("logfile must be a string")

        # Setting debug and _logfile parameters
        self.debug = debug
        self.backtest = backtest
        self.quiet_logs = quiet_logs  # Turns off all logging execpt for error messages in backtesting

        logfile_value = cast(str, logfile)
        if logfile_value:
            self.logfile = Path(logfile_value)
            self.logfile.parent.mkdir(parents=True, exist_ok=True)
            self.logdir = self.logfile.parent
        else:
            self.logfile = None
            # default_logdir = appdirs.user_log_dir(appauthor="Lumiwealth", appname="lumibot", version="1.0")
            self.logdir = Path("logs")

        # Setting the list of strategies if defined
        self._strategies = list(strategies) if strategies else []
        self._pool = []

    @property
    def is_backtest_broker(self) -> bool:
        return any(strategy.broker.is_backtesting_broker() for strategy in self._strategies)

    def add_strategy(self, strategy: _TraderStrategyProtocol) -> None:
        """Adds a strategy to the trader"""
        self._strategies.append(strategy)

    def run_all(
        self,
        async_: bool = False,
        show_plot: bool = True,
        show_tearsheet: bool = True,
        save_tearsheet: bool = True,
        show_indicators: bool = True,
        plot_file_html: ArtifactPath = None,
        trades_file: ArtifactPath = None,
        trade_events_file: ArtifactPath = None,
        settings_file: ArtifactPath = None,
        indicators_file: ArtifactPath = None,
        tearsheet_csv_file: ArtifactPath = None,
        tearsheet_file: ArtifactPath = None,
        tearsheet_metrics_file: ArtifactPath = None,
        base_filename: ArtifactPath = None,
    ) -> StrategyResults:
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
                "No strategies to run. You must call trader.add_strategy(strategy) before trader.run_all()."
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
                    f"Received {len(self._strategies)} strategies for backtesting.You can only backtest one at a time."
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

        yappi_api: Any | None = None
        if profiling and profiling.enabled and profiling.tool == "yappi":
            try:
                api = cast(Any, import_module("yappi"))
                yappi_api = api

                api.set_clock_type(profiling.clock)
                api.clear_stats()
                api.start()
                logger.info(
                    "Backtest profiling enabled: tool=%s clock=%s artifact=%s",
                    profiling.tool,
                    profiling.clock,
                    profiling.output_path.name,
                )
            except Exception as exc:
                yappi_api = None
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
            if yappi_api is not None and profiling is not None and profiling.enabled:
                self._write_yappi_profile(yappi_api, profiling)

    def _get_backtest_profiling_config(
        self,
        *,
        strat: _TraderStrategyProtocol,
        base_filename: str | None,
    ) -> _BacktestProfilingConfig | None:
        if not self.is_backtest_broker:
            return None

        profile_mode = os.environ.get("BACKTESTING_PROFILE", "").strip().lower()
        if profile_mode != "yappi":
            return None

        strategy_name = _strategy_name(strat)
        resolved_base = base_filename or strategy_name
        output_path = (self.logdir / f"{resolved_base}_profile_yappi.csv").resolve()

        # Make settings.json aware of the profiling artifact (best-effort; should never crash).
        try:
            strat._backtest_profiling_enabled = True
            strat._backtest_profiling_tool = "yappi"
            strat._backtest_profiling_format = "csv"
            strat._backtest_profiling_clock = "wall"
            strat._backtest_profiling_artifact = output_path.name
        except Exception:
            pass

        return _BacktestProfilingConfig(
            enabled=True,
            tool="yappi",
            format="csv",
            clock="wall",
            output_path=output_path,
        )

    def _write_yappi_profile(self, yappi_api: Any, profiling: _BacktestProfilingConfig) -> None:
        try:
            yappi_api.stop()
            stats = yappi_api.get_func_stats()
            stats.sort("ttot", "desc")

            profiling.output_path.parent.mkdir(parents=True, exist_ok=True)

            # Write a text CSV artifact so existing backtest artifact download paths
            # (Bot Manager -> BotSpot "View Files") can serve it without binary handling.
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
                for entry in cast(Iterable[Any], stats):
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
                yappi_api.clear_stats()
            except Exception:
                pass

    # Async version of run_all
    def run_all_async(self) -> list[_TraderStrategyProtocol]:
        """run all strategies"""
        self.run_all(async_=True)
        return self._strategies

    def stop_all(self) -> None:
        logger.info("Stopping all strategies for this trader")
        self._stop_pool()

    def _set_logger(self) -> None:
        """Setting Logging to both console and a file if logfile is specified"""
        # Import here to avoid circular imports
        from lumibot.tools.lumibot_logger import add_file_handler, set_log_level

        # Set external library log levels to reduce noise
        # NOTE: lumilogger.get_logger doesn't work with non-lumibot loggers, so we use logging.getLogger directly
        for log_name in _NOISY_EXTERNAL_LOGGERS:
            logging.getLogger(log_name).setLevel(logging.ERROR)

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
            add_file_handler(
                str(self.logfile), level="DEBUG" if self.debug else "INFO", is_backtest=self.is_backtest_broker
            )

        # Disable Interactive Brokers logs
        for log_name, _log_obj in logging.Logger.manager.loggerDict.items():
            if log_name.startswith("ibapi"):
                iblogger = logging.getLogger(log_name)
                iblogger.setLevel(logging.CRITICAL)
                iblogger.disabled = True

    def _init_pool(self) -> None:
        self._pool = [_strategy_executor(strategy) for strategy in self._strategies]

    def _start_pool(self) -> None:
        for strategy_thread in self._pool:
            strategy_thread.start()

    def _join_pool(self) -> None:
        for strategy_thread in self._pool:
            strategy_thread.join()

        # For backtesting, check if any strategy failed and raise exception
        if self.is_backtest_broker:
            for strategy_thread in self._pool:
                # Check if the thread stored an exception
                if hasattr(strategy_thread, "exception") and strategy_thread.exception is not None:
                    raise strategy_thread.exception

    def _stop_pool(self, sig: int | None = None, frame: FrameType | None = None) -> None:
        """Run all strategies on_abrupt_closing
        lifecycle method. python signal handlers
        needs two positional arguments, the signal
        and the frame"""

        logger.debug(f"Received signal number {sig}.")
        logger.debug(f"Closing Trader in {frame} frame.")
        for strategy_thread in self._pool:
            if not strategy_thread.abrupt_closing:
                strategy_thread.stop()
                logger.info("Trading finished for %s", _strategy_name(strategy_thread.strategy, strategy_thread.name))

    def _collect_analysis(self) -> StrategyResults:
        result: StrategyResults = {}
        for strategy_thread in self._pool:
            result[strategy_thread.name] = strategy_thread.result
        return result
