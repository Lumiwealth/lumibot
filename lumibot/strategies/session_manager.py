"""
Session Management System for Lumibot Strategy Execution

This module implements a clean session-based architecture that separates 
backtesting and live trading concerns, solving the infinite restart bug 
by ensuring guaranteed time progression in backtesting scenarios.

Root Cause Analysis:
The original _run_trading_session method mixed live trading and backtesting 
logic in a single 963+ line method, with time advancement scattered across 
multiple methods (safe_sleep, await_market_to_open, _strategy_sleep) that 
provided no guarantees of forward progress. This caused infinite restarts 
when _run_trading_session completed without advancing time.

Architecture Solution:
- SessionManager: Abstract base class defining session interface
- BacktestingSession: Handles backtesting with guaranteed time progression  
- LiveTradingSession: Handles live trading with APScheduler management
- Clean separation of concerns with data source agnostic design
"""

import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from lumibot.strategies.strategy_executor import StrategyExecutor

logger = logging.getLogger(__name__)


class SessionManager(ABC):
    """
    Abstract base class for managing strategy execution sessions.
    
    Provides a clean interface for both backtesting and live trading sessions
    with guaranteed time progression and clear separation of concerns.
    """

    def __init__(self, strategy_executor: 'StrategyExecutor'):
        """
        Initialize the session manager.
        
        Args:
            strategy_executor: The strategy executor instance
        """
        self.strategy_executor = strategy_executor
        self._session_start_time = None
        self._last_execution_time = None

    @abstractmethod
    def should_continue_session(self) -> bool:
        """
        Determine if the trading session should continue.
        
        Returns:
            bool: True if session should continue, False otherwise
        """
        pass

    @abstractmethod
    def advance_time(self) -> bool:
        """
        Advance time in the session.
        
        This method MUST guarantee forward time progression in backtesting
        to prevent infinite restart loops.
        
        Returns:
            bool: True if time was successfully advanced, False if at end
        """
        pass

    @abstractmethod
    def execute_trading_cycle(self) -> None:
        """
        Execute one complete trading cycle (on_trading_iteration).
        
        This method handles the core strategy execution logic while
        delegating time management to the session manager.
        """
        pass

    @abstractmethod
    def wait_for_next_cycle(self) -> None:
        """
        Wait for the next trading cycle to begin.
        
        In backtesting: Advances to next time period
        In live trading: Sleeps until next scheduled execution
        """
        pass

    def run_session(self) -> None:
        """
        Main session execution loop.
        
        This replaces the problematic _run_trading_session method with
        a clean, session-aware approach that guarantees progress.
        """
        logger.info(f"[{self.__class__.__name__}] Starting trading session")
        self._session_start_time = datetime.now()

        try:
            while self.should_continue_session():
                # Execute trading logic
                self.execute_trading_cycle()

                # Advance time - CRITICAL for preventing infinite loops
                if not self.advance_time():
                    logger.info(f"[{self.__class__.__name__}] Reached end of session data")
                    break

                # Wait for next cycle
                self.wait_for_next_cycle()

        except Exception as e:
            logger.error(f"[{self.__class__.__name__}] Session error: {e}")
            raise
        finally:
            logger.info(f"[{self.__class__.__name__}] Trading session completed")

    def get_session_duration(self) -> Optional[timedelta]:
        """Get the current session duration."""
        if self._session_start_time:
            return datetime.now() - self._session_start_time
        return None


class BacktestingSession(SessionManager):
    """
    Session manager for backtesting scenarios.
    
    Ensures guaranteed time progression to prevent infinite restart loops
    that occur when _run_trading_session completes without advancing time.
    """

    def __init__(self, strategy_executor: 'StrategyExecutor'):
        super().__init__(strategy_executor)
        self._current_time = None
        self._end_time = None
        self._time_step = timedelta(days=1)  # Default daily progression

    def should_continue_session(self) -> bool:
        """Check if backtesting should continue."""

        # Check if strategy executor should continue
        if hasattr(self.strategy_executor, 'should_continue') and not self.strategy_executor.should_continue:
            logger.info("[BacktestingSession] Strategy executor should_continue is False")
            return False

        # Check if broker should continue
        if hasattr(self.strategy_executor, 'broker'):
            if hasattr(self.strategy_executor.broker, 'should_continue'):
                if not self.strategy_executor.broker.should_continue():
                    logger.info("[BacktestingSession] Broker should_continue is False")
                    return False

        # Check if we've reached the end time
        if self._end_time and self._current_time and self._current_time >= self._end_time:
            logger.info(f"[BacktestingSession] Reached end time: {self._end_time}")
            return False

        # Check if backtesting is finished (if method exists)
        if hasattr(self.strategy_executor, 'is_backtesting_finished'):
            if self.strategy_executor.is_backtesting_finished():
                logger.info("[BacktestingSession] Backtesting finished")
                return False

        return True

    def advance_time(self) -> bool:
        """
        Advance time in backtesting.
        
        CRITICAL: This method guarantees forward time progression,
        solving the infinite restart bug by ensuring _run_trading_session
        never completes without time advancement.
        
        Returns:
            bool: True if time advanced successfully, False if at end
        """
        if self._current_time is None:
            # Initialize with strategy executor's current time or strategy's datetime
            if hasattr(self.strategy_executor, 'strategy') and hasattr(self.strategy_executor.strategy, 'datetime'):
                self._current_time = self.strategy_executor.strategy.datetime
            elif hasattr(self.strategy_executor, 'datetime'):
                self._current_time = self.strategy_executor.datetime
            else:
                self._current_time = datetime.now()

        previous_time = self._current_time

        # Advance time by the configured step
        self._current_time += self._time_step

        # Update strategy executor's datetime
        if hasattr(self.strategy_executor, 'datetime'):
            self.strategy_executor.datetime = self._current_time

        # Update strategy's datetime
        if hasattr(self.strategy_executor, 'strategy') and hasattr(self.strategy_executor.strategy, 'datetime'):
            self.strategy_executor.strategy.datetime = self._current_time

        self._last_execution_time = self._current_time

        logger.debug(f"[BacktestingSession] Time advanced: {previous_time} -> {self._current_time}")

        # Check if we've reached the end time
        if self._end_time and self._current_time >= self._end_time:
            logger.info(f"[BacktestingSession] Reached end time: {self._end_time}")
            return False

        return True

    def execute_trading_cycle(self) -> None:
        """Execute one trading iteration in backtesting."""
        try:
            # Update strategy datetime to current session time before executing
            if hasattr(self.strategy_executor, 'strategy') and hasattr(self.strategy_executor.strategy, 'datetime'):
                self.strategy_executor.strategy.datetime = self._current_time

            # Instead of calling the problematic _run_trading_session method,
            # we call the core trading iteration directly to avoid infinite loops
            if hasattr(self.strategy_executor, 'on_trading_iteration'):
                self.strategy_executor.on_trading_iteration()
            elif hasattr(self.strategy_executor, 'strategy') and hasattr(self.strategy_executor.strategy, 'on_trading_iteration'):
                # Call the strategy's on_trading_iteration method directly
                self.strategy_executor.strategy.on_trading_iteration()
            else:
                # Last resort: minimal trading cycle
                logger.warning("[BacktestingSession] No trading iteration method found, using minimal cycle")

        except Exception as e:
            logger.error(f"[BacktestingSession] Error in trading cycle: {e}")
            raise

    def wait_for_next_cycle(self) -> None:
        """
        Wait for next cycle in backtesting.
        
        In backtesting, this is typically a no-op since time advancement
        is handled by advance_time() method.
        """
        # No actual waiting needed in backtesting
        # Time progression is controlled by advance_time()
        pass

    def set_time_parameters(self, start_time: datetime, end_time: datetime, time_step: timedelta = None):
        """
        Set time parameters for backtesting session.
        
        Args:
            start_time: Session start time
            end_time: Session end time  
            time_step: Time step for progression (default: 1 day)
        """
        self._current_time = start_time
        self._end_time = end_time
        if time_step:
            self._time_step = time_step

        logger.info(f"[BacktestingSession] Configured: {start_time} to {end_time}, step: {self._time_step}")


class LiveTradingSession(SessionManager):
    """
    Session manager for live trading scenarios.
    
    Handles APScheduler integration and real-time execution without
    the time advancement requirements of backtesting.
    """

    def __init__(self, strategy_executor: 'StrategyExecutor'):
        super().__init__(strategy_executor)
        self._is_market_open = False
        self._scheduler = None

    def should_continue_session(self) -> bool:
        """Check if live trading should continue."""
        # Check if strategy executor is still running
        if hasattr(self.strategy_executor, 'is_alive'):
            return self.strategy_executor.is_alive()
        return True

    def advance_time(self) -> bool:
        """
        Time advancement for live trading.
        
        In live trading, time advances naturally, so this method
        primarily validates that we're still within trading hours
        and updates internal tracking.
        
        Returns:
            bool: Always True for live trading (time advances naturally)
        """
        current_time = datetime.now()
        self._last_execution_time = current_time

        # Update strategy executor's datetime if needed
        if hasattr(self.strategy_executor, 'datetime'):
            self.strategy_executor.datetime = current_time

        logger.debug(f"[LiveTradingSession] Time updated: {current_time}")
        return True

    def execute_trading_cycle(self) -> None:
        """Execute one trading iteration in live trading."""
        try:
            # Call the strategy's trading iteration
            if hasattr(self.strategy_executor, 'on_trading_iteration'):
                self.strategy_executor.on_trading_iteration()
            else:
                logger.warning("[LiveTradingSession] No on_trading_iteration method found")

        except Exception as e:
            logger.error(f"[LiveTradingSession] Error in trading cycle: {e}")
            raise

    def wait_for_next_cycle(self) -> None:
        """
        Wait for next cycle in live trading.
        
        This method handles the real-time scheduling and market hours
        checking that's required for live trading scenarios.
        """
        # Implementation would depend on strategy executor's scheduling logic
        # For now, delegate to existing safe_sleep mechanism
        if hasattr(self.strategy_executor, 'safe_sleep'):
            # Use existing sleep mechanism for compatibility
            sleep_duration = getattr(self.strategy_executor, 'sleeptime', 1)
            self.strategy_executor.safe_sleep(sleep_duration)
        else:
            # Fallback to simple sleep
            import time
            time.sleep(1)
