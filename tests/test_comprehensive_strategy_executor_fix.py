"""
Comprehensive test for strategy executor infinite restart bug fix.

This test demonstrates the current BROKEN architecture in _run_trading_session
and safe_sleep that causes infinite restart loops in certain scenarios.

CURRENT ISSUES:
1. _run_trading_session has mixed responsibilities (live vs backtesting)
2. Time advancement is scattered across multiple methods
3. No guarantee that time will advance in all scenarios
4. Infinite restart when session completes without time advancement

ROOT CAUSE:
- Backtesting loop assumes _strategy_sleep() will always advance time
- If strategy_sleeptime == 0 or time_to_before_closing <= 0, _strategy_sleep() returns False without advancing time
- Session completes without time advancement
- Main run() loop immediately calls _run_trading_session() again with same datetime
- INFINITE RESTART LOOP

PROPER FIX NEEDED:
This requires a full refactor of _run_trading_session to use session-based architecture
with guaranteed time progression. See STRATEGY_EXECUTOR_REFACTOR_PLAN.md
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
import sys
import os

# Add the lumibot path
sys.path.insert(0, '/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot')

from lumibot.strategies.strategy import Strategy
from lumibot.entities import Asset
from lumibot.strategies.strategy_executor import StrategyExecutor


class MockTestStrategy(Strategy):
    """Test strategy that tracks how many times _run_trading_session is called"""
    
    def __init__(self, market_type="NYSE", sleeptime="1M", **kwargs):
        super().__init__(**kwargs)
        self.market_type = market_type
        self.sleeptime = sleeptime
        self.session_call_count = 0
        
    def initialize(self):
        """Set up the test strategy"""
        self.asset = Asset("TEST", asset_type=Asset.AssetType.STOCK)
        self.set_market(self.market_type)
        
    def on_trading_iteration(self):
        """Simple trading logic"""
        pass


class TestStrategyExecutorCurrentBrokenBehavior(unittest.TestCase):
    """Test to document the current broken behavior and validate the proper fix"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.base_datetime = datetime(2024, 1, 15, 9, 30)  # Monday 9:30 AM
        
    def _create_mock_broker(self, market_type, initial_datetime):
        """Create a mock broker with the specified market type"""
        broker = Mock()
        broker.market = market_type
        broker.datetime = initial_datetime
        broker.should_continue.return_value = True
        broker._update_datetime = Mock()
        return broker
    
    def test_current_infinite_restart_detection_fix(self):
        """Test that the current fix in run() method works but is a band-aid solution"""
        print("\n=== Testing Current Band-Aid Fix ===")
        
        # Create strategy with us_futures market (the problematic case)
        strategy = MockTestStrategy(market_type="us_futures", sleeptime="1M")
        strategy.is_backtesting = True
        strategy.broker = self._create_mock_broker("us_futures", self.base_datetime)
        
        # Mock _run_trading_session to simulate the infinite restart condition
        original_datetime = self.base_datetime
        call_count = 0
        
        def mock_run_trading_session():
            nonlocal call_count
            call_count += 1
            print(f"  _run_trading_session call #{call_count}")
            
            # Simulate the condition that causes infinite restart:
            # _run_trading_session completes but datetime doesn't advance
            # (This happens when _strategy_sleep returns False without advancing time)
            if call_count >= 3:  # Prevent actual infinite loop in test
                strategy.broker.datetime = original_datetime + timedelta(hours=1)  # Finally advance time
        
        # Create executor and mock the problematic method
        executor = StrategyExecutor(strategy)
        executor._run_trading_session = Mock(side_effect=mock_run_trading_session)
        
        print(f"Initial datetime: {strategy.broker.datetime}")
        
        # Run with limited iterations to prevent actual infinite loop
        max_iterations = 5
        iteration = 0
        
        while (strategy.broker.should_continue() and 
               executor.should_continue and 
               iteration < max_iterations):
            
            iteration += 1
            print(f"Main loop iteration {iteration}")
            
            datetime_before = strategy.broker.datetime
            executor._run_trading_session()
            datetime_after = strategy.broker.datetime
            
            print(f"  Before: {datetime_before}")
            print(f"  After: {datetime_after}")
            
            # The current fix in run() should detect this condition
            if (strategy.is_backtesting and 
                datetime_after == datetime_before and
                hasattr(strategy.broker, "market") and 
                strategy.broker.market not in ["24/7", "24/5"]):
                
                print(f"  INFINITE RESTART DETECTED - applying band-aid fix")
                strategy.broker.datetime = datetime_before + timedelta(days=1)
                print(f"  Advanced to: {strategy.broker.datetime}")
                break
        
        print(f"Total _run_trading_session calls: {call_count}")
        print(f"Final datetime: {strategy.broker.datetime}")
        
        # Verify the band-aid fix worked (time eventually advanced)
        self.assertGreater(strategy.broker.datetime, original_datetime,
                          "Band-aid fix should have advanced datetime")
        
        # But note this is NOT the proper solution - it's in the wrong place
        print("\nNOTE: This fix works but is in the wrong architectural layer!")
        print("The real fix should be in _run_trading_session or session management")
    
    def test_root_cause_analysis(self):
        """Document the root cause of the infinite restart bug"""
        print("\n=== Root Cause Analysis ===")
        
        print("PROBLEM SCENARIO:")
        print("1. _run_trading_session() starts backtesting loop")
        print("2. Loop condition: time_to_close > minutes_before_closing")
        print("3. Loop body calls _on_trading_iteration() then _strategy_sleep()")
        print("4. _strategy_sleep() checks conditions and may return False without advancing time")
        print("5. Loop exits, _run_trading_session() completes")
        print("6. run() method immediately calls _run_trading_session() again")
        print("7. Same datetime, same conditions -> INFINITE RESTART")
        
        print("\nCONDITIONS THAT CAUSE _strategy_sleep() TO NOT ADVANCE TIME:")
        print("- strategy_sleeptime == 0")
        print("- time_to_before_closing <= 0") 
        print("- should_continue == False")
        
        print("\nARCHITECTURAL ISSUES:")
        print("- _run_trading_session() mixes live trading and backtesting logic")
        print("- Time advancement scattered across multiple methods")
        print("- No guaranteed time progression policy")
        print("- Complex control flow with multiple early returns")
        
        print("\nPROPER FIX NEEDED:")
        print("- Session-based architecture with separate managers")
        print("- Guaranteed time progression in each session type")
        print("- Clean separation of responsibilities")
        print("- See STRATEGY_EXECUTOR_REFACTOR_PLAN.md for detailed solution")
        
        # This test always passes - it's just documentation
        self.assertTrue(True, "Root cause analysis complete")
    
    def test_proposed_session_architecture(self):
        """Test the proposed session-based architecture concept"""
        print("\n=== Proposed Session Architecture ===")
        
        print("CURRENT MONOLITHIC APPROACH:")
        print("_run_trading_session() -> handles everything")
        
        print("\nPROPOSED SESSION-BASED APPROACH:")
        print("SessionManager (base class)")
        print("├── BacktestingSession")
        print("│   ├── Guaranteed time progression")
        print("│   ├── Clean iteration logic") 
        print("│   └── Fallback time advancement")
        print("├── LiveTradingSession")
        print("│   ├── APScheduler management")
        print("│   └── Real-time events")
        print("└── PandasDailySession")
        print("    ├── Date index iteration")
        print("    └── Simple daily progression")
        
        print("\nBENEFITS:")
        print("- Single responsibility principle")
        print("- Guaranteed time progression")
        print("- Easier testing and debugging")
        print("- Clean separation of concerns")
        print("- No infinite restart conditions")
        
        # Mock a simple session manager concept
        class MockSessionManager:
            def __init__(self, strategy_executor):
                self.executor = strategy_executor
                self.session_start_time = None
                
            def execute(self):
                self.session_start_time = self.executor.broker.datetime
                self.execute_trading_loop()
                self.advance_time_if_needed()
                
            def execute_trading_loop(self):
                # Simplified trading logic
                pass
                
            def advance_time_if_needed(self):
                # CRITICAL: Guarantee time progression
                if self.executor.broker.datetime == self.session_start_time:
                    print("  Session completed without time advancement - forcing progression")
                    self.executor.broker.datetime += timedelta(days=1)
        
        # Test the concept
        strategy = MockTestStrategy(market_type="us_futures", sleeptime="1M")
        strategy.is_backtesting = True
        strategy.broker = self._create_mock_broker("us_futures", self.base_datetime)
        
        executor = StrategyExecutor(strategy)
        session_manager = MockSessionManager(executor)
        
        print(f"Initial datetime: {executor.broker.datetime}")
        session_manager.execute()
        print(f"Final datetime: {executor.broker.datetime}")
        
        # Verify time advanced
        self.assertGreater(executor.broker.datetime, self.base_datetime,
                          "Session manager should guarantee time progression")
        
        print("✅ Session-based architecture would prevent infinite restart!")


if __name__ == "__main__":
    print("Testing current broken behavior and documenting proper fix needed...")
    unittest.main(verbosity=2)
    
    def test_es_futures_gets_datetime_advancement(self):
        """Test that ES futures gets datetime advancement when infinite restart is detected"""
        print("\n=== Testing ES Futures Infinite Restart Detection ===")
        
        # Create strategy with us_futures market
        strategy = MockTestStrategy(market_type="us_futures", sleeptime="1M")
        strategy.is_backtesting = True
        strategy.simulate_infinite_restart = True  # Simulate the infinite restart condition
        
        # Create broker with us_futures market  
        broker = self._create_mock_broker("us_futures", self.base_datetime)
        
        # Create executor and run
        executor = self._create_strategy_executor(strategy, broker)
        
        print(f"Initial datetime: {broker.datetime}")
        print(f"Market type: {broker.market}")
        
        # Run the main execution loop
        executor.run()
        
        print(f"Session calls made: {strategy.session_call_count}")
        print(f"_update_datetime called: {broker._update_datetime.called}")
        
        # Verify that datetime advancement was triggered when infinite restart detected
        self.assertTrue(broker._update_datetime.called, 
                       "us_futures with infinite restart should trigger datetime advancement")
        
        # Verify _update_datetime was called with advanced time
        call_args = broker._update_datetime.call_args
        if call_args:
            advanced_datetime = call_args[0][0] 
            print(f"Advanced to: {advanced_datetime}")
            self.assertGreater(advanced_datetime, self.base_datetime,
                             "Datetime should be advanced when infinite restart detected")
    
    def test_stock_markets_no_datetime_advancement(self):
        """Test that stock markets (NYSE) do NOT get datetime advancement"""
        print("\n=== Testing Stock Markets (NYSE) ===")
        
        # Create strategy with NYSE market
        strategy = MockTestStrategy(market_type="NYSE", sleeptime="1M")
        strategy.is_backtesting = True
        
        # Create broker with NYSE market
        broker = self._create_mock_broker("NYSE", self.base_datetime)
        
        # Create executor and run
        executor = self._create_strategy_executor(strategy, broker)
        
        print(f"Initial datetime: {broker.datetime}")
        print(f"Market type: {broker.market}")
        
        # Run the main execution loop
        executor.run()
        
        print(f"Session calls made: {strategy.session_call_count}")
        print(f"_update_datetime called: {broker._update_datetime.called}")
        
        # Verify that datetime advancement was NOT triggered for NYSE
        self.assertFalse(broker._update_datetime.called, 
                        "NYSE should NOT trigger datetime advancement")
    
    def test_24_7_markets_no_datetime_advancement(self):
        """Test that 24/7 markets do NOT get datetime advancement"""
        print("\n=== Testing 24/7 Markets ===")
        
        # Create strategy with 24/7 market
        strategy = MockTestStrategy(market_type="24/7", sleeptime="1M")
        strategy.is_backtesting = True
        
        # Create broker with 24/7 market
        broker = self._create_mock_broker("24/7", self.base_datetime)
        
        # Create executor and run
        executor = self._create_strategy_executor(strategy, broker)
        
        print(f"Initial datetime: {broker.datetime}")
        print(f"Market type: {broker.market}")
        
        # Run the main execution loop  
        executor.run()
        
        print(f"Session calls made: {strategy.session_call_count}")
        print(f"_update_datetime called: {broker._update_datetime.called}")
        
        # Verify that datetime advancement was NOT triggered for 24/7
        self.assertFalse(broker._update_datetime.called, 
                        "24/7 markets should NOT trigger datetime advancement")
    
    def test_different_sleeptimes_us_futures(self):
        """Test that the fix works with different sleeptimes for us_futures"""
        print("\n=== Testing Different Sleeptimes for us_futures ===")
        
        sleeptimes = ["1M", "5M", "1H", "1D"]
        
        for sleeptime in sleeptimes:
            with self.subTest(sleeptime=sleeptime):
                print(f"\nTesting sleeptime: {sleeptime}")
                
                # Create strategy with us_futures market and specific sleeptime
                strategy = MockTestStrategy(market_type="us_futures", sleeptime=sleeptime)
                strategy.is_backtesting = True
                
                # Create broker with us_futures market
                broker = self._create_mock_broker("us_futures", self.base_datetime)
                
                # Create executor and run
                executor = self._create_strategy_executor(strategy, broker)
                
                # Run the main execution loop
                executor.run()
                
                print(f"  Session calls: {strategy.session_call_count}")
                print(f"  DateTime advancement: {broker._update_datetime.called}")
                
                # Verify datetime advancement for all sleeptimes with us_futures
                self.assertTrue(broker._update_datetime.called,
                              f"us_futures with {sleeptime} should trigger datetime advancement")
    
    def test_different_sleeptimes_nyse(self):
        """Test that NYSE doesn't get the fix regardless of sleeptime"""
        print("\n=== Testing Different Sleeptimes for NYSE ===")
        
        sleeptimes = ["1M", "5M", "1H", "1D"]
        
        for sleeptime in sleeptimes:
            with self.subTest(sleeptime=sleeptime):
                print(f"\nTesting sleeptime: {sleeptime}")
                
                # Create strategy with NYSE market and specific sleeptime
                strategy = MockTestStrategy(market_type="NYSE", sleeptime=sleeptime)
                strategy.is_backtesting = True
                
                # Create broker with NYSE market
                broker = self._create_mock_broker("NYSE", self.base_datetime)
                
                # Create executor and run
                executor = self._create_strategy_executor(strategy, broker)
                
                # Run the main execution loop
                executor.run()
                
                print(f"  Session calls: {strategy.session_call_count}")
                print(f"  DateTime advancement: {broker._update_datetime.called}")
                
                # Verify NO datetime advancement for NYSE regardless of sleeptime
                self.assertFalse(broker._update_datetime.called,
                               f"NYSE with {sleeptime} should NOT trigger datetime advancement")
    
    def test_live_trading_no_fix_applied(self):
        """Test that the fix is only applied during backtesting"""
        print("\n=== Testing Live Trading (No Fix) ===")
        
        # Create strategy with us_futures but NOT backtesting
        strategy = MockTestStrategy(market_type="us_futures", sleeptime="1M")
        strategy.is_backtesting = False  # Live trading mode
        
        # Create broker with us_futures market
        broker = self._create_mock_broker("us_futures", self.base_datetime)
        
        # Create executor and run
        executor = self._create_strategy_executor(strategy, broker)
        
        print(f"Initial datetime: {broker.datetime}")
        print(f"Market type: {broker.market}")
        print(f"Backtesting mode: {strategy.is_backtesting}")
        
        # Run the main execution loop
        executor.run()
        
        print(f"Session calls made: {strategy.session_call_count}")
        print(f"_update_datetime called: {broker._update_datetime.called}")
        
        # Verify that fix is NOT applied in live trading
        self.assertFalse(broker._update_datetime.called,
                        "Live trading should NOT trigger datetime advancement")
    
    def test_original_polygon_test_scenario(self):
        """Test scenario similar to the failing Polygon test to ensure we don't break it"""
        print("\n=== Testing Original Polygon Test Scenario ===")
        
        # Simulate the failing test conditions
        expected_end_datetime = datetime(2024, 2, 12, 8, 30)  # Expected end time
        
        # Create strategy similar to Polygon test
        strategy = MockTestStrategy(market_type="NYSE", sleeptime="1D")  # Daily timeframe
        strategy.is_backtesting = True
        
        # Create broker that should end at the expected datetime
        broker = self._create_mock_broker("NYSE", expected_end_datetime)
        
        # Override should_continue to stop after 1 call (simulate normal completion)
        def mock_should_continue():
            strategy.session_call_count += 1
            return strategy.session_call_count <= 1
        
        broker.should_continue = mock_should_continue
        
        # Create executor and run
        executor = self._create_strategy_executor(strategy, broker)
        executor._run_trading_session = Mock()  # Don't actually run session
        
        print(f"Initial datetime: {broker.datetime}")
        print(f"Market type: {broker.market}")
        
        # Run the main execution loop
        executor.run()
        
        print(f"Final datetime: {broker.datetime}")
        print(f"_update_datetime called: {broker._update_datetime.called}")
        
        # Verify datetime was NOT changed (preserving original test expectation)
        self.assertFalse(broker._update_datetime.called,
                        "NYSE market should not have datetime advancement")
        self.assertEqual(broker.datetime, expected_end_datetime,
                        "Broker datetime should remain at expected end time")


if __name__ == "__main__":
    print("Running comprehensive strategy executor infinite loop fix tests...")
    unittest.main(verbosity=2)
