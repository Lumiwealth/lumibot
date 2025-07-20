# Strategy Executor Refactor Plan

## Current Problems

### 1. Mixed Responsibilities in `_run_trading_session()`
- Handles both live trading and backtesting in same method
- Complex conditional logic based on data source type
- Different time management for different scenarios

### 2. Datetime Management Issues
- Time advancement scattered across multiple methods:
  - `safe_sleep()` -> `broker._update_datetime()`
  - `await_market_to_open()` -> internal time logic
  - `await_market_to_close()` -> internal time logic
- No guarantee that time will advance in all scenarios
- Infinite restart when session completes without time advancement

### 3. Control Flow Complexity
- Multiple early returns based on conditions
- Nested if/else blocks for different scenarios
- Hard to trace execution path and debug issues

## Proposed Solution: Session-Based Architecture

### Core Concept
Split execution into three distinct session managers:
1. **LiveTradingSession** - Handles real-time trading with APScheduler
2. **BacktestingSession** - Handles simulation with guaranteed time progression  
3. **PandasDailySession** - Handles pandas daily data iteration

### New Architecture

```
StrategyExecutor.run()
├── Initialize strategy and broker
├── Create appropriate session manager based on context
└── session_manager.execute()

SessionManager (Base Class)
├── setup_session()
├── execute_trading_loop() 
├── cleanup_session()
└── advance_time_if_needed()  # Critical: ensures time always progresses

LiveTradingSession(SessionManager)
├── Uses APScheduler for timing
├── Handles real-time market events
└── No artificial time advancement needed

BacktestingSession(SessionManager)  
├── Guarantees time advancement after each iteration
├── Handles market open/close logic
├── Prevents infinite restart conditions
└── Clean separation of time management

PandasDailySession(SessionManager)
├── Iterates through pandas date index
├── Simple daily progression logic
└── Minimal complexity
```

### Key Benefits

1. **Single Responsibility**: Each session type handles one scenario
2. **Guaranteed Time Progression**: Every session ensures time advances appropriately
3. **Cleaner Control Flow**: Linear execution with clear state transitions
4. **Easier Testing**: Each session type can be tested independently
5. **Better Debugging**: Clear separation of concerns

## Detailed Implementation Plan

### Phase 1: Extract Base SessionManager

```python
class SessionManager:
    def __init__(self, strategy_executor):
        self.executor = strategy_executor
        self.strategy = strategy_executor.strategy
        self.broker = strategy_executor.broker
        
    def execute(self):
        """Main entry point for session execution"""
        try:
            self.setup_session()
            result = self.execute_trading_loop()
            self.cleanup_session()
            return result
        finally:
            # Critical: ensure time advances even if errors occur
            self.advance_time_if_needed()
    
    def setup_session(self):
        """Prepare for trading session"""
        raise NotImplementedError
    
    def execute_trading_loop(self):
        """Execute the main trading logic"""
        raise NotImplementedError
        
    def cleanup_session(self):
        """Clean up after session"""
        pass
        
    def advance_time_if_needed(self):
        """Ensure time progresses to prevent infinite restart"""
        raise NotImplementedError
```

### Phase 2: Implement BacktestingSession

```python
class BacktestingSession(SessionManager):
    def __init__(self, strategy_executor):
        super().__init__(strategy_executor)
        self.session_start_time = None
        
    def setup_session(self):
        """Setup backtesting session"""
        self.session_start_time = self.broker.datetime
        
        # Handle market open logic
        if not self._is_247_market():
            self.strategy.await_market_to_open()
            
            if not self.broker.should_continue():
                return False
                
            self._handle_market_open_lifecycle()
            
        return True
    
    def execute_trading_loop(self):
        """Execute backtesting trading loop with guaranteed time progression"""
        
        if self._is_pandas_daily():
            return self._execute_pandas_daily_loop()
        
        iteration_count = 0
        while self._should_continue_trading():
            iteration_count += 1
            
            # Execute trading iteration
            self.executor._on_trading_iteration()
            
            if self.broker.IS_BACKTESTING_BROKER:
                self.broker.process_pending_orders(strategy=self.strategy)
            
            # CRITICAL: Always attempt to advance time
            if not self._advance_time_for_next_iteration():
                break
                
        return True
    
    def _advance_time_for_next_iteration(self):
        """Advance time for next iteration, preventing infinite restart"""
        current_time = self.broker.datetime
        
        # Try strategy sleep first
        if self.executor._strategy_sleep():
            return True
            
        # If strategy sleep failed, force time advancement
        self._force_time_advancement()
        return False  # Signal end of session
    
    def _force_time_advancement(self):
        """Force time advancement when normal sleep fails"""
        current_time = self.broker.datetime
        
        if self._is_247_market():
            # For 24/7 markets, advance by sleeptime or minimum 1 minute
            advance_seconds = max(
                self.executor._sleeptime_to_seconds(self.strategy.sleeptime),
                60  # Minimum 1 minute
            )
        else:
            # For regular markets, advance to next trading day
            advance_seconds = 24 * 60 * 60  # 1 day
            
        next_time = current_time + timedelta(seconds=advance_seconds)
        
        self.broker._update_datetime(
            next_time,
            cash=self.strategy.cash, 
            portfolio_value=self.strategy.portfolio_value
        )
        
        print(f"[DEBUG] Forced time advancement: {current_time} -> {self.broker.datetime}")
    
    def advance_time_if_needed(self):
        """Ensure time has advanced since session start"""
        if (self.session_start_time is not None and 
            self.broker.datetime == self.session_start_time):
            
            print(f"[DEBUG] Session completed without time advancement, forcing progression")
            self._force_time_advancement()
```

### Phase 3: Implement LiveTradingSession

```python
class LiveTradingSession(SessionManager):
    def setup_session(self):
        """Setup live trading with APScheduler"""
        if not self._is_247_market():
            self.strategy.await_market_to_open()
            
        # Start APScheduler
        if not self.executor.scheduler.running:
            self.executor.scheduler.start()
            self._setup_trading_jobs()
            
    def execute_trading_loop(self):
        """Execute live trading loop"""
        self._start_queue_thread()
        
        while self._should_continue_live_trading():
            self._handle_lifecycle_methods()
            self._send_cloud_updates()
            time.sleep(1)
            
    def advance_time_if_needed(self):
        """Live trading doesn't need artificial time advancement"""
        pass  # Real time progresses naturally
```

### Phase 4: Refactor StrategyExecutor.run()

```python
def run(self):
    try:
        self._initialize_strategy()
        
        # Create appropriate session manager
        session_manager = self._create_session_manager()
        
        # Execute trading session
        while self.broker.should_continue() and self.should_continue:
            try:
                session_manager.execute()
            except Exception as e:
                if not self._handle_session_error(e):
                    break
                    
        self._finalize_strategy()
        
    except Exception as e:
        self.exception = e
        return False
        
def _create_session_manager(self):
    """Factory method to create appropriate session manager"""
    if self.strategy.is_backtesting:
        return BacktestingSession(self)
    else:
        return LiveTradingSession(self)
```

## Benefits of This Refactor

### 1. Eliminates Infinite Restart Bug
- Each session guarantees time progression
- Clear separation between time advancement logic
- Fallback mechanisms when normal sleep fails

### 2. Improves Code Maintainability  
- Single responsibility for each session type
- Linear execution flow
- Easier to understand and debug

### 3. Better Error Handling
- Errors contained within session scope
- Guaranteed cleanup and time advancement
- More robust recovery mechanisms

### 4. Easier Testing
- Mock individual session managers
- Test time advancement scenarios independently
- Clearer test boundaries

### 5. Future Extensibility
- Easy to add new session types
- Plugin architecture for different brokers
- Clean separation of concerns

## Migration Strategy

1. **Phase 1**: Extract base SessionManager interface
2. **Phase 2**: Implement BacktestingSession with existing logic
3. **Phase 3**: Test backtesting scenarios thoroughly
4. **Phase 4**: Implement LiveTradingSession 
5. **Phase 5**: Update StrategyExecutor.run() to use session managers
6. **Phase 6**: Remove old _run_trading_session() method
7. **Phase 7**: Clean up safe_sleep() and related methods

This refactor will solve the infinite restart bug while making the codebase much more maintainable and testable.
