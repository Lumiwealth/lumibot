"""
Example demonstrating Botspot error reporting integration with Lumibot logger.

This example shows how errors logged through the unified logger are automatically
reported to the Botspot API when the LUMIWEALTH_API_KEY is available.

To enable Botspot error reporting:
1. Set LUMIWEALTH_API_KEY environment variable with your API key (or have it in your .env file)
2. Use the standard Lumibot logger for all logging

Example:
    export LUMIWEALTH_API_KEY="your-api-key-here"
    python botspot_error_reporting_example.py
"""

import os
from lumibot.tools.lumibot_logger import get_logger, get_strategy_logger

# Example 1: Basic logger usage
def example_basic_logging():
    """Demonstrate basic logging with automatic Botspot reporting."""
    logger = get_logger(__name__)
    
    # Info messages are not reported to Botspot
    logger.info("Application started successfully")
    
    # Warning messages ARE reported to Botspot
    logger.warning("Configuration file not found, using defaults")
    
    # Error messages ARE reported to Botspot (as CRITICAL severity)
    logger.error("Failed to connect to data source")
    
    # Critical messages ARE reported to Botspot
    logger.critical("System is in an unsafe state - shutting down")


# Example 2: Strategy-specific logging
def example_strategy_logging():
    """Demonstrate strategy logging with automatic Botspot reporting."""
    logger = get_strategy_logger(__name__, "StockDiversifiedLeverage")
    
    # Strategy-specific messages include the strategy name
    logger.info("Strategy initialized")
    
    # Warnings include strategy context
    logger.warning("Portfolio imbalance detected")
    
    # Errors are reported with strategy-specific error codes
    logger.error("Failed to execute rebalancing trade")


# Example 3: Structured error reporting
def example_structured_errors():
    """Demonstrate structured error format for better Botspot integration."""
    logger = get_logger(__name__)
    
    # Use structured format: "ERROR_CODE: message | details"
    logger.error("DATA_FEED_ERROR: Market data connection lost | Provider: AlphaVantage, Retry count: 3")
    
    # Strategy logger with structured format
    strategy_logger = get_strategy_logger(__name__, "MomentumStrategy")
    strategy_logger.error("EXECUTION_ERROR: Order rejected by broker | Symbol: AAPL, Reason: Insufficient margin")


# Example 4: Error deduplication
def example_error_deduplication():
    """Demonstrate how duplicate errors are counted rather than spammed."""
    logger = get_logger(__name__)
    
    # These identical errors will be counted, not duplicated
    for i in range(5):
        logger.error("Database connection timeout")
    
    # The Botspot handler will report this as a single error with count=5


def main():
    """Run all examples."""
    print("Botspot Error Reporting Examples")
    print("=" * 50)
    
    # Check if Botspot is configured
    from lumibot.credentials import LUMIWEALTH_API_KEY
    if LUMIWEALTH_API_KEY or os.environ.get("LUMIWEALTH_API_KEY"):
        print("✅ Botspot error reporting is ENABLED")
        print("   Bot ID is handled automatically by the API")
    else:
        print("❌ Botspot error reporting is DISABLED")
        print("   Set LUMIWEALTH_API_KEY to enable")
    
    print("\nRunning examples...\n")
    
    print("1. Basic logging example:")
    example_basic_logging()
    
    print("\n2. Strategy logging example:")
    example_strategy_logging()
    
    print("\n3. Structured error example:")
    example_structured_errors()
    
    print("\n4. Error deduplication example:")
    example_error_deduplication()
    
    print("\n✅ Examples completed!")
    print("\nNote: If Botspot is configured, all WARNING+ messages above were")
    print("automatically reported to the Botspot API endpoint.")


if __name__ == "__main__":
    main()