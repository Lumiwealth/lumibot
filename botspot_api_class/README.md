# BotSpot API Client

A simple, elegant Python client for the BotSpot trading platform API.

## Features

- **World-class simplicity**: `client = BotSpot()` → `client.users.get_profile()`
- **Automatic authentication**: Uses Selenium for OAuth, then pure API calls
- **Token caching**: Avoids re-login with persistent token storage
- **Lazy authentication**: Only authenticates on first API call
- **Resource-based API**: Intuitive organization (users, strategies, backtests, deployments)
- **Comprehensive error handling**: Specific exceptions with helpful messages
- **Thread-safe**: Token management with locks for concurrent access

## Installation

The dependencies are listed in `requirements_api_class.txt` at the root level:

```bash
cd /Users/marvin/repos/lumibot
pip install -r requirements_api_class.txt
```

Dependencies:
- `requests>=2.31.0` - HTTP client
- `selenium>=4.15.0` - Browser automation for OAuth
- `python-dotenv>=1.0.0` - Environment variable management

## Configuration

Credentials are loaded from `/Users/marvin/repos/lumibot/.env`:

```bash
BOTSPOT_USERNAME=your@email.com
BOTSPOT_PASSWORD=your_password
```

## Quick Start

```python
from botspot_api_class import BotSpot

# Initialize client (loads from .env automatically)
client = BotSpot()

# Get user profile (triggers authentication on first call)
profile = client.users.get_profile()
print(f"Logged in as: {profile['firstName']} {profile['lastName']}")

# List strategies
strategies = client.strategies.list()
for strategy in strategies:
    print(f"{strategy['name']}: {strategy['status']}")
```

## Context Manager Support

```python
from botspot_api_class import BotSpot

with BotSpot() as client:
    profile = client.users.get_profile()
    strategies = client.strategies.list()
```

## API Reference

### Users Resource

```python
# Get current user profile
profile = client.users.get_profile()

# Update profile
profile = client.users.update_profile(
    firstName="John",
    lastName="Doe",
    phone="+1234567890"
)
```

### Strategies Resource

```python
# List all strategies
strategies = client.strategies.list(limit=10)

# Get specific strategy
strategy = client.strategies.get("strategy_id")

# Create strategy
strategy = client.strategies.create(
    name="My Strategy",
    description="A simple strategy",
    code="# Python code here"
)

# Update strategy
strategy = client.strategies.update(
    "strategy_id",
    name="Updated Name"
)

# Delete strategy
client.strategies.delete("strategy_id")
```

### Backtests Resource

```python
# List backtests
backtests = client.backtests.list(strategy_id="strategy_id")

# Get backtest details
backtest = client.backtests.get("backtest_id")

# Run backtest
backtest = client.backtests.run(
    strategy_id="strategy_id",
    start_date="2023-01-01",
    end_date="2023-12-31",
    initial_capital=10000
)

# Get detailed results
results = client.backtests.get_results("backtest_id")

# Delete backtest
client.backtests.delete("backtest_id")
```

### Deployments Resource

```python
# List deployments
deployments = client.deployments.list(status="running")

# Get deployment details
deployment = client.deployments.get("deployment_id")

# Create deployment
deployment = client.deployments.create(
    strategy_id="strategy_id",
    name="My Live Bot",
    broker="alpaca"
)

# Start/stop deployment
client.deployments.start("deployment_id")
client.deployments.stop("deployment_id")

# Get deployment logs
logs = client.deployments.get_logs("deployment_id", level="error")

# Delete deployment
client.deployments.delete("deployment_id")
```

## Token Caching

Tokens are automatically cached in `~/.config/botspot/tokens.json` with:
- **Secure permissions**: 0600 (read/write for owner only)
- **Auto-refresh**: Re-authenticates 5 minutes before expiration
- **Thread-safe**: Lock-based access for concurrent use

```python
# Check cache info
cache_info = client.get_cache_info()
if cache_info:
    print(f"Expires: {cache_info['expires_at']}")
    print(f"Time remaining: {cache_info['time_remaining']}")

# Clear cache (forces re-authentication)
client.clear_cache()
```

## Exception Handling

```python
from botspot_api_class import (
    BotSpot,
    AuthenticationError,
    APIError,
    NetworkError,
    RateLimitError,
    ResourceNotFoundError
)

try:
    client = BotSpot()
    profile = client.users.get_profile()

except AuthenticationError as e:
    print(f"Login failed: {e}")

except ResourceNotFoundError as e:
    print(f"Not found: {e.resource_type} {e.resource_id}")

except RateLimitError as e:
    print(f"Rate limited. Retry after {e.retry_after}s")

except APIError as e:
    print(f"API error: {e.message} (status: {e.status_code})")

except NetworkError as e:
    print(f"Network error: {e}")
```

## Custom Configuration

```python
# Custom credentials (instead of .env)
client = BotSpot(
    username="user@example.com",
    password="secret"
)

# Custom .env path
client = BotSpot(env_path="/path/to/.env")

# Disable token caching
client = BotSpot(cache_tokens=False)

# Visible browser (for debugging)
client = BotSpot(headless=False)
```

## Architecture

```
botspot_api_class/
├── __init__.py              # Public API exports
├── client.py                # Main BotSpot client
├── auth.py                  # Selenium authentication manager
├── token_cache.py           # Token persistence & validation
├── exceptions.py            # Exception hierarchy
├── base.py                  # BaseResource with HTTP methods
└── resources/
    ├── __init__.py
    ├── users.py             # UsersResource
    ├── strategies.py        # StrategiesResource
    ├── backtests.py         # BacktestsResource
    └── deployments.py       # DeploymentsResource
```

## Design Principles

1. **Simplicity**: Stripe-inspired API - minimal code to accomplish tasks
2. **Lazy evaluation**: Only authenticate when needed, not on init
3. **Resource organization**: Logical grouping (users, strategies, etc.)
4. **Error transparency**: Specific exceptions with helpful messages
5. **Performance**: Token caching avoids repeated authentication
6. **Thread safety**: Safe for concurrent use
7. **No surprises**: Sensible defaults, explicit overrides

## Example: From 267 Lines to 30 Lines

**Before (automated_login_test.py - 267 lines):**
```python
# Selenium setup, browser management, localStorage extraction,
# manual API calls with requests, token handling, etc.
```

**After (test_api_client.py - 30 lines):**
```python
from botspot_api_class import BotSpot

with BotSpot() as client:
    profile = client.users.get_profile()
    print(f"Logged in as: {profile['firstName']} {profile['lastName']}")
```

## Logging

The client uses Python's `logging` module with INFO level by default:

```python
import logging

# Customize logging
logging.getLogger("botspot_api_class").setLevel(logging.DEBUG)

# Disable client logging
logging.getLogger("botspot_api_class").setLevel(logging.WARNING)
```

## Testing

See `test_api_client.py` in the `botspot_api_discovery/` directory for a complete example.

## Notes

- This is an **internal project module**, not a redistributable package
- No `setup.py` - just import directly from the project
- Uses root `.env` for credentials
- Works with existing project virtual environment
- Designed for simplicity and maintainability within the Lumibot project

## Version

**1.0.0** - Initial release
