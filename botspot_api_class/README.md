# BotSpot API Client

Python client library for the BotSpot API - AI-powered trading strategy generation and backtesting platform.

## Features

- 🤖 **AI Strategy Generation** - Generate trading strategies using GPT-5 with natural language prompts
- 📊 **Backtesting** - Run comprehensive backtests on historical data via BotSpot API
- 💾 **Local Execution** - Save and run strategies locally with Lumibot
- 🔐 **Automatic Authentication** - Token caching with auto re-authentication
- ⚡ **Real-time Updates** - Server-Sent Events (SSE) for strategy generation progress
- 📝 **Full Type Hints** - Complete IDE autocomplete support

## Installation

```bash
# Install dependencies
cd /Users/marvin/repos/lumibot
pip install -r requirements_api_class.txt
```

Dependencies:
- `requests>=2.31.0` - HTTP client
- `selenium>=4.15.0` - Browser automation for OAuth
- `python-dotenv>=1.0.0` - Environment variable management
- `webdriver-manager>=4.0.0` - Automatic ChromeDriver management

## Quick Start

### 1. Configure Credentials

Create `.env` file in `/Users/marvin/repos/lumibot/`:

```bash
BOTSPOT_USERNAME=your_email@example.com
BOTSPOT_PASSWORD=your_password
```

### 2. Initialize Client

```python
from botspot_api_class import BotSpot

# Simple usage
client = BotSpot()
profile = client.users.get_profile()
print(f"Welcome, {profile['email']}")

# Or use context manager (recommended)
with BotSpot() as client:
    strategies = client.strategies.list()
    print(f"You have {len(strategies)} strategies")
```

### 3. Generate a Strategy

```python
with BotSpot() as client:
    # Generate with real-time progress
    def on_progress(event):
        if event.get('action') == 'thinking':
            print("AI is thinking...")

    result = client.strategies.generate(
        prompt="Create a simple moving average crossover strategy for SPY",
        progress_callback=on_progress
    )

    print(f"Generated: {result['strategy_name']}")
    print(f"Code: {len(result['generated_code'])} characters")
```

### 4. Save & Run Locally

```python
# Save to local file
filepath = client.strategies.save_to_file(
    code=result['generated_code'],
    filename="sma_crossover",
    output_dir="strategies"
)

print(f"Saved to: {filepath}")
```

## API Reference

### Client Initialization

```python
client = BotSpot(
    username=None,          # Or from BOTSPOT_USERNAME env var
    password=None,          # Or from BOTSPOT_PASSWORD env var
    env_path=None,          # Path to .env file (default: project root)
    cache_tokens=True,      # Enable token caching
    headless=True           # Run browser in headless mode
)
```

### Users Resource

```python
# Get user profile
profile = client.users.get_profile()
# Returns: {"user_id": "...", "email": "...", "firstName": "...", ...}
```

### Strategies Resource

```python
# List all strategies
strategies = client.strategies.list()

# Get strategy versions (includes code, diagram, metadata)
data = client.strategies.get_versions("ai_strategy_id")
code = data['versions'][0]['code_out']
diagram = data['versions'][0]['mermaidDiagram']

# Generate new strategy with AI (takes 2-3 minutes)
result = client.strategies.generate(
    prompt="Create a momentum trading strategy",
    progress_callback=lambda event: print(event.get('content', ''))
)

# Check usage limits (X/500 prompts)
limits = client.strategies.get_usage_limits()
print(f"Prompts used: {limits['promptsUsed']}/{limits['maxPrompts']}")

# Save strategy to local file
filepath = client.strategies.save_to_file(
    code=code,
    filename="my_strategy",      # .py added automatically
    output_dir="strategies",      # Created if doesn't exist
    overwrite=False               # Raises FileExistsError if file exists
)

# Generate Mermaid diagram from code
diagram = client.strategies.generate_diagram(
    python_code=code,
    revision_id="revision_uuid"
)
```

### Backtests Resource

```python
# Submit backtest (returns immediately with backtestId)
# Note: Backtests take 10-30+ minutes to complete
result = client.backtests.run(
    bot_id="ai_strategy_id",
    code=strategy_code,
    start_date="2024-01-01T00:00:00.000Z",
    end_date="2024-12-31T00:00:00.000Z",
    revision_id="revision_uuid",
    data_provider="theta_data",
    requirements="lumibot"
)

backtest_id = result['backtestId']

# Poll for status (call every 2-5 seconds while running)
status = client.backtests.get_status(backtest_id)
print(f"Running: {status['running']}, Stage: {status['stage']}")

# Wait for completion (blocking with optional callback)
final_status = client.backtests.wait_for_completion(
    backtest_id,
    poll_interval=5,        # Check every 5 seconds
    timeout=1800,           # Wait up to 30 minutes
    callback=lambda s: print(f"Stage: {s['stage']}")
)

# Get backtest history for a strategy
stats = client.backtests.get_stats("strategy_id")
print(f"Total backtests: {len(stats['backtests'])}")
```

### Token Management

```python
# Clear cached tokens (forces re-authentication)
client.clear_cache()

# Get cache info
info = client.get_cache_info()
if info:
    print(f"Token expires in: {info['expires_in_seconds']}s")
```

## Complete Workflow Example

```python
from botspot_api_class import BotSpot

with BotSpot() as client:
    # 1. Generate strategy
    print("Generating strategy...")
    result = client.strategies.generate(
        "Create a RSI-based strategy for QQQ"
    )

    # 2. Save locally
    filepath = client.strategies.save_to_file(
        code=result['generated_code'],
        filename="rsi_qqq"
    )
    print(f"Saved to: {filepath}")

    # 3. Get the AI strategy ID
    strategies = client.strategies.list()
    ai_strategy_id = strategies[0]['id']

    # 4. Run backtest via API
    backtest = client.backtests.run(
        bot_id=ai_strategy_id,
        code=result['generated_code'],
        start_date="2024-01-01T00:00:00.000Z",
        end_date="2024-03-31T00:00:00.000Z",
        revision_id="1",
        data_provider="theta_data"
    )

    print(f"Backtest submitted: {backtest['backtestId']}")
```

## Showcase Scripts

The repository includes 8 complete example scripts in the project root:

1. **`api_showcase_getuser.py`** - Get user profile and token info
2. **`api_showcase_logout.py`** - Clear token cache
3. **`api_showcase_generate.py`** - Generate AI strategy with progress tracking
4. **`api_showcase_strategy_results.py`** - View strategy code and metadata
5. **`api_showcase_backtests.py`** - Submit and monitor API backtest
6. **`api_showcase_historical_data.py`** - List strategies and backtest history
7. **`api_showcase_save_and_run.py`** - Save strategy locally and validate
8. **`api_showcase_run_local_backtest.py`** - Run local backtest with Lumibot

Run any script:
```bash
cd /Users/marvin/repos/lumibot
python api_showcase_generate.py
```

## Authentication

The client uses **Selenium-based authentication** with Auth0:

1. On first use, opens Chrome browser (headless by default)
2. Logs in via Auth0 Universal Login
3. Extracts tokens from browser localStorage
4. Caches tokens to `~/.botspot_tokens.json`
5. Auto re-authenticates when tokens expire (~24 hours)

### Token Cache Location

```
~/.botspot_tokens.json
```

Clear cache programmatically:
```python
client.clear_cache()
```

Or delete manually:
```bash
rm ~/.botspot_tokens.json
```

## Error Handling

```python
from botspot_api_class import BotSpot, AuthenticationError, APIError

try:
    with BotSpot() as client:
        result = client.strategies.generate("Create a strategy")
except AuthenticationError as e:
    print(f"Login failed: {e}")
except APIError as e:
    print(f"API error: {e}")
    print(f"Status code: {e.status_code}")
```

## Troubleshooting

### Authentication Issues

**Problem**: "Authentication failed"
- Check credentials in `.env` file
- Ensure Chrome/Chromium is installed
- Try with `headless=False` to see browser:
  ```python
  client = BotSpot(headless=False)
  ```

### Token Expiration

**Problem**: 401 errors after 24 hours
- Client auto re-authenticates automatically
- If issues persist:
  ```python
  client.clear_cache()
  ```

### Backtest Timeouts

**Problem**: Backtests take 10-30+ minutes
- This is normal behavior
- Use `wait_for_completion()` with callback for progress:
  ```python
  client.backtests.wait_for_completion(
      backtest_id,
      callback=lambda s: print(f"Stage: {s['stage']}")
  )
  ```

## API Endpoints Reference

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/users/profile` | GET | Get user profile |
| `/ai-bot-builder/list-strategies` | GET | List all strategies |
| `/ai-bot-builder/list-versions` | GET | Get strategy versions (code, diagram) |
| `/ai-bot-builder/usage-limits` | GET | Check prompt usage (X/500) |
| `/sse/stream` | POST | Generate strategy via SSE (streaming) |
| `/ai-bot-builder/generate-diagram` | POST | Generate Mermaid flowchart |
| `/backtests` | POST | Submit backtest (returns 202) |
| `/backtests/{id}/status` | GET | Poll backtest status |
| `/backtests/{strategyId}/stats` | GET | Get backtest history |
| `/data-providers` | GET | List available data providers |
| `/data-providers/access` | GET | Check provider access |

## Project Structure

```
botspot_api_class/
├── __init__.py               # Public API exports
├── auth.py                   # Selenium-based authentication
├── base.py                   # BaseResource with HTTP methods
├── client.py                 # Main BotSpot client
├── exceptions.py             # Custom exceptions
├── token_cache.py            # Token persistence & validation
├── prompt_cache.py           # Prompt usage tracking
└── resources/
    ├── users.py              # User profile API
    ├── strategies.py         # Strategy generation & management
    └── backtests.py          # Backtesting API
```

## Testing

Run the test suite:
```bash
cd botspot_api_discovery
pytest tests/
```

## Design Principles

1. **Simplicity**: Stripe-inspired API - minimal code to accomplish tasks
2. **Lazy Authentication**: Only authenticates when needed, not on init
3. **Resource Organization**: Logical grouping (users, strategies, backtests)
4. **Error Transparency**: Specific exceptions with helpful messages
5. **Token Caching**: Avoids repeated authentication
6. **Real-time Updates**: SSE for strategy generation progress
7. **Local Execution**: Save and run strategies outside BotSpot

## Notes

- BotSpot API scope: **AI strategy generation + backtesting only**
- No live trading/deployment endpoints (strategies run locally with Lumibot)
- Token expiration: ~24 hours (auto re-authentication enabled)
- Strategy generation: 2-3 minutes via GPT-5
- Backtests: 10-30+ minutes depending on date range
- Prompt limit: 500 prompts per account

## Version

**1.0.0** - Initial release with complete BotSpot API coverage

## Support

- BotSpot Documentation: [https://botspot.trade](https://botspot.trade)
- Lumibot Documentation: [https://lumibot.lumiwealth.com](https://lumibot.lumiwealth.com)
- Email: support@lumiwealth.com
