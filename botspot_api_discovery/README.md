# BotSpot API Discovery

Systematic discovery and documentation of BotSpot API endpoints using Chrome DevTools MCP and interactive testing methodology.

## 📋 Project Overview

This project documents the complete API surface of BotSpot (https://botspot.trade) through systematic, interactive discovery sessions. Each workflow is captured, tested, and verified.

**Status**: Session 1 Complete (TC-001 ✅)
**Session Date**: November 3, 2025
**Endpoints Discovered**: 8
**Endpoints Verified**: 3

---

## 📁 Project Structure

```
botspot_api_discovery/
├── README.md                           # This file
├── .env.example                        # Environment template
├── requirements.txt                    # Python dependencies
├── pytest.ini                          # Pytest configuration
├── run_tests.sh                        # Test runner script
│
├── botspot_test_plan.json              # Test case tracking (6/23 steps complete)
├── botspot_api_endpoints.json          # Discovered endpoints catalog
├── session_notes.md                    # Human-readable session notes
├── TC001_AUTHENTICATION_SUMMARY.md     # Detailed TC-001 report
│
├── test_api_replication.py             # Quick verification script
│
├── tests/                              # Pytest test suite
│   ├── __init__.py
│   ├── conftest.py                     # Pytest fixtures
│   └── test_tc001_authentication_endpoints.py
│
└── venv/                               # Python virtual environment (gitignored)
```

---

## 🚀 Quick Start

### Prerequisites
- Python 3.10+
- Valid BotSpot account credentials
- Active access token (24-hour expiration)

### Setup

```bash
# 1. Clone or navigate to project
cd botspot_api_discovery

# 2. Copy environment template
cp .env.example .env

# 3. Add your access token to .env
echo "ACCESS_TOKEN=your_token_here" >> .env

# 4. Run tests
./run_tests.sh
```

### Running Tests

```bash
# Run all tests
./run_tests.sh

# Run specific test class
./run_tests.sh -k TestUserProfile

# Run with verbose output
./run_tests.sh -v -s

# Run integration tests only
./run_tests.sh -m integration

# Skip slow tests
./run_tests.sh -m "not slow"
```

---

## 📊 Discovery Progress

### Completed Test Cases

#### ✅ TC-001: Authentication Flow (6/6 steps)
**Status**: Complete
**Endpoints Discovered**: 8
**Endpoints Verified**: 3

Completed steps:
1. ✅ Navigate to login page
2. ✅ Submit credentials
3. ✅ Handle redirect/callback
4. ✅ Extract tokens from storage
5. ✅ Fetch user profile/account data
6. ✅ Logout

**Key Discoveries**:
- OAuth 2.0 with PKCE flow
- 24-hour token expiration
- Client-side logout (no API call)
- 7+ API calls on login
- Comprehensive onboarding tracking

[View Full TC-001 Report →](./TC001_AUTHENTICATION_SUMMARY.md)

---

### Pending Test Cases

#### ⏳ TC-002: Create Strategy Workflow (0/5 steps)
**Next Session**

Steps:
1. Navigate to "Create Strategy" page
2. Fill strategy form/prompt
3. Submit strategy generation request
4. Monitor generation progress
5. Capture completion notification

#### ⏳ TC-003: Strategy Results (0/3 steps)
**Next Session**

Steps:
1. View generated strategy code
2. Download Mermaid diagram/chart
3. Get strategy metadata/details

#### ⏳ TC-004: Backtesting (0/5 steps)
**Next Session**

Steps:
1. Navigate to backtest interface
2. Configure custom date ranges
3. Submit backtest request
4. Monitor backtest execution progress
5. Retrieve/analyze backtest results

#### ⏳ TC-005: Historical Data (0/4 steps)
**Next Session**

Steps:
1. List existing strategies
2. List existing backtests
3. View specific strategy details
4. View specific backtest results

---

## 🔐 Authentication

### OAuth 2.0 Flow

```
1. User → Auth0 Login Page
   https://botspot.us.auth0.com/u/login

2. Credentials Submitted
   POST /usernamepassword/login

3. Authorization Code Received
   Redirect to: https://botspot.trade/?code=...

4. Token Exchange
   POST https://botspot.us.auth0.com/oauth/token

5. Access Token Stored
   localStorage: @@auth0spajs@@::...
   Expires: 24 hours
```

### Using Tokens

All BotSpot API endpoints require Bearer authentication:

```python
import requests

headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"
}

response = requests.get(
    "https://api.botspot.trade/users/user_profile",
    headers=headers
)
```

---

## 🌐 Discovered Endpoints

### Authentication

| Method | Endpoint | Status | Description |
|--------|----------|--------|-------------|
| POST | `/oauth/token` | ⚠️ Auth0 | Exchange code for token |

### User Management

| Method | Endpoint | Status | Description |
|--------|----------|--------|-------------|
| GET | `/users/user_profile` | ✅ Verified | Get user profile & onboarding status |
| GET | `/users/profile` | ✅ Verified | Alternative user profile endpoint |
| POST | `/auth/ensure-user` | 📝 Documented | Ensure user exists in database |
| PUT | `/users/login-stats` | 📝 Documented | Update login statistics |

### Billing & Commerce

| Method | Endpoint | Status | Description |
|--------|----------|--------|-------------|
| GET | `/checkout/cart` | 📝 Documented | Get shopping cart |
| GET | `/stripe/is-paying-customer` | 📝 Documented | Check payment status |

### Strategies

| Method | Endpoint | Status | Description |
|--------|----------|--------|-------------|
| GET | `/strategies/onboarding` | ✅ Verified | Get example strategies |

**Legend**:
- ✅ Verified = Tested with Python script
- 📝 Documented = Discovered via network capture
- ⚠️ Auth0 = External Auth0 endpoint

---

## 🧪 Test Suite

### Test Coverage

```
tests/test_tc001_authentication_endpoints.py
├── TestUserProfile (2 tests)
│   ├── test_get_user_profile          ✅
│   └── test_get_user_profile_alt      ✅
├── TestStrategies (1 test)
│   └── test_get_onboarding_strategies ✅
├── TestAuthentication (2 tests)
│   ├── test_unauthorized_access       ✅
│   └── test_invalid_token             ✅
└── TestEndToEndFlow (1 test)
    └── test_login_flow_simulation     ✅

Total: 6 tests
```

### Running Specific Tests

```bash
# Test user profile endpoints
pytest tests/ -k TestUserProfile

# Test authentication behavior
pytest tests/ -k TestAuthentication

# Test integration flow
pytest tests/ -k integration

# Run with JSON report
pytest tests/ --json-report --json-report-file=report.json
```

---

## 📖 Documentation Files

### Session Documents
- **`TC001_AUTHENTICATION_SUMMARY.md`** - Comprehensive TC-001 report with all findings
- **`session_notes.md`** - Real-time session observations
- **`botspot_test_plan.json`** - Machine-readable test progress (6/23 steps)
- **`botspot_api_endpoints.json`** - Structured endpoint catalog

### Code Examples
- **`test_api_replication.py`** - Quick verification script (3 endpoints)
- **`tests/`** - Full pytest test suite with 6 tests

---

## 🔄 Next Steps

### Session 2 Goals
1. ✅ Complete TC-002: Create Strategy Workflow
   - Discover strategy creation API
   - Document generation progress mechanism
   - Capture completion events

2. ✅ Complete TC-003: Strategy Results
   - View generated code endpoint
   - Download Mermaid diagrams
   - Get strategy metadata

3. ✅ Complete TC-004: Backtesting
   - Backtest submission API
   - Progress monitoring
   - Results retrieval

4. ✅ Complete TC-005: Historical Data
   - List strategies endpoint
   - List backtests endpoint
   - Detail retrieval APIs

### Future Enhancements
- [ ] Generate OpenAPI 3.0 specification
- [ ] Build Python SDK from OpenAPI spec
- [ ] Create Postman collection
- [ ] Add request/response examples to docs
- [ ] Implement token refresh mechanism
- [ ] Add more integration tests

---

## 🛠️ Development

### Adding New Tests

1. Create test file in `tests/` directory:
```python
# tests/test_new_feature.py
import pytest

def test_new_endpoint(api_config, auth_headers):
    # Your test here
    pass
```

2. Run tests:
```bash
./run_tests.sh
```

### Updating Documentation

After each discovery session:
1. Update `botspot_test_plan.json` with progress
2. Add endpoints to `botspot_api_endpoints.json`
3. Create session summary (e.g., `TC002_SUMMARY.md`)
4. Update this README with new findings

---

## 📝 Notes

### Token Management
- Access tokens expire after 24 hours
- Refresh tokens not currently implemented
- Logout is client-side only (token remains valid)

### Rate Limiting
- Auth0: 100 requests per window
- BotSpot API: Limits not yet documented

### CORS
- Configured for `https://botspot.trade` origin
- May need additional configuration for local development

---

## 🤝 Contributing

This is an internal documentation project. When continuing discovery:

1. Follow the systematic approach (announce → action → observe → verify)
2. Update tracking JSON files after each step
3. Create verification scripts for discovered endpoints
4. Add pytest tests for verified endpoints
5. Document findings in session summaries

---

## 📜 License

Internal documentation project for Lumibot/BotSpot integration.

---

## 📞 Contact

Questions about this discovery project? Contact the Lumibot team.

---

**Last Updated**: November 3, 2025
**Session**: 1 of N
**Progress**: 26% (6/23 steps complete)
