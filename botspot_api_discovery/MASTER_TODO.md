# BotSpot API Discovery - Master To-Do List

**Project Status**: Phase 1-3 Complete ✅ | Phase 4 Ready to Start 🔜

**Last Updated**: 2025-11-04

---

## 📊 Project Overview

This document tracks the complete workflow for discovering, testing, and implementing the BotSpot API integration.

**Workflow Pattern** (repeated for each feature):
```
1. Chrome MCP Discovery → 2. Document Findings → 3. Write Tests →
4. Implement/Refine SDK → 5. Create Showcase Script → 6. Verify
```

---

## ✅ Phase 1: Foundation & Authentication (COMPLETED)

### Foundation Setup
- [x] Set up botspot_api_discovery directory structure
- [x] Initialize botspot_test_plan.json with all test cases
- [x] Create botspot_api_endpoints.json template
- [x] Create session_notes.md for documentation
- [x] Create .env.example template
- [x] Connect to Chrome MCP and verify browser access

### TC-001: Authentication Flow
- [x] Step 1: Navigate to login page (Chrome MCP)
- [x] Step 2: Submit credentials
- [x] Step 3: Handle redirect/callback
- [x] Step 4: Extract tokens from storage
- [x] Step 5: Fetch user profile/account data
- [x] Step 6: Logout
- [x] Document all findings in botspot_test_plan.json
- [x] Update botspot_api_endpoints.json with auth endpoints

### API Client Library (botspot_api_class/)
- [x] Create directory structure
- [x] Implement exceptions.py (exception hierarchy)
- [x] Implement token_cache.py (persistent token storage)
- [x] Implement auth.py (Selenium-based authentication)
- [x] Implement base.py (BaseResource with HTTP methods)
- [x] Implement client.py (main BotSpot client)
- [x] Implement resources/users.py
- [x] Implement resources/strategies.py (placeholder)
- [x] Implement resources/backtests.py (placeholder)
- [x] Implement resources/deployments.py (placeholder)
- [x] Implement __init__.py (public API exports)

### Token Management Enhancements
- [x] Add refresh token checking (diagnostic mode)
- [x] Implement auto re-authentication on expiration
- [x] Add visible expiry countdown display
- [x] Add bright yellow TODO reminder for Rob
- [x] Test auto re-auth flow

### Testing & Examples
- [x] Create pytest test suite with venv
- [x] Create requirements_api_class.txt
- [x] Create test_api_client.py (basic test)
- [x] Create api_showcase_getuser.py ✨
- [x] Create api_showcase_logout.py ✨

---

## ✅ Phase 2: TC-002 - Create Strategy Workflow (COMPLETED)

### Step 1: Interactive API Discovery (Chrome MCP)
- [x] Launch Chrome MCP browser session
- [x] Navigate to "Create Strategy" page
- [x] Document page structure and form fields
- [x] Fill strategy form with test data
- [x] Submit strategy creation request
- [x] Capture POST /sse/stream API call (SSE streaming)
- [x] Monitor progress mechanism (SSE with real-time events)
- [x] Wait for strategy generation to complete
- [x] Capture completion response
- [x] Update botspot_api_endpoints.json with discovered endpoints
- [x] Update session_notes.md with workflow observations

**Key Discoveries**:
- Uses Server-Sent Events (SSE) for real-time streaming
- Generation powered by GPT-5 (OpenAI) with medium reasoning effort
- Takes 2-3 minutes on average
- Endpoint: `POST /sse/stream` with type="generate_strategy"

### Step 2: Testing
- [x] Create test_tc002_strategies.py in botspot_api_discovery/tests/
- [x] Write test_list_strategies()
- [x] Write test_get_usage_limits()
- [x] Write test_create_strategy_sse_stream()
- [x] Write test_get_strategy_versions()
- [x] Write test_generate_diagram()
- [x] Run pytest and verify all pass

### Step 3: SDK Implementation
- [x] Review StrategiesResource in botspot_api_class/resources/strategies.py
- [x] Implement generate() method with SSE streaming support
- [x] Implement list() method with actual response format
- [x] Implement get_versions() method
- [x] Implement generate_diagram() method
- [x] Update docstrings with real examples from API
- [x] Add prompt usage tracking with PromptUsageCache

### Step 4: Showcase Script
- [x] Create api_showcase_generate.py in project root
- [x] Demonstrate: generate strategy with SSE progress tracking
- [x] Demonstrate: real-time progress updates
- [x] Display: strategy name, code length, token usage
- [x] Clean, informative console output with emojis

---

## ✅ Phase 3: TC-003 - Strategy Results (COMPLETED)

### Step 1: Interactive API Discovery (Chrome MCP)
- [x] Navigate to strategy results/details page
- [x] Identify which strategy to use (from TC-002)
- [x] Document results data structure
- [x] Capture GET /ai-bot-builder/list-versions API call
- [x] Document code editor UI and diagram modal
- [x] Update botspot_api_endpoints.json
- [x] Update session_notes.md

**Key Discoveries**:
- No separate endpoints for code/diagram - everything from `list-versions`
- Code displayed in inline editor with Copy/Expand/Hide/Save Changes
- Mermaid diagram in modal with zoom controls
- Rating system (1-5 stars) for user feedback
- Refinement section for iterating on strategies

### Step 2: Testing
- [x] Create test_tc003_strategy_results.py
- [x] Write test_get_strategy_versions_structure()
- [x] Write test_get_generated_code()
- [x] Write test_get_mermaid_diagram()
- [x] Write test_strategy_metadata()
- [x] Write test_view_complete_strategy_results() (integration)
- [x] Run pytest and verify all pass

### Step 3: SDK Implementation
- [x] Enhance get_versions() method documentation
- [x] Document complete response structure (code, diagram, metadata)
- [x] Add comprehensive docstring examples
- [x] Mark as primary endpoint for viewing strategy results

### Step 4: Showcase Script
- [x] Create api_showcase_strategy_results.py
- [x] Demonstrate: fetch complete strategy data
- [x] Demonstrate: display code stats and preview
- [x] Demonstrate: show diagram availability
- [x] Demonstrate: display metadata (name, type, visibility)
- [x] Clean, informative console output (~120 lines)

---

## ⏳ Phase 4: TC-004 - Backtesting

### Step 1: Interactive API Discovery (Chrome MCP)
- [ ] Navigate to backtest interface
- [ ] Select a strategy to backtest
- [ ] Configure backtest parameters:
  - [ ] Start date
  - [ ] End date
  - [ ] Initial capital
  - [ ] Other parameters
- [ ] Submit backtest request
- [ ] Capture POST /backtests API call
- [ ] Monitor backtest progress
- [ ] Identify progress polling mechanism
- [ ] Wait for backtest completion
- [ ] View backtest results
- [ ] Capture GET /backtests/{id} API call
- [ ] Capture GET /backtests/{id}/results API call
- [ ] Document results structure (metrics, trades, equity curve)
- [ ] Update botspot_api_endpoints.json
- [ ] Update session_notes.md

### Step 2: Testing
- [ ] Create test_backtests.py
- [ ] Write test_create_backtest()
- [ ] Write test_get_backtest_status()
- [ ] Write test_wait_for_backtest_completion()
- [ ] Write test_get_backtest_results()
- [ ] Write test_list_backtests()
- [ ] Write test_delete_backtest()
- [ ] Run pytest and verify all pass

### Step 3: SDK Implementation
- [ ] Review BacktestsResource
- [ ] Update run() method with actual parameters
- [ ] Add wait_for_completion() helper method (polling)
- [ ] Update get_results() with actual response structure
- [ ] Add methods for equity curve data if available
- [ ] Document all parameters and return values

### Step 4: Showcase Script
- [ ] Create api_showcase_backtests.py
- [ ] Demonstrate: submit backtest
- [ ] Demonstrate: poll for completion
- [ ] Demonstrate: fetch and display results
- [ ] Display key metrics (Sharpe, returns, drawdown, etc.)
- [ ] Keep code minimal (~30-40 lines with polling)

---

## ⏳ Phase 5: TC-005 - Historical Data

### Step 1: Interactive API Discovery (Chrome MCP)
- [ ] Navigate to strategies list page
- [ ] Capture GET /strategies list API call
- [ ] Document pagination parameters
- [ ] Navigate to backtests list page
- [ ] Capture GET /backtests list API call
- [ ] Click on specific strategy
- [ ] Capture GET /strategies/{id} detail call
- [ ] Click on specific backtest
- [ ] Capture GET /backtests/{id} detail call
- [ ] Test filtering/search if available
- [ ] Update botspot_api_endpoints.json
- [ ] Update session_notes.md

### Step 2: Testing
- [ ] Add tests to test_strategies.py and test_backtests.py
- [ ] Write test_list_strategies_pagination()
- [ ] Write test_list_backtests_pagination()
- [ ] Write test_filter_strategies()
- [ ] Write test_search_strategies()
- [ ] Run pytest and verify all pass

### Step 3: SDK Implementation
- [ ] Verify list() methods in StrategiesResource
- [ ] Verify list() methods in BacktestsResource
- [ ] Add pagination support if needed
- [ ] Add filtering/search parameters
- [ ] Document query parameters

### Step 4: Showcase Script
- [ ] Create api_showcase_historical_data.py
- [ ] Demonstrate: list all strategies
- [ ] Demonstrate: list all backtests
- [ ] Demonstrate: fetch specific strategy by ID
- [ ] Demonstrate: fetch specific backtest by ID
- [ ] Keep code minimal (~20-25 lines)

---

## ⏳ Phase 6: Deployments (If Applicable)

### Step 1: Interactive API Discovery (Chrome MCP)
- [ ] Navigate to deployment interface
- [ ] Check if deployment feature is available
- [ ] If available:
  - [ ] Create deployment
  - [ ] Start deployment
  - [ ] Stop deployment
  - [ ] View deployment logs
  - [ ] Capture all relevant API calls
  - [ ] Update botspot_api_endpoints.json
  - [ ] Update session_notes.md
- [ ] If not available: Mark phase as N/A

### Step 2: Testing
- [ ] If deployments available:
  - [ ] Create test_deployments.py
  - [ ] Write deployment lifecycle tests
  - [ ] Run pytest and verify

### Step 3: SDK Implementation
- [ ] If deployments available:
  - [ ] Review DeploymentsResource
  - [ ] Update with actual API behavior
  - [ ] Add log streaming helpers if needed

### Step 4: Showcase Script
- [ ] If deployments available:
  - [ ] Create api_showcase_deployments.py
  - [ ] Demonstrate: create, start, stop, logs
  - [ ] Keep code minimal (~25-30 lines)

---

## ⏳ Phase 7: Documentation & Polish

### OpenAPI Specification
- [ ] Create openapi.yaml in botspot_api_discovery/
- [ ] Document all discovered endpoints
- [ ] Include request/response schemas
- [ ] Add authentication details
- [ ] Include example requests/responses
- [ ] Validate YAML syntax
- [ ] Test in Swagger UI or similar tool

### Comprehensive Documentation
- [ ] Update README.md with complete API overview
- [ ] Document all showcase scripts
- [ ] Add troubleshooting guide
- [ ] Document error codes and handling
- [ ] Add rate limiting information
- [ ] Add best practices guide

### Final Testing
- [ ] Run complete pytest suite
- [ ] Verify all showcase scripts work
- [ ] Test error handling edge cases
- [ ] Test with expired tokens (auto re-auth)
- [ ] Test with invalid credentials
- [ ] Performance testing (if needed)

### Cleanup
- [ ] Remove diagnostic logging from production code
- [ ] Clean up commented-out code
- [ ] Remove FULL TOKEN DATA logging from auth.py
- [ ] Archive discovery session notes
- [ ] Review and clean up temporary test files
- [ ] Final code review

### Prepare for Commit
- [ ] Review all changes
- [ ] Update version numbers if needed
- [ ] Write comprehensive commit message
- [ ] Create pull request (if applicable)
- [ ] Celebrate! 🎉

---

## 📈 Progress Tracking

| Phase | Status | Test Cases | Showcase Scripts | Completion |
|-------|--------|------------|------------------|-----------|
| Phase 1: Foundation & Auth | ✅ Complete | TC-001 | getuser, logout | 100% |
| Phase 2: Strategies | ✅ Complete | TC-002 | generate | 100% |
| Phase 3: Results | ✅ Complete | TC-003 | strategy_results | 100% |
| Phase 4: Backtesting | ⏳ Pending | TC-004 | backtests | 0% |
| Phase 5: Historical | ⏳ Pending | TC-005 | historical_data | 0% |
| Phase 6: Deployments | ⏳ Pending | N/A | deployments | 0% |
| Phase 7: Documentation | ⏳ Pending | N/A | N/A | 0% |

**Overall Project Completion**: ~43% (3/7 phases complete)

---

## 🎯 Next Immediate Steps

1. ✅ Create this MASTER_TODO.md file
2. ✅ Update session_notes.md with workflow pattern
3. ✅ Complete Phase 2: TC-002 Strategy Discovery
4. ✅ Complete Phase 3: TC-003 Strategy Results
5. 🔜 **Begin Phase 4: TC-004 Backtesting Discovery**
6. 🔜 Launch Chrome MCP and navigate to backtest interface

---

## 📝 Notes

### Workflow Pattern (Repeat for Each Phase)
```
┌─────────────────────────────────────────────────────────┐
│ 1. Chrome MCP Discovery (Interactive, exploratory)     │
│    - Use browser to interact with BotSpot UI           │
│    - Capture all network requests                      │
│    - Document UI behavior and flows                    │
└──────────────────┬──────────────────────────────────────┘
                   ↓
┌─────────────────────────────────────────────────────────┐
│ 2. Document Findings                                    │
│    - Update botspot_test_plan.json with observations   │
│    - Update botspot_api_endpoints.json with endpoints  │
│    - Update session_notes.md with workflow details     │
└──────────────────┬──────────────────────────────────────┘
                   ↓
┌─────────────────────────────────────────────────────────┐
│ 3. Write Tests (pytest validates API behavior)         │
│    - Create test_*.py files                            │
│    - Write comprehensive test cases                     │
│    - Verify API behavior matches documentation         │
└──────────────────┬──────────────────────────────────────┘
                   ↓
┌─────────────────────────────────────────────────────────┐
│ 4. Implement/Refine SDK (botspot_api_class)           │
│    - Update resource classes with discovered APIs      │
│    - Add missing methods                               │
│    - Improve docstrings with real examples             │
└──────────────────┬──────────────────────────────────────┘
                   ↓
┌─────────────────────────────────────────────────────────┐
│ 5. Create Showcase Script (minimal, clean example)     │
│    - api_showcase_*.py in project root                 │
│    - Demonstrate feature with ~20-40 lines             │
│    - Clean, informative console output                 │
└──────────────────┬──────────────────────────────────────┘
                   ↓
┌─────────────────────────────────────────────────────────┐
│ 6. Verify & Move to Next Phase                         │
│    - Test showcase script                              │
│    - Update progress tracking                          │
│    - Begin next phase                                  │
└─────────────────────────────────────────────────────────┘
```

### Key Files
- **botspot_test_plan.json** - Tracks all test cases and steps
- **botspot_api_endpoints.json** - Documents all discovered endpoints
- **session_notes.md** - Human-readable observations and notes
- **MASTER_TODO.md** - This file (overall project checklist)

### Showcase Scripts Completed
1. ✅ api_showcase_getuser.py - Get user profile
2. ✅ api_showcase_logout.py - Clear token cache
3. ✅ api_showcase_generate.py - Generate AI strategy with SSE streaming
4. ✅ api_showcase_strategy_results.py - View complete strategy data

### Showcase Scripts To Create
5. ⏳ api_showcase_backtests.py
6. ⏳ api_showcase_historical_data.py
7. ⏳ api_showcase_deployments.py (if applicable)

---

**Ready to proceed with Phase 4: Backtesting!** 🚀
