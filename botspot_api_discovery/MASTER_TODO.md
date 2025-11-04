# BotSpot API Discovery - Master To-Do List

**Project Status**: Phase 1-6 Complete ✅ | Phase 7 (Final Documentation) Ready 🔜

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

## ✅ Phase 4: TC-004 - Backtesting (COMPLETED)

### Step 1: Interactive API Discovery (Chrome MCP)
- [x] Navigate to backtest interface
- [x] Select a strategy to backtest
- [x] Configure backtest parameters:
  - [x] Start date
  - [x] End date
  - [x] Data provider (Theta Data)
- [x] Submit backtest request
- [x] Capture POST /backtests API call
- [x] Monitor backtest progress
- [x] Identify progress polling mechanism (GET /backtests/{id}/status)
- [x] Document data provider trial activation flow
- [x] Capture data provider endpoints
- [x] Update botspot_api_endpoints.json
- [x] Update session_notes.md

**Key Discoveries**:
- Backtest submission returns immediately with 202 status and backtestId
- Status polling pattern: UI polls every ~2 seconds while running
- Backtests take 10-30+ minutes depending on date range
- Data provider trial modal prompts before first backtest
- Theta Data 30-day trial with multiple products (stocks, options, indexes)

### Step 2: Testing
- [x] Create test_tc004_backtests.py
- [x] Write test_submit_backtest_with_valid_strategy()
- [x] Write test_get_backtest_status()
- [x] Write test_list_data_providers()
- [x] Write test_get_data_provider_access()
- [x] Write test_get_backtest_stats()
- [x] Run pytest and verify all pass

### Step 3: SDK Implementation
- [x] Review BacktestsResource
- [x] Update run() method with actual parameters (bot_id, code, dates, revision_id, data_provider)
- [x] Add get_status() method for polling
- [x] Add wait_for_completion() helper method (polling with timeout and callback)
- [x] Add get_stats() method for strategy backtest history
- [x] Document all parameters and return values

### Step 4: Showcase Script
- [x] Create api_showcase_backtests.py
- [x] Demonstrate: submit backtest
- [x] Demonstrate: poll for completion (5 times)
- [x] Display real-time status updates (running, stage, elapsed time)
- [x] Clean, informative console output (~120 lines)

---

## ✅ Phase 5: TC-005 - Historical Data (COMPLETED)

### Step 1: Interactive API Discovery (Chrome MCP)
- [x] Navigate to strategies list page
- [x] Capture GET /ai-bot-builder/list-strategies API call
- [x] Document response structure (aiStrategies array with nested strategy objects)
- [x] Observe client-side filtering (no server-side pagination)
- [x] Verify backtest stats endpoint GET /backtests/{strategyId}/stats
- [x] Update botspot_api_endpoints.json
- [x] Update session_notes.md

**Key Discoveries**:
- No new endpoints - TC-005 reuses endpoints from TC-002 and TC-004
- Strategy listing returns all user strategies (no pagination)
- Search/filtering appears to be client-side in UI
- Each AI strategy has nested strategy object with metadata
- Backtest stats endpoint returns all backtests for a strategy

### Step 2: Testing
- [x] Create test_tc005_historical_data.py
- [x] Write test_list_strategies()
- [x] Write test_list_strategies_requires_auth()
- [x] Write test_get_strategy_versions()
- [x] Write test_list_backtest_stats_for_strategy()
- [x] Write test_complete_strategy_history_workflow()
- [x] Run pytest and verify all pass

### Step 3: SDK Implementation
- [x] Verify list() method in StrategiesResource (already documented)
- [x] Verify list() method in BacktestsResource (already documented)
- [x] Add get_stats() method to BacktestsResource
- [x] Document get_stats() parameters and response structure

### Step 4: Showcase Script
- [x] Create api_showcase_historical_data.py
- [x] Demonstrate: list all strategies with metadata
- [x] Demonstrate: fetch specific strategy details (versions, code length)
- [x] Demonstrate: fetch backtest history for strategy
- [x] Clean, informative console output (~90 lines)

---

## ✅ Phase 6: Local Execution & Validation (COMPLETED)

**Note**: Repurposed from "Deployments" after determining BotSpot deployment features are not accessible.

### Step 1: Investigation & Decision
- [x] Navigate to deployment interface - NOT FOUND
- [x] Check if deployment feature is available - NOT ACCESSIBLE
- [x] Determine BotSpot scope: AI generation + backtesting only
- [x] Remove DeploymentsResource from SDK
- [x] Pivot to local execution functionality

**Findings**:
- BotSpot focused on AI strategy generation and backtesting
- No live trading/deployment endpoints in BotSpot API
- Users need ability to save and run strategies locally

### Step 2: SDK Implementation
- [x] Remove DeploymentsResource from client.py
- [x] Add save_to_file() method to StrategiesResource
- [x] Implement file validation and security checks
- [x] Add comprehensive docstrings with examples

### Step 3: Showcase Scripts
- [x] Create api_showcase_save_and_run.py
  - Fetch strategy from BotSpot
  - Save to local file
  - Validate syntax and imports
  - Test import capability
- [x] Create api_showcase_run_local_backtest.py
  - Load strategy from file
  - Run local backtest with Lumibot
  - Display performance results
  - Security warnings and path validation

### Step 4: Security & Validation
- [x] Implement path validation (strategies/ directory only)
- [x] Add security warnings in docstrings
- [x] Add runtime warnings before code execution
- [x] Exception handling for file operations
- [x] Kluster verification with security review

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
| Phase 4: Backtesting | ✅ Complete | TC-004 | backtests | 100% |
| Phase 5: Historical | ✅ Complete | TC-005 | historical_data | 100% |
| Phase 6: Local Execution | ✅ Complete | TC-006 | save_and_run, run_local_backtest | 100% |
| Phase 7: Documentation | ⏳ Pending | N/A | N/A | 0% |

**Overall Project Completion**: ~86% (6/7 phases complete)

---

## 🎯 Next Immediate Steps

1. ✅ Create this MASTER_TODO.md file
2. ✅ Update session_notes.md with workflow pattern
3. ✅ Complete Phase 2: TC-002 Strategy Discovery
4. ✅ Complete Phase 3: TC-003 Strategy Results
5. ✅ Complete Phase 4: TC-004 Backtesting Discovery
6. ✅ Complete Phase 5: TC-005 Historical Data
7. 🔜 **Begin Phase 6: Deployments Discovery** (or skip if not applicable)
8. 🔜 Begin Phase 7: Documentation & Polish

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
5. ✅ api_showcase_backtests.py - Submit and monitor backtest
6. ✅ api_showcase_historical_data.py - List strategies and backtest history
7. ✅ api_showcase_save_and_run.py - Save strategy locally and validate
8. ✅ api_showcase_run_local_backtest.py - Run saved strategy with local backtest

---

**Ready to proceed with Phase 7: Documentation & Polish!** 🚀
