# BotSpot API Discovery - Session Notes

## Session Information
- **Start Time**: 2025-11-03 (Phase 1 Completed)
- **Participant (User)**: Marvin
- **Observer (AI)**: Claude Code
- **Objective**: Systematically discover and document all BotSpot API endpoints

---

## 📋 Workflow Pattern (Repeat for Each Phase)

```
1. Chrome MCP Discovery (Interactive)
   - Use browser to interact with BotSpot UI
   - Capture all network requests
   - Document UI behavior and flows
   ↓
2. Document Findings
   - Update botspot_test_plan.json with observations
   - Update botspot_api_endpoints.json with endpoints
   - Update this session_notes.md with workflow details
   ↓
3. Write Tests (pytest validates API)
   - Create test_*.py files
   - Write comprehensive test cases
   - Verify API behavior matches docs
   ↓
4. Implement/Refine SDK (botspot_api_class)
   - Update resource classes
   - Add missing methods
   - Improve docstrings with examples
   ↓
5. Create Showcase Script (minimal example)
   - api_showcase_*.py in project root
   - ~20-40 lines of clean code
   - Informative console output
   ↓
6. Verify & Move to Next Phase
```

---

## Test Case Progress

### ✅ TC-001: Authentication Flow
**Status**: COMPLETED ✓
**Completed**: 2025-11-03

#### Steps:
- [x] TC-001-01: Navigate to login page
- [x] TC-001-02: Submit credentials
- [x] TC-001-03: Handle redirect/callback
- [x] TC-001-04: Extract tokens from storage
- [x] TC-001-05: Fetch user profile/account data
- [x] TC-001-06: Logout

**Key Observations**:
- Uses Auth0 Universal Login with PKCE flow
- Tokens stored in browser localStorage
- Access token valid for 24 hours (86400s)
- No refresh tokens provided (need "Refresh Token Rotation" enabled)
- Multiple API calls on login: user_profile, ensure-user, checkout/cart, login-stats, etc.
- Logout is client-side only (localStorage cleared, no API call)

**Endpoints Discovered**:
- `GET /users/user_profile`
- `GET /users/profile` - Primary user profile endpoint
- `POST /auth/ensure-user`
- `GET /checkout/cart`
- `GET /stripe/is-paying-customer`
- `GET /strategies/onboarding`
- `PUT /users/login-stats`

**SDK Implementation**:
- ✅ Full botspot_api_class library created
- ✅ Token caching with auto re-auth
- ✅ Expiry countdown display
- ✅ UsersResource.get_profile() implemented

**Showcase Scripts**:
- ✅ api_showcase_getuser.py
- ✅ api_showcase_logout.py

**TODO for Rob**:
- ⚠️  Ask Rob to enable "Refresh Token Rotation" in Auth0 for longer sessions (>24h)

---

### TC-002: Create Strategy Workflow
**Status**: COMPLETED ✓
**Completed**: 2025-11-04

#### Steps:
- [x] TC-002-01: Navigate to 'Create Strategy' page
- [x] TC-002-02: Fill strategy form/prompt
- [x] TC-002-03: Submit strategy generation request
- [x] TC-002-04: Monitor generation progress
- [x] TC-002-05: Capture completion notification

**Key Observations**:
- Uses **Server-Sent Events (SSE)** for real-time streaming during generation
- Generation powered by **GPT-5** (OpenAI) with "medium" reasoning effort
- Average generation time: **2-3 minutes**
- UI shows "⏱️ Generation typically takes 2-3 minutes" message
- Form disables during generation to prevent multiple submissions
- Prompt counter updates after submission (2/500 → 3/500)
- Strategies automatically named by AI (e.g., "SMA Crossover")
- Each strategy gets unique `aiStrategyId` and `strategyId` (UUIDs)
- Revisions tracked separately with `revisionId` (UUID)
- Generated code includes full Lumibot strategy class with:
  - `initialize()` method
  - `on_trading_iteration()` method
  - Backtesting configuration
  - Live trading setup
  - Comprehensive comments and logging
- Mermaid diagram auto-generated from code (separate POST request)
- Success UI includes options: "START TRADING" and "Run Backtest"
- User feedback system (1-5 stars rating) for each generation
- "Prompt History" shows all refinement attempts

**UI Structure**:
- Multiline textbox for natural language strategy description
- "Attach files" button (for additional context/data)
- "Generate Strategy" button (disabled when empty)
- Prompt counter: "X/500 prompts" (usage tracking)
- Existing strategies list with:
  - Strategy name
  - Status badge (e.g., "Active")
  - Creation time
  - Revision count
  - "Use" button

**SSE Event Flow**:
1. `prompt_to_ai` (phase: "sending") - Prompt sent to AI
2. `thinking` (phase: "code_generation") - Processing started
3. `code_generation_started` - Generation begins
4. `:heartbeat` messages (keep-alive during long operation)
5. `code_generation_completed` - Code finished
6. `validation_started` (phase: "validation") - Validating code
7. `strategy_generated` (phase: "complete") - **Final event with full code**

**Token Usage Example**:
```json
{
  "model": "gpt-5",
  "provider": "openai",
  "reasoning_effort": "medium",
  "input_tokens": 24070,
  "output_tokens": 4932,
  "total_tokens": 29002
}
```

**Endpoints Discovered**:
- `POST /sse/stream` - Generate strategy via SSE (streaming)
- `GET /ai-bot-builder/list-strategies` - List user's strategies
- `GET /ai-bot-builder/usage-limits` - Check remaining prompts (X/500)
- `POST /ai-bot-builder/generate-diagram` - Generate Mermaid flowchart
- `GET /ai-bot-builder/list-versions?aiStrategyId={id}` - Get strategy versions
- `GET /ai-bot-builder/feedback?revisionId={id}` - Get user feedback/rating
- `GET /marketplace/check-published/{strategyId}` - Check marketplace publication status

**Generated Strategy Example**:
- Prompt: "Create a simple moving average crossover strategy..."
- Output: Full `SMACrossoverStrategy` class (~200 lines)
- Includes: SMA20/SMA50 crossover logic, buy/sell signals, chart markers
- Backtesting: YahooDataBacktesting with $10,000 budget
- Live trading: Broker-agnostic setup

---

### TC-003: Strategy Results
**Status**: Pending

#### Steps:
- [ ] TC-003-01: View generated strategy code
- [ ] TC-003-02: Download Mermaid diagram/chart
- [ ] TC-003-03: Get strategy metadata/details

**Key Observations**:
(To be filled during session)

**Endpoints Discovered**:
(To be filled during session)

---

### TC-004: Backtesting
**Status**: Pending

#### Steps:
- [ ] TC-004-01: Navigate to backtest interface
- [ ] TC-004-02: Configure custom date ranges
- [ ] TC-004-03: Submit backtest request
- [ ] TC-004-04: Monitor backtest execution progress
- [ ] TC-004-05: Retrieve/analyze backtest results

**Key Observations**:
(To be filled during session)

**Endpoints Discovered**:
(To be filled during session)

---

### TC-005: Historical Data
**Status**: Pending

#### Steps:
- [ ] TC-005-01: List existing strategies
- [ ] TC-005-02: List existing backtests
- [ ] TC-005-03: View specific strategy details
- [ ] TC-005-04: View specific backtest results

**Key Observations**:
(To be filled during session)

**Endpoints Discovered**:
(To be filled during session)

---

## General Observations

### API Patterns
(To be filled as patterns emerge)

### Authentication & Authorization
(To be documented during TC-001)

### Data Models
(To be documented as we discover entity structures)

### Error Handling
(To be documented when we encounter errors)

### Rate Limiting
(To be noted if we observe any rate limiting)

---

## Questions & Anomalies
(To be filled during session)

---

## Session Summary

- **Total Time**: Phase 1: ~2 hours, Phase 2: ~2 hours
- **Endpoints Discovered**: 15 total (8 from TC-001, 7 from TC-002)
- **Endpoints Verified**: 8
- **Test Cases Completed**: 2/5 (TC-001 ✓, TC-002 ✓)
- **Issues Encountered**: None - smooth execution
- **Key Achievement**: Successfully discovered SSE-based strategy generation system
- **Next Phase**: TC-003 (Strategy Results - viewing code, diagrams, metadata)
