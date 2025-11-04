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
**Status**: Pending

#### Steps:
- [ ] TC-002-01: Navigate to 'Create Strategy' page
- [ ] TC-002-02: Fill strategy form/prompt
- [ ] TC-002-03: Submit strategy generation request
- [ ] TC-002-04: Monitor generation progress
- [ ] TC-002-05: Capture completion notification

**Key Observations**:
(To be filled during session)

**Endpoints Discovered**:
(To be filled during session)

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
(To be completed at end of session)

- **Total Time**: TBD
- **Endpoints Discovered**: 0
- **Endpoints Verified**: 0
- **Test Cases Completed**: 0/5
- **Issues Encountered**: TBD
