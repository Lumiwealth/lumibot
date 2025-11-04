# TC-001: Authentication Flow - Complete Summary

## Status: ✅ 5/6 Steps Completed

**Session Date**: November 3, 2025
**Completion**: Steps 01-05 completed successfully. Step 06 (Logout) pending.

---

## 🎯 Objectives Achieved

1. ✅ Successfully authenticated using Auth0 OAuth2 flow
2. ✅ Captured all network traffic during authentication
3. ✅ Extracted access token and ID token from browser storage
4. ✅ Documented 8 API endpoints
5. ✅ Verified 3 endpoints with Python script (100% success rate)

---

## 🔐 Authentication Flow Discovered

### Flow Type: **OAuth 2.0 Authorization Code with PKCE**

```
1. User → Auth0 Login Page
   GET https://botspot.us.auth0.com/u/login

2. User Submits Credentials
   POST /usernamepassword/login (Auth0)

3. Auth0 Redirects with Code
   GET /authorize → Redirect to https://botspot.trade/?code=...

4. Frontend Exchanges Code for Token
   POST https://botspot.us.auth0.com/oauth/token
   Body: {
     client_id, code_verifier, grant_type, code, redirect_uri
   }

5. Receive Tokens
   Response: {
     access_token: JWT (valid 24 hours)
     id_token: JWT
     scope: "openid profile email"
     expires_in: 86400
   }

6. Frontend Makes Authenticated API Calls
   All requests include: Authorization: Bearer {access_token}
```

---

## 📦 Token Storage

**Location**: Browser `localStorage`

**Keys**:
- `@@auth0spajs@@::sys7COPgURwmEVYFi5Wc5U9rXJEsx55d::urn:botspot-prod-api::openid profile email offline_access`
  - Contains: access_token, scope, expires_in, token_type, audience

- `@@auth0spajs@@::sys7COPgURwmEVYFi5Wc5U9rXJEsx55d::@@user@@`
  - Contains: id_token with decoded user claims

**Token Expiration**: 86400 seconds (24 hours)

---

## 🌐 API Endpoints Discovered

### Authentication Endpoints

#### 1. POST /oauth/token
- **Base URL**: https://botspot.us.auth0.com
- **Purpose**: Exchange authorization code for access token
- **Authentication**: client_id + code_verifier (PKCE)
- **Status**: ⚠️ Not verified (Auth0 managed)

---

### BotSpot API Endpoints (https://api.botspot.trade)

#### 2. GET /users/user_profile
- **Purpose**: Get current user profile with subscription and onboarding status
- **Authentication**: Bearer token required
- **Status**: ✅ Verified
- **Response Fields**:
  - User details (id, email, name, phone, location)
  - Trading experience
  - Active products/subscriptions
  - Onboarding status flags (hasCreatedStrategy, hasRunBacktest, etc.)
  - Login statistics

#### 3. GET /users/profile
- **Purpose**: Alternative endpoint for user profile (returns same data as #2)
- **Authentication**: Bearer token required
- **Status**: ✅ Verified

#### 4. POST /auth/ensure-user
- **Purpose**: Ensure user exists in database (called automatically on login)
- **Authentication**: Bearer token required
- **Request Body**: `{"email": "user@example.com"}`
- **Status**: ⚠️ Not verified yet

#### 5. GET /checkout/cart
- **Purpose**: Get user shopping cart
- **Authentication**: Bearer token required
- **Status**: ⚠️ Not verified yet

#### 6. GET /stripe/is-paying-customer
- **Purpose**: Check if user is a paying customer
- **Authentication**: Bearer token required
- **Status**: ⚠️ Not verified yet

#### 7. GET /strategies/onboarding
- **Purpose**: Get onboarding/example strategies for new users
- **Authentication**: Bearer token required (appears optional)
- **Status**: ✅ Verified
- **Response**: Array of strategy objects with:
  - Strategy code (full Python class)
  - Backtest performance data
  - Strategy metadata
  - Mermaid diagrams

#### 8. PUT /users/login-stats
- **Purpose**: Update user login statistics
- **Authentication**: Bearer token required
- **Status**: ⚠️ Not verified yet
- **Note**: Called automatically on each login

---

## 🧪 Verification Results

**Test Script**: `test_api_replication.py`

```
✓ /users/user_profile      - SUCCESS (200 OK)
✓ /users/profile           - SUCCESS (200 OK)
✓ /strategies/onboarding   - SUCCESS (200 OK)

Success Rate: 3/3 (100%)
```

---

## 🔍 Key Discoveries

### Security
- **PKCE Flow**: Uses PKCE (Proof Key for Code Exchange) for enhanced security
- **Token Expiration**: 24-hour access token lifetime
- **Rate Limiting**: Auth0 rate limit: 100 requests (98 remaining after login)
- **CORS**: Properly configured for https://botspot.trade origin

### API Design Patterns
- RESTful endpoints
- Consistent Bearer token authentication
- JSON request/response format
- Proper HTTP status codes
- Detailed user profile with onboarding flags

### User Experience
- **Automatic API Calls on Login**: 7+ endpoints called automatically
- **Onboarding Tracking**: Comprehensive flags for user journey
  - hasCreatedStrategy
  - hasRunBacktest
  - hasDeployedBot
  - hasWatchedVideo
  - hasDismissedOnboarding
- **Subscription Management**: Integrated Stripe payment status checks

---

## 📊 Data Models Identified

### User Profile
```json
{
  "id": "uuid",
  "email": "string",
  "nickname": "string",
  "firstName": "string",
  "lastName": "string",
  "phone": "string",
  "location": "string (e.g., 'CA')",
  "tradingExperience": "string (e.g., 'more than 5 years')",
  "bio": "string",
  "role": "string (e.g., 'user')",
  "activeProducts": [{
    "productId": "uuid",
    "productName": "string",
    "productSlug": "string",
    "isRecurring": "boolean",
    "status": "string (e.g., 'active')",
    "stripeSubscriptionId": "string"
  }],
  "hasSetPassword": "boolean",
  "hasDismissedOnboarding": "boolean",
  "hasDeployedBot": "boolean",
  "hasCreatedStrategy": "boolean",
  "hasRunBacktest": "boolean",
  "hasRunningBot": "boolean",
  "hasWatchedVideo": "boolean",
  "loginCount": "integer",
  "lastLoginAt": "datetime"
}
```

### Strategy Object (Onboarding)
```json
{
  "id": "uuid",
  "name": "string",
  "description": "markdown string",
  "performanceData": {
    "mostRecentBacktest": {
      "id": "uuid",
      "startDate": "string",
      "endDate": "string",
      "cagrAnnualReturn": "string (percentage)",
      "totalReturn": "string (percentage)",
      "maxDrawdown": "string (percentage)",
      "sharpe": "number",
      "completedAt": "datetime",
      "revisionVersion": "integer"
    }
  },
  "type": "string (e.g., 'AI')",
  "assetTypes": ["array of strings"],
  "dataRequirements": ["array of strings"],
  "aiStrategyId": "uuid",
  "latestRevisionId": "uuid",
  "codeOut": "string (full Python code)"
}
```

---

## ⏭️ Next Steps

### Immediate (TC-001-06)
- [ ] Test logout functionality
- [ ] Document logout endpoint
- [ ] Observe token invalidation behavior

### Upcoming Test Cases
- [ ] **TC-002**: Create Strategy Workflow
- [ ] **TC-003**: Strategy Results (view code, download Mermaid)
- [ ] **TC-004**: Backtesting (run with custom dates)
- [ ] **TC-005**: Historical Data (list strategies/backtests)

---

## 📁 Files Generated

1. `botspot_test_plan.json` - Updated with TC-001 observations
2. `botspot_api_endpoints.json` - 8 endpoints documented
3. `test_api_replication.py` - Verified 3 endpoints successfully
4. `TC001_AUTHENTICATION_SUMMARY.md` - This file

---

## 💡 Recommendations

### For API Integration
1. Implement token refresh logic (24-hour expiration)
2. Handle 401 responses (token expiration)
3. Cache user profile data to reduce API calls
4. Monitor rate limits on Auth0 endpoints

### For Documentation
1. Generate OpenAPI 3.0 spec from discovered endpoints
2. Create Postman collection for testing
3. Build Python SDK wrapping these endpoints

---

**Status**: Ready to proceed with TC-002 (Create Strategy Workflow) or complete TC-001-06 (Logout)
