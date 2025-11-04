# BotSpot Auth0 Authentication & API Discovery Plan

## Approach: Authenticate + Reverse Engineer API Endpoints

Since you only have username/password and need to discover available APIs, we'll use a two-phase approach:

---

## Phase 1: Extract Access Token (3 Methods - Try in Order)

### **Method 1: Resource Owner Password Flow** (Fastest if supported)
Try authenticating directly via Auth0 API:
```python
POST https://botspot.us.auth0.com/oauth/token
{
  "grant_type": "password",
  "username": "your_username",
  "password": "your_password",
  "client_id": "sys7COPgURwmEVYFi5Wc5U9rXJEsx55d",  # from your URL
  "scope": "openid profile email",
  "audience": "https://api.botspot.trade"  # might need adjustment
}
```
If this works, you'll get an `access_token` immediately.

### **Method 2: Browser Automation with Token Extraction** (Most reliable)
1. Use Selenium/Playwright to automate login through the web form
2. After successful login, extract token from:
   - `localStorage` (check for `access_token`, `id_token`, `auth0_token`)
   - Cookies (check for session/auth cookies)
   - Network requests (intercept API calls to see token in headers)
3. Save token for API calls

### **Method 3: Manual Token Extraction** (Fallback)
1. Login manually through browser
2. Open DevTools → Application → Local Storage
3. Look for Auth0 tokens (access_token, id_token)
4. Copy token manually
5. Use for testing API calls

---

## Phase 2: Discover API Endpoints

### **Step 1: Inspect Network Traffic**
1. Login to BotSpot web app with DevTools Network tab open
2. Filter by XHR/Fetch requests
3. Document all API calls:
   - Endpoint URLs
   - Request methods (GET/POST/PUT/DELETE)
   - Headers (especially Authorization)
   - Request/response payloads
4. Create endpoint inventory

### **Step 2: Test Endpoints Programmatically**
Create Python script to:
- Use extracted access token
- Call discovered endpoints
- Document responses
- Build API client based on findings

---

## Implementation Structure

```
botspot_auth/
├── .env.example              # Template (USERNAME, PASSWORD)
├── requirements.txt          # selenium/playwright, requests, python-dotenv
├── auth/
│   ├── resource_owner.py    # Method 1: Direct API auth
│   ├── browser_auth.py      # Method 2: Browser automation
│   └── token_manager.py     # Token storage/refresh
├── discovery/
│   ├── api_inspector.py     # Network traffic analyzer
│   └── endpoint_tester.py   # Test discovered endpoints
├── client/
│   └── botspot_api.py       # Final API client (built from discovery)
└── examples/
    ├── 1_authenticate.py    # Get token
    └── 2_discover_apis.py   # Find available endpoints
```

---

## Dependencies
```
requests>=2.27.1
python-dotenv>=0.19.2
selenium>=4.0.0  # OR playwright>=1.40.0
pyjwt[crypto]>=2.6.0
```

---

## Expected Workflow

**Step 1**: Run authentication script
```python
# Tries Method 1, falls back to Method 2 if needed
token = authenticate(username, password)
```

**Step 2**: Use token to explore
```python
# Make authenticated requests to discover endpoints
api_map = discover_endpoints(token, base_url="https://api.botspot.trade")
```

**Step 3**: Build permanent API client
```python
# Create reusable client based on discoveries
client = BotSpotAPI(username, password)
client.get_account_info()
client.get_positions()
# etc.
```

---

## What We'll Build

1. **Flexible auth module** supporting both password flow and browser automation
2. **API discovery tools** to map available endpoints
3. **Token management** with automatic refresh
4. **Documented API client** based on reverse-engineered endpoints
5. **Example scripts** showing usage patterns

The authentication will handle token acquisition automatically, then we'll use browser DevTools + Python to map out all available API endpoints.

---

## Key Information

- **Auth0 Domain**: botspot.us.auth0.com
- **Client ID**: sys7COPgURwmEVYFi5Wc5U9rXJEsx55d (extracted from login URL)
- **Login URL**: https://botspot.us.auth0.com/u/login?state=hKFo2SBpeDZVQjlfcnB2b09jNFlEM0xNblUxdV94bU0ycjRXeaFur3VuaXZlcnNhbC1sb2dpbqN0aWTZIE4tdFN6dFFMNkdNa1ZxR2lyd3prY0swaXFST1pOYjZZo2NpZNkgc3lzN0NPUGdVUndtRVZZRmk1V2M1VTlyWEpFc3g1NWQ
- **Credentials**: Username + Password (from .env)
- **Expected API Base**: https://api.botspot.trade (to be confirmed)

---

## Security Considerations

1. Never commit `.env` file with credentials
2. Use `.env.example` as template (checked into git)
3. Store tokens securely (encrypted at rest if persisting)
4. Implement proper SSL certificate validation
5. Log authentication events (but never log credentials/tokens)
6. Use separate credentials for testing vs production
7. Implement token expiration handling
8. Consider token refresh mechanisms
