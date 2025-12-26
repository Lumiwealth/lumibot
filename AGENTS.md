# LumiBot Agent Instructions (Theta / Downloader Focus)

These rules are mandatory whenever you work on ThetaData integrations.

1. **Never launch ThetaTerminal locally WITH PRODUCTION CREDENTIALS.** Production has the only licensed session for that account. Starting the jar with prod credentials (even briefly or via Docker) instantly terminates the prod connection and halts all customers.
2. **Use the shared downloader endpoint for backtests.** All tests/backtests must set `DATADOWNLOADER_BASE_URL=http://data-downloader.lumiwealth.com:8080` and `DATADOWNLOADER_API_KEY=<secret>`. Do not short-cut by hitting Theta directly (and avoid hard-coded IPs—they can change on redeploy).

### Dev Credentials for Local ThetaTerminal Testing (SAFE)

There is a **separate dev account** that CAN be used for local debugging without affecting production:

| Field | Value |
|-------|-------|
| Username | `rob-dev@lumiwealth.com` |
| Password | `TestTestTest` |
| Bundle | STOCK.PRO, OPTION.PRO, INDEX.PRO |
| Location | `Strategy Library/Demos/.env` (commented out) |

**Verified working:** Dec 7, 2025

```bash
# Quick test with dev credentials
mkdir -p /tmp/theta-dev-test
echo -e "rob-dev@lumiwealth.com\nTestTestTest" > /tmp/theta-dev-test/creds.txt
java -jar $(python -c "import lumibot; import os; print(os.path.join(os.path.dirname(lumibot.__file__), 'tools', 'ThetaTerminal.jar'))") /tmp/theta-dev-test/creds.txt &
sleep 10
curl "http://127.0.0.1:25510/v2/status"  # Should show CONNECTED
pkill -f "ThetaTerminal.jar"  # Clean up
rm -rf /tmp/theta-dev-test
```

**Use dev credentials ONLY for:** Debugging ThetaTerminal itself, testing API endpoints, investigating data issues.
**Do NOT use for:** Running backtests (always use prod Data Downloader for consistent results).
3. **Respect the queue/backoff contract.** LumiBot no longer enforces a 30 s client timeout; instead it listens for the downloader’s `{"error":"queue_full"}` responses and retries with exponential backoff. If you add new downloader
   integrations, reuse that helper so we never DDoS the server.
4. **Long commands = safe-timeout (20m default max).** Wrap backtests/pytest/stress jobs with `/Users/robertgrzesik/bin/safe-timeout 1200s …` and break work into smaller chunks if it would run longer. Only use longer timeouts when absolutely necessary (e.g., explicit full-window acceptance backtests).
5. **Artifacts.** When demonstrating fixes, capture `Strategy\ Library/logs/*.log`, tear sheets, and downloader stress JSONs so the accuracy/dividend/resilience story stays reproducible.

Failure to follow these rules will break everyone's workflows—double-check env vars before running anything.

---

## Documentation Layout

- `docs/` = hand-authored markdown (architecture, investigations, handoffs, ops notes); start with `docs/BACKTESTING_ARCHITECTURE.md`
- Handoffs: `docs/handoffs/`
- Investigations: `docs/investigations/`
- `docsrc/` = Sphinx source for the public docs site
- `generated-docs/` = local build output from `docsrc/` (gitignored)
- Docs publishing should happen via GitHub Actions on `dev` (avoid committing generated HTML)

---

## Test Philosophy (CRITICAL FOR ALL PROJECTS)

### Test Age = Test Authority

When tests fail, how you fix them depends on **how old the test is**:

| Test Age | Authority Level | How to Fix |
|----------|----------------|------------|
| **>1 year old** | LEGACY - High authority | **Fix the CODE**, not the test. These tests have proven themselves over time. |
| **6-12 months** | ESTABLISHED - Medium authority | Investigate carefully. Likely fix the code, but could be test issue. |
| **<6 months** | NEW - Lower authority | Test may need adjustment. Still verify code isn't broken. |
| **<1 month** | EXPERIMENTAL | Test is still being refined. Adjust as needed. |

### Check Test Age Before Fixing

```bash
git log --format="%ai" --follow -- tests/path/to/test.py | tail -1
```

### Conflict Resolution

When old tests and new tests conflict:
1. **Old test wins by default** - it has proven track record
2. If the new test represents genuinely new functionality, **ask the user for judgment**
3. Document any judgment calls in the test file with comments

This philosophy applies to ALL projects, not just LumiBot.
