# LumiBot Agent Instructions (Theta / Downloader Focus)

These rules are mandatory whenever you work on ThetaData integrations.

1. **Never launch ThetaTerminal locally.** Production has the only licensed session. Starting the jar (even briefly or via Docker) instantly terminates the prod connection and halts all customers.
2. **Use the shared downloader endpoint.** All tests/backtests must set `DATADOWNLOADER_BASE_URL=http://44.192.43.146:8080` (or whatever prod IP `/version` reports) and `DATADOWNLOADER_API_KEY=<secret>`. Do not short-cut by hitting Theta
   directly.
3. **Respect the queue/backoff contract.** LumiBot no longer enforces a 30 s client timeout; instead it listens for the downloader’s `{"error":"queue_full"}` responses and retries with exponential backoff. If you add new downloader
   integrations, reuse that helper so we never DDoS the server.
4. **Long commands = safe-timeout.** Wrap backtests/pytest/stress jobs with `/Users/robertgrzesik/bin/safe-timeout <duration> …` to ensure we never spawn orphaned processes.
5. **Artifacts.** When demonstrating fixes, capture `Strategy\ Library/logs/*.log`, tear sheets, and downloader stress JSONs so the accuracy/dividend/resilience story stays reproducible.

Failure to follow these rules will break everyone’s workflows—double-check env vars before running anything.
