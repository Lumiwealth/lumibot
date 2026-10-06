Promote the exact LumiBot 4.6.4 Bot Manager artifact set qualified in Development. Production resolves the immutable manifest for the develop merge second parent, copies and verifies its runtime/Lambda bytes and ECR image digests, and does not rebuild source artifacts.

Local qualification passed 1822/1822 tests, static/format checks, audit, package builds, promotion contracts and read-only Terraform validation. Development deployment, all deployed integrations, manifest/version readback and Production Readiness must be green before this merge.

Rob explicitly authorized this Production deployment in the release request. Verify a new owner-account production backtest completes and settings.json.lumibot_version is 4.6.4, then check MCP account, history, live bot inventory and saved trade-artifact queries. No customer-owned bot restart or account mutation is part of this release.
