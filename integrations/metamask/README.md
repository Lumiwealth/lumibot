# MetaMask broker service

Optional local companion to LumiBot's `MetaMask` broker. Use the Python adapter
and service from the same revision. Not a published standalone package.

Read [setup, architecture, recovery and test requirements](../../docs/METAMASK.md).

```sh
# Node 24 recommended
npm ci --ignore-scripts
npm run build
npm test
npm start
```

The service prints its loopback setup URL and admin-token file location. Keep
`.state/` private and persistent. Default setup is testnet. No credentials,
wallet creation, funding, or transactions are needed for offline tests.

`METAMASK_TEST_PUBLIC_TESTNET=1 npm test` additionally validates live public
Hyperliquid testnet reads. It does not authenticate or trade.
