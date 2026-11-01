"""In-process Python client for the MetaMask managed-wallet broker."""

from pathlib import Path
from threading import RLock

from lumibot.tools.metamask.models import TokenAsset
from lumibot.tools.metamask.models import decimal as decimal_value
from lumibot.tools.metamask.provider import MetaMaskError as MetaMaskAPIError

__all__ = ["MetaMaskClient", "MetaMaskAPIError", "decimal_value", "token_payload"]


def token_payload(asset):
    if asset.asset_type != "crypto" or getattr(asset, "chain_id", None) is None:
        raise ValueError("Onchain tokens require chain_id, token_address and decimals")
    return TokenAsset.parse(
        {"symbol": asset.symbol, "chainId": asset.chain_id, "address": asset.token_address, "decimals": asset.decimals}
    ).record()


class MetaMaskClient:
    """Native operations. Construction never creates wallets or grants authority.

    Public perpetual reads need only NETWORK. Managed operations require an
    explicit wallet, session, agent and owner-reviewed durable local binding.
    """

    def __init__(self, config):
        self.config = dict(config)
        if self.config.get("BROKER_URL") or self.config.get("BROKER_TOKEN"):
            raise ValueError(
                "MetaMask now runs in Python; replace BROKER_URL/BROKER_TOKEN with SESSION_FILE and STATE_FILE"
            )
        self.network = self.config.get("NETWORK")
        if self.network not in ("testnet", "mainnet"):
            raise ValueError("Set explicit NETWORK='testnet' or 'mainnet'")
        self._lock = RLock()
        self._venue = self._provider = self._tokens = self._journal = self._engine = self._evm = None

    @property
    def venue(self):
        with self._lock:
            if self._venue is None:
                try:
                    from lumibot.tools.metamask.venue import HyperliquidVenue
                except ImportError as error:
                    raise ImportError(
                        "Install Python MetaMask dependencies: pip install 'lumibot[metamask]'"
                    ) from error
                self._venue = HyperliquidVenue(self.network)
            return self._venue

    @property
    def provider(self):
        with self._lock:
            if self._provider is None:
                from lumibot.tools.metamask.auth import TokenManager
                from lumibot.tools.metamask.provider import Credentials, MetaMaskProvider

                path = self.config.get("SESSION_FILE")
                credentials = (
                    Credentials.from_config(self.config)
                    if any(self.config.get(k) for k in ("ACCESS_TOKEN", "CLI_TOKEN"))
                    else None
                )
                legacy = self.config.get("LEGACY_SESSION_FILE")
                if legacy:
                    if credentials is not None or not path:
                        raise ValueError("Legacy migration needs a distinct SESSION_FILE and no inline credentials")
                    from lumibot.tools.metamask.owner import migrate_session

                    self._tokens = migrate_session(legacy, path)
                else:
                    self._tokens = TokenManager(credentials, path=Path(path).expanduser() if path else None)
                provider = MetaMaskProvider(self._tokens)
                project = self.config.get("PROJECT_ID")
                if project is not None and provider.credentials().project_id != project:
                    provider.close()
                    raise MetaMaskAPIError("PROJECT_MISMATCH", "Selected session belongs to another project")
                self._provider = provider
            return self._provider

    @property
    def journal(self):
        with self._lock:
            if self._journal is None:
                from lumibot.tools.metamask.journal import Journal

                path = self.config.get("STATE_FILE")
                if not path:
                    raise ValueError(
                        "STATE_FILE must name the durable wallet journal; reuse existing journal when migrating"
                    )
                self._journal = Journal(Path(path))
            return self._journal

    @property
    def engine(self):
        with self._lock:
            if self._engine is None:
                from lumibot.tools.metamask.engine import ExecutionEngine

                self.verify()
                self._engine = ExecutionEngine(self.journal, self.provider, self.venue, self.config["AGENT_ID"])
            return self._engine

    def verify(self):
        from lumibot.tools.metamask.models import Binding
        from lumibot.tools.metamask.provider import address, identifier

        agent = identifier(self.config.get("AGENT_ID"), "AGENT_ID")
        wallet = address(self.config.get("WALLET_ADDRESS"))
        row = self.journal.binding(agent)
        binding = Binding.parse(row)
        if binding.wallet != wallet or binding.network != self.network:
            raise MetaMaskAPIError("IDENTITY_MISMATCH", "Journal wallet or network differs from configuration")
        if row.get("projectId") is not None and row["projectId"] != self.provider.credentials().project_id:
            raise MetaMaskAPIError("PROJECT_MISMATCH", "Journal belongs to another project")
        self.provider.verify_wallet(wallet)
        return {"agent": agent, "wallet": wallet, "network": self.network}

    def authorize(self, binding):
        """Owner action to persist reviewed limits without changing remote policy.

        Broker startup never renews expiry or broadens authority. Call explicitly
        after reviewing changes to expiry, assets, markets or limits.
        """
        from lumibot.tools.metamask.models import Binding
        from lumibot.tools.metamask.provider import address

        value = dict(binding)
        checked = Binding.parse(value)
        if (
            checked.agent_id != self.config.get("AGENT_ID")
            or checked.wallet != address(self.config.get("WALLET_ADDRESS"))
            or checked.network != self.network
        ):
            raise MetaMaskAPIError("IDENTITY_MISMATCH", "Reviewed binding differs from the selected wallet")
        self.provider.verify_wallet(checked.wallet)
        policy = self.provider.policy(checked.wallet)
        if checked.policy_digest != policy.digest:
            raise MetaMaskAPIError("POLICY_CHANGED", "Review current provider policy before authorizing limits")
        value["projectId"] = self.provider.credentials().project_id
        self.journal.bind(value)
        return self.verify()

    def submit(self, intent):
        if intent.get("kind") == "swap":
            return self.evm.submit(intent)
        return self.engine.submit(intent)

    def orders(self):
        rows = self.journal.operations(self.config["AGENT_ID"])
        return [self.get_order(str(row["id"])) for row in rows]

    def get_order(self, operation_id):
        row = self.journal.get(self.config["AGENT_ID"], operation_id)
        if row is not None and row["request"].get("kind") == "swap":
            return self.evm.order(operation_id)
        return self.engine.order(operation_id)

    def cancel(self, operation_id):
        return self.engine.cancel(operation_id)

    def owner_operations(self):
        self.verify()
        return self.journal.owner_operations(self.config["AGENT_ID"])

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    @property
    def evm(self):
        with self._lock:
            if self._evm is None:
                from lumibot.tools.metamask.evm import EvmWallet

                self.verify()
                self._evm = EvmWallet(self.journal, self.provider, self.config)
            return self._evm

    def account(self, kind):
        identity = self.verify()
        if kind == "spot":
            return self.evm.account()
        return self.venue.account(identity["wallet"])

    def price(self, symbol):
        return self.venue.price(symbol)

    def token_price(self, asset, quote=None):
        return self.evm.price(TokenAsset.parse(asset), TokenAsset.parse(quote) if quote is not None else None)

    def candles(self, symbol, interval, start, end):
        return self.venue.candles(symbol, interval, start, end)

    def funding(self, start_time, end_time, symbol=None):
        identity = self.verify()
        return self.venue.funding(identity["wallet"], start_time, end_time, symbol)

    def close(self):
        with self._lock:
            for component in (self._evm, self._venue, self._provider, self._tokens, self._journal):
                if component is not None:
                    component.close()
