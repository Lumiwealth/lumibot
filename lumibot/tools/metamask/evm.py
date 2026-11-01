"""Native same-chain EVM balances, MetaMask quotes and managed transactions."""

from __future__ import annotations

import re
import time
from decimal import Decimal
from urllib.parse import urlsplit
from uuid import uuid4

import requests
from eth_utils import keccak

from .models import Binding, SwapIntent, decimal
from .provider import MetaMaskError, address, json_object

PROXY = "https://agentic-proxy.workers.cx.metamask.io/proxy/prd"
ZERO = "0x" + "0" * 40
_NATIVE = {ZERO, "0x" + "e" * 40, "native"}
_TRANSFER = "0x" + keccak(text="Transfer(address,address,uint256)").hex()
_HEX = re.compile(r"0x(?:[0-9a-fA-F]{2})*\Z")


def integer(value):
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise MetaMaskError("INVALID_RESPONSE", "Expected a nonnegative integer")
    try:
        result = int(value, 16) if isinstance(value, str) and value.startswith("0x") else int(value)
    except (ValueError, TypeError):
        raise MetaMaskError("INVALID_RESPONSE", "Expected an integer chain quantity") from None
    if result < 0:
        raise MetaMaskError("INVALID_RESPONSE", "Chain quantity must be an exact nonnegative integer")
    return result


class EvmWallet:
    def __init__(self, journal, provider, config, *, session=None):
        self.journal, self.provider = journal, provider
        self.agent_id, self.network = config["AGENT_ID"], config["NETWORK"]
        self.rpc_urls = dict(config.get("CHAIN_RPC_URLS", {}))
        self.gas_limits = dict(config.get("GAS_LIMITS", {}))
        self.session = session or requests.Session()
        self.session.trust_env = False
        self._owns_session = session is None
        self.epoch = str(uuid4())
        self._execution = journal.execution_lock(str(journal.binding(self.agent_id)["wallet"]))

    def _binding(self, *, execution=False, expected=None):
        record = self.journal.binding(self.agent_id)
        binding = Binding.parse(record)
        if (
            binding.network != self.network
            or record.get("projectId", self.provider.credentials().project_id) != self.provider.credentials().project_id
        ):
            raise MetaMaskError("IDENTITY_MISMATCH", "Wallet binding differs from the configured network or project")
        if execution and (not binding.enabled or binding.expires_at <= int(time.time() * 1000)):
            raise MetaMaskError("REVOKED", "Wallet binding expired or was revoked")
        if expected is not None and record != expected:
            raise MetaMaskError("POLICY_CHANGED", "Wallet authority changed after the quote was prepared")
        self.provider.verify_wallet(binding.wallet)
        if execution and self.provider.policy(binding.wallet).digest != binding.policy_digest:
            raise MetaMaskError("POLICY_CHANGED", "Provider policy differs from the reviewed binding")
        return binding, record

    def _rpc(self, chain, method, params):
        url = self.rpc_urls.get(str(chain), self.rpc_urls.get(chain))
        if not isinstance(url, str):
            raise MetaMaskError("RPC_REQUIRED", "Configure CHAIN_RPC_URLS for every approved EVM chain")
        parsed = urlsplit(url)
        if (
            (
                parsed.scheme != "https"
                and not (parsed.scheme == "http" and parsed.hostname in {"localhost", "127.0.0.1"})
            )
            or parsed.username
            or parsed.password
            or parsed.fragment
        ):
            raise MetaMaskError("INVALID_RPC", "Use an HTTPS chain RPC or explicit local test endpoint")

        def request(name, arguments):
            try:
                response = self.session.request(
                    "POST",
                    url,
                    json={"jsonrpc": "2.0", "id": 1, "method": name, "params": arguments},
                    timeout=(5, 30),
                    allow_redirects=False,
                )
                if response.status_code != 200:
                    raise ValueError("RPC HTTP failure")
                body = json_object(response.json(), "RPC response")
            except (requests.RequestException, ValueError):
                raise MetaMaskError("RPC_UNAVAILABLE", "Configured chain RPC did not return a valid response") from None
            if body.get("id") != 1 or body.get("jsonrpc") != "2.0" or "error" in body or "result" not in body:
                raise MetaMaskError("RPC_ERROR", "Configured chain RPC returned an error")
            return body["result"]

        if integer(request("eth_chainId", [])) != chain:
            raise MetaMaskError("CHAIN_MISMATCH", "RPC network differs from the approved chain")
        return request(method, params)

    def _proxy(self, route, params):
        try:
            response = self.session.request(
                "GET",
                PROXY + route,
                params=params,
                headers={
                    "Authorization": "Bearer " + self.provider.credentials().access_token,
                    "Client-Version": "lumibot-python/1",
                },
                timeout=(5, 45),
                allow_redirects=False,
            )
        except requests.RequestException:
            raise MetaMaskError("QUOTE_UNAVAILABLE", "MetaMask quote or price service is unavailable") from None
        if not 200 <= response.status_code < 300:
            raise MetaMaskError(
                "QUOTE_UNAVAILABLE", f"MetaMask quote or price service returned HTTP {response.status_code}"
            )
        try:
            return response.json()
        except ValueError:
            raise MetaMaskError("INVALID_RESPONSE", "MetaMask returned malformed quote or price data") from None

    @staticmethod
    def _scope(binding, asset):
        if asset.chain_id not in binding.chains or asset.identity not in {item.identity for item in binding.assets}:
            raise MetaMaskError("ASSET_DENIED", "Asset identity is absent from the reviewed wallet registry")

    def price(self, asset, quote=None):
        binding, _ = self._binding()
        self._scope(binding, asset)
        if quote is not None:
            self._scope(binding, quote)
        assets = [asset] + ([quote] if quote is not None else [])
        result = json_object(
            self._proxy(
                "/price/v3/spot-prices",
                {
                    "assetIds": ",".join(item.caip for item in assets),
                    "vsCurrency": "usd",
                    "includeMarketData": "false",
                    "cacheOnly": "false",
                    "useExternalProviders": "false",
                },
            )
        )
        prices = [
            decimal(json_object(result.get(item.caip), "token price").get("usd"), positive=True) for item in assets
        ]
        return prices[0] / prices[1] if quote is not None else prices[0]

    def _balance(self, wallet, asset, block="latest"):
        if asset.address == "native":
            if asset.decimals != 18:
                raise MetaMaskError("NATIVE_DECIMALS_UNSUPPORTED", "Native EVM gas accounting requires 18 decimals")
            atomic = integer(self._rpc(asset.chain_id, "eth_getBalance", [wallet, block]))
        else:
            places = integer(
                self._rpc(asset.chain_id, "eth_call", [{"to": asset.address, "data": "0x313ce567"}, block])
            )
            if places != asset.decimals:
                raise MetaMaskError("TOKEN_DECIMALS_MISMATCH", "Onchain decimals differ from the reviewed registry")
            atomic = integer(
                self._rpc(
                    asset.chain_id,
                    "eth_call",
                    [{"to": asset.address, "data": "0x70a08231" + wallet[2:].rjust(64, "0")}, block],
                )
            )
        return Decimal(atomic).scaleb(-asset.decimals)

    def account(self):
        binding, _ = self._binding()
        positions, equity = [], Decimal(0)
        for asset in binding.assets:
            quantity = self._balance(binding.wallet, asset)
            if quantity == 0:
                continue
            value = quantity * self.price(asset)
            equity += value
            positions.append(
                {
                    "symbol": asset.symbol,
                    "asset": asset.record(),
                    "quantity": format(quantity, "f"),
                    "marketValue": format(value, "f"),
                }
            )
        # USD is the valuation unit, not an invented spendable cash balance.
        return {
            "cash": "0",
            "positionsValue": format(equity, "f"),
            "equity": format(equity, "f"),
            "currency": "USD",
            "positions": positions,
        }

    @staticmethod
    def _quote_asset(raw, expected):
        item = json_object(raw, "quote asset")
        contract = str(item.get("address", "")).lower()
        contract = "native" if contract in _NATIVE else address(contract)
        if contract != expected.address or item.get("decimals") != expected.decimals:
            raise MetaMaskError("QUOTE_MISMATCH", "Quote token differs from the approved contract or decimals")

    def _transaction(self, raw, binding, chain):
        item = json_object(raw, "quote transaction")
        if item.get("chainId") != chain or address(item.get("from")) != binding.wallet:
            raise MetaMaskError("QUOTE_MISMATCH", "Transaction chain or sender differs from the bound wallet")
        target, data = address(item.get("to")), item.get("data")
        if not isinstance(data, str) or not _HEX.fullmatch(data):
            raise MetaMaskError("INVALID_RESPONSE", "Quote calldata is malformed")
        value = integer(item.get("value", "0x0"))
        tx = {"from": binding.wallet, "to": target, "chainId": chain, "data": data, "value": hex(value)}
        quoted = item.get("gasLimit")
        ceiling = quoted if quoted is not None else self.gas_limits.get(str(chain), self.gas_limits.get(chain))
        if ceiling is None or integer(ceiling) <= 0:
            raise MetaMaskError("GAS_LIMIT_REQUIRED", "Quote or explicit GAS_LIMITS must bound each transaction")
        gas = integer(ceiling)
        latest = json_object(self._rpc(chain, "eth_getBlockByNumber", ["latest", False]), "latest chain block")
        base = integer(latest.get("baseFeePerGas"))
        tip = integer(self._rpc(chain, "eth_maxPriorityFeePerGas", []))
        tx.update(gasLimit=hex(gas), maxFeePerGas=hex(2 * base + tip), maxPriorityFeePerGas=hex(tip))
        return tx

    def _quote(self, binding, intent):
        atomic = int(intent.amount.scaleb(intent.source.decimals))
        result = self._proxy(
            "/bridge/getQuote",
            {
                "walletAddress": binding.wallet,
                "destWalletAddress": binding.wallet,
                "srcChainId": intent.source.chain_id,
                "destChainId": intent.destination.chain_id,
                "srcTokenAddress": ZERO if intent.source.address == "native" else intent.source.address,
                "destTokenAddress": ZERO if intent.destination.address == "native" else intent.destination.address,
                "srcTokenAmount": str(atomic),
                "slippage": float(Decimal(intent.slippage_bps) / 100),
                "resetApproval": "false",
                "insufficientBal": "false",
                "gasIncluded": "false",
                "gasIncluded7702": "false",
            },
        )
        if not isinstance(result, list) or not result:
            raise MetaMaskError("NO_ROUTE", "MetaMask returned no same-chain swap route")
        errors = []
        for raw in result:
            try:
                row = json_object(raw, "quote response")
                quote = json_object(row.get("quote"), "quote")
                if (
                    quote.get("srcChainId") != intent.source.chain_id
                    or quote.get("destChainId") != intent.destination.chain_id
                ):
                    raise MetaMaskError("QUOTE_MISMATCH", "Quote crossed the approved chain boundary")
                for key in ("walletAddress", "destWalletAddress"):
                    if quote.get(key) is not None and address(quote[key]) != binding.wallet:
                        raise MetaMaskError("QUOTE_MISMATCH", "Quote redirects the approved wallet")
                if (
                    quote.get("gasIncluded")
                    or quote.get("gasIncluded7702")
                    or quote.get("intent")
                    or quote.get("refuel")
                ):
                    raise MetaMaskError(
                        "ROUTE_UNSUPPORTED", "Gasless, delegated and cross-chain routes require separate qualification"
                    )
                self._quote_asset(quote.get("srcAsset"), intent.source)
                self._quote_asset(quote.get("destAsset"), intent.destination)
                sent = integer(quote.get("srcTokenAmount"))
                fees = json_object(quote.get("feeData"), "quote fees")
                if set(fees) - {"metabridge", "txFee"}:
                    raise MetaMaskError("FEE_UNSUPPORTED", "Quote fee decomposition contains unsupported charges")
                for fee in fees.values():
                    fee = json_object(fee)
                    amount = integer(fee.get("amount"))
                    if amount:
                        self._quote_asset(fee.get("asset"), intent.source)
                        sent += amount
                if sent != atomic:
                    raise MetaMaskError(
                        "QUOTE_MISMATCH", "Quote source amount plus source fees differs from exact input"
                    )
                minimum, output = integer(quote.get("minDestTokenAmount")), integer(quote.get("destTokenAmount"))
                if not 0 < minimum <= output or minimum * 10000 < output * (10000 - intent.slippage_bps):
                    raise MetaMaskError("SLIPPAGE_LIMIT", "Quote minimum output exceeds the allowed slippage")
                quote_id = row.get("quoteId")
                if not isinstance(quote_id, str) or not quote_id:
                    raise MetaMaskError("QUOTE_ID_REQUIRED", "Managed execution requires the provider quote ID")
                tx = self._transaction(row.get("trade"), binding, intent.source.chain_id)
                if integer(tx["value"]) != (atomic if intent.source.address == "native" else 0):
                    raise MetaMaskError("QUOTE_MISMATCH", "Quote transaction attaches unexpected native value")
                approvals = []
                if row.get("approval") is not None:
                    approval = json_object(row["approval"])
                    data = str(approval.get("data", ""))
                    if (
                        intent.source.address == "native"
                        or address(approval.get("to")) != intent.source.address
                        or integer(approval.get("value", "0x0")) != 0
                    ):
                        raise MetaMaskError("APPROVAL_MISMATCH", "Approval targets an unexpected token or native value")
                    if not re.fullmatch(r"0x095ea7b3[0-9a-fA-F]{128}", data):
                        raise MetaMaskError("APPROVAL_MISMATCH", "Only a bounded ERC20 approval is supported")
                    spender = address("0x" + data[34:74])
                    if data[10:34] != "0" * 24 or int(data[74:], 16) < atomic:
                        raise MetaMaskError("APPROVAL_MISMATCH", "Approval encoding or quoted allowance is invalid")
                    allowance = integer(
                        self._rpc(
                            intent.source.chain_id,
                            "eth_call",
                            [
                                {
                                    "to": intent.source.address,
                                    "data": "0xdd62ed3e"
                                    + binding.wallet[2:].rjust(64, "0")
                                    + spender[2:].rjust(64, "0"),
                                },
                                "latest",
                            ],
                        )
                    )
                    if allowance < atomic:
                        base_approval = {**approval, "data": "0x095ea7b3" + spender[2:].rjust(64, "0")}
                        if allowance:
                            approvals.append(
                                self._transaction(
                                    {**base_approval, "data": base_approval["data"] + "0" * 64},
                                    binding,
                                    intent.source.chain_id,
                                )
                            )
                        approvals.append(
                            self._transaction(
                                {**base_approval, "data": base_approval["data"] + format(atomic, "064x")},
                                binding,
                                intent.source.chain_id,
                            )
                        )
                return {
                    "quoteId": quote_id,
                    "trade": tx,
                    "approvals": approvals,
                    "minimumOutput": str(minimum),
                    "quotedAt": int(time.time() * 1000),
                    "expiresAt": int(time.time() * 1000) + 60_000,
                }
            except (MetaMaskError, ValueError) as error:
                errors.append(type(error).__name__)
        raise MetaMaskError(
            "NO_VERIFIABLE_ROUTE", "No quote matched the exact wallet, assets, amount, fees and transaction constraints"
        )

    def _save(self, operation, **changes):
        updated = {**operation, **changes, "updatedAt": int(time.time() * 1000)}
        self.journal.save(updated)
        return updated

    def submit(self, request):
        intent = SwapIntent.parse(request)
        with self._execution:
            prior = self.journal.get(self.agent_id, intent.identifier)
            if prior is not None:
                if prior["request"] != intent.record():
                    raise MetaMaskError("ID_CONFLICT", "Swap ID already belongs to different intent")
                return self._advance(prior)
            binding, record = self._binding(execution=True)
            self._scope(binding, intent.source)
            self._scope(binding, intent.destination)
            if self.network != "mainnet":
                raise MetaMaskError(
                    "SWAP_NETWORK_UNSUPPORTED", "MetaMask does not provide supported testnet swap routes"
                )
            if intent.slippage_bps > binding.max_slippage_bps:
                raise MetaMaskError("SLIPPAGE_LIMIT", "Swap exceeds the reviewed slippage limit")
            if intent.destination.address == "native":
                raise MetaMaskError(
                    "NATIVE_OUTPUT_UNSUPPORTED",
                    "Use the wrapped token; native output requires qualified trace verification",
                )
            quote = self._quote(binding, intent)
            native = next(
                (a for a in binding.assets if a.chain_id == intent.source.chain_id and a.address == "native"), None
            )
            if native is None:
                raise MetaMaskError("ASSET_DENIED", "Register the chain's native gas token before swap execution")
            gas = sum(
                integer(tx["gasLimit"]) * integer(tx["maxFeePerGas"]) for tx in [*quote["approvals"], quote["trade"]]
            )
            reservation = intent.amount * self.price(intent.source) + Decimal(gas).scaleb(
                -native.decimals
            ) * self.price(native)
            if self._balance(binding.wallet, intent.source) < intent.amount or self._balance(
                binding.wallet, native
            ) < Decimal(gas).scaleb(-native.decimals) + (intent.amount if intent.source.address == "native" else 0):
                raise MetaMaskError("INSUFFICIENT_BALANCE", "Wallet cannot cover exact input and bounded gas")
            operation, _ = self.journal.reserve(
                self.agent_id, intent.record(), format(reservation, "f"), expected_binding=record
            )
            self.journal.save_effect(
                operation,
                "swap-plan",
                {
                    **quote,
                    "binding": record,
                    "wallet": binding.wallet,
                    "projectId": self.provider.credentials().project_id,
                    "network": binding.network,
                    "ownerEpoch": self.epoch,
                },
            )
            return self._advance(operation)

    def _receipt(self, tx_hash, expected):
        chain = expected["chainId"]
        receipt = self._rpc(chain, "eth_getTransactionReceipt", [tx_hash])
        if receipt is None:
            return None
        receipt = json_object(receipt, "transaction receipt")
        transaction = json_object(self._rpc(chain, "eth_getTransactionByHash", [tx_hash]), "mined transaction")
        if (
            str(transaction.get("hash", "")).lower() != tx_hash.lower()
            or address(transaction.get("from")) != expected["from"]
            or address(transaction.get("to")) != expected["to"]
            or str(transaction.get("input", "")).lower() != expected["data"].lower()
            or integer(transaction.get("value")) != integer(expected["value"])
            or integer(transaction.get("chainId")) != chain
            or integer(transaction.get("nonce")) != expected["nonce"]
            or str(receipt.get("transactionHash", "")).lower() != tx_hash.lower()
            or receipt.get("blockHash") != transaction.get("blockHash")
        ):
            raise MetaMaskError("TRANSACTION_MISMATCH", "Mined transaction differs from the persisted managed intent")
        if integer(transaction.get("gas")) > integer(expected["gasLimit"]) or integer(
            transaction.get("maxFeePerGas")
        ) > integer(expected["maxFeePerGas"]):
            raise MetaMaskError("GAS_BUDGET_EXCEEDED", "Mined transaction exceeded its frozen gas budget")
        number = integer(receipt.get("blockNumber"))
        block = json_object(self._rpc(chain, "eth_getBlockByNumber", [hex(number), False]), "receipt block")
        if block.get("hash") != receipt.get("blockHash"):
            raise MetaMaskError("REORG_PENDING", "Receipt is not on the current canonical chain")
        if integer(self._rpc(chain, "eth_blockNumber", [])) < number + 1:
            return None
        return receipt

    def _advance(self, operation):
        try:
            return self._advance_inner(operation)
        except MetaMaskError as error:
            current = self.journal.get(self.agent_id, str(operation["id"]))
            if current is None:
                raise
            return self._save(current, state="unknown", lastErrorCode=error.code, message=str(error))

    def _advance_inner(self, operation):
        if operation["state"] in {"filled", "rejected", "canceled"}:
            return operation
        intent = SwapIntent.parse(json_object(operation["request"]))
        plan = self.journal.effect(self.agent_id, str(operation["id"]), "swap-plan")
        if plan is None:
            return self._save(
                operation,
                state="unknown",
                message="Legacy swap needs explicit transaction-hash reconciliation; no replay",
            )
        transactions = [*plan["approvals"], plan["trade"]]
        receipts = []
        for index, tx in enumerate(transactions):
            step = "swap-" + str(index)
            effect = self.journal.effect(self.agent_id, intent.identifier, step)
            if effect is None:
                if plan["ownerEpoch"] != self.epoch:
                    return self._save(
                        operation,
                        state="rejected",
                        message="Interrupted swap was not submitted; existing approvals may remain",
                    )
                try:
                    self._binding(execution=True, expected=plan["binding"])
                    if int(time.time() * 1000) >= integer(plan["expiresAt"]):
                        raise MetaMaskError("QUOTE_EXPIRED", "Swap quote expired before managed submission")
                except MetaMaskError as error:
                    return self._save(operation, state="rejected", message=str(error))
                # Simulate after prior allowances mine, within the frozen gas budget.
                estimate = integer(
                    self._rpc(
                        tx["chainId"],
                        "eth_estimateGas",
                        [
                            {
                                **{k: v for k, v in tx.items() if k not in {"gasLimit", "chainId"}},
                                "gas": tx["gasLimit"],
                                "chainId": hex(tx["chainId"]),
                            }
                        ],
                    )
                )
                if estimate > integer(tx["gasLimit"]):
                    return self._save(
                        operation,
                        state="rejected",
                        message="Simulation exceeds frozen gas ceiling; allowances may remain",
                    )
                tx = {
                    **tx,
                    "nonce": integer(self._rpc(tx["chainId"], "eth_getTransactionCount", [plan["wallet"], "pending"])),
                }
                effect = {
                    "requestId": str(uuid4()),
                    "transaction": tx,
                    "wallet": plan["wallet"],
                    "network": self.network,
                    "projectId": plan["projectId"],
                    "quoteId": plan["quoteId"],
                    "state": "requested",
                    "ownerEpoch": self.epoch,
                }
                self.journal.save_effect(operation, step, effect)
                operation = self._save(
                    operation, state="unknown", message="Managed transaction prepared; reconcile original request ID"
                )
                try:
                    job = self.provider.submit_transaction(
                        effect["requestId"],
                        tx,
                        quote_id=plan["quoteId"] if index == len(transactions) - 1 else None,
                        intent="Same-chain exact-input swap"
                        if index == len(transactions) - 1
                        else "Bounded token allowance",
                    )
                except MetaMaskError:
                    return operation
            else:
                tx = effect["transaction"]
                try:
                    job = self.provider.transaction(effect["requestId"], tx)
                except MetaMaskError:
                    if not effect.get("txHash"):
                        return operation
                    job = {"status": effect["state"]}
            if job.get("txHash"):
                effect = {**effect, "txHash": job["txHash"], "state": job["status"]}
                self.journal.save_effect(operation, step, effect)
                operation = self._save(operation, hashes=list(dict.fromkeys([*operation["hashes"], job["txHash"]])))
            if not effect.get("txHash"):
                if job["status"] in {"DENIED", "EXPIRED"}:
                    return self._save(
                        operation,
                        state="rejected",
                        message="Managed transaction denied or expired; earlier allowances may remain",
                    )
                return self._save(
                    operation,
                    state="awaiting_approval" if job["status"] == "AWAITING_MFA" else "unknown",
                    approvalId=effect["requestId"],
                    message="Managed transaction pending; MetaMask may broadcast after approval",
                )
            try:
                receipt = self._receipt(effect["txHash"], tx)
            except MetaMaskError as error:
                return self._save(operation, state="unknown", lastErrorCode=error.code, message=str(error))
            if receipt is None:
                return self._save(operation, state="submitted")
            receipts.append(receipt)
            if integer(receipt.get("status")) != 1:
                return self._save(
                    operation,
                    state="rejected",
                    message="Transaction reverted; earlier allowances and gas costs may remain",
                )
        # Receipt logs establish net ERC20 output, not the quoted estimate.
        output = Decimal(0)
        if intent.destination.address == "native":
            return self._save(
                operation,
                state="unknown",
                message="Native-output receipt needs supported trace verification; no replacement swap",
            )
        logs = receipts[-1].get("logs")
        if not isinstance(logs, list):
            raise MetaMaskError("INVALID_RESPONSE", "Mined receipt is missing its output transfer logs")
        for raw in logs:
            log = json_object(raw, "receipt log")
            topics = log.get("topics")
            if (
                str(log.get("address", "")).lower() == intent.destination.address
                and isinstance(topics, list)
                and len(topics) == 3
                and str(topics[0]).lower() == _TRANSFER
            ):
                wallet_topic = "0x" + plan["wallet"][2:].rjust(64, "0")
                units = Decimal(integer(log.get("data"))).scaleb(-intent.destination.decimals)
                if str(topics[2]).lower() == wallet_topic:
                    output += units
                if str(topics[1]).lower() == wallet_topic:
                    output -= units
        if output.scaleb(intent.destination.decimals) < integer(plan["minimumOutput"]):
            return self._save(
                operation, state="unknown", message="Mined output does not satisfy the frozen quote minimum"
            )
        fees = [
            {
                "asset": "native",
                "chainId": intent.source.chain_id,
                "atomicAmount": str(
                    sum(integer(row.get("gasUsed")) * integer(row.get("effectiveGasPrice")) for row in receipts)
                ),
            }
        ]
        return self._save(
            operation,
            state="filled",
            filledQuantity=format(output, "f"),
            inputAmount=format(intent.amount, "f"),
            outputAmount=format(output, "f"),
            fees=fees,
            lastErrorCode=None,
            message="Managed swap transaction and net output reconciled",
        )

    def order(self, operation_id):
        with self._execution:
            operation = self.journal.get(self.agent_id, operation_id)
            if operation is None:
                raise MetaMaskError("NOT_FOUND", "Swap operation not found")
            return self._advance(operation)

    def close(self):
        if self._owns_session:
            self.session.close()
