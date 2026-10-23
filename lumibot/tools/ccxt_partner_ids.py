"""Exchange partner IDs and builder fees for CCXT exchanges.

Why this exists
---------------
The CCXT library attaches *its own* partner (broker) IDs to orders on many
exchanges, so the exchange credits CCXT with the user's volume. On some DEXes
it also adds an *extra fee paid by the user* (a "builder fee") to a CCXT
address, and on Hyperliquid it sets CCXT as the account's referrer the first
time a wallet connects.

LumiBot owns the code that creates every CCXT client, so it takes those
defaults out:

* CCXT's partner IDs are switched OFF unless LumiBot's own IDs are configured.
  A partner ID never changes what the user pays.
* Extra user-paid builder fees are OFF by default, including the CCXT
  referrer. A builder fee is only attached when one is explicitly configured,
  and that is logged because it costs the user extra.

Configuration (all optional, all default to empty)
---------------------------------------------------
``LUMIBOT_CCXT_PARTNER_IDS``
    JSON object: exchange id -> partner ID. KuCoin needs ``{"id": ..., "key": ...}``.
``LUMIBOT_CCXT_BUILDER_FEES``
    JSON object: exchange id -> CCXT option overrides for that exchange's
    builder fee (for example ``{"hyperliquid": {"builder": "0x..",
    "feeRate": "0.01%", "feeInt": 10}}``).
``LUMIBOT_CCXT_REFERRAL_CODES``
    JSON object: exchange id -> referral code (Hyperliquid only today).

How it works
------------
``apply_partner_config(exchange)`` is called wherever LumiBot creates a CCXT
client (live data/broker, backtest data store, CCXT Pro stream). It works on
the client instance only: it sets options, edits instance headers, and wraps
the instance's ``sign`` method to drop CCXT's partner headers (headers are not
part of these exchanges' signatures) and neutral-ise CCXT client-order-ID
prefixes before signing. CCXT itself is not patched.

Where CCXT puts an ID into the client order ID, "off" means the prefix becomes
the neutral string ``lumi`` (just text, not a registered partner code), because
CCXT falls back to its own prefix when the option is empty.

The table below was verified against the pinned CCXT (4.5.64); the test
``tests/test_ccxt_partner_ids.py`` fails if CCXT stops sending an ID we strip.
"""

from __future__ import annotations

import inspect
import json
import logging
import os
import secrets
from dataclasses import dataclass

logger = logging.getLogger(__name__)

ENV_PARTNER_IDS = "LUMIBOT_CCXT_PARTNER_IDS"
ENV_BUILDER_FEES = "LUMIBOT_CCXT_BUILDER_FEES"
ENV_REFERRAL_CODES = "LUMIBOT_CCXT_REFERRAL_CODES"

NEUTRAL_PREFIX = "lumi"
_MARKER = "_lumibot_partner_state"


@dataclass(frozen=True)
class Rule:
    """How one exchange carries CCXT's partner ID.

    kind:
      ``prefix``          option holds a client-order-ID prefix; off -> NEUTRAL_PREFIX
      ``option``          option honours an "off" value (``off``)
      ``header``          request header added in ``sign``; off -> header removed
      ``instance_header`` header stored on ``exchange.headers`` (Gate)
      ``param``           order field equal to ``ccxt_value``; off -> field removed before signing
      ``param_prefix``    client order ID built from a hardcoded prefix; rewritten before signing
      ``unremovable``     CCXT re-adds it inside ``sign`` with a hardcoded default; ours can replace it
    """

    kind: str
    target: str
    option: str | None = None
    ccxt_value: str | None = None
    off: object = None


PARTNER_RULES: dict[str, tuple[Rule, ...]] = {
    "binance": (Rule("prefix", "newClientOrderId", option="broker.*"),),
    "bitget": (Rule("header", "X-CHANNEL-API-CODE", option="broker"),),
    "mexc": (Rule("header", "source", option="broker"),),
    "bingx": (Rule("header", "X-SOURCE-KEY", option="broker"),),
    "htx": (
        Rule("param_prefix", "client-order-id", option="broker.id", ccxt_value="AA03022abc"),
        Rule("unremovable", "channel_code", option="broker.id"),
    ),
    "okx": (Rule("prefix", "clOrdId and tag", option="brokerId"),),
    "bybit": (Rule("header", "Referer", option="brokerId"),),
    "phemex": (Rule("prefix", "clOrdID", option="brokerId"),),
    "woo": (Rule("unremovable", "broker_id", option="brokerId"),),
    "coinbase": (Rule("prefix", "client_order_id", option="brokerId"),),
    "gate": (Rule("instance_header", "X-Gate-Channel-Id"),),
    "kucoin": (Rule("option", "KC-API-PARTNER", option="partner", off={}),),
    "apex": (Rule("param", "brokerId", option="brokerId", ccxt_value="6956"),),
    "backpack": (Rule("header", "X-Broker-Id"),),
    "bitmart": (Rule("header", "X-BM-BROKER-ID", option="brokerId"),),
    "blofin": (Rule("param", "brokerId", option="brokerId", ccxt_value="ec6dd3a7dd982d0b"),),
    "coinbaseinternational": (Rule("prefix", "client_order_id", option="brokerId"),),
    "coinex": (Rule("prefix", "client_id", option="brokerId"),),
    "cryptocom": (Rule("param", "broker_id", option="broker", ccxt_value="CCXT"),),
    "hashkey": (Rule("header", "INPUT-SOURCE", option="broker"),),
    "modetrade": (Rule("unremovable", "order_tag", option="brokerId"),),
    "mudrex": (Rule("option", "Partner-Id", option="broker", off=None),),
    "paradex": (Rule("header", "PARADEX-PARTNER", option="broker"),),
    "weex": (Rule("param_prefix", "newClientOrderId", ccxt_value="b-WEEX111125"),),
    "whitebit": (Rule("option", "clientOrderId", option="brokerId", off=None),),
    "woofipro": (Rule("unremovable", "order_tag", option="brokerId"),),
}

# CCXT option values that switch each exchange's extra user-paid builder fee off.
BUILDER_FEE_DEFAULTS: dict[str, dict] = {
    "hyperliquid": {"builderFee": False, "approvedBuilderFee": False},
    "lighter": {"builderFee": False},
    "grvt": {"builderFee": False},
    "aster": {"builderFee": False},
    "pacifica": {"builderFee": False},
    # Extended ignores builderFee in createOrder; a zero rate and no builder id is the off switch.
    "extended": {"builderFeeRate": "0", "builderId": None},
}

# Exchanges where CCXT registers itself as the account's referrer.
REFERRER_EXCHANGES = ("hyperliquid",)


def _load_json_env(name: str) -> dict:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return {}
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{name} must be a JSON object: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be a JSON object keyed by CCXT exchange id")
    return {str(key).lower(): val for key, val in value.items()}


def _exchange_id(exchange) -> str:
    return str(getattr(exchange, "id", "") or "").lower()


def _set_option(exchange, path: str, value) -> None:
    parts = path.split(".")
    node = exchange.options
    for part in parts[:-1]:
        child = node.get(part)
        if not isinstance(child, dict):
            child = {}
            node[part] = child
        node = child
    last = parts[-1]
    if last == "*":
        for key in list(node):
            node[key] = value
    else:
        node[last] = value


def _ours_option_value(rule: Rule, ours):
    if rule.option == "partner":  # KuCoin signs with a partner key as well as an id
        if not isinstance(ours, dict) or not ours.get("id") or not ours.get("key"):
            raise ValueError(f"{ENV_PARTNER_IDS}: kucoin needs {{\"id\": ..., \"key\": ...}}")
        return {"id": str(ours["id"]), "key": str(ours["key"])}
    if isinstance(ours, dict):
        raise ValueError(f"{ENV_PARTNER_IDS}: expected a string partner id, got an object")
    return str(ours)


def _replace_prefix(value: str, prefix: str, replacement: str | None) -> str:
    if replacement is None:
        replacement = secrets.token_hex(len(prefix))[: len(prefix)]
    return replacement + value[len(prefix):]


def _pre_sign_params(exchange, params):
    """Remove or replace CCXT partner fields before the request is signed."""
    state = getattr(exchange, _MARKER, None)
    if not state or params is None:
        return params
    ours = state["partner_id"]
    rules = [r for r in state["rules"] if r.kind in ("param", "param_prefix")]
    if not rules:
        return params

    def fix(item):
        if not isinstance(item, dict):
            return item
        item = dict(item)
        for rule in rules:
            for key, value in list(item.items()):
                if isinstance(value, list):
                    item[key] = [fix(v) for v in value]
                    continue
                if not isinstance(value, str):
                    continue
                if rule.kind == "param" and key == rule.target and value == rule.ccxt_value:
                    if ours is None:
                        del item[key]
                    else:
                        item[key] = str(ours)
                elif rule.kind == "param_prefix" and value.startswith(rule.ccxt_value):
                    item[key] = _replace_prefix(value, rule.ccxt_value, None if ours is None else str(ours))
        return item

    if isinstance(params, list):
        return [fix(p) for p in params]
    return fix(params)


def _post_sign_headers(exchange, headers):
    """Drop CCXT partner headers (or replace them with ours) after signing."""
    state = getattr(exchange, _MARKER, None)
    if not state or not headers:
        return headers
    ours = state["partner_id"]
    headers = dict(headers)
    for rule in state["rules"]:
        if rule.kind != "header":
            continue
        for name in [h for h in headers if h.lower() == rule.target.lower()]:
            if ours is None:
                del headers[name]
            elif rule.option is None:
                headers[name] = str(ours)
    return {k: v for k, v in headers.items() if v is not None}


def _wrap_sign(exchange) -> None:
    if getattr(exchange, "_lumibot_sign_wrapped", False):
        return
    original = exchange.sign

    def sign(*args, **kwargs):
        args = list(args)
        if len(args) > 3:
            args[3] = _pre_sign_params(exchange, args[3])
        elif "params" in kwargs:
            kwargs["params"] = _pre_sign_params(exchange, kwargs["params"])
        result = original(*args, **kwargs)
        if isinstance(result, dict) and result.get("headers"):
            result["headers"] = _post_sign_headers(exchange, result["headers"])
        return result

    exchange.sign = sign
    exchange._lumibot_sign_wrapped = True


def _noop_like(original, value):
    if inspect.iscoroutinefunction(original):
        async def _async_noop(*args, **kwargs):
            return value
        return _async_noop

    def _noop(*args, **kwargs):
        return value
    return _noop


def _apply_partner_ids(exchange, exchange_id, ours, state) -> None:
    rules = PARTNER_RULES.get(exchange_id, ())
    state["rules"] = rules
    for rule in rules:
        if rule.kind == "prefix":
            _set_option(exchange, rule.option, NEUTRAL_PREFIX if ours is None else _ours_option_value(rule, ours))
        elif rule.kind == "option":
            _set_option(exchange, rule.option, rule.off if ours is None else _ours_option_value(rule, ours))
        elif rule.kind in ("header", "param", "unremovable", "param_prefix"):
            if ours is not None and rule.option:
                _set_option(exchange, rule.option, _ours_option_value(rule, ours))
            if rule.kind == "unremovable" and ours is None:
                state["unremovable"].append(rule.target)
        elif rule.kind == "instance_header":
            headers = dict(getattr(exchange, "headers", None) or {})
            if ours is None:
                headers.pop(rule.target, None)
            else:
                headers[rule.target] = _ours_option_value(rule, ours)
            exchange.headers = headers
    if any(r.kind in ("header", "param", "param_prefix") for r in rules):
        _wrap_sign(exchange)


def _apply_builder_fee(exchange, exchange_id, configured, referral, state) -> None:
    if exchange_id in BUILDER_FEE_DEFAULTS:
        if configured:
            if not isinstance(configured, dict):
                raise ValueError(f"{ENV_BUILDER_FEES}: {exchange_id} needs an object of CCXT options")
            exchange.options.update(configured)
            if exchange_id != "extended":
                exchange.options["builderFee"] = True
            state["builder_fee"] = dict(configured)
            logger.warning(
                "LumiBot builder fee enabled on %s (%s). This costs the user extra on every order.",
                exchange_id, configured,
            )
        else:
            exchange.options.update(BUILDER_FEE_DEFAULTS[exchange_id])
            if exchange_id == "hyperliquid":
                # Even with builderFee False CCXT approves its own builder at 0% and attaches
                # it to every order "for statistics". Skip the approval entirely so no builder
                # is attached (create_order only attaches one after approval).
                original = exchange.handle_builder_fee_approval
                exchange.handle_builder_fee_approval = _noop_like(original, True)
    if exchange_id in REFERRER_EXCHANGES:
        if referral:
            exchange.options["ref"] = str(referral)
            exchange.options["refSet"] = False
            state["referral_code"] = str(referral)
        else:
            exchange.options["refSet"] = True  # never register CCXT1 as the referrer
            original = exchange.set_ref
            exchange.set_ref = _noop_like(original, True)


def apply_partner_config(exchange, partner_ids: dict | None = None, builder_fees: dict | None = None,
                         referral_codes: dict | None = None) -> dict:
    """Apply LumiBot's partner-ID and builder-fee policy to a CCXT client.

    Returns a small description: ``partner_id`` (ours or None), ``builder_fee``
    (ours or None), ``referral_code`` and ``unremovable`` (CCXT fields this
    exchange still sends because CCXT re-adds them inside ``sign``).
    Safe to call more than once.
    """
    exchange_id = _exchange_id(exchange)
    partner_ids = _load_json_env(ENV_PARTNER_IDS) if partner_ids is None else partner_ids
    builder_fees = _load_json_env(ENV_BUILDER_FEES) if builder_fees is None else builder_fees
    referral_codes = _load_json_env(ENV_REFERRAL_CODES) if referral_codes is None else referral_codes

    ours = partner_ids.get(exchange_id) or None
    state = {"exchange": exchange_id, "partner_id": ours, "builder_fee": None,
             "referral_code": None, "unremovable": [], "rules": ()}
    if getattr(exchange, "options", None) is None:
        exchange.options = {}
    _apply_partner_ids(exchange, exchange_id, ours, state)
    _apply_builder_fee(exchange, exchange_id, builder_fees.get(exchange_id), referral_codes.get(exchange_id), state)
    setattr(exchange, _MARKER, state)
    if state["unremovable"]:
        logger.debug("CCXT %s still sends its own %s; CCXT re-adds it while signing",
                     exchange_id, ", ".join(state["unremovable"]))
    return {k: v for k, v in state.items() if k != "rules"}
