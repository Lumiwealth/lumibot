#!/usr/bin/env python3
"""Read Schwab cash events without starting a strategy or sending orders.

Use an explicit local token path and app environment file. The normal LumiBot
browser login asks for its payload with hidden terminal input. Output contains
counts and transaction shapes, never tokens, account hashes or event amounts.
"""

from __future__ import annotations

import argparse
import builtins
import datetime as dt
import getpass
import json
import logging
import os
import sys
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def configure_environment(app_env: Path) -> None:
    from dotenv import dotenv_values

    values = dotenv_values(app_env)
    for key in ("SCHWAB_APP_KEY", "SCHWAB_APP_SECRET", "SCHWAB_BACKEND_CALLBACK_URL"):
        if values.get(key) and not os.environ.get(key):
            os.environ[key] = values[key]
    if not all(os.environ.get(key) for key in ("SCHWAB_APP_KEY", "SCHWAB_APP_SECRET")):
        raise RuntimeError("The selected app configuration must provide the Schwab app key and secret")
    os.environ.update({
        "LUMIBOT_DISABLE_DOTENV": "true", "IS_BACKTESTING": "false",
        "TRADING_BROKER": "schwab", "LUMIBOT_CONNECT_STREAM": "false",
        "LUMIBOT_START_ORDERS_THREAD": "false", "LUMIBOT_SCHEDULED_EXECUTION": "true",
    })
    for key in ("SCHWAB_TOKEN", "LUMIBOT_OAUTH_REFRESH_MODE", "BOTSPOT_FORCE_BROKER_TOKEN_REFRESH"):
        os.environ.pop(key, None)


def login(token_path: Path) -> None:
    from lumibot.brokers.schwab import LUMI_DEFAULT_CALLBACK
    from lumibot.tools.schwab_helper import SchwabHelper

    token_path.parent.mkdir(parents=True, exist_ok=True)
    # Set the file's permissions before the helper writes authentication data.
    token_path.touch(mode=0o600, exist_ok=True)
    token_path.chmod(0o600)
    old_input = builtins.input
    try:
        builtins.input = getpass.getpass
        ok = SchwabHelper._initiate_schwab_auth_and_get_token_payload(
            os.environ["SCHWAB_APP_KEY"],
            os.environ.get("SCHWAB_BACKEND_CALLBACK_URL") or LUMI_DEFAULT_CALLBACK,
            token_path,
        )
    finally:
        builtins.input = old_input
    if not ok:
        raise RuntimeError("The normal Schwab login did not complete")
    token_path.chmod(0o600)


def inspect_account(broker, account: dict, days: int) -> dict:
    broker.account_number = str(account["accountNumber"])
    broker.hash_value = account["hashValue"]
    types = Counter()
    shapes = set()
    original_get = broker.client.get_transactions

    def observed_get(*args, **kwargs):
        response = original_get(*args, **kwargs)
        if response.status_code == 200:
            payload = response.json()
            if isinstance(payload, list):
                for row in payload:
                    if isinstance(row, dict):
                        types[str(row.get("type"))] += 1
                        shapes.add(tuple(sorted(row)))
        return response

    broker.client.get_transactions = observed_get
    try:
        events = broker.get_cash_events(
            since=dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=days), limit=5000,
        )
        result = {"normalized_count": len(events), "event_types": dict(Counter(e.event_type for e in events))}
    except (ValueError, ConnectionError) as exc:
        # Adapter messages are sanitized; provider bodies are never printed.
        result = {"failure": str(exc), "failure_type": type(exc).__name__}
    finally:
        broker.client.get_transactions = original_get
    return {
        "account_suffix": str(account["accountNumber"])[-4:], "requested_days": days,
        "raw_types": dict(types), "response_field_sets": [list(s) for s in sorted(shapes)], **result,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--token-path", type=Path, required=True)
    parser.add_argument("--app-env", type=Path, required=True)
    parser.add_argument("--account-suffix", help="Read only the uniquely matching account, otherwise all linked accounts")
    parser.add_argument("--days", type=int, default=59, choices=range(1, 60))
    parser.add_argument("--login", action="store_true", help="Run normal browser login before reading")
    args = parser.parse_args()
    logging.disable(logging.CRITICAL)
    configure_environment(args.app_env)
    if args.login:
        login(args.token_path)
    elif not args.token_path.exists():
        raise RuntimeError("No local token exists; run with --login")
    from lumibot.brokers.schwab import Schwab

    broker = None
    try:
        broker = Schwab(config={"SCHWAB_TOKEN_PATH": str(args.token_path), "SCHWAB_ACCOUNT_NUMBER": "unselected"},
                        connect_stream=False)
        response = broker.client.get_account_numbers()
        if response.status_code != 200:
            raise RuntimeError(f"Schwab account discovery failed (HTTP {response.status_code})")
        accounts = response.json()
        if args.account_suffix:
            accounts = [a for a in accounts if str(a["accountNumber"]).endswith(args.account_suffix)]
            if len(accounts) != 1:
                raise RuntimeError("The account suffix must select exactly one authorized account")
        summaries = [inspect_account(broker, account, args.days) for account in accounts]
        print(json.dumps({"accounts": summaries, "mode": "read_only"}, indent=2), flush=True)
        return int(any("failure" in result for result in summaries))
    finally:
        if broker is not None:
            broker.cleanup()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        # Initialization/transport exceptions may contain credential-bearing URLs.
        print(f"Schwab read-only probe failed ({type(exc).__name__})", flush=True)
        raise SystemExit(1) from None
