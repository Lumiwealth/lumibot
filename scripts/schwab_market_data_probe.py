#!/usr/bin/env python3
"""Read-only Schwab probe for option candle history, crypto, and token refresh.

This is a local LumiBot script. It places no orders. It prints status codes,
symbols, and bar counts only.

Login uses LumiBot's own browser flow. Paste the temporary payload into
schwab_token_probe_payload.json (gitignored) or pass it as SCHWAB_TOKEN.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

os.environ["LUMIBOT_DISABLE_DOTENV"] = "1"
os.environ["IS_BACKTESTING"] = "false"
os.environ["TRADING_BROKER"] = "schwab"
os.environ.pop("LUMIBOT_OAUTH_REFRESH_MODE", None)
os.environ.pop("BOTSPOT_FORCE_BROKER_TOKEN_REFRESH", None)

TOKEN_PATH = REPO_ROOT / "schwab_token_probe.json"
PAYLOAD_PATH = REPO_ROOT / "schwab_token_probe_payload.json"
RESULT_PATH = REPO_ROOT / "docs" / "investigations" / "2026-09-22_schwab-option-crypto-probe-result.json"
ENV_PATH = REPO_ROOT.parent / "botspot_node" / ".env-local"
PROBE_ACCOUNT_PLACEHOLDER = "000"


def _load_app_env() -> None:
    """Load the Schwab app key and secret. Never print their values."""
    if not ENV_PATH.exists():
        raise SystemExit(f"Missing Schwab app env file: {ENV_PATH}")
    wanted = {"SCHWAB_APP_KEY", "SCHWAB_APP_SECRET"}
    for raw_line in ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key not in wanted or os.environ.get(key):
            continue
        os.environ[key] = value.strip().strip('"').strip("'")
    missing = [key for key in sorted(wanted) if not os.environ.get(key)]
    if missing:
        raise SystemExit("Schwab app env is missing: " + ", ".join(missing))


def _redact(text: str) -> str:
    cleaned = text or ""
    for key in ("SCHWAB_APP_SECRET", "SCHWAB_APP_KEY", "SCHWAB_TOKEN"):
        secret = os.environ.get(key)
        if secret:
            cleaned = cleaned.replace(secret, "[redacted]")
    if "access_token" in cleaned or "refresh_token" in cleaned:
        return "[redacted token payload]"
    return cleaned[:240]


class _SecretLogFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        if "access_token" in message or "refresh_token=" in message:
            record.msg = "[redacted token log]"
            record.args = ()
        return True


def _wait_for_pasted_payload(prompt: str = "") -> str:
    if prompt:
        print(prompt, flush=True)
    print("PROBE_WAITING_FOR_PAYLOAD", flush=True)
    print(f"PAYLOAD_FILE {PAYLOAD_PATH}", flush=True)
    deadline = time.time() + 12 * 60
    while time.time() < deadline:
        if PAYLOAD_PATH.exists():
            text = PAYLOAD_PATH.read_text(encoding="utf-8").strip()
            if text:
                PAYLOAD_PATH.unlink(missing_ok=True)
                print("PROBE_PAYLOAD_RECEIVED", flush=True)
                return text
        time.sleep(0.5)
    raise EOFError("Timed out waiting for the Schwab payload.")


def _ensure_token() -> None:
    from lumibot.tools.schwab_helper import SchwabHelper

    existing = os.environ.get("SCHWAB_TOKEN")
    if existing:
        SchwabHelper._save_payload_str_to_token_file(existing, TOKEN_PATH)
        TOKEN_PATH.chmod(0o600)
        os.environ.pop("SCHWAB_TOKEN", None)
        return
    if TOKEN_PATH.exists() and SchwabHelper._is_token_valid_for_schwab_py(TOKEN_PATH):
        print("PROBE_USING_EXISTING_TOKEN_FILE", flush=True)
        return

    import builtins

    builtins.input = _wait_for_pasted_payload
    from lumibot.brokers.schwab import LUMI_DEFAULT_CALLBACK

    api_key = os.environ["SCHWAB_APP_KEY"]
    callback = os.environ.get("SCHWAB_BACKEND_CALLBACK_URL") or LUMI_DEFAULT_CALLBACK
    os.environ["SCHWAB_BACKEND_CALLBACK_URL"] = callback
    print(f"PROBE_CALLBACK_HOST {callback.split('/')[2]}", flush=True)
    ok = SchwabHelper._initiate_schwab_auth_and_get_token_payload(api_key, callback, TOKEN_PATH)
    if not ok:
        raise SystemExit("Schwab login did not produce a token file.")
    TOKEN_PATH.chmod(0o600)


def _response_bits(response) -> dict:
    status = getattr(response, "status_code", None)
    count = None
    detail = ""
    if status == 200:
        try:
            data = response.json()
        except Exception as exc:
            data = None
            detail = type(exc).__name__
        if isinstance(data, dict):
            candles = data.get("candles")
            instruments = data.get("instruments")
            if isinstance(candles, list):
                count = len(candles)
            elif isinstance(instruments, list):
                count = len(instruments)
            elif "callExpDateMap" in data or "putExpDateMap" in data:
                count = len(data.get("callExpDateMap") or {})
        elif isinstance(data, list):
            count = len(data)
    else:
        detail = _redact(getattr(response, "text", "") or "")
    return {"status": status, "count": count, "detail": detail}


def _print_result(name: str, payload: dict) -> None:
    print(
        f"RESULT {name} status={payload.get('status')} count={payload.get('count')} detail={payload.get('detail', '')}",
        flush=True,
    )


def _option_symbol(asset) -> str:
    root_symbol = asset.symbol.ljust(6)
    year_str = asset.expiration.strftime("%y")
    month_str = asset.expiration.strftime("%m")
    day_str = asset.expiration.strftime("%d")
    option_type = "C" if str(asset.right).upper() == "CALL" else "P"
    strike_whole = int(asset.strike)
    strike_decimal = int(round((float(asset.strike) - strike_whole) * 1000))
    return f"{root_symbol}{year_str}{month_str}{day_str}{option_type}{strike_whole:05d}{strike_decimal:03d}"


def _pick_call(chains: dict):
    from datetime import date

    from lumibot.entities import Asset

    calls = (chains or {}).get("Chains", {}).get("CALL", {})
    today = date.today()
    expirations = sorted(exp for exp in calls if date.fromisoformat(exp) >= today)
    if not expirations:
        return None, None
    expiration = expirations[0]
    strikes = calls.get(expiration) or []
    if not strikes:
        return None, None
    strike = float(strikes[len(strikes) // 2])
    asset = Asset(
        "SPY",
        asset_type=Asset.AssetType.OPTION,
        expiration=date.fromisoformat(expiration),
        strike=strike,
        right="CALL",
    )
    return asset, _option_symbol(asset)


def _remember_account_number(broker) -> None:
    """Store the account number in gitignored .env.local. Do not print it."""
    response = broker.client.get_account_numbers()
    status = getattr(response, "status_code", None)
    if status != 200:
        print(f"RESULT account_numbers status={status}", flush=True)
        return
    accounts = response.json() or []
    numbers = [str(item.get("accountNumber")) for item in accounts if item.get("accountNumber")]
    print(f"RESULT account_numbers status=200 count={len(numbers)}", flush=True)
    if len(numbers) != 1:
        return
    env_path = REPO_ROOT / ".env.local"
    existing = env_path.read_text(encoding="utf-8") if env_path.exists() else ""
    if "SCHWAB_ACCOUNT_NUMBER=" in existing:
        return
    with env_path.open("a", encoding="utf-8") as handle:
        if existing and not existing.endswith("\n"):
            handle.write("\n")
        handle.write(f"SCHWAB_ACCOUNT_NUMBER={numbers[0]}\n")
    print("RESULT account_number_saved=true", flush=True)


def _token_has_refresh(path: Path) -> bool:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return False
    token = payload.get("token") if isinstance(payload, dict) else None
    if not isinstance(token, dict):
        token = payload if isinstance(payload, dict) else {}
    return bool(token.get("refresh_token")) and bool(token.get("access_token"))


def _prove_refresh(broker) -> dict:
    token = broker.client.token_metadata.token
    token["expires_at"] = int(time.time()) - 120
    session = broker.client.session
    client = getattr(session, "_client", None)
    if client is not None:
        try:
            client.expires_at = float(token["expires_at"])
        except Exception:
            pass
    before = TOKEN_PATH.stat().st_mtime_ns
    deadline = time.time() + 45
    refreshed = False
    while time.time() < deadline:
        if TOKEN_PATH.stat().st_mtime_ns != before and _token_has_refresh(TOKEN_PATH):
            refreshed = True
            break
        time.sleep(1)
    if not refreshed:
        from lumibot.entities import Asset

        broker.data_source.get_historical_prices(Asset("SPY"), 2, "day")
        refreshed = TOKEN_PATH.stat().st_mtime_ns != before and _token_has_refresh(TOKEN_PATH)
    return {
        "refreshed": refreshed,
        "refresh_token_present": _token_has_refresh(TOKEN_PATH),
        "file_changed": TOKEN_PATH.stat().st_mtime_ns != before,
    }


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    logging.getLogger().addFilter(_SecretLogFilter())
    _load_app_env()
    os.environ["SCHWAB_TOKEN_PATH"] = str(TOKEN_PATH)
    from lumibot.brokers.schwab import LUMI_DEFAULT_CALLBACK, Schwab

    os.environ.setdefault("SCHWAB_BACKEND_CALLBACK_URL", LUMI_DEFAULT_CALLBACK)
    _ensure_token()

    print(
        f"PROBE_PREFLIGHT broker=Schwab backtesting={os.environ.get('IS_BACKTESTING')} "
        f"secret_loaded={bool(os.environ.get('SCHWAB_APP_SECRET'))} "
        f"external_refresh=off token_file={TOKEN_PATH.name}",
        flush=True,
    )

    Schwab._launch_stream = lambda self: None
    broker = Schwab(
        config={
            "SCHWAB_TOKEN_PATH": str(TOKEN_PATH),
            "SCHWAB_ACCOUNT_NUMBER": os.environ.get("SCHWAB_ACCOUNT_NUMBER") or PROBE_ACCOUNT_PLACEHOLDER,
            "SCHWAB_APP_KEY": os.environ["SCHWAB_APP_KEY"],
            "SCHWAB_APP_SECRET": os.environ["SCHWAB_APP_SECRET"],
            "SCHWAB_BACKEND_CALLBACK_URL": os.environ["SCHWAB_BACKEND_CALLBACK_URL"],
            "MARKET": "NASDAQ",
        }
    )
    if broker.client is None:
        raise SystemExit("Schwab client did not initialize.")
    if broker.data_source.client is None:
        broker.data_source.set_client(broker.client)
    _remember_account_number(broker)

    from lumibot.entities import Asset

    results = {}
    spy = Asset("SPY")
    spy_bars = broker.data_source.get_historical_prices(spy, 5, "day")
    spy_count = 0 if spy_bars is None else len(spy_bars.df)
    results["spy_daily"] = {"status": None if spy_bars is None else 200, "count": spy_count, "detail": ""}
    _print_result("spy_daily", results["spy_daily"])

    chains = broker.data_source.get_chains(spy, strike_count=10)
    call_count = len((chains or {}).get("Chains", {}).get("CALL", {}))
    results["spy_option_chain"] = {"status": 200 if call_count else 0, "count": call_count, "detail": ""}
    _print_result("spy_option_chain", results["spy_option_chain"])

    option_asset, option_symbol = _pick_call(chains)
    if option_asset is None:
        results["option_price_history_direct"] = {"status": None, "count": 0, "detail": "no call contract"}
        results["option_price_history_lumibot"] = {"status": None, "count": 0, "detail": "no call contract"}
    else:
        print(f"RESULT option_symbol={option_symbol.strip()}", flush=True)
        end = datetime.now()
        start = end - timedelta(days=10)
        direct = broker.client.get_price_history_every_day(
            option_symbol,
            start_datetime=start,
            end_datetime=end,
        )
        results["option_price_history_direct"] = _response_bits(direct)
        _print_result("option_price_history_direct", results["option_price_history_direct"])

        lumibot_bars = broker.data_source.get_historical_prices(option_asset, 5, "day")
        results["option_price_history_lumibot"] = {
            "status": None if lumibot_bars is None else 200,
            "count": 0 if lumibot_bars is None else len(lumibot_bars.df),
            "detail": "early_return" if lumibot_bars is None else "",
        }
        _print_result("option_price_history_lumibot", results["option_price_history_lumibot"])

    for symbol in ("BTC", "BTC/USD", "BTCUSD"):
        search = broker.client.get_instruments(symbol, broker.client.Instrument.Projection.SYMBOL_SEARCH)
        results[f"crypto_search_{symbol}"] = _response_bits(search)
        _print_result(f"crypto_search_{symbol}", results[f"crypto_search_{symbol}"])
        history = broker.client.get_price_history_every_day(symbol)
        results[f"crypto_history_{symbol}"] = _response_bits(history)
        _print_result(f"crypto_history_{symbol}", results[f"crypto_history_{symbol}"])

    crypto_asset = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    crypto_bars = broker.data_source.get_historical_prices(crypto_asset, 5, "day")
    results["crypto_history_lumibot_btc"] = {
        "status": None if crypto_bars is None else 200,
        "count": 0 if crypto_bars is None else len(crypto_bars.df),
        "detail": "",
    }
    _print_result("crypto_history_lumibot_btc", results["crypto_history_lumibot_btc"])

    refresh = _prove_refresh(broker)
    results["refresh"] = refresh
    print(
        f"RESULT refresh refreshed={refresh['refreshed']} "
        f"refresh_token_present={refresh['refresh_token_present']} "
        f"file_changed={refresh['file_changed']}",
        flush=True,
    )
    RESULT_PATH.parent.mkdir(parents=True, exist_ok=True)
    RESULT_PATH.write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")
    print(f"PROBE_DONE {RESULT_PATH}", flush=True)


if __name__ == "__main__":
    main()
