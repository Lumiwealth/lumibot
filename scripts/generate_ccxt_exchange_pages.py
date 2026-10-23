#!/usr/bin/env python3
"""Render one documentation page per CCXT exchange.

Inputs (all under ``docsrc/_data/``):

* ``ccxt_exchange_facts.json``: facts read from CCXT metadata by
  ``scripts/generate_ccxt_exchange_facts.py``. Never edited by hand.
* ``ccxt_exchange_notes.json``: hand-checked facts from each exchange's own
  official pages (sign-up steps, API key steps, permissions), each with its
  source and date. An exchange is rendered only when it has notes, so no page
  ships with nothing but template text.
* ``ccxt_exchange_affiliates.json``: our own referral links. Empty means the
  sign-up link goes to the plain official site. CCXT's ``urls.referral`` links
  are never used.

Output: ``docsrc/brokers.ccxt.<id>.rst``. These files are generated; edit the
JSON inputs or this template instead of the pages.

Rules baked into the template (Rob, 2026-10-23): plain English, no em dashes,
no profit claims, no country or region labels, and no "tested by us" or
"community supported" labels.

Usage::

    python scripts/generate_ccxt_exchange_pages.py            # every exchange with notes
    python scripts/generate_ccxt_exchange_pages.py phemex bybit
"""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "docsrc" / "_data"
DOCSRC = ROOT / "docsrc"

CREDENTIAL_ENV_SUFFIX = {
    "apiKey": "API_KEY",
    "secret": "API_SECRET",
    "password": "API_PASSPHRASE",
    "uid": "UID",
    "accountId": "ACCOUNT_ID",
    "login": "LOGIN",
    "twofa": "TWOFA",
    "privateKey": "PRIVATE_KEY",
    "walletAddress": "WALLET_ADDRESS",
    "token": "TOKEN",
}
CREDENTIAL_LABEL = {
    "apiKey": "API key",
    "secret": "API secret",
    "password": "API passphrase (CCXT calls it ``password``)",
    "uid": "user id",
    "accountId": "account id",
    "login": "login",
    "twofa": "two-factor secret",
    "privateKey": "private key",
    "walletAddress": "wallet address",
    "token": "token",
}
MARKET_LABEL = {
    "spot": "Spot",
    "margin": "Margin",
    "swap": "Perpetual swaps",
    "future": "Dated futures",
    "option": "Options",
}
ORDER_METHODS = [
    ("createMarketOrder", "market orders"),
    ("createLimitOrder", "limit orders"),
    ("createStopOrder", "stop orders"),
    ("createStopLossOrder", "stop-loss orders"),
    ("createTakeProfitOrder", "take-profit orders"),
    ("createReduceOnlyOrder", "reduce-only orders"),
    ("editOrder", "editing open orders"),
    ("cancelOrder", "canceling orders"),
]
STREAMS = [
    ("watchTicker", "prices"),
    ("watchOrderBook", "order book"),
    ("watchTrades", "trades"),
    ("watchOHLCV", "candles"),
    ("watchOrders", "your orders"),
    ("watchBalance", "your balance"),
    ("watchPositions", "your positions"),
]


def load(name):
    return json.loads((DATA / name).read_text(encoding="utf-8"))


def yes_no(value):
    if value is True:
        return "Yes"
    if value == "emulated":
        return "Yes (emulated by CCXT)"
    if value is False:
        return "No"
    return "Not listed by CCXT"


def heading(text, char):
    return f"{text}\n{char * len(text)}\n"


def bullets(items, indent=""):
    return "\n".join(f"{indent}* {item}" for item in items)


def numbered(items):
    return "\n".join(f"#. {item}" for item in items)


def code(lang, body, indent="   "):
    lines = body.strip("\n").splitlines()
    return f".. code-block:: {lang}\n\n" + "\n".join(f"{indent}{line}" if line else "" for line in lines) + "\n"


def env_name(exchange_id, credential):
    return f"{exchange_id.upper()}_{CREDENTIAL_ENV_SUFFIX.get(credential, credential.upper())}"


def config_block(fx, quote, sandbox):
    name = fx["id"].upper()
    lines = ["import os", "", f"{name}_CONFIG = {{", f'    "exchange_id": "{fx["id"]}",']
    for credential in fx["required_credentials"]:
        lines.append(f'    "{credential}": os.environ.get("{env_name(fx["id"], credential)}"),')
    if sandbox:
        lines.append('    "sandbox": True,  # testnet first; set False for your live account')
    else:
        lines.append('    "sandbox": False,  # this exchange has no CCXT sandbox')
    lines.append("}")
    return "\n".join(lines)


def env_block(fx, sandbox):
    """The .env lines LumiBot reads when TRADING_BROKER is a CCXT exchange id."""
    lines = [f"TRADING_BROKER={fx['id']}"]
    lines += [f"{env_name(fx['id'], c)}=your_{c}" for c in fx["required_credentials"]]
    if sandbox:
        lines.append(f"{fx['id'].upper()}_SANDBOX=true   # testnet; remove this line for your live account")
    return "\n".join(lines)


def main_block(cls, quote):
    return f'''if __name__ == "__main__":
    # LumiBot reads TRADING_BROKER and the keys from your .env file.
    strategy = {cls}(quote_asset=Asset("{quote}", asset_type=Asset.AssetType.CRYPTO))
    strategy.run_live()
'''


def regular_example(fx, quote, sandbox):
    cls = "BuyBitcoinOn" + "".join(part.capitalize() for part in fx["id"].split("_"))
    return f'''from lumibot.entities import Asset
from lumibot.strategies import Strategy


class {cls}(Strategy):
    """Buy a small amount of BTC once, then hold it."""

    def initialize(self):
        self.sleeptime = "1H"
        self.set_market("24/7")  # crypto trades all day, every day

    def on_trading_iteration(self):
        btc = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
        if self.get_position(btc) is not None:
            return  # already holding BTC
        price = self.get_last_price(btc, quote=self.quote_asset)
        if price:
            quantity = round(50 / price, 6)  # about 50 {quote} worth
            order = self.create_order(btc, quantity, "buy", quote=self.quote_asset)
            self.submit_order(order)


{main_block(cls, quote)}'''


def agent_example(fx, display, quote, sandbox):
    cls = "".join(part.capitalize() for part in fx["id"].split("_")) + "AITrader"
    return f'''from lumibot.entities import Asset
from lumibot.strategies import Strategy


class {cls}(Strategy):
    """One AI agent researches the market and places the trades itself."""

    def initialize(self):
        self.sleeptime = "4H"
        self.set_market("24/7")
        self.agents.create(
            name="trader",
            model="openai/gpt-6-luna",
            reasoning_effort="high",
            allow_trading=True,
            system_prompt=(
                "You manage a small crypto account on {display}. You may trade BTC and ETH "
                "against {quote} (asset_type 'crypto', quote_symbol '{quote}'), or hold cash. "
                "Before every order, call account_portfolio, account_positions and "
                "market_last_price for that exact symbol in the same run. "
                "Keep each order under 10% of the account value. "
                "Explain each decision in one short paragraph."
            ),
        )

    def on_trading_iteration(self):
        self.agents["trader"].run(
            task_prompt=(
                "Review recent BTC/{quote} and ETH/{quote} prices and your current holdings, "
                "then decide whether to buy, sell or hold."
            ),
            context={{"exchange": "{display}", "quote": "{quote}", "date": self.get_datetime().isoformat()}},
        )


{main_block(cls, quote)}'''


def related_pages(exchange_id, facts_by_id, limit=4):
    existing = sorted(p.stem.split("brokers.ccxt.", 1)[1] for p in DOCSRC.glob("brokers.ccxt.*.rst"))
    mine = facts_by_id[exchange_id]["markets"]
    scored = []
    for other in existing:
        if other == exchange_id or other not in facts_by_id:
            continue
        theirs = facts_by_id[other]["markets"]
        score = sum(1 for k in mine if bool(mine[k]) and bool(theirs.get(k)))
        scored.append((-score, other))
    return [other for _, other in sorted(scored)[:limit]]


def render(exchange_id, facts_by_id, notes, affiliates, meta):
    fx = facts_by_id[exchange_id]
    nt = notes[exchange_id]
    display = fx["name"]
    quote = nt.get("quote_symbol", "USDT")
    sandbox = bool(fx["sandbox"])
    aff = affiliates["exchanges"].get(exchange_id, {})
    referral = (aff.get("referral_url") or "").strip()
    signup_url = referral or nt.get("signup_url") or fx["urls"]["www"]
    markets_yes = [MARKET_LABEL[k] for k, v in fx["markets"].items() if v]
    markets_text = ", ".join(markets_yes) if markets_yes else "the markets CCXT lists"
    creds = fx["required_credentials"]
    doc_url = (fx["urls"]["doc"] or [None])[0]
    fees_url = (fx["urls"]["fees"] or [None])[0]
    rate = fx["rate_limit_ms"]
    streams = [label for method, label in STREAMS if fx["streams"].get(method) in (True, "emulated")]
    orders = [label for method, label in ORDER_METHODS if fx["methods"].get(method) in (True, "emulated")]
    perps = bool(fx["markets"].get("swap"))

    title = f"{display} Trading Bot in Python: AI Agent and Algorithmic Trading with LumiBot"
    description = (
        f"Build a {display} trading bot in Python with LumiBot and CCXT. Create {display} API keys, "
        f"connect them, and run a regular or AI agent strategy."
    )

    out = []
    out.append(f".. Generated by scripts/generate_ccxt_exchange_pages.py from docsrc/_data/. Edit the data, not this page.\n")
    out.append(heading(title, "="))
    out.append(f".. meta::\n   :description: {description}\n   :keywords: {display} trading bot, {display} API, {display} Python, agentic trading {display}, AI trading bot {display}, algorithmic trading {display}, ccxt {fx['id']}\n")
    out.append(
        f"This guide shows how to build a {display} trading bot in Python with LumiBot. "
        f"LumiBot connects to {display} through CCXT, the open source library that talks to more than 100 crypto exchanges. "
        f"You will create a {display} account and API key, connect it, and run two examples: "
        f"a simple algorithmic trading bot and an agentic trading bot where an AI agent makes the calls.\n"
    )

    # At a glance
    rows = [
        ("CCXT exchange id", f"``{fx['id']}``"),
        ("Markets", markets_text),
        ("Testnet (sandbox)", "Yes" if sandbox else "No"),
        ("Credentials", ", ".join(CREDENTIAL_LABEL.get(c, c) for c in creds) or "None listed by CCXT"),
        ("Live data streams", ", ".join(streams) if streams else "None listed; LumiBot uses REST"),
        ("CCXT request spacing", f"about {rate:g} ms between requests" if isinstance(rate, (int, float)) else "Not listed"),
        ("Official site", fx["urls"]["www"]),
    ]
    if doc_url:
        rows.append(("API docs", doc_url))
    if fees_url:
        rows.append(("Fees", fees_url))
    out.append(heading(f"{display} at a glance", "-"))
    table = [".. list-table::", "   :widths: 35 65", ""]
    for key, value in rows:
        table.append(f"   * - {key}")
        table.append(f"     - {value}")
    out.append("\n".join(table) + "\n")

    out.append(heading(f"What is {display}?", "-"))
    out.append(nt["summary"] + "\n")

    out.append(heading(f"Sign up for {display}", "-"))
    out.append(numbered(nt["signup_steps"]) + "\n")
    out.append(f"`Open a {display} account <{signup_url}>`_\n")
    if referral:
        out.append(f"*{affiliates['disclosure']}*\n")

    out.append(heading(f"Create a {display} API key", "-"))
    out.append(numbered(nt["api_key_steps"]) + "\n")
    out.append(f"**Which permissions to turn on:** {nt['permissions']}\n")
    out.append(
        ".. warning::\n\n"
        f"   Never turn on withdrawals for a {display} trading bot key. Bind the key to your server's IP address when "
        f"{display} lets you, keep the secret out of your code and out of Git, and delete the key if it ever leaks.\n"
    )

    out.append(heading(f"Connect {display} to LumiBot", "-"))
    out.append(
        f"The simplest way: put these lines in a ``.env`` file next to your strategy. LumiBot loads the file, sees "
        f"``TRADING_BROKER={fx['id']}`` and connects to {display} for you, so your strategy needs no broker code.\n"
    )
    out.append(code("bash", env_block(fx, sandbox)))
    out.append(
        f"LumiBot reads exactly the fields CCXT needs for {display}"
        + (f" ({', '.join('``' + c + '``' for c in creds)})" if creds else "")
        + ". If one is missing, LumiBot stops at start-up and names the variable you still need.\n"
    )
    out.append(
        f"You can also build the broker in code. This is handy when one script talks to more than one exchange. "
        f"The keys in the config are CCXT's own credential names for {display}:\n"
    )
    out.append(code("python", config_block(fx, quote, sandbox) + "\n\nfrom lumibot.brokers import Ccxt\n\nbroker = Ccxt(" + fx["id"].upper() + "_CONFIG)\n# then: MyStrategy(broker=broker, ...)"))
    out.append("Either way, LumiBot turns on CCXT's built-in rate limiting for you.\n")

    out.append(heading(f"Practice on the {display} testnet" if sandbox else f"Testing on {display}", "-"))
    out.append(nt["testnet_note"] + "\n")
    if sandbox:
        tests = ", ".join(fx["urls"]["test"][:2])
        out.append(f"CCXT sends sandbox requests to: {tests}. Testnet and live accounts use different keys.\n")

    out.append(heading(f"Algorithmic trading example: a simple {display} bot", "-"))
    out.append(
        f"This bot buys about 50 {quote} of bitcoin on {display} once and then holds it. "
        "It is a starting point to check that your keys, balances and orders work.\n"
    )
    out.append(code("python", regular_example(fx, quote, sandbox)))

    out.append(heading(f"Agentic trading example: an AI agent that trades on {display}", "-"))
    out.append(
        f"Agentic trading means an AI agent reads the market and decides what to do. In this example one LumiBot agent "
        f"looks at prices and your {display} account, then places the orders itself. There is no Python logic in between: "
        "you set the rules in plain English in the prompt, such as which coins it may trade and how big each order can be. "
        "This follows the same pattern as LumiBot's AI trading team examples.\n"
    )
    out.append(code("python", agent_example(fx, display, quote, sandbox)))
    out.append(
        "The agent needs a model key such as ``OPENAI_API_KEY`` in your environment. See the "
        ":doc:`AI agents quickstart <agents_quickstart>` for models, tools and memory.\n"
    )

    out.append(heading(f"What you can trade on {display} through CCXT", "-"))
    market_rows = [".. list-table::", "   :header-rows: 1", "   :widths: 40 60", "", "   * - Market", "     - Listed by CCXT"]
    for key, label in MARKET_LABEL.items():
        market_rows += [f"   * - {label}", f"     - {yes_no(fx['markets'].get(key))}"]
    out.append("\n".join(market_rows) + "\n")
    if orders:
        out.append(f"CCXT lists these order features for {display}: {', '.join(orders)}.\n")
    if perps:
        out.append(
            "For perpetual contracts, use the full CCXT symbol so the settlement currency is clear, for example "
            "``BTC/USDT:USDT``. See :doc:`the CCXT guide <brokers.ccxt>` for contract quantities, position modes and leverage.\n"
        )

    if nt.get("backtesting"):
        out.append(heading(f"Backtesting with {display} data", "-"))
        out.append(f"``{fx['id']}`` is a documented CCXT backtesting exchange id in LumiBot:\n")
        out.append(code("bash", f"export BACKTESTING_DATA_SOURCE={fx['id']}"))
        out.append("Backtests use historical candles through the CCXT data path. Live trading results can differ from a backtest.\n")

    if nt.get("extra_notes"):
        out.append(heading(f"{display} tips", "-"))
        out.append(bullets(nt["extra_notes"]) + "\n")

    out.append(heading("Frequently asked questions", "-"))
    faq = [
        (f"Can I build a {display} trading bot in Python?",
         f"Yes. Install LumiBot (``pip install lumibot``), set your {display} keys and use the ``{fx['id']}`` CCXT exchange id as shown above."),
        (f"Does {display} have a testnet?",
         (f"Yes. Set ``{fx['id'].upper()}_SANDBOX=true`` and use keys from the {display} testnet." if sandbox
          else f"CCXT does not list a testnet for {display}. Leave ``{fx['id'].upper()}_SANDBOX`` unset and start with a small amount.")),
        (f"Which API permissions does a {display} bot need?", nt["permissions"]),
        (f"Can an AI agent trade on {display} by itself?",
         "Yes. The agentic example above gives one agent permission to trade. You control what it may trade and how much through its prompt."),
        (f"Can I trade {display} perpetual futures with LumiBot?",
         ("CCXT lists perpetual swaps for this exchange. Use the full contract symbol, such as ``BTC/USDT:USDT``." if perps
          else f"CCXT does not list perpetual swaps for {display}, so use spot markets.")),
        ("Is LumiBot free?",
         "Yes. LumiBot is open source on GitHub at https://github.com/Lumiwealth/lumibot. You pay only your exchange's trading fees and, for AI agents, your model provider."),
    ]
    for question, answer in faq:
        out.append(f"**{question}**\n\n{answer}\n")

    out.append(heading("Run it in the cloud with BotSpot", "-"))
    out.append(
        f"Do not want to keep a computer running for your {display} bot? BotSpot is the hosted home for LumiBot strategies "
        "and AI trading agents: build with AI, backtest, connect a broker, and watch every trade with logs and alerts.\n"
    )
    out.append(
        f"`Try BotSpot <https://botspot.trade/sales?showLogin=1&utm_source=documentation&utm_medium=exchange_page"
        f"&utm_campaign=lumibot_ccxt&utm_content={fx['id']}>`_\n"
    )

    related = related_pages(exchange_id, facts_by_id)
    out.append(heading("Related guides", "-"))
    links = [":doc:`CCXT crypto trading with LumiBot <brokers.ccxt>`", ":doc:`AI agents quickstart <agents_quickstart>`"]
    links += [f":doc:`{facts_by_id[r]['name']} trading bot <brokers.ccxt.{r}>`" for r in related]
    out.append(bullets(links) + "\n")

    out.append(heading("Sources", "-"))
    src = [f"`{label} <{url}>`_ (read {date})" for label, url, date in nt["sources"]]
    src.append(f"Exchange id, URLs, markets, credentials and stream support: CCXT {meta['ccxt_version']} metadata, generated {meta['generated_at']}")
    out.append(bullets(src) + "\n")

    return "\n".join(out)


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    facts = load("ccxt_exchange_facts.json")
    notes = load("ccxt_exchange_notes.json")["exchanges"]
    affiliates = load("ccxt_exchange_affiliates.json")
    facts_by_id = {fx["id"]: fx for fx in facts["exchanges"]}
    targets = argv or sorted(notes)
    for exchange_id in targets:
        if exchange_id not in facts_by_id:
            raise SystemExit(f"{exchange_id}: not in the installed CCXT facts")
        if exchange_id not in notes:
            raise SystemExit(f"{exchange_id}: no hand-checked notes yet; refusing to render a template-only page")
        page = render(exchange_id, facts_by_id, notes, affiliates, facts)
        if "—" in page or "–" in page:
            raise SystemExit(f"{exchange_id}: page contains an em or en dash")
        (DOCSRC / f"brokers.ccxt.{exchange_id}.rst").write_text(page, encoding="utf-8")
        print(f"wrote docsrc/brokers.ccxt.{exchange_id}.rst")


if __name__ == "__main__":
    main()
