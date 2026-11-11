# AI wheel with inexpensive monitoring

This example uses SPY by default. Change `parameters["symbol"]` for future entries;
the trader retains each existing contract and share obligation under its original
symbol. One researcher gathers compact dated evidence. One trader alone selects
contracts, sizes positions, places orders and manages risk. Python wires the clock
and the shared attention monitor; it never interprets an answer or places an order.

The default policy sells cash-secured puts, accepts share delivery, sells covered
calls, accepts call-away and considers puts again. It targets 30–45 days to expiry,
absolute delta near 0.25, at most one short option and collateral within 90% of
equity. Favorable review begins at 90% premium capture after costs. These are
configurable example choices, not promises or universally appropriate settings.

Copy `main.py`, `rules.json` and `requirements.txt` together. Use a LumiBot runtime
that includes `AgentManager.attention` and the signed profit-basis repair; older
published installations lacking that API cannot run this example. This source
candidate does not establish that a broker host has installed those changes.
Both agents use `chatgpt/gpt-6-luna`, Medium reasoning and Login with ChatGPT.
Use your own supported ChatGPT login and an options-capable broker/data connection.
No model API key is required for this selected inference route.

The first review is scheduled from the broker clock for the final 15 minutes of
the session. Between decisions, `attention.poll()` reads saved conditions without
calling a model. `dispatch_team()` groups events into one review: setup/research
scope refreshes the researcher, and management scope reuses dated research. The
trader establishes later profit, quantity, order and time conditions. Continuous
unchanged conditions are suppressed; pending events and research answers use the
ordinary strategy-state backup. Termination joins the monitor's owned worker.
A live decision leaves polling available; a historical decision holds simulation
time still. These mechanics select no trades.

Ordinary bounded limits are the default. The trader inspects exact option quotes,
bid/ask spread, positions and working orders. Smart-limit repricing is not enabled
by this example. Historical trade aggregates do not prove executable bid/ask or
fill quality. Assignment, dividends, early exercise, fees, slippage and broker
permissions can materially change outcomes. Option premium is not portfolio
profit; account for stock value and outstanding option liabilities.

Qualification must distinguish controlled mechanical tests, real-model decisions,
historical comparisons, broker execution and Marketplace installation. A job that
finishes is not proof that its decisions followed policy, and a short diagnostic
is not evidence of long-term profitability. Do not interpret this source example
as a completed broker deployment or a published performance claim.
