Use your ChatGPT subscription
=============================

LumiBot can connect to your own ChatGPT account through OpenAI's supported
`Sign in with ChatGPT <https://developers.openai.com/siwc/token-sharing-open-source>`_
flow. Choose this connection explicitly for an agent:

.. code-block:: bash

   python -m lumibot.components.agents.chatgpt_auth login
   python -m lumibot.components.agents.chatgpt_auth status
   python -m lumibot.components.agents.chatgpt_auth models

.. code-block:: python

   researcher = self.agents.create(
       name="researcher",
       model="chatgpt/gpt-6-luna",  # Choose a slug returned by the models command.
       reasoning_effort="medium",
       system_prompt="Research evidence available at the strategy's current time.",
       allow_trading=False,
   )

No OpenAI API key is needed for this connection. It does not fall back to an
API key or BotSpot's managed inference. Existing explicit provider models retain
their own authentication. Broker, price-data and research permissions are
separate from model authentication.

Keep paid credits disabled
--------------------------

In `ChatGPT Usage <https://chatgpt.com/settings/usage>`_, turn off **Allow other
apps to use credits after reaching your usage limit** for subscription-only
execution. This setting applies to connected apps across the account. A
ChatGPT OAuth token alone does **not** guarantee that paid credits are disabled.
The public inference API does not provide a documented way for LumiBot to
change or verify this account setting; verify it before qualification runs.

Exhausted or revoked plan permission produces a visible error. It does not
create successful eval freshness or silently select another funding source.
No automatic purchase, reset or credit reload is performed by LumiBot.

Profiles and sign-out
---------------------

Profiles live outside the repository in ``~/.config/lumibot-chatgpt`` by
default. On POSIX systems credential files are owner-only; keep the containing
directory private too. On Windows, keep the user-profile directory's access
controls restricted to your account. Never commit profiles, share them with
other users, or copy credentials into a public issue or workflow artifact.

Login reconnects the active registration. Use ``login --profile <profile-id>``
to reconnect another existing profile or ``login --new-profile`` to create a
separate registration for another account. Returning registrations validate
the selected account rather than silently switching identity. In-flight
processes pin their selected profile; changing the active selection does not
move running agents to another account.

.. code-block:: bash

   python -m lumibot.components.agents.chatgpt_auth logout

Sign-out revokes the renewable grant when possible and clears local tokens
even if remote revocation cannot be confirmed. The registration and account
identity remain so reconnection can reuse them. Refresh rotation is serialized
across local processes.

Headless and self-hosted use
---------------------------

``login --no-browser`` starts the same bounded loopback listener and writes a
private ``pending-browser.json`` link. Forward the host's loopback callback
port and open that link on your trusted browser. Preserve the registration's
stable host identity and protect any transferred profile. Follow OpenAI's
`self-hosted VM instructions <https://developers.openai.com/siwc/token-sharing-open-source/self-hosted-vms>`_.
Do not extract Codex credentials or use private ChatGPT endpoints.

``LUMIBOT_CHATGPT_HOME`` selects a private configuration directory.
``LUMIBOT_CHATGPT_PROFILE`` selects an existing profile ID.
``LUMIBOT_CHATGPT_PROFILE_PATH`` explicitly selects a protected profile file
from your own supported SIWC registration. It is useful for an isolated
developer evaluator; it does not bundle or distribute that registration.

Commercially hosted customer inference needs an approved product integration
and each customer's consent. A developer's local subscription is not a shared
funding account for customer bots. BotSpot's research MCP connection is also a
separate grant; signing into ChatGPT does not authorize deployment or trading.

Canonical evaluations
---------------------

The canonical LumiBot actor and semantic judge both use this subscription
connection. Ordinary provider API keys and broker credentials are stripped
from the isolated fixture environment. Select a protected developer profile,
verify credit fallback is disabled, then run:

.. code-block:: bash

   python scripts/run_agent_evals.py --case-id research_macro_point_in_time \
       --repeat 5 --max-model-calls 80 --max-cost-usd 0 \
       --output-root /your/private/eval-run \
       --freshness-state /your/private/eval-freshness.json

The append-only call and repetition ledgers permit resume without repaying
passed repetitions. The call cap covers continuations and judge requests as
well as initial actor requests. Missing authoritative usage retains the
reservation. Reports distinguish subscription usage from API spend; zero API
spend is not proof of the account's credit setting.

Private CI funding must have its own authorized registration, host identity,
protected storage and serialized refresh owner. Public forks receive no
refresh credentials. Missing CI authentication fails the release gate; it is
not permission to run a paid model or skip the gate.
