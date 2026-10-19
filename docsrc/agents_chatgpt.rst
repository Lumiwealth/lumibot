Use your ChatGPT subscription
=============================

.. meta::
   :description: Connect LumiBot agents and actor/judge evaluations to your own ChatGPT account, with secure login, private profiles and explicit credit usage.

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

Choose your ChatGPT usage policy
-------------------------------

Your ChatGPT connection can use included plan allowance and, when you allow
it, your existing ChatGPT credit balance. Both use the same OAuth connection;
neither requires an API key. Reaching zero remaining plan allowance does not
mean inference is unavailable when credit use is enabled.

Review `ChatGPT Usage <https://chatgpt.com/settings/usage>`_ before running
evaluations. If you want included allowance only, turn off **Allow other apps
to use credits after reaching your usage limit**. This setting applies across
connected apps. If existing credits are authorized, leave it enabled. LumiBot
does not change this account-wide setting, and its public inference API does
not provide a documented way to verify it.

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

Bounded test-worker credentials
-------------------------------

A host running an authorized test can select a nonrenewable run profile through
the existing ``LUMIBOT_CHATGPT_PROFILE_PATH`` setting. This does not transfer a
credential, create a remote worker, change an agent's selected model, or enable
subscription inference for ordinary provider models. The agent must still use
an explicit ``chatgpt/<model slug>`` connection.

The private run profile contains ``credential_kind: "run"``, a unique ``run_id``,
``status: "connected"``, the granted ``scopes`` including
``chatgpt.tokens.use.direct``, one authorized OAuth ``access_token``, an absolute
Unix ``expires_at``, one model ``model`` slug, and positive integer ``limits``
for ``calls``, ``input_tokens`` and ``output_tokens``. It may be valid for at most
one hour when loaded for inference. It must not contain a refresh token or an
API key. The trusted host prepares it from its authorized connection; do not
copy a renewable account profile into a test artifact or public workflow.

The adapter reserves the selected model catalog's full input/output envelope
before each actual HTTP request, including tool continuations. Unknown model
limits fail before the request. A smaller caller output limit cannot reduce the
reservation because this subscription endpoint does not accept that limit.
Completed Responses usage settles the reservation; interrupted streams, missing
usage and failed requests keep it. Output counts already include reasoning.

An owner-only sibling ``.usage.jsonl`` file records reservations and usage,
without prompts, tool arguments or access tokens. Concurrent processes using
that profile share its lock and limits. Restarting cannot reset use or enlarge
the original policy. Expired credentials refuse new requests, but existing usage
can still be recorded and inspected. Run profiles never refresh, switch
accounts, or fall back to API billing.

These are client-side limits, not a newly scoped or independently revocable
OpenAI credential. The underlying bearer token retains its original permission
and provider expiration. Protect it through the host's existing secret channel.
The controlling test runner must reserve each worker's whole allowance in its
own cumulative ledger before provisioning it, and collect the usage receipt
afterward. Separate workers do not share a local ledger automatically.

This SDK support alone does not qualify a hosted integration. Credential
transfer, host identity, consent, cleanup and end-to-end inference need their
own authorized validation. Existing production provider connections keep their
current authentication and billing unless their owner explicitly selects a
different connection.

Canonical evaluations
---------------------

The canonical LumiBot actor and semantic judge both use this subscription
connection. Ordinary provider API keys and broker credentials are stripped
from the isolated fixture environment. Select a protected developer profile
and your authorized ChatGPT usage policy, then run:

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
