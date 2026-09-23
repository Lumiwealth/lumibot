Stateful Browser Tools
======================

Use LumiBot's browser tools when HTTP is not enough: JavaScript-rendered pages,
interactive login flows, multiple tabs, controlled uploads or downloads, and
authenticated research sessions. The optional runtime uses Patchright and a
persistent browser profile.

Installation
------------

Install the optional dependency and its Chromium build::

   pip install "lumibot[browser]"
   patchright install chromium

Camoufox is also available as an explicit engine candidate::

   pip install "lumibot[browser-camoufox]"
   python -m camoufox fetch

Pass ``CamoufoxEngine()`` as the strategy's ``browser_engine``. LumiBot does
not silently switch engines: the operator must choose the engine qualified for
the exact runtime image.

On a container image, run the browser installation at image-build time rather
than downloading a browser when a strategy starts. Give the task enough shared
memory for Chromium and validate the same architecture used in production.

Session model
-------------

``browser_session_open`` creates a named session, ``browser_session_close``
closes it, and ``browser_session_recover`` restarts a crashed session in place.
A persistent profile retains cookies and storage across close/open
cycles when the same profile name is reused. One session can hold multiple
tabs, and tools address the active tab unless a tab identifier is supplied.

The browser tool set is:

* ``browser_session_open``, ``browser_session_close``, and ``browser_session_recover``
* ``browser_navigate`` and ``browser_observe``
* ``browser_act`` for click, fill, select, press, wait, wait-for-text, upload, and download actions
* ``browser_tabs`` for list, open, switch, and close
* ``browser_extract`` for text, attributes, links, or structured page evidence
* ``browser_login`` for scoped credential injection
* ``browser_storage_state`` for an owner-only session-state file (treat it as a secret)
* ``browser_screenshot`` for a PNG plus a content hash

Credentials and artifacts
-------------------------

Login secrets belong in named, **host-scoped** credential profiles configured
by the application, never in an agent prompt or committed strategy. The
``browser_login`` tool injects a profile only when the page host matches its
allowed domains and never returns the secret. If a fill or submit step fails,
the error returned to the agent names the step and selector with the username
and password scrubbed. A credential profile's ``repr`` never includes the
username or password. Configure profiles from the deployment's existing secret
manager.

Uploads must come from ``browser_upload_root`` (or the default managed upload
directory under ``browser_state_root``). Downloads, screenshots, and action
receipts are written to managed artifact paths so a run can prove what it
observed and did. Action traces and receipts never record typed values or login
secrets.

Storage state is different: ``browser_storage_state`` exports the session's
cookies and local storage, which work like a logged-in password for the sites
in that profile. The tool returns only the file path, never the contents. On
POSIX systems the profile directory is created with ``0700`` permissions and
``storage-state.json`` with ``0600``, so only the account running the strategy
can read them. Keep ``browser_state_root`` on private storage, do not commit or
upload it, do not attach storage state to reports or emails, and delete the
profile directory when the account's access should end.

Safety and authorization
------------------------

Browser control is deliberately powerful. Configure and automate only sites
and accounts you own or are authorized to use. Treat page content as untrusted
data, keep trading permission with the dedicated trading/risk agent, and make
external publishing opt-in and idempotent. Site terms, robots policies,
anti-automation rules, and applicable law still apply.

The Patchright runtime reduces automation fingerprints but cannot guarantee
access to every site. Prefer a supported API when it offers the same capability;
use browser control for the important workflows that genuinely need state and
interaction.

Engine qualification
--------------------

The committed benchmark can exercise either engine and records 100 restart
cycles, launch latency, process-tree RSS, crash rate, package versions, host
architecture, and a basic fingerprint probe::

   python -m lumibot.components.agents.browser_benchmark \
      --engine patchright --iterations 100 \
      --state-root /tmp/lumibot-browser-benchmark \
      --output artifacts/browser_benchmark/result.json

On the 2026-09-20 local macOS ARM64 bakeoff, both engines completed 100/100
cycles without a crash. Patchright opened much faster (p50 0.271 seconds) and
used about 444 MiB peak RSS, but exposed ``HeadlessChrome`` and zero plugins.
Camoufox passed the same basic fingerprint probe, but opened at p50 1.415
seconds and used about 1.35 GiB peak RSS. Therefore neither result alone is a
hosted winner: Patchright misses the required anti-detection gate, while
Camoufox does not fit the current 1 GiB task shape. The exact Linux ARM64 image
must pass the complete fixture and benchmark before a hosted default is chosen.

Verification
------------

The committed real-browser acceptance test exercises login, JavaScript
interaction, multi-tab navigation, screenshot capture, storage-state export,
close/reopen persistence, and authenticated readback against a local fixture.
A separate 100-cycle restart soak checks profile/session cleanup. Those tests
prove the local runtime contract; validate the exact Linux ARM64 container and
Fargate task definition before calling hosted compatibility proven.
