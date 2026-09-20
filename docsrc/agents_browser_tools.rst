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

On a container image, run the browser installation at image-build time rather
than downloading a browser when a strategy starts. Give the task enough shared
memory for Chromium and validate the same architecture used in production.

Session model
-------------

``browser_session_open`` creates a named session and ``browser_session_close``
closes it. A persistent profile retains cookies and storage across close/open
cycles when the same profile name is reused. One session can hold multiple
tabs, and tools address the active tab unless a tab identifier is supplied.

The browser tool set is:

* ``browser_session_open`` and ``browser_session_close``
* ``browser_navigate`` and ``browser_observe``
* ``browser_act`` for click, fill, select, press, upload, and download actions
* ``browser_tabs`` for list, open, switch, and close
* ``browser_extract`` for text, attributes, links, or structured page evidence
* ``browser_login`` for scoped credential injection
* ``browser_storage_state`` for an inspectable session-state artifact
* ``browser_screenshot`` for a PNG plus a content hash

Credentials and artifacts
-------------------------

Login secrets belong in named, **host-scoped** credential profiles configured
by the application, never in an agent prompt or committed strategy. The
``browser_login`` tool injects a profile only when the page host matches its
allowed domains and never returns the secret. Configure profiles from the
deployment's existing secret manager.

Uploads must come from ``browser_upload_root`` (or the default managed upload
directory under ``browser_state_root``). Downloads,
screenshots, storage state, and action receipts are written to managed artifact
paths so a run can prove what it observed and did without exposing credentials.

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

Verification
------------

The committed real-browser acceptance test exercises login, JavaScript
interaction, multi-tab navigation, screenshot capture, storage-state export,
close/reopen persistence, and authenticated readback against a local fixture.
A separate 100-cycle restart soak checks profile/session cleanup. Those tests
prove the local runtime contract; validate the exact Linux ARM64 container and
Fargate task definition before calling hosted compatibility proven.
