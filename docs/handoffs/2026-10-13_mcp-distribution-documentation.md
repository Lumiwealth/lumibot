# MCP distribution documentation

The public connector repository README is the maintained installation and
first-use guide: https://github.com/Lumiwealth/botspot-mcp#connect .
`docsrc/botspot_mcp.rst` and the LumiBot README point to that guide rather than
duplicating client configurations that can drift. The canonical explicit
endpoint is `https://mcp.botspot.trade/mcp`; strategy creation uses
`create_strategy` and immutable file revisions, not retired generation tools.

This is a documentation change only. It does not change LumiBot runtime,
broker behavior, package version, permissions or BotSpot served code. Custom
MCP setup, store submission, approval, publication and authenticated client
installation must remain distinct claims. The restricted store profile has
not yet been qualified or released.

LumiBot's verified default branch is `dev`; the workspace helper's hardcoded
`origin/main` start failed because this repository has no remote main branch.
An isolated task-owned worktree from `origin/dev` is used for this docs change.
