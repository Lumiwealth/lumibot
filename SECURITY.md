# Security Policy

## Reporting a Vulnerability

Please report suspected security issues privately instead of opening a public
GitHub issue.

- Use GitHub private vulnerability reporting if it is available for this
  repository, or contact the repository maintainers privately.
- Include the affected version or commit, reproduction steps, impact, and any
  relevant logs with secrets redacted.
- Do not include live credentials, account tokens, customer data, private local
  paths, or production-only URLs in public issues, pull requests, docs, or
  examples.

## Public Repository Hygiene

LumiBot is open source. Public files must not contain usernames, passwords, API
keys, account emails, private URLs, local `.env` paths, credential-file paths,
or absolute personal filesystem paths.

The repository uses GitHub secret scanning with push protection plus a lightweight
public-hygiene CI check for instruction files such as `AGENTS.md` and
`CLAUDE.md`.
