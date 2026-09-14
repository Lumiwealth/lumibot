# Managed model family selections

Managed AI can use `google/gemini-pro`, `google/gemini-flash`,
`google/gemini-flash-lite`, `openai/luna`, `openai/terra`, `openai/sol`,
`openai/astra`, `anthropic/sonnet`, and `xai/grok`.

The managed gateway owns the reviewed exact model and price mapping. On the
first request it returns `resolvedModel`; this adapter sends that exact id for
every later tool request in the decision. Provider response version labels are
not used as billable request ids. Unexpected model changes fail visibly as
`protocol_integrity_error`. Only first resolution is serialized; exact-id
requests and credential renewal keep their existing concurrency behavior.

This requires a compatible v2 gateway and configured managed access. Family
names are not provider-native aliases. Direct provider/BYOK users must select an
exact provider id. A personal key is never ignored and rejected personal
authentication never falls back to managed credit spending.

Exact ids remain supported with older gateways that omit `resolvedModel`.
Once a gateway starts supplying model resolution, it must preserve it. Each
AgentManager runtime decision constructs a new model instance; new decisions
may adopt the gateway's latest reviewed mapping. Pin exact ids when reproducing
historical experiments. A family does not promise automatic adoption of every
new provider model, nor does this client prove that a hosted model is available.
