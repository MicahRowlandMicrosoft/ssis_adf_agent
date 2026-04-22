# Security policy

## Supported versions

This project is on a `0.x` line. Only the latest minor version receives
security fixes. Once the project ships `1.0.0`, the previous major version
will receive backported security fixes for at least 6 months.

| Version  | Supported |
|----------|-----------|
| `0.1.x`  | ✅ |
| `< 0.1`  | ❌ |

## Reporting a vulnerability

**Do not open a public GitHub issue for security vulnerabilities.**

Instead, report privately by one of the following channels:

1. **GitHub Private Vulnerability Reporting** — open a "Security advisory" via
   the repository's **Security** tab → *Report a vulnerability*. This is the
   preferred channel.
2. **Email** — send a description and reproduction steps to the maintainer
   listed in the repository's `CODEOWNERS` (or, if none, the address in the
   most recent commit on `main`). Encrypt with the maintainer's public key if
   one is published.

When reporting, please include:

- A short description of the issue.
- The version / commit SHA you tested against.
- A minimal reproduction (a `.dtsx` snippet, an MCP tool invocation, or a
  Python repro).
- The impact you observed and the impact you believe is achievable
  (data exfiltration, unauthorised Azure operation, code execution in the
  caller's process, etc.).

We aim to acknowledge a report within **5 business days** and to ship a fix
or mitigation within **30 days** for high-severity issues.

## Hardening expectations

If you deploy this MCP server in a shared / multi-tenant context, please be
aware:

- **The server reads `.dtsx` files supplied by the caller.** Treat input
  packages as untrusted XML. The parser uses `lxml` and does not enable
  network entity resolution, but a defence-in-depth review is welcome.
- **Generated pipeline JSON is redacted for known credential-shaped
  defaults** (see [BACKLOG.md](BACKLOG.md) item B3 and the regression tests
  in `tests/test_pipeline_sensitive_redaction.py`). Always pair generation
  with a code review before pushing JSON to source control. Do not assume
  the redaction list is exhaustive for every customer's naming convention.
- **`deploy_to_adf` and `provision_adf_environment` use
  `DefaultAzureCredential`.** The caller's identity is what writes to your
  factory / resource group. Scope RBAC accordingly — *Data Factory
  Contributor* on a dedicated RG, not Owner on a subscription.
- **Generated Azure Function stubs contain `# TODO` blocks and the original
  Script Task source as comments.** Review before publishing — the original
  source may itself contain hardcoded secrets.
- **The LLM Script Task translator (opt-in)** sends the original C# / VB
  source to the configured Azure OpenAI endpoint. Do not enable in
  environments where the source code may not leave the boundary of the
  Azure OpenAI resource you have provisioned.

## Out of scope

- Running an SSIS package or an ADF pipeline on the caller's behalf to
  validate behavior — the tool is offline / structural.
- Verifying that customer-provided values (linked-service connection
  strings, Key Vault references, SHIR identities) are themselves safe.
