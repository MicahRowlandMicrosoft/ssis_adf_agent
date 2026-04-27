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
  Contributor* on a dedicated RG, not Owner on a subscription. The full
  per-tool minimum-permissions matrix lives in [RBAC.md](RBAC.md).
- **Generated Azure Function stubs contain `# TODO` blocks and the original
  Script Task source as comments.** Review before publishing — the original
  source may itself contain hardcoded secrets.
- **The LLM Script Task translator (opt-in)** sends the original C# / VB
  source to the configured Azure OpenAI endpoint. Do not enable in
  environments where the source code may not leave the boundary of the
  Azure OpenAI resource you have provisioned.

### What the LLM translator sends, where, and how to disable (P4-8)

When `convert_ssis_package` is invoked with `llm_translate=true`, the
following data is sent **once per Script Task** to your configured
**Azure OpenAI** deployment (`AZURE_OPENAI_ENDPOINT`):

| Field | Value | Notes |
|---|---|---|
| System prompt | Static, embedded in the agent. | No customer data. |
| Task name | The SSIS Script Task `Name` attribute. | |
| Read-only variables | The SSIS variable identifiers (e.g. `User::CustomerId`). | Names only — never values. |
| Read-write variables | Same as above. | |
| `source_code` | The raw C# / VB Script Task body extracted from the .dtsx. | Truncated at 18 000 chars. May include hardcoded literals. |
| Model | `AZURE_OPENAI_DEPLOYMENT` (default `gpt-4o`). | Caller chooses. |

Nothing else from the package, the connection managers, the agent's
configuration, or the host environment is transmitted. No telemetry is
sent to a third party. The Azure OpenAI deployment is **the customer's
own resource** authenticated through `DefaultAzureCredential` (the
caller's identity needs *Cognitive Services OpenAI User* on that resource).

**Behavior at the 18 000-char truncation bound (P5-24).** The 18 000-char
cap on `source_code` is enforced silently in
[`translators/csharp_to_python.py`](ssis_adf_agent/translators/csharp_to_python.py)
(`_MAX_INPUT_CHARS`, ~6 000 tokens at ~3 chars/token). When the source
exceeds that bound:

- The first 18 000 characters are kept verbatim.
- A literal `// ... [TRUNCATED: source exceeded 18000 chars] ...` marker is
  appended to the slice before it is sent to Azure OpenAI. The model
  sees the marker and is expected to generate an explicit "remainder
  not translated — port manually" comment in the output.
- **No exception is raised. No warning is logged. No tool-level signal
  is emitted.** The translation succeeds; only the *prompt input* is
  truncated.
- The generated stub will reflect only the first 18 000 chars of logic.
  The original full source is still embedded as comments in the stub
  (preserved by the Script Task converter's source-as-comments
  behavior — see H3); the operator can scroll the comment block to
  port the truncated tail by hand.
- For a Script Task whose source genuinely exceeds the bound, the
  recommended pattern is the same as a port the LLM cannot translate
  at all: follow the [Database_Access_Configuration case study](docs/case-studies/script_task_port_database_access_configuration/README.md)
  methodology and treat the LLM stub as a starting point only.

**Three mutually-reinforcing kill switches** disable the LLM call:

1. **`llm_translate=false`** (default) — no LLM call is ever attempted.
2. **`no_llm=true`** parameter on `convert_ssis_package` — overrides the
   `llm_translate` argument for the duration of one tool call. Use to
   prove deterministic behaviour from a CI job or a one-off review.
3. **`SSIS_ADF_NO_LLM=1`** environment variable — process-wide hard
   switch. When set, the translator's `is_configured()` returns False
   and `translate()` raises `TranslationError` *before* constructing
   any client. Set this in regulated tenants, in the agent's container
   env file, or in the deploying user's shell profile to make it
   physically impossible for an interactive caller to bypass.

When any of the above disables the LLM, the Script Task converter still
produces a deterministic Azure Function stub — the only thing degraded
is the *quality of the generated Python body*: it remains the
"Re-implement the C# logic here" TODO scaffolding rather than an
attempted Python port. Every other artifact (pipelines, linked services,
datasets, data flows, triggers) is bit-for-bit identical with or without
the LLM.

### What the agent talks to and how to disable it (P5-8)

This is the complete catalogue of *every* outbound network call the agent
itself can make. There are exactly **three** distinct egress destinations,
all gated by an explicit caller action — no telemetry, no auto-update, no
phone-home. The audit was performed against the source tree at the commit
that introduced this section by grep-walking for `requests`, `httpx`,
`urllib`, `aiohttp`, `http.client`, `azure.mgmt.*`, `azure.identity`,
`azure.keyvault`, `AzureOpenAI`, and direct `subprocess` invocations of
`az` / `curl` / `wget`.

| Destination | Triggered by | Library | How to disable |
|---|---|---|---|
| **Azure OpenAI** (your tenant) | `convert_ssis_package(llm_translate=true)` *and* `AZURE_OPENAI_ENDPOINT` set *and* `SSIS_ADF_NO_LLM` unset. | `openai.AzureOpenAI` from [`translators/csharp_to_python.py`](ssis_adf_agent/translators/csharp_to_python.py). | Default. Or set `SSIS_ADF_NO_LLM=1`, or pass `no_llm=true`, or leave `llm_translate` unset (defaults to false). |
| **Azure Resource Manager** (control plane) | The deployment / provisioning tools: `deploy_to_adf`, `provision_adf_environment`, `provision_function_app`, `validate_adf_artifacts` (when sub/RG supplied), `activate_triggers`, `deploy_function_stubs`, the `keyvault_uploader`, the `preflight` / RBAC checks. | `azure.identity.DefaultAzureCredential` + the `azure.mgmt.*` SDKs. Plus `httpx` for Function App zip-deploy in [`deployer/func_deployer.py`](ssis_adf_agent/deployer/func_deployer.py). | Don't invoke those tools. The conversion path (`scan_ssis_packages`, `analyze_ssis_package`, `convert_ssis_package`, `convert_estate`, `validate_adf_artifacts` *without* sub/RG, `bulk_analyze`, `propose_adf_design`, `consolidate_packages`, `explain_ssis_package`, `build_estate_report`, `build_predeployment_report`, every `*_plan.json` / cost / wave tool) is fully offline. |
| **A SQL Server instance** | `scan_ssis_packages` with the SQL reader path (reads `.dtsx` rows from SSISDB or a `[ssis].[packages]` table). | `pyodbc` from [`parsers/readers/sql_reader.py`](ssis_adf_agent/parsers/readers/sql_reader.py). | Use the local-disk or Git reader instead — they are the default for `scan_ssis_packages`. |

**Calls explicitly NOT made by the agent**, verified by grep:

- No `import requests`, no `import urllib.request`, no `import aiohttp`,
  no `import http.client` anywhere under `ssis_adf_agent/`. The only
  HTTP client present is `httpx`, scoped to the Function App zip-deploy
  path.
- No telemetry, analytics, crash-reporting, or update-check code path.
  No `pip install` or `az login` is invoked from inside any tool.
- No call to `pypi.org`, `github.com`, Microsoft Learn, or any public
  internet endpoint other than (optionally) the customer's own Azure
  OpenAI deployment.
- Generated artifacts (Bicep, Function stubs, lineage JSON, cost
  estimates, PDF reports) are all written to local disk only — they are
  never POSTed anywhere.

**Air-gapped / no-egress operating mode.** The conversion path
(`scan_ssis_packages` + `analyze_ssis_package` + `propose_adf_design` +
`convert_ssis_package` / `convert_estate` + `validate_adf_artifacts`
without sub/RG) makes **zero** outbound network calls when:

- `SSIS_ADF_NO_LLM=1` is exported in the host environment, **and**
- The local-disk or Git readers are used (the default).

This is enforced by a regression test:
[`tests/test_no_egress_conversion_path.py`](tests/test_no_egress_conversion_path.py).
The test runs the full `convert_estate` path through a pytest fixture
that monkey-patches `socket.socket`, `socket.create_connection`, and
`httpx.HTTPTransport.handle_request` to raise on any attempt to open a
network connection, and asserts the path completes successfully.
Adding any new outbound HTTP call to the conversion path will fail this
test and force an explicit code-review decision.



- Running an SSIS package or an ADF pipeline on the caller's behalf to
  validate behavior — the tool is offline / structural.
- Verifying that customer-provided values (linked-service connection
  strings, Key Vault references, SHIR identities) are themselves safe.
