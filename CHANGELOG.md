# Changelog

All notable changes to **ssis-adf-agent** will be recorded here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
While the package is still on the `0.x` line, the public surface (MCP tool
names + argument schemas, generated ADF JSON shape) may change between minor
versions. From `1.0.0` onward, breaking changes will only land in major bumps.

## [Unreleased]

### Added
- **P4-6** — Deeper deploy dry-run / pre-flight. New module
  `ssis_adf_agent/deployer/preflight.py` plus a `pre_flight=true` flag on
  the `deploy_to_adf` MCP tool that short-circuits the actual deploy and
  instead probes the external dependencies the linked services declare:
  Key Vault secret existence + read permission, host DNS resolution, and
  a managed-identity token-fetch against ARM. Failures carry actionable
  remediation messages (named role to grant, named tool to run). Every
  probe boundary is injectable so the test path never touches Azure or
  DNS. 19 new tests with stub clients.
- **P4-5** — Cost-actuals join helper. New module
  `ssis_adf_agent/migration_plan/cost_actuals.py` and MCP tool **#29
  `compare_estimates_to_actuals`** join the deployed `lineage.json` (M1) +
  the prediction from `estimate_adf_costs` against an Azure Cost
  Management export (REST response JSON *or* portal CSV). Per-factory
  variance ($ + %), per-meter breakdown, and an explicitly-flagged
  *estimated* per-pipeline allocation weighted by activity-count from the
  manifest. The estimated allocation carries a note clarifying that Cost
  Management does not invoice ADF spend below factory granularity —
  customers do not mistake the allocation for billed truth. 29 tests with
  captured CM REST + CSV fixtures (zero Azure dependency in the test path).
- **P4-4** — Encrypted-package automation helper. New module
  `ssis_adf_agent/deployer/keyvault_uploader.py` and MCP tool **#28
  `upload_encrypted_secrets`** automate Steps 2 + 4 of the ENCRYPTED_PACKAGES.md
  recipe in one command: extract secrets from an unprotected `.dtsx` (the
  customer still runs `dtutil` manually so decrypt remains auditable on
  their side), upload to Azure Key Vault via `azure-keyvault-secrets`, and
  rewrite the placeholder `secretName` fields inside generated linked-service
  JSON to point at the real secret names. `dry_run` and `overwrite`
  semantics, KV-safe name slugification, recursive JSON rewrite, never-leak
  `__repr__` on data classes. 26 new tests with a fake `SecretClient` (zero
  Azure dependency in the test path).
- **P4-2** — Vendor-curated substitution registries shipped in-repo at
  [`registries/`](registries/README.md): `cozyroc_salesforce.json`,
  `kingswaysoft_dynamics.json`, `pragmatic_works.json`. Together they cover
  the COZYROC Salesforce family, KingswaySoft Dynamics 365/CRM components
  (incl. Premium Derived Column / Premium Lookup), and Pragmatic Works Task
  Factory (Upsert Destination, Dimension Merge SCD, Advanced E-Mail Task,
  Secure FTP Task, Compression Task, Terminate Process Task, REST Source
  Task, plus six MDF transformations) — every non-trivial mapping carries a
  `_review_required` audit marker that lands in the generated ADF JSON. 15
  new tests including captured component XML fragment routing for each
  vendor and a cross-registry key-collision guard.
- **P4-3** — Worked Script Task port checked into the repo at
  [docs/case-studies/script_task_port_database_access_configuration/](docs/case-studies/script_task_port_database_access_configuration/README.md).
  Anchors the `moderate` bucket in [EFFORT_METHODOLOGY.md](EFFORT_METHODOLOGY.md)
  to a real LNI Script Task (80 LOC VB → production-ready Python Function;
  predicted 3.2h vs actual 3.5h, with phase-by-phase breakdown). Demonstrates
  the canonical migration pattern for SSIS Connection-Manager-mutating Script
  Tasks: Function returns resolved settings, ADF pipeline binds them to a
  parameterized linked service, password comes from Azure Key Vault via
  managed identity (replacing the cleartext-pipeline-variable pattern in the
  original VB). 11 new tests.
- **P4-1** — Behavioral data-flow parity harness (`compare_dataflow_output`,
  MCP tool #27). Runs the same controlled input set through an SSIS Data Flow
  (via `dtexec.exe`) and through its converted ADF Mapping Data Flow (via an
  ADF debug session), and emits a row-and-column diff report. Supports three
  modes: `captured` (replay pre-captured CSVs — recommended for CI),
  `live` (real dtexec + ADF debug), and `mixed`. Pluggable runner protocols
  let customers wire their own SSIS environment in. Pure diff engine
  (`diff_rows`) with row-key matching, schema-drift detection, configurable
  ignore lists, numeric tolerance, and case/whitespace normalization. Worked
  example with a *seeded regression* under
  [tests/fixtures/dataflow_parity/](tests/fixtures/dataflow_parity/) plus
  documentation in [BEHAVIORAL_PARITY.md](BEHAVIORAL_PARITY.md). 27 new tests.

### Fixed
- **B1** — `convert_ssis_package` now derives a Copy activity's `source.type`
  and `sink.type` from the actual SSIS component (`OLE DB Source` →
  `AzureSqlSource`, `Flat File Destination` → `DelimitedTextSink`, etc.) instead
  of always emitting `AzureSqlSource` / `AzureSqlSink`. SQL-only sink properties
  are gated; OLE DB `sqlReaderQuery` is preserved.
- **B3** — Pipeline parameters / project parameters / variables whose **name**
  matches a credential keyword (password / userid / token / secret / login /
  account / sas / connectionstring / clientsecret / credential) **or** whose
  **value** matches a Windows-domain account or on-prem FQDN are now redacted
  from the generated pipeline JSON. The entry is kept (so callers / Key Vault
  references still bind) but `defaultValue` is stripped and replaced with a
  `[SENSITIVE]` description block. Azure cloud hostnames are intentionally not
  flagged.
- **H3** — Script Task source extraction now handles the SSIS 2017+ inline
  `<ProjectItem>` CDATA layout. Packages from the LNI estate (and any other
  modern VSTA-style package) now report the correct `script_language`
  (CSharp / VisualBasic) and the LLM Script Task translator receives the
  original source instead of `None`. The misleading "self-closing stub
  format" warning was rewritten.

### Added
- **H4** — `PARITY.md` documents every check `validate_conversion_parity`
  performs, with explicit boundaries (no row-level / performance comparison).
  A worked example is captured against the LNI ADDS-MIPS-TC sample.
- `BACKLOG.md` — buyer's-perspective evaluation findings, prioritised P0–P3
  with execution order. Each entry is updated as items land.
- `CHANGELOG.md`, `SECURITY.md` — repo metadata required for enterprise
  consumption.

## [0.1.0] — initial published shape

- 23 MCP tools spanning scan → analyze → bulk-analyze → propose → plan →
  convert → validate → parity → smoke-test → deploy → provision → estate
  reporting.
- SSIS parser supporting Control Flow + Data Flow, Script Tasks (binary
  `BinaryData` / `ProjectBytes` patterns), event handlers, sequence /
  ForEach / For Loop containers.
- ADF generators for pipeline, linkedService, dataset, dataflow, trigger
  (Stopped state by default), Azure Function stubs.
- Bicep-based ADF + Function App provisioner.
- `azure-mgmt-datafactory` SDK dry-run before deploy.
- Pre-deployment PDF report.
