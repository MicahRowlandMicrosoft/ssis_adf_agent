# Changelog

All notable changes to **ssis-modernization-agent** will be recorded here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
While the package is still on the `0.x` line, the public surface (MCP tool
names + argument schemas, generated ADF JSON shape) may change between minor
versions. From `1.0.0` onward, breaking changes will only land in major bumps.

## [Unreleased]

### Added
- **Microsoft Fabric Data Pipelines target.** The agent can now convert SSIS
  packages to Microsoft Fabric Data Pipelines + PySpark Notebook stubs in
  addition to Azure Data Factory. Shipped across four phases on the
  `fabric` branch:
  - **Phase 0 — project rename.** `ssis-adf-agent` → `ssis-modernization-agent`
    so the package name reflects multi-target support. CLI entry point and
    MCP server name updated; module path is now
    `ssis_modernization_agent`. No tool surface changes.
  - **Phase 1 — Fabric MVP converter.** New
    [`ssis_modernization_agent.fabric`](ssis_modernization_agent/fabric/)
    package: `ConnectionResolver` (deterministic GUID placeholders for
    SSIS Connection Managers + on-prem detection note), `pipeline_translator`
    (translates ADF activity dicts to Fabric pipeline-content shape, inlines
    Copy datasets, replaces `ExecuteDataFlow` with `TridentNotebook`,
    recurses into ForEach / IfCondition / Switch),
    `notebook_stub_generator` (writes `notebook-content.py` + `.platform`
    sidecar with TODO blocks for sources / transforms / destinations),
    `fabric_converter.convert_package_to_fabric` (one-call API), and
    `validator.validate_fabric_artifacts` (structural validation of the
    generated Fabric layout). Implementation reuses the proven ADF
    dispatcher in-memory rather than duplicating per-task converters.
    23 new tests in `tests/test_fabric_phase1.py` including end-to-end on
    the LNI ADDS-MIPS-TC sample.
  - **Phase 2 — Fabric deployer.** New
    [`deployer/fabric_deployer.py`](ssis_modernization_agent/deployer/fabric_deployer.py)
    that shells out to the Microsoft `fabric-cli` (`fab`) via a `FabRunner`
    Protocol so tests substitute a fake. Refuses to provision a workspace
    without an explicit `capacity_id` (no silent capacity allocation).
    `discover_notebook_ids` resolves the placeholders from
    `notebook_placeholders.json` against the deployed workspace.
    `apply_substitutions_in_place` rewrites both Connection placeholders
    (`externalReferences.connection`) and notebook placeholders
    (`TridentNotebook.typeProperties.notebookId`) before the Fabric API
    sees the JSON. 17 new tests in `tests/test_fabric_phase2.py`.
  - **Phase 3 — Fabric-aware migration plan + cost projection.** New
    `MigrationTarget` enum (`ADF | FABRIC`) on `MigrationPlan`. The
    `propose_adf_design` MCP tool gained a `target` parameter — when
    `target="fabric"` the plan summary calls out the PySpark hand-port
    consequence and per-Data-Flow-Task Risks (`MEDIUM` if ≤ 2,
    `HIGH` if more) are added. New `migration_plan/fabric_costs.py` with
    `estimate_fabric_costs` (CU-second projection → smallest F-SKU at
    configurable headroom, F2..F2048 list-price table documented inline)
    and `estimate_costs_dispatch` (single entry point that filters
    kwargs per platform). `estimate_adf_costs` MCP tool gained `target`
    and Fabric-specific knobs (`avg_notebook_minutes`,
    `notebook_executors`, `headroom_pct`, `onelake_storage_gb`). 15 new
    tests in `tests/test_fabric_phase3.py`.
  - **Phase 4 — behavioral parity label + worked case study.**
    `compare_dataflow_output` MCP tool and `render_diff_markdown` gained an
    optional `target_label` parameter ("ADF" by default, pass "Fabric" to
    relabel the report headers and table columns). The underlying JSON diff
    keys (`adf_row_count`, `missing_in_adf`, etc.) are deliberately not
    renamed so existing CI assertions keep working. New
    [docs/conversion/fabric.md](docs/conversion/fabric.md) end-to-end
    Fabric conversion guide. New
    [docs/case-studies/fabric_conversion_adds_mips_tc/](docs/case-studies/fabric_conversion_adds_mips_tc/README.md)
    worked example with the actual generated artifacts checked in
    (1 pipeline, 1 PySpark notebook stub, 3 connection placeholders,
    2 Function stubs) — pinned by
    `tests/test_fabric_phase4.py::test_case_study_lni_fabric_conversion_artifact_counts`
    so the doc cannot drift.
  - **Phase 5 — SSIS↔Fabric structural parity + estate-scale orchestration.**
    Closes the two acknowledged Phase 4 gaps. Two new MCP tools (count
    35 → 37):
      - **`validate_fabric_conversion_parity`** — Fabric counterpart to
        `validate_conversion_parity`. Compares an SSIS package to its
        converted Fabric artifacts (pipeline-content.json, notebook stubs,
        `connections_required.json`, function stubs) and reports task
        coverage, connection coverage, notebook stub coverage, function
        stub coverage, and event handler coverage. Returns the same
        top-level shape as the ADF parity validator (`ok` / `summary` /
        `matches` / `issues` / `artifact_dryrun`) plus `target="fabric"`
        so CI can route on a single field. Optional Markdown report.
        Deterministic and offline.
      - **`convert_estate_to_fabric`** — Fabric counterpart to
        `convert_estate`. Converts every `.dtsx` under a directory with
        per-package failure isolation, optional per-package parity report,
        optional Fabric cost projection, and **estate-wide connection
        deduplication** — a SQL Server shared by N packages collapses to
        one Fabric Connection placeholder, not N, with each entry
        recording `used_by_packages`.
    `MigrationTarget` and `estimate_costs_dispatch` are now exported from
    `ssis_modernization_agent.migration_plan`. 12 new tests in
    `tests/test_fabric_phase5.py` covering parity happy/sad paths
    (missing activities, missing notebook stub, on-prem CM, missing
    manifest), Markdown rendering, estate orchestration (multi-package,
    dedup, per-package failure routing, parity sidecar emission), and the
    MCP tool listing wire-up.

- **P5-19** — [encrypted-packages.md](docs/operations/encrypted-packages.md) gained
  a "🧯 Real failure walkthrough" callout at the top linking to
  [docs/case-studies/first_deploy_keyvault_recovery/](docs/case-studies/first_deploy_keyvault_recovery/README.md),
  the captured P4-11 case study. The cross-link is now
  bidirectional, so `upload_encrypted_secrets` users discover the
  pre-flight requirement (factory MI vs. deployer SP) before they
  hit the same `ManagedServiceIdentityCredentialNotFound` failure.

- **P5-18** — [howto.md](docs/getting-started/howto.md) gained a "🚀 Start here"
  callout at the top pointing at [workflow.md](docs/getting-started/workflow.md) as the
  recommended first read for new users. Names the 6-tool minimum
  path inline. Closes the gap that buyers landing on HOWTO from a
  Google search were missing the minimum-path doc.

- **P5-9** — README tool-count + workflow diagram drift fixed.
  Trailing "22 tools" references in `README.md` updated to `29` to
  match the header. Architecture diagram replaced with the
  6-step `bulk_analyze → propose → convert → validate → deploy →
  activate` path that [workflow.md](docs/getting-started/workflow.md) prescribes (was the
  older 5-step `scan → analyze → convert → validate → deploy`).
  New [`tests/test_readme_consistency.py`](tests/test_readme_consistency.py)
  asserts (a) all three README tool-count strings equal
  `len(list_tools())` and (b) the 6-step diagram references both
  `bulk_analyze` and `activate` and the older 5-step prose is gone,
  so the next time a tool is added/removed the README must be
  updated in lockstep or CI fails.

- **P4-14** — New top-level [`SUPPORT.md`](SUPPORT.md) splits support
  into two tiers. **Tier 1 (OSS):** GitHub Issues only, best-effort
  2–5 business-day first response, no SLA, with explicit "do not
  expect" list (hours-scale response, phone/chat, overnight). **Tier 2
  (engaged customers):** named-channel response-time commitments by
  severity — Sev 1 (migration blocked) **2 business hours**, Sev 2
  (impaired w/ workaround) **1 business day**, Sev 3 (question /
  minor defect) **3 business days**. Names three supported channel
  formats (Teams channel preferred, shared email DL ≥ 2 recipients,
  on-call rotation roster with backup primary) and excludes
  single-engineer phone numbers as a single-point-of-failure pattern.
  5-field bug-report template and a non-negotiable sanitization
  checklist (no connection strings, no customer-identifying KV/sub/RG
  names, no business-sensitive table/column names, no package
  passwords). README "Need help?" callout added pointing at
  SUPPORT.md. **All 14 P4 buyer follow-ups now closed.**

- **P4-13** — New top-level [`roadmap.md`](docs/development/roadmap.md) defines what
  1.0 means (committed: MCP tool I/O, CLI, on-disk artifact layout +
  `lineage.json`/`migration_plan.json` schemas, public Pydantic IR;
  not committed: internal implementation, generated stub text,
  warning wording, the C#→Python translator), lists 14 engineering
  items required for the bump (grouped Stability / Quality / Surface /
  Operational, each tagged done / NEW / partial), and pins the
  pre-1.0 → 1.0 transition policy: a `0.9.0` release ships first with
  a `removed-in-1.0` deprecation manifest, supported for a minimum
  30 days before `1.0.0` removes the deprecated surface. README gained
  a "Pre-1.0 status" callout pointing at ROADMAP.md.

### Changed
- **P4-12** — [`coverage.md`](docs/conversion/coverage.md) gained an **Evidence**
  column on all five tables (Control Flow tasks, Sources,
  Transformations, Destinations, Connection managers, Package-level
  constructs). Each ✅ / 🟡 row links to the unit-test fixture that
  exercises the conversion (or to the generator/converter source
  where no per-construct test exists). 🔴 / ⚪ rows carry an em-dash
  by design (analyzer flags them `manual_required`); a one-line
  reading guide above the first table explains this.

### Added
- **P4-11** — New captured case study at
  [`docs/case-studies/first_deploy_keyvault_recovery/`](docs/case-studies/first_deploy_keyvault_recovery/README.md)
  documents one sanitized real first-deploy failure: 6 encrypted-
  package linked services failed with
  `ManagedServiceIdentityCredentialNotFound` because the factory's
  system-assigned managed identity — not the deployer SP — lacked
  *Key Vault Secrets User* on the referenced vault. Captures the
  verbatim error log, the 22 minutes spent on the wrong hypothesis,
  the three recovery commands, and four lessons tying back to P4-6
  pre-flight, P4-7 RBAC matrix, and the WORKFLOW.md "pre-flight first"
  default. Linked from the top of ROLLBACK.md so the generalized
  strategies sit on top of one concrete instance.
- **P4-10** — New top-level [`observability.md`](docs/operations/observability.md)
  documents the post-cut-over BAU monitoring story: Log Analytics as
  the recommended diagnostic-settings target (with the five log/metric
  categories to enable and a one-time Bicep snippet), two saved KQL
  queries for a failures-and-trends workbook, and three alert rules
  with full KQL, thresholds, evaluation cadence, and severity rationale
  (any failure, duration regression vs. 14-day p95, trigger silently
  stopped firing). BEHAVIORAL_PARITY.md "when published" placeholder
  link replaced with the live cross-reference.
- **P4-9** — New top-level [`workflow.md`](docs/getting-started/workflow.md) names the
  **6-tool minimum path** through a full migration
  (`bulk_analyze` → `propose_adf_design` → `convert_estate` →
  `validate_adf_artifacts` → `deploy_to_adf` (pre-flight first) →
  `activate_triggers`) with one Copilot prompt per step and a
  decision gate per stage. Catalogues the remaining ~23 tools as
  optional, themed by reason-to-reach-for, with a six-row
  "Tool overlap, signposted" table that names which pairs are
  confusable (e.g. `convert_estate` vs `convert_ssis_package` in a
  loop) and which one to prefer. README "New to the agent?" callout
  now points at WORKFLOW.md first.
- **P4-8** — No-LLM mode hard switch. New `SSIS_ADF_NO_LLM` env var
  (truthy: `1`/`true`/`yes`/`on`) and new `no_llm=true` per-call
  argument on `convert_ssis_package` that disable the Azure OpenAI
  Script Task translator regardless of `llm_translate`. When the LLM
  is disabled, `convert_ssis_package` emits a UserWarning naming which
  switch overrode the request; Script Task stubs degrade to
  deterministic TODO scaffolding. SECURITY.md gained a new "What the
  LLM translator sends, where, and how to disable" section
  enumerating the exact fields transmitted, where they go (the
  customer's own Azure OpenAI deployment), and the three
  mutually-reinforcing kill switches. 877 tests passing.
- **P4-7** — Published per-tool RBAC / least-privilege matrix in new
  [`rbac.md`](docs/operations/rbac.md). Names the minimum Azure RBAC role(s) and Key
  Vault data-plane permission for each of the 29 MCP tools, the scope
  that role should be granted at, and the rationale for the chosen role
  family (Data Factory Contributor, Key Vault Secrets Officer / User,
  Cost Management Reader). Includes an `assign_rbac=false` escape hatch
  for environments that disallow `User Access Administrator` on the
  deploying identity, the `az role assignment create` snippets, and an
  audit checklist the security reviewer can run before approving the
  change ticket. Cross-linked from SECURITY.md and SETUP.md.
- **P4-6** — Deeper deploy dry-run / pre-flight. New module
  `ssis_modernization_agent/deployer/preflight.py` plus a `pre_flight=true` flag on
  the `deploy_to_adf` MCP tool that short-circuits the actual deploy and
  instead probes the external dependencies the linked services declare:
  Key Vault secret existence + read permission, host DNS resolution, and
  a managed-identity token-fetch against ARM. Failures carry actionable
  remediation messages (named role to grant, named tool to run). Every
  probe boundary is injectable so the test path never touches Azure or
  DNS. 19 new tests with stub clients.
- **P4-5** — Cost-actuals join helper. New module
  `ssis_modernization_agent/migration_plan/cost_actuals.py` and MCP tool **#29
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
  `ssis_modernization_agent/deployer/keyvault_uploader.py` and MCP tool **#28
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
  Anchors the `moderate` bucket in [effort-methodology.md](docs/conversion/effort-methodology.md)
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
  documentation in [behavioral-parity.md](docs/conversion/behavioral-parity.md). 27 new tests.

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
- **H4** — `parity.md` documents every check `validate_conversion_parity`
  performs, with explicit boundaries (no row-level / performance comparison).
  A worked example is captured against the LNI ADDS-MIPS-TC sample.
- `backlog.md` — buyer's-perspective evaluation findings, prioritised P0–P3
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
