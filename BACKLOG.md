# SSIS → ADF Agent — Prioritized Backlog

Derived from the skeptical-buyer evaluation (April 2026) against the LNI ADDS sample
([test-lni-packages/](../test-lni-packages/)) and the README/SETUP/HOWTO docs.

Priority legend:
- **P0** — Blocker. Tool cannot be credibly demoed until fixed.
- **P1** — High. Must land before a Conditional-Go can become a Go.
- **P2** — Medium. Required for enterprise adoption but not for first-customer pilot.
- **P3** — Nice to have / polish.

---

## P0 — Blockers

### B1. Copy Activity emits wrong source/sink types for OLEDB → FlatFile — **DONE**
- **Fix:** `_COPY_SOURCE_BY_COMPONENT` / `_COPY_SINK_BY_COMPONENT` in `data_flow_converter.py`; SQL-only sink properties gated by `_SQL_SINK_TYPES`; `sqlReaderQuery` carried through.
- **Verified:** Regenerated [PL_ADDS_MIPS_TC.json](../test-lni-packages/adf/ADDS-MIPS-TC/pipeline/PL_ADDS_MIPS_TC.json) now emits `AzureSqlSource` + `DelimitedTextSink` matching the wired datasets.

### B2. End-to-end deploy + smoke-test on the LNI sample — **CUSTOMER-SIDE PROOF**
- **Status:** Cannot be executed in this environment (no Azure subscription / factory / SHIR available). Listed as a customer-side acceptance test.
- **Acceptance unchanged:** captured deploy + smoke-test logs against a real factory for all three LNI packages.

### B3. Credentials / on-prem identifiers in cleartext pipeline variables — **DONE**
- **Fix:** `_redact_sensitive_default()` in `pipeline_generator.py` strips defaultValues whose name matches a credential keyword OR whose value matches a Windows-domain account / on-prem FQDN regex. Stripped entries get a `description` instructing the deployer to inject via Key Vault reference, pipeline parameter, or env-specific override. Azure cloud hostnames (`*.windows.net` etc.) are intentionally not flagged.
- **Verified:** [PL_ADDS_MIPS_TC.json](../test-lni-packages/adf/ADDS-MIPS-TC/pipeline/PL_ADDS_MIPS_TC.json) — `DBUserID`, `DatabaseServer`, and the LNI service-account values no longer appear as cleartext defaults; replaced by `[SENSITIVE]` description blocks. Covered by [test_pipeline_sensitive_redaction.py](../tests/test_pipeline_sensitive_redaction.py).

---

## P1 — High

### H1. Doc/reality mismatch on tool count — **DONE**
- **Resolution:** Authoritative count is **23** (verified by introspecting `mcp_server.py`'s `name=` declarations). README headline now reads "23 tools"; SETUP step 4 lists the 10 most-used tools and points to README for the full set; `.github/copilot-instructions.md` says "23 tools" and clarifies the table is a partial mapping with `mcp_server.py` as the single source of truth.

### H2. Repository ownership + version + changelog — **DONE**
- **Resolution:** README clone URL replaced with `<org>/<repo>` placeholder (the prior `chsimons_microsoft` slug was a personal alias and broke for any other consumer). LICENSE + README copyright now read `Microsoft and contributors`. Added [CHANGELOG.md](CHANGELOG.md) (Keep-a-Changelog format, semver pre-1.0 caveat documented) and [SECURITY.md](SECURITY.md) (private vulnerability reporting via GH advisories or maintainer email; hardening expectations for shared deployments; explicit out-of-scope list). `pyproject.toml` already pins `version = "0.1.0"` — left as-is, with the 0.x compatibility caveat now spelled out in CHANGELOG.

### H3. LLM Script Task translation silently no-ops on real input — **DONE**
- **Root cause:** the parser only recognised the SSIS 2008 / classic 2012 wrapper elements (`ScriptTaskProjectConfiguration` / `ScriptTask`) and only the `BinaryData` / `ProjectBytes` source patterns. Modern SSIS 2017+ packages (including the LNI estate) put a bare `<ScriptProject>` directly under `<ObjectData>` and embed the source as `<ProjectItem>` CDATA. Nothing matched, so the parser returned `script_language="CSharp"` (default) and `source_code=None`, and the LLM translator silently no-op'd.
- **Fix:** `_parse_script_task` in `ssis_parser.py` now recognises a bare `<ScriptProject>` as the config holder, reads its `Language` attribute (CSharp / VisualBasic), and `_extract_source_from_inline_project_items` concatenates `ScriptMain.{vb,cs}` and any other code-shaped `ProjectItem` CDATA. XML / project-metadata items are skipped. The LLM-skip warning in `script_task_converter.py` was rewritten to drop the "self-closing stub format" line and instead list real causes (unsupported VSTA layout, EncryptAllWithPassword, pre-2008 binary stub).
- **Verified:** [adf/ADDS-MIPS-TC/stubs/Database_Access_Configuration/__init__.py](../test-lni-packages/adf/ADDS-MIPS-TC/stubs/Database_Access_Configuration/__init__.py) regenerated — now reports `Original language: VisualBasic` and embeds the original `ScriptMain.vb` source as line comments. Covered by [test_script_task_inline_project_items.py](../tests/test_script_task_inline_project_items.py) (6 tests, including a smoke test against the real LNI sample).

### H4. Parity validation — define and demonstrate — **DONE**
- **Doc:** [PARITY.md](PARITY.md) — table-form definition of every check (`task coverage`, `linked services`, `parameters`, `data flows`, `event handlers`, `script tasks`, `SDK dry-run`, `factory reachability`), explicit list of what is *not* compared (row-level, performance, transform correctness), output schema, and a reproduction recipe.
- **Worked example:** [PARITY_REPORT_ADDS_MIPS_TC.md](../test-lni-packages/PARITY_REPORT_ADDS_MIPS_TC.md) + [PARITY_REPORT_ADDS_MIPS_TC.json](../test-lni-packages/PARITY_REPORT_ADDS_MIPS_TC.json) captured by running `validate_conversion_parity` against the LNI sample. Catches the two linked-service placeholder warnings and the two pending Script Task ports — exactly the kind of issues a buyer asked to see surfaced *before* deploy.
- **Defect-catching example:** PARITY.md "Catching a known defect" section explains how the SDK dry-run catches B1-class regressions (Copy sink/source type ≠ dataset type) before deploy.

### H5. Mark HOWTO transcripts as illustrative, not captured runs — **DONE**
- **Resolution:** [HOWTO.md](HOWTO.md) now leads with a prominent caveat block stating the dialogues are illustrative — numbers, package names, and paths inside them are constructed for clarity, and a real session will produce different output. The caveat points readers to [PARITY.md](PARITY.md#worked-example--lni-adds-mips-tc) and the captured LNI parity report for an actual recorded run on a production-shape package.

### H6. SSIS supported / partial / unsupported matrix — **DONE**
- **Resolution:** New [COVERAGE.md](COVERAGE.md) ships a complete ✅ supported / 🟡 partial / 🔴 unsupported matrix covering every Control Flow task type, every Data Flow source/transform/destination, every connection-manager kind, and package-level constructs (project params, package configurations, `.ispac`, `EncryptAllWithPassword`, package parts, parent-package variables, Kerberos / cert auth). Each row points at the dispatcher / gap-analyzer entry that is the source of truth so the doc cannot drift from code without a test failing.

### H7. Bulk trigger activation — **DONE**
- **Resolution:** New `activate_triggers` MCP tool (#24) in [`mcp_server.py`](ssis_adf_agent/mcp_server.py) + `AdfDeployer.list_triggers()` / `AdfDeployer.activate_triggers()` in [`adf_deployer.py`](ssis_adf_agent/deployer/adf_deployer.py). Defaults to `dry_run=True` so the operator must opt in to actually start triggers; per-trigger results carry status `activated` / `already_started` / `would_activate` / `not_found` / `failed`. 8 unit tests in [test_activate_triggers.py](tests/test_activate_triggers.py) cover dry-run, live activation, name filter, idempotence on already-Started, and per-trigger failure isolation.

### H8. Non-destructive re-deploy mode — **DONE**
- **Resolution:** Added `skip_if_exists` parameter to `AdfDeployer.deploy_all()` (and threaded through `_deploy_file()`) plus a new `_artifact_exists()` probe that calls the per-type `.get()` API and treats only HTTP 404 as "does not exist". `DeployResult` gained a `skipped: bool` field so callers can distinguish a no-op from a real success. The `deploy_to_adf` MCP tool now exposes `skip_if_exists` with documentation explaining the hand-edit-preservation use case. 4 unit tests in [test_skip_if_exists.py](tests/test_skip_if_exists.py) cover: default destructive behavior unchanged, existing pipeline is skipped, 404 falls through to deploy, non-404 probe errors fall through to deploy (so the real error surfaces from create_or_update).

---

## P2 — Medium

### M1. Lineage manifest — **DONE**
- **Resolution:** Every `convert_ssis_package` run now emits `lineage.json` next to the artifact tree (see [`generators/lineage_generator.py`](ssis_adf_agent/generators/lineage_generator.py)). The manifest carries source-side metadata (sha256 of the .dtsx, parsed protection level, counts), the file path of every generated artifact, and the per-activity SSIS-task origin (`ssis_task_id` / `ssis_task_name` from userProperties). `deploy_to_adf` patches it in place with full ARM resource IDs for every successfully-deployed artifact, so a single JSON file answers "where did this come from" *and* "where does it live in Azure". 7 unit tests in [test_lineage_manifest.py](tests/test_lineage_manifest.py).

### M2. ARM / azd export of ADF *content* (not just infra) — **DONE**
- **Resolution:** New `export_arm_template` MCP tool (#25) and `generators/arm_template_generator.py` produce `adf_content.arm.json` + `adf_content.parameters.json` from any ADF artifacts directory. The template assumes the factory already exists (so it composes cleanly with `infra/main.bicep` / azd) and declares each linkedService → dataset → dataflow → pipeline → trigger as a child resource with correct `dependsOn` ordering. Triggers default to `runtimeState='Stopped'` to match `deploy_to_adf` semantics. 6 unit tests in [test_arm_template_export.py](tests/test_arm_template_export.py).

### M3. Headless CI recipe — **DONE**
- **Resolution:** New [`ssis_adf_agent/cli.py`](ssis_adf_agent/cli.py) + `__main__.py` expose a headless CLI mirroring every long-running MCP tool 1:1 (`analyze` / `convert` / `validate` / `deploy` / `activate-triggers`). Each subcommand prints the same JSON the MCP tool would and exits 0 on success, 1 on `issues_found` / `failed`, 2 on a Python exception — so CI pipelines fail loudly. [CI_RECIPES.md](CI_RECIPES.md) ships GitHub Actions and Azure DevOps recipes. 8 unit tests in [test_cli.py](tests/test_cli.py).

### M4. Cost estimate calibration — **BLOCKED (customer-side)**
- **Why blocked:** Acceptance requires a deployed pipeline running ≥30 days against real data — that is customer-time, not engineering-time. The agent ships everything needed: `estimate_adf_costs` produces the prediction, `lineage.json` (M1) anchors every Azure resource id so actuals can be pulled from Cost Management and joined back. Once a customer ships the first 30-day actual-vs-estimated comparison, that data feeds M4 closure and a revision of [EFFORT_METHODOLOGY.md](EFFORT_METHODOLOGY.md).

### M5. Effort range tightening / methodology disclosure — **DONE**
- **Resolution:** [EFFORT_METHODOLOGY.md](EFFORT_METHODOLOGY.md) documents the full per-package formula (bucket bases, Script Task / Data Flow weighting, the 0.5h-per-simplification rebalance, the asymmetric –30 / +60% envelope) plus the wave-level adjustments (`apply_learning_curve`, `estate_setup_hours`). The doc cites `migration_plan/proposer.py` line by line as the source of truth and explicitly invites customers to file calibration data when actuals fall outside the band — that data feeds M4.

### M6. EncryptAllWithPassword end-to-end recipe — **DONE**
- **Resolution:** [ENCRYPTED_PACKAGES.md](ENCRYPTED_PACKAGES.md) ships a six-step recipe: dtutil decrypt to a working folder, `az keyvault secret set` per (package, connection-manager) with a documented naming convention, re-convert against the *original* encrypted .dtsx with `use_key_vault=true`, edit secret-name placeholders, deploy with `skip_if_exists=true` (H8) so it doesn't stomp on hand-edited factory artifacts, then clean the working folder. Includes a reviewer checklist and cross-references SECURITY.md / B3 / H8.

### M7. Custom-component / 3rd-party substitution registry — **DONE**
- **Resolution:** New [`converters/substitution_registry.py`](ssis_adf_agent/converters/substitution_registry.py) plus a `substitution_registry_path` parameter on `convert_ssis_package`. A small JSON registry maps SSIS Data Flow `component_type` (e.g. `Cozyroc.SSISPlus.SuperLookupTask`) to a specific ADF MDF transformation type with optional `type_properties`. Substitutions short-circuit both the dispatcher *and* the `_unsupported` placeholder, so a customer can stop manually rewriting the same handful of components on every estate refresh. Schema documented in [docs/SUBSTITUTION_REGISTRY.md](docs/SUBSTITUTION_REGISTRY.md). 9 unit tests in [test_substitution_registry.py](tests/test_substitution_registry.py).

### M8. Estate-scale evidence (≥100 packages) — **BLOCKED (customer-side)**
- **Why blocked:** Acceptance requires running `bulk_analyze` + `convert_estate` against a public ≥100-package corpus and reporting runtime/memory/dedup numbers. We don't currently have a public corpus that size that can be redistributed; the LNI 3-package set we ship is the largest sanitized sample in the tree. The tooling needed to produce the evidence (`bulk_analyze`, `convert_estate`, `consolidate_packages`) all ships and is unit-tested. Once a customer or partner can host a sanitized 100+ package corpus we can run the harness and publish numbers.

---

## P3 — Polish

### N1. Cross-pipeline regression harness
- **Acceptance:** Smoke-test multiple pipelines in a wave with a single call; aggregate report.

### N2. Rollback story
- **Acceptance:** Documented rollback flow (delete / soft-revert / branch-restore).

### N3. Naming-convention configurability
- **Acceptance:** Customer-tunable prefix/suffix patterns for generated artifacts.

---

## Suggested execution order

1. **B1** ✅ done (regenerated Copy types correctly on LNI).
2. **B3** ✅ done (sensitive defaults stripped from generated pipelines).
3. **B2** — customer-side proof; cannot be executed without an Azure factory.
4. **H3** (LLM translator on LNI dialect) — same sample, same regen pass.
5. **H4** (parity report) — produce on the deployed pipeline.
6. **H1**, **H5**, **H2** (doc/repo hygiene) — fast wins, can parallelize.
7. **H6, H7, H8** — flesh out the deliverable story.
8. **P2** items as adoption progresses.
