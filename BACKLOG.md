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

### N1. Cross-pipeline regression harness — **DONE**
- **Resolution:** New `smoke_test_wave` MCP tool (#26) wraps `smoke_test_pipeline` across many pipelines. Accepts either an explicit `pipeline_names` list or auto-discovers from an `artifacts_dir/pipeline/*.json` set. Returns aggregated `summary` (total / succeeded / failed / cancelled / timed_out / errored / skipped) plus full per-pipeline results. `stop_on_failure=true` short-circuits the rest of the wave for sign-off gates. 6 unit tests in [test_smoke_test_wave.py](tests/test_smoke_test_wave.py).

### N2. Rollback story — **DONE**
- **Resolution:** [ROLLBACK.md](ROLLBACK.md) ships three named strategies tied to blast radius: per-artifact delete via `lineage.json` (mid-flight failures, leverages M1), soft-revert single pipeline via git history + targeted re-deploy, and full git-worktree restore + force re-deploy for wave-scale rollbacks. Includes pre-flight checklist (deploy tag, dry-run triggers, predeployment report) and a special-case section on triggers (always Stopped on deploy by design — H7).

### N3. Naming-convention configurability — **DONE**
- **Resolution:** Naming helpers (`ds_name`, `df_name`, `pl_name`, `tr_name`, `ls_name_for_cm`, `build_ls_name_map`) now honor `{LS,DS,DF,PL,TR}_PREFIX` keys in `name_overrides` to swap the default `LS_/DS_/DF_/PL_/TR_` prefixes globally for a conversion. Empty string drops the prefix entirely. Per-artifact overrides still win over prefix overrides. Documented in the module docstring of [generators/naming.py](ssis_adf_agent/generators/naming.py); 8 unit tests in [test_naming_prefix_overrides.py](tests/test_naming_prefix_overrides.py).

---

## P4 — Skeptical-buyer follow-ups (April 22 2026)

Captured from the buyer-evaluation review against the closed-out P0–P3 backlog.
Each item is the *vendor-actionable* slice of a buyer concern — customer-side
proof items (live deploy log, 30-day cost actuals, ≥100-package corpus,
named-customer reference) remain on B2 / M4 / M8 and are not duplicated here.

### P4-1. Behavioral parity harness — **DONE**
- **Resolution:** New `compare_dataflow_output` MCP tool (#27) backed by the [`ssis_adf_agent.parity`](ssis_adf_agent/parity/) package: a pure row-and-column [`diff_rows`](ssis_adf_agent/parity/diff.py) engine, pluggable [`SSISDataFlowRunner`](ssis_adf_agent/parity/runners.py) / [`AdfDataFlowRunner`](ssis_adf_agent/parity/runners.py) protocols with three concrete impls (`DtexecRunner`, `AdfDebugRunner`, `CapturedOutputRunner`), an [orchestrator](ssis_adf_agent/parity/orchestrator.py), and a [Markdown report](ssis_adf_agent/parity/report.py). Three modes (`captured` / `live` / `mixed`) so the harness is usable without dtexec or live ADF — captured CSVs make it run in CI in sub-second time.
- **Worked example:** [BEHAVIORAL_PARITY.md](BEHAVIORAL_PARITY.md) + [tests/fixtures/dataflow_parity/](tests/fixtures/dataflow_parity/) — sales DF with 6 rows, three CSVs (input, correct ADF output, regressed ADF output that drops a discount and mis-tiers a row). [test_dataflow_parity_worked_example.py](tests/test_dataflow_parity_worked_example.py) asserts the correct conversion passes and the regressed conversion fails with exactly the expected two value mismatches — proving the harness catches real regressions.
- **Tests:** 27 new tests across [test_dataflow_parity_diff.py](tests/test_dataflow_parity_diff.py) (16 — pure diff engine), [test_dataflow_parity_orchestrator.py](tests/test_dataflow_parity_orchestrator.py) (9 — runners + orchestrator + MCP handler), [test_dataflow_parity_worked_example.py](tests/test_dataflow_parity_worked_example.py) (2). PARITY.md cross-links to BEHAVIORAL_PARITY.md so the structural-vs-behavioral split is signposted.

### P4-2. Vendor-curated substitution registry entries — **HIGH** ✅ DONE
- **Buyer concern:** M7 ships the *mechanism* but zero curated entries for popular paid components. Customers are expected to author every entry.
- **Acceptance:** At least three vendor-authored registry files under `registries/` covering Cozyroc Salesforce, KingswaySoft Dynamics CRM, and Pragmatic Works Productivity Pack family. Each accompanied by a unit test demonstrating the substitution against a captured component XML fragment.
- **Resolution:** Three curated registries shipped under [`registries/`](registries/README.md): `cozyroc_salesforce.json` (Salesforce Source/Destination/Lookup, Bulk variants, Salesforce Task), `kingswaysoft_dynamics.json` (CRM Source/Destination/Lookup/OptionSet, Premium Derived Column, Premium Lookup, Retrieve Data Task), `pragmatic_works.json` (Task Factory Upsert Destination, Dimension Merge SCD, Regex Replace, Advanced Derived Column, Data Validation, Aggregate, Advanced E-Mail Task, Secure FTP Task, Terminate Process Task, Compression Task, REST Source Task — both `Pragmaticworks` and `PragmaticWorks` namespace casings). Each entry carries a `_review_required` marker for non-trivial mappings (Type 2 SCD, Upsert externalIdFieldName, ExecuteWorkflow action, etc.). 15 tests in `tests/test_vendor_registries.py` exercise loader, structural sanity (every non-trivial entry must carry a review marker), captured-fragment routing for all three vendors, namespace-alias handling, and cross-registry collision detection. SUBSTITUTION_REGISTRY.md cross-links to the catalog.

### P4-3. Worked Script Task port in the repo — **HIGH** ✅ DONE
- **Buyer concern:** Stub generation (H3) lifts source as comments but the *port* is opaque. EFFORT_METHODOLOGY.md weights Script Tasks but the buckets (trivial / simple / moderate / complex) are unbacked by published examples.
- **Acceptance:** One of the LNI Script Tasks ported end-to-end (VB → Python Function), check the finished `__init__.py` into the repo, document the hours spent in [EFFORT_METHODOLOGY.md](EFFORT_METHODOLOGY.md), and link from [COVERAGE.md](COVERAGE.md) Script Task rows.
- **Resolution:** Ported `Database_Access_Configuration` (LNI ADDS-MIPS-TC) end-to-end at [docs/case-studies/script_task_port_database_access_configuration/](docs/case-studies/script_task_port_database_access_configuration/README.md). Ships `original_script.vb` (verbatim source), production `__init__.py` (Key-Vault-backed, parameterized-LS-friendly), `function.json`, `requirements.txt`, and a 6-section README with the mapping decisions table, hours breakdown (predicted 3.2h vs actual 3.5h), and gotchas for buyers (linked-service mutation, cleartext passwords, `MsgBox` debug shims, `DateTime.Now` timezone). 11 tests in `tests/test_script_task_port_database_access_configuration.py` cover all branches without needing the Functions runtime. EFFORT_METHODOLOGY.md and COVERAGE.md cross-link to the case study.

### P4-4. Encrypted-package automation helper — **MEDIUM**
- **Buyer concern:** ENCRYPTED_PACKAGES.md is a 6-step manual recipe. Doing it for 50 encrypted packages by hand is error-prone and a security review hot spot.
- **Acceptance:** New helper module (working name `deployer/keyvault_uploader.py`) plus an MCP / CLI entry point that reads a sensitivity map produced by the existing parser, pushes secrets via `azure-keyvault-secrets`, and rewrites the linked-service placeholder secret names in one shot. Unit-tested with mocked `SecretClient`.

### P4-5. Cost-actuals join helper — **MEDIUM**
- **Buyer concern:** `lineage.json` (M1) anchors every Azure resource ID and `estimate_adf_costs` produces a prediction, but nothing joins them to actuals. M4 is blocked on customer time, but the join helper is not.
- **Acceptance:** New tool `compare_estimates_to_actuals` reads `lineage.json` + a Cost Management export (CSV or REST) and emits a per-pipeline / per-resource variance report. Unit-tested with a captured Cost Management response fixture.

### P4-6. Deeper deploy dry-run — **MEDIUM**
- **Buyer concern:** SDK dry-run only validates JSON shape. The failure modes that consume real migration weeks (SHIR connectivity, Key Vault permission gaps, regional quotas, host firewalls) are not caught until the live deploy.
- **Acceptance:** New tool / flag (`deploy_to_adf --pre-flight`) that resolves Key Vault references, attempts a managed-identity token-fetch against each linked-service host, and reports per-target reachability / permission status without creating ADF resources.

### P4-7. Published RBAC / least-privilege matrix — **MEDIUM**
- **Buyer concern:** SECURITY.md does not enumerate the precise ARM roles + Key Vault data-plane permissions required by the deploying identity per tool. Security review will reject "Owner on the resource group."
- **Acceptance:** New `RBAC.md` table mapping each MCP / CLI command (`provision_adf_environment`, `deploy_to_adf`, `activate_triggers`, `provision_function_app`, `export_arm_template`, etc.) to required Azure RBAC roles + KV access policies. Cross-linked from SECURITY.md and SETUP.md.

### P4-8. No-LLM mode statement + switch — **MEDIUM**
- **Buyer concern:** Regulated customers cannot route .dtsx contents through a public LLM endpoint and need a documented opt-out with explicit feature delta.
- **Acceptance:** `--no-llm` flag (or env var) on `convert_ssis_package` that forces the Script Task translator to skip the OpenAI call entirely. SECURITY.md gains a "What the LLM translator sends, where, and how to disable" section enumerating exactly what is degraded (Script Task port quality only).

### P4-9. Minimum useful workflow guide — **MEDIUM**
- **Buyer concern:** 26 MCP tools is a large surface. New engineers explore all of them. Tool overlap (`smoke_test_pipeline` vs `smoke_test_wave`, `convert_ssis_package` vs `convert_estate`, `build_estate_report` vs `build_predeployment_report`) is not signposted.
- **Acceptance:** New `WORKFLOW.md` (or section of HOWTO.md) naming the 5–6-tool minimum path tied to standard wave milestones (triage → propose → convert → validate → deploy → activate). Clarifies which tools are advanced / optional. Linked from README.

### P4-10. Pipeline-execution observability story — **MEDIUM**
- **Buyer concern:** Post-migration BAU operations have no documented monitoring story (Log Analytics workbook, standard alerts, run-history retention).
- **Acceptance:** `OBSERVABILITY.md` documenting the recommended diagnostic-settings target, a sample KQL workbook for pipeline failures + duration trending, and at least three suggested alert rules with thresholds.

### P4-11. Captured "first deploy that failed" recovery doc — **LOW**
- **Buyer concern:** ROLLBACK.md is theoretical. A captured real failure-and-recovery (sanitized) builds trust no prose can.
- **Acceptance:** One captured deploy that failed (any failure mode — KV permission, SHIR offline, name collision), the error log, and the steps used to recover, written up under `docs/case-studies/` and linked from ROLLBACK.md.

### P4-12. COVERAGE.md per-row evidence links — **LOW**
- **Buyer concern:** ✅ rows in COVERAGE.md cite the dispatcher but not a captured artifact. A skeptic asked for cell-level evidence.
- **Acceptance:** Each ✅ / 🟡 row in COVERAGE.md gains a "Sample" column linking to a captured generated artifact (or a unit-test fixture) demonstrating the conversion.

### P4-13. Public roadmap + 1.0 milestone definition — **LOW**
- **Buyer concern:** Version is 0.1.0 with a semver pre-1.0 caveat; no signal of when 1.0 lands or what it means.
- **Acceptance:** New `ROADMAP.md` listing the engineering items required for 1.0, the current quarter's focus, and the breaking-change-deprecation window for pre-1.0 → 1.0 transitions.

### P4-14. Named support channel + response-time commitment — **LOW**
- **Buyer concern:** "GitHub issues" is not a support channel for a customer mid-migration at 11 p.m.
- **Acceptance:** SUPPORT.md naming the support channel (alias / Teams channel / on-call rotation) with a stated response-time commitment for the duration of an active engagement. Acknowledges this may differ between OSS users and engaged customers.

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
9. **P3** ✅ done (smoke wave, rollback, naming config).
10. **P4 — buyer follow-ups**, ordered for maximum trust gain per unit work:
    1. **P4-1** behavioral parity harness (clears the #2 blocker on the buyer review).
    2. **P4-3** worked Script Task port (anchors the effort buckets in real numbers).
    3. **P4-2** vendor-curated registry entries (closes the "bring your own mappings" gap for the most common 3rd-party components).
    4. **P4-7** RBAC matrix + **P4-8** no-LLM mode (parallel; security-review unblockers).
    5. **P4-9** minimum useful workflow guide + **P4-10** observability story (parallel; onboarding + BAU).
    6. **P4-4** encrypted-package automation, **P4-5** cost-actuals join, **P4-6** deeper deploy dry-run (parallel; quality-of-life).
    7. **P4-11** captured failure-recovery doc, **P4-12** per-row evidence links, **P4-13** roadmap, **P4-14** support channel (low-cost trust polish).
