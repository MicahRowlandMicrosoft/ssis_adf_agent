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
- **Doc:** [parity.md](../conversion/parity.md) — table-form definition of every check (`task coverage`, `linked services`, `parameters`, `data flows`, `event handlers`, `script tasks`, `SDK dry-run`, `factory reachability`), explicit list of what is *not* compared (row-level, performance, transform correctness), output schema, and a reproduction recipe.
- **Worked example:** [PARITY_REPORT_ADDS_MIPS_TC.md](../test-lni-packages/PARITY_REPORT_ADDS_MIPS_TC.md) + [PARITY_REPORT_ADDS_MIPS_TC.json](../test-lni-packages/PARITY_REPORT_ADDS_MIPS_TC.json) captured by running `validate_conversion_parity` against the LNI sample. Catches the two linked-service placeholder warnings and the two pending Script Task ports — exactly the kind of issues a buyer asked to see surfaced *before* deploy.
- **Defect-catching example:** PARITY.md "Catching a known defect" section explains how the SDK dry-run catches B1-class regressions (Copy sink/source type ≠ dataset type) before deploy.

### H5. Mark HOWTO transcripts as illustrative, not captured runs — **DONE**
- **Resolution:** [howto.md](../getting-started/howto.md) now leads with a prominent caveat block stating the dialogues are illustrative — numbers, package names, and paths inside them are constructed for clarity, and a real session will produce different output. The caveat points readers to [parity.md](../conversion/parity.md#worked-example--lni-adds-mips-tc) and the captured LNI parity report for an actual recorded run on a production-shape package.

### H6. SSIS supported / partial / unsupported matrix — **DONE**
- **Resolution:** New [coverage.md](../conversion/coverage.md) ships a complete ✅ supported / 🟡 partial / 🔴 unsupported matrix covering every Control Flow task type, every Data Flow source/transform/destination, every connection-manager kind, and package-level constructs (project params, package configurations, `.ispac`, `EncryptAllWithPassword`, package parts, parent-package variables, Kerberos / cert auth). Each row points at the dispatcher / gap-analyzer entry that is the source of truth so the doc cannot drift from code without a test failing.

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
- **Resolution:** New [`ssis_adf_agent/cli.py`](ssis_adf_agent/cli.py) + `__main__.py` expose a headless CLI mirroring every long-running MCP tool 1:1 (`analyze` / `convert` / `validate` / `deploy` / `activate-triggers`). Each subcommand prints the same JSON the MCP tool would and exits 0 on success, 1 on `issues_found` / `failed`, 2 on a Python exception — so CI pipelines fail loudly. [ci-recipes.md](ci-recipes.md) ships GitHub Actions and Azure DevOps recipes. 8 unit tests in [test_cli.py](tests/test_cli.py).

### M4. Cost estimate calibration — **BLOCKED (customer-side)**
- **Why blocked:** Acceptance requires a deployed pipeline running ≥30 days against real data — that is customer-time, not engineering-time. The agent ships everything needed: `estimate_adf_costs` produces the prediction, `lineage.json` (M1) anchors every Azure resource id so actuals can be pulled from Cost Management and joined back. Once a customer ships the first 30-day actual-vs-estimated comparison, that data feeds M4 closure and a revision of [effort-methodology.md](../conversion/effort-methodology.md).

### M5. Effort range tightening / methodology disclosure — **DONE**
- **Resolution:** [effort-methodology.md](../conversion/effort-methodology.md) documents the full per-package formula (bucket bases, Script Task / Data Flow weighting, the 0.5h-per-simplification rebalance, the asymmetric –30 / +60% envelope) plus the wave-level adjustments (`apply_learning_curve`, `estate_setup_hours`). The doc cites `migration_plan/proposer.py` line by line as the source of truth and explicitly invites customers to file calibration data when actuals fall outside the band — that data feeds M4.

### M6. EncryptAllWithPassword end-to-end recipe — **DONE**
- **Resolution:** [encrypted-packages.md](../operations/encrypted-packages.md) ships a six-step recipe: dtutil decrypt to a working folder, `az keyvault secret set` per (package, connection-manager) with a documented naming convention, re-convert against the *original* encrypted .dtsx with `use_key_vault=true`, edit secret-name placeholders, deploy with `skip_if_exists=true` (H8) so it doesn't stomp on hand-edited factory artifacts, then clean the working folder. Includes a reviewer checklist and cross-references SECURITY.md / B3 / H8.

### M7. Custom-component / 3rd-party substitution registry — **DONE**
- **Resolution:** New [`converters/substitution_registry.py`](ssis_adf_agent/converters/substitution_registry.py) plus a `substitution_registry_path` parameter on `convert_ssis_package`. A small JSON registry maps SSIS Data Flow `component_type` (e.g. `Cozyroc.SSISPlus.SuperLookupTask`) to a specific ADF MDF transformation type with optional `type_properties`. Substitutions short-circuit both the dispatcher *and* the `_unsupported` placeholder, so a customer can stop manually rewriting the same handful of components on every estate refresh. Schema documented in [docs/SUBSTITUTION_REGISTRY.md](docs/SUBSTITUTION_REGISTRY.md). 9 unit tests in [test_substitution_registry.py](tests/test_substitution_registry.py).

### M8. Estate-scale evidence (≥100 packages) — **BLOCKED (customer-side)**
- **Why blocked:** Acceptance requires running `bulk_analyze` + `convert_estate` against a public ≥100-package corpus and reporting runtime/memory/dedup numbers. We don't currently have a public corpus that size that can be redistributed; the LNI 3-package set we ship is the largest sanitized sample in the tree. The tooling needed to produce the evidence (`bulk_analyze`, `convert_estate`, `consolidate_packages`) all ships and is unit-tested. Once a customer or partner can host a sanitized 100+ package corpus we can run the harness and publish numbers.

---

## P3 — Polish

### N1. Cross-pipeline regression harness — **DONE**
- **Resolution:** New `smoke_test_wave` MCP tool (#26) wraps `smoke_test_pipeline` across many pipelines. Accepts either an explicit `pipeline_names` list or auto-discovers from an `artifacts_dir/pipeline/*.json` set. Returns aggregated `summary` (total / succeeded / failed / cancelled / timed_out / errored / skipped) plus full per-pipeline results. `stop_on_failure=true` short-circuits the rest of the wave for sign-off gates. 6 unit tests in [test_smoke_test_wave.py](tests/test_smoke_test_wave.py).

### N2. Rollback story — **DONE**
- **Resolution:** [rollback.md](../operations/rollback.md) ships three named strategies tied to blast radius: per-artifact delete via `lineage.json` (mid-flight failures, leverages M1), soft-revert single pipeline via git history + targeted re-deploy, and full git-worktree restore + force re-deploy for wave-scale rollbacks. Includes pre-flight checklist (deploy tag, dry-run triggers, predeployment report) and a special-case section on triggers (always Stopped on deploy by design — H7).

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
- **Worked example:** [behavioral-parity.md](../conversion/behavioral-parity.md) + [tests/fixtures/dataflow_parity/](tests/fixtures/dataflow_parity/) — sales DF with 6 rows, three CSVs (input, correct ADF output, regressed ADF output that drops a discount and mis-tiers a row). [test_dataflow_parity_worked_example.py](tests/test_dataflow_parity_worked_example.py) asserts the correct conversion passes and the regressed conversion fails with exactly the expected two value mismatches — proving the harness catches real regressions.
- **Tests:** 27 new tests across [test_dataflow_parity_diff.py](tests/test_dataflow_parity_diff.py) (16 — pure diff engine), [test_dataflow_parity_orchestrator.py](tests/test_dataflow_parity_orchestrator.py) (9 — runners + orchestrator + MCP handler), [test_dataflow_parity_worked_example.py](tests/test_dataflow_parity_worked_example.py) (2). PARITY.md cross-links to BEHAVIORAL_PARITY.md so the structural-vs-behavioral split is signposted.

### P4-2. Vendor-curated substitution registry entries — **HIGH** ✅ DONE
- **Buyer concern:** M7 ships the *mechanism* but zero curated entries for popular paid components. Customers are expected to author every entry.
- **Acceptance:** At least three vendor-authored registry files under `registries/` covering Cozyroc Salesforce, KingswaySoft Dynamics CRM, and Pragmatic Works Productivity Pack family. Each accompanied by a unit test demonstrating the substitution against a captured component XML fragment.
- **Resolution:** Three curated registries shipped under [`registries/`](registries/README.md): `cozyroc_salesforce.json` (Salesforce Source/Destination/Lookup, Bulk variants, Salesforce Task), `kingswaysoft_dynamics.json` (CRM Source/Destination/Lookup/OptionSet, Premium Derived Column, Premium Lookup, Retrieve Data Task), `pragmatic_works.json` (Task Factory Upsert Destination, Dimension Merge SCD, Regex Replace, Advanced Derived Column, Data Validation, Aggregate, Advanced E-Mail Task, Secure FTP Task, Terminate Process Task, Compression Task, REST Source Task — both `Pragmaticworks` and `PragmaticWorks` namespace casings). Each entry carries a `_review_required` marker for non-trivial mappings (Type 2 SCD, Upsert externalIdFieldName, ExecuteWorkflow action, etc.). 15 tests in `tests/test_vendor_registries.py` exercise loader, structural sanity (every non-trivial entry must carry a review marker), captured-fragment routing for all three vendors, namespace-alias handling, and cross-registry collision detection. SUBSTITUTION_REGISTRY.md cross-links to the catalog.

### P4-3. Worked Script Task port in the repo — **HIGH** ✅ DONE
- **Buyer concern:** Stub generation (H3) lifts source as comments but the *port* is opaque. EFFORT_METHODOLOGY.md weights Script Tasks but the buckets (trivial / simple / moderate / complex) are unbacked by published examples.
- **Acceptance:** One of the LNI Script Tasks ported end-to-end (VB → Python Function), check the finished `__init__.py` into the repo, document the hours spent in [effort-methodology.md](../conversion/effort-methodology.md), and link from [coverage.md](../conversion/coverage.md) Script Task rows.
- **Resolution:** Ported `Database_Access_Configuration` (LNI ADDS-MIPS-TC) end-to-end at [docs/case-studies/script_task_port_database_access_configuration/](docs/case-studies/script_task_port_database_access_configuration/README.md). Ships `original_script.vb` (verbatim source), production `__init__.py` (Key-Vault-backed, parameterized-LS-friendly), `function.json`, `requirements.txt`, and a 6-section README with the mapping decisions table, hours breakdown (predicted 3.2h vs actual 3.5h), and gotchas for buyers (linked-service mutation, cleartext passwords, `MsgBox` debug shims, `DateTime.Now` timezone). 11 tests in `tests/test_script_task_port_database_access_configuration.py` cover all branches without needing the Functions runtime. EFFORT_METHODOLOGY.md and COVERAGE.md cross-link to the case study.

### P4-4. Encrypted-package automation helper — **MEDIUM** ✅ DONE
- **Buyer concern:** ENCRYPTED_PACKAGES.md is a 6-step manual recipe. Doing it for 50 encrypted packages by hand is error-prone and a security review hot spot.
- **Acceptance:** New helper module (working name `deployer/keyvault_uploader.py`) plus an MCP / CLI entry point that reads a sensitivity map produced by the existing parser, pushes secrets via `azure-keyvault-secrets`, and rewrites the linked-service placeholder secret names in one shot. Unit-tested with mocked `SecretClient`.
- **Resolution:** New `ssis_adf_agent/deployer/keyvault_uploader.py` automates Steps 2 + 4 of the recipe end-to-end. Pure-Python boundaries: `extract_secrets_from_dtsx` (XML walk — pulls direct `Password` properties, embedded `Password=...` substrings, and `Sensitive="1"` package/project parameters), `build_secret_map` (templated naming + KV-safe slugification), `upload_secrets` (talks to a `SecretClientProtocol` so tests pass a fake; `dry_run` and `overwrite` flags), `rewrite_linked_services` (recursive walk + targeted `secretName` substitution; `dry_run` previews without writing). Top-level `process_encrypted_package` orchestrator wires them together. New MCP tool **#28 `upload_encrypted_secrets`** + ENCRYPTED_PACKAGES.md "Automation via MCP" section. 26 tests in `tests/test_keyvault_uploader.py` covering extraction (all 3 secret shapes, no-secrets, invalid XML), name templating + slugification, upload semantics (new/existing/overwrite/dry-run/error routing), rewrite (matching/non-matching/recursive/dry-run/missing-dir/invalid-JSON), end-to-end orchestrator, and a `__repr__`-redaction sanity check so secrets cannot leak into logs.

### P4-5. Cost-actuals join helper — **MEDIUM** ✅ DONE
- **Buyer concern:** `lineage.json` (M1) anchors every Azure resource ID and `estimate_adf_costs` produces a prediction, but nothing joins them to actuals. M4 is blocked on customer time, but the join helper is not.
- **Acceptance:** New tool `compare_estimates_to_actuals` reads `lineage.json` + a Cost Management export (CSV or REST) and emits a per-pipeline / per-resource variance report. Unit-tested with a captured Cost Management response fixture.
- **Resolution:** New `ssis_adf_agent/migration_plan/cost_actuals.py` with `load_actuals` (accepts a Cost Management Query REST response dict, REST JSON file, portal Cost Analysis CSV export, or pre-normalized list) and `compare_estimates_to_actuals` (resolves the factory ARM id from the deployed lineage manifest, filters actuals by prefix-match so sub-resource rows like `.../integrationruntimes/...` roll up, computes variance vs. the optional `estimate_adf_costs` baseline, and emits an estimated per-pipeline allocation weighted by `activity_count` from the manifest). Allocation rows are explicitly tagged `allocation: "estimated"` and a note is appended explaining Cost Management does not invoice ADF spend below factory granularity — buyers do not get to confuse the allocation with billed truth. New MCP tool **#29 `compare_estimates_to_actuals`**. 29 tests in `tests/test_cost_actuals.py` covering both REST and CSV input shapes (with captured fixtures), variance math, currency-mixing detection, factory-id resolution + override, the legacy single-dict pipeline manifest shape, and the unresolved-factory / no-matching-actuals / zero-weight edge cases.

### P4-6. Deeper deploy dry-run — **MEDIUM** ✅ DONE
- **Buyer concern:** SDK dry-run only validates JSON shape. The failure modes that consume real migration weeks (SHIR connectivity, Key Vault permission gaps, regional quotas, host firewalls) are not caught until the live deploy.
- **Acceptance:** New tool / flag (`deploy_to_adf --pre-flight`) that resolves Key Vault references, attempts a managed-identity token-fetch against each linked-service host, and reports per-target reachability / permission status without creating ADF resources.
- **Resolution:** New `ssis_adf_agent/deployer/preflight.py` with `extract_dependencies` (pure-Python walk of every linked-service JSON — indexes AzureKeyVault linked services to resolve `baseUrl`, then collects `AzureKeyVaultSecret` refs and host strings from `connectionString` / `host` / `endpoint` / `url` / `server` properties) and `run_preflight` (orchestrator). Three probe classes — KV secret existence + read permission, host DNS resolution, and a managed-identity token-fetch against ARM — each runs through an injectable boundary (`secret_client_factory`, `dns_resolver`, `credential`) so the test path never touches Azure or DNS. Per-check `status` is `pass` / `fail` / `warn` / `skipped`; failures carry actionable messages naming the exact remediation (`upload_encrypted_secrets` for missing secrets, `Key Vault Secrets User` role for forbidden, `private DNS / firewall` for unresolved hosts, `az login` for token-fetch failures). Parameterized hosts (`@{...}` / `${...}`) are deliberately classified `skipped` rather than `fail`. Repeat hosts are de-duplicated. Wired into the existing `deploy_to_adf` MCP tool via a new `pre_flight=true` flag (plus `preflight_skip_kv` / `preflight_skip_dns` / `preflight_skip_mi_token` / `preflight_report_path` knobs) that short-circuits before the deployer is constructed — no ADF resources are created. 19 tests in `tests/test_preflight.py` cover extraction (KV resolution / orphan refs / host de-dup / invalid JSON / missing dir), every probe outcome (pass / missing / forbidden / unresolved-host / DNS-error / parameterized / MI-token failure), the unresolved-baseUrl short-circuit (does not construct a client), `skip_*` flags, and the `to_dict` round-trip.

### P4-7. Published RBAC / least-privilege matrix — **MEDIUM** ✅ DONE
- **Buyer concern:** SECURITY.md does not enumerate the precise ARM roles + Key Vault data-plane permissions required by the deploying identity per tool. Security review will reject "Owner on the resource group."
- **Acceptance:** New `rbac.md` table mapping each MCP / CLI command (`provision_adf_environment`, `deploy_to_adf`, `activate_triggers`, `provision_function_app`, `export_arm_template`, etc.) to required Azure RBAC roles + KV access policies. Cross-linked from SECURITY.md and SETUP.md.
- **Resolution:** New top-level [`rbac.md`](../operations/rbac.md) carries a per-tool minimum-permissions matrix covering all 29 MCP tools (including the new P4 additions — `compare_dataflow_output`, `upload_encrypted_secrets`, `compare_estimates_to_actuals`, plus the `deploy_to_adf --pre_flight` pre-flight path). Each row names the Azure RBAC role(s), the scope, the matching Key Vault data-plane permission where relevant, and a notes column flagging cases like the `User Access Administrator` requirement on `provision_adf_environment` (only needed when the plan declares RBAC assignments) and the `assign_rbac=false` escape hatch when the reviewer disallows UAA on the deploy identity. Doc also explains *why* each role was chosen (Data Factory Contributor, Key Vault Secrets Officer / User, Cost Management Reader), enumerates the `az role assignment create` snippets, calls out a deliberate non-list of "never required" roles (Owner, sub-scoped Contributor, Network Contributor, Storage Account Contributor), and ends with an audit checklist (`az role assignment list --assignee --all`) the reviewer can run before approving the change ticket. Cross-linked from [SECURITY.md](SECURITY.md) (hardening section) and [setup.md](../getting-started/setup.md) (auth section).

### P4-8. No-LLM mode statement + switch — **MEDIUM** ✅ DONE
- **Buyer concern:** Regulated customers cannot route .dtsx contents through a public LLM endpoint and need a documented opt-out with explicit feature delta.
- **Acceptance:** `--no-llm` flag (or env var) on `convert_ssis_package` that forces the Script Task translator to skip the OpenAI call entirely. SECURITY.md gains a "What the LLM translator sends, where, and how to disable" section enumerating exactly what is degraded (Script Task port quality only).
- **Resolution:** Three mutually-reinforcing kill switches now disable the LLM call: (1) `llm_translate=false` (default) on `convert_ssis_package`, (2) new `no_llm=true` per-call argument that overrides `llm_translate` for one tool call, and (3) new process-wide `SSIS_ADF_NO_LLM` env var (truthy: `1`/`true`/`yes`/`on`, case-insensitive) that forces `is_configured()` to return False and `translate()` to raise `TranslationError` *before* any client is constructed. The translator's `no_llm_policy_enabled()` helper is the single source of truth used by the Script Task converter and the MCP tool layer; when the env var or the per-call arg is on but `llm_translate=true` was requested, the MCP layer emits a UserWarning naming which switch overrode the request so the degraded behaviour is never silent. SECURITY.md gained a new "What the LLM translator sends, where, and how to disable" section that enumerates exactly which fields are transmitted (system prompt + task name + variable identifiers + `source_code` truncated at 18 000 chars), where they go (the customer's own Azure OpenAI deployment, authenticated via `DefaultAzureCredential`), and what is *not* transmitted (no telemetry, no third party, no connection-manager values). The section also documents that disabling the LLM degrades only the *quality of the generated Python body* — every other artifact (pipelines, linked services, datasets, data flows, triggers) is bit-for-bit identical with or without the LLM. RBAC.md row for `convert_ssis_package` now also names the *Cognitive Services OpenAI User* role on the Azure OpenAI resource as the only RBAC needed when the LLM is on, with a pointer to `SSIS_ADF_NO_LLM=1` as the escape hatch. New `tests/test_no_llm_mode.py` (20 tests) verifies the helper across truthy/falsy values, that `is_configured()` returns False under policy even with a configured endpoint, that `translate()` raises with a "disabled by policy" message, and that the Script Task converter emits a clean UserWarning naming the env var. 877 tests passing.

### P4-9. Minimum useful workflow guide — **MEDIUM** ✅ DONE
- **Buyer concern:** 26 MCP tools is a large surface. New engineers explore all of them. Tool overlap (`smoke_test_pipeline` vs `smoke_test_wave`, `convert_ssis_package` vs `convert_estate`, `build_estate_report` vs `build_predeployment_report`) is not signposted.
- **Acceptance:** New `workflow.md` (or section of HOWTO.md) naming the 5–6-tool minimum path tied to standard wave milestones (triage → propose → convert → validate → deploy → activate). Clarifies which tools are advanced / optional. Linked from README.
- **Resolution:** New top-level [workflow.md](../getting-started/workflow.md) names the **6-tool minimum path** (`bulk_analyze` → `propose_adf_design` → `convert_estate` → `validate_adf_artifacts` → `deploy_to_adf` (with `pre_flight=true` first) → `activate_triggers`) tied to the standard wave milestones (triage → design → convert → validate → deploy → cut over) with a one-line natural-language Copilot prompt per step and a "Stop and decide" gate where a reviewer needs to make a call. Optional tools are catalogued in five themed groups (estate-scale planning, per-package deep dives, validation & parity, infrastructure & secrets, discovery) each carrying a "reach for it when…" trigger so engineers know when *not* to use them. A separate "Tool overlap, signposted" table explicitly names the 6 confusable pairs and which one to prefer (`convert_estate` over `convert_ssis_package` in a loop, `smoke_test_wave` over `smoke_test_pipeline` in a loop, etc.). Closing "Why these six?" section explains every other tool exists for a non-required reason. README "New to the agent?" callout now points at WORKFLOW.md first, HOWTO.md second.

### P4-10. Pipeline-execution observability story — **MEDIUM** ✅ DONE
- **Buyer concern:** Post-migration BAU operations have no documented monitoring story (Log Analytics workbook, standard alerts, run-history retention).
- **Acceptance:** `observability.md` documenting the recommended diagnostic-settings target, a sample KQL workbook for pipeline failures + duration trending, and at least three suggested alert rules with thresholds.
- **Resolution:** New top-level [observability.md](../operations/observability.md) names Log Analytics as the recommended diagnostic-settings target (with rationale vs. Event Hubs / Storage), enumerates the five log/metric categories to enable (`PipelineRuns`, `ActivityRuns`, `TriggerRuns`, `PipelineActivityRuns`, `AllMetrics`), and ships a one-time-per-factory Bicep snippet wiring the diagnostic setting to the workspace. Workbook section provides two saved KQL queries — Query A (failed runs last 24h with parameters + error message) and Query B (14-day p50/p95 duration trend per pipeline) — plus a recommended two-tab layout (Health / Trends). Three alert rules are spec'd with full KQL, thresholds, evaluation cadence, and severity rationale: Alert A (any failure, sev 2, every 5m), Alert B (duration > 1.5× 14-day p95, sev 3, hourly), Alert C (trigger silently stopped firing — config-driven `expected` table joined left-outer against `ADFTriggerRun`, sev 2, hourly over 6h). Closes with explicit non-promises (does not replace SLO design, does not provision anything, does not cover Function-host or SHIR observability — links to the respective Microsoft Learn docs). Cross-linked from BEHAVIORAL_PARITY.md (replaces the stale "when published" placeholder).

### P4-11. Captured "first deploy that failed" recovery doc — **LOW** ✅ DONE
- **Buyer concern:** ROLLBACK.md is theoretical. A captured real failure-and-recovery (sanitized) builds trust no prose can.
- **Acceptance:** One captured deploy that failed (any failure mode — KV permission, SHIR offline, name collision), the error log, and the steps used to recover, written up under `docs/case-studies/` and linked from ROLLBACK.md.
- **Resolution:** New case study at [docs/case-studies/first_deploy_keyvault_recovery/](docs/case-studies/first_deploy_keyvault_recovery/README.md) captures one sanitized real failure: a first-time `deploy_to_adf` against a fresh factory with 6 encrypted packages whose linked services pointed at Key Vault references failed with `ManagedServiceIdentityCredentialNotFound` because the *factory's* system-assigned MI — not the deployer SP — had no role on the vault. The capture documents the verbatim error message, the 22 minutes spent on the wrong hypothesis (granting Key Vault role to the deployer instead of to the factory MI), the three commands that recovered (`az datafactory show --query identity.principalId`, `az role assignment create --role "Key Vault Secrets User" --scope $kvId`, re-run with `skip_if_exists=true`), and four lessons including "pre-flight (P4-6) is now mandatory for first deploys" tied back to WORKFLOW.md step 5. ROLLBACK.md gained a callout at the top linking to the case study so the generalized strategies sit on top of one concrete instance.

### P4-12. COVERAGE.md per-row evidence links — **LOW** ✅ DONE
- **Buyer concern:** ✅ rows in COVERAGE.md cite the dispatcher but not a captured artifact. A skeptic asked for cell-level evidence.
- **Acceptance:** Each ✅ / 🟡 row in COVERAGE.md gains a "Sample" column linking to a captured generated artifact (or a unit-test fixture) demonstrating the conversion.
- **Resolution:** Added an **Evidence** column to all five COVERAGE.md tables (Control Flow tasks, Sources, Transformations, Destinations, Connection managers, Package-level constructs). Each ✅ / 🟡 row now links either to the unit-test fixture that exercises the conversion end-to-end (e.g. `test_execute_sql_params.py`, `test_data_flow_transforms.py`, `test_script_classifier.py`, `test_linked_service_generation.py`, `test_pipeline_sensitive_redaction.py`, `test_realworld_fixes.py`, `test_constraint_resolution.py`, `test_foreach_prereq.py`, `test_proposer_project_params.py`, `test_expression_functions.py`, `test_script_task_port_database_access_configuration.py`) or, where no dedicated test fixture covers the construct alone, to the generator / converter source file that emits it (the data-flow source/destination/transformation converters and the linked-service generator). 🔴 / ⚪ rows correctly carry an em-dash since they have no captured evidence by design (the analyzer flags them as `manual_required`); a one-line reading guide above the first table explains this. All linked paths verified to exist.

### P4-13. Public roadmap + 1.0 milestone definition — **LOW** ✅ DONE
- **Buyer concern:** Version is 0.1.0 with a semver pre-1.0 caveat; no signal of when 1.0 lands or what it means.
- **Acceptance:** New `roadmap.md` listing the engineering items required for 1.0, the current quarter's focus, and the breaking-change-deprecation window for pre-1.0 → 1.0 transitions.
- **Resolution:** New top-level [roadmap.md](roadmap.md) defines what 1.0 *means* by naming the four committed surfaces (MCP tool inputs/outputs, CLI, on-disk artifact layout including `lineage.json` and `migration_plan.json`, and the public Pydantic IR models in `parsers.models`) along with the explicit non-commitments (internal converter implementation, generated stub text, warning wording, test helpers, the C#→Python translator). Lists 14 engineering items required for 1.0 grouped by Stability / Quality / Surface ergonomics / Operational, each tagged with current status ("done", "NEW", or "partial"), so the gap to 1.0 is countable rather than aspirational. Defines the pre-1.0 → 1.0 transition window: `0.9.0` ships first with a `removed-in-1.0` deprecation manifest, supported for a minimum 30 days before `1.0.0` removes the deprecated surface, with breaking changes listed in the `0.9.0` change log as `BREAKING (in 1.0):` plus migration recipe. Pre-1.0 release cadence (4–6 weeks per minor) and post-1.0 cadence (driven by need, not calendar) named. README gained a "Pre-1.0 status" callout pointing at ROADMAP.md.

### P4-14. Named support channel + response-time commitment — **LOW** ✅ DONE
- **Buyer concern:** "GitHub issues" is not a support channel for a customer mid-migration at 11 p.m.
- **Acceptance:** SUPPORT.md naming the support channel (alias / Teams channel / on-call rotation) with a stated response-time commitment for the duration of an active engagement. Acknowledges this may differ between OSS users and engaged customers.
- **Resolution:** [SUPPORT.md](SUPPORT.md) ships a community-supported, best-effort model with a documented bug-report template and non-negotiable sanitization checklist (no connection strings, no KV/sub/RG/SQL names that identify the customer, no real table/column names if business-sensitive, no package passwords). The original draft included an "engaged-customer" tier with response-time commitments by severity; that tier was removed because this is not yet an officially-supported Microsoft product, and committing to severity-based SLAs without a backing engagement contract would mislead readers. Once / if the project becomes an officially-supported offering, the engaged-customer tier should be reinstated. Cross-linked from the README "Need help?" callout. **All 14 P4 buyer follow-ups now closed.**

---

## P5 — Skeptical-buyer-review followups (April 2026)

Sourced from the second-round skeptical-buyer review run after P4 closed.
Only items the coding agent can ship end-to-end (no real estate, no real
Azure tenant, no human-in-the-loop port-time capture) are tracked here.
Evidence-capture items (estate-scale run, additional behavioral-parity
walkthroughs, additional Script Task ports, real-Azure KV runs, captured
failure case studies) are tracked separately as part of the engaged-
customer pilot, not in this backlog.

### P5-6. Schema-version `lineage.json` and `migration_plan.json` — **MEDIUM** ✅ DONE
- **Buyer concern:** Downstream CI parses both files. Without a `schemaVersion`, a minor-version bump silently breaks the customer's pipeline. ROADMAP S3/S4 already commit to this for 1.0.
- **Acceptance:** Both files carry a top-level `schemaVersion: "1"`; loader rejects unknown major versions with a clear message; loader accepts an unknown *minor* version as forward-compatible (logs a warning); a forward-compat unit test pins the contract.
- **Resolution:** `migration_plan.json` already shipped a `schema_version` field with major-rejection / minor-warning loader semantics in `migration_plan/persistence.load_plan` (constant `PLAN_SCHEMA_VERSION = "1.0"`). `lineage.json` already wrote `schema_version: "1.0"` but had no loader; added [`load_lineage()`](ssis_adf_agent/generators/lineage_generator.py) plus a `LINEAGE_SCHEMA_VERSION` module constant that mirrors the migration-plan policy. Forward-compat contract is now pinned by 6 tests in [test_schema_version_contract.py](tests/test_schema_version_contract.py): both files load at the current version, both warn on an unknown minor, both raise `ValueError` with "incompatible schema_version" on an unknown major.

### P5-7. `provision_adf_environment --with-observability=<workspace-id>` — **MEDIUM** ✅ DONE
- **Buyer concern:** OBSERVABILITY.md (P4-10) is excellent prose but the factory provisioner does not emit the diagnostic-settings target. Day-1 production-readiness still requires a separate Bicep PR. ROADMAP Q4.
- **Acceptance:** New optional `--with-observability=<workspace-id>` flag on `provision_adf_environment` emits a `Microsoft.Insights/diagnosticSettings` resource targeting the named Log Analytics workspace with the categories named in OBSERVABILITY.md (`PipelineRuns`, `ActivityRuns`, `TriggerRuns`, `PipelineActivityRuns`, `AllMetrics`). Unit-tested against the captured Bicep output.
- **Resolution:** [`generate_bicep()`](ssis_adf_agent/migration_plan/bicep_generator.py) gained an `observability_workspace_id` keyword argument; when set (full ARM id of a Log Analytics workspace), a `Microsoft.Insights/diagnosticSettings@2021-05-01-preview` child resource is emitted on the generated factory enabling all five categories named in OBSERVABILITY.md (categories are intentionally not parameterized — drift between doc and generator is a defect, not a feature toggle). The `provision_adf_environment` MCP tool exposes this as `with_observability=<workspace-resource-id>` and the description was updated to remove the "tool does not yet emit" disclaimer. If the plan does not provision a factory the request is honored as a `// Skipped diagnosticSettings: ...` comment rather than silently dropped. 3 new tests in [test_bicep_generator.py](tests/test_bicep_generator.py) cover: emission with all five categories present, skip-comment when no factory, default-off when the flag is unset.

### P5-8. Confirm + document that `SSIS_ADF_NO_LLM=1` disables every form of network egress — **HIGH** ✅ DONE
- **Buyer concern:** No-LLM mode (P4-8) gates the Script Task LLM translator. The buyer asked whether the same switch also disables every *other* form of egress (telemetry, version-check pings, PyPI lookups, etc.). SECURITY.md does not currently say either way.
- **Acceptance:** Audit the codebase for any outbound HTTP/HTTPS call site, document each in SECURITY.md under "What the agent talks to and how to disable it", confirm `SSIS_ADF_NO_LLM=1` (or document a separate switch) disables all of them, and add a unit/integration test that asserts no socket / no HTTP client construction occurs under no-LLM mode for the conversion path. **Requires one decision from the maintainer:** confirm there is no telemetry the audit doesn't surface.
- **Resolution:** Audit completed by grep-walking the source tree for `requests`, `httpx`, `urllib`, `aiohttp`, `http.client`, `azure.mgmt.*`, `azure.identity`, `azure.keyvault`, `AzureOpenAI`, and `subprocess` invocations of `az` / `curl` / `wget`. Result: exactly **three** distinct egress destinations exist and all are gated by an explicit caller action — Azure OpenAI (gated by `llm_translate=true` + endpoint env vars + absence of `SSIS_ADF_NO_LLM`), Azure Resource Manager (only the deployment / provisioning tools), and SQL Server (only `scan_ssis_packages` with the SQL reader path). Documented as a per-destination table in [SECURITY.md](SECURITY.md) "What the agent talks to and how to disable it (P5-8)" with explicit "calls explicitly NOT made" findings (no `import requests`, no telemetry, no auto-update, no public-internet call other than the customer's own AOAI). New regression test [tests/test_no_egress_conversion_path.py](tests/test_no_egress_conversion_path.py) monkey-patches `socket.socket`, `socket.create_connection`, and `httpx.HTTPTransport.handle_request` to forbid any non-loopback connection, then runs `convert_estate` + `bulk_analyze` end-to-end and asserts they complete successfully — fails loudly if a future change introduces an outbound HTTP call into the conversion path.

### P5-9. README tool-count + diagram inconsistencies — **LOW** ✅ DONE
- **Buyer concern:** README header says "29 tools"; §"All tools are invoked from GitHub Copilot Chat" still says "22 tools". Architecture diagram lists 5 steps (scan → analyze → convert → validate → deploy); WORKFLOW.md correctly says 6 (adds activate-triggers). Procurement reviewers screenshot the inconsistency.
- **Acceptance:** Trailing tool-count reference updated to match the header; diagram updated to match WORKFLOW.md's 6-step path. Add a unit test that asserts the README tool-count and the actual `len(list_tools())` agree (catches the next bump).
- **Resolution:** Updated the two trailing "22 tools" references in [README.md](README.md) to 29 to match the header; replaced the older 5-step `scan → analyze → convert → validate → deploy` architecture diagram with the 6-step `bulk_analyze → propose → convert → validate → deploy → activate` path that [workflow.md](../getting-started/workflow.md) documents. Added [tests/test_readme_consistency.py](tests/test_readme_consistency.py) with two assertions: (1) all three README tool-count strings equal `len(list_tools())`, and (2) the 6-step diagram references both `bulk_analyze` and `activate` and the older 5-step prose is gone. Future tool additions/removals fail this test until README is updated.

### P5-11. Uniform `--dry-run` across `provision_adf_environment` and `provision_function_app` — **MEDIUM** ✅ DONE
- **Buyer concern:** `deploy_to_adf --pre-flight=true` and `activate_triggers --dry_run=true` exist; the two `provision_*` tools do not. ROADMAP E2. CAB approval often requires a "what would happen" report before any provisioning.
- **Acceptance:** Both `provision_adf_environment` and `provision_function_app` accept a `dry_run: bool = False` arg that emits the would-be Bicep / would-be `az` calls and the planned RBAC assignments without creating any resource. Tested against captured Bicep.
- **Resolution:** Both tools already accept `dry_run` end-to-end. [`provision_adf_environment`](ssis_adf_agent/mcp_server.py) returns `mode: "offline_dry_run"` (or validates against Azure when sub/RG are supplied) and emits the rendered Bicep without applying it; covered by [test_bug_fixes.py::test_provision_dry_run_offline_returns_bicep_only](tests/test_bug_fixes.py). [`provision_function_app`](ssis_adf_agent/mcp_server.py) wires `dry_run` through to [`FuncProvisioner.provision()`](ssis_adf_agent/deployer/func_provisioner.py) which lists every resource it would create and tags the result with `[DRY RUN] No resources created`; covered by three tests in [test_func_provisioner.py](tests/test_func_provisioner.py) (`test_dry_run_reports_resources`, `test_dry_run_skip_insights`, `test_dry_run_custom_python_version`). No code change required for this item — the gap was tracked but already closed by the time it was triaged.

### P5-12. New MCP tool `validate_deployer_rbac` — **MEDIUM** ✅ DONE
- **Buyer concern:** RBAC.md (P4-7) is excellent prose; verifying compliance is still manual and the captured KV-recovery case study (P4-11) is exactly the failure this would have caught.
- **Acceptance:** New tool `validate_deployer_rbac` accepts the deploying identity, the planned tools, the target subscription/RG/factory/KV, and reports per-tool which RBAC.md-required roles are present vs. missing, without creating any resource. Mocked-SDK unit tests; real-Azure verification deferred to the engaged-customer pilot.
- **Resolution:** New 30th MCP tool [`validate_deployer_rbac`](ssis_adf_agent/deployer/rbac_validator.py) ships in two modes. **Offline mode**: caller passes `held_arm_roles` + `held_kv_roles` lists, no Azure call is made — useful in air-gapped review. **Live mode**: caller passes `principal_object_id` + `subscription_id` (+ optional resource_group / factory_name / key_vault_name) and the tool calls `AuthorizationManagementClient.role_assignments.list_for_subscription` (read-only) to resolve the principal's role display names. The compliance matrix mirrors the table in [rbac.md](../operations/rbac.md) (12 tools covered; expanded as the deployment surface grows). Per-tool findings are classified `ok` / `missing_arm` / `missing_kv` / `missing_both` / `unknown` with the missing-role alternatives and the rbac_md_anchor for cross-reference. Tool count bumped to 30 in [README.md](README.md), [.github/copilot-instructions.md](.github/copilot-instructions.md), and the [`mcp_server.py`](ssis_adf_agent/mcp_server.py) module docstring; the existing test_readme_consistency.py guard validates the bump. 9 new tests in [tests/test_validate_deployer_rbac.py](tests/test_validate_deployer_rbac.py) cover all classifications, the AND-relationship for `provision_adf_environment` (Contributor + UAA), KV alternative roles, the unknown-tool path, and the MCP handler argument validation.

### P5-14. Per-pipeline cost projection emitted at `convert_estate` time — **LOW** ✅ DONE
- **Buyer concern:** `estimate_adf_costs` and `compare_estimates_to_actuals` exist but require a separate run. Steering-committee deck would be one step shorter if `convert_estate` emitted projection alongside `lineage.json`.
- **Acceptance:** `convert_estate` accepts an optional `--with-cost-projection=true` flag; when set, writes `cost_projection.json` next to `lineage.json` reusing the `estimate_adf_costs` engine. Unit-tested.
- **Resolution:** [`convert_estate`](ssis_adf_agent/mcp_server.py) gained a `with_cost_projection: bool = False` arg. When true, after every package is converted the saved plans are loaded back and fed through [`estimate_adf_costs()`](ssis_adf_agent/migration_plan/estate_tools.py); the resulting estimate is written to `<output_dir>/cost_projection.json` and the bottom-line numbers (monthly_total_usd, annual_total_usd, package_count) are echoed in the tool's JSON response under `cost_projection`. If `save_plans=false` the request is honored as `cost_projection.status="skipped"` rather than silently dropped. Three tests in [tests/test_convert_estate_cost_projection.py](tests/test_convert_estate_cost_projection.py) cover the happy path, the default-off case, and the save_plans=false skip path.

### P5-16. New tool `diff_estate` — **MEDIUM** ✅ DONE
- **Buyer concern:** Re-running `convert_estate` after an upstream `.dtsx` edit reconverts everything; no signal what *changed*. Small upstream change still triggers full re-validation.
- **Acceptance:** New tool `diff_estate` compares two `out/` directories (or one `out/` against a saved snapshot) and emits a focused report: per-package classification (byte-identical / changed / added / removed) with the per-artifact diff for changed packages. Unit-tested against synthetic before/after fixtures.
- **Resolution:** New 31st MCP tool [`diff_estate`](ssis_adf_agent/analyzers/estate_diff.py) compares two estate output directories using SHA-256 file digests for cheap identity, then emits a `difflib.unified_diff` for changed text artifacts (JSON, Python, Bicep, Markdown, YAML) and a `binary, byte-different` note for everything else. Per-package status is `identical` / `changed` / `added` / `removed`; per-file status is the same set. Diffs longer than 200 lines are truncated with an explicit marker so the report stays reviewable. Pure stdlib — no Azure calls, no LLM, no network. Tool count bumped to 31 in [README.md](README.md), [.github/copilot-instructions.md](.github/copilot-instructions.md), and the [`mcp_server.py`](ssis_adf_agent/mcp_server.py) module docstring; passes the test_readme_consistency.py guard. 6 new tests in [tests/test_diff_estate.py](tests/test_diff_estate.py) cover identical estates, changed packages with diff content assertions, added/removed packages, mixed per-file changes within one changed package, the truncation path, and the MCP handler's report_path write.

### P5-17. CLI parity for every MCP tool — **HIGH** ✅ DONE
- **Buyer concern:** Workflow assumes Copilot Chat in VS Code Agent mode. Air-gapped / Copilot-blocked / CI-only customers have no first-class entry point. ROADMAP E1.
- **Acceptance:** `ssis-adf-agent <tool-name> --arg1=value --arg2=value` accepts every MCP tool with the same surface as the MCP server. Help text auto-generated from the same `types.Tool` schemas the MCP server uses, so the two surfaces stay synchronized. Unit-tested per tool.
- **Resolution:** [ssis_adf_agent/cli.py](ssis_adf_agent/cli.py) now reflects every MCP tool 1:1 by introspecting `mcp_server.list_tools()` at parser-build time. Each tool gets a sub-command named after itself (`diff-estate`, `convert-estate`, `validate-deployer-rbac`, etc.); each `inputSchema` property becomes a `--<name>` flag with the right argparse type (string/integer/number/boolean via `BooleanOptionalAction`/array via `nargs="*"`/object via JSON literal), required-ness, default value, and `enum` choices pulled straight from the schema. Help text comes from the schema descriptions, so adding a new MCP tool automatically extends the CLI with no separate update needed. The five legacy aliases (`analyze`, `convert`, `validate`, `deploy`, `activate-triggers`) are preserved with their curated positional surface for backward compatibility with existing CI scripts. 6 new tests in [tests/test_cli_parity.py](tests/test_cli_parity.py) cover every-tool-has-a-subcommand coverage, schema-driven required-arg enforcement, schema-derived help text, auto-dispatch of `diff_estate`, `BooleanOptionalAction` flags on `convert_estate`, and array-arg collection on `consolidate_packages`.

### P5-18. Cross-link HOWTO.md → WORKFLOW.md "Start here" callout — **LOW** ✅ DONE
- **Buyer concern:** Buyers landing on HOWTO from a Google search miss the new minimum-path doc.
- **Acceptance:** HOWTO.md gains a "Start here" callout at the top pointing at WORKFLOW.md as the recommended first read.
- **Resolution:** Added a "🚀 Start here" blockquote at the top of [howto.md](../getting-started/howto.md) (above the existing intro paragraph) that names the 6-tool minimum path (`bulk_analyze` → `propose_adf_design` → `convert_estate` → `validate_adf_artifacts` → `deploy_to_adf` → `activate_triggers`) and links to [workflow.md](../getting-started/workflow.md) as the recommended first read. The conversation guide remains the second-read for the per-package backbone and wider tool surface.

### P5-19. Cross-link ENCRYPTED_PACKAGES.md → P4-11 KV-recovery case study — **LOW** ✅ DONE
- **Buyer concern:** The case study links to ENCRYPTED_PACKAGES.md but not the reverse. `upload_encrypted_secrets` users miss the prerequisite reading.
- **Acceptance:** ENCRYPTED_PACKAGES.md gains a "Real failure walkthrough" callout linking to the captured KV case study.
- **Resolution:** Added a "🧯 Real failure walkthrough" blockquote at the top of [encrypted-packages.md](../operations/encrypted-packages.md) (above "Why this hurts") that names the captured `ManagedServiceIdentityCredentialNotFound` failure mode, the wrong-hypothesis cost (22 minutes granting KV role to the deployer SP instead of to the factory MI), and links to [docs/case-studies/first_deploy_keyvault_recovery/](docs/case-studies/first_deploy_keyvault_recovery/README.md). The cross-link is now bidirectional.

### P5-20. COVERAGE.md "Mapped vs. unmapped SSIS expression functions" — **MEDIUM** ✅ DONE
- **Buyer concern:** COVERAGE.md is silent on the `(DT_STR,2,1252) DATEPART("mm", GETDATE())` family of casts and string functions. Buyers ask for a 5-line table showing what works and what does not.
- **Acceptance:** New COVERAGE.md section enumerates SSIS expression functions covered by the converter (`DATEPART`, `RIGHT`, `LEFT`, `SUBSTRING`, `(DT_STR,…)` casts, `GETDATE()`, etc.) vs. unmapped, derived from the actual `expression_functions` source so the table cannot drift.
- **Resolution:** Added a "SSIS expression functions: mapped vs. unmapped" section to [coverage.md](../conversion/coverage.md). Table is derived from the `_FUNC_MAP` constant in [`translators/control_flow_expression.py`](ssis_adf_agent/translators/control_flow_expression.py) (named in the section header as the source of truth) and uses the existing ✅ / 🟡 / 🔴 legend: ✅ for direct ADF equivalents (date/time, string, null-handling, math, common DT_ casts), 🟡 for entries that translate but emit a `/* TODO: ... */` marker (DATEDIFF month/year/etc., DATEPART month/year/hour/minute, REVERSE, ISNUMERIC, PATINDEX, `(DT_DBTIMESTAMP)`), and 🔴 for the pass-through fallback on uncatalogued functions. Closes with a note on the data-flow expression translator's two known shape differences (`currentTimestamp()` vs. `utcNow()`, simplified `DATEPART`).

### P5-21. Cross-link `provision_adf_environment` → OBSERVABILITY.md — **LOW** ✅ DONE
- **Buyer concern:** Buyers using the factory provisioner discover OBSERVABILITY.md too late (after the first failed run with no logs).
- **Acceptance:** `provision_adf_environment` MCP tool description and CLI help text cross-link OBSERVABILITY.md as the recommended Day-2 follow-up.
- **Resolution:** `provision_adf_environment`'s description in [ssis_adf_agent/mcp_server.py](ssis_adf_agent/mcp_server.py) now closes with a "Day-2 follow-up" pointer at OBSERVABILITY.md naming the five log/metric categories and the three baseline alert rules, and explicitly notes the diagnostic-settings resource is not yet emitted (tracked as P5-7). The CLI does not currently expose a `provision` subcommand; CLI-side cross-link will land with [P5-17](#p5-17-cli-parity-for-every-mcp-tool--high) (CLI parity).

### P5-23. Document the `EncryptAllWithPassword` failure modes — **LOW** ✅ DONE
- **Buyer concern:** Buyer asked whether wrong-password vs. missing-password vs. key-derivation-failure produce distinct error messages or one generic failure.
- **Acceptance:** ENCRYPTED_PACKAGES.md gains a "Failure modes and how to read them" subsection enumerating the actual error messages the parser raises for each, derived from the parser source.
- **Resolution:** Added a "Failure modes and how to read them" section to [encrypted-packages.md](../operations/encrypted-packages.md). Honest answer: the parser does **not** decrypt — there is no `--password` flag and no key derivation, so the classic SSIS wrong-password / missing-password / key-derivation-failed messages do not appear. Encrypted content is silently absent. Documented the five real downstream symptoms (gap entry at package level, empty `connectionString` placeholders, missing parameter `defaultValue`, Script Task `source_code=None` warning, missing connection-manager records) with each row pointing at the source-of-truth file (`gap_analyzer.py`, `linked_service_generator.py`, `pipeline_generator.py` `_redact_sensitive_default`, `script_task_converter.py`, `ssis_parser.py` `_parse_connection_manager`). Closes with: a parser *exception* on an encrypted package is a bug, not the encryption.

### P5-24. Document LLM translator behavior at the 18 000-char truncation bound — **LOW** ✅ DONE
- **Buyer concern:** SECURITY.md notes the translator transmits `source_code` truncated at 18 000 chars. What happens to a Script Task whose source exceeds that bound is not documented.
- **Acceptance:** SECURITY.md "What the LLM translator sends" section gains a paragraph documenting the truncation behavior (silent truncation? warning? skip?), derived from the translator source, and what the user should expect in the generated stub.
- **Resolution:** Added a "Behavior at the 18 000-char truncation bound" paragraph to [SECURITY.md](SECURITY.md). Honest answer derived from [`translators/csharp_to_python.py`](ssis_adf_agent/translators/csharp_to_python.py) (`_MAX_INPUT_CHARS`): truncation is silent — no exception, no warning, no tool-level signal. The first 18 000 chars are kept verbatim, a literal `// ... [TRUNCATED: source exceeded 18000 chars] ...` marker is appended before the model sees the prompt, and the generated stub reflects only the truncated slice. The full original source is still embedded as comments in the stub (H3 source-as-comments behavior), so the operator can port the truncated tail by hand. Documented the recommended workflow for genuinely oversized Script Tasks: treat the LLM stub as a starting point and follow the Database_Access_Configuration case study methodology.

### P5-25. New ROLLBACK.md section: tearing down a provisioned factory — **MEDIUM** ✅ DONE
- **Buyer concern:** ROLLBACK.md covers artifacts; not the factory itself. Customers asked what happens when the *factory* (provisioned by `provision_adf_environment`) needs to be torn down.
- **Acceptance:** ROLLBACK.md gains a new "Strategy 4 — tearing down a provisioned factory" section covering the `az` recipe, RBAC cleanup, KV access-policy / role removal, and what `lineage.json` looks like for a fully-deprovisioned environment.
- **Resolution:** [rollback.md](../operations/rollback.md) gained a "Strategy 4 — tearing down a provisioned factory" section covering: the order-of-operations recipe (stop triggers first → snapshot lineage → `az datafactory delete`), an explicit warning that this is rarely the right answer mid-migration, RBAC cleanup (orphaned role assignments after the factory MI is gone — `az role assignment list --all` query for empty principalNames, deletion by id, run at sub / RG / per-vault scope), Key Vault cleanup (RBAC mode vs. access-policy mode — different cleanup paths), and what `lineage.json` looks like post-teardown (azure_resource_id values now point at non-existent ARM resources; recommended path is rename to `lineage.pre-teardown.json` for the audit trail rather than rewriting). Closes with a "Strategy 4 vs. the others" table tying choice to situation. Decision tree at the top of the doc updated to put the factory-decommission question first.

### P5-26. Confirm or replace the GitHub URL in SUPPORT.md - **LOW** DONE
- **Buyer concern:** SUPPORT.md cited `MicahRowlandMicrosoft/ssis_adf_agent`, which no longer matched the configured repository remote.
- **Acceptance:** SUPPORT.md, the README "Need help?" callout, clone instructions, and any other GitHub references use the confirmed customer-facing URL.
- **Resolution:** Both configured Git remotes use `https://github.com/MicahRowlandMicrosoft/ssis_modernization_agent`. README.md, SUPPORT.md, and the setup guide now use that canonical repository URL.

---

## P6 - Functional automation review (July 24, 2026)

These items come from an executable review of the assessment, conversion,
validation, and deployment paths. The review ran the complete test suite and
used focused in-memory fixtures to verify the highest-risk findings. P6 work is
ordered by data-correctness and deployment-safety risk, not implementation size.

Status legend for this section:
- **READY**: scoped and ready for implementation.
- **IN PROGRESS**: implementation has started.
- **BLOCKED**: requires another P6 item or external evidence.
- **DONE**: acceptance criteria and tests are complete.

### P6-1. Fix reflected CLI handling for negative boolean names - **P0 / DONE**
- **Problem:** The schema-driven CLI passes the existing `--no-llm` option to `argparse.BooleanOptionalAction`, which attempts to create a second negative form. On Python 3.14, parser construction fails before any command can run.
- **Scope:** Centralize boolean action selection in `cli.py`. Positive boolean properties retain `BooleanOptionalAction`; schema properties whose CLI name already starts with `no-` use a single `store_true` option while preserving the JSON argument name.
- **Acceptance:** `build_parser()` succeeds on every supported Python version; every MCP tool has a CLI subcommand; `convert-ssis-package --no-llm` dispatches `no_llm=True`; help contains no `--no-no-*` option; legacy aliases remain compatible.
- **Tests:** Add focused parser and dispatch tests in `test_cli_parity.py`, then run `test_cli.py`, `test_cli_parity.py`, and the full suite.
- **Depends on:** None.
- **Resolution:** `_add_property()` now uses `store_true` for schema properties whose names start with `no_` and retains `BooleanOptionalAction` for ordinary booleans. The same schema boundary escapes literal percent signs in help descriptions, which Python 3.14 validates during parser construction. Focused CLI tests pass, and the full suite completed with 934 passed and 1 pre-existing Azure OpenAI configuration warning.

### P6-2. Introduce a recursive, scope-aware execution IR - **P0 / IN PROGRESS**
- **Problem:** Assessment and artifact generators traverse different task shapes. Nested tasks, scoped variables, disabled executables, container boundaries, and event handlers therefore receive inconsistent treatment.
- **Scope:** Add one normalization layer between parsing and analysis/conversion. It must recursively enumerate package tasks, container children, and handlers; retain owning scope; expose stable entry and terminal nodes; and preserve source IDs for lineage.
- **Acceptance:** Every executable appears exactly once in the normalized graph; each node records its owner, scope path, enabled state, inbound and outbound constraints, and source construct; all analyzers and generators consume the shared traversal API rather than walking `package.tasks` independently.
- **Tests:** Nested Sequence, ForEach, For Loop, and event-handler fixtures prove traversal order, scope ownership, and source-ID stability.
- **Depends on:** None. P6-3 through P6-7 should build on this item.
- **Progress:** Added `parsers/task_traversal.py` with deterministic recursive traversal across package tasks, nested containers, and event handlers. Each location carries owner kind/ID/name, stable scope path, depth, local/inbound/outbound constraints, event context, and the original task. `NormalizedTaskScope` now computes enabled tasks, rewritten constraints, entry nodes, terminal nodes, and omitted disabled nodes for any execution scope. Complexity analysis, gap analysis, top-level pipeline generation, Sequence, ForEach, and For Loop conversion use the shared traversal/normalization path. Scoped-variable normalization, event-handler graph integration, and the non-pipeline artifact generators remain open.

### P6-3. Preserve disabled-task semantics - **P0 / DONE**
- **Problem:** The parser records disabled tasks, but pipeline generation emits them as live ADF activities.
- **Scope:** Normalize disabled executables before generation. Omit them and bypass their graph node only when incoming/outgoing constraints can be preserved exactly; otherwise emit a blocking fidelity finding instead of guessing.
- **Acceptance:** Disabled tasks never become executable ADF activities; downstream dependencies never reference omitted activity names; ambiguous success/failure or expression-based bypasses make the conversion non-deployable; assessment reports disabled constructs and the chosen disposition.
- **Tests:** Cover isolated, chained, fan-in, fan-out, nested, and expression-constrained disabled tasks.
- **Depends on:** P6-2.
- **Resolution:** `normalize_task_scope()` omits isolated disabled tasks and safely rewrites plain success-only constraint chains, including chained disabled nodes and fan-in/fan-out graphs. Expression, failure, completion, OR, and self-dependency cases raise `DisabledTaskBypassError` before artifact generation. The dispatcher applies the same normalization at package, Sequence, ForEach, and For Loop scopes. Gap analysis reports safe omission as informational and unsafe omission as manual-required. Tests include model-level graph cases, nested conversion, direct dispatch, and a parsed DTSX chain with `DTS:Disabled="1"`.

### P6-4. Compile complete precedence-constraint semantics - **P0 / READY**
- **Problem:** Current dependency generation keeps only success, failure, or completion and discards `eval_op`, expression text, and AND/OR grouping.
- **Scope:** Model constraint groups explicitly and lower supported combinations into ADF dependencies plus `IfCondition` or equivalent gates. Unsupported combinations must produce blockers, not silently weakened dependencies.
- **Acceptance:** Generated control flow is truth-table equivalent for constraint-only, expression-only, expression-and-constraint, expression-or-constraint, multi-predecessor AND, and multi-predecessor OR cases; translated expressions retain variable references; no expression metadata is silently dropped.
- **Tests:** Table-driven truth cases plus parsed DTSX fixtures for each `PrecedenceEvalOp` and `logical_and` value.
- **Depends on:** P6-2.

### P6-5. Rewrite container boundaries and loop ordering - **P0 / READY**
- **Problem:** Flattened Sequence containers leave dependencies pointing at activities that do not exist, and For Loop increments can run in parallel with the loop body.
- **Scope:** Compute entry and terminal sets for every container. Rewrite external edges through those sets, preserve internal ordering, and make For Loop increment activities depend on every successful terminal body path before re-evaluating the condition.
- **Acceptance:** No dependency references a flattened container name; predecessors gate all container entry nodes; successors wait for the correct terminal nodes and outcomes; loop increments cannot start before the body completes; empty and nested containers have defined behavior.
- **Tests:** Sequence before/after, nested Sequence, fan-in/fan-out, empty container, single/multi-terminal For Loop, and failure-path fixtures.
- **Depends on:** P6-2 and P6-4.

### P6-6. Integrate event handlers into production conversion - **P0 / READY**
- **Problem:** An event-handler converter exists but is not called by pipeline generation, and parity validation currently treats omitted handlers as informational.
- **Scope:** Normalize package-level and task-level handlers, lower supported events such as OnError and OnPostExecute into explicit ADF branches or child pipelines, and classify unsupported event semantics as blocking.
- **Acceptance:** Every parsed handler has an emitted artifact/branch or a blocking fidelity record; handler tasks use normal converters rather than placeholders; owner scope and system-variable bindings are retained; parity fails when a handler is omitted.
- **Tests:** Package and task-level handlers, nested handler tasks, OnError propagation, OnPostExecute ordering, unsupported event type, and deliberate omission regression.
- **Depends on:** P6-2, P6-4, and P6-5.

### P6-7. Use shared recursive traversal in every artifact generator - **P0 / READY**
- **Problem:** Pipeline conversion reaches nested tasks, while dataset and Mapping Data Flow generators inspect only top-level `package.tasks`. Nested activities can reference artifacts that were never generated.
- **Scope:** Replace generator-specific top-level loops with the normalized execution traversal for datasets, data flows, Function stubs, conversion warnings, and lineage relationships.
- **Acceptance:** A Data Flow Task nested at any supported depth emits its data flow and all datasets exactly once; nested Script/XML/File System tasks emit required stubs; generated summaries and lineage include nested artifacts; names remain deterministic.
- **Tests:** Data Flow and stub-producing tasks nested in Sequence, ForEach, and For Loop containers, including duplicate-name and repeated-reference cases.
- **Depends on:** P6-2.

### P6-8. Compile multi-input Mapping Data Flow graphs correctly - **P0 / READY**
- **Problem:** Join and Union generation selects only the first predecessor, silently dropping other input streams.
- **Scope:** Build Mapping Data Flow DSL from the complete path graph, with deterministic stream aliases, operator-specific arity checks, join-side mapping, union input ordering, and explicit rejection of malformed graphs.
- **Acceptance:** Join emits both left and right streams and the translated condition; Union emits every connected input exactly once; graph fan-out remains intact; disconnected or wrong-arity operators are blocking validation errors.
- **Tests:** Two-input Join, self-join aliases, three-input Union, fan-out/fan-in, reversed path ordering, missing input, and behavioral parity fixtures.
- **Depends on:** P6-7.

### P6-9. Generate every converter-referenced artifact - **P0 / READY**
- **Problem:** Some converters, including FTP and Bulk Insert paths, emit dataset or linked-service references for which no producer exists.
- **Scope:** Inventory every artifact reference emitted by control-flow and data-flow converters. Add producers for valid automatic mappings and convert manual placeholders into explicit blockers where a deployable artifact cannot be generated safely.
- **Acceptance:** Every emitted linked-service, dataset, data-flow, pipeline, Function, and trigger reference resolves to exactly one generated or declared shared artifact; FTP and Bulk Insert fixtures have complete reference closure; executable `SELECT 1` or similar placeholders cannot be classified as deployable.
- **Tests:** One reference-closure fixture for every converter family plus shared-artifact and missing-artifact cases.
- **Depends on:** P6-7. Validated by P6-12.

### P6-10. Parse and resolve SSIS property expressions - **P0 / READY**
- **Problem:** Dynamic `PropertyExpression` values are not represented in the IR or covered by tests, so assessment and conversion can use stale design-time values.
- **Scope:** Parse property-expression target paths, source scope, and expression text. Resolve supported expressions at runtime through ADF parameters/variables and classify unsupported dynamic targets as blockers.
- **Acceptance:** Assessment lists every property expression and affected task/property; supported connection, SQL, path, and task-property expressions become ADF dynamic content; unresolved expressions cannot silently fall back to the design-time value.
- **Tests:** Package/task/container scope, variable shadowing, connection-string expression, SQL command expression, file path expression, unsupported target, and malformed expression.
- **Depends on:** P6-2 and P6-4.

### P6-11. Emit a schema-versioned conversion-fidelity manifest - **P0 / READY**
- **Problem:** Conversion completeness is inferred from description strings such as `MANUAL REVIEW`, while assessment, conversion summaries, and parity use different definitions of a gap.
- **Scope:** Emit one structured record per source construct with `exact`, `approximate`, `manual`, `blocking`, or `omitted` status; source ID/path; generated artifact references; reason code; and remediation. Derive assessment and conversion summary counts from this manifest.
- **Acceptance:** No warning count depends on free-text scanning; every parsed executable, constraint, handler, Data Flow component, schedule, and property expression has one fidelity disposition; unknown major schema versions are rejected and unknown minor versions warn.
- **Tests:** Manifest coverage/uniqueness, status aggregation, nested constructs, schema compatibility, and regression fixtures for every current manual placeholder.
- **Depends on:** P6-2 through P6-10.

### P6-12. Replace structural validation with semantic reference-closure validation - **P0 / READY**
- **Problem:** Validation accepts parseable JSON even when dependencies point to missing activities/artifacts, placeholders remain, or Mapping Data Flow topology is incomplete.
- **Scope:** Build a reusable validator for activity graphs, nested activities, artifact references, linked-service/dataset compatibility, dynamic expressions, Mapping Data Flow stream closure, trigger targets, and forbidden placeholder markers. Reuse it in conversion, parity, ARM export, and deployment.
- **Acceptance:** The known broken fixtures for disabled FTP, missing event handler, missing Sequence target, dropped Join input, `TODO_KEY_COLUMN`, `_ssis_todo`, and manual executable placeholders all fail with stable reason codes and source locations; valid shared-artifact references pass.
- **Tests:** Focused rule tests, aggregate malformed-estate fixture, valid nested-estate fixture, and optional Azure SDK deserialization as a secondary check.
- **Depends on:** P6-7 through P6-11.

### P6-13. Make conversion readiness and deployment fail closed - **P0 / READY**
- **Problem:** Deployment validates one file at a time, skips invalid files, and continues creating other resources, producing partially deployed factories.
- **Scope:** Validate the entire artifact set and fidelity manifest before constructing any create/update operation. Add an explicit non-deployable conversion status and require a separately named operator override for acknowledged non-blocking approximations only.
- **Acceptance:** Any validation error or blocking fidelity item causes zero Azure mutations; dependency failure stops dependent deployment; partial success is reported as an interrupted transaction, never overall success; triggers remain stopped.
- **Tests:** Mock clients assert zero calls on pre-validation failure, no dependent calls after a deployment failure, and correct behavior for warnings versus blockers.
- **Depends on:** P6-11 and P6-12.

### P6-14. Return structured MCP errors and reliable CLI exit codes - **P1 / READY**
- **Problem:** MCP exceptions are returned as ordinary text content, and the CLI can interpret non-JSON error text as a successful command.
- **Scope:** Define a stable error envelope with code, phase, message, retryability, and optional details; set MCP `isError`; map tool outcomes and exceptions to documented CLI exit codes without parsing human-readable text.
- **Acceptance:** Every raised tool exception yields `isError=true`; machine-readable failures round-trip through MCP and CLI; invalid text can never exit 0; validation and deployment blockers use distinct stable codes; secrets remain redacted.
- **Tests:** Unit tests for exception, validation failure, deployment failure, retryable Azure error, malformed result, and successful text/JSON results across both surfaces.
- **Depends on:** P6-1 for the working CLI surface. Align reason codes with P6-11 and P6-12.

### P6-15. Preserve complete SQL Agent schedule semantics - **P1 / READY**
- **Problem:** Trigger generation treats a daily active-window end time as a permanent ADF trigger end date and lacks metadata for one-time, relative-monthly, date-bounded, seconds-based, and source-time-zone schedules.
- **Scope:** Expand `SqlAgentSchedule` and the `msdb` query to retain active dates, relative interval, enabled state, sub-day cadence, and time-zone provenance. Generate equivalent ADF recurrence arrays where possible and blockers where ADF has no equivalent.
- **Acceptance:** One-time, daily, sub-day window, weekly, absolute monthly, and relative monthly schedules preserve occurrence semantics; service-start, idle, and unsupported seconds schedules are explicit blockers; time zone is never guessed silently; generated triggers remain stopped.
- **Tests:** Table-driven fixtures for every SQL Agent `freq_type`, active date/window boundaries, daylight-saving time zones, disabled schedules, and unsupported modes.
- **Depends on:** P6-11 and P6-12 for fidelity and validation reporting.

### P6-16. Add immutable deployment planning and remote diff - **P1 / READY**
- **Problem:** Deployment has no complete create/update/unchanged plan, ownership boundary, or optimistic-concurrency guard before mutation.
- **Scope:** Build a plan from local checksums, remote definitions, dependency order, ownership metadata, and remote ETags. Expose the same plan through dry-run and require the apply phase to consume an unchanged plan.
- **Acceptance:** Every artifact is classified `create`, `update`, `unchanged`, `skip_unowned`, or `blocked`; remote changes after planning fail with a concurrency error; ambiguous ownership is non-destructive by default; the plan is serializable and secret-redacted.
- **Tests:** New/update/unchanged, unowned artifact, remote mutation after plan, checksum stability, dependency ordering, and redaction.
- **Depends on:** P6-12 through P6-14.

### P6-17. Add deployment journal, resume, and compensating rollback - **P1 / READY**
- **Problem:** A mid-wave failure leaves no durable machine-readable checkpoint and recovery relies on manual interpretation of lineage and logs.
- **Scope:** Persist plan ID, operation attempts, prior remote definitions/ETags, successful mutations, failures, and rollback status. Support idempotent resume and compensating restoration/deletion for agent-owned artifacts.
- **Acceptance:** Interrupted deployment resumes without replaying completed operations; rollback restores updates and deletes newly created owned artifacts in reverse dependency order; failed compensation is explicit and resumable; journals contain no secrets.
- **Tests:** Failure at each artifact layer, process restart/resume, repeated resume, rollback of creates and updates, partial rollback failure, and redaction.
- **Depends on:** P6-16.

### P6-18. Verify Azure Function stub deployment to readiness - **P1 / READY**
- **Problem:** Zip deployment can return before Functions are indexed and callable, and generated stubs may still contain manual TODO work.
- **Scope:** Poll Kudu/Function deployment to a terminal state, collect sanitized failure logs, enumerate expected functions/routes, and distinguish generated placeholders from completed ports in the fidelity manifest.
- **Acceptance:** Deployment succeeds only after all expected functions are indexed; failed or timed-out deployments return actionable structured errors; blocking stubs prevent pipeline readiness; completed functions receive a lightweight HTTP readiness probe where configured.
- **Tests:** Mocked successful, failed, timed-out, missing-function, TODO-stub, and redacted-log cases.
- **Depends on:** P6-11 and P6-14.

### P6-19. Gate trigger activation on smoke and parity evidence - **P1 / READY**
- **Problem:** Trigger activation is a separate operation that does not require proof that the exact deployed revision passed smoke or parity checks.
- **Scope:** Record deployment-plan identity and validation evidence, run selected smoke/parity checks against that revision, and make activation reject stale, failed, incomplete, or blocking evidence unless an audited override is supplied.
- **Acceptance:** Activation names the exact deployment and test evidence; any failed pipeline, unresolved blocker, stale artifact checksum, or unready Function prevents activation; dry-run explains every decision; override use is explicit in the journal.
- **Tests:** Passing gate, failed smoke, failed parity, stale deployment, missing evidence, Function not ready, dry-run, and override audit.
- **Depends on:** P6-13 and P6-16 through P6-18.

### P6-20. Add golden DTSX regression fixtures for reviewed semantics - **P1 / READY**
- **Problem:** Existing tests did not cover disabled conversion or property expressions and allowed several parseable-but-wrong graph transformations to regress.
- **Scope:** Add minimal sanitized DTSX fixtures and expected normalized IR/artifacts for disabled tasks, every precedence mode, nested data flows, Join/Union, Sequence boundaries, loop ordering, handlers, property expressions, FTP/Bulk references, and SQL Agent schedules.
- **Acceptance:** Each P6 correctness defect has a fixture that fails on the pre-fix implementation and passes only when semantic behavior is preserved; expected artifacts are deterministic and reviewed as golden files.
- **Tests:** Run fixture tests on every supported Python version and include them in the default suite, not an optional marker.
- **Depends on:** Fixtures should land with P6-2 through P6-15 rather than as a final test-only change.

### P6-21. Add an ephemeral Azure integration and recovery pipeline - **P2 / READY**
- **Problem:** Unit and SDK-shape tests cannot prove that generated artifacts publish, run, fail closed, and recover correctly in the real ADF control plane.
- **Scope:** Provision an isolated test factory, deploy a small non-secret fixture estate, run smoke tests, exercise one forced mid-deploy failure/resume, verify triggers remain stopped, and tear down all resources. Keep pull-request validation mocked and run this pipeline on schedule or release candidates.
- **Acceptance:** The run publishes logs and deployment journal as artifacts, enforces a cost/time budget, always attempts cleanup, and blocks release promotion on artifact publish/run/recovery regressions.
- **Tests:** Scheduled Azure pipeline plus an offline test of pipeline configuration and cleanup guards.
- **Depends on:** P6-13 and P6-16 through P6-20. Requires an Azure test subscription and service connection.

### P6-22. Align coverage, workflow, and release gates with fidelity status - **P2 / READY**
- **Problem:** Documentation can label a construct supported even when only structural JSON or an executable placeholder exists.
- **Scope:** Generate or verify coverage claims from golden fixtures and fidelity records; document strict conversion/deployment defaults, override policy, error codes, deployment journals, and activation evidence; make unresolved P0/P1 fidelity gaps release blockers.
- **Acceptance:** Every supported/partial coverage row links to semantic evidence; `workflow.md` uses the strict validator and activation gate; roadmap and changelog identify schema/behavior changes; a consistency test fails when code, manifest status, and coverage disagree.
- **Tests:** Documentation link/claim consistency and published-schema compatibility checks in the default suite.
- **Depends on:** P6-11 through P6-21.

---

## Suggested execution order

All completed B / H / M / N / P3 / P4 / P5 items remain historical evidence.
Active engineering work should proceed in this order:

1. **P6-1**: restore a working CLI and CI surface.
2. **P6-2 through P6-7**: establish normalized control-flow semantics and recursive generation.
3. **P6-8 through P6-10**: close Data Flow, artifact-reference, and dynamic-property correctness gaps.
4. **P6-11 through P6-14**: make fidelity, validation, deployment readiness, and error signaling machine-enforceable.
5. **P6-15**: complete trigger schedule fidelity.
6. **P6-16 through P6-19**: add safe plan/apply, recovery, Function readiness, and activation gates.
7. **P6-20 through P6-22**: complete regression evidence, live Azure proof, and documentation/release alignment.
8. **B2 / M4 / M8 / P6-21**: collect customer or test-subscription evidence when the required Azure environment and sanitized estates are available.
9. **P5-15**: remains parked in [maybe.md](maybe.md) pending the release-engineering decision.
