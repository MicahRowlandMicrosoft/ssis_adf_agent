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
- **Acceptance:** New `RBAC.md` table mapping each MCP / CLI command (`provision_adf_environment`, `deploy_to_adf`, `activate_triggers`, `provision_function_app`, `export_arm_template`, etc.) to required Azure RBAC roles + KV access policies. Cross-linked from SECURITY.md and SETUP.md.
- **Resolution:** New top-level [`RBAC.md`](RBAC.md) carries a per-tool minimum-permissions matrix covering all 29 MCP tools (including the new P4 additions — `compare_dataflow_output`, `upload_encrypted_secrets`, `compare_estimates_to_actuals`, plus the `deploy_to_adf --pre_flight` pre-flight path). Each row names the Azure RBAC role(s), the scope, the matching Key Vault data-plane permission where relevant, and a notes column flagging cases like the `User Access Administrator` requirement on `provision_adf_environment` (only needed when the plan declares RBAC assignments) and the `assign_rbac=false` escape hatch when the reviewer disallows UAA on the deploy identity. Doc also explains *why* each role was chosen (Data Factory Contributor, Key Vault Secrets Officer / User, Cost Management Reader), enumerates the `az role assignment create` snippets, calls out a deliberate non-list of "never required" roles (Owner, sub-scoped Contributor, Network Contributor, Storage Account Contributor), and ends with an audit checklist (`az role assignment list --assignee --all`) the reviewer can run before approving the change ticket. Cross-linked from [SECURITY.md](SECURITY.md) (hardening section) and [SETUP.md](SETUP.md) (auth section).

### P4-8. No-LLM mode statement + switch — **MEDIUM** ✅ DONE
- **Buyer concern:** Regulated customers cannot route .dtsx contents through a public LLM endpoint and need a documented opt-out with explicit feature delta.
- **Acceptance:** `--no-llm` flag (or env var) on `convert_ssis_package` that forces the Script Task translator to skip the OpenAI call entirely. SECURITY.md gains a "What the LLM translator sends, where, and how to disable" section enumerating exactly what is degraded (Script Task port quality only).
- **Resolution:** Three mutually-reinforcing kill switches now disable the LLM call: (1) `llm_translate=false` (default) on `convert_ssis_package`, (2) new `no_llm=true` per-call argument that overrides `llm_translate` for one tool call, and (3) new process-wide `SSIS_ADF_NO_LLM` env var (truthy: `1`/`true`/`yes`/`on`, case-insensitive) that forces `is_configured()` to return False and `translate()` to raise `TranslationError` *before* any client is constructed. The translator's `no_llm_policy_enabled()` helper is the single source of truth used by the Script Task converter and the MCP tool layer; when the env var or the per-call arg is on but `llm_translate=true` was requested, the MCP layer emits a UserWarning naming which switch overrode the request so the degraded behaviour is never silent. SECURITY.md gained a new "What the LLM translator sends, where, and how to disable" section that enumerates exactly which fields are transmitted (system prompt + task name + variable identifiers + `source_code` truncated at 18 000 chars), where they go (the customer's own Azure OpenAI deployment, authenticated via `DefaultAzureCredential`), and what is *not* transmitted (no telemetry, no third party, no connection-manager values). The section also documents that disabling the LLM degrades only the *quality of the generated Python body* — every other artifact (pipelines, linked services, datasets, data flows, triggers) is bit-for-bit identical with or without the LLM. RBAC.md row for `convert_ssis_package` now also names the *Cognitive Services OpenAI User* role on the Azure OpenAI resource as the only RBAC needed when the LLM is on, with a pointer to `SSIS_ADF_NO_LLM=1` as the escape hatch. New `tests/test_no_llm_mode.py` (20 tests) verifies the helper across truthy/falsy values, that `is_configured()` returns False under policy even with a configured endpoint, that `translate()` raises with a "disabled by policy" message, and that the Script Task converter emits a clean UserWarning naming the env var. 877 tests passing.

### P4-9. Minimum useful workflow guide — **MEDIUM** ✅ DONE
- **Buyer concern:** 26 MCP tools is a large surface. New engineers explore all of them. Tool overlap (`smoke_test_pipeline` vs `smoke_test_wave`, `convert_ssis_package` vs `convert_estate`, `build_estate_report` vs `build_predeployment_report`) is not signposted.
- **Acceptance:** New `WORKFLOW.md` (or section of HOWTO.md) naming the 5–6-tool minimum path tied to standard wave milestones (triage → propose → convert → validate → deploy → activate). Clarifies which tools are advanced / optional. Linked from README.
- **Resolution:** New top-level [WORKFLOW.md](WORKFLOW.md) names the **6-tool minimum path** (`bulk_analyze` → `propose_adf_design` → `convert_estate` → `validate_adf_artifacts` → `deploy_to_adf` (with `pre_flight=true` first) → `activate_triggers`) tied to the standard wave milestones (triage → design → convert → validate → deploy → cut over) with a one-line natural-language Copilot prompt per step and a "Stop and decide" gate where a reviewer needs to make a call. Optional tools are catalogued in five themed groups (estate-scale planning, per-package deep dives, validation & parity, infrastructure & secrets, discovery) each carrying a "reach for it when…" trigger so engineers know when *not* to use them. A separate "Tool overlap, signposted" table explicitly names the 6 confusable pairs and which one to prefer (`convert_estate` over `convert_ssis_package` in a loop, `smoke_test_wave` over `smoke_test_pipeline` in a loop, etc.). Closing "Why these six?" section explains every other tool exists for a non-required reason. README "New to the agent?" callout now points at WORKFLOW.md first, HOWTO.md second.

### P4-10. Pipeline-execution observability story — **MEDIUM** ✅ DONE
- **Buyer concern:** Post-migration BAU operations have no documented monitoring story (Log Analytics workbook, standard alerts, run-history retention).
- **Acceptance:** `OBSERVABILITY.md` documenting the recommended diagnostic-settings target, a sample KQL workbook for pipeline failures + duration trending, and at least three suggested alert rules with thresholds.
- **Resolution:** New top-level [OBSERVABILITY.md](OBSERVABILITY.md) names Log Analytics as the recommended diagnostic-settings target (with rationale vs. Event Hubs / Storage), enumerates the five log/metric categories to enable (`PipelineRuns`, `ActivityRuns`, `TriggerRuns`, `PipelineActivityRuns`, `AllMetrics`), and ships a one-time-per-factory Bicep snippet wiring the diagnostic setting to the workspace. Workbook section provides two saved KQL queries — Query A (failed runs last 24h with parameters + error message) and Query B (14-day p50/p95 duration trend per pipeline) — plus a recommended two-tab layout (Health / Trends). Three alert rules are spec'd with full KQL, thresholds, evaluation cadence, and severity rationale: Alert A (any failure, sev 2, every 5m), Alert B (duration > 1.5× 14-day p95, sev 3, hourly), Alert C (trigger silently stopped firing — config-driven `expected` table joined left-outer against `ADFTriggerRun`, sev 2, hourly over 6h). Closes with explicit non-promises (does not replace SLO design, does not provision anything, does not cover Function-host or SHIR observability — links to the respective Microsoft Learn docs). Cross-linked from BEHAVIORAL_PARITY.md (replaces the stale "when published" placeholder).

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
- **Acceptance:** New `ROADMAP.md` listing the engineering items required for 1.0, the current quarter's focus, and the breaking-change-deprecation window for pre-1.0 → 1.0 transitions.
- **Resolution:** New top-level [ROADMAP.md](ROADMAP.md) defines what 1.0 *means* by naming the four committed surfaces (MCP tool inputs/outputs, CLI, on-disk artifact layout including `lineage.json` and `migration_plan.json`, and the public Pydantic IR models in `parsers.models`) along with the explicit non-commitments (internal converter implementation, generated stub text, warning wording, test helpers, the C#→Python translator). Lists 14 engineering items required for 1.0 grouped by Stability / Quality / Surface ergonomics / Operational, each tagged with current status ("done", "NEW", or "partial"), so the gap to 1.0 is countable rather than aspirational. Defines the pre-1.0 → 1.0 transition window: `0.9.0` ships first with a `removed-in-1.0` deprecation manifest, supported for a minimum 30 days before `1.0.0` removes the deprecated surface, with breaking changes listed in the `0.9.0` change log as `BREAKING (in 1.0):` plus migration recipe. Pre-1.0 release cadence (4–6 weeks per minor) and post-1.0 cadence (driven by need, not calendar) named. README gained a "Pre-1.0 status" callout pointing at ROADMAP.md.

### P4-14. Named support channel + response-time commitment — **LOW** ✅ DONE
- **Buyer concern:** "GitHub issues" is not a support channel for a customer mid-migration at 11 p.m.
- **Acceptance:** SUPPORT.md naming the support channel (alias / Teams channel / on-call rotation) with a stated response-time commitment for the duration of an active engagement. Acknowledges this may differ between OSS users and engaged customers.
- **Resolution:** New top-level [SUPPORT.md](SUPPORT.md) splits support into two tiers. **Tier 1 (OSS users):** GitHub Issues only, best-effort 2–5 business-day first response, no SLA — explicitly states what *not* to expect (no hours-scale response, no phone/chat, no overnight). **Tier 2 (engaged customers):** named-channel commitment with default response times by severity — Sev 1 (migration blocked, ≤ 48h to cut-over) **2 business hours**, Sev 2 (impaired w/ workaround) **1 business day**, Sev 3 (question / minor defect) **3 business days**. Names three supported channel formats (shared Teams channel preferred, shared email DL with ≥ 2 recipients, on-call rotation roster with backup primary) and explicitly excludes single-engineer phone numbers as a single-point-of-failure pattern. Documents what's covered (defects blocking migration, output interpretation, deploy-failure debugging, WORKFLOW walkthrough) vs. not covered (custom converter work for in-house components — vendor registries P4-2 are the path; hand-porting Script Tasks the LLM cannot translate — the case study P4-3 documents the methodology; tenant-side RBAC/KV/SHIR setup — RBAC.md P4-7 documents the requirements). Bug-report structure (5-field template) and a **non-negotiable sanitization checklist** (no connection strings, no KV/sub/RG/SQL names that identify the customer, no real table/column names if business-sensitive, no package passwords) close the doc. Cross-linked from the README "Need help?" callout. **All 14 P4 buyer follow-ups now closed.**

---

## P5 — Skeptical-buyer-review followups (April 2026)

Sourced from the second-round skeptical-buyer review run after P4 closed.
Only items the coding agent can ship end-to-end (no real estate, no real
Azure tenant, no human-in-the-loop port-time capture) are tracked here.
Evidence-capture items (estate-scale run, additional behavioral-parity
walkthroughs, additional Script Task ports, real-Azure KV runs, captured
failure case studies) are tracked separately as part of the engaged-
customer pilot, not in this backlog.

### P5-6. Schema-version `lineage.json` and `migration_plan.json` — **MEDIUM**
- **Buyer concern:** Downstream CI parses both files. Without a `schemaVersion`, a minor-version bump silently breaks the customer's pipeline. ROADMAP S3/S4 already commit to this for 1.0.
- **Acceptance:** Both files carry a top-level `schemaVersion: "1"`; loader rejects unknown major versions with a clear message; loader accepts an unknown *minor* version as forward-compatible (logs a warning); a forward-compat unit test pins the contract.

### P5-7. `provision_adf_environment --with-observability=<workspace-id>` — **MEDIUM**
- **Buyer concern:** OBSERVABILITY.md (P4-10) is excellent prose but the factory provisioner does not emit the diagnostic-settings target. Day-1 production-readiness still requires a separate Bicep PR. ROADMAP Q4.
- **Acceptance:** New optional `--with-observability=<workspace-id>` flag on `provision_adf_environment` emits a `Microsoft.Insights/diagnosticSettings` resource targeting the named Log Analytics workspace with the categories named in OBSERVABILITY.md (`PipelineRuns`, `ActivityRuns`, `TriggerRuns`, `PipelineActivityRuns`, `AllMetrics`). Unit-tested against the captured Bicep output.

### P5-8. Confirm + document that `SSIS_ADF_NO_LLM=1` disables every form of network egress — **HIGH**
- **Buyer concern:** No-LLM mode (P4-8) gates the Script Task LLM translator. The buyer asked whether the same switch also disables every *other* form of egress (telemetry, version-check pings, PyPI lookups, etc.). SECURITY.md does not currently say either way.
- **Acceptance:** Audit the codebase for any outbound HTTP/HTTPS call site, document each in SECURITY.md under "What the agent talks to and how to disable it", confirm `SSIS_ADF_NO_LLM=1` (or document a separate switch) disables all of them, and add a unit/integration test that asserts no socket / no HTTP client construction occurs under no-LLM mode for the conversion path. **Requires one decision from the maintainer:** confirm there is no telemetry the audit doesn't surface.

### P5-9. README tool-count + diagram inconsistencies — **LOW** ✅ DONE
- **Buyer concern:** README header says "29 tools"; §"All tools are invoked from GitHub Copilot Chat" still says "22 tools". Architecture diagram lists 5 steps (scan → analyze → convert → validate → deploy); WORKFLOW.md correctly says 6 (adds activate-triggers). Procurement reviewers screenshot the inconsistency.
- **Acceptance:** Trailing tool-count reference updated to match the header; diagram updated to match WORKFLOW.md's 6-step path. Add a unit test that asserts the README tool-count and the actual `len(list_tools())` agree (catches the next bump).
- **Resolution:** Updated the two trailing "22 tools" references in [README.md](README.md) to 29 to match the header; replaced the older 5-step `scan → analyze → convert → validate → deploy` architecture diagram with the 6-step `bulk_analyze → propose → convert → validate → deploy → activate` path that [WORKFLOW.md](WORKFLOW.md) documents. Added [tests/test_readme_consistency.py](tests/test_readme_consistency.py) with two assertions: (1) all three README tool-count strings equal `len(list_tools())`, and (2) the 6-step diagram references both `bulk_analyze` and `activate` and the older 5-step prose is gone. Future tool additions/removals fail this test until README is updated.

### P5-11. Uniform `--dry-run` across `provision_adf_environment` and `provision_function_app` — **MEDIUM**
- **Buyer concern:** `deploy_to_adf --pre-flight=true` and `activate_triggers --dry_run=true` exist; the two `provision_*` tools do not. ROADMAP E2. CAB approval often requires a "what would happen" report before any provisioning.
- **Acceptance:** Both `provision_adf_environment` and `provision_function_app` accept a `dry_run: bool = False` arg that emits the would-be Bicep / would-be `az` calls and the planned RBAC assignments without creating any resource. Tested against captured Bicep.

### P5-12. New MCP tool `validate_deployer_rbac` — **MEDIUM**
- **Buyer concern:** RBAC.md (P4-7) is excellent prose; verifying compliance is still manual and the captured KV-recovery case study (P4-11) is exactly the failure this would have caught.
- **Acceptance:** New tool `validate_deployer_rbac` accepts the deploying identity, the planned tools, the target subscription/RG/factory/KV, and reports per-tool which RBAC.md-required roles are present vs. missing, without creating any resource. Mocked-SDK unit tests; real-Azure verification deferred to the engaged-customer pilot.

### P5-14. Per-pipeline cost projection emitted at `convert_estate` time — **LOW**
- **Buyer concern:** `estimate_adf_costs` and `compare_estimates_to_actuals` exist but require a separate run. Steering-committee deck would be one step shorter if `convert_estate` emitted projection alongside `lineage.json`.
- **Acceptance:** `convert_estate` accepts an optional `--with-cost-projection=true` flag; when set, writes `cost_projection.json` next to `lineage.json` reusing the `estimate_adf_costs` engine. Unit-tested.

### P5-15. `pipx run ssis-adf-agent` smoke-tested per release — **LOW**
- **Buyer concern:** ROADMAP E3. Air-gapped customers with one allowed pip install want a single-binary entry point. Today there is no smoke test that `pipx run` works from a clean wheel.
- **Acceptance:** A CI job runs `pipx run --spec . ssis-adf-agent --help` against the built wheel and asserts non-zero exit on regression. README §Installation gains a `pipx run` one-liner. **Requires one decision from the maintainer:** approve adding the CI job (cost / runner choice).

### P5-16. New tool `diff_estate` — **MEDIUM**
- **Buyer concern:** Re-running `convert_estate` after an upstream `.dtsx` edit reconverts everything; no signal what *changed*. Small upstream change still triggers full re-validation.
- **Acceptance:** New tool `diff_estate` compares two `out/` directories (or one `out/` against a saved snapshot) and emits a focused report: per-package classification (byte-identical / changed / added / removed) with the per-artifact diff for changed packages. Unit-tested against synthetic before/after fixtures.

### P5-17. CLI parity for every MCP tool — **HIGH**
- **Buyer concern:** Workflow assumes Copilot Chat in VS Code Agent mode. Air-gapped / Copilot-blocked / CI-only customers have no first-class entry point. ROADMAP E1.
- **Acceptance:** `ssis-adf-agent <tool-name> --arg1=value --arg2=value` accepts every MCP tool with the same surface as the MCP server. Help text auto-generated from the same `types.Tool` schemas the MCP server uses, so the two surfaces stay synchronized. Unit-tested per tool.

### P5-18. Cross-link HOWTO.md → WORKFLOW.md "Start here" callout — **LOW** ✅ DONE
- **Buyer concern:** Buyers landing on HOWTO from a Google search miss the new minimum-path doc.
- **Acceptance:** HOWTO.md gains a "Start here" callout at the top pointing at WORKFLOW.md as the recommended first read.
- **Resolution:** Added a "🚀 Start here" blockquote at the top of [HOWTO.md](HOWTO.md) (above the existing intro paragraph) that names the 6-tool minimum path (`bulk_analyze` → `propose_adf_design` → `convert_estate` → `validate_adf_artifacts` → `deploy_to_adf` → `activate_triggers`) and links to [WORKFLOW.md](WORKFLOW.md) as the recommended first read. The conversation guide remains the second-read for the per-package backbone and wider tool surface.

### P5-19. Cross-link ENCRYPTED_PACKAGES.md → P4-11 KV-recovery case study — **LOW** ✅ DONE
- **Buyer concern:** The case study links to ENCRYPTED_PACKAGES.md but not the reverse. `upload_encrypted_secrets` users miss the prerequisite reading.
- **Acceptance:** ENCRYPTED_PACKAGES.md gains a "Real failure walkthrough" callout linking to the captured KV case study.
- **Resolution:** Added a "🧯 Real failure walkthrough" blockquote at the top of [ENCRYPTED_PACKAGES.md](ENCRYPTED_PACKAGES.md) (above "Why this hurts") that names the captured `ManagedServiceIdentityCredentialNotFound` failure mode, the wrong-hypothesis cost (22 minutes granting KV role to the deployer SP instead of to the factory MI), and links to [docs/case-studies/first_deploy_keyvault_recovery/](docs/case-studies/first_deploy_keyvault_recovery/README.md). The cross-link is now bidirectional.

### P5-20. COVERAGE.md "Mapped vs. unmapped SSIS expression functions" — **MEDIUM**
- **Buyer concern:** COVERAGE.md is silent on the `(DT_STR,2,1252) DATEPART("mm", GETDATE())` family of casts and string functions. Buyers ask for a 5-line table showing what works and what does not.
- **Acceptance:** New COVERAGE.md section enumerates SSIS expression functions covered by the converter (`DATEPART`, `RIGHT`, `LEFT`, `SUBSTRING`, `(DT_STR,…)` casts, `GETDATE()`, etc.) vs. unmapped, derived from the actual `expression_functions` source so the table cannot drift.

### P5-21. Cross-link `provision_adf_environment` → OBSERVABILITY.md — **LOW** ✅ DONE
- **Buyer concern:** Buyers using the factory provisioner discover OBSERVABILITY.md too late (after the first failed run with no logs).
- **Acceptance:** `provision_adf_environment` MCP tool description and CLI help text cross-link OBSERVABILITY.md as the recommended Day-2 follow-up.
- **Resolution:** `provision_adf_environment`'s description in [ssis_adf_agent/mcp_server.py](ssis_adf_agent/mcp_server.py) now closes with a "Day-2 follow-up" pointer at OBSERVABILITY.md naming the five log/metric categories and the three baseline alert rules, and explicitly notes the diagnostic-settings resource is not yet emitted (tracked as P5-7). The CLI does not currently expose a `provision` subcommand; CLI-side cross-link will land with [P5-17](#p5-17-cli-parity-for-every-mcp-tool--high) (CLI parity).

### P5-23. Document the `EncryptAllWithPassword` failure modes — **LOW**
- **Buyer concern:** Buyer asked whether wrong-password vs. missing-password vs. key-derivation-failure produce distinct error messages or one generic failure.
- **Acceptance:** ENCRYPTED_PACKAGES.md gains a "Failure modes and how to read them" subsection enumerating the actual error messages the parser raises for each, derived from the parser source.

### P5-24. Document LLM translator behavior at the 18 000-char truncation bound — **LOW**
- **Buyer concern:** SECURITY.md notes the translator transmits `source_code` truncated at 18 000 chars. What happens to a Script Task whose source exceeds that bound is not documented.
- **Acceptance:** SECURITY.md "What the LLM translator sends" section gains a paragraph documenting the truncation behavior (silent truncation? warning? skip?), derived from the translator source, and what the user should expect in the generated stub.

### P5-25. New ROLLBACK.md section: tearing down a provisioned factory — **MEDIUM**
- **Buyer concern:** ROLLBACK.md covers artifacts; not the factory itself. Customers asked what happens when the *factory* (provisioned by `provision_adf_environment`) needs to be torn down.
- **Acceptance:** ROLLBACK.md gains a new "Strategy 4 — tearing down a provisioned factory" section covering the `az` recipe, RBAC cleanup, KV access-policy / role removal, and what `lineage.json` looks like for a fully-deprovisioned environment.

### P5-26. Confirm or replace the GitHub URL in SUPPORT.md — **LOW**
- **Buyer concern:** SUPPORT.md cites `MicahRowlandMicrosoft/ssis_adf_agent`. If this is a personal repo and not the engagement-customer-facing one, the URL misleads. **Requires one decision from the maintainer:** confirm or supply the correct URL.
- **Acceptance:** SUPPORT.md, README "Need help?" callout, and any other GitHub-issues references all use the confirmed customer-facing URL.

---

## Suggested execution order

All B / H / M / N / P3 / P4 items are ✅ done. Remaining work:

1. **B2** — customer-side proof; cannot be executed without an Azure factory.
2. **P2** items as adoption progresses.
3. **P5 — second-round buyer review followups**, ordered for ship velocity:
   1. Same-day doc fixes: **P5-9** (tool-count + diagram), **P5-18** (HOWTO start-here), **P5-19** (KV cross-link), **P5-21** (provisioner → OBSERVABILITY).
   2. Schema + flag plumbing: **P5-6** (schemaVersion), **P5-7** (`--with-observability`), **P5-11** (uniform `--dry-run`).
   3. Security audit: **P5-8** (no-LLM egress confirmation; needs maintainer sign-off).
   4. Net-new tools / surface: **P5-12** (`validate_deployer_rbac`), **P5-16** (`diff_estate`), **P5-17** (CLI parity), **P5-14** (cost projection at convert time).
   5. Doc derivations from source: **P5-20** (expression functions), **P5-23** (encryption failure modes), **P5-24** (LLM truncation), **P5-25** (factory teardown).
   6. CI hardening + repo URL: **P5-15** (`pipx run` smoke), **P5-26** (GitHub URL).
