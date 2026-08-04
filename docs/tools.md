# MCP Tools Reference

The `ssis-adf-agent` MCP server registers **31 tools**, grouped here by
lifecycle phase. Each row links to the deeper doc that explains *when*
to use the tool and *what* the output looks like in context.

For the recommended end-to-end path through these tools, see
[workflow.md](getting-started/workflow.md). For copy-paste prompts and
worked examples, see [howto.md](getting-started/howto.md).

> The authoritative source of tool behavior is always the tool's
> docstring in [`ssis_adf_agent/mcp_server.py`](../ssis_adf_agent/mcp_server.py).
> This page is a discoverability index — it does not redefine schemas.

---

## 1. Discover & Analyze

| Tool | Purpose | Deep dive |
|---|---|---|
| `scan_ssis_packages` | Find every `.dtsx` from a local path, Git repo, or SQL Server (msdb). | [setup.md](getting-started/setup.md), [workflow.md](getting-started/workflow.md) |
| `bulk_analyze` | Estate-scale triage: complexity score + recommended target pattern per package, bucketed low/medium/high/very_high. | [coverage.md](conversion/coverage.md), [effort-methodology.md](conversion/effort-methodology.md) |
| `analyze_ssis_package` | Single-package complexity, gap analysis, dependency execution order, ESI / CDM detection. | [effort-methodology.md](conversion/effort-methodology.md) |
| `explain_ssis_package` | Structured explanation + Mermaid control-flow / data-flow diagrams of one package. | [workflow.md](getting-started/workflow.md) |

## 2. Design (Migration Plan)

| Tool | Purpose | Deep dive |
|---|---|---|
| `propose_adf_design` | SSIS → ADF design recommendation: target pattern, simplifications, infra, RBAC, risks, effort. | [workflow.md](getting-started/workflow.md), [howto.md](getting-started/howto.md) |
| `save_migration_plan` | Persist a MigrationPlan JSON to disk. | [workflow.md](getting-started/workflow.md) |
| `load_migration_plan` | Load + validate a saved plan; renders Markdown for review. | [workflow.md](getting-started/workflow.md) |
| `edit_migration_plan` | Structured edits (set_auth_mode, add_simplification, set_customer_decision, …) — safer than hand-editing JSON. | [workflow.md](getting-started/workflow.md) |
| `plan_migration_waves` | Group saved plans into ordered migration waves with optional learning-curve discount. | [effort-methodology.md](conversion/effort-methodology.md) |
| `estimate_adf_costs` | Coarse monthly USD projection from saved plans (Copy / Data Flow / orchestration mix). | [effort-methodology.md](conversion/effort-methodology.md) |

## 3. Convert

| Tool | Purpose | Deep dive |
|---|---|---|
| `convert_ssis_package` | Single-package conversion → pipeline / linked-service / dataset / data-flow / trigger / Function-stub JSON. Supports `translation_mode` (`none` / `host` / `aoai`) for Script Task translation. | [workflow.md](getting-started/workflow.md), [script-task-translation.md](conversion/script-task-translation.md), [SUBSTITUTION_REGISTRY.md](SUBSTITUTION_REGISTRY.md) |
| `convert_estate` | Bulk conversion of every `.dtsx` in a directory; per-package status summary. Aggregates Script Task translation manifests into `translation_index.json`. | [workflow.md](getting-started/workflow.md), [script-task-translation.md](conversion/script-task-translation.md), [coverage.md](conversion/coverage.md) |
| `consolidate_packages` | Detect structurally identical packages and emit a single parameterized child + ForEach parent pipeline. | [workflow.md](getting-started/workflow.md) |
| `explain_adf_artifacts` | Structured explanation + Mermaid activity-graph of generated ADF artifacts. | [workflow.md](getting-started/workflow.md) |
| `diff_estate` | Compare two `convert_estate` output directories; per-package byte-identical / changed / added / removed + unified diffs. | [workflow.md](getting-started/workflow.md) |

## 4. Reports

| Tool | Purpose | Deep dive |
|---|---|---|
| `build_estate_report` | Customer-facing PDF: executive summary, complexity & pattern breakdown, waves, projected costs, per-package detail. | [effort-methodology.md](conversion/effort-methodology.md) |
| `build_predeployment_report` | Engineer / admin pre-deployment Markdown (and optional PDF) with Mermaid diagrams + manual-task checklists. **Recommended before `deploy_to_adf`.** | [workflow.md](getting-started/workflow.md) |

## 5. Validate

| Tool | Purpose | Deep dive |
|---|---|---|
| `validate_adf_artifacts` | Structural validation of generated JSON. | [workflow.md](getting-started/workflow.md) |
| `validate_conversion_parity` | Pre-deployment SSIS-vs-ADF logic parity check; optional SDK dry-run, factory reachability, PDF report. | [parity.md](conversion/parity.md) |
| `compare_dataflow_output` | Behavioral parity: run identical input through SSIS Data Flow + ADF Mapping Data Flow; row & column diff. | [behavioral-parity.md](conversion/behavioral-parity.md) |
| `validate_deployer_rbac` | Read-only RBAC compliance check — confirms the deploying identity has the minimum roles for a planned tool set. | [rbac.md](operations/rbac.md) |

## 6. Provision & Deploy

| Tool | Purpose | Deep dive |
|---|---|---|
| `provision_adf_environment` | Generate + deploy Bicep for ADF (system-assigned MI), Storage / ADLS Gen2, Key Vault (when needed), RBAC, optional Log Analytics diagnostic settings. | [observability.md](operations/observability.md), [rbac.md](operations/rbac.md) |
| `provision_function_app` | Provision Storage + App Insights + Consumption Plan + Python Linux Function App for hosting Script-Task stubs. | [workflow.md](getting-started/workflow.md) |
| `export_arm_template` | Bundle a converted artifacts directory into an ARM template (azd / `az deployment` workflows). | [workflow.md](getting-started/workflow.md) |
| `upload_encrypted_secrets` | Extract secrets from a decrypted `.dtsx`, push to Key Vault, rewrite linked-service secret names. | [encrypted-packages.md](operations/encrypted-packages.md) |
| `deploy_to_adf` | Deploy artifacts in dependency order. Triggers always land Stopped. Supports `skip_if_exists` (non-destructive) and `pre_flight` (reachability probe). | [workflow.md](getting-started/workflow.md), [rollback.md](operations/rollback.md) |
| `deploy_function_stubs` | Zip-deploy generated Azure Function stubs to an existing Function App. | [workflow.md](getting-started/workflow.md) |
| `activate_triggers` | Bulk-activate triggers landed Stopped by `deploy_to_adf`. Defaults to dry-run. | [rollback.md](operations/rollback.md), [workflow.md](getting-started/workflow.md) |

## 7. Operate

| Tool | Purpose | Deep dive |
|---|---|---|
| `smoke_test_pipeline` | Trigger a single pipeline run and return per-activity results. | [workflow.md](getting-started/workflow.md) |
| `smoke_test_wave` | Cross-pipeline regression harness; per-pipeline status + summary + optional fail-fast. | [workflow.md](getting-started/workflow.md) |
| `compare_estimates_to_actuals` | Join the deployed lineage manifest with an Azure Cost Management export → per-factory variance vs. `estimate_adf_costs`. | [observability.md](operations/observability.md) |

---

## See also

- [setup.md](getting-started/setup.md) — environment prerequisites
- [workflow.md](getting-started/workflow.md) — recommended end-to-end path
- [howto.md](getting-started/howto.md) — conversation examples + copy-paste prompts
- [SUBSTITUTION_REGISTRY.md](SUBSTITUTION_REGISTRY.md) — mapping 3rd-party SSIS components to ADF
- [rbac.md](operations/rbac.md) — minimum RBAC per tool
- [observability.md](operations/observability.md) — Day-1 logging / alerting
- [rollback.md](operations/rollback.md) — undo / recovery patterns
- [parity.md](conversion/parity.md), [behavioral-parity.md](conversion/behavioral-parity.md) — structural & behavioral parity
- [coverage.md](conversion/coverage.md), [effort-methodology.md](conversion/effort-methodology.md) — what's supported and how effort is scored
