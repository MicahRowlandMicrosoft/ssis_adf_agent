# SSIS → ADF Agent

![Python](https://img.shields.io/badge/python-3.11%2B-blue)
![License](https://img.shields.io/badge/license-MIT-green)

An MCP (Model Context Protocol) server that turns SSIS migration into an agent-driven workflow inside **GitHub Copilot**.

The server exposes **22 tools** that span the full lifecycle: estate-scale triage, design proposal & plan editing, wave planning & cost projection, deterministic SSIS → ADF conversion, infrastructure provisioning (Bicep), deployment, and post-deployment smoke testing.

All generated artifacts follow **Microsoft Recommended patterns** from [learn.microsoft.com](https://learn.microsoft.com/en-us/azure/data-factory/).

```
.dtsx file(s)  ───┐
                  │      ┌────────────────────────┐
SQL Agent jobs ───┤      │  Optional configs:     │
                  ├─────>│ • ESI tables JSON      │
  Config files ───┘      │ • Schema remap JSON    │
                         │ • Shared artifacts dir │
                         └──────────┬─────────────┘
                                    ▼
                    ┌─────────────────────────────┐
                    │      ssis-adf-agent         │  ← MCP stdio server
                    │                             │
                    │  scan → analyze → convert   │
                    │      → validate → deploy    │
                    │                             │
                    │  Detects:                   │
                    │  • Cross-DB / linked server │
                    │  • Delta / MERGE patterns   │
                    │  • CDM-layer logic          │
                    │  • ESI reuse candidates     │
                    └──────────┬──────────────────┘
                               ▼
                    ADF JSON artifacts
            (pipeline / linkedService / dataset /
             dataflow / trigger / stubs)
                               ▼
                    Azure Data Factory
```

---

## Table of Contents

- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Registering as an MCP Server in VS Code](#registering-as-an-mcp-server-in-vs-code)
- [Trying It Out — Samples Directory](#trying-it-out--samples-directory)
- [Migration Copilot Workflow (recommended)](#migration-copilot-workflow-recommended)
- [Usage — End-to-End Walkthrough](#usage--end-to-end-walkthrough)
  - [1. Scan for packages](#1-scan-for-packages)
  - [2. Analyze a package](#2-analyze-a-package)
  - [3. Convert a package](#3-convert-a-package)
  - [4. Validate generated artifacts](#4-validate-generated-artifacts)
  - [5. Deploy to Azure Data Factory](#5-deploy-to-azure-data-factory)
  - [6. Deploy Azure Function stubs](#6-deploy-azure-function-stubs)
  - [7. Provision Azure Function App](#7-provision-azure-function-app)
- [Enterprise Features](#enterprise-features)
  - [Self-Hosted Integration Runtime](#self-hosted-integration-runtime)
  - [Azure Key Vault Secrets](#azure-key-vault-secrets)
  - [Cross-Package Deduplication](#cross-package-deduplication)
  - [Schema Remapping (Database Consolidation)](#schema-remapping-database-consolidation)
  - [ESI Reuse Detection](#esi-reuse-detection)
  - [CDM Pattern Flagging](#cdm-pattern-flagging)
  - [SQL Agent Schedule Mapping](#sql-agent-schedule-mapping)
- [LLM-Powered Script Task Translation](#llm-powered-script-task-translation)
- [Using the Built-in Prompt Files](#using-the-built-in-prompt-files)
- [Authentication](#authentication)
- [SSIS Component Mapping Reference](#ssis-component-mapping-reference)
- [Generated Artifact Structure](#generated-artifact-structure)
- [Manual Steps After Conversion](#manual-steps-after-conversion)
- [Development](#development)
- [License](#license)

> **New to the agent?** See [HOWTO.md](HOWTO.md) for an end-to-end conversation guide with copy-paste example prompts.

---

## Prerequisites

| Requirement | Version / Notes |
|---|---|
| **Python** | 3.11 or later |
| **pip** | Included with Python; or use [uv](https://github.com/astral-sh/uv) / [hatch](https://hatch.pypa.io/) |
| **Git** | Required if scanning packages from a Git repository |
| **ODBC Driver for SQL Server** | 17 or later — required only when scanning packages from SQL Server (`msdb`) |
| **Azure CLI** | [`az`](https://learn.microsoft.com/cli/azure/install-azure-cli) — required for `az login` when deploying from a developer machine |
| **VS Code** | Latest stable |
| **GitHub Copilot extension** | With agent / MCP support enabled |

---

## Installation

Clone the repository and install in editable mode (recommended for development):

```bash
git clone https://github.com/chsimons_microsoft/ssis_adf_agent.git
cd ssis_adf_agent
pip install -e .
```

To also install development tools (pytest, ruff, mypy):

```bash
pip install -e ".[dev]"
```

To enable **automatic C# → Python translation** of Script Tasks via Azure OpenAI:

```bash
pip install -e ".[llm]"
```

Verify the installation:

```bash
ssis-adf-agent --help
```

> **Note:** When the package is published to PyPI, you can install it with `pip install ssis-adf-agent` without cloning the repository.

---

## Registering as an MCP Server in VS Code

Add the server to your VS Code `settings.json` so GitHub Copilot can discover it as a set of agent tools.

1. Open **Command Palette** (`Ctrl+Shift+P`) → **Preferences: Open User Settings (JSON)**
2. Add the following inside the root object:

```jsonc
{
  "github.copilot.chat.experimental.mcpServers": {
    "ssis-adf-agent": {
      "type": "stdio",
      "command": "ssis-adf-agent",
      "args": []
    }
  }
}
```

> If you installed into a virtual environment, replace `"command": "ssis-adf-agent"` with the full path to the script, e.g. `"C:\\path\\to\\.venv\\Scripts\\ssis-adf-agent.exe"` (Windows) or `"/path/to/.venv/bin/ssis-adf-agent"` (macOS/Linux).

3. Restart VS Code (or reload the window: `Ctrl+Shift+P` → **Developer: Reload Window**).
4. Open **Copilot Chat**, switch to **Agent** mode, and verify that the 22 tools appear. They group into three tiers:

   **Per-package backbone** — the deterministic conversion path:
   - `scan_ssis_packages`
   - `analyze_ssis_package`
   - `convert_ssis_package`
   - `validate_adf_artifacts`
   - `deploy_to_adf`
   - `consolidate_packages`
   - `deploy_function_stubs`
   - `provision_function_app`
   - `explain_ssis_package`
   - `explain_adf_artifacts`
   - `validate_conversion_parity`

   **Migration Copilot — design & infrastructure:**
   - `propose_adf_design` — emit a recommended `MigrationPlan` for a package
   - `edit_migration_plan` — apply structured edits (auth mode, region, simplifications)
   - `save_migration_plan` / `load_migration_plan` — round-trip plans to disk
   - `provision_adf_environment` — generate Bicep + deploy ADF / Storage / Key Vault / RBAC

   **Migration Copilot — estate scale:**
   - `bulk_analyze` — triage every `.dtsx` in a directory
   - `convert_estate` — propose + convert every package in one shot
   - `plan_migration_waves` — group saved plans into ordered delivery waves *(requires saved plans)*
   - `estimate_adf_costs` — plan-aware monthly USD projection from activity mix *(requires saved plans)*
   - `build_estate_report` — stakeholder PDF from saved plans + waves + costs *(requires saved plans)*
   - `smoke_test_pipeline` — trigger one ADF pipeline run + return per-activity results

---

## Trying It Out — Samples Directory

The `samples/` directory is intended as a convenient drop zone for `.dtsx` files you want to experiment with locally.

1. Copy one or more `.dtsx` files into `samples/`:

   ```
   samples/
     MyETLPackage.dtsx
     LoadDimCustomer.dtsx
   ```

2. When using any tool that requires a `package_path` or `path_or_connection`, supply the **absolute path** to the file or directory. For example:

   - **Windows:** `C:\Users\you\ssis_adf_agent\samples\MyETLPackage.dtsx`
   - **macOS/Linux:** `/home/you/ssis_adf_agent/samples/MyETLPackage.dtsx`

3. For output, create a directory alongside `samples/` (e.g. `adf_output/`) to keep generated artifacts separate from source packages.

> The `samples/` directory is `.gitignore`-friendly — add your test packages there without worrying about committing proprietary SSIS files.

---

## Migration Copilot Workflow (recommended)

The Migration Copilot tools wrap the per-package backbone into an estate-scale, agent-driven flow. Use this when you have a folder of SSIS packages and need to produce a credible plan and deliver it incrementally.

```
  Your SSIS project
          │
          ▼
  bulk_analyze ............... Walk all .dtsx, score, classify, roll up effort
          │
          ▼
  Per package (or convert_estate to do all at once):
     propose_adf_design ...... Recommended MigrationPlan (target pattern,
                               simplifications, linked services, infra,
                               RBAC, risks, effort)
     edit_migration_plan ..... Refinements (auth mode, region,
                               drop a fold)
     save_migration_plan
          │
          ▼
  plan_migration_waves ....... Groups saved plans into delivery waves
  estimate_adf_costs ......... Plan-aware monthly $ projection (activity mix)
  build_estate_report ........ Stakeholder PDF (executive summary + waves + cost)
          │
          ▼
  provision_adf_environment .. Generate Bicep + deploy ADF / Storage / KV / RBAC
          │
          ▼
  convert_ssis_package ....... Honors the saved plan when design_path is supplied
  validate_adf_artifacts
  deploy_to_adf
          │
          ▼
  smoke_test_pipeline ........ Trigger one run, poll, return per-activity status
```

### What `propose_adf_design` recommends

The rule-based proposer is opinionated but conservative. It emits a `MigrationPlan` covering:

- **Target pattern** — one of `scheduled_file_drop`, `ingest_file_to_sql`, `sql_to_sql_copy`, `incremental_load`, `dimensional_load`, `script_heavy`, `custom`.
- **Simplifications** vs. SSIS-faithful conversion. Auto-detected patterns include:

  | Detector | When it fires | Action |
  |---|---|---|
  | Atomic-write cleanup | FileSystemTask CopyFile/MoveFile/Rename around a cloud-targeted Data Flow | `drop` |
  | Trivial Data Flow fold | 1 source + 1 sink + only DerivedColumn / DataConversion | `fold_to_copy_activity` |
  | Lookup-only Data Flow fold | 1 source + 1 sink + only Lookup transforms | `fold_to_copy_activity` |
  | Stage-then-merge fold | TRUNCATE/DELETE + INSERT/MERGE/UPDATE on same connection | `fold_to_stored_proc` |
  | Audit-only ExecuteSQL drop | INSERT/UPDATE into `*log*` / `*audit*` tables | `drop` |
  | Send Mail replacement | SSIS Send Mail Task | `replace_with_function` (Logic App / Function) |

- **Linked services** — recommended target shape with **Managed Identity** by default.
- **Infrastructure** to provision (ADF, Storage Account, Key Vault) and **RBAC** assignments.
- **Risks** with severity and mitigation.
- **Effort estimate** in hours, bucketed (low / medium / high / very_high).

### `Project.params`-aware Key Vault recommendations

When a sibling `Project.params` file exists next to a `.dtsx`, its parameters are auto-loaded onto the package. The proposer scans for **Sensitive** parameters whose names look like credentials (`password`, `secret`, `token`, `apikey`, `connectionstring`, `clientsecret`, etc.) and:

- Emits an `AzureKeyVaultSecret` linked service per credential (e.g. `LS_KV_DbPassword` with `secret_name=ssis-dbpassword`).
- Forces a `Microsoft.KeyVault/vaults` entry into `infrastructure_needed`.
- Adds a `Key Vault Secrets User` RBAC assignment for `<ADF MI>` on each KV linked service.

No flag required — the proposer just consumes whatever `Project.params` provides.

### Editing the plan with `edit_migration_plan`

Rather than hand-editing the JSON, apply structured mutations:

```jsonc
{
  "set_auth_mode": "ManagedIdentity",
  "set_region": "eastus2",
  "set_target_pattern": "scheduled_file_drop",
  "set_summary": "Approved simplification",
  "add_simplification": {
    "action": "drop",
    "items": ["Send Audit Email"],
    "reason": "Replaced by Logic App"
  },
  "drop_simplification": "fold_to_stored_proc",
  "set_customer_decision": { "region": "eastus2", "approver": "jane@contoso.com" }
}
```

Unknown keys are rejected so typos surface immediately.

### Wave planning & costs (design-first)

These tools **require saved MigrationPlans** — call `propose_adf_design` and `save_migration_plan` first. Estimating effort, cost, and delivery sequence before the architectural blueprints are agreed is like creating a project plan before the design is made.

- `plan_migration_waves` reads saved plans (from a `plans_dir`) and produces ordered waves: bulk-convertible first (grouped by `target_pattern` so reviewers share context), then design-review waves capped at `max_packages_per_wave`.
- `estimate_adf_costs` introspects each plan's `reasoning_input.task_counts` and `simplifications` to derive per-pipeline Copy vs Data Flow vs orchestration activity counts, then projects monthly USD across orchestration (activity runs), Copy DIU·hours, Mapping Data Flow v-cores, ADLS storage, and Key Vault ops. List-price US East defaults; override via the `rates` parameter.
- `build_estate_report` rolls plans + waves + costs into a PDF deliverable for stakeholders. If no pre-computed wave or cost JSON is supplied, it derives both automatically from the plans.

### Cross-package shared infrastructure detection

`bulk_analyze` groups packages by their source directory and surfaces shared-infrastructure recommendations in `estate_summary.shared_infra_recommendations`:

- **Single Key Vault per project** when 2+ packages share a `Project.params` file (instead of one KV per package).
- **Single Self-Hosted Integration Runtime per project** when 2+ packages connect to the same on-prem SQL server.

Each package row also gains `project_dir`, `has_project_params`, and `sensitive_project_params`. A top-level `projects` array provides per-project rollups (package count, shared sensitive params, shared on-prem SQL servers).

### Provisioning with Bicep

`provision_adf_environment` consumes a saved `MigrationPlan` and:

1. Generates a Bicep template covering Data Factory (system-assigned MI), Storage Account (HNS-enabled ADLS Gen2), Key Vault, and the RBAC assignments the plan declared.
2. Compiles the Bicep via `az bicep build` (requires Azure CLI on PATH).
3. Deploys via `azure-mgmt-resource` against an existing resource group.

Supports `dry_run=true` to validate without applying.

### Smoke testing

After `deploy_to_adf` succeeds, `smoke_test_pipeline` triggers one pipeline run, polls until terminal status, and returns per-activity results (status, duration, error) so the agent can immediately see what worked and what didn't.

---

## Usage — End-to-End Walkthrough

All 22 tools are invoked from **GitHub Copilot Chat in Agent mode**. Type your request in natural language and Copilot will call the appropriate tool(s). The sections below cover the per-package backbone; see [Migration Copilot Workflow](#migration-copilot-workflow-recommended) above for the estate-scale tools.

---

### 1. Scan for packages

**Tool:** `scan_ssis_packages`

Discovers all `.dtsx` files from a local directory, a Git repository, or SQL Server (`msdb`).

**Example prompts:**

```
Scan C:\Projects\LegacyETL for all SSIS packages.
```

```
Scan the git repo at https://github.com/myorg/etl-packages for SSIS packages on the release branch.
```

```
List all SSIS packages stored in SQL Server at SERVER=MYSERVER;DATABASE=msdb.
```

**Key parameters:**

| Parameter | Required | Description |
|---|---|---|
| `source_type` | Yes | `local`, `git`, or `sql` |
| `path_or_connection` | Yes | Local directory path, Git repo URL, or SQL connection string |
| `recursive` | No | Search subdirectories (default: `true`) |
| `git_branch` | No | Branch to check out when `source_type` is `git` (default: `main`) |

---

### 2. Analyze a package

**Tool:** `analyze_ssis_package`

Produces a complexity score, gap analysis, component inventory, cross-database/linked server detection, CDM pattern flags, and optional ESI reuse candidates for a single package. Run this before converting to understand the scope of manual work required.

**Example prompts:**

```
Analyze the SSIS package at C:\Projects\LegacyETL\LoadFactSales.dtsx and tell me how complex it is.
```

```
Analyze C:\Projects\LegacyETL\LoadFactSales.dtsx with ESI tables config at C:\config\esi_tables.json.
```

**Key parameters:**

| Parameter | Required | Description |
|---|---|---|
| `package_path` | Yes | Absolute path to the `.dtsx` file |
| `esi_tables_path` | No | Path to a JSON file mapping source systems to ESI-available tables (see [ESI Reuse Detection](#esi-reuse-detection)) |

**Complexity score guide:**

| Score | Label | Typical Effort |
|---|---|---|
| 0–30 | Low | < 1 day |
| 31–55 | Medium | 1–3 days |
| 56–80 | High | 3–5 days |
| 81–100 | Very High | 1–3 weeks |

Score drivers (raw points are soft-capped to 0–100 via a logarithmic curve):

| Driver | Weight | Notes |
|---|---|---|
| Script Task (trivial) | +2 | Variable assignment only — auto-converted to SetVariable |
| Script Task (simple) | +6 | String/path manipulation, ADF-expressible |
| Script Task (moderate) | +13 | File I/O, regex, HTTP, XML — needs Azure Function |
| Script Task (complex) | +25 | DB connections, COM interop, threading |
| Data Flow Task | +5 | Base weight per task |
| Data Flow component | +1.5 | Per source/transform/destination inside a Data Flow |
| ForEach / For Loop | +5 | Per loop container |
| Event Handler | +4 | Per OnError / OnPostExecute handler |
| Nesting depth | +3 | Per level beyond depth 1 |
| Unknown task type | +10 | Unrecognised task — requires manual review |
| Linked server reference | +8 | OPENQUERY, OPENROWSET, four-part names |
| Cross-database reference | +3 | Three-part names (different database, same server) |
| Execute SQL | +2 | Execute Package (+3), File System (+2), FTP (+3), Send Mail (+4), Execute Process (+4), Sequence (+1) |

> **Supported task types:** Execute SQL, Data Flow, Execute Package, Script Task, ForEach Loop, For Loop, Sequence Container, File System, FTP, Send Mail, Execute Process, Bulk Insert, Web Service, XML Task, Transfer SQL Server Objects. Additional task types (Data Profiling, Transfer Database, Transfer Logins, etc.) are mapped to placeholder Wait activities with manual-review guidance.

**Key output:**
- Complexity score and effort label
- Component inventory (task types, connection managers, parameters, variables)
- Gap analysis grouped by severity: `manual_required` / `warning` / `info`
- Recommended execution order of tasks

---

### 3. Convert a package

**Tool:** `convert_ssis_package`

Converts a single `.dtsx` file to a complete set of ADF JSON artifacts.

**Example prompt:**

```
Convert C:\Projects\LegacyETL\LoadFactSales.dtsx to ADF artifacts and write them to C:\adf_output\LoadFactSales.
```

**Key parameters:**

| Parameter | Required | Description |
|---|---|---|
| `package_path` | Yes | Absolute path to the `.dtsx` file |
| `output_dir` | Yes | Directory to write artifacts into |
| `generate_trigger` | No | Emit a `ScheduleTrigger` template (default: `true`) |
| `llm_translate` | No | Call Azure OpenAI to translate C# Script Tasks to Python. Default: `false` |
| `on_prem_ir_name` | No | Integration Runtime name for on-prem connections (default: `SelfHostedIR`) |
| `auth_type` | No | Default auth for Azure SQL linked services: `SystemAssignedManagedIdentity` (default), `SQL`, or `ServicePrincipal` |
| `use_key_vault` | No | Use Azure Key Vault secret references for passwords (default: `false`) |
| `kv_ls_name` | No | Name for the Key Vault linked service (default: `LS_KeyVault`) |
| `kv_url` | No | Azure Key Vault base URL (default: `https://TODO.vault.azure.net/`) |
| `esi_tables_path` | No | Path to ESI tables config JSON for reuse detection |
| `schema_remap_path` | No | Path to schema remap JSON for database consolidation |
| `shared_artifacts_dir` | No | Shared directory for cross-package linked service/dataset deduplication |
| `pipeline_prefix` | No | Prefix for pipeline names (default: `PL_`) |
| `file_path_map_path` | No | Path to a JSON file mapping local/UNC path prefixes to Azure Storage URLs for automatic path rewriting |

Sub-folders are created automatically inside `output_dir`. See [Generated Artifact Structure](#generated-artifact-structure).

---

### 4. Validate generated artifacts

**Tool:** `validate_adf_artifacts`

Checks the generated JSON files for structural correctness (required fields, valid activity references) before touching Azure. Always validate before deploying.

> **Note:** `convert_ssis_package` now runs validation automatically after generation. The response summary includes `"validation"` (status + issues) and `"unresolved_pipeline_refs"` (ExecutePipeline cross-reference check). Run `validate_adf_artifacts` again after any manual edits.

**Example prompt:**

```
Validate the ADF artifacts in C:\adf_output\LoadFactSales.
```

**Key parameter:**

| Parameter | Required | Description |
|---|---|---|
| `artifacts_dir` | Yes | Directory containing the generated ADF JSON files |

Fix any reported issues in the JSON files, then validate again before proceeding to deployment.

---

### 5. Deploy to Azure Data Factory

**Tool:** `deploy_to_adf`

Deploys the validated artifacts to an existing Azure Data Factory instance. Deployment order is enforced automatically: linked services → datasets → data flows → pipelines → triggers.

> **Important:** Always run a dry run first to confirm what will be deployed without making any Azure API calls.

**Example prompt (dry run):**

```
Do a dry run deployment of C:\adf_output\LoadFactSales to my ADF instance named my-adf in resource group rg-data-prod, subscription 00000000-0000-0000-0000-000000000000.
```

**Example prompt (live deployment):**

```
Deploy C:\adf_output\LoadFactSales to ADF instance my-adf in resource group rg-data-prod, subscription 00000000-0000-0000-0000-000000000000.
```

**Key parameters:**

| Parameter | Required | Description |
|---|---|---|
| `artifacts_dir` | Yes | Directory containing generated ADF JSON artifacts |
| `subscription_id` | Yes | Azure subscription GUID |
| `resource_group` | Yes | Azure resource group name |
| `factory_name` | Yes | Azure Data Factory instance name |
| `dry_run` | No | `true` to log only without calling Azure APIs (default: `false`) |

> **Triggers are always deployed in Stopped state.** Activate them manually in the ADF Studio after validating pipeline runs.

---

### 6. Deploy Azure Function stubs

**Tool:** `deploy_function_stubs`

Zip-deploys the generated Azure Function stubs to an existing Azure Function App. The `stubs/` directory must have been generated by a prior `convert_ssis_package` call (it includes `host.json`, `requirements.txt`, and per-function directories).

> **Prerequisite:** Create the Function App first (Python runtime, Consumption or Premium plan), or use the `provision_function_app` tool to create one. The deploy tool uploads code only — it does not provision infrastructure.

**Example prompt (dry run):**

```
Do a dry run deploy of the function stubs in C:\adf_output\LoadFactSales\stubs to my Function App func-stubs-prod in resource group rg-data-prod, subscription 00000000-0000-0000-0000-000000000000.
```

**Example prompt (live deployment):**

```
Deploy the function stubs in C:\adf_output\LoadFactSales\stubs to Function App func-stubs-prod in resource group rg-data-prod, subscription 00000000-0000-0000-0000-000000000000.
```

**Key parameters:**

| Parameter | Required | Description |
|---|---|---|
| `stubs_dir` | Yes | Path to the stubs directory (typically `<output_dir>/stubs`) |
| `subscription_id` | Yes | Azure subscription GUID |
| `resource_group` | Yes | Azure resource group name |
| `function_app_name` | Yes | Name of the existing Azure Function App |
| `dry_run` | No | `true` to build the zip and validate without uploading (default: `false`) |

> **Note:** The tool uses `DefaultAzureCredential`. Run `az login` before deploying from a developer machine, or set `AZURE_CLIENT_ID` / `AZURE_CLIENT_SECRET` / `AZURE_TENANT_ID` for CI/CD.

---

### 7. Provision Azure Function App

**Tool:** `provision_function_app`

Creates the Azure infrastructure needed to host Function stubs: Storage Account, Application Insights (optional), Consumption App Service Plan, and a Python Linux Function App. Run this before `deploy_function_stubs` if no Function App exists yet.

**Example prompt (dry run):**

```
Do a dry run provision of a Function App called func-stubs-prod in resource group rg-data-prod, location eastus2, subscription 00000000-0000-0000-0000-000000000000.
```

**Example prompt (live provisioning):**

```
Provision a Function App called func-stubs-prod in resource group rg-data-prod, location eastus2, subscription 00000000-0000-0000-0000-000000000000.
```

**Key parameters:**

| Parameter | Required | Description |
|---|---|---|
| `function_app_name` | Yes | Globally unique name for the Function App |
| `subscription_id` | Yes | Azure subscription GUID |
| `resource_group` | Yes | Azure resource group (must already exist) |
| `location` | Yes | Azure region (e.g. `eastus2`, `westeurope`) |
| `storage_account_name` | No | Override auto-derived storage account name (3-24 lowercase alphanumeric) |
| `skip_app_insights` | No | Skip creating Application Insights (default: `false`) |
| `python_version` | No | Python runtime version (default: `3.11`) |
| `dry_run` | No | `true` to report what would be created without provisioning (default: `false`) |

**Resources created:**

| Resource | Configuration |
|---|---|
| Storage Account | Standard_LRS, StorageV2, HTTPS-only, TLS 1.2 |
| Application Insights | Web type (skippable via `skip_app_insights`) |
| App Service Plan | Consumption / Y1 / Dynamic, Linux |
| Function App | Python runtime, Linux, FTPS disabled, TLS 1.2, HTTP/2 enabled |

> **Note:** The tool uses `DefaultAzureCredential`. Run `az login` before provisioning from a developer machine, or set `AZURE_CLIENT_ID` / `AZURE_CLIENT_SECRET` / `AZURE_TENANT_ID` for CI/CD.

---

## Enterprise Features

These features were designed for large-scale enterprise SSIS migrations where packages share connections, target consolidated databases, or operate alongside existing data platforms (ESI, CDM layers).

### Self-Hosted Integration Runtime

On-prem connections are automatically detected (heuristics: `localhost`, IP addresses, non-`.database.windows.net` server names). These connections generate `SqlServer` linked services with a `connectVia` reference to a Self-Hosted Integration Runtime. Use `on_prem_ir_name` to override the default name `SelfHostedIR`.

#### Multi-IR Mapping

For environments with multiple Integration Runtimes (e.g. per-region or per-network-zone), pass an `ir_mapping` dictionary from glob patterns to IR names:

```json
{
  "*-east-*": "IR_EastUS",
  "*-west-*": "IR_WestUS",
  "legacy*": "IR_Legacy"
}
```

Connection names are matched against patterns (first match wins). Unmatched on-prem connections fall back to `on_prem_ir_name`.

### Path Traversal Protection

All MCP tool inputs that accept file/directory paths are validated against path traversal attacks. Null bytes and `..` segments are rejected. Optionally, set the `SSIS_ADF_ALLOWED_ROOT` environment variable to restrict all file operations to a specific directory tree.

### Deployer Retry with Jitter

The `deploy_to_adf` tool uses exponential backoff with jitter (±50%) and a 60-second cap for Azure API retries, preventing thundering herd problems during batch deployments across many packages.

### Azure Key Vault Secrets

When `use_key_vault=true`, linked services reference Azure Key Vault secrets instead of embedding credentials:

```json
{
  "password": {
    "type": "AzureKeyVaultSecret",
    "store": { "referenceName": "LS_KeyVault", "type": "LinkedServiceReference" },
    "secretName": "conn-MyDatabase-password"
  }
}
```

A Key Vault linked service (`LS_KeyVault`) is auto-generated. Customize the name with `kv_ls_name` and the vault URL with `kv_url`.

### Cross-Package Deduplication

When migrating multiple SSIS packages that share connection managers, pass `shared_artifacts_dir` to avoid duplicate linked services and datasets:

```
Convert LoadDimCustomer.dtsx with shared_artifacts_dir=C:\output\shared
Convert LoadFactSales.dtsx with shared_artifacts_dir=C:\output\shared
```

The generator writes each linked service / dataset only once. Subsequent packages that reference the same connection reuse the existing file.

### Schema Remapping (Database Consolidation)

When consolidating multiple on-prem databases into a single Azure SQL database, provide a schema remap config:

```json
{
  "StagingDB": "staging",
  "ReportingDB": "reporting",
  "DWDB": "dw"
}
```

Keys are original database names; values are target schemas. Pass the file path via `schema_remap_path`. The converter replaces cross-database references in:

- **Script activities** — SQL text in `scripts[].text`
- **Lookup activities** — `source.sqlReaderQuery`
- **Stored Procedure activities** — `storedProcedureName`
- **Dataset definitions** — qualified table names

### ESI Reuse Detection

If your organization maintains an ESI (Enterprise Source Integration) layer, you can provide a JSON config mapping source systems to tables already available in the ESI Azure SQL layer:

```json
{
  "source_system": "SAP",
  "esi_database": "ESI_SAP",
  "tables": ["VBAK", "VBAP", "MARA", "KNA1"]
}
```

Pass this file via `esi_tables_path` (available on both `analyze` and `convert` tools). The analyzer produces INFO-level gap items identifying data flow sources that could read from ESI instead of pulling from the on-prem source via SHIR.

### CDM Pattern Flagging

The analyzer automatically detects Common Data Model (CDM) layer patterns:

- **Multi-source joins** \u2014 data flows with 3+ sources feeding a Merge Join or Union All
- **Aggregation** \u2014 data flows with grouped aggregation transformations
- **Cross-system enrichment** \u2014 joins between sources from different connection managers
- **Denormalization** \u2014 3+ lookup transformations in a single data flow

Detected patterns produce INFO-level gap items with `[CDM REVIEW]` recommendations and `cdm-review-required` pipeline annotations. These are informational \u2014 they help teams decide whether to migrate the logic as-is or replace it with existing CDM entities.

### SQL Agent Schedule Mapping

When the SSIS package source is a SQL Server (`sql_server` source type in `scan_ssis_packages`), the tool reads SQL Agent job schedules from `msdb`. The converted trigger uses the mapped ADF recurrence:

| SQL Agent `freq_type` | ADF Recurrence |
|---|---|
| 4 (Daily) | `Day` with `interval` from `freq_interval` |
| 8 (Weekly) | `Week` with weekday schedule from bitmask |
| 16 (Monthly, day-of-month) | `Month` with day schedule |
| 32 (Monthly, relative) | `Month` \u2014 flag for manual review |

If no SQL Agent schedule is available, the trigger falls back to a placeholder daily-at-midnight schedule.

---

## LLM-Powered Script Task Translation

SSIS Script Tasks contain C# (or VB.NET) code that cannot be rule-based converted. By default the converter generates a Python Azure Function stub with `TODO` comments and the original source embedded as comments. When `llm_translate=true` is passed to `convert_ssis_package`, the agent extracts the embedded C# source from the DTSX binary blob and calls **Azure OpenAI** to produce a working Python implementation body.

### How it works

1. **Extraction** — The parser decodes the base64-encoded ZIP blob inside `DTS:ObjectData/ScriptProject/BinaryData`, unzips it, and reads all `.cs` / `.vb` source files (excluding `AssemblyInfo` and designer files).
2. **Translation** — `CSharpToPythonTranslator` sends the source to Azure OpenAI Chat Completions with a structured prompt that preserves business logic and replaces unsupported patterns (SQL calls, file I/O, SMTP) with `# TODO` comments pointing to Azure equivalents.
3. **Stub output** — The generated `stubs/<FunctionName>/__init__.py` contains the translated Python body. The original C# is preserved as line comments below the implementation for reference.
4. **Graceful fallback** — If the API key is not configured, the model deployment is unavailable, or the DTSX uses a self-closing stub format (no embedded source), the converter falls back to the standard `TODO` stub without raising an error. A warning comment is embedded in the stub file.

### Required environment variables

| Variable | Description | Default |
|---|---|---|
| `AZURE_OPENAI_ENDPOINT` | Your Azure OpenAI resource URL, e.g. `https://my-resource.openai.azure.com/` | required |
| `AZURE_OPENAI_API_KEY` | Azure OpenAI API key | required |
| `AZURE_OPENAI_DEPLOYMENT` | Model deployment name | `gpt-4o` |

### Installation

The `openai` SDK is an optional dependency — install it alongside the package:

```bash
pip install "ssis-adf-agent[llm]"
```

### Example prompt

```
Convert C:\Projects\LegacyETL\LoadFactSales.dtsx to C:\adf_output\LoadFactSales and translate all Script Tasks to Python using Azure OpenAI.
```

> **Note:** Translated code should always be reviewed before deploying to production. The LLM preserves control flow and business logic but replaces infrastructure calls (SQL, file I/O, SMTP) with `# TODO` placeholders that require manual completion.

---

## Using the Built-in Prompt Files

Three reusable prompt files are included in `.vscode/` and can be invoked directly from Copilot Chat to run the full workflow with guided inputs.

| Prompt File | Mode | Description |
|---|---|---|
| `analyze_packages.prompt.md` | Agent | Scan a source, then analyze every package found and produce a prioritized conversion report |
| `convert_package.prompt.md` | Agent | Analyze, convert, and validate a single package; produces a Markdown summary with a manual-steps checklist |
| `deploy_adf.prompt.md` | Agent | Validate artifacts and deploy to ADF with optional dry-run |

**To invoke from Copilot Chat:**

1. Open Copilot Chat (`Ctrl+Alt+I`)
2. Switch to **Agent** mode
3. Type `/` and select the prompt file from the list, or type the prompt name
4. Fill in the prompted inputs (package path, output directory, Azure details, etc.)

---

## Authentication

The `deploy_to_adf` tool uses `DefaultAzureCredential`, which tries the following in order:

| Priority | Method | When to use |
|---|---|---|
| 1 | Environment variables | CI/CD pipelines (service principal) |
| 2 | Workload Identity | Azure-hosted compute (AKS, etc.) |
| 3 | Azure CLI (`az login`) | Local developer machines |
| 4 | Azure PowerShell | Local developer machines |

**For local development,** the simplest approach is:

```bash
az login
```

**For CI/CD pipelines,** set these environment variables for a service principal:

| Variable | Description |
|---|---|
| `AZURE_CLIENT_ID` | Service principal application (client) ID |
| `AZURE_CLIENT_SECRET` | Service principal secret |
| `AZURE_TENANT_ID` | Azure Active Directory tenant ID |

The service principal must have the **Data Factory Contributor** role on the target ADF instance.

### Azure OpenAI (for LLM Script Task translation)

Set the following environment variables before calling `convert_ssis_package` with `llm_translate=true`:

```powershell
# Windows (PowerShell)
$env:AZURE_OPENAI_ENDPOINT   = "https://my-resource.openai.azure.com/"
$env:AZURE_OPENAI_API_KEY    = "<your-key>"
$env:AZURE_OPENAI_DEPLOYMENT = "gpt-4o"   # optional, defaults to gpt-4o
```

```bash
# macOS / Linux
export AZURE_OPENAI_ENDPOINT="https://my-resource.openai.azure.com/"
export AZURE_OPENAI_API_KEY="<your-key>"
export AZURE_OPENAI_DEPLOYMENT="gpt-4o"
```

---

## SSIS Component Mapping Reference

| SSIS Component | ADF Equivalent | Notes |
|---|---|---|
| Execute SQL Task | Stored Procedure / Script / Lookup Activity | Depends on `ResultSetType` and SQL pattern |
| Data Flow Task (simple) | Copy Activity | Single source → single destination. Sink pattern varies: `insert` (full load), `upsert` with keys (delta/merge). Retry policy: 2 retries, 60s interval. |
| Data Flow Task (complex) | Execute Data Flow Activity (Mapping Data Flow) | Multiple sources, transformations, or fanout. `READ_UNCOMMITTED` isolation, `errorHandlingOption: stopOnFirstError`. |
| Execute Package Task | Execute Pipeline Activity | Child pipeline must also be converted. The referenced pipeline name uses the `pipeline_prefix` (default `PL_`). Supports project references and external package references, including parameter pass-through. |
| Script Task (C# / VB) | Azure Function Activity | Stub generated at `stubs/<FunctionName>/__init__.py`. When `llm_translate=true`, C# source is extracted from the DTSX and translated to Python via Azure OpenAI; otherwise a `TODO` stub is generated. |
| ForEach Loop Container | ForEach Activity | Expression varies by enumerator type |
| For Loop Container | SetVariable (init) + Until Activity + SetVariable (increment) | Condition logic is inverted |
| Sequence Container | Flattened into parent with `dependsOn` chaining | No ADF equivalent. Activity names are auto-deduplicated (suffixed `_2`, `_3`, …) to prevent ADF validation errors when multiple containers have identically-named tasks. |
| File System Task | Copy Activity (Azure paths) or Web Activity → Azure Function | Local paths need Azure-path substitution |
| Execute Process Task | Web Activity → Azure Function | Manual: wrap process call in a Function |
| FTP Task | Copy Activity via FTP connector | Requires FTP linked service |
| Send Mail Task | Logic App / Web Activity | No native ADF equivalent |
| Bulk Insert Task | Copy Activity | DelimitedText source → AzureSqlSink. Generates linked service for the source file and SQL connection. |
| Web Service Task | Web Activity | Configurable URL and HTTP method from the SSIS task properties. |
| XML Task | Azure Function Activity | Operation type (XPATH, Merge, Validate, Diff, XSLT) extracted from the package. Azure Function stub with operation-specific boilerplate generated at `stubs/<FuncName>/`. |
| Transfer SQL Server Objects Task | Script Activity | Migration guidance in description. Recommends Copy Activity pipeline or Azure Database Migration Service. |
| Execute Process Task | Web Activity → Azure Function | Manual: wrap process call in a Function |
| Event Handler (`OnError`) | Pipeline fails path / error handling | Converted to sub-pipeline reference |
| Event Handler (`OnWarning`) | Completed dependency path | Converted to sub-pipeline reference |
| Event Handler (`OnPostExecute`) | Succeeded dependency path | Converted to sub-pipeline reference |
| Event Handler (`OnInformation`, `OnProgress`) | Succeeded dependency path | Converted to sub-pipeline reference |
| Connection Manager (Azure SQL) | Linked Service (`AzureSqlDatabase`) | Microsoft Recommended version: `server`/`database`/`authenticationType`. Default auth: `SystemAssignedManagedIdentity`. |
| Connection Manager (on-prem SQL) | Linked Service (`SqlServer`) | Auto-detected. Uses Self-Hosted IR with `pooling: false`. |
| Connection Manager (FILE / MULTIFILE) | Linked Service (`FileServer` or `AzureBlobStorage`) | UNC/drive paths → `FileServer` with Self-Hosted IR; other paths → `AzureBlobStorage` with TODO connection string. |
| Connection Manager (FTP) | Linked Service (`FtpServer`) | Basic auth with SSL enabled. Password stored in Key Vault when `use_key_vault=true`. |
| Connection Manager (HTTP) | Linked Service (`HttpServer`) | Anonymous auth by default. |
| Connection Manager (SMTP) | Linked Service (`AzureFunction`) | Stub for Azure Communication Services or Logic App. No native ADF SMTP support. |
| SQL Agent Job Schedule | Schedule Trigger | Mapped from `msdb` `freq_type`/`freq_interval`. Falls back to placeholder if unavailable. |

---

## Generated Artifact Structure

`convert_ssis_package` writes the following directory structure under `output_dir`:

```
<output_dir>/
  pipeline/
    PL_<PackageName>.json       ← Main ADF pipeline (prefix configurable)
  linkedService/
    LS_<ConnectionName>.json    ← Microsoft Recommended version format
    LS_KeyVault.json            ← Auto-generated when use_key_vault=true
  dataset/
    DS_<DatasetName>.json       ← Uses schema+table (not deprecated tableName)
  dataflow/
    DF_<DataFlowName>.json      ← Mapping Data Flow with READ_UNCOMMITTED + error handling
  trigger/
    TR_<PackageName>.json       ← ScheduleTrigger (Stopped state); accurate if SQL Agent schedule provided
  stubs/                        ← Deploy-ready Azure Functions project
    host.json                   ← Functions runtime config (v2, 10-min timeout)
    requirements.txt            ← Auto-detected Python dependencies
    local.settings.json         ← Local dev settings (not deployed)
    .funcignore                 ← Deployment exclusion list
    <FunctionName>/
      __init__.py               ← Python stub with TODO blocks
      function.json             ← HTTP trigger binding definition
```

> **Deploy-ready stubs:** The `stubs/` directory is a complete Azure Functions project. Deploy with `func azure functionapp publish <APP_NAME>` or zip-deploy via CI/CD. Run `func start` locally to test before deploying.

### Linked Service Format

Linked services use the **Microsoft Recommended version** format with discrete properties instead of the legacy `connectionString` format:

```json
{
  "type": "AzureSqlDatabase",
  "typeProperties": {
    "server": "myserver.database.windows.net",
    "database": "mydb",
    "encrypt": "mandatory",
    "trustServerCertificate": false,
    "authenticationType": "SystemAssignedManagedIdentity"
  }
}
```

For on-prem connections, the `SqlServer` connector type with Self-Hosted IR is used automatically:

```json
{
  "type": "SqlServer",
  "typeProperties": {
    "server": "on-prem-server",
    "database": "mydb",
    "authenticationType": "Windows",
    "pooling": false
  },
  "connectVia": { "referenceName": "SelfHostedIR", "type": "IntegrationRuntimeReference" }
}
```

### Dataset Format

Datasets use separate `schema` and `table` properties per Microsoft's recommendation:

```json
{
  "type": "AzureSqlTable",
  "typeProperties": {
    "schema": "dbo",
    "table": "MyTable"
  }
}
```

### Pipeline Annotations

Generated pipelines include automatic annotations based on detected patterns:

- `ssis-adf-agent` — identifies the source tool
- `source-package:<name>` — original SSIS package name
- `ingestion-pattern:delta` or `ingestion-pattern:merge` — when delta/merge patterns detected
- `has-cross-db-references` — when cross-database or linked server references found
- `cdm-review-required` — when CDM-layer patterns detected
- `esi-reuse-candidate` — when ESI reuse opportunities found

---

## Manual Steps After Conversion

After running `convert_ssis_package`, review the following checklist before deploying:

- [ ] **Connection string passwords** — packages with `EncryptAllWithPassword` protection level may have missing passwords. When `use_key_vault=true`, linked services reference Key Vault secrets — verify the secret names exist and are populated. Otherwise fill in plaintext credentials.
- [ ] **Script Task stubs** — each stub in `stubs/<FunctionName>/` contains `TODO` comments and an HTTP trigger binding (`function.json`). If `llm_translate=true` was used, the stub contains LLM-translated Python. The `stubs/` directory is a complete Azure Functions project — deploy with `func azure functionapp publish <APP_NAME>` after implementing the TODO blocks.
- [x] **XML Task stubs** *(automated)* — XML operations (XPATH, Merge, Validate, Diff, XSLT) now generate Azure Function stubs with operation-specific boilerplate (lxml examples for XPath, Merge, Validate, XSLT, Diff). Each stub includes `__init__.py` and `function.json`. Review and complete the `TODO` blocks, then deploy to Azure Functions.
- [ ] **Bulk Insert / Web Service / Transfer SQL activities** — these are converted to Copy Activity, Web Activity, or Script Activity respectively with TODO guidance. Review the generated descriptions for migration advice.
- [x] **Local file paths** *(automated with `file_path_map_path`)* — pass a JSON file mapping local/UNC prefixes to Azure Storage URLs (e.g. `{"C:\\Data\\Input": "https://blob/input"}`). The converter applies longest-prefix-match substitution across linked services, pipeline activities, and datasets. Remaining unmapped paths still need manual attention.
- [ ] **Trigger schedules** — if no SQL Agent schedule was available, the trigger uses a placeholder daily-at-midnight schedule. Update it to match your production schedule. When SQL Agent metadata was provided, verify the mapped ADF recurrence matches the original.
- [ ] **Cross-database / linked server references** — check the gap analysis for `manual_required` severity items. Replace linked server four-part names with Azure SQL elastic queries, external tables, or separate linked services. Remap three-part names if consolidating databases.
- [ ] **CDM review items** — if the pipeline has a `cdm-review-required` annotation, coordinate with the CDM team to decide whether the transformation logic should migrate as-is or be replaced by existing CDM-layer entities.
- [ ] **ESI reuse candidates** — if the pipeline has an `esi-reuse-candidate` annotation, review whether reading from the ESI Azure SQL layer is preferable to re-staging from the on-prem source via SHIR.
- [ ] **Upsert key columns** — Copy Activities with `writeBehavior: "upsert"` include detected key columns. Verify these match the target table's unique key. Replace `TODO_KEY_COLUMN` placeholders where keys could not be auto-detected.
- [x] **Auto-validate** *(automated)* — `convert_ssis_package` now automatically runs `validate_adf_artifacts` after generation and includes results in the response summary under `"validation"`. Re-run manually after any manual edits.
- [ ] **Duplicate activity names** — when multiple Sequence Containers contain tasks with the same name, the generator auto-deduplicates by appending `_2`, `_3`, etc. Review renamed activities and their `dependsOn` references.
- [ ] **FILE connection linked services** — FILE/MULTIFILE connection managers produce `FileServer` linked services for UNC/drive paths. Verify the host path and credentials, or migrate files to Azure Blob Storage.
- [x] **Execute Pipeline references** *(automated)* — the converter now cross-checks all `ExecutePipeline` activity references against pipeline JSON files in the output directory (and shared artifacts directory). Unresolved references are reported in the response summary under `"unresolved_pipeline_refs"`.
- [ ] **Activate triggers** — triggers are deployed in **Stopped** state. Activate them in ADF Studio only after a successful pipeline smoke-test.

---

## Development

Install development dependencies:

```bash
pip install -e ".[dev]"
```

**Run tests:**

```bash
pytest
```

**Lint:**

```bash
ruff check .
```

**Type-check:**

```bash
mypy ssis_adf_agent/
```

The project targets Python 3.11+, uses `ruff` with `line-length = 100`, and enforces `mypy --strict`.

---

## License

This project is licensed under the [MIT License](LICENSE).

```
MIT License

Copyright (c) 2026 chsimons_microsoft

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```
