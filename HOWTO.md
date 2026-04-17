# HOWTO — SSIS → ADF Agent Conversation Guide

This guide shows how to use the SSIS → ADF Agent through natural-language prompts in **GitHub Copilot Chat**. Each section includes example conversations you can copy and adapt.

> **Tip:** Switch Copilot Chat to **Agent** mode before sending prompts so it can call the MCP tools automatically.
>
> For installation, architecture, and reference details see [README.md](README.md).

---

## Table of Contents

- [End-to-End Migration](#end-to-end-migration)
  - [Quick Start: One Package](#quick-start-one-package)
  - [Full Migration: Multiple Packages](#full-migration-multiple-packages)
- [Per-Feature Guides](#per-feature-guides)
  - [Scan for Packages](#scan-for-packages)
  - [Analyze a Package](#analyze-a-package)
  - [Convert a Package](#convert-a-package)
  - [Validate Artifacts](#validate-artifacts)
  - [Deploy to ADF](#deploy-to-adf)
  - [Consolidate Similar Packages](#consolidate-similar-packages)
  - [Provision a Function App](#provision-a-function-app)
  - [Deploy Function Stubs](#deploy-function-stubs)
- [Enterprise Scenarios](#enterprise-scenarios)
  - [Key Vault Secrets](#key-vault-secrets)
  - [Schema Remapping (Database Consolidation)](#schema-remapping-database-consolidation)
  - [File Path Mapping](#file-path-mapping)
  - [Self-Hosted Integration Runtime](#self-hosted-integration-runtime)
  - [ESI Reuse Detection](#esi-reuse-detection)
  - [LLM Script Task Translation](#llm-script-task-translation)
  - [Cross-Package Deduplication](#cross-package-deduplication)

---

## End-to-End Migration

### Quick Start: One Package

This conversation takes a single SSIS package from analysis through deployment.

> **You:** I have an SSIS package at `C:\Projects\LegacyETL\LoadFactSales.dtsx`. Can you analyze it and tell me how complex it is?

> **Copilot:** *(calls `analyze_ssis_package`)* The package scores **47 / Medium** with an estimated effort of 1–3 days. It contains 3 Execute SQL tasks, 1 Data Flow with 5 components, and 1 ForEach Loop. There are 2 gap items: a Script Task classified as "moderate" (needs Azure Function stub) and a cross-database reference to `StagingDB.dbo.DimCustomer`.

> **You:** Go ahead and convert it. Write the output to `C:\adf_output\LoadFactSales`.

> **Copilot:** *(calls `convert_ssis_package`, then auto-validates)* Conversion complete. Generated:
> - 1 pipeline: `PL_LoadFactSales.json`
> - 3 linked services
> - 2 datasets
> - 1 trigger (daily at midnight — placeholder)
> - 1 Azure Function stub for `SCR_ProcessRecords`
>
> Auto-validation passed with 0 errors. The function stub at `stubs/SCR_ProcessRecords/__init__.py` has TODO blocks that need to be implemented.

> **You:** Deploy this to our ADF instance. Subscription: `aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee`, resource group: `rg-data-prod`, factory: `adf-prod-etl`. Do a dry run first.

> **Copilot:** *(calls `deploy_to_adf` with `dry_run=true`)* Dry run complete. Would deploy:
> - 3 linked services
> - 2 datasets
> - 1 pipeline
> - 1 trigger (Stopped state)
>
> No validation issues found. Ready to deploy for real when you are.

> **You:** Looks good. Deploy for real.

> **Copilot:** *(calls `deploy_to_adf`)* Deployment complete. All 7 artifacts deployed successfully. The trigger `TR_LoadFactSales` is in Stopped state — activate it manually in the ADF portal when you're ready.

---

### Full Migration: Multiple Packages

> **You:** I have a folder of SSIS packages at `C:\Projects\LegacyETL\`. Scan it and tell me what's there.

> **Copilot:** *(calls `scan_ssis_packages`)* Found 12 `.dtsx` packages:
> 1. LoadFactSales.dtsx
> 2. LoadFactOrders.dtsx
> 3. LoadDimCustomer.dtsx
> *(… 9 more …)*

> **You:** Analyze all of them and give me a prioritized report — which ones are easy wins and which will take the most work?

> **Copilot:** *(calls `analyze_ssis_package` for each)* Here's the migration assessment:
>
> | Package | Score | Effort | Key Risk |
> |---|---|---|---|
> | LoadDimDate | 8 | Low | None |
> | LoadDimRegion | 15 | Low | None |
> | LoadFactSales | 47 | Medium | 1 Script Task, 1 cross-DB ref |
> | LoadFactOrders | 52 | Medium | 2 event handlers |
> | MasterETL | 78 | High | 4 Script Tasks (2 complex) |
> | *(… more …)* | | | |
>
> **Recommended order:** Start with the 5 Low packages (< 1 day each), then tackle the Medium ones. Save MasterETL for last.

> **You:** Are any of those packages similar enough to consolidate?

> **Copilot:** *(calls `consolidate_packages`)* Yes! `LoadDimCustomer`, `LoadDimProduct`, and `LoadDimRegion` have identical structure — they each run one SQL query and load into a table. I can consolidate them into one parameterized pipeline with a ForEach that iterates over a config array.

> **You:** Do it. Output to `C:\adf_output\consolidated`.

> **Copilot:** *(calls `consolidate_packages` with output_dir)* Generated:
> - `PL_LoadDim_Child.json` — parameterized child pipeline
> - `PL_LoadDim_Parent.json` — parent with ForEach over 3 config entries
> - 1 shared linked service

---

## Per-Feature Guides

### Scan for Packages

Find all `.dtsx` files in a directory, Git repo, or SQL Server.

**Local directory:**
> Scan `C:\Projects\LegacyETL` for SSIS packages, including subdirectories.

**Git repository:**
> Scan the Git repo at `https://dev.azure.com/myorg/myproject/_git/ssis-packages` on the `main` branch for SSIS packages.

**SQL Server (msdb):**
> Scan the SQL Server at `SERVER=SQLPROD01;DATABASE=msdb;Trusted_Connection=yes` for deployed SSIS packages.

---

### Analyze a Package

Get a complexity score, gap analysis, component inventory, and recommended execution order.

**Basic analysis:**
> Analyze the SSIS package at `C:\Projects\LegacyETL\LoadFactSales.dtsx`.

**With ESI reuse detection:**
> Analyze `C:\Projects\LegacyETL\LoadFactSales.dtsx` with ESI tables config at `C:\config\esi_tables.json`.

**Interpreting the results:**

| Score | What it means |
|---|---|
| 0–30 (Low) | Simple package. Mostly Execute SQL / File System tasks. Converts cleanly with little to no manual work. |
| 31–55 (Medium) | Moderate complexity. Likely has data flows, loops, or a simple Script Task. Plan 1–3 days including testing. |
| 56–80 (High) | Complex package. Script Tasks, deep nesting, event handlers, or cross-database references. Plan 3–5 days. |
| 81–100 (Very High) | Highly complex. Multiple complex Script Tasks, unsupported components, or linked server references. Plan 1–3 weeks. |

The gap analysis groups issues by severity:

- **`manual_required`** — Cannot be auto-converted. You must implement this manually (e.g. complex Script Tasks, linked server rewrites).
- **`warning`** — Auto-converted but needs review (e.g. moderate Script Tasks, placeholder connection strings).
- **`info`** — Informational (e.g. trivial Script Tasks auto-mapped to SetVariable, ESI reuse candidates, CDM pattern flags).

---

### Convert a Package

Convert a `.dtsx` file to ADF JSON artifacts.

**Basic conversion:**
> Convert `C:\Projects\LegacyETL\LoadFactSales.dtsx` to `C:\adf_output\LoadFactSales`.

**With Key Vault and Managed Identity:**
> Convert `C:\Projects\LegacyETL\LoadFactSales.dtsx` to `C:\adf_output\LoadFactSales`. Use Key Vault for secrets with vault URL `https://kv-data-prod.vault.azure.net/` and use Managed Identity for all SQL connections.

**With file path mapping:**
> Convert `C:\Projects\LegacyETL\LoadFactSales.dtsx` to `C:\adf_output\LoadFactSales` with file path mapping from `C:\config\path_map.json`.

**With LLM translation for Script Tasks:**
> Convert `C:\Projects\LegacyETL\LoadFactSales.dtsx` to `C:\adf_output\LoadFactSales` and translate all Script Tasks to Python using Azure OpenAI.

**With schema remapping (database consolidation):**
> Convert `C:\Projects\LegacyETL\LoadFactSales.dtsx` to `C:\adf_output\LoadFactSales` with schema remap config at `C:\config\schema_remap.json`.

**With everything together:**
> Convert `C:\Projects\LegacyETL\LoadFactSales.dtsx` to `C:\adf_output\LoadFactSales`. Use Key Vault at `https://kv-data-prod.vault.azure.net/`, Managed Identity auth, file path mapping from `C:\config\path_map.json`, schema remapping from `C:\config\schema_remap.json`, and LLM translation for Script Tasks.

---

### Validate Artifacts

Check generated ADF JSON for structural correctness before deploying.

> Validate the ADF artifacts in `C:\adf_output\LoadFactSales`.

> **Note:** Validation runs automatically after conversion. Use this tool to re-validate after you've manually edited any artifact files.

---

### Deploy to ADF

Push generated artifacts to an Azure Data Factory instance.

**Dry run first (recommended):**
> Do a dry run deployment of `C:\adf_output\LoadFactSales` to ADF factory `adf-prod-etl` in resource group `rg-data-prod`, subscription `aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee`.

**Live deployment:**
> Deploy `C:\adf_output\LoadFactSales` to ADF factory `adf-prod-etl` in resource group `rg-data-prod`, subscription `aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee`.

**What gets deployed and in what order:**
1. Linked services (connections)
2. Datasets (table/file references)
3. Data flows (mapping data flows)
4. Pipelines (orchestration logic)
5. Triggers (schedules — deployed in **Stopped** state)

> **Important:** Triggers are always deployed in Stopped state. Activate them manually in the ADF portal after validating the pipeline.

**Authentication:** Run `az login` before deploying from your machine, or set `AZURE_CLIENT_ID` / `AZURE_CLIENT_SECRET` / `AZURE_TENANT_ID` for CI/CD.

---

### Consolidate Similar Packages

Detect structurally similar packages and merge them into one parameterized pipeline.

**Analysis only (no output files):**
> Analyze these packages for consolidation opportunities: `C:\Projects\LegacyETL\LoadDimCustomer.dtsx`, `C:\Projects\LegacyETL\LoadDimProduct.dtsx`, `C:\Projects\LegacyETL\LoadDimRegion.dtsx`.

**Full consolidation:**
> Consolidate these packages into parameterized pipelines and write output to `C:\adf_output\consolidated`: `C:\Projects\LegacyETL\LoadDimCustomer.dtsx`, `C:\Projects\LegacyETL\LoadDimProduct.dtsx`, `C:\Projects\LegacyETL\LoadDimRegion.dtsx`.

---

### Provision a Function App

Create Azure infrastructure to host the Function stubs generated during conversion.

**Dry run (see what would be created):**
> Do a dry run provision of a Function App called `func-stubs-prod` in resource group `rg-data-prod`, location `eastus2`, subscription `aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee`.

**Live provisioning:**
> Provision a Function App called `func-stubs-prod` in resource group `rg-data-prod`, location `eastus2`, subscription `aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee`.

**Without Application Insights:**
> Provision a Function App called `func-stubs-prod` in resource group `rg-data-prod`, location `eastus2`, subscription `aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee`. Skip Application Insights.

**What gets created:**
- Storage Account (for Azure Functions runtime)
- Application Insights (optional, for monitoring)
- App Service Plan (Consumption / serverless)
- Function App (Python, Linux)

---

### Deploy Function Stubs

Upload the generated Azure Function stubs to a Function App.

**Dry run first:**
> Do a dry run deploy of the function stubs in `C:\adf_output\LoadFactSales\stubs` to Function App `func-stubs-prod` in resource group `rg-data-prod`, subscription `aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee`.

**Live deployment:**
> Deploy the function stubs in `C:\adf_output\LoadFactSales\stubs` to Function App `func-stubs-prod` in resource group `rg-data-prod`, subscription `aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee`.

**Full workflow — provision then deploy:**

> **You:** Provision a Function App called `func-stubs-prod` in resource group `rg-data-prod`, location `eastus2`, subscription `aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee`.

> **Copilot:** *(creates Storage Account, App Insights, Plan, Function App)* Function App provisioned at `https://func-stubs-prod.azurewebsites.net`.

> **You:** Now deploy the function stubs from `C:\adf_output\LoadFactSales\stubs` to it.

> **Copilot:** *(zip deploys the stubs)* Deployed 2 functions: `SCR_ProcessRecords`, `SCR_ValidateInput`. Zip size: 4.2 KB.

---

## Enterprise Scenarios

### Key Vault Secrets

Store connection passwords in Azure Key Vault instead of embedding them in linked services.

> Convert `C:\Projects\LegacyETL\LoadFactSales.dtsx` to `C:\adf_output\LoadFactSales`. Use Key Vault for secrets with vault URL `https://kv-data-prod.vault.azure.net/`.

**What you get:** Linked services reference Key Vault secrets like this:
```json
{
  "password": {
    "type": "AzureKeyVaultSecret",
    "store": { "referenceName": "LS_KeyVault", "type": "LinkedServiceReference" },
    "secretName": "conn-MyDatabase-password"
  }
}
```

**After conversion:** Verify that secrets with the generated names exist in your Key Vault. The naming convention is `conn-<ConnectionName>-password`.

---

### Schema Remapping (Database Consolidation)

When consolidating multiple on-prem databases into one Azure SQL database with different schemas.

**Step 1 — Create a remap config file** (`schema_remap.json`):
```json
{
  "StagingDB": "staging",
  "ReportingDB": "reporting",
  "DWDB": "dw"
}
```

**Step 2 — Convert with remapping:**
> Convert `C:\Projects\LegacyETL\LoadFactSales.dtsx` to `C:\adf_output\LoadFactSales` with schema remap config at `C:\config\schema_remap.json`.

**What happens:** References like `StagingDB.dbo.MyTable` become `staging.MyTable` in all SQL statements, stored procedure names, and dataset definitions.

---

### File Path Mapping

Replace local/UNC file paths with Azure Storage URLs.

**Step 1 — Create a path map file** (`path_map.json`):
```json
{
  "C:\\Data\\Input": "https://mystorage.blob.core.windows.net/input",
  "C:\\Data\\Output": "https://mystorage.blob.core.windows.net/output",
  "\\\\fileserver\\share": "https://mystorage.blob.core.windows.net/legacy-share"
}
```

**Step 2 — Convert with path mapping:**
> Convert `C:\Projects\LegacyETL\FileExport.dtsx` to `C:\adf_output\FileExport` with file path mapping from `C:\config\path_map.json`.

**What happens:** Local paths are replaced with Azure Storage URLs in linked services, pipeline activities, and datasets using longest-prefix matching.

---

### Self-Hosted Integration Runtime

On-prem connections are auto-detected (localhost, IP addresses, non-`.database.windows.net` server names) and automatically configured with a Self-Hosted IR reference. No special prompt needed — just convert normally.

**To customize the IR name:**
> Convert `C:\Projects\LegacyETL\LoadFactSales.dtsx` to `C:\adf_output\LoadFactSales`. Use IR name `IR_OnPremGateway` for on-prem connections.

**For multi-IR environments,** create an IR mapping file and pass it as a parameter. See the [README](README.md#multi-ir-mapping) for details.

---

### ESI Reuse Detection

Identify data flow sources that could read from your existing Enterprise Source Integration (ESI) layer instead of pulling directly from on-prem.

**Step 1 — Create an ESI config file** (`esi_tables.json`):
```json
[
  {
    "source_system": "SAP",
    "esi_database": "ESI_SAP",
    "tables": ["VBAK", "VBAP", "MARA", "KNA1"]
  },
  {
    "source_system": "Oracle",
    "esi_database": "ESI_Oracle",
    "tables": ["ORDERS", "CUSTOMERS"]
  }
]
```

**Step 2 — Analyze with ESI detection:**
> Analyze `C:\Projects\LegacyETL\LoadFactSales.dtsx` with ESI tables config at `C:\config\esi_tables.json`.

**What you get:** INFO-level gap items identifying sources like "Table `VBAK` is available in ESI layer `ESI_SAP` — consider reading from ESI instead of SHIR."

---

### LLM Script Task Translation

Use Azure OpenAI to translate C# Script Task code to Python automatically.

**Before you start,** set these environment variables:
```powershell
$env:AZURE_OPENAI_ENDPOINT   = "https://my-resource.openai.azure.com/"
$env:AZURE_OPENAI_API_KEY    = "<your-key>"
$env:AZURE_OPENAI_DEPLOYMENT = "gpt-4o"
```

**Then convert with LLM translation:**
> Convert `C:\Projects\LegacyETL\LoadFactSales.dtsx` to `C:\adf_output\LoadFactSales` and translate Script Tasks to Python using Azure OpenAI.

**What you get:** The Azure Function stubs at `stubs/<FunctionName>/__init__.py` contain working Python code (translated from C#) instead of empty TODO blocks. The original C# is included as comments for reference.

> **Important:** Always review LLM-translated code before deploying. Infrastructure calls (SQL, file I/O, SMTP) are replaced with TODO placeholders that need manual completion.

---

### Cross-Package Deduplication

When migrating multiple packages that share the same connection managers, avoid creating duplicate linked services and datasets.

> **You:** Convert `C:\Projects\LegacyETL\LoadDimCustomer.dtsx` to `C:\adf_output\LoadDimCustomer`. Use `C:\adf_output\shared` as the shared artifacts directory.

> **Copilot:** *(generates linked services and datasets in the shared dir)*

> **You:** Now convert `C:\Projects\LegacyETL\LoadFactSales.dtsx` to `C:\adf_output\LoadFactSales`. Use the same shared directory `C:\adf_output\shared`.

> **Copilot:** *(reuses existing linked services and datasets from the shared dir, only writes new ones)*

The shared directory accumulates linked services and datasets across conversions. Each file is written only once — subsequent packages that reference the same connection reuse the existing artifact.

---

## Tips and Tricks

**Always dry run first.** Both `deploy_to_adf` and `deploy_function_stubs` support `dry_run` mode. Use it to preview what would happen before making real changes.

**Check the gap analysis before converting.** The `analyze_ssis_package` tool identifies everything that will need manual attention. Run it first so there are no surprises after conversion.

**Combine features freely.** You can use Key Vault, schema remapping, file path mapping, ESI detection, and LLM translation all in a single conversion call.

**Use shared artifacts for batch migrations.** When converting 10+ packages, point them all to the same `shared_artifacts_dir` to avoid dozens of duplicate linked service files.

**Provision + Deploy in sequence.** If you don't have a Function App yet, call `provision_function_app` first, then `deploy_function_stubs`. The provision tool creates everything you need.

**Review triggers manually.** Triggers are always deployed in Stopped state. If no SQL Agent schedule was found, the trigger defaults to daily at midnight. Update it to match your production schedule before activating.
