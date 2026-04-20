# HOWTO — SSIS → ADF Agent Conversation Guide

This guide shows how to use the SSIS → ADF Agent through natural-language prompts in **GitHub Copilot Chat**. Each section includes example conversations you can copy and adapt.

> **Tip:** Switch Copilot Chat to **Agent** mode before sending prompts so it can call the MCP tools automatically.
>
> For installation, architecture, and reference details see [README.md](README.md).

---

## Table of Contents

- [Migration Copilot — Estate Workflow (recommended)](#migration-copilot--estate-workflow-recommended)
- [End-to-End Migration](#end-to-end-migration)
  - [Quick Start: One Package](#quick-start-one-package)
  - [Full Migration: Multiple Packages](#full-migration-multiple-packages)
- [Per-Feature Guides](#per-feature-guides)
  - [Triage an Estate](#triage-an-estate)
  - [Propose a Migration Plan](#propose-a-migration-plan)
  - [Edit a Plan](#edit-a-plan)
  - [Plan Migration Waves](#plan-migration-waves)
  - [Estimate ADF Costs](#estimate-adf-costs)
  - [Build the Estate Report PDF](#build-the-estate-report-pdf)
  - [Convert an Estate](#convert-an-estate)
  - [Provision ADF Infrastructure](#provision-adf-infrastructure)
  - [Smoke-Test a Pipeline](#smoke-test-a-pipeline)
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

## Migration Copilot — Estate Workflow (recommended)

This is the conversation pattern when a customer hands you a folder of SSIS packages and you want a credible, sequenced plan before touching Azure.

> **You:** A customer just dropped their SSIS project at `C:\Customers\Contoso\Project`. Triage the whole estate so we know what we're dealing with.

> **Copilot:** *(calls `bulk_analyze`)* Found **18 packages**, no parse failures.
>
> | Bucket | Count |
> |---|---|
> | Low | 7 |
> | Medium | 6 |
> | High | 4 |
> | Very High | 1 |
>
> **By target pattern:** 11 `scheduled_file_drop`, 4 `ingest_file_to_sql`, 2 `sql_to_sql_copy`, 1 `script_heavy`. Total estate effort: **312 hours**, 6 manual-required items. 13 are bulk-convertible; 5 need design review.

> **You:** Plan the migration waves and project monthly cost. Save both reports under `C:\Customers\Contoso\reports\`.

> **Copilot:** *(calls `plan_migration_waves` and `estimate_adf_costs`)* 4 waves planned:
> - **Wave 1** — Bulk convert (scheduled_file_drop), 8 packages, 64h
> - **Wave 2** — Bulk convert (ingest_file_to_sql), 4 packages, 32h
> - **Wave 3** — Design review (script_heavy), 1 package, 60h
> - **Wave 4** — Design review (scheduled_file_drop), 5 packages, 156h
>
> Projected cost: **\$248/month** (\$2,976/year). Major lines: orchestration \$162, Copy DIU \$60, ADLS storage \$18, Key Vault ops \$2, Mapping Data Flow \$6.

> **You:** Build the estate PDF for the customer.

> **Copilot:** *(calls `build_estate_report`)* PDF written to `C:\Customers\Contoso\reports\estate-report.pdf` — executive summary, complexity & pattern composition, 4-wave plan, cost breakdown, per-package detail, and a parse-failures section (empty).

> **You:** For Wave 1, propose designs for each of the 8 packages and save the plans under `C:\Customers\Contoso\plans\`.

> **Copilot:** *(calls `propose_adf_design` then `save_migration_plan` for each)* Plans saved. Common recommendations across the wave: drop SMB atomic-write file system tasks, fold trivial Data Flows into Copy Activities, use Managed Identity for all SQL connections, target eastus2 ADLS Gen2 storage. One package also flagged a Send Mail task for replacement with a Logic App.

> **You:** Set the auth mode to ManagedIdentity across all 8 plans (it's already the default but make it explicit) and set the region to eastus2.

> **Copilot:** *(calls `edit_migration_plan` for each)* Done. All 8 plans now have `set_auth_mode=ManagedIdentity` and `set_region=eastus2` recorded explicitly.

> **You:** Provision the shared infrastructure using one of the plans as the spec — call it `adf-contoso-prod` in `rg-contoso-data`, location eastus2, subscription `aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee`. Dry run first.

> **Copilot:** *(calls `provision_adf_environment` with `dry_run=true`)* Bicep validated. Would create: Data Factory `adf-contoso-prod` (system MI), Storage Account `stcontosoprod{hash}` (ADLS Gen2), Key Vault `kv-contoso-prod`, plus 3 RBAC assignments granting the factory's MI: Storage Blob Data Contributor on the storage account, Key Vault Secrets User on the vault.

> **You:** Looks right. Provision for real.

> **Copilot:** *(re-runs without dry_run)* Provisioned in 73 seconds. Outputs returned.

> **You:** Now convert the entire Wave 1 estate using the saved plans. Output to `C:\Customers\Contoso\adf\`.

> **Copilot:** *(calls `convert_estate` with the plans path)* Converted 8 packages, 8 succeeded / 0 failed. Each lives in its own subdirectory with pipeline + linked services + datasets + (where applicable) data flow + trigger + stubs.

> **You:** Deploy `LoadFactSales` and run a smoke test.

> **Copilot:** *(calls `deploy_to_adf` then `smoke_test_pipeline`)* Deployed 7 artifacts. Pipeline run `run-9b3f` completed in **42s**, status **Succeeded**. Per-activity: `Lookup_Cfg` 2s, `Copy_Sales` 28s, `SetVar_RowCount` 1s — all green.

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

### Triage an Estate

Walk a directory of `.dtsx` files and produce an estate-level summary.

> Triage every SSIS package under `C:\Customers\Contoso\Project` and write the report to `C:\Customers\Contoso\reports\triage.json`.

The report includes per-package complexity score, target pattern, recommended simplifications, manual-required count, and a roll-up by complexity bucket and target pattern (with total estimated hours).

---

### Propose a Migration Plan

Generate a recommended `MigrationPlan` for one package.

> Propose an ADF design for `C:\Customers\Contoso\Project\LoadFactSales.dtsx` and save the plan to `C:\Customers\Contoso\plans\LoadFactSales.plan.json`.

The plan covers: target pattern, simplifications (with confidence scores), recommended linked services (Managed Identity by default), Azure infrastructure to provision, RBAC assignments, risks, and effort estimate.

---

### Edit a Plan

Apply structured mutations rather than hand-editing the JSON.

**Switch all linked services to Managed Identity:**
> Edit the plan at `C:\plans\LoadFactSales.plan.json` and set the auth mode to ManagedIdentity.

**Change the target region:**
> Edit `C:\plans\LoadFactSales.plan.json` and set the region to eastus2.

**Drop a recommended simplification you disagree with:**
> Edit `C:\plans\LoadFactSales.plan.json` and drop the fold_to_stored_proc simplification.

**Record a customer decision:**
> Edit `C:\plans\LoadFactSales.plan.json` and add a customer_decision noting that approver is jane@contoso.com.

Unknown edit keys are rejected so typos surface immediately.

---

### Plan Migration Waves

Group an estate into ordered delivery waves.

> Plan migration waves from `C:\Customers\Contoso\reports\triage.json`, max 8 packages per wave, and write the wave plan to `C:\Customers\Contoso\reports\waves.json`.

Bulk-convertible packages (low/medium complexity) come first, grouped by target pattern so reviewers share context. Design-review packages (high/very_high) follow, hardest-first within each pattern. Parse failures land in a final `triage` wave.

---

### Estimate ADF Costs

Project monthly Azure spend for the proposed estate.

**Default assumptions (1 run/day, 6 activities, 4 DIU, 5 min copy, 100 GB):**
> Estimate ADF costs for the estate report at `C:\Customers\Contoso\reports\triage.json`.

**Override the runtime profile:**
> Estimate ADF costs for `C:\Customers\Contoso\reports\triage.json` with 4 runs per day, 12 activities per run, 8 copy DIU, 10 minutes per copy, and 500 GB of storage.

The output breaks costs out by orchestration / Copy DIU / Mapping Data Flow / storage / Key Vault, with monthly and annual totals.

---

### Build the Estate Report PDF

Combine triage + waves + costs into one stakeholder PDF.

> Build the estate report PDF for customer Contoso. Use triage at `C:\Customers\Contoso\reports\triage.json`, waves at `C:\Customers\Contoso\reports\waves.json`, costs at `C:\Customers\Contoso\reports\costs.json`. Output to `C:\Customers\Contoso\reports\estate-report.pdf`.

The PDF includes an executive summary, complexity & pattern composition, the wave plan, projected monthly cost, and per-package detail.

---

### Convert an Estate

Run propose + convert across an entire directory in one shot.

> Convert every SSIS package under `C:\Customers\Contoso\Project` to `C:\Customers\Contoso\adf\`. Save the migration plans alongside.

Each package gets its own subdirectory containing the saved `migration_plan.json`, the generated ADF artifacts, and Function stubs (if applicable). The summary reports per-package status so failures can be triaged immediately.

---

### Provision ADF Infrastructure

Generate Bicep from a plan and deploy ADF + Storage + Key Vault + RBAC.

**Dry run first (validates the deployment without applying):**
> Do a dry run provision of the ADF environment from plan `C:\plans\LoadFactSales.plan.json` into resource group `rg-contoso-data`, subscription `aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee`. Save the Bicep template to `C:\Customers\Contoso\bicep\main.bicep`.

**Live provisioning:**
> Provision the ADF environment from plan `C:\plans\LoadFactSales.plan.json` into resource group `rg-contoso-data`, subscription `aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee`.

The Data Factory is created with a system-assigned managed identity. Storage Accounts default to ADLS Gen2 (HNS enabled). Key Vault is created with RBAC authorisation. RBAC assignments declared in the plan (e.g. Storage Blob Data Contributor for the factory MI) are applied automatically.

---

### Smoke-Test a Pipeline

Trigger one ADF pipeline run, poll until terminal, and return per-activity results.

**Default 10-minute timeout:**
> Smoke-test pipeline `PL_LoadFactSales` in factory `adf-contoso-prod`, resource group `rg-contoso-data`, subscription `aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee`.

**With pipeline parameters and a longer timeout:**
> Smoke-test pipeline `PL_LoadFactSales` in factory `adf-contoso-prod` with parameters `runDate=2026-04-15` and `region=eastus2`. Use a 30-minute timeout. Resource group `rg-contoso-data`, subscription `aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee`.

The result includes the run ID, terminal status, total duration, ADF-supplied error message (on failure), and per-activity rows with name / type / status / duration / error message.

---

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
