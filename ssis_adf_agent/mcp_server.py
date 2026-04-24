"""
SSIS → ADF MCP Server.

Exposes twenty-nine tools to GitHub Copilot (and any MCP-compatible client):

1. scan_ssis_packages         — discover .dtsx files (local / git / sql server)
2. analyze_ssis_package       — complexity + gap analysis of a single package
3. convert_ssis_package       — full conversion of a package to ADF JSON artifacts
4. validate_adf_artifacts     — structural validation of generated artifacts
5. deploy_to_adf              — deploy artifacts to Azure Data Factory
6. consolidate_packages       — merge similar packages into parameterized pipelines
7. deploy_function_stubs      — zip-deploy Azure Function stubs to an existing Function App
8. provision_function_app     — create Azure resources (Storage, App Insights, Function App)
9. explain_ssis_package       — prose + Mermaid documentation of a source SSIS package
10. explain_adf_artifacts     — prose + Mermaid documentation of generated ADF artifacts
11. validate_conversion_parity — SSIS↔ADF parity check + optional pre-migration PDF report
12. propose_adf_design        — recommend a best-practice target ADF design (MigrationPlan)
13. save_migration_plan       — persist a (possibly customer-edited) MigrationPlan to disk
14. load_migration_plan       — load a MigrationPlan from disk for downstream tools
15. provision_adf_environment — generate Bicep from plan + deploy to Azure (infra + RBAC)
16. bulk_analyze              — estate-scale triage of a directory of SSIS packages
17. smoke_test_pipeline       — trigger one ADF pipeline run, poll, return per-activity results
18. convert_estate            — propose + convert every package in a directory in one shot
19. edit_migration_plan       — structured mutations on a saved MigrationPlan
20. plan_migration_waves      — group saved plans into ordered migration waves
21. estimate_adf_costs        — plan-aware monthly USD cost projection for the estate
22. build_estate_report       — PDF deliverable from saved plans + waves + costs
23. build_predeployment_report — engineer-facing Markdown report with diagrams + checklists
24. activate_triggers         — bulk-activate ADF triggers (dry-run by default; H7)
25. export_arm_template       — bundle ADF artifacts into an ARM template (M2)
26. smoke_test_wave           — run smoke_test_pipeline against many pipelines, aggregate (N1)
27. compare_dataflow_output   — behavioral parity: row+column diff of SSIS DFT vs converted MDF (P4-1)
28. upload_encrypted_secrets  — push secrets from an unprotected .dtsx to Key Vault + rewrite linked services (P4-4)
29. compare_estimates_to_actuals — join lineage.json + Cost Management actuals into a per-factory variance report (P4-5)

Run as an MCP stdio server::

    python -m ssis_adf_agent.mcp_server

Or via the installed script::

    ssis-adf-agent
"""
from __future__ import annotations

import json
import logging
import tempfile
from pathlib import Path
from typing import Any

import mcp.server.stdio
import mcp.types as types
from mcp.server import Server

from .path_safety import safe_resolve as _safe_resolve
from .warnings_collector import WarningsCollector

logger = logging.getLogger("ssis_adf_agent")

# ---------------------------------------------------------------------------
# Server setup
# ---------------------------------------------------------------------------

server = Server("ssis-adf-agent")


# ---------------------------------------------------------------------------
# Tool: scan_ssis_packages
# ---------------------------------------------------------------------------

@server.list_tools()
async def list_tools() -> list[types.Tool]:
    return [
        types.Tool(
            name="scan_ssis_packages",
            description=(
                "Discover all SSIS packages (.dtsx files) from a given source. "
                "Returns a JSON list of found packages with name, path, and basic metadata. "
                "source_type must be one of: 'local', 'git', 'sql'."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "source_type": {
                        "type": "string",
                        "enum": ["local", "git", "sql"],
                        "description": "Where to find .dtsx files.",
                    },
                    "path_or_connection": {
                        "type": "string",
                        "description": (
                            "For 'local': absolute filesystem directory path. "
                            "For 'git': repository URL or local path. "
                            "For 'sql': SQL Server connection string or 'SERVER=...;DATABASE=msdb'."
                        ),
                    },
                    "recursive": {
                        "type": "boolean",
                        "description": "Search subdirectories (local/git only). Default: true.",
                        "default": True,
                    },
                    "git_branch": {
                        "type": "string",
                        "description": "Branch to check out (git source only). Default: 'main'.",
                        "default": "main",
                    },
                },
                "required": ["source_type", "path_or_connection"],
            },
        ),
        types.Tool(
            name="analyze_ssis_package",
            description=(
                "Analyze a single SSIS package (.dtsx file) and return a detailed report including: "
                "complexity score (0-100), effort estimate (Low/Medium/High/Very High), "
                "component inventory, gap analysis (items needing manual work), "
                "cross-database/linked server references, CDM pattern detection, "
                "ESI reuse candidates, and dependency execution order."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "package_path": {
                        "type": "string",
                        "description": "Absolute path to the .dtsx file.",
                    },
                    "esi_tables_path": {
                        "type": "string",
                        "description": (
                            "Optional path to a JSON file mapping source_system → table list for ESI reuse detection. "
                            "Format: {\"PHINEOS\": [\"TocPartyAddress\", \"TLBenefit\"]}."
                        ),
                    },
                },
                "required": ["package_path"],
            },
        ),
        types.Tool(
            name="convert_ssis_package",
            description=(
                "Convert a single SSIS package (.dtsx file) to Azure Data Factory JSON artifacts. "
                "Generates: pipeline JSON, linked service JSONs, dataset JSONs, "
                "mapping data flow JSONs, trigger JSONs, and Azure Function stubs for Script Tasks. "
                "Supports Self-Hosted IR, Key Vault secrets, Microsoft Recommended linked service format, "
                "schema remapping, ESI reuse detection, CDM pattern flagging, and cross-package dedup. "
                "Returns a summary of generated files and any warnings."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "package_path": {
                        "type": "string",
                        "description": "Absolute path to the .dtsx file to convert.",
                    },
                    "output_dir": {
                        "type": "string",
                        "description": (
                            "Directory to write ADF artifacts to. "
                            "Sub-folders pipeline/, linkedService/, dataset/, dataflow/, trigger/, stubs/ "
                            "will be created automatically."
                        ),
                    },
                    "generate_trigger": {
                        "type": "boolean",
                        "description": "Whether to emit a template ScheduleTrigger JSON. Default: true.",
                        "default": True,
                    },
                    "llm_translate": {
                        "type": "boolean",
                        "description": (
                            "If true, call Azure OpenAI to translate C# Script Task source code to Python "
                            "in the generated Azure Function stubs. Requires AZURE_OPENAI_ENDPOINT and "
                            "AZURE_OPENAI_API_KEY environment variables. Falls back gracefully if unavailable. "
                            "Default: false. Forced to false when SSIS_ADF_NO_LLM env var is set, or when "
                            "the no_llm parameter is true."
                        ),
                        "default": False,
                    },
                    "no_llm": {
                        "type": "boolean",
                        "description": (
                            "P4-8 hard switch — when true, forbid any LLM call regardless of "
                            "llm_translate. Equivalent to setting the SSIS_ADF_NO_LLM env var "
                            "for the duration of this single tool call. Use in regulated tenants "
                            "or to verify deterministic behaviour. Default: false."
                        ),
                        "default": False,
                    },
                    "on_prem_ir_name": {
                        "type": "string",
                        "description": "Integration Runtime name for on-prem connections. Default: 'SelfHostedIR'.",
                        "default": "SelfHostedIR",
                    },
                    "auth_type": {
                        "type": "string",
                        "description": (
                            "Default authentication type for Azure SQL linked services. "
                            "Default: 'SystemAssignedManagedIdentity'."
                        ),
                        "enum": ["SystemAssignedManagedIdentity", "SQL", "ServicePrincipal"],
                        "default": "SystemAssignedManagedIdentity",
                    },
                    "use_key_vault": {
                        "type": "boolean",
                        "description": (
                            "Use Azure Key Vault secret references for passwords/connection "
                            "strings. Default: false."
                        ),
                        "default": False,
                    },
                    "kv_ls_name": {
                        "type": "string",
                        "description": "Name for the Key Vault linked service. Default: 'LS_KeyVault'.",
                        "default": "LS_KeyVault",
                    },
                    "kv_url": {
                        "type": "string",
                        "description": "Azure Key Vault base URL. Default: 'https://TODO.vault.azure.net/'.",
                        "default": "https://TODO.vault.azure.net/",
                    },
                    "esi_tables_path": {
                        "type": "string",
                        "description": (
                            "Optional path to a JSON file mapping source_system → table list for ESI reuse detection."
                        ),
                    },
                    "schema_remap_path": {
                        "type": "string",
                        "description": (
                            "Optional path to a JSON file mapping old schema prefixes to new ones "
                            "for database consolidation. "
                            "Format: {\"StagingDB.dbo\": \"ConsolidatedDB.staging\"}."
                        ),
                    },
                    "shared_artifacts_dir": {
                        "type": "string",
                        "description": (
                            "Optional shared directory for cross-package linked service/dataset deduplication. "
                            "When converting multiple packages, point all to the same shared dir."
                        ),
                    },
                    "pipeline_prefix": {
                        "type": "string",
                        "description": "Prefix for pipeline names. Default: 'PL_'.",
                        "default": "PL_",
                    },
                    "file_path_map_path": {
                        "type": "string",
                        "description": (
                            "Optional path to a JSON file mapping local/UNC path prefixes to Azure Storage URLs. "
                            "Format: {\"C:\\\\Data\\\\Input\": \"https://mystorage.blob.core.windows.net/input\"}. "
                            "Applies to linked services, pipeline activities, and datasets."
                        ),
                    },
                    "substitution_registry_path": {
                        "type": "string",
                        "description": (
                            "Optional path to a substitution-registry JSON file (M7) that "
                            "maps 3rd-party SSIS Data Flow / Control Flow components "
                            "(Cozyroc, KingswaySoft, in-house) to specific ADF activity "
                            "or transformation types. Use this when the agent reports "
                            "'Unknown component type' or 'Unsupported' and you have a "
                            "deterministic ADF replacement in mind. See "
                            "docs/SUBSTITUTION_REGISTRY.md for the schema."
                        ),
                    },
                    "design_path": {
                        "type": "string",
                        "description": (
                            "Optional path to a MigrationPlan JSON file (output of propose_adf_design, "
                            "possibly customer-edited). When supplied, the converter applies the plan's "
                            "simplifications before generating ADF artifacts \u2014 e.g. dropping listed "
                            "FileSystemTasks and rewiring precedence constraints to preserve order. "
                            "Non-DROP simplifications are recorded in the response as deferred TODOs."
                        ),
                    },
                },
                "required": ["package_path", "output_dir"],
            },
        ),
        types.Tool(
            name="validate_adf_artifacts",
            description=(
                "Validate ADF JSON artifacts in a directory for structural correctness. "
                "Checks that required fields (name, properties, activities) are present. "
                "Returns a list of validation issues found, or a success message if all artifacts are valid."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "artifacts_dir": {
                        "type": "string",
                        "description": "Directory containing the generated ADF JSON artifacts.",
                    },
                },
                "required": ["artifacts_dir"],
            },
        ),
        types.Tool(
            name="deploy_to_adf",
            description=(
                "Deploy ADF JSON artifacts from a local directory to an Azure Data Factory instance. "
                "Deploys in correct dependency order: linked services → datasets → data flows → pipelines → triggers. "
                "Triggers are deployed in Stopped state and must be activated manually (see activate_triggers). "
                "Uses DefaultAzureCredential (az login, managed identity, or service principal env vars). "
                "By default the deploy is destructive (put_or_update overwrites in-factory edits). "
                "Pass skip_if_exists=true (H8) to refuse to overwrite any artifact that already exists "
                "in the target factory — recommended once a customer has hand-edited a linked service "
                "or pipeline in ADF Studio."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "artifacts_dir": {
                        "type": "string",
                        "description": "Directory containing generated ADF JSON artifacts.",
                    },
                    "subscription_id": {
                        "type": "string",
                        "description": "Azure subscription ID (GUID) or display name.",
                    },
                    "resource_group": {
                        "type": "string",
                        "description": "Azure resource group name containing the ADF instance.",
                    },
                    "factory_name": {
                        "type": "string",
                        "description": "Name of the Azure Data Factory to deploy to.",
                    },
                    "dry_run": {
                        "type": "boolean",
                        "description": "If true, validate and log but do not call Azure APIs. Default: false.",
                        "default": False,
                    },
                    "validate_first": {
                        "type": "boolean",
                        "description": (
                            "If true (default), run structural validation before deploying. "
                            "Invalid artifacts are skipped and reported."
                        ),
                        "default": True,
                    },
                    "skip_if_exists": {
                        "type": "boolean",
                        "description": (
                            "If true, before each create_or_update call check whether an artifact "
                            "of the same name already exists in the target factory; if it does, "
                            "skip it (status='skipped_exists') instead of overwriting. Use this for "
                            "non-destructive re-deploys against a factory that contains hand-edited "
                            "artifacts. Default: false (preserves the existing destructive behavior)."
                        ),
                        "default": False,
                    },
                    "pre_flight": {
                        "type": "boolean",
                        "description": (
                            "P4-6 — if true, skip the actual deploy and instead probe every "
                            "external dependency the linked services declare: Key Vault secret "
                            "existence + read permission, host DNS resolution, and a managed-identity "
                            "token-fetch against ARM. Returns a per-target reachability / "
                            "permission report. No ADF resources are created. Default: false."
                        ),
                        "default": False,
                    },
                    "preflight_skip_kv": {"type": "boolean", "default": False, "description": "With pre_flight=true, skip the Key Vault probes (e.g. air-gapped review)."},
                    "preflight_skip_dns": {"type": "boolean", "default": False, "description": "With pre_flight=true, skip the DNS resolution probes."},
                    "preflight_skip_mi_token": {"type": "boolean", "default": False, "description": "With pre_flight=true, skip the managed-identity token probe."},
                    "preflight_report_path": {"type": "string", "description": "With pre_flight=true, optional path to write the JSON pre-flight report."},
                },
                "required": ["artifacts_dir", "subscription_id", "resource_group", "factory_name"],
            },
        ),
        types.Tool(
            name="consolidate_packages",
            description=(
                "Analyze multiple SSIS packages for structural similarity and consolidate "
                "identical packages into a single parameterized ADF pipeline. "
                "For example, 10 packages that all do 'run SQL → export to CSV' become "
                "one child pipeline with parameters and one parent pipeline with a ForEach "
                "that iterates a config array. Returns similarity analysis, consolidation "
                "groups, and generated pipeline file paths."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "package_paths": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of absolute paths to .dtsx files to analyze and consolidate.",
                    },
                    "output_dir": {
                        "type": "string",
                        "description": (
                            "Directory to write consolidated ADF artifacts to. "
                            "Sub-folders pipeline/, linkedService/, dataset/, etc. will be created."
                        ),
                    },
                    "pipeline_prefix": {
                        "type": "string",
                        "description": "Prefix for pipeline names. Default: 'PL_'.",
                        "default": "PL_",
                    },
                    "analyze_only": {
                        "type": "boolean",
                        "description": (
                            "If true, only perform similarity analysis and return groupings "
                            "without generating consolidated pipelines. Default: false."
                        ),
                        "default": False,
                    },
                },
                "required": ["package_paths"],
            },
        ),
        types.Tool(
            name="deploy_function_stubs",
            description=(
                "Deploy Azure Function stubs to an existing Azure Function App via zip deploy. "
                "The stubs directory must be a complete Azure Functions project (as generated by "
                "convert_ssis_package: host.json, requirements.txt, function directories with "
                "__init__.py and function.json). Uses DefaultAzureCredential for authentication. "
                "Supports dry_run mode to validate and build the zip without uploading."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "stubs_dir": {
                        "type": "string",
                        "description": (
                            "Path to the stubs directory to deploy. This is typically "
                            "<output_dir>/stubs from a prior convert_ssis_package call."
                        ),
                    },
                    "subscription_id": {
                        "type": "string",
                        "description": "Azure subscription ID (GUID) or display name.",
                    },
                    "resource_group": {
                        "type": "string",
                        "description": "Azure resource group containing the Function App.",
                    },
                    "function_app_name": {
                        "type": "string",
                        "description": "Name of the existing Azure Function App to deploy to.",
                    },
                    "dry_run": {
                        "type": "boolean",
                        "description": (
                            "If true, build the deployment zip and validate but do not upload. "
                            "Returns the zip size and list of functions that would be deployed. "
                            "Default: false."
                        ),
                        "default": False,
                    },
                },
                "required": ["stubs_dir", "subscription_id", "resource_group", "function_app_name"],
            },
        ),
        types.Tool(
            name="provision_function_app",
            description=(
                "Provision Azure infrastructure for hosting Function stubs: "
                "Storage Account, Application Insights (optional), Consumption App Service Plan, "
                "and a Python Linux Function App. All resources are created in the specified "
                "resource group and location. Uses DefaultAzureCredential for authentication. "
                "Run this BEFORE deploy_function_stubs if no Function App exists yet."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "function_app_name": {
                        "type": "string",
                        "description": "Globally unique name for the Function App.",
                    },
                    "subscription_id": {
                        "type": "string",
                        "description": "Azure subscription ID (GUID) or display name.",
                    },
                    "resource_group": {
                        "type": "string",
                        "description": "Azure resource group (must already exist).",
                    },
                    "location": {
                        "type": "string",
                        "description": "Azure region (e.g. 'eastus2', 'westeurope').",
                    },
                    "storage_account_name": {
                        "type": "string",
                        "description": (
                            "Override the auto-derived storage account name. "
                            "Must be 3-24 lowercase alphanumeric characters."
                        ),
                    },
                    "skip_app_insights": {
                        "type": "boolean",
                        "description": "Skip creating Application Insights. Default: false.",
                        "default": False,
                    },
                    "python_version": {
                        "type": "string",
                        "description": "Python runtime version. Default: '3.11'.",
                        "default": "3.11",
                    },
                    "dry_run": {
                        "type": "boolean",
                        "description": (
                            "If true, report what would be created without provisioning. "
                            "Default: false."
                        ),
                        "default": False,
                    },
                },
                "required": ["function_app_name", "subscription_id", "resource_group", "location"],
            },
        ),
        types.Tool(
            name="explain_ssis_package",
            description=(
                "Produce a structured explanation of an SSIS package: what it does, the systems "
                "involved (databases, file shares, services), step-by-step execution order, and "
                "Mermaid diagrams of the control flow and each Data Flow Task. Returns both a JSON "
                "outline (for an LLM to elaborate on) and a deterministic Markdown rendering."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "package_path": {
                        "type": "string",
                        "description": "Absolute path to the .dtsx file.",
                    },
                    "format": {
                        "type": "string",
                        "enum": ["markdown", "json", "both"],
                        "description": "Output format. Default 'both'.",
                        "default": "both",
                    },
                },
                "required": ["package_path"],
            },
        ),
        types.Tool(
            name="explain_adf_artifacts",
            description=(
                "Produce a structured explanation of generated ADF artifacts: pipeline activities, "
                "linked services, datasets, mapping data flows, triggers, and Azure Function stubs. "
                "Includes Mermaid activity-graph diagrams. Reads the directory produced by "
                "convert_ssis_package."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "output_dir": {
                        "type": "string",
                        "description": (
                            "Directory containing generated ADF JSON artifacts "
                            "(with pipeline/, linkedService/, dataset/, dataflow/, trigger/ subfolders)."
                        ),
                    },
                    "format": {
                        "type": "string",
                        "enum": ["markdown", "json", "both"],
                        "description": "Output format. Default 'both'.",
                        "default": "both",
                    },
                },
                "required": ["output_dir"],
            },
        ),
        types.Tool(
            name="validate_conversion_parity",
            description=(
                "Pre-deployment validation: compare an SSIS source package to its converted ADF "
                "artifacts and verify they preserve the same logic. Reports task coverage, linked "
                "service mapping, parameter coverage, Script Task stub generation, and event-handler "
                "considerations. Optionally performs an SDK dry-run (deserialize each artifact via "
                "azure-mgmt-datafactory) and a target-factory reachability check. Can also produce "
                "a pre-migration PDF report."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "package_path": {
                        "type": "string",
                        "description": "Absolute path to the source .dtsx file.",
                    },
                    "output_dir": {
                        "type": "string",
                        "description": "Directory containing the generated ADF JSON artifacts.",
                    },
                    "dry_run": {
                        "type": "boolean",
                        "description": (
                            "If true, additionally deserialize each ADF artifact via the SDK to "
                            "catch schema errors. Default true."
                        ),
                        "default": True,
                    },
                    "subscription_id": {
                        "type": "string",
                        "description": (
                            "Optional. Subscription ID (GUID) or display name. If supplied with "
                            "resource_group + factory_name, also confirm factory reachability."
                        ),
                    },
                    "resource_group": {
                        "type": "string",
                        "description": "Optional. Used with subscription_id + factory_name.",
                    },
                    "factory_name": {
                        "type": "string",
                        "description": "Optional. Used with subscription_id + resource_group.",
                    },
                    "pdf_report_path": {
                        "type": "string",
                        "description": (
                            "Optional. If supplied, write a pre-migration PDF report to this path. "
                            "Requires reportlab to be installed."
                        ),
                    },
                },
                "required": ["package_path", "output_dir"],
            },
        ),
        types.Tool(
            name="propose_adf_design",
            description=(
                "SSIS Migration Copilot: analyze an SSIS package and recommend a best-practice "
                "target ADF design (a MigrationPlan). The plan describes the target pattern, "
                "recommended simplifications (e.g. drop SMB atomic-write FileSystemTasks when the "
                "sink is Blob/ADLS, fold trivial Mapping Data Flows into Copy Activities), "
                "linked-service auth recommendations (managed identity by default), Azure "
                "infrastructure to provision, RBAC assignments, risks, and an effort estimate. "
                "The agent should review the plan with the customer, edit it, persist it via "
                "save_migration_plan, then pass it to convert_ssis_package and "
                "provision_adf_environment."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "package_path": {
                        "type": "string",
                        "description": "Absolute path to the .dtsx file to analyze.",
                    },
                    "output_path": {
                        "type": "string",
                        "description": (
                            "Optional. If supplied, also save the proposed plan as JSON to this "
                            "path. Equivalent to calling propose_adf_design then save_migration_plan."
                        ),
                    },
                },
                "required": ["package_path"],
            },
        ),
        types.Tool(
            name="save_migration_plan",
            description=(
                "Persist a MigrationPlan (typically the output of propose_adf_design, possibly "
                "edited by the agent/customer) to a JSON file. Returns the resolved path."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "plan": {
                        "type": "object",
                        "description": "The MigrationPlan as a JSON object.",
                    },
                    "path": {
                        "type": "string",
                        "description": "Absolute path to write the plan JSON to.",
                    },
                },
                "required": ["plan", "path"],
            },
        ),
        types.Tool(
            name="load_migration_plan",
            description=(
                "Load a MigrationPlan from a JSON file on disk. Validates schema_version. "
                "Returns the plan as a JSON object plus a Markdown rendering for human review."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Absolute path to the plan JSON file.",
                    },
                },
                "required": ["path"],
            },
        ),
        types.Tool(
            name="provision_adf_environment",
            description=(
                "Generate a Bicep template from a MigrationPlan's infrastructure_needed and "
                "rbac_needed sections, then deploy it to an Azure resource group. Provisions "
                "the ADF instance (with system-assigned managed identity), Storage account "
                "(HNS-enabled for ADLS Gen2 when the plan requests it), and Key Vault when "
                "non-MI auth is required. Built-in Azure RBAC role assignments are emitted "
                "into the same template; SQL-server-side roles like db_datareader are skipped "
                "with a note (must be granted via T-SQL post-deploy). Requires the Azure CLI "
                "on PATH (for 'az bicep build') and authentication via DefaultAzureCredential. "
                "Day-2 follow-up: see OBSERVABILITY.md for the recommended Log Analytics "
                "diagnostic-settings target (PipelineRuns, ActivityRuns, TriggerRuns, "
                "PipelineActivityRuns, AllMetrics) plus the three baseline alert rules. Pass "
                "with_observability=<workspace-resource-id> to emit the diagnosticSettings "
                "resource into the same template so the factory streams logs to your Log "
                "Analytics workspace from Day-1 (P5-7)."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "plan_path": {
                        "type": "string",
                        "description": "Absolute path to the MigrationPlan JSON file.",
                    },
                    "subscription_id": {
                        "type": "string",
                        "description": "Azure subscription ID (GUID) or display name. Required unless dry_run=true and you only want offline Bicep compilation.",
                    },
                    "resource_group": {
                        "type": "string",
                        "description": "Target resource group (must already exist). Required unless dry_run=true and you only want offline Bicep compilation.",
                    },
                    "name_prefix": {
                        "type": "string",
                        "description": "3-11 char lowercase prefix used in generated resource names. Default: 'ssismig'.",
                        "default": "ssismig",
                    },
                    "deployment_name": {
                        "type": "string",
                        "description": "Name for the ARM deployment record. Default: 'ssis-migration-copilot'.",
                        "default": "ssis-migration-copilot",
                    },
                    "output_bicep_path": {
                        "type": "string",
                        "description": "Optional. If supplied, also write the generated Bicep to this path for inspection / source control.",
                    },
                    "dry_run": {
                        "type": "boolean",
                        "description": "If true, do not actually apply the deployment. If subscription_id and resource_group are supplied, the template is validated against Azure. If they are omitted, the Bicep is only compiled locally (requires Azure CLI on PATH) so you can preview it without any Azure access. Default: false.",
                        "default": False,
                    },
                    "with_observability": {
                        "type": "string",
                        "description": "Optional. Full ARM resource id of a Log Analytics workspace (e.g. /subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.OperationalInsights/workspaces/<name>). When set, the generated Bicep includes a Microsoft.Insights/diagnosticSettings child resource on the factory wired to this workspace with the five log/metric categories named in OBSERVABILITY.md (P5-7).",
                    },
                },
                "required": ["plan_path"],
            },
        ),
        types.Tool(
            name="bulk_analyze",
            description=(
                "Estate-scale triage. Recursively scan a directory for .dtsx files, analyze each "
                "one, and emit a sortable summary that buckets packages by complexity (low / "
                "medium / high / very_high) and recommended target pattern. Use this as the first "
                "step when a customer drops a large project on you \u2014 it separates the simple "
                "file-drop packages that can be bulk-converted from the complex ones that need "
                "human design review. Output also includes per-package risks, manual-required "
                "counts, and effort estimates rolled up across the estate."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "source_path": {
                        "type": "string",
                        "description": "Absolute path to a directory containing .dtsx files (recursively scanned).",
                    },
                    "output_path": {
                        "type": "string",
                        "description": "Optional. If supplied, write the full triage report (JSON) to this path.",
                    },
                    "recursive": {
                        "type": "boolean",
                        "description": "Recurse into subdirectories. Default: true.",
                        "default": True,
                    },
                },
                "required": ["source_path"],
            },
        ),
        types.Tool(
            name="smoke_test_pipeline",
            description=(
                "Trigger a single run of an existing ADF pipeline and return per-activity "
                "results. Use this immediately after deploy_to_adf to verify a converted "
                "pipeline actually executes end-to-end against real linked services. Polls "
                "until the run reaches a terminal status (Succeeded / Failed / Cancelled) "
                "or the timeout elapses, then queries activity_runs for a per-activity "
                "breakdown (name, type, status, duration, error message). Authentication "
                "uses DefaultAzureCredential (az login on dev machines)."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "subscription_id": {"type": "string", "description": "Azure subscription ID (GUID) or display name."},
                    "resource_group": {"type": "string", "description": "Resource group of the factory."},
                    "factory_name": {"type": "string", "description": "Data Factory name."},
                    "pipeline_name": {"type": "string", "description": "Pipeline to run (must already exist)."},
                    "parameters": {
                        "type": "object",
                        "description": "Optional parameter overrides for the pipeline run.",
                        "additionalProperties": True,
                    },
                    "timeout_seconds": {
                        "type": "integer",
                        "description": "Maximum seconds to wait for the run. Default 600.",
                        "default": 600,
                    },
                    "poll_interval_seconds": {
                        "type": "integer",
                        "description": "Seconds between status polls. Default 10.",
                        "default": 10,
                    },
                },
                "required": ["subscription_id", "resource_group", "factory_name", "pipeline_name"],
            },
        ),
        types.Tool(
            name="convert_estate",
            description=(
                "Convert every .dtsx in a directory in one shot. For each package: propose a "
                "MigrationPlan (saved alongside), then run convert_ssis_package using that plan, "
                "writing artifacts to <output_dir>/<package_name>/. Returns a summary with per-"
                "package status (succeeded / failed) so the agent can immediately follow up on "
                "failures. Use after bulk_analyze to bulk-convert the low/medium tier."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "source_path": {"type": "string", "description": "Directory of .dtsx files."},
                    "output_dir": {"type": "string", "description": "Root directory for converted artifacts."},
                    "recursive": {"type": "boolean", "default": True},
                    "save_plans": {
                        "type": "boolean",
                        "description": "If true (default), persist each MigrationPlan to <output>/<pkg>/migration_plan.json.",
                        "default": True,
                    },
                    "generate_trigger": {"type": "boolean", "default": True},
                },
                "required": ["source_path", "output_dir"],
            },
        ),
        types.Tool(
            name="edit_migration_plan",
            description=(
                "Apply structured edits to a saved MigrationPlan and write the result back. "
                "Supports: set_auth_mode, set_region, set_summary, set_target_pattern, "
                "add_simplification, drop_simplification, set_customer_decision. Safer than "
                "hand-editing the JSON because it validates enum values and preserves shape."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "plan_path": {"type": "string", "description": "Path to existing plan JSON."},
                    "edits": {
                        "type": "object",
                        "description": "Mutation payload \u2014 see tool description for keys.",
                        "additionalProperties": True,
                    },
                    "output_path": {
                        "type": "string",
                        "description": "Optional. Where to write the edited plan. Defaults to overwriting plan_path.",
                    },
                },
                "required": ["plan_path", "edits"],
            },
        ),
        types.Tool(
            name="plan_migration_waves",
            description=(
                "Group saved MigrationPlans into ordered migration waves. Requires plans to be "
                "proposed (propose_adf_design) and saved (save_migration_plan) first — do not "
                "call before the ADF design is agreed. Wave 1 is bulk-convertible packages "
                "grouped by target pattern; later waves cover design-review-needed packages, "
                "hardest-first."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "plans_dir": {
                        "type": "string",
                        "description": (
                            "Directory containing saved *_plan.json files "
                            "(produced by save_migration_plan)."
                        ),
                    },
                    "max_packages_per_wave": {"type": "integer", "default": 10},
                    "estate_setup_hours": {
                        "type": "number",
                        "description": (
                            "One-time estate bring-up hours added to Wave 1 "
                            "(IR / Key Vault / CI-CD / RBAC / observability). "
                            "Default: 24."
                        ),
                        "default": 24,
                    },
                    "apply_learning_curve": {
                        "type": "boolean",
                        "description": (
                            "Discount later packages within a wave (100%, 90%, 85%, 80%, ...) "
                            "to reflect reuse of design decisions and linked services. Default: true."
                        ),
                        "default": True,
                    },
                    "output_path": {
                        "type": "string",
                        "description": "Optional. Write the wave plan JSON to this path.",
                    },
                },
                "required": ["plans_dir"],
            },
        ),
        types.Tool(
            name="estimate_adf_costs",
            description=(
                "Coarse monthly USD cost projection for the proposed ADF estate. Requires saved "
                "MigrationPlans — call propose_adf_design and save_migration_plan first. "
                "Introspects each plan's activity mix (Copy vs Data Flow vs orchestration) and "
                "linked-service count for accurate projections. Returns a per-line-item breakdown "
                "plus monthly and annual totals. List-price US East defaults; override via rates."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "plans_dir": {
                        "type": "string",
                        "description": (
                            "Directory containing saved *_plan.json files "
                            "(produced by save_migration_plan)."
                        ),
                    },
                    "runs_per_day": {"type": "integer", "default": 1},
                    "avg_copy_diu": {"type": "number", "default": 4.0},
                    "avg_copy_minutes": {"type": "number", "default": 5.0},
                    "avg_dataflow_minutes": {"type": "number", "default": 10.0},
                    "avg_dataflow_vcores": {"type": "integer", "default": 8},
                    "storage_gb": {"type": "number", "default": 100.0},
                    "rates": {"type": "object", "additionalProperties": {"type": "number"}},
                    "output_path": {"type": "string", "description": "Optional JSON output path."},
                },
                "required": ["plans_dir"],
            },
        ),
        types.Tool(
            name="build_estate_report",
            description=(
                "Produce the customer-facing estate-level PDF: executive summary, complexity & "
                "pattern breakdown, recommended migration waves, projected costs, per-package "
                "detail. Requires a directory of saved MigrationPlans. Optionally accepts "
                "pre-computed wave and cost JSONs; otherwise derives them from the plans."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "plans_dir": {
                        "type": "string",
                        "description": (
                            "Directory containing saved *_plan.json files "
                            "(produced by save_migration_plan)."
                        ),
                    },
                    "waves_path": {"type": "string", "description": "Optional pre-computed waves JSON. If supplied, wave settings are taken from this file and max_packages_per_wave is ignored."},
                    "cost_estimate_path": {"type": "string", "description": "Optional pre-computed cost JSON. If supplied, runs_per_day and the other cost knobs are ignored."},
                    "max_packages_per_wave": {
                        "type": "integer",
                        "description": "Maximum packages per wave when deriving waves inline. Ignored if waves_path is supplied. Default: 10.",
                        "default": 10,
                    },
                    "runs_per_day": {
                        "type": "number",
                        "description": "Cost knob: average pipeline runs per day per package. Ignored if cost_estimate_path is supplied. Default: 1.",
                        "default": 1,
                    },
                    "estate_setup_hours": {
                        "type": "number",
                        "description": "One-time estate bring-up hours added to Wave 1. Ignored if waves_path is supplied. Default: 24.",
                        "default": 24,
                    },
                    "apply_learning_curve": {
                        "type": "boolean",
                        "description": "Apply learning-curve discount inside waves. Ignored if waves_path is supplied. Default: true.",
                        "default": True,
                    },
                    "output_pdf": {"type": "string", "description": "Path to write the PDF."},
                    "customer_name": {"type": "string"},
                },
                "required": ["plans_dir", "output_pdf"],
            },
        ),
        types.Tool(
            name="build_predeployment_report",
            description=(
                "Generate a comprehensive pre-deployment Markdown report for an SSIS migration estate. "
                "For each package: SSIS summary, Mermaid control-flow and data-flow diagrams, "
                "component descriptions, ADF solution overview with pipeline activity graphs, "
                "and detailed pre- and post-deployment checklists of manual tasks. "
                "Targets the engineer/admin persona. Requires converted ADF artifacts on disk."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "entries": {
                        "type": "array",
                        "description": (
                            "List of packages to include. Each entry has: "
                            "package_path (absolute .dtsx path), "
                            "adf_dir (absolute path to ADF output directory), "
                            "plan_path (optional, absolute path to migration plan JSON)."
                        ),
                        "items": {
                            "type": "object",
                            "properties": {
                                "package_path": {"type": "string", "description": "Absolute path to the .dtsx file."},
                                "adf_dir": {"type": "string", "description": "Absolute path to the ADF output directory for this package."},
                                "plan_path": {"type": "string", "description": "Optional. Absolute path to the migration plan JSON."},
                            },
                            "required": ["package_path", "adf_dir"],
                        },
                    },
                    "output_path": {
                        "type": "string",
                        "description": "Absolute path to write the Markdown report. If omitted, the report is returned inline.",
                    },
                    "title": {
                        "type": "string",
                        "description": "Optional custom report title.",
                    },
                },
                "required": ["entries"],
            },
        ),
        types.Tool(
            name="activate_triggers",
            description=(
                "Bulk-activate one or many ADF triggers in a target factory. "
                "Triggers are deployed in Stopped state by deploy_to_adf; this tool is the "
                "explicit, opt-in counterpart to start them. Defaults to dry_run=true: it lists "
                "what would be activated without making any state-changing call. Pass "
                "dry_run=false to actually start triggers. Already-running triggers are reported "
                "as 'already_started' (no-op). Unknown trigger names are reported as 'not_found' "
                "(error, not silently ignored). Returns one row per processed trigger with "
                "before/after runtime state."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "subscription_id": {
                        "type": "string",
                        "description": "Subscription ID (GUID) or display name of the Azure subscription owning the factory.",
                    },
                    "resource_group": {
                        "type": "string",
                        "description": "Resource group name containing the Data Factory.",
                    },
                    "factory_name": {
                        "type": "string",
                        "description": "Name of the target Data Factory.",
                    },
                    "trigger_names": {
                        "type": "array",
                        "description": (
                            "Optional explicit list of trigger names to activate. If omitted, "
                            "EVERY trigger in the factory is considered (still dry-run by "
                            "default). Recommended workflow: list_triggers first, then pass the "
                            "filtered set."
                        ),
                        "items": {"type": "string"},
                    },
                    "dry_run": {
                        "type": "boolean",
                        "description": (
                            "If true (default), report what would happen without calling the "
                            "Azure trigger start API. If false, actually start the triggers."
                        ),
                        "default": True,
                    },
                },
                "required": ["subscription_id", "resource_group", "factory_name"],
            },
        ),
        types.Tool(
            name="export_arm_template",
            description=(
                "Bundle an ADF artifacts directory into an ARM template (M2). "
                "Useful for customers who deploy ADF content via azd / "
                "az deployment instead of the agent's deploy_to_adf path. "
                "The template assumes the target Data Factory already exists "
                "(it only declares child resources: linkedservices, datasets, "
                "dataflows, pipelines, triggers). Triggers are written with "
                "runtimeState='Stopped' to match deploy_to_adf semantics."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "artifacts_dir": {
                        "type": "string",
                        "description": "Directory containing the generated ADF JSON artifacts.",
                    },
                    "output_path": {
                        "type": "string",
                        "description": (
                            "Optional explicit output path for the ARM template. "
                            "Defaults to <artifacts_dir>/adf_content.arm.json."
                        ),
                    },
                },
                "required": ["artifacts_dir"],
            },
        ),
        types.Tool(
            name="smoke_test_wave",
            description=(
                "Cross-pipeline regression harness (N1). Runs smoke_test_pipeline "
                "against every pipeline in `pipeline_names` (or auto-discovered from "
                "an artifacts_dir) and returns one aggregated report: per-pipeline "
                "status + a summary section with succeeded / failed / cancelled / "
                "timed_out counts. Stops early on first failure when stop_on_failure "
                "is true (useful for wave-by-wave sign-off; default false runs all)."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "subscription_id": {"type": "string", "description": "Azure subscription ID (GUID) or display name."},
                    "resource_group": {"type": "string", "description": "Resource group of the factory."},
                    "factory_name": {"type": "string", "description": "Data Factory name."},
                    "pipeline_names": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Explicit list of pipeline names to test. Mutually exclusive with artifacts_dir.",
                    },
                    "artifacts_dir": {
                        "type": "string",
                        "description": "Directory containing generated ADF artifacts; pipelines/*.json filenames become the test set. Used when pipeline_names is omitted.",
                    },
                    "parameters": {
                        "type": "object",
                        "description": "Optional pipeline parameters applied to every pipeline run.",
                    },
                    "timeout_seconds": {"type": "integer", "default": 600},
                    "poll_interval_seconds": {"type": "integer", "default": 10},
                    "stop_on_failure": {"type": "boolean", "default": False},
                },
                "required": ["subscription_id", "resource_group", "factory_name"],
            },
        ),
        types.Tool(
            name="compare_dataflow_output",
            description=(
                "Behavioral parity harness (P4-1). Runs the *same* input dataset "
                "through an SSIS Data Flow Task and through its converted ADF "
                "Mapping Data Flow, then emits a row-and-column diff report. "
                "Closes the gap left by validate_conversion_parity (which only "
                "checks structural parity, not actual data values).\n\n"
                "Three runner modes via the `mode` field:\n"
                "  - `captured` (default): replay previously-captured CSV outputs from "
                "    `ssis_captured_csv` and `adf_captured_csv`. No dtexec or live "
                "    Azure required. Use this for the worked example, regression "
                "    tests, and air-gapped reviews.\n"
                "  - `live`: run dtexec for the SSIS side and an ADF Mapping Data Flow "
                "    debug session for the ADF side. Requires dtexec on PATH and an "
                "    Azure subscription with the ADF reachable. Slow and "
                "    environment-dependent; intended for one-shot evidence captures.\n"
                "  - `mixed`: any combination of `captured` / `live` per side "
                "    (e.g. SSIS captured + ADF live for spot checks)."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "package_path": {"type": "string", "description": "Absolute path to the SSIS .dtsx package."},
                    "dataflow_task_name": {"type": "string", "description": "Name of the Data Flow Task inside the package to compare."},
                    "adf_dataflow_path": {"type": "string", "description": "Absolute path to the generated ADF Mapping Data Flow JSON."},
                    "input_dataset_path": {"type": "string", "description": "Absolute path to the controlled input dataset (CSV) fed to both sides."},
                    "key_columns": {"type": "array", "items": {"type": "string"}, "description": "Columns that uniquely identify each output row."},
                    "compare_columns": {"type": "array", "items": {"type": "string"}, "description": "Optional subset of columns to compare. If omitted, all common non-key columns are compared."},
                    "ignore_columns": {"type": "array", "items": {"type": "string"}, "description": "Columns to exclude (e.g. non-deterministic timestamps)."},
                    "ignore_case": {"type": "boolean", "default": False},
                    "strip_whitespace": {"type": "boolean", "default": True},
                    "numeric_tolerance": {"type": "number", "default": 0.0, "description": "Absolute tolerance for numeric comparisons (0 = exact)."},
                    "mode": {"type": "string", "enum": ["captured", "live", "mixed"], "default": "captured"},
                    "ssis_mode": {"type": "string", "enum": ["captured", "live"], "description": "Override for SSIS side when mode='mixed'."},
                    "adf_mode": {"type": "string", "enum": ["captured", "live"], "description": "Override for ADF side when mode='mixed'."},
                    "ssis_captured_csv": {"type": "string", "description": "Required when SSIS side is captured. CSV of the SSIS Data Flow's destination output."},
                    "adf_captured_csv": {"type": "string", "description": "Required when ADF side is captured. CSV of the ADF Mapping Data Flow's sink output."},
                    "dtexec": {
                        "type": "object",
                        "description": "Required when SSIS side is live. Settings for the dtexec runner.",
                        "properties": {
                            "source_connection_path": {"type": "string", "description": "SSIS /Set property path of the source Connection Manager's ConnectionString."},
                            "destination_connection_path": {"type": "string", "description": "SSIS /Set property path of the destination Connection Manager's ConnectionString."},
                            "destination_filename": {"type": "string", "default": "adf_parity_dest.csv"},
                            "dtexec_path": {"type": "string", "description": "Optional explicit path to dtexec.exe."},
                            "timeout_seconds": {"type": "integer", "default": 600},
                        },
                    },
                    "adf_debug": {
                        "type": "object",
                        "description": "Required when ADF side is live. Settings for the ADF debug runner.",
                        "properties": {
                            "subscription_id": {"type": "string", "description": "Azure subscription ID (GUID) or display name."},
                            "resource_group": {"type": "string"},
                            "factory_name": {"type": "string"},
                            "compute_type": {"type": "string", "default": "General"},
                            "core_count": {"type": "integer", "default": 8},
                            "time_to_live_minutes": {"type": "integer", "default": 10},
                            "output_stream_name": {"type": "string"},
                            "row_limit": {"type": "integer", "default": 1000},
                        },
                    },
                    "report_path": {"type": "string", "description": "Optional Markdown report path."},
                    "diff_json_path": {"type": "string", "description": "Optional JSON diff path (full machine-readable result)."},
                },
                "required": ["package_path", "dataflow_task_name", "adf_dataflow_path", "input_dataset_path", "key_columns"],
            },
        ),
        types.Tool(
            name="upload_encrypted_secrets",
            description=(
                "Encrypted-package automation helper (P4-4). Automates Steps 2 + 4 "
                "of the ENCRYPTED_PACKAGES.md recipe: extracts secrets from an "
                "unprotected .dtsx (the customer still runs dtutil manually so "
                "the decrypt remains auditable on their side), uploads each secret "
                "to Azure Key Vault, then rewrites the placeholder secretName "
                "fields inside the generated linked-service JSON to point at the "
                "real Key Vault secret names. Pass `dry_run=true` to preview "
                "without touching Key Vault or the linked-service files."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "unprotected_dtsx_path": {"type": "string", "description": "Absolute path to the unprotected .dtsx (output of `dtutil /DECRYPT`)."},
                    "package_name": {"type": "string", "description": "Logical package name; becomes the `{package}` token in secret names."},
                    "kv_url": {"type": "string", "description": "Azure Key Vault URL, e.g. https://kv-ssis.vault.azure.net/"},
                    "linked_service_dir": {"type": "string", "description": "Directory containing the generated linked-service JSON files (typically out/<package>/linkedService)."},
                    "secret_name_template": {"type": "string", "default": "{package}-{cm}-{kind}", "description": "Template for the Key Vault secret name. Tokens: {package}, {cm}, {kind}."},
                    "placeholder_template": {"type": "string", "default": "{cm}-password", "description": "Template the linked-service generator used for the placeholder secretName. Must match generator convention."},
                    "dry_run": {"type": "boolean", "default": False, "description": "Preview without uploading or rewriting."},
                    "overwrite": {"type": "boolean", "default": False, "description": "When false (default), skip secrets that already exist in the vault."},
                    "report_path": {"type": "string", "description": "Optional path for a JSON upload report."},
                },
                "required": ["unprotected_dtsx_path", "package_name", "kv_url", "linked_service_dir"],
            },
        ),
        types.Tool(
            name="compare_estimates_to_actuals",
            description=(
                "Cost-actuals join helper (P4-5). Reads the deployed lineage.json "
                "manifest plus an Azure Cost Management export (REST response "
                "JSON or portal CSV) and emits a per-factory variance report. "
                "Optionally accepts the dict returned by `estimate_adf_costs` to "
                "compute variance vs. the original prediction. Per-pipeline "
                "allocation is included as an estimate (Cost Management does not "
                "invoice ADF spend below factory granularity)."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "lineage_path": {"type": "string", "description": "Path to the deployed lineage.json (azure_resource_id fields populated by deploy_to_adf)."},
                    "actuals_path": {"type": "string", "description": "Path to a Cost Management REST response JSON or a portal Cost Analysis CSV export."},
                    "estimate_path": {"type": "string", "description": "Optional path to a JSON file containing the dict returned by `estimate_adf_costs`. Used as the variance baseline."},
                    "period_label": {"type": "string", "description": "Caller-supplied label for the cost period (echoed back, not interpreted), e.g. '2026-03'."},
                    "factory_resource_id": {"type": "string", "description": "Override the factory ARM id; otherwise derived from lineage.json."},
                    "report_path": {"type": "string", "description": "Optional path to write the JSON variance report."},
                },
                "required": ["lineage_path", "actuals_path"],
            },
        ),
    ]


# ---------------------------------------------------------------------------
# Tool handlers
# ---------------------------------------------------------------------------

@server.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[types.TextContent]:
    try:
        # Resolve subscription name → GUID before dispatching to any tool.
        if "subscription_id" in arguments and arguments["subscription_id"]:
            from .credential import resolve_subscription_id
            arguments["subscription_id"] = resolve_subscription_id(
                arguments["subscription_id"]
            )

        if name == "scan_ssis_packages":
            return await _scan(arguments)
        elif name == "analyze_ssis_package":
            return await _analyze(arguments)
        elif name == "convert_ssis_package":
            return await _convert(arguments)
        elif name == "validate_adf_artifacts":
            return await _validate(arguments)
        elif name == "deploy_to_adf":
            return await _deploy(arguments)
        elif name == "consolidate_packages":
            return await _consolidate(arguments)
        elif name == "deploy_function_stubs":
            return await _deploy_stubs(arguments)
        elif name == "provision_function_app":
            return await _provision_func_app(arguments)
        elif name == "explain_ssis_package":
            return await _explain_ssis(arguments)
        elif name == "explain_adf_artifacts":
            return await _explain_adf(arguments)
        elif name == "validate_conversion_parity":
            return await _validate_parity(arguments)
        elif name == "propose_adf_design":
            return await _propose_design(arguments)
        elif name == "save_migration_plan":
            return await _save_plan(arguments)
        elif name == "load_migration_plan":
            return await _load_plan(arguments)
        elif name == "provision_adf_environment":
            return await _provision_adf_env(arguments)
        elif name == "bulk_analyze":
            return await _bulk_analyze(arguments)
        elif name == "smoke_test_pipeline":
            return await _smoke_test_pipeline(arguments)
        elif name == "convert_estate":
            return await _convert_estate(arguments)
        elif name == "edit_migration_plan":
            return await _edit_plan(arguments)
        elif name == "plan_migration_waves":
            return await _plan_waves(arguments)
        elif name == "estimate_adf_costs":
            return await _estimate_costs(arguments)
        elif name == "build_estate_report":
            return await _build_estate_pdf(arguments)
        elif name == "build_predeployment_report":
            return await _build_predeployment_report(arguments)
        elif name == "activate_triggers":
            return await _activate_triggers(arguments)
        elif name == "export_arm_template":
            return await _export_arm_template(arguments)
        elif name == "smoke_test_wave":
            return await _smoke_test_wave(arguments)
        elif name == "compare_dataflow_output":
            return await _compare_dataflow_output(arguments)
        elif name == "upload_encrypted_secrets":
            return await _upload_encrypted_secrets(arguments)
        elif name == "compare_estimates_to_actuals":
            return await _compare_estimates_to_actuals(arguments)
        else:
            return [types.TextContent(type="text", text=f"Unknown tool: {name}")]
    except Exception as exc:
        logger.error("Tool %s failed: %s", name, exc, exc_info=True)
        return [types.TextContent(type="text", text=f"Error: {exc}")]


async def _scan(args: dict[str, Any]) -> list[types.TextContent]:
    source_type = args["source_type"]
    path_or_conn = args["path_or_connection"]
    recursive = args.get("recursive", True)
    branch = args.get("git_branch", "main")

    # Validate local paths against traversal
    if source_type == "local":
        _safe_resolve(path_or_conn, must_exist=True, label="path_or_connection")

    packages_info: list[dict[str, Any]] = []

    with WarningsCollector() as wc:
        if source_type == "local":
            from .parsers.readers.local_reader import LocalReader
            reader = LocalReader()
            paths = reader.scan(path_or_conn, recursive=recursive)
            for p in paths:
                packages_info.append({"name": p.stem, "path": str(p), "source": "local"})

        elif source_type == "git":
            from .parsers.readers.git_reader import GitReader
            reader = GitReader(branch=branch)
            pkgs = reader.read_all(path_or_conn, recursive=recursive)
            for pkg in pkgs:
                packages_info.append({
                    "name": pkg.name,
                    "source_file": pkg.source_file,
                    "protection_level": pkg.protection_level.value,
                    "task_count": len(pkg.tasks),
                    "connection_count": len(pkg.connection_managers),
                })

        elif source_type == "sql":
            # Expect path_or_conn to be a pyodbc-style connection string
            # Parse it first to get server/database
            import re

            from .parsers.readers.sql_reader import SqlServerReader
            server_m = re.search(r"SERVER=([^;]+)", path_or_conn, re.I)
            db_m = re.search(r"DATABASE=([^;]+)", path_or_conn, re.I)
            server = server_m.group(1) if server_m else "localhost"
            database = db_m.group(1) if db_m else "msdb"
            reader = SqlServerReader(server=server, database=database, trusted_connection=True)
            names = reader.list_packages()
            for n in names:
                packages_info.append({"name": n, "source": "msdb", "server": server})

        result = {
            "found": len(packages_info),
            "packages": packages_info,
            "conversion_warnings": [w.model_dump() for w in wc.warnings],
        }

    return [types.TextContent(type="text", text=json.dumps(result, indent=2))]


async def _analyze(args: dict[str, Any]) -> list[types.TextContent]:
    from .analyzers.cdm_pattern_detector import detect_cdm_patterns
    from .analyzers.complexity_scorer import score_package_detailed
    from .analyzers.dependency_graph import build_package_dependency_order
    from .analyzers.esi_reuse_analyzer import analyze_esi_reuse, load_esi_config
    from .analyzers.gap_analyzer import analyze_gaps
    from .analyzers.similarity_analyzer import fingerprint_package
    from .parsers.readers.local_reader import LocalReader

    with WarningsCollector() as wc:
        path = _safe_resolve(args["package_path"], must_exist=True, label="package_path")
        reader = LocalReader()
        package = reader.read(path)

        complexity, script_classifications = score_package_detailed(package)
        gaps = analyze_gaps(package)

        # CDM pattern detection
        cdm_gaps = detect_cdm_patterns(package)
        gaps.extend(cdm_gaps)

        # ESI reuse detection
        esi_gaps: list = []
        esi_tables_path = args.get("esi_tables_path")
        if esi_tables_path:
            _safe_resolve(esi_tables_path, must_exist=True, label="esi_tables_path")
            esi_config = load_esi_config(esi_tables_path)
            esi_gaps = analyze_esi_reuse(package, esi_config)
            gaps.extend(esi_gaps)

        dep_order = build_package_dependency_order(package)

        # Structural fingerprint for consolidation grouping
        fp = fingerprint_package(package)

        # Get task names in order
        task_by_id = {t.id: t for t in package.tasks}
        ordered_names = [task_by_id[tid].name for tid in dep_order if tid in task_by_id]

        report = {
            "package_name": package.name,
            "source_file": package.source_file,
            "complexity": complexity.model_dump(),
            "gap_count": len(gaps),
            "gaps_by_severity": {
                "manual_required": [g.model_dump() for g in gaps if g.severity == "manual_required"],
                "warning": [g.model_dump() for g in gaps if g.severity == "warning"],
                "info": [g.model_dump() for g in gaps if g.severity == "info"],
            },
            "execution_order": ordered_names,
            "connection_managers": [
                {"name": cm.name, "type": cm.type.value, "server": cm.server, "database": cm.database}
                for cm in package.connection_managers
            ],
            "parameters": [p.name for p in package.parameters],
            "variables": [v.name for v in package.variables if v.namespace.lower() == "user"],
            "event_handlers": [eh.event_name for eh in package.event_handlers],
            "consolidation_fingerprint": {
                "digest": fp.digest[:12],
                "shape": fp.shape_summary,
                "task_sequence": list(fp.task_type_sequence),
                "connection_types": list(fp.connection_manager_types),
            },
            "script_task_classifications": [
                {
                    "tier": sc.tier.value,
                    "weight": sc.weight,
                    "reason": sc.reason,
                    "variables_only": sc.variables_only,
                    "adf_expressible": sc.adf_expressible,
                }
                for sc in script_classifications
            ],
            "conversion_warnings": [w.model_dump() for w in wc.warnings],
        }

    return [types.TextContent(type="text", text=json.dumps(report, indent=2))]


async def _convert(args: dict[str, Any]) -> list[types.TextContent]:
    from .analyzers.cdm_pattern_detector import detect_cdm_patterns
    from .analyzers.esi_reuse_analyzer import analyze_esi_reuse, load_esi_config
    from .generators.dataflow_generator import generate_data_flows
    from .generators.dataset_generator import generate_datasets
    from .generators.linked_service_generator import generate_linked_services
    from .generators.pipeline_generator import generate_pipeline
    from .generators.trigger_generator import generate_triggers
    from .parsers.readers.local_reader import LocalReader

    path = _safe_resolve(args["package_path"], must_exist=True, label="package_path")
    output_dir = _safe_resolve(args["output_dir"], label="output_dir")
    gen_trigger = args.get("generate_trigger", True)
    llm_translate = args.get("llm_translate", False)

    # P4-8 — per-call no-LLM switch.  Honour both the per-call arg and the
    # environment-level policy switch; either one wins.
    from .translators.csharp_to_python import no_llm_policy_enabled
    no_llm_arg = bool(args.get("no_llm", False))
    if no_llm_arg or no_llm_policy_enabled():
        if llm_translate:
            # Caller asked for LLM but policy / arg forbids it — surface a
            # warning so the response makes the degraded behaviour explicit.
            import warnings as _warnings
            reason = (
                "no_llm=true argument" if no_llm_arg
                else "SSIS_ADF_NO_LLM environment variable"
            )
            _warnings.warn(
                f"[P4-8] llm_translate=true was requested but is overridden "
                f"by {reason}. Script Task stubs will use deterministic "
                "TODO scaffolding only.",
                stacklevel=2,
            )
        llm_translate = False

    # New parameters
    on_prem_ir_name = args.get("on_prem_ir_name", "SelfHostedIR")
    auth_type = args.get("auth_type", "SystemAssignedManagedIdentity")
    use_key_vault = args.get("use_key_vault", False)
    kv_ls_name = args.get("kv_ls_name", "LS_KeyVault")
    kv_url = args.get("kv_url", "https://TODO.vault.azure.net/")
    if use_key_vault and "TODO" in kv_url:
        return [types.TextContent(
            type="text",
            text=(
                "Error: Key Vault URL is still the placeholder "
                "'https://TODO.vault.azure.net/'. "
                "Set the kv_url parameter to your actual Azure Key Vault URL."
            ),
        )]
    pipeline_prefix = args.get("pipeline_prefix", "PL_")
    shared_artifacts_dir = (
        _safe_resolve(args["shared_artifacts_dir"], label="shared_artifacts_dir")
        if args.get("shared_artifacts_dir")
        else None
    )

    # Load optional config files
    schema_remap: dict[str, str] | None = None
    schema_remap_path = args.get("schema_remap_path")
    if schema_remap_path:
        safe_remap = _safe_resolve(schema_remap_path, must_exist=True, label="schema_remap_path")
        schema_remap = json.loads(safe_remap.read_text(encoding="utf-8"))

    esi_config: dict = {}
    esi_tables_path = args.get("esi_tables_path")
    if esi_tables_path:
        _safe_resolve(esi_tables_path, must_exist=True, label="esi_tables_path")
        esi_config = load_esi_config(esi_tables_path)

    file_path_map: dict[str, str] | None = None
    file_path_map_path = args.get("file_path_map_path")
    if file_path_map_path:
        safe_fpm = _safe_resolve(file_path_map_path, must_exist=True, label="file_path_map_path")
        file_path_map = json.loads(safe_fpm.read_text(encoding="utf-8"))

    # M7 — optional substitution registry for 3rd-party (Cozyroc/KingswaySoft/in-house)
    # components.  Loaded once and threaded into the data-flow generator.
    substitution_registry = None
    sub_reg_path = args.get("substitution_registry_path")
    if sub_reg_path:
        from .converters.substitution_registry import load_registry
        safe_reg = _safe_resolve(sub_reg_path, must_exist=True, label="substitution_registry_path")
        substitution_registry = load_registry(safe_reg)

    with WarningsCollector() as wc:
        reader = LocalReader()
        package = reader.read(path)

        # Apply migration plan, if supplied
        plan_application = None
        design_path = args.get("design_path")
        if design_path:
            from .migration_plan import apply_plan, load_plan
            safe_design = _safe_resolve(design_path, must_exist=True, label="design_path")
            plan = load_plan(safe_design)
            package, plan_application = apply_plan(package, plan)

        stubs_dir = output_dir / "stubs"

        # Extract name overrides from plan, if present
        name_overrides: dict[str, str] | None = None
        if plan_application and plan_application.name_overrides:
            name_overrides = plan_application.name_overrides

        # Run gap analysis for manual-work checklist
        from .analyzers.gap_analyzer import analyze_gaps
        gaps = analyze_gaps(package)

        # Run analyzers for annotations
        cdm_gaps = detect_cdm_patterns(package)
        esi_gaps = analyze_esi_reuse(package, esi_config) if esi_config else []

        # Run generators with new parameters
        linked_services, ls_name_map = generate_linked_services(
            package, output_dir,
            on_prem_ir_name=on_prem_ir_name,
            auth_type=auth_type,
            use_key_vault=use_key_vault,
            kv_ls_name=kv_ls_name,
            kv_url=kv_url,
            shared_artifacts_dir=shared_artifacts_dir,
            name_overrides=name_overrides,
        )
        datasets = generate_datasets(
            package, output_dir,
            schema_remap=schema_remap,
            shared_artifacts_dir=shared_artifacts_dir,
            ls_name_map=ls_name_map,
            name_overrides=name_overrides,
        )
        data_flows = generate_data_flows(
            package, output_dir,
            ls_name_map=ls_name_map,
            name_overrides=name_overrides,
            substitution_registry=substitution_registry,
        )
        pipeline = generate_pipeline(
            package, output_dir,
            stubs_dir=stubs_dir,
            llm_translate=llm_translate,
            pipeline_prefix=pipeline_prefix,
            cdm_gaps=cdm_gaps,
            esi_gaps=esi_gaps,
            schema_remap=schema_remap,
            ls_name_map=ls_name_map,
            name_overrides=name_overrides,
        )
        triggers = generate_triggers(
            package, output_dir,
            name_overrides=name_overrides,
        ) if gen_trigger else []

        # Apply file path mapping (rewrite local/UNC paths to Azure Storage URLs)
        path_rewrites = 0
        if file_path_map:
            from .generators.file_path_mapper import apply_file_path_map
            path_rewrites = apply_file_path_map(
                {
                    "linked_services": linked_services,
                    "pipeline": pipeline,
                    "datasets": datasets,
                },
                file_path_map,
            )
            # Re-write the modified pipeline JSON to disk
            pipeline_file = output_dir / "pipeline" / f"{pipeline['name']}.json"
            pipeline_file.write_text(
                json.dumps(pipeline, indent=4, ensure_ascii=False), encoding="utf-8",
            )
            # Re-write linked services
            ls_dir = output_dir / "linkedService"
            for ls_obj in linked_services:
                ls_file = ls_dir / f"{ls_obj['name']}.json"
                if ls_file.exists():
                    ls_file.write_text(
                        json.dumps(ls_obj, indent=4, ensure_ascii=False), encoding="utf-8",
                    )

        # Find stub files
        stub_files = list(stubs_dir.rglob("*.py")) if stubs_dir.exists() else []

        # Generate Azure Functions project files around the stubs
        func_project_files: dict[str, str] = {}
        if stub_files:
            from .generators.func_project_generator import generate_func_project
            func_project_files = generate_func_project(stubs_dir)

        # Auto-validate generated artifacts
        from .deployer.adf_deployer import AdfDeployer
        deployer = AdfDeployer.__new__(AdfDeployer)
        validation_issues = deployer.validate_artifacts(output_dir)

        # M1 — emit lineage manifest (package -> artifact files; deploy IDs
        # are filled in later by deploy_to_adf).
        from .generators.lineage_generator import generate_lineage_manifest
        try:
            generate_lineage_manifest(package, output_dir, pipeline)
        except Exception as exc:  # noqa: BLE001 — never block conversion
            logger.warning("lineage manifest generation failed: %s", exc)

        # Check for unresolved ExecutePipeline references
        pipeline_refs = _check_execute_pipeline_refs(
            pipeline, output_dir, shared_artifacts_dir,
        )

        # Collect warnings from pipeline activities
        conversion_warnings = [
            act["description"]
            for act in pipeline.get("properties", {}).get("activities", [])
            if "MANUAL REVIEW" in act.get("description", "") or "UNSUPPORTED" in act.get("description", "")
        ]

        summary = {
            "package_name": package.name,
            "output_directory": str(output_dir),
            "artifacts_generated": {
                "pipelines": 1,
                "linked_services": len(linked_services),
                "datasets": len(datasets),
                "data_flows": len(data_flows),
                "triggers": len(triggers),
                "azure_function_stubs": len(stub_files),
                "func_project_files": func_project_files,
            },
            "validation": {
                "status": "valid" if not validation_issues else "issues_found",
                "issue_count": len(validation_issues),
                "issues": validation_issues[:10],
            },
            "unresolved_pipeline_refs": pipeline_refs,
            "file_path_rewrites": path_rewrites,
            "manual_review_required": len(conversion_warnings),
            "cdm_patterns_flagged": len(cdm_gaps),
            "esi_reuse_candidates": len(esi_gaps),
            "gap_analysis": {
                "total": len(gaps),
                "manual_required": [g.model_dump() for g in gaps if g.severity == "manual_required"],
                "warnings": [g.model_dump() for g in gaps if g.severity == "warning"],
            },
            "warnings": conversion_warnings[:20],  # cap output size
            "conversion_warnings": [w.model_dump() for w in wc.warnings],
            "files": {
                "pipeline": str(output_dir / "pipeline" / f"{pipeline['name']}.json"),
                "linked_services": [ls["name"] for ls in linked_services],
                "datasets": [ds["name"] for ds in datasets],
                "data_flows": [df["name"] for df in data_flows],
                "stubs": [str(f) for f in stub_files],
            },
        }
        if plan_application is not None:
            summary["migration_plan_applied"] = plan_application.model_dump()

    return [types.TextContent(type="text", text=json.dumps(summary, indent=2))]


def _check_execute_pipeline_refs(
    pipeline: dict[str, Any],
    output_dir: Path,
    shared_artifacts_dir: Path | None,
) -> list[str]:
    """Return a list of ExecutePipeline reference names that have no matching
    pipeline JSON in *output_dir* or *shared_artifacts_dir*."""
    # Collect all available pipeline names on disk
    available: set[str] = set()
    for search_dir in (output_dir, shared_artifacts_dir):
        if search_dir is None:
            continue
        pl_dir = search_dir / "pipeline"
        if pl_dir.exists():
            for f in pl_dir.glob("*.json"):
                available.add(f.stem)

    # Also include the current pipeline name itself
    available.add(pipeline.get("name", ""))

    unresolved: list[str] = []
    for act in pipeline.get("properties", {}).get("activities", []):
        if act.get("type") == "ExecutePipeline":
            ref = act.get("typeProperties", {}).get("pipeline", {}).get("referenceName", "")
            if ref and ref not in available:
                unresolved.append(ref)
    return unresolved


async def _validate(args: dict[str, Any]) -> list[types.TextContent]:
    from .deployer.adf_deployer import AdfDeployer

    artifacts_dir = _safe_resolve(args["artifacts_dir"], must_exist=True, label="artifacts_dir")
    # Validation doesn't require Azure credentials
    deployer = AdfDeployer.__new__(AdfDeployer)
    issues = deployer.validate_artifacts(artifacts_dir)

    if not issues:
        result = {"status": "valid", "message": "All artifacts passed structural validation."}
    else:
        result = {"status": "issues_found", "issue_count": len(issues), "issues": issues}

    return [types.TextContent(type="text", text=json.dumps(result, indent=2))]


async def _deploy(args: dict[str, Any]) -> list[types.TextContent]:
    from .deployer.adf_deployer import AdfDeployer

    # P4-6 — short-circuit to pre-flight probes when requested. No ADF
    # resources are created and the deployer is not constructed.
    if args.get("pre_flight"):
        from .deployer.preflight import run_preflight

        report = run_preflight(
            artifacts_dir=_safe_resolve(args["artifacts_dir"], must_exist=True, label="artifacts_dir"),
            subscription_id=args["subscription_id"],
            resource_group=args["resource_group"],
            factory_name=args["factory_name"],
            skip_kv=bool(args.get("preflight_skip_kv", False)),
            skip_dns=bool(args.get("preflight_skip_dns", False)),
            skip_mi_token=bool(args.get("preflight_skip_mi_token", False)),
        )
        payload = report.to_dict()
        if args.get("preflight_report_path"):
            out = _safe_resolve(args["preflight_report_path"], label="preflight_report_path")
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
            payload["report_path"] = str(out)
        summary_lines = [
            f"deploy_to_adf PRE-FLIGHT — {args['factory_name']}",
            f"  Counts: {payload['counts'] or {'(none)': 0}}",
            f"  Has failures: {payload['has_failures']}",
        ]
        for c in payload["checks"]:
            summary_lines.append(f"  [{c['status'].upper():7}] {c['kind']:11} {c['target']}")
        return [
            types.TextContent(type="text", text="\n".join(summary_lines)),
            types.TextContent(type="text", text=json.dumps(payload, indent=2, default=str)),
        ]

    with WarningsCollector() as wc:
        deployer = AdfDeployer(
            subscription_id=args["subscription_id"],
            resource_group=args["resource_group"],
            factory_name=args["factory_name"],
        )
        results = deployer.deploy_all(
            _safe_resolve(args["artifacts_dir"], must_exist=True, label="artifacts_dir"),
            dry_run=args.get("dry_run", False),
            validate_first=args.get("validate_first", True),
            skip_if_exists=args.get("skip_if_exists", False),
        )

        # M1 — backfill the lineage manifest with Azure resource IDs for every
        # successfully-deployed artifact (skipped artifacts still exist under
        # the same name, so they are also resolvable).
        if not args.get("dry_run", False):
            from .generators.lineage_generator import update_lineage_with_deployment
            try:
                update_lineage_with_deployment(
                    _safe_resolve(args["artifacts_dir"], must_exist=True, label="artifacts_dir"),
                    results,
                    subscription_id=args["subscription_id"],
                    resource_group=args["resource_group"],
                    factory_name=args["factory_name"],
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("lineage manifest backfill failed: %s", exc)

        summary = {
            "total": len(results),
            "succeeded": sum(1 for r in results if r.success),
            "failed": sum(1 for r in results if not r.success),
            "skipped": sum(1 for r in results if getattr(r, "skipped", False)),
            "results": [
                {"type": r.artifact_type, "name": r.name, "success": r.success,
                 "skipped": getattr(r, "skipped", False),
                 "error": r.error, "retries": r.retries}
                for r in results
            ],
            "warnings": [w.model_dump() for w in wc.warnings],
        }
    return [types.TextContent(type="text", text=json.dumps(summary, indent=2))]


async def _deploy_stubs(args: dict[str, Any]) -> list[types.TextContent]:
    from .deployer.func_deployer import FuncDeployer

    stubs_dir = _safe_resolve(args["stubs_dir"], must_exist=True, label="stubs_dir")

    deployer = FuncDeployer(
        subscription_id=args["subscription_id"],
        resource_group=args["resource_group"],
        function_app_name=args["function_app_name"],
    )
    result = deployer.deploy(stubs_dir, dry_run=args.get("dry_run", False))

    summary = {
        "success": result.success,
        "function_app_name": result.function_app_name,
        "functions_deployed": result.functions_deployed,
        "zip_size_bytes": result.zip_size_bytes,
        "scm_url": result.scm_url,
        "error": result.error,
    }
    return [types.TextContent(type="text", text=json.dumps(summary, indent=2))]


async def _provision_func_app(args: dict[str, Any]) -> list[types.TextContent]:
    from .deployer.func_provisioner import FuncProvisioner

    provisioner = FuncProvisioner(
        subscription_id=args["subscription_id"],
        resource_group=args["resource_group"],
        location=args["location"],
    )
    result = provisioner.provision(
        function_app_name=args["function_app_name"],
        storage_account_name=args.get("storage_account_name"),
        skip_app_insights=args.get("skip_app_insights", False),
        python_version=args.get("python_version", "3.11"),
        dry_run=args.get("dry_run", False),
    )

    summary = {
        "success": result.success,
        "function_app_name": result.function_app_name,
        "resource_group": result.resource_group,
        "location": result.location,
        "storage_account_name": result.storage_account_name,
        "app_insights_name": result.app_insights_name,
        "app_service_plan_name": result.app_service_plan_name,
        "function_app_url": result.function_app_url,
        "resources_created": result.resources_created,
        "error": result.error,
    }
    return [types.TextContent(type="text", text=json.dumps(summary, indent=2))]


async def _consolidate(args: dict[str, Any]) -> list[types.TextContent]:
    from .analyzers.similarity_analyzer import group_similar_packages
    from .generators.consolidated_pipeline_generator import generate_consolidated_pipelines
    from .parsers.readers.local_reader import LocalReader

    package_paths = [_safe_resolve(p, must_exist=True, label="package_paths") for p in args["package_paths"]]
    output_dir = _safe_resolve(args["output_dir"], label="output_dir") if args.get("output_dir") else None
    pipeline_prefix = args.get("pipeline_prefix", "PL_")
    analyze_only = args.get("analyze_only", False)

    with WarningsCollector() as wc:
        reader = LocalReader()
        packages = [reader.read(p) for p in package_paths]

        result = group_similar_packages(packages)

        # Build the analysis report
        report: dict[str, Any] = {
            "total_packages": result.total_packages,
            "consolidation_groups": len(result.groups),
            "ungrouped_packages": len(result.ungrouped),
            "groups": [],
            "ungrouped": [
                {
                    "package_name": fp.package_name,
                    "source_file": fp.source_file,
                    "fingerprint": fp.digest[:12],
                    "shape": fp.shape_summary,
                }
                for fp in result.ungrouped
            ],
        }

        for group in result.groups:
            group_info: dict[str, Any] = {
                "fingerprint": group.fingerprint.digest[:12],
                "shape": group.fingerprint.shape_summary,
                "package_count": len(group.packages),
                "packages": [pkg.name for pkg in group.packages],
                "varying_parameters": group.shared_parameter_names,
                "parameter_sets": [
                    {"package": ps.package_name, "values": ps.values}
                    for ps in group.parameter_sets
                ],
            }

            if not analyze_only and output_dir is not None:
                gen_result = generate_consolidated_pipelines(
                    group,
                    output_dir,
                    pipeline_prefix=pipeline_prefix,
                )
                group_info["generated"] = gen_result

            report["groups"].append(group_info)

        report["conversion_warnings"] = [w.model_dump() for w in wc.warnings]

    return [types.TextContent(type="text", text=json.dumps(report, indent=2))]


# ---------------------------------------------------------------------------
# Documentation + parity tools
# ---------------------------------------------------------------------------

async def _explain_ssis(args: dict[str, Any]) -> list[types.TextContent]:
    from .documentation import build_ssis_outline, render_ssis_markdown
    from .parsers.readers.local_reader import LocalReader

    path = _safe_resolve(args["package_path"], must_exist=True, label="package_path")
    fmt = args.get("format", "both")

    with WarningsCollector() as wc:
        package = LocalReader().read(path)
        outline = build_ssis_outline(package)
        outline["conversion_warnings"] = [w.model_dump() for w in wc.warnings]

    if fmt == "json":
        return [types.TextContent(type="text", text=json.dumps(outline, indent=2, default=str))]

    markdown = render_ssis_markdown(outline)
    if fmt == "markdown":
        return [types.TextContent(type="text", text=markdown)]

    # both
    return [
        types.TextContent(type="text", text=markdown),
        types.TextContent(type="text", text=json.dumps(outline, indent=2, default=str)),
    ]


async def _explain_adf(args: dict[str, Any]) -> list[types.TextContent]:
    from .documentation import build_adf_outline, render_adf_markdown

    output_dir = _safe_resolve(args["output_dir"], must_exist=True, label="output_dir")
    fmt = args.get("format", "both")

    outline = build_adf_outline(output_dir)

    if fmt == "json":
        return [types.TextContent(type="text", text=json.dumps(outline, indent=2, default=str))]

    markdown = render_adf_markdown(outline)
    if fmt == "markdown":
        return [types.TextContent(type="text", text=markdown)]

    return [
        types.TextContent(type="text", text=markdown),
        types.TextContent(type="text", text=json.dumps(outline, indent=2, default=str)),
    ]


async def _validate_parity(args: dict[str, Any]) -> list[types.TextContent]:
    from .documentation import build_pre_migration_pdf, validate_parity
    from .documentation.adf_explainer import build_adf_outline
    from .documentation.parity_validator import render_parity_markdown
    from .documentation.ssis_explainer import build_ssis_outline
    from .parsers.readers.local_reader import LocalReader

    package_path = _safe_resolve(args["package_path"], must_exist=True, label="package_path")
    output_dir = _safe_resolve(args["output_dir"], must_exist=True, label="output_dir")

    with WarningsCollector() as wc:
        package = LocalReader().read(package_path)

        result = validate_parity(
            package,
            output_dir,
            dry_run=args.get("dry_run", True),
            subscription_id=args.get("subscription_id"),
            resource_group=args.get("resource_group"),
            factory_name=args.get("factory_name"),
        )

        result_dict = result.to_dict()
        result_dict["conversion_warnings"] = [w.model_dump() for w in wc.warnings]

        markdown = render_parity_markdown(result)

        # Optional PDF
        pdf_path_arg = args.get("pdf_report_path")
        if pdf_path_arg:
            pdf_path = _safe_resolve(pdf_path_arg, label="pdf_report_path")
            ssis_outline = build_ssis_outline(package)
            adf_outline = build_adf_outline(output_dir)
            factory_target = None
            if args.get("factory_name"):
                factory_target = {
                    "Subscription": str(args.get("subscription_id", "")),
                    "Resource group": str(args.get("resource_group", "")),
                    "Factory": str(args.get("factory_name", "")),
                }
            written = build_pre_migration_pdf(
                output_pdf=pdf_path,
                ssis_outline=ssis_outline,
                adf_outline=adf_outline,
                parity=result_dict,
                factory_target=factory_target,
            )
            result_dict["pdf_report_path"] = written
            markdown += f"\n\n_Pre-migration PDF written to:_ `{written}`"

    return [
        types.TextContent(type="text", text=markdown),
        types.TextContent(type="text", text=json.dumps(result_dict, indent=2, default=str)),
    ]


# ---------------------------------------------------------------------------
# Tool: propose_adf_design / save_migration_plan / load_migration_plan
# ---------------------------------------------------------------------------

async def _propose_design(args: dict[str, Any]) -> list[types.TextContent]:
    from .migration_plan import propose_design, save_plan
    from .parsers.readers.local_reader import LocalReader

    path = _safe_resolve(args["package_path"], must_exist=True, label="package_path")
    package = LocalReader().read(path)
    plan = propose_design(package)

    saved_to: str | None = None
    if args.get("output_path"):
        out = _safe_resolve(args["output_path"], label="output_path")
        save_plan(plan, out)
        saved_to = str(out)

    payload: dict[str, Any] = {
        "plan": plan.model_dump(mode="json"),
        "markdown": plan.render_markdown(),
    }
    if saved_to:
        payload["saved_to"] = saved_to
    return [types.TextContent(type="text", text=json.dumps(payload, indent=2, default=str))]


async def _save_plan(args: dict[str, Any]) -> list[types.TextContent]:
    from .migration_plan import MigrationPlan, save_plan

    plan = MigrationPlan.model_validate(args["plan"])
    out = _safe_resolve(args["path"], label="path")
    saved = save_plan(plan, out)
    return [types.TextContent(
        type="text",
        text=json.dumps({"saved_to": str(saved), "package_name": plan.package_name}),
    )]


async def _load_plan(args: dict[str, Any]) -> list[types.TextContent]:
    from .migration_plan import load_plan

    p = _safe_resolve(args["path"], must_exist=True, label="path")
    plan = load_plan(p)
    return [types.TextContent(type="text", text=json.dumps({
        "plan": plan.model_dump(mode="json"),
        "markdown": plan.render_markdown(),
    }, indent=2, default=str))]


async def _provision_adf_env(args: dict[str, Any]) -> list[types.TextContent]:
    from .migration_plan import deploy_bicep, generate_bicep, load_plan
    from .migration_plan.provisioner import _compile_bicep

    plan_path = _safe_resolve(args["plan_path"], must_exist=True, label="plan_path")
    plan = load_plan(plan_path)
    name_prefix = args.get("name_prefix", "ssismig")
    bicep = generate_bicep(
        plan,
        name_prefix=name_prefix,
        observability_workspace_id=args.get("with_observability"),
    )
    dry_run = bool(args.get("dry_run", False))
    subscription_id = args.get("subscription_id")
    resource_group = args.get("resource_group")

    # Always write the Bicep first so the user has something to inspect
    # regardless of whether the Azure round-trip succeeds.
    bicep_saved_to: str | None = None
    if args.get("output_bicep_path"):
        out = _safe_resolve(args["output_bicep_path"], label="output_bicep_path")
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(bicep, encoding="utf-8")
        bicep_saved_to = str(out)

    payload: dict[str, Any] = {
        "plan_package": plan.package_name,
        "name_prefix": name_prefix,
        "bicep_lines": len(bicep.splitlines()),
    }
    if bicep_saved_to:
        payload["bicep_saved_to"] = bicep_saved_to

    # Live deployment requires both Azure identifiers.
    azure_required = not dry_run
    if azure_required and (not subscription_id or not resource_group):
        raise ValueError(
            "subscription_id and resource_group are required unless dry_run=true."
        )

    # Offline dry run: compile Bicep locally and return without touching Azure.
    if dry_run and (not subscription_id or not resource_group):
        if bicep_saved_to is None:
            # Need a file on disk so `az bicep build` can compile it.
            tmp_dir = Path(tempfile.mkdtemp(prefix="ssis_adf_bicep_"))
            tmp_bicep = tmp_dir / "main.bicep"
            tmp_bicep.write_text(bicep, encoding="utf-8")
        else:
            tmp_bicep = Path(bicep_saved_to)
        try:
            _compile_bicep(tmp_bicep)
            payload["status"] = "bicep_compiled"
            payload["mode"] = "offline_dry_run"
        except RuntimeError as exc:
            payload["status"] = "bicep_compile_failed"
            payload["mode"] = "offline_dry_run"
            payload["details"] = str(exc)
        return [types.TextContent(type="text", text=json.dumps(payload, indent=2, default=str))]

    # Online path (live deploy or dry_run with credentials).
    result = deploy_bicep(
        bicep_source=bicep,
        subscription_id=subscription_id,
        resource_group=resource_group,
        deployment_name=args.get("deployment_name", "ssis-migration-copilot"),
        parameters={"prefix": name_prefix},
        dry_run=dry_run,
    )
    payload.update(result)
    return [types.TextContent(type="text", text=json.dumps(payload, indent=2, default=str))]


async def _bulk_analyze(args: dict[str, Any]) -> list[types.TextContent]:
    from .analyzers.complexity_scorer import score_package
    from .analyzers.gap_analyzer import analyze_gaps
    from .migration_plan import propose_design
    from .parsers.models import ConnectionManagerType
    from .parsers.readers.local_reader import LocalReader

    source_path = _safe_resolve(args["source_path"], must_exist=True, label="source_path")
    recursive = args.get("recursive", True)

    if source_path.is_file():
        files = [source_path] if source_path.suffix.lower() == ".dtsx" else []
    else:
        pattern = "**/*.dtsx" if recursive else "*.dtsx"
        files = sorted(source_path.glob(pattern))

    reader = LocalReader()
    rows: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []
    parsed_packages: list[Any] = []  # Retained for cross-package consolidation analysis.
    # Per-project-directory grouping for shared-infra detection. Key is the
    # parent directory path (where Project.params would live); value tracks
    # which packages share it and what credentials/connections recur.
    projects: dict[str, dict[str, Any]] = {}

    for f in files:
        try:
            pkg = reader.read(f)
            parsed_packages.append(pkg)
            score = score_package(pkg).score
            gaps = analyze_gaps(pkg)
            plan = propose_design(pkg)
            project_dir = str(f.parent)
            has_proj_params = (f.parent / "Project.params").exists()
            sensitive_param_names = sorted({
                p.name for p in (pkg.project_parameters or []) if p.sensitive
            })
            # Only count SQL-flavored connection managers; Excel/Flat File/FTP
            # connections also populate `cm.server` with a file path or URL,
            # which is not what "shared on-prem SQL server" means.
            sql_cm_types = {
                ConnectionManagerType.OLEDB,
                ConnectionManagerType.ADO_NET,
                ConnectionManagerType.ODBC,
            }
            sql_servers = sorted({
                cm.server for cm in pkg.connection_managers
                if cm.type in sql_cm_types
                and cm.server
                and "database.windows.net" not in cm.server.lower()
            })
            rows.append({
                "package_name": pkg.name,
                "path": str(f),
                "project_dir": project_dir,
                "has_project_params": has_proj_params,
                "complexity_score": score,
                "complexity_bucket": plan.effort.bucket,
                "target_pattern": plan.target_pattern.value,
                "task_count": sum(plan.reasoning_input.get("task_counts", {}).values()),
                "manual_required_count": sum(1 for g in gaps if g.severity.value == "manual_required"),
                "warning_count": sum(1 for g in gaps if g.severity.value == "warning"),
                "risk_count": len(plan.risks),
                "high_risks": [r.message for r in plan.risks if r.severity.value == "high"],
                "simplifications_recommended": [s.action.value for s in plan.simplifications],
                "estimated_total_hours": plan.effort.total_hours,
                "sensitive_project_params": sensitive_param_names,
            })
            proj = projects.setdefault(project_dir, {
                "project_dir": project_dir,
                "has_project_params": has_proj_params,
                "package_names": [],
                "shared_sensitive_params": set(),
                "shared_sql_servers": set(),
            })
            proj["package_names"].append(pkg.name)
            proj["shared_sensitive_params"].update(sensitive_param_names)
            proj["shared_sql_servers"].update(sql_servers)
        except Exception as exc:
            failures.append({"path": str(f), "error": str(exc)})

    # Estate roll-up
    by_bucket: dict[str, int] = {"low": 0, "medium": 0, "high": 0, "very_high": 0}
    by_pattern: dict[str, int] = {}
    total_hours = 0.0
    total_manual = 0
    for r in rows:
        by_bucket[r["complexity_bucket"]] = by_bucket.get(r["complexity_bucket"], 0) + 1
        by_pattern[r["target_pattern"]] = by_pattern.get(r["target_pattern"], 0) + 1
        total_hours += r["estimated_total_hours"]
        total_manual += r["manual_required_count"]

    # Project-level groupings + shared-infra recommendations.
    project_summaries: list[dict[str, Any]] = []
    shared_infra_recs: list[dict[str, Any]] = []
    for proj_dir, proj in projects.items():
        pkg_count = len(proj["package_names"])
        proj_record = {
            "project_dir": proj_dir,
            "has_project_params": proj["has_project_params"],
            "package_count": pkg_count,
            "package_names": sorted(proj["package_names"]),
            "shared_sensitive_params": sorted(proj["shared_sensitive_params"]),
            "shared_on_prem_sql_servers": sorted(proj["shared_sql_servers"]),
        }
        project_summaries.append(proj_record)
        if pkg_count >= 2 and proj["has_project_params"]:
            shared_infra_recs.append({
                "project_dir": proj_dir,
                "package_count": pkg_count,
                "recommendation": (
                    f"{pkg_count} packages share a Project.params file. Provision ONE "
                    "Key Vault and ONE Storage Account / ADF for the project rather than "
                    "per-package resources."
                ),
                "shared_secrets": sorted(proj["shared_sensitive_params"]),
            })
        if pkg_count >= 2 and proj["shared_sql_servers"]:
            shared_infra_recs.append({
                "project_dir": proj_dir,
                "package_count": pkg_count,
                "recommendation": (
                    f"{pkg_count} packages connect to the same on-prem SQL server(s). "
                    "Provision a single Self-Hosted Integration Runtime and reuse it "
                    "across the project's linked services."
                ),
                "shared_on_prem_sql_servers": sorted(proj["shared_sql_servers"]),
            })

    # Estate-level deduplication & consolidation findings.
    from .analyzers.consolidation_analyzer import analyze_estate_consolidation
    consolidation = analyze_estate_consolidation(parsed_packages)

    report = {
        "scanned_path": str(source_path),
        "package_count": len(rows),
        "failure_count": len(failures),
        "estate_summary": {
            "by_complexity_bucket": by_bucket,
            "by_target_pattern": by_pattern,
            "estimated_total_hours": round(total_hours, 1),
            "manual_required_total": total_manual,
            "bulk_convertible_count": by_bucket.get("low", 0) + by_bucket.get("medium", 0),
            "needs_design_review_count": by_bucket.get("high", 0) + by_bucket.get("very_high", 0),
            "project_count": len(projects),
            "shared_infra_recommendations": shared_infra_recs,
            "deduplication_hours_saved": consolidation["deduplication"]["total_hours_saved"],
            "consolidation_potential_hours_saved": consolidation["consolidation"]["potential_hours_saved"],
        },
        "consolidation": consolidation,
        "projects": sorted(project_summaries, key=lambda p: -p["package_count"]),
        "packages": sorted(rows, key=lambda r: -r["complexity_score"]),
        "failures": failures,
    }

    if args.get("output_path"):
        out = _safe_resolve(args["output_path"], label="output_path")
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
        report["saved_to"] = str(out)

    return [types.TextContent(type="text", text=json.dumps(report, indent=2, default=str))]


async def _smoke_test_pipeline(args: dict[str, Any]) -> list[types.TextContent]:
    from .migration_plan import smoke_test_pipeline

    result = smoke_test_pipeline(
        subscription_id=args["subscription_id"],
        resource_group=args["resource_group"],
        factory_name=args["factory_name"],
        pipeline_name=args["pipeline_name"],
        parameters=args.get("parameters"),
        timeout_seconds=int(args.get("timeout_seconds", 600)),
        poll_interval_seconds=int(args.get("poll_interval_seconds", 10)),
    )
    return [types.TextContent(type="text", text=json.dumps(result, indent=2, default=str))]


async def _convert_estate(args: dict[str, Any]) -> list[types.TextContent]:
    from .migration_plan import propose_design, save_plan
    from .parsers.readers.local_reader import LocalReader

    source_path = _safe_resolve(args["source_path"], must_exist=True, label="source_path")
    output_dir = _safe_resolve(args["output_dir"], label="output_dir")
    output_dir.mkdir(parents=True, exist_ok=True)
    recursive = args.get("recursive", True)
    save_plans = args.get("save_plans", True)
    generate_trigger = args.get("generate_trigger", True)
    shared_artifacts_dir = args.get("shared_artifacts_dir")

    if source_path.is_file():
        files = [source_path] if source_path.suffix.lower() == ".dtsx" else []
    else:
        pattern = "**/*.dtsx" if recursive else "*.dtsx"
        files = sorted(source_path.glob(pattern))

    reader = LocalReader()
    results: list[dict[str, Any]] = []
    for f in files:
        pkg_dir_name = f.stem.replace(" ", "_")
        pkg_output = output_dir / pkg_dir_name
        try:
            pkg = reader.read(f)
            plan = propose_design(pkg)
            plan_path: Path | None = None
            if save_plans:
                pkg_output.mkdir(parents=True, exist_ok=True)
                plan_path = pkg_output / "migration_plan.json"
                save_plan(plan, plan_path)
            convert_args: dict[str, Any] = {
                "package_path": str(f),
                "output_dir": str(pkg_output),
                "generate_trigger": generate_trigger,
            }
            if shared_artifacts_dir:
                convert_args["shared_artifacts_dir"] = shared_artifacts_dir
            if plan_path is not None:
                convert_args["design_path"] = str(plan_path)
            convert_result = await _convert(convert_args)
            # _convert returns the converted summary as TextContent JSON
            convert_payload = json.loads(convert_result[0].text)
            results.append({
                "package_name": pkg.name,
                "source_path": str(f),
                "output_dir": str(pkg_output),
                "plan_path": str(plan_path) if plan_path else None,
                "status": "succeeded",
                "convert_summary": {
                    k: convert_payload.get(k)
                    for k in ("pipeline", "linked_services", "datasets", "data_flows", "triggers", "stubs")
                    if k in convert_payload
                },
            })
        except Exception as exc:
            logger.exception("convert_estate failed for %s", f)
            results.append({
                "package_name": f.stem,
                "source_path": str(f),
                "output_dir": str(pkg_output),
                "status": "failed",
                "error": str(exc),
            })

    succeeded = sum(1 for r in results if r["status"] == "succeeded")
    summary = {
        "scanned_path": str(source_path),
        "output_dir": str(output_dir),
        "package_count": len(results),
        "succeeded_count": succeeded,
        "failed_count": len(results) - succeeded,
        "packages": results,
    }
    return [types.TextContent(type="text", text=json.dumps(summary, indent=2, default=str))]


async def _edit_plan(args: dict[str, Any]) -> list[types.TextContent]:
    from .migration_plan import edit_migration_plan, load_plan, save_plan

    plan_path = _safe_resolve(args["plan_path"], must_exist=True, label="plan_path")
    edits = args.get("edits") or {}
    if not isinstance(edits, dict):
        return [types.TextContent(type="text", text="Error: 'edits' must be an object")]

    plan = load_plan(plan_path)
    new_plan = edit_migration_plan(plan, edits)
    output_path = _safe_resolve(args["output_path"], label="output_path") if args.get("output_path") else plan_path
    save_plan(new_plan, output_path)
    return [types.TextContent(type="text", text=json.dumps({
        "plan_path": str(plan_path),
        "saved_to": str(output_path),
        "applied_edits": list(edits.keys()),
        "plan": new_plan.model_dump(mode="json"),
    }, indent=2, default=str))]


def _load_plans_from_dir(plans_dir: str) -> list:
    """Load all migration plan JSON files from a directory as MigrationPlan instances.

    Accepts either flat layouts (``<plans_dir>/<pkg>_plan.json``) or the nested
    layout produced by ``convert_estate`` (``<plans_dir>/<pkg>/migration_plan.json``).
    """
    from .migration_plan import MigrationPlan

    dir_path = _safe_resolve(plans_dir, must_exist=True, label="plans_dir")
    plan_files = sorted(dir_path.glob("*_plan.json"))
    if not plan_files:
        plan_files = sorted(dir_path.glob("*/migration_plan.json"))
    if not plan_files:
        raise FileNotFoundError(
            f"No migration plan files found in {dir_path}. Looked for "
            "'*_plan.json' (flat) and '*/migration_plan.json' (convert_estate "
            "layout). Run propose_adf_design + save_migration_plan, or "
            "convert_estate with save_plans=True first."
        )
    plans = []
    for f in plan_files:
        data = json.loads(f.read_text(encoding="utf-8"))
        plans.append(MigrationPlan.model_validate(data))
    return plans


async def _plan_waves(args: dict[str, Any]) -> list[types.TextContent]:
    from .migration_plan import MigrationPlan, plan_migration_waves

    plans = _load_plans_from_dir(args["plans_dir"])
    waves = plan_migration_waves(
        plans,
        max_packages_per_wave=int(args.get("max_packages_per_wave", 10)),
        estate_setup_hours=float(args.get("estate_setup_hours", 24)),
        apply_learning_curve=bool(args.get("apply_learning_curve", True)),
    )
    if args.get("output_path"):
        out = _safe_resolve(args["output_path"], label="output_path")
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(waves, indent=2, default=str), encoding="utf-8")
        waves["saved_to"] = str(out)
    return [types.TextContent(type="text", text=json.dumps(waves, indent=2, default=str))]


async def _estimate_costs(args: dict[str, Any]) -> list[types.TextContent]:
    from .migration_plan import estimate_adf_costs

    plans = _load_plans_from_dir(args["plans_dir"])
    estimate = estimate_adf_costs(
        plans=plans,
        runs_per_day=int(args.get("runs_per_day", 1)),
        avg_copy_diu=float(args.get("avg_copy_diu", 4.0)),
        avg_copy_minutes=float(args.get("avg_copy_minutes", 5.0)),
        avg_dataflow_minutes=float(args.get("avg_dataflow_minutes", 10.0)),
        avg_dataflow_vcores=int(args.get("avg_dataflow_vcores", 8)),
        storage_gb=float(args.get("storage_gb", 100.0)),
        rates=args.get("rates"),
    )
    if args.get("output_path"):
        out = _safe_resolve(args["output_path"], label="output_path")
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(estimate, indent=2, default=str), encoding="utf-8")
        estimate["saved_to"] = str(out)
    return [types.TextContent(type="text", text=json.dumps(estimate, indent=2, default=str))]


async def _build_estate_pdf(args: dict[str, Any]) -> list[types.TextContent]:
    from .documentation.estate_pdf_report import build_estate_report_pdf
    from .migration_plan import MigrationPlan, estimate_adf_costs, plan_migration_waves
    from .migration_plan.estate_tools import _plan_to_pkg_summary

    plans = _load_plans_from_dir(args["plans_dir"])
    output_pdf = _safe_resolve(args["output_pdf"], label="output_pdf")

    # Build an estate_report dict from plans for the PDF generator
    packages = [_plan_to_pkg_summary(p) for p in plans]
    by_bucket: dict[str, int] = {}
    by_pattern: dict[str, int] = {}
    for pkg in packages:
        bucket = pkg["complexity_bucket"]
        by_bucket[bucket] = by_bucket.get(bucket, 0) + 1
        pattern = pkg["target_pattern"]
        by_pattern[pattern] = by_pattern.get(pattern, 0) + 1
    total_hours = round(sum(p["estimated_total_hours"] for p in packages), 1)
    total_low = round(sum(p.get("estimated_low_hours", 0) for p in packages), 1)
    total_high = round(sum(p.get("estimated_high_hours", 0) for p in packages), 1)
    total_saved = round(sum(p.get("mcp_automated_hours_saved", 0) for p in packages), 1)
    bulk_count = sum(v for k, v in by_bucket.items() if k in ("low", "medium"))
    review_count = sum(v for k, v in by_bucket.items() if k in ("high", "very_high"))
    estate_report = {
        "package_count": len(plans),
        "failure_count": 0,
        "estate_summary": {
            "by_complexity_bucket": by_bucket,
            "by_target_pattern": by_pattern,
            "estimated_total_hours": total_hours,
            "estimated_low_hours": total_low,
            "estimated_high_hours": total_high,
            "mcp_automated_hours_saved": total_saved,
            "manual_required_total": 0,
            "bulk_convertible_count": bulk_count,
            "needs_design_review_count": review_count,
        },
        "packages": packages,
        "failures": [],
    }

    # Re-parse the source packages so we can compute estate-level dedup +
    # consolidation findings.  Failure to read any individual file is non-fatal
    # — we just skip that one and still produce the report.
    consolidation = None
    try:
        from .analyzers.consolidation_analyzer import analyze_estate_consolidation
        from .parsers.readers.local_reader import LocalReader

        reader = LocalReader()
        parsed = []
        for plan in plans:
            try:
                parsed.append(reader.read(plan.package_path))
            except Exception:
                continue
        if parsed:
            consolidation = analyze_estate_consolidation(parsed)
            estate_report["consolidation"] = consolidation
            estate_report["estate_summary"]["deduplication_hours_saved"] = (
                consolidation["deduplication"]["total_hours_saved"]
            )
            estate_report["estate_summary"]["consolidation_potential_hours_saved"] = (
                consolidation["consolidation"]["potential_hours_saved"]
            )
    except Exception as exc:
        logger.warning("Consolidation analysis skipped: %s", exc)

    waves = None
    if args.get("waves_path"):
        waves_path = _safe_resolve(args["waves_path"], must_exist=True, label="waves_path")
        waves = json.loads(waves_path.read_text(encoding="utf-8"))
    else:
        waves = plan_migration_waves(
            plans,
            max_packages_per_wave=int(args.get("max_packages_per_wave", 10)),
            estate_setup_hours=float(args.get("estate_setup_hours", 24)),
            apply_learning_curve=bool(args.get("apply_learning_curve", True)),
        )

    cost_estimate = None
    if args.get("cost_estimate_path"):
        cost_path = _safe_resolve(args["cost_estimate_path"], must_exist=True, label="cost_estimate_path")
        cost_estimate = json.loads(cost_path.read_text(encoding="utf-8"))
    else:
        cost_estimate = estimate_adf_costs(
            plans=plans,
            runs_per_day=int(args.get("runs_per_day", 1)),
        )

    pdf_path = build_estate_report_pdf(
        output_pdf=output_pdf,
        estate_report=estate_report,
        waves=waves,
        cost_estimate=cost_estimate,
        customer_name=args.get("customer_name"),
    )
    return [types.TextContent(type="text", text=json.dumps({
        "pdf_path": pdf_path,
        "package_count": len(plans),
        "wave_count": (waves or {}).get("wave_count"),
        "monthly_total_usd": (cost_estimate or {}).get("monthly_total_usd"),
    }, indent=2, default=str))]


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    import asyncio

    async def _run() -> None:
        async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
            await server.run(
                read_stream,
                write_stream,
                server.create_initialization_options(),
            )

    asyncio.run(_run())


async def _build_predeployment_report(args: dict[str, Any]) -> list[types.TextContent]:
    from .documentation.predeployment_report import build_predeployment_report

    entries = args.get("entries", [])
    if not entries:
        return [types.TextContent(type="text", text="Error: 'entries' must be a non-empty list.")]

    # Validate paths
    resolved: list[dict[str, str]] = []
    for entry in entries:
        r: dict[str, str] = {
            "package_path": str(_safe_resolve(entry["package_path"], must_exist=True, label="package_path")),
            "adf_dir": str(_safe_resolve(entry["adf_dir"], must_exist=True, label="adf_dir")),
        }
        if entry.get("plan_path"):
            r["plan_path"] = str(_safe_resolve(entry["plan_path"], must_exist=True, label="plan_path"))
        resolved.append(r)

    report = build_predeployment_report(resolved, title=args.get("title"))

    result: dict[str, Any] = {
        "package_count": len(entries),
    }

    if args.get("output_path"):
        out = _safe_resolve(args["output_path"], label="output_path")
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(report, encoding="utf-8")
        result["saved_to"] = str(out)

    result["report_lines"] = len(report.splitlines())

    return [
        types.TextContent(type="text", text=report),
        types.TextContent(type="text", text=json.dumps(result, indent=2)),
    ]


# ---------------------------------------------------------------------------
# Tool: activate_triggers (H7)
# ---------------------------------------------------------------------------

async def _activate_triggers(args: dict[str, Any]) -> list[types.TextContent]:
    """Bulk-activate ADF triggers. Dry-run by default; see AdfDeployer.activate_triggers."""
    from .deployer.adf_deployer import AdfDeployer

    subscription_id = args["subscription_id"]
    resource_group = args["resource_group"]
    factory_name = args["factory_name"]
    trigger_names = args.get("trigger_names")
    dry_run = bool(args.get("dry_run", True))

    deployer = AdfDeployer(
        subscription_id=subscription_id,
        resource_group=resource_group,
        factory_name=factory_name,
    )

    with WarningsCollector() as wc:
        results = deployer.activate_triggers(names=trigger_names, dry_run=dry_run)

    summary: dict[str, int] = {}
    for row in results:
        summary[row["status"]] = summary.get(row["status"], 0) + 1

    payload: dict[str, Any] = {
        "factory": factory_name,
        "resource_group": resource_group,
        "subscription_id": subscription_id,
        "dry_run": dry_run,
        "trigger_count": len(results),
        "summary": summary,
        "results": results,
        "warnings": [w.model_dump() for w in wc.warnings],
    }
    if dry_run:
        payload["next_step"] = (
            "Re-run with dry_run=false to actually start the triggers reported as "
            "'would_activate'. Triggers reported as 'already_started' will be skipped."
        )
    return [types.TextContent(type="text", text=json.dumps(payload, indent=2, default=str))]


async def _export_arm_template(args: dict[str, Any]) -> list[types.TextContent]:
    """Bundle an ADF artifacts directory into an ARM template (M2)."""
    from .generators.arm_template_generator import export_arm_template

    artifacts_dir = _safe_resolve(args["artifacts_dir"], must_exist=True, label="artifacts_dir")
    output_path = (
        _safe_resolve(args["output_path"], label="output_path")
        if args.get("output_path") else None
    )

    with WarningsCollector() as wc:
        paths = export_arm_template(artifacts_dir, output_path=output_path)

    payload = {
        "template": str(paths["template"]),
        "parameters": str(paths["parameters"]),
        "warnings": [w.model_dump() for w in wc.warnings],
        "next_step": (
            "Edit the parameters file (set factoryName) and deploy with: "
            "az deployment group create --resource-group <rg> "
            "--template-file <template> --parameters @<parameters>"
        ),
    }
    return [types.TextContent(type="text", text=json.dumps(payload, indent=2, default=str))]


async def _smoke_test_wave(args: dict[str, Any]) -> list[types.TextContent]:
    """Run smoke_test_pipeline against many pipelines and aggregate (N1)."""
    from .migration_plan import smoke_test_pipeline

    pipeline_names: list[str] = list(args.get("pipeline_names") or [])
    if not pipeline_names:
        artifacts_dir = args.get("artifacts_dir")
        if not artifacts_dir:
            raise ValueError("Provide either pipeline_names or artifacts_dir.")
        pipelines_dir = _safe_resolve(artifacts_dir, must_exist=True, label="artifacts_dir") / "pipeline"
        if not pipelines_dir.is_dir():
            raise ValueError(f"No pipeline/ subdirectory found under {artifacts_dir}.")
        pipeline_names = sorted(p.stem for p in pipelines_dir.glob("*.json"))
        if not pipeline_names:
            raise ValueError(f"No *.json files found in {pipelines_dir}.")

    stop_on_failure = bool(args.get("stop_on_failure", False))
    parameters = args.get("parameters")
    timeout_seconds = int(args.get("timeout_seconds", 600))
    poll_interval_seconds = int(args.get("poll_interval_seconds", 10))

    results: list[dict[str, Any]] = []
    summary = {"total": len(pipeline_names), "succeeded": 0, "failed": 0, "cancelled": 0, "timed_out": 0, "errored": 0, "skipped": 0}

    for name in pipeline_names:
        if stop_on_failure and (summary["failed"] or summary["cancelled"] or summary["timed_out"] or summary["errored"]):
            results.append({"pipeline_name": name, "status": "skipped", "reason": "stop_on_failure"})
            summary["skipped"] += 1
            continue
        try:
            r = smoke_test_pipeline(
                subscription_id=args["subscription_id"],
                resource_group=args["resource_group"],
                factory_name=args["factory_name"],
                pipeline_name=name,
                parameters=parameters,
                timeout_seconds=timeout_seconds,
                poll_interval_seconds=poll_interval_seconds,
            )
        except Exception as exc:
            results.append({"pipeline_name": name, "status": "errored", "error": str(exc)})
            summary["errored"] += 1
            continue
        status = str(r.get("status", "")).lower()
        if status == "succeeded":
            summary["succeeded"] += 1
        elif status == "failed":
            summary["failed"] += 1
        elif status == "cancelled":
            summary["cancelled"] += 1
        elif status in {"timed_out", "timedout"}:
            summary["timed_out"] += 1
        else:
            summary["errored"] += 1
        results.append(r)

    payload = {"summary": summary, "results": results}
    return [types.TextContent(type="text", text=json.dumps(payload, indent=2, default=str))]


async def _compare_dataflow_output(args: dict[str, Any]) -> list[types.TextContent]:
    """Behavioral parity: row+column diff of SSIS DFT vs converted MDF (P4-1)."""
    from .parity import (
        AdfDebugRunner,
        CapturedOutputRunner,
        DtexecRunner,
        compare_dataflow_output,
        render_diff_markdown,
    )

    package_path = _safe_resolve(args["package_path"], must_exist=True, label="package_path")
    adf_dataflow_path = _safe_resolve(args["adf_dataflow_path"], must_exist=True, label="adf_dataflow_path")
    input_dataset_path = _safe_resolve(args["input_dataset_path"], must_exist=True, label="input_dataset_path")
    key_columns = list(args["key_columns"])
    if not key_columns:
        raise ValueError("key_columns must contain at least one column.")

    mode = args.get("mode", "captured")
    ssis_mode = args.get("ssis_mode") or ("live" if mode == "live" else "captured")
    adf_mode = args.get("adf_mode") or ("live" if mode == "live" else "captured")

    if ssis_mode == "captured":
        if not args.get("ssis_captured_csv"):
            raise ValueError("ssis_captured_csv is required when ssis side is captured.")
        ssis_csv = _safe_resolve(args["ssis_captured_csv"], must_exist=True, label="ssis_captured_csv")
        ssis_runner = CapturedOutputRunner(ssis_csv, name="ssis-captured")
    else:
        cfg = args.get("dtexec") or {}
        if not cfg.get("source_connection_path") or not cfg.get("destination_connection_path"):
            raise ValueError(
                "dtexec.source_connection_path and dtexec.destination_connection_path are required for live SSIS mode."
            )
        ssis_runner = DtexecRunner(
            source_connection_path=cfg["source_connection_path"],
            destination_connection_path=cfg["destination_connection_path"],
            destination_filename=cfg.get("destination_filename", "adf_parity_dest.csv"),
            dtexec_path=cfg.get("dtexec_path"),
            timeout_seconds=int(cfg.get("timeout_seconds", 600)),
        )

    if adf_mode == "captured":
        if not args.get("adf_captured_csv"):
            raise ValueError("adf_captured_csv is required when adf side is captured.")
        adf_csv = _safe_resolve(args["adf_captured_csv"], must_exist=True, label="adf_captured_csv")
        adf_runner = CapturedOutputRunner(adf_csv, name="adf-captured")
    else:
        cfg = args.get("adf_debug") or {}
        for required in ("subscription_id", "resource_group", "factory_name"):
            if not cfg.get(required):
                raise ValueError(f"adf_debug.{required} is required for live ADF mode.")
        from .credential import resolve_subscription_id
        adf_runner = AdfDebugRunner(
            subscription_id=resolve_subscription_id(cfg["subscription_id"]),
            resource_group=cfg["resource_group"],
            factory_name=cfg["factory_name"],
            compute_type=cfg.get("compute_type", "General"),
            core_count=int(cfg.get("core_count", 8)),
            time_to_live_minutes=int(cfg.get("time_to_live_minutes", 10)),
            output_stream_name=cfg.get("output_stream_name"),
            row_limit=int(cfg.get("row_limit", 1000)),
        )

    comparison = compare_dataflow_output(
        ssis_runner=ssis_runner,
        adf_runner=adf_runner,
        package_path=package_path,
        dataflow_task_name=args["dataflow_task_name"],
        adf_dataflow_path=adf_dataflow_path,
        input_dataset_path=input_dataset_path,
        key_columns=key_columns,
        compare_columns=args.get("compare_columns"),
        ignore_columns=args.get("ignore_columns") or (),
        ignore_case=bool(args.get("ignore_case", False)),
        strip_whitespace=bool(args.get("strip_whitespace", True)),
        numeric_tolerance=float(args.get("numeric_tolerance", 0.0)),
    )

    payload = comparison.to_dict()
    markdown = render_diff_markdown(payload)

    if args.get("report_path"):
        out = _safe_resolve(args["report_path"], label="report_path")
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(markdown, encoding="utf-8")
        payload["report_path"] = str(out)
    if args.get("diff_json_path"):
        out = _safe_resolve(args["diff_json_path"], label="diff_json_path")
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        payload["diff_json_path"] = str(out)

    return [
        types.TextContent(type="text", text=markdown),
        types.TextContent(type="text", text=json.dumps(payload, indent=2, default=str)),
    ]


async def _upload_encrypted_secrets(args: dict[str, Any]) -> list[types.TextContent]:
    """P4-4 — automate Steps 2 + 4 of the encrypted-package recipe."""
    from .deployer.keyvault_uploader import (
        DEFAULT_SECRET_NAME_TEMPLATE,
        process_encrypted_package,
    )

    unprotected = _safe_resolve(args["unprotected_dtsx_path"], label="unprotected_dtsx_path")
    ls_dir = _safe_resolve(args["linked_service_dir"], label="linked_service_dir")
    if not unprotected.is_file():
        return [types.TextContent(type="text", text=f"Unprotected .dtsx not found: {unprotected}")]
    if not ls_dir.is_dir():
        return [types.TextContent(type="text", text=f"Linked-service directory not found: {ls_dir}")]

    report = process_encrypted_package(
        unprotected_dtsx_path=str(unprotected),
        package_name=args["package_name"],
        kv_url=args["kv_url"],
        linked_service_dir=str(ls_dir),
        secret_name_template=args.get("secret_name_template", DEFAULT_SECRET_NAME_TEMPLATE),
        placeholder_template=args.get("placeholder_template", "{cm}-password"),
        dry_run=bool(args.get("dry_run", False)),
        overwrite=bool(args.get("overwrite", False)),
    )
    payload = report.to_dict()

    if args.get("report_path"):
        out = _safe_resolve(args["report_path"], label="report_path")
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        payload["report_path"] = str(out)

    summary_lines = [
        f"upload_encrypted_secrets — {args['package_name']} ({'DRY RUN' if report.dry_run else 'APPLIED'})",
        f"  Vault: {report.kv_url}",
        f"  Secrets uploaded: {len(report.secrets_uploaded)}",
        f"  Secrets skipped:  {len(report.secrets_skipped)}",
        f"  Linked-service files rewritten: {len(report.linked_services_rewritten)} "
        f"({report.rewrite_count} secretName references)",
    ]
    if report.skip_reasons:
        summary_lines.append("Skip reasons:")
        for name, reason in report.skip_reasons.items():
            summary_lines.append(f"  - {name}: {reason}")

    return [
        types.TextContent(type="text", text="\n".join(summary_lines)),
        types.TextContent(type="text", text=json.dumps(payload, indent=2, default=str)),
    ]


async def _compare_estimates_to_actuals(args: dict[str, Any]) -> list[types.TextContent]:
    """P4-5 — join lineage.json + Cost Management actuals into a variance report."""
    from .migration_plan import compare_estimates_to_actuals

    lineage_path = _safe_resolve(args["lineage_path"], label="lineage_path")
    actuals_path = _safe_resolve(args["actuals_path"], label="actuals_path")
    if not lineage_path.is_file():
        return [types.TextContent(type="text", text=f"lineage.json not found: {lineage_path}")]
    if not actuals_path.is_file():
        return [types.TextContent(type="text", text=f"actuals file not found: {actuals_path}")]

    estimate: dict[str, Any] | None = None
    if args.get("estimate_path"):
        estimate_path = _safe_resolve(args["estimate_path"], label="estimate_path")
        if estimate_path.is_file():
            try:
                estimate = json.loads(estimate_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError as exc:
                return [types.TextContent(
                    type="text",
                    text=f"estimate_path is not valid JSON: {exc}",
                )]

    report = compare_estimates_to_actuals(
        lineage_path=str(lineage_path),
        actuals_source=str(actuals_path),
        estimate=estimate,
        period_label=args.get("period_label", ""),
        factory_resource_id=args.get("factory_resource_id"),
    )

    if args.get("report_path"):
        out = _safe_resolve(args["report_path"], label="report_path")
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
        report["report_path"] = str(out)

    pct = report.get("variance_pct")
    pct_str = f"{pct:+.1f}%" if isinstance(pct, (int, float)) else "n/a"
    summary_lines = [
        f"compare_estimates_to_actuals — {report.get('period_label') or '(no period)'}",
        f"  Factory: {report.get('factory_resource_id') or '(unresolved)'}",
        f"  Actuals total:   ${report['actuals_total_usd']:.2f}",
        f"  Estimate (mo):   ${report['estimate_monthly_usd']:.2f}",
        f"  Variance:        ${report['variance_usd']:+.2f} ({pct_str})",
        f"  Per-pipeline rows: {len(report.get('pipelines') or [])} (estimated allocation)",
    ]
    if report.get("notes"):
        summary_lines.append("Notes:")
        for n in report["notes"]:
            summary_lines.append(f"  - {n}")

    return [
        types.TextContent(type="text", text="\n".join(summary_lines)),
        types.TextContent(type="text", text=json.dumps(report, indent=2, default=str)),
    ]


if __name__ == "__main__":
    main()
