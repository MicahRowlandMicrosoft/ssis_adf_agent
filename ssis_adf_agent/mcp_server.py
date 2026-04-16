"""
SSIS → ADF MCP Server.

Exposes five tools to GitHub Copilot (and any MCP-compatible client):

1. scan_ssis_packages      — discover .dtsx files (local / git / sql server)
2. analyze_ssis_package    — complexity + gap analysis of a single package
3. convert_ssis_package    — full conversion of a package to ADF JSON artifacts
4. validate_adf_artifacts  — structural validation of generated artifacts
5. deploy_to_adf           — deploy artifacts to Azure Data Factory

Run as an MCP stdio server::

    python -m ssis_adf_agent.mcp_server

Or via the installed script::

    ssis-adf-agent
"""
from __future__ import annotations

import json
import traceback
from pathlib import Path
from typing import Any

import mcp.server.stdio
import mcp.types as types
from mcp.server import Server

from .warnings_collector import WarningsCollector
from .path_safety import safe_resolve as _safe_resolve

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
                            "Default: false."
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
                        "description": "Default authentication type for Azure SQL linked services. Default: 'SystemAssignedManagedIdentity'.",
                        "enum": ["SystemAssignedManagedIdentity", "SQL", "ServicePrincipal"],
                        "default": "SystemAssignedManagedIdentity",
                    },
                    "use_key_vault": {
                        "type": "boolean",
                        "description": "Use Azure Key Vault secret references for passwords/connection strings. Default: false.",
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
                            "Optional path to a JSON file mapping old schema prefixes to new ones for database consolidation. "
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
                "Triggers are deployed in Stopped state and must be activated manually. "
                "Uses DefaultAzureCredential (az login, managed identity, or service principal env vars)."
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
                        "description": "Azure subscription ID.",
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
                        "description": "If true (default), run structural validation before deploying. Invalid artifacts are skipped and reported.",
                        "default": True,
                    },
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
    ]


# ---------------------------------------------------------------------------
# Tool handlers
# ---------------------------------------------------------------------------

@server.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[types.TextContent]:
    try:
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
        else:
            return [types.TextContent(type="text", text=f"Unknown tool: {name}")]
    except Exception as exc:
        tb = traceback.format_exc()
        return [types.TextContent(type="text", text=f"Error: {exc}\n\n{tb}")]


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
            from .parsers.readers.sql_reader import SqlServerReader
            # Expect path_or_conn to be a pyodbc-style connection string
            # Parse it first to get server/database
            import re
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
    from .parsers.readers.local_reader import LocalReader
    from .analyzers.complexity_scorer import score_package_detailed
    from .analyzers.gap_analyzer import analyze_gaps
    from .analyzers.dependency_graph import build_package_dependency_order
    from .analyzers.cdm_pattern_detector import detect_cdm_patterns
    from .analyzers.esi_reuse_analyzer import analyze_esi_reuse, load_esi_config
    from .analyzers.similarity_analyzer import fingerprint_package

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
    from .parsers.readers.local_reader import LocalReader
    from .generators.pipeline_generator import generate_pipeline
    from .generators.linked_service_generator import generate_linked_services
    from .generators.dataset_generator import generate_datasets
    from .generators.dataflow_generator import generate_data_flows
    from .generators.trigger_generator import generate_triggers
    from .analyzers.cdm_pattern_detector import detect_cdm_patterns
    from .analyzers.esi_reuse_analyzer import analyze_esi_reuse, load_esi_config

    path = _safe_resolve(args["package_path"], must_exist=True, label="package_path")
    output_dir = _safe_resolve(args["output_dir"], label="output_dir")
    gen_trigger = args.get("generate_trigger", True)
    llm_translate = args.get("llm_translate", False)

    # New parameters
    on_prem_ir_name = args.get("on_prem_ir_name", "SelfHostedIR")
    auth_type = args.get("auth_type", "SystemAssignedManagedIdentity")
    use_key_vault = args.get("use_key_vault", False)
    kv_ls_name = args.get("kv_ls_name", "LS_KeyVault")
    kv_url = args.get("kv_url", "https://TODO.vault.azure.net/")
    if use_key_vault and "TODO" in kv_url:
        from .warnings_collector import warn
        warn(
            phase="convert", severity="warning", source="mcp_server",
            message=(
                "Key Vault URL is still the placeholder 'https://TODO.vault.azure.net/'. "
                "Set the kv_url parameter to your actual Azure Key Vault URL."
            ),
        )
    pipeline_prefix = args.get("pipeline_prefix", "PL_")
    shared_artifacts_dir = _safe_resolve(args["shared_artifacts_dir"], label="shared_artifacts_dir") if args.get("shared_artifacts_dir") else None

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

    with WarningsCollector() as wc:
        reader = LocalReader()
        package = reader.read(path)

        stubs_dir = output_dir / "stubs"

        # Run analyzers for annotations
        cdm_gaps = detect_cdm_patterns(package)
        esi_gaps = analyze_esi_reuse(package, esi_config) if esi_config else []

        # Run generators with new parameters
        linked_services = generate_linked_services(
            package, output_dir,
            on_prem_ir_name=on_prem_ir_name,
            auth_type=auth_type,
            use_key_vault=use_key_vault,
            kv_ls_name=kv_ls_name,
            kv_url=kv_url,
            shared_artifacts_dir=shared_artifacts_dir,
        )
        datasets = generate_datasets(
            package, output_dir,
            schema_remap=schema_remap,
            shared_artifacts_dir=shared_artifacts_dir,
        )
        data_flows = generate_data_flows(package, output_dir)
        pipeline = generate_pipeline(
            package, output_dir,
            stubs_dir=stubs_dir,
            llm_translate=llm_translate,
            pipeline_prefix=pipeline_prefix,
            cdm_gaps=cdm_gaps,
            esi_gaps=esi_gaps,
            schema_remap=schema_remap,
        )
        triggers = generate_triggers(package, output_dir) if gen_trigger else []

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

        # Auto-validate generated artifacts
        from .deployer.adf_deployer import AdfDeployer
        deployer = AdfDeployer.__new__(AdfDeployer)
        validation_issues = deployer.validate_artifacts(output_dir)

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
        )

        summary = {
            "total": len(results),
            "succeeded": sum(1 for r in results if r.success),
            "failed": sum(1 for r in results if not r.success),
            "results": [
                {"type": r.artifact_type, "name": r.name, "success": r.success,
                 "error": r.error, "retries": r.retries}
                for r in results
            ],
            "warnings": [w.model_dump() for w in wc.warnings],
        }
    return [types.TextContent(type="text", text=json.dumps(summary, indent=2))]


async def _consolidate(args: dict[str, Any]) -> list[types.TextContent]:
    from .parsers.readers.local_reader import LocalReader
    from .analyzers.similarity_analyzer import group_similar_packages, fingerprint_package
    from .generators.consolidated_pipeline_generator import generate_consolidated_pipelines

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


if __name__ == "__main__":
    main()
