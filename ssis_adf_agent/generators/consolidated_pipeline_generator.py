"""
Consolidated pipeline generator — takes a ``ConsolidationGroup`` and produces:

1. A **child pipeline** — parameterized version of the shared package structure.
   Each varying value (SQL statement, file path, connection string, etc.) becomes
   a pipeline parameter.
2. A **parent pipeline** — contains a ForEach activity that iterates over a JSON
   config array and invokes the child pipeline once per parameter set.

The parent pipeline's ``configItems`` parameter is pre-populated with the actual
values extracted from each source SSIS package, so it's ready to run out of the box.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ..analyzers.similarity_analyzer import ConsolidationGroup
from ..generators.pipeline_generator import generate_pipeline
from ..warnings_collector import warn


def generate_consolidated_pipelines(
    group: ConsolidationGroup,
    output_dir: Path,
    *,
    pipeline_prefix: str = "PL_",
    stubs_dir: Path | None = None,
    llm_translate: bool = False,
    schema_remap: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Generate a parent + child pipeline pair for a consolidation group.

    Args:
        group: A ``ConsolidationGroup`` from the similarity analyzer.
        output_dir: Root output directory for ADF artifacts.
        pipeline_prefix: Prefix for pipeline names.
        stubs_dir: Optional stubs directory for script task conversion.
        llm_translate: Whether to use LLM for C# → Python translation.
        schema_remap: Optional schema remap configuration.

    Returns:
        Summary dict with generated file paths and config.
    """
    # Use the first package as the template for the child pipeline
    if not group.packages:
        warn(
            phase="generate", severity="error",
            source="consolidated_pipeline_generator",
            message="Consolidation group has no packages — cannot generate pipelines",
        )
        return {"error": "Consolidation group has no packages"}
    template_package = group.packages[0]
    param_names = group.shared_parameter_names

    # --- Generate the child pipeline (standard conversion of the template) ---
    child_name = f"{pipeline_prefix}Consolidated_{_safe_group_name(group)}"
    child_pipeline = generate_pipeline(
        template_package,
        output_dir,
        stubs_dir=stubs_dir,
        llm_translate=llm_translate,
        pipeline_prefix="",  # we control the name directly
        schema_remap=schema_remap,
    )

    # Rename the child pipeline
    old_name = child_pipeline["name"]
    child_pipeline["name"] = child_name

    # Rename the file on disk
    old_file = output_dir / "pipeline" / f"{old_name}.json"
    new_file = output_dir / "pipeline" / f"{child_name}.json"
    if old_file.exists():
        old_file.rename(new_file)

    # Inject the varying values as pipeline parameters
    props = child_pipeline.setdefault("properties", {})
    params = props.setdefault("parameters", {})
    for pname in param_names:
        # Determine type from first non-None value
        sample = None
        for ps in group.parameter_sets:
            if ps.values.get(pname) is not None:
                sample = ps.values[pname]
                break
        param_type = _infer_adf_type(sample)
        params[pname] = {
            "type": param_type,
            **({"defaultValue": str(sample)} if sample is not None else {}),
        }

    # Add annotation
    annotations = props.setdefault("annotations", [])
    annotations.append("consolidated-child")
    annotations.append(f"source-packages:{len(group.packages)}")

    # Update description
    source_names = ", ".join(pkg.name for pkg in group.packages)
    props["description"] = (
        f"Consolidated parameterized pipeline generated from {len(group.packages)} "
        f"structurally identical SSIS packages: {source_names}. "
        "Each execution receives its specific values via pipeline parameters."
    )

    # Re-write child pipeline JSON
    new_file.write_text(
        json.dumps(child_pipeline, indent=4, ensure_ascii=False),
        encoding="utf-8",
    )

    # --- Generate the parent pipeline ---
    parent_name = f"{pipeline_prefix}Parent_{_safe_group_name(group)}"
    config_items = _build_config_array(group)
    parent_pipeline = _build_parent_pipeline(
        parent_name, child_name, param_names, config_items, group,
    )

    parent_file = output_dir / "pipeline" / f"{parent_name}.json"
    parent_file.parent.mkdir(parents=True, exist_ok=True)
    parent_file.write_text(
        json.dumps(parent_pipeline, indent=4, ensure_ascii=False),
        encoding="utf-8",
    )

    return {
        "group_name": _safe_group_name(group),
        "child_pipeline": child_name,
        "parent_pipeline": parent_name,
        "packages_consolidated": len(group.packages),
        "source_packages": [pkg.name for pkg in group.packages],
        "parameter_count": len(param_names),
        "parameters": param_names,
        "config_items": config_items,
        "files": {
            "child_pipeline": str(new_file),
            "parent_pipeline": str(parent_file),
        },
    }


# ---------------------------------------------------------------------------
# Parent pipeline builder
# ---------------------------------------------------------------------------

def _build_parent_pipeline(
    parent_name: str,
    child_name: str,
    param_names: list[str],
    config_items: list[dict[str, Any]],
    group: ConsolidationGroup,
) -> dict[str, Any]:
    """Build a parent pipeline with ForEach → ExecutePipeline."""
    # Build the parameter mapping from ForEach item to child pipeline params
    child_params: dict[str, Any] = {}
    for pname in param_names:
        child_params[pname] = {
            "value": f"@item().{pname}",
            "type": "Expression",
        }

    source_names = ", ".join(pkg.name for pkg in group.packages)

    return {
        "name": parent_name,
        "properties": {
            "description": (
                f"Parent orchestrator for {len(group.packages)} consolidated SSIS packages: "
                f"{source_names}. "
                "Iterates over configItems and invokes the child pipeline for each entry."
            ),
            "activities": [
                {
                    "name": "ForEach_ConfigItem",
                    "type": "ForEach",
                    "typeProperties": {
                        "isSequential": False,
                        "items": {
                            "value": "@pipeline().parameters.configItems",
                            "type": "Expression",
                        },
                        "activities": [
                            {
                                "name": f"Execute_{child_name}",
                                "type": "ExecutePipeline",
                                "typeProperties": {
                                    "pipeline": {
                                        "referenceName": child_name,
                                        "type": "PipelineReference",
                                    },
                                    "waitOnCompletion": True,
                                    "parameters": child_params,
                                },
                            },
                        ],
                    },
                },
            ],
            "parameters": {
                "configItems": {
                    "type": "Array",
                    "defaultValue": config_items,
                },
            },
            "variables": {},
            "annotations": [
                "ssis-adf-agent",
                "consolidated-parent",
                f"child-pipeline:{child_name}",
            ],
        },
    }


def _build_config_array(group: ConsolidationGroup) -> list[dict[str, Any]]:
    """Build the JSON config array from the parameter sets."""
    items: list[dict[str, Any]] = []
    for ps in group.parameter_sets:
        item: dict[str, Any] = {"_source_package": ps.package_name}
        for key in group.shared_parameter_names:
            val = ps.values.get(key)
            item[key] = str(val) if val is not None else ""
        items.append(item)
    return items


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_group_name(group: ConsolidationGroup) -> str:
    """Generate a short, safe name for the consolidation group."""
    import re
    # Use the shape summary as a base
    summary = group.fingerprint.shape_summary
    # Take first 40 chars, replace non-alphanum with underscore
    safe = re.sub(r"[^A-Za-z0-9]", "_", summary)[:40].strip("_")
    return safe or "Group"


def _infer_adf_type(value: Any) -> str:
    """Infer ADF parameter type from a Python value."""
    if isinstance(value, bool):
        return "Bool"
    if isinstance(value, int):
        return "Int"
    if isinstance(value, float):
        return "Float"
    return "String"
