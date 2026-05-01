"""
Structural validation of generated Fabric Data Pipelines artifacts.

Mirrors deployer.adf_deployer.validate_artifacts but for the Fabric layout:
    pipeline/
        <PipelineName>.DataPipeline/
            pipeline-content.json
            .platform
    notebook/
        <NotebookName>.Notebook/
            notebook-content.py
            .platform
    connections_required.json

Validation is intentionally lightweight — checks file structure, JSON
parse-ability, schema_version on the manifest, and a few activity-level
sanity checks (no orphaned `__missing_*` markers from the translator).
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _check_pipeline(pipeline_path: Path, errors: list[str], warnings: list[str]) -> None:
    try:
        doc = json.loads(pipeline_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        errors.append(f"{pipeline_path}: invalid JSON ({e})")
        return
    props = doc.get("properties")
    if not isinstance(props, dict):
        errors.append(f"{pipeline_path}: missing top-level 'properties' object")
        return
    activities = props.get("activities")
    if not isinstance(activities, list):
        errors.append(f"{pipeline_path}: 'properties.activities' must be a list")
        return
    _check_activities(activities, pipeline_path, errors, warnings)


def _check_activities(
    activities: list[Any],
    pipeline_path: Path,
    errors: list[str],
    warnings: list[str],
) -> None:
    seen_names: set[str] = set()
    for act in activities:
        if not isinstance(act, dict):
            errors.append(f"{pipeline_path}: non-dict activity entry")
            continue
        name = act.get("name")
        if not isinstance(name, str) or not name:
            errors.append(f"{pipeline_path}: activity missing 'name'")
            continue
        if name in seen_names:
            errors.append(f"{pipeline_path}: duplicate activity name '{name}'")
        seen_names.add(name)
        if "type" not in act:
            errors.append(f"{pipeline_path}: activity '{name}' missing 'type'")

        # Translator markers — these mean a CM or notebook stub couldn't be
        # resolved at convert time. Surface them as warnings, not errors,
        # because the deployer can sometimes still resolve them.
        if "__connection_placeholder_missing" in act:
            warnings.append(
                f"{pipeline_path}: activity '{name}' references unmapped "
                f"linked service '{act['__connection_placeholder_missing']}'"
            )
        if "__notebook_stub_missing" in act:
            warnings.append(
                f"{pipeline_path}: activity '{name}' references missing "
                f"notebook stub for data flow '{act['__notebook_stub_missing']}'"
            )

        # Recurse into nested activity containers
        tp = act.get("typeProperties") or {}
        for k in ("activities", "ifTrueActivities", "ifFalseActivities", "defaultActivities"):
            if isinstance(tp.get(k), list):
                _check_activities(tp[k], pipeline_path, errors, warnings)
        cases = tp.get("cases")
        if isinstance(cases, list):
            for case in cases:
                if isinstance(case, dict) and isinstance(case.get("activities"), list):
                    _check_activities(case["activities"], pipeline_path, errors, warnings)


def _check_connections_manifest(
    path: Path, errors: list[str], warnings: list[str],
) -> None:
    if not path.exists():
        warnings.append(f"{path}: connections_required.json not found")
        return
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        errors.append(f"{path}: invalid JSON ({e})")
        return
    if doc.get("schema_version") != "1.0":
        warnings.append(
            f"{path}: schema_version is '{doc.get('schema_version')}'; "
            "expected '1.0'"
        )
    conns = doc.get("connections")
    if not isinstance(conns, list):
        errors.append(f"{path}: 'connections' must be a list")
        return
    seen: set[str] = set()
    for c in conns:
        if not isinstance(c, dict):
            errors.append(f"{path}: non-dict connection entry")
            continue
        pid = c.get("placeholder_id")
        if not pid:
            errors.append(f"{path}: connection entry missing placeholder_id")
            continue
        if pid in seen:
            errors.append(f"{path}: duplicate placeholder_id '{pid}'")
        seen.add(pid)


def validate_fabric_artifacts(artifacts_dir: Path) -> dict[str, Any]:
    """Validate Fabric artifacts under *artifacts_dir*.

    Returns a dict with `valid` (bool), `errors` (list[str]), `warnings` (list[str]),
    and counts.
    """
    errors: list[str] = []
    warnings: list[str] = []

    if not artifacts_dir.exists():
        return {
            "valid": False,
            "errors": [f"artifacts_dir does not exist: {artifacts_dir}"],
            "warnings": [],
            "pipelines": 0,
            "notebooks": 0,
        }

    pipeline_dirs = sorted((artifacts_dir / "pipeline").glob("*.DataPipeline")) \
        if (artifacts_dir / "pipeline").exists() else []
    notebook_dirs = sorted((artifacts_dir / "notebook").glob("*.Notebook")) \
        if (artifacts_dir / "notebook").exists() else []

    if not pipeline_dirs:
        errors.append(f"{artifacts_dir}: no .DataPipeline directories found under pipeline/")

    for pdir in pipeline_dirs:
        content = pdir / "pipeline-content.json"
        platform = pdir / ".platform"
        if not content.exists():
            errors.append(f"{pdir}: missing pipeline-content.json")
        else:
            _check_pipeline(content, errors, warnings)
        if not platform.exists():
            warnings.append(f"{pdir}: missing .platform sidecar")

    for ndir in notebook_dirs:
        content = ndir / "notebook-content.py"
        platform = ndir / ".platform"
        if not content.exists():
            errors.append(f"{ndir}: missing notebook-content.py")
        if not platform.exists():
            warnings.append(f"{ndir}: missing .platform sidecar")

    _check_connections_manifest(
        artifacts_dir / "connections_required.json", errors, warnings,
    )

    return {
        "valid": not errors,
        "errors": errors,
        "warnings": warnings,
        "pipelines": len(pipeline_dirs),
        "notebooks": len(notebook_dirs),
    }
