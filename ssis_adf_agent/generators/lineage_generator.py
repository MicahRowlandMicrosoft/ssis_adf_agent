"""
Lineage manifest generator (M1).

For every conversion we emit ``lineage.json`` next to the ADF artifact tree.
The manifest answers three questions an auditor / SRE will ask:

1. **What .dtsx file did this come from?** — `package.source_file`, hash, parser
   version.
2. **Which SSIS task produced which ADF activity / dataset / linked service?**
   — per-artifact entries with the originating ``task_id`` / ``connection_id``.
3. **Where does it live in Azure once deployed?** — `azure_resource_id` is a
   placeholder until populated by ``deploy_to_adf``; that tool can update the
   same file in place.

The manifest is intentionally a flat JSON document so it can be diffed in CI
and round-tripped by other tooling without requiring this package to be
installed.
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..parsers.models import SSISPackage

logger = logging.getLogger(__name__)

#: Current ``lineage.json`` schema version. Same forward-compat policy as
#: ``MigrationPlan`` (see ``migration_plan.persistence.load_plan``):
#: incompatible *major* version is rejected, unknown *minor* version is
#: accepted with a warning.
LINEAGE_SCHEMA_VERSION = "1.0"

try:
    from .. import __version__ as _agent_version  # type: ignore[attr-defined]
except ImportError:  # not declared in __init__
    _agent_version = "0.0.0+local"


def _sha256_of_file(path: Path) -> str:
    h = hashlib.sha256()
    try:
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
    except OSError:
        return ""
    return h.hexdigest()


def _list_json_artifacts(
    output_dir: Path, artifact_type: str
) -> list[dict[str, str]]:
    sub = output_dir / artifact_type
    if not sub.exists():
        return []
    rows: list[dict[str, str]] = []
    for f in sorted(sub.glob("*.json")):
        try:
            payload = json.loads(f.read_text(encoding="utf-8"))
        except Exception:
            payload = {}
        rows.append({
            "name": payload.get("name", f.stem),
            "file": str(f.relative_to(output_dir)).replace("\\", "/"),
            "azure_resource_id": "",  # populated later by deploy_to_adf
        })
    return rows


def _activity_to_origin(activity: dict[str, Any]) -> dict[str, Any]:
    """Best-effort: derive the SSIS task id/name an activity came from."""
    user_props = (
        activity.get("userProperties")
        or activity.get("user_properties")
        or []
    )
    src_task_id = ""
    src_task_name = ""
    for prop in user_props:
        key = (prop.get("name") or "").lower()
        val = prop.get("value")
        if key in ("ssis_task_id", "source_task_id"):
            src_task_id = str(val or "")
        elif key in ("ssis_task_name", "source_task_name"):
            src_task_name = str(val or "")
    return {
        "adf_activity": activity.get("name", ""),
        "adf_type": activity.get("type", ""),
        "ssis_task_id": src_task_id,
        "ssis_task_name": src_task_name,
    }


def _walk_activities(activities: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Flatten nested ForEach/IfCondition/Until containers."""
    flat: list[dict[str, Any]] = []
    for act in activities or []:
        flat.append(act)
        type_props = act.get("typeProperties") or {}
        for nested_key in ("activities", "ifTrueActivities", "ifFalseActivities"):
            nested = type_props.get(nested_key) or []
            if nested:
                flat.extend(_walk_activities(nested))
    return flat


def generate_lineage_manifest(
    package: SSISPackage,
    output_dir: Path,
    pipeline: dict[str, Any],
    *,
    write: bool = True,
) -> dict[str, Any]:
    """
    Build the lineage manifest for a single converted package and (by default)
    write ``lineage.json`` into ``output_dir``.

    The returned dict is the same content that's written to disk; callers can
    embed it in conversion summaries or post-process it.
    """
    src = Path(package.source_file) if package.source_file else None
    src_hash = _sha256_of_file(src) if (src and src.exists()) else ""

    activities = (
        (pipeline.get("properties") or {}).get("activities") or []
    )
    activity_rows = [
        _activity_to_origin(a) for a in _walk_activities(activities)
    ]

    # Also surface SSIS-side counts so a reader can spot-check coverage.
    ssis_task_count = len(package.tasks)

    manifest: dict[str, Any] = {
        "schema_version": LINEAGE_SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "agent_version": _agent_version,
        "source": {
            "package_name": package.name,
            "package_id": package.id,
            "source_file": package.source_file,
            "sha256": src_hash,
            "protection_level": str(package.protection_level),
            "ssis_top_level_task_count": ssis_task_count,
            "connection_manager_count": len(package.connection_managers),
            "variable_count": len(package.variables),
        },
        "artifacts": {
            "pipeline": [{
                "name": pipeline.get("name", ""),
                "file": f"pipeline/{pipeline.get('name','')}.json",
                "activity_count": len(activity_rows),
                "azure_resource_id": "",
            }],
            "linked_services": _list_json_artifacts(output_dir, "linkedService"),
            "datasets": _list_json_artifacts(output_dir, "dataset"),
            "data_flows": _list_json_artifacts(output_dir, "dataflow"),
            "triggers": _list_json_artifacts(output_dir, "trigger"),
        },
        "activity_origins": activity_rows,
    }

    if write:
        out_path = output_dir / "lineage.json"
        out_path.write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    return manifest


def update_lineage_with_deployment(
    output_dir: Path,
    deploy_results: list[Any],
    *,
    subscription_id: str,
    resource_group: str,
    factory_name: str,
) -> Path | None:
    """
    Patch an existing ``lineage.json`` with Azure resource IDs after a deploy.

    Each ``deploy_results`` item must expose ``artifact_type``, ``name``, and
    ``success`` (DeployResult dataclass). Only successful deploys get an
    ``azure_resource_id`` written.

    Returns the path that was updated, or ``None`` if no manifest is present.
    """
    manifest_path = output_dir / "lineage.json"
    if not manifest_path.exists():
        return None

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    base = (
        f"/subscriptions/{subscription_id}"
        f"/resourceGroups/{resource_group}"
        f"/providers/Microsoft.DataFactory/factories/{factory_name}"
    )
    type_to_segment = {
        "linkedservice": ("linked_services", "linkedservices"),
        "dataset":       ("datasets",        "datasets"),
        "dataflow":      ("data_flows",      "dataflows"),
        "pipeline":      ("pipeline",        "pipelines"),
        "trigger":       ("triggers",        "triggers"),
    }

    deployed_ids: dict[tuple[str, str], str] = {}
    for r in deploy_results:
        if not getattr(r, "success", False):
            continue
        if getattr(r, "skipped", False):
            # Still in the factory under that name — record the resource id so
            # downstream tooling can find it.
            pass
        mapping = type_to_segment.get(str(r.artifact_type).lower())
        if not mapping:
            continue
        _, seg = mapping
        deployed_ids[(str(r.artifact_type).lower(), r.name)] = (
            f"{base}/{seg}/{r.name}"
        )

    artifacts = manifest.get("artifacts", {})
    for type_key, (manifest_key, _) in type_to_segment.items():
        rows = artifacts.get(manifest_key)
        if not rows:
            continue
        # `pipeline` is a single dict-list; handle both shapes uniformly.
        if isinstance(rows, dict):
            rows = [rows]
            artifacts[manifest_key] = rows
        for row in rows:
            rid = deployed_ids.get((type_key, row.get("name", "")))
            if rid:
                row["azure_resource_id"] = rid

    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return manifest_path


def load_lineage(path: str | Path) -> dict[str, Any]:
    """Load a lineage manifest with forward-compatibility checks.

    Mirrors the policy of ``migration_plan.persistence.load_plan``:
    an incompatible *major* schema version is rejected with a clear
    message; an unknown *minor* version is accepted and a warning is
    logged so downstream tooling keeps working across point bumps.
    """
    p = Path(path).expanduser().resolve()
    raw = json.loads(p.read_text(encoding="utf-8"))
    found = str(raw.get("schema_version", "0.0"))
    expected = LINEAGE_SCHEMA_VERSION
    if found.split(".")[0] != expected.split(".")[0]:
        raise ValueError(
            f"Lineage manifest at {p} has incompatible schema_version={found} "
            f"(expected {expected}). Migration required."
        )
    if found != expected:
        logger.warning(
            "Lineage manifest schema_version=%s differs from current %s; "
            "loading anyway (forward-compat).",
            found, expected,
        )
    return raw
