"""
ARM-template export of ADF content (M2).

`deploy_to_adf` is the supported, idempotent path for pushing artifacts into
a factory. But some customers run their entire estate through ARM/azd and
don't want a separate deployment story for ADF content. This generator
packs an existing ADF artifacts directory into a single ARM template that
declares every linked service / dataset / data flow / pipeline / trigger as
a child resource of an existing Microsoft.DataFactory/factories resource.

Output: ``adf_content.arm.json`` next to the artifacts directory, plus a
``adf_content.parameters.json`` skeleton.

Constraints baked in (intentionally conservative):

* The factory itself is **not** declared — we assume it already exists
  (created by infra/main.bicep). The template just adds content under it.
* Triggers are emitted with ``runtimeState: Stopped`` to match deploy_to_adf.
* Inter-resource dependsOn chains follow the same order deploy_to_adf uses:
  linkedServices -> datasets -> dataflows -> pipelines -> triggers.
* No managed-identity wiring is invented; if the source linked services
  already use ManagedIdentity auth, that flows through verbatim.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

# ARM resource type names for ADF child resources.
_TYPE_BY_DIR = {
    "linkedService": "Microsoft.DataFactory/factories/linkedservices",
    "dataset":       "Microsoft.DataFactory/factories/datasets",
    "dataflow":      "Microsoft.DataFactory/factories/dataflows",
    "pipeline":      "Microsoft.DataFactory/factories/pipelines",
    "trigger":       "Microsoft.DataFactory/factories/triggers",
}

# Deploy order = depends-on order. Each type depends on every artifact in
# the previous types so ARM serialises correctly.
_DEPLOY_ORDER = ["linkedService", "dataset", "dataflow", "pipeline", "trigger"]


def _load_artifact(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _list_for(artifacts_dir: Path, sub: str) -> list[Path]:
    d = artifacts_dir / sub
    if not d.exists():
        return []
    return sorted(d.glob("*.json"))


def _resource_id_expr(type_full: str, name: str) -> str:
    """ARM resourceId() expression for a child of the parameterised factory."""
    return (
        f"[resourceId('{type_full}', "
        f"parameters('factoryName'), '{name}')]"
    )


def export_arm_template(
    artifacts_dir: Path,
    *,
    output_path: Path | None = None,
    api_version: str = "2018-06-01",
) -> dict[str, Path]:
    """
    Build an ARM template wrapping every ADF artifact in ``artifacts_dir``.

    Returns a dict ``{"template": <path>, "parameters": <path>}`` pointing
    at the generated files.
    """
    output_path = output_path or (artifacts_dir / "adf_content.arm.json")
    params_path = output_path.with_name("adf_content.parameters.json")

    resources: list[dict[str, Any]] = []
    # Track names already declared per-type so dependsOn references are valid.
    declared_by_type: dict[str, list[str]] = {t: [] for t in _DEPLOY_ORDER}

    for sub in _DEPLOY_ORDER:
        type_full = _TYPE_BY_DIR[sub]
        for art_file in _list_for(artifacts_dir, sub):
            payload = _load_artifact(art_file)
            name = payload.get("name") or art_file.stem
            properties = payload.get("properties", payload)

            # Triggers explicitly Stopped — same convention as deploy_to_adf.
            if sub == "trigger":
                properties = dict(properties)
                properties.setdefault("runtimeState", "Stopped")

            depends_on: list[str] = []
            # Depend on every prior-stage resource so ARM serialises correctly.
            prior_idx = _DEPLOY_ORDER.index(sub)
            for prior_sub in _DEPLOY_ORDER[:prior_idx]:
                prior_type = _TYPE_BY_DIR[prior_sub]
                for prior_name in declared_by_type[prior_sub]:
                    depends_on.append(_resource_id_expr(prior_type, prior_name))

            resources.append({
                "type": type_full,
                "apiVersion": api_version,
                "name": f"[concat(parameters('factoryName'), '/{name}')]",
                "dependsOn": depends_on,
                "properties": properties,
            })
            declared_by_type[sub].append(name)

    template = {
        "$schema": "https://schema.management.azure.com/schemas/2019-04-01/deploymentTemplate.json#",
        "contentVersion": "1.0.0.0",
        "parameters": {
            "factoryName": {
                "type": "string",
                "metadata": {
                    "description": (
                        "Name of the existing Azure Data Factory that the "
                        "content below should be added to. The factory must "
                        "already exist; this template only declares child "
                        "resources."
                    ),
                },
            },
        },
        "variables": {},
        "resources": resources,
        "outputs": {
            "artifactCount": {
                "type": "int",
                "value": len(resources),
            },
        },
    }
    parameters_skeleton = {
        "$schema": "https://schema.management.azure.com/schemas/2019-04-01/deploymentParameters.json#",
        "contentVersion": "1.0.0.0",
        "parameters": {
            "factoryName": {"value": "REPLACE_ME"},
        },
    }

    output_path.write_text(
        json.dumps(template, indent=2, ensure_ascii=False), encoding="utf-8",
    )
    params_path.write_text(
        json.dumps(parameters_skeleton, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return {"template": output_path, "parameters": params_path}
