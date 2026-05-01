"""
ADF → Fabric pipeline translator.

Converts ADF pipeline / dataset / linked-service JSON dicts (produced by the
existing ADF generators) into Microsoft Fabric Data Pipelines pipeline-content
JSON. Differences handled:

  - ADF wraps activities in {properties: {activities: [...]}}; Fabric uses
    the same wrapper, so we keep it.
  - ADF references linked services by name via `linkedServiceName`. Fabric
    embeds the connection GUID via `externalReferences.connection` on the
    linked-service-shaped block.
  - ADF Copy activities reference datasets by name (`inputs` / `outputs`).
    Fabric inlines the dataset under `source.datasetSettings` /
    `sink.datasetSettings`.
  - ADF Mapping Data Flow (ExecuteDataFlow) has no Fabric equivalent. We
    replace each one with a TridentNotebook activity pointing at a stub
    notebook that an engineer hand-ports.
  - ADF `dependsOn` shape is identical in Fabric — pass through.
"""
from __future__ import annotations

import copy
from typing import Any

from .connection_resolver import ConnectionResolver

# Top-level activity types that Fabric Data Pipelines supports as-is. Anything
# in this set passes through with only the linked-service → connection rewrite
# applied recursively.
_PASS_THROUGH_TYPES = frozenset({
    "SqlServerStoredProcedure",
    "Lookup",
    "Script",
    "ForEach",
    "Until",
    "IfCondition",
    "Switch",
    "ExecutePipeline",
    "WebActivity",
    "AzureFunctionActivity",
    "Wait",
    "SetVariable",
    "AppendVariable",
    "Filter",
    "GetMetadata",
    "Validation",
    "Delete",
    "Fail",
})

# Activity types that have NO Fabric Data Pipelines equivalent. We translate
# them to a Fail activity with a TODO message so the unsupported task is loud
# rather than silent.
_UNSUPPORTED_TYPES = frozenset({
    "AzureMLBatchExecution",
    "AzureMLUpdateResource",
    "DatabricksSparkJar",  # use TridentNotebook instead
})


def _build_ls_to_cm_id(ls_name_map: dict[str, str]) -> dict[str, str]:
    """Invert the cm_id → ls_name map produced by generate_linked_services."""
    return {ls_name: cm_id for cm_id, ls_name in ls_name_map.items()}


def _connection_for_ls(
    ls_name: str,
    ls_to_cm_id: dict[str, str],
    cm_to_placeholder: dict[str, str],
) -> str | None:
    """Look up the placeholder Fabric Connection GUID for an ADF LS name."""
    cm_id = ls_to_cm_id.get(ls_name)
    if not cm_id:
        return None
    return cm_to_placeholder.get(cm_id)


def _rewrite_linked_service_refs(
    obj: Any,
    ls_to_cm_id: dict[str, str],
    cm_to_placeholder: dict[str, str],
) -> Any:
    """Recursively replace `linkedServiceName: {referenceName: X, type: ...}`
    with `externalReferences: {connection: <guid>}` on the *parent* object.

    Returns the modified object (mutated in place; same reference returned for
    convenience).
    """
    if isinstance(obj, dict):
        ls_ref = obj.get("linkedServiceName")
        if isinstance(ls_ref, dict) and "referenceName" in ls_ref:
            ls_name = ls_ref["referenceName"]
            placeholder = _connection_for_ls(ls_name, ls_to_cm_id, cm_to_placeholder)
            if placeholder:
                obj.pop("linkedServiceName", None)
                obj["externalReferences"] = {"connection": placeholder}
            else:
                # Keep the reference but mark it for the deployer
                obj["__connection_placeholder_missing"] = ls_name
        for v in obj.values():
            _rewrite_linked_service_refs(v, ls_to_cm_id, cm_to_placeholder)
    elif isinstance(obj, list):
        for item in obj:
            _rewrite_linked_service_refs(item, ls_to_cm_id, cm_to_placeholder)
    return obj


def _inline_dataset(
    dataset_ref: dict[str, Any] | None,
    datasets_by_name: dict[str, dict[str, Any]],
    ls_to_cm_id: dict[str, str],
    cm_to_placeholder: dict[str, str],
) -> dict[str, Any] | None:
    """Resolve a `{referenceName: X, type: DatasetReference}` to an inline
    Fabric `datasetSettings` block."""
    if not dataset_ref or "referenceName" not in dataset_ref:
        return None
    ds_name = dataset_ref["referenceName"]
    ds = datasets_by_name.get(ds_name)
    if ds is None:
        return {"__missing_dataset": ds_name}
    # The Fabric datasetSettings block mirrors the ADF dataset.properties
    # block but inlines the linked service with externalReferences.
    props = copy.deepcopy(ds.get("properties") or {})
    settings: dict[str, Any] = {
        "type": props.get("type"),
        "annotations": props.get("annotations", []),
        "schema": props.get("schema", []),
        "typeProperties": props.get("typeProperties", {}),
    }
    # Resolve the linked service to a connection placeholder
    ls_ref = props.get("linkedServiceName") or {}
    ls_name = ls_ref.get("referenceName")
    if ls_name:
        placeholder = _connection_for_ls(ls_name, ls_to_cm_id, cm_to_placeholder)
        if placeholder:
            settings["externalReferences"] = {"connection": placeholder}
        else:
            settings["__connection_placeholder_missing"] = ls_name
    return settings


def _translate_copy_activity(
    activity: dict[str, Any],
    datasets_by_name: dict[str, dict[str, Any]],
    ls_to_cm_id: dict[str, str],
    cm_to_placeholder: dict[str, str],
) -> dict[str, Any]:
    """Inline source/sink datasets into a Copy activity."""
    new_act = copy.deepcopy(activity)
    tp = new_act.setdefault("typeProperties", {})

    inputs = new_act.pop("inputs", []) or []
    outputs = new_act.pop("outputs", []) or []

    if inputs:
        src_settings = _inline_dataset(
            inputs[0], datasets_by_name, ls_to_cm_id, cm_to_placeholder,
        )
        if src_settings is not None:
            source = tp.setdefault("source", {})
            source["datasetSettings"] = src_settings
    if outputs:
        sink_settings = _inline_dataset(
            outputs[0], datasets_by_name, ls_to_cm_id, cm_to_placeholder,
        )
        if sink_settings is not None:
            sink = tp.setdefault("sink", {})
            sink["datasetSettings"] = sink_settings

    return new_act


def _translate_executedataflow_to_notebook(
    activity: dict[str, Any],
    notebook_id_by_dataflow_name: dict[str, str],
) -> dict[str, Any]:
    """Replace an ADF ExecuteDataFlow activity with a Fabric TridentNotebook
    activity referencing the hand-port stub.

    The notebook GUID is a placeholder — the deployer substitutes the real
    Fabric notebook item id at deploy time, the same way connections are
    resolved.
    """
    df_ref = (
        (activity.get("typeProperties") or {})
        .get("dataflow", {})
        .get("referenceName")
    )
    notebook_placeholder = notebook_id_by_dataflow_name.get(df_ref or "", "")
    new_act: dict[str, Any] = {
        "name": activity.get("name"),
        "description": (
            (activity.get("description") or "")
            + " [Fabric: hand-port stub — see notebook]"
        ).strip(),
        "type": "TridentNotebook",
        "dependsOn": activity.get("dependsOn", []),
        "policy": activity.get("policy", {}),
        "typeProperties": {
            "notebookId": notebook_placeholder or "00000000-0000-4000-8000-notebookmiss",
            "workspaceId": "00000000-0000-4000-8000-workspaceplc",
            "parameters": {},
        },
    }
    if not notebook_placeholder:
        new_act["__notebook_stub_missing"] = df_ref
    return new_act


def _walk_translate(
    activities: list[dict[str, Any]],
    datasets_by_name: dict[str, dict[str, Any]],
    ls_to_cm_id: dict[str, str],
    cm_to_placeholder: dict[str, str],
    notebook_id_by_dataflow_name: dict[str, str],
) -> list[dict[str, Any]]:
    """Translate a list of ADF activities to Fabric activities."""
    out: list[dict[str, Any]] = []
    for act in activities:
        atype = act.get("type")
        if atype == "Copy":
            translated = _translate_copy_activity(
                act, datasets_by_name, ls_to_cm_id, cm_to_placeholder,
            )
        elif atype == "ExecuteDataFlow":
            translated = _translate_executedataflow_to_notebook(
                act, notebook_id_by_dataflow_name,
            )
        elif atype in _UNSUPPORTED_TYPES:
            translated = {
                "name": act.get("name"),
                "description": f"[FABRIC UNSUPPORTED] Original ADF type: {atype}",
                "type": "Fail",
                "dependsOn": act.get("dependsOn", []),
                "typeProperties": {
                    "message": f"Activity type '{atype}' has no Fabric equivalent.",
                    "errorCode": "FABRIC_UNSUPPORTED_ACTIVITY",
                },
            }
        else:
            translated = copy.deepcopy(act)

        # Recurse into nested activity containers (ForEach / Until / IfCondition / Switch)
        tp = translated.get("typeProperties") or {}
        for nested_key in ("activities", "ifTrueActivities", "ifFalseActivities"):
            if nested_key in tp and isinstance(tp[nested_key], list):
                tp[nested_key] = _walk_translate(
                    tp[nested_key],
                    datasets_by_name,
                    ls_to_cm_id,
                    cm_to_placeholder,
                    notebook_id_by_dataflow_name,
                )
        cases = tp.get("cases")
        if isinstance(cases, list):
            for case in cases:
                if isinstance(case, dict) and isinstance(case.get("activities"), list):
                    case["activities"] = _walk_translate(
                        case["activities"],
                        datasets_by_name,
                        ls_to_cm_id,
                        cm_to_placeholder,
                        notebook_id_by_dataflow_name,
                    )
        if "defaultActivities" in tp and isinstance(tp["defaultActivities"], list):
            tp["defaultActivities"] = _walk_translate(
                tp["defaultActivities"],
                datasets_by_name,
                ls_to_cm_id,
                cm_to_placeholder,
                notebook_id_by_dataflow_name,
            )

        # Rewrite any linkedServiceName references on this activity
        _rewrite_linked_service_refs(translated, ls_to_cm_id, cm_to_placeholder)
        out.append(translated)
    return out


def translate_pipeline(
    adf_pipeline: dict[str, Any],
    datasets: list[dict[str, Any]],
    ls_name_map: dict[str, str],
    resolver: ConnectionResolver,
    notebook_id_by_dataflow_name: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Translate one ADF pipeline dict to a Fabric pipeline-content dict.

    Args:
        adf_pipeline: The ADF pipeline dict (output of generate_pipeline).
        datasets: All ADF dataset dicts from generate_datasets.
        ls_name_map: Map of SSIS CM id → ADF linked-service name (returned
            by generate_linked_services).
        resolver: The ConnectionResolver pre-populated with every CM in the
            package (so the placeholder ids are already minted).
        notebook_id_by_dataflow_name: Map of dataflow name → notebook
            placeholder id, populated by the notebook stub generator.

    Returns:
        A Fabric pipeline-content dict ready to write as `pipeline-content.json`.
    """
    notebook_id_by_dataflow_name = notebook_id_by_dataflow_name or {}
    datasets_by_name = {ds["name"]: ds for ds in datasets if ds.get("name")}
    ls_to_cm_id = _build_ls_to_cm_id(ls_name_map)

    cm_to_placeholder = {
        e.ssis_cm_id: e.placeholder_id
        for e in resolver._entries.values()  # noqa: SLF001 — internal helper
    }

    src_props = adf_pipeline.get("properties") or {}
    src_activities = src_props.get("activities") or []
    new_activities = _walk_translate(
        src_activities,
        datasets_by_name,
        ls_to_cm_id,
        cm_to_placeholder,
        notebook_id_by_dataflow_name,
    )

    # Drop ADF-only annotations and add a Fabric source-of-record annotation.
    annotations = list(src_props.get("annotations") or [])
    annotations.append("fabric-target")

    return {
        "properties": {
            "activities": new_activities,
            "parameters": src_props.get("parameters", {}),
            "variables": src_props.get("variables", {}),
            "annotations": annotations,
        },
    }
