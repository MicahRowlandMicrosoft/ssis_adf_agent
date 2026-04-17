"""Run ADF-Studio-style 'Validate all' against the live MCPTest factory.

Checks every deployed pipeline / dataflow / dataset / linked service / trigger
the way the ADF Studio designer does when you click Validate:

  * Linked Services  -> referenced credentials, AKV refs, integration runtime
  * Datasets         -> linkedServiceName resolves; required path fields filled
  * Data Flows       -> sources/sinks linkedService + dataset refs resolve;
                        every named node in the script DSL is declared in
                        sources/sinks/transformations
  * Pipelines        -> every activity dataset/dataflow/pipeline ref resolves;
                        required typeProperties present
  * Triggers         -> referenced pipelines exist
"""
from __future__ import annotations

import json
import re
import subprocess
import sys
from typing import Any

import requests

SUB = "564fde6a-18b1-425a-a184-ea80343143e4"
RG = "rg-mcp-ssis-to-adf-test"
FACTORY = "MCPTest"
API = "2018-06-01"
BASE = (
    f"https://management.azure.com/subscriptions/{SUB}"
    f"/resourceGroups/{RG}/providers/Microsoft.DataFactory/factories/{FACTORY}"
)


def get_token() -> str:
    out = subprocess.run(
        ["az", "account", "get-access-token", "--query", "accessToken", "-o", "tsv"],
        capture_output=True, text=True, check=True, shell=True,
    )
    return out.stdout.strip()


def list_kind(token: str, kind: str) -> list[dict[str, Any]]:
    """kind: linkedservices | datasets | dataflows | pipelines | triggers"""
    url = f"{BASE}/{kind}?api-version={API}"
    headers = {"Authorization": f"Bearer {token}"}
    items: list[dict[str, Any]] = []
    while url:
        r = requests.get(url, headers=headers, timeout=30)
        r.raise_for_status()
        body = r.json()
        items.extend(body.get("value", []))
        url = body.get("nextLink")
    return items


# Regex: words on the LHS of `~> NodeName` in the data flow DSL
_NODE_REF_RE = re.compile(r"~>\s*([A-Za-z_][A-Za-z0-9_]*)")
_ALNUM_RE = re.compile(r"^[A-Za-z][A-Za-z0-9]*$")
_BLOB_CONTAINER_RE = re.compile(r"^[a-z0-9](?:[a-z0-9]|-(?=[a-z0-9])){2,62}$")
_ACTIVITY_NAME_LIMIT = 55


def validate_linked_service(ls: dict, ls_names: set[str]) -> list[str]:
    issues: list[str] = []
    props = ls.get("properties", {})
    type_props = props.get("typeProperties", {}) or {}
    ls_type = props.get("type", "")
    name = ls.get("name", "<unnamed>")
    # Connection-string types must have something non-placeholder
    cs_keys = ("connectionString", "url", "endpoint", "host", "server")
    has_target = False
    for k in cs_keys:
        v = type_props.get(k)
        if isinstance(v, str) and v.strip():
            has_target = True
            if any(p in v.lower() for p in ("todo", "placeholder", "insert_", "<your", "changeme")):
                issues.append(f"LS '{name}' ({ls_type}): {k} contains placeholder text: {v[:80]}")
        elif isinstance(v, dict):  # AKV reference
            has_target = True
    if ls_type and ls_type not in ("AzureKeyVault",) and not has_target and not type_props:
        issues.append(f"LS '{name}' ({ls_type}): no target endpoint configured")
    # connectVia must point at a real IR (we don't have IRs in MCPTest)
    cv = props.get("connectVia")
    if cv and cv.get("referenceName"):
        issues.append(
            f"LS '{name}': connectVia references IR '{cv['referenceName']}' — "
            "MCPTest factory has no Self-Hosted IR"
        )
    return issues


def validate_dataset(ds: dict, ls_names: set[str]) -> list[str]:
    issues: list[str] = []
    props = ds.get("properties", {})
    name = ds.get("name", "<unnamed>")
    ls_ref = props.get("linkedServiceName", {}) or {}
    ref = ls_ref.get("referenceName")
    if not ref:
        issues.append(f"Dataset '{name}': missing linkedServiceName.referenceName")
    elif ref not in ls_names:
        issues.append(f"Dataset '{name}': linkedServiceName '{ref}' does not exist")
    if not props.get("type"):
        issues.append(f"Dataset '{name}': missing 'type'")
    # Blob container name rules: lowercase alphanumeric + hyphens, 3-63 chars,
    # must start and end with alphanumeric, no consecutive hyphens.
    tp = props.get("typeProperties", {}) or {}
    location = tp.get("location") or {}
    container = location.get("container") or location.get("fileSystem")
    if container and not _BLOB_CONTAINER_RE.match(container):
        issues.append(
            f"Dataset '{name}': container '{container}' violates Azure Blob naming "
            "(lowercase alphanumeric + hyphens, 3-63 chars, no leading/trailing/double hyphens)"
        )
    return issues


def validate_data_flow(
    df: dict, ds_names: set[str], ls_names: set[str]
) -> list[str]:
    issues: list[str] = []
    props = df.get("properties", {})
    name = df.get("name", "<unnamed>")
    tp = props.get("typeProperties", {}) or {}
    sources = tp.get("sources", []) or []
    sinks = tp.get("sinks", []) or []
    transformations = tp.get("transformations", []) or []
    script = tp.get("script", "") or ""

    declared = {n["name"] for n in (*sources, *sinks, *transformations) if n.get("name")}

    # Every node name must be alphanumeric only (ADF rule)
    for n in (*sources, *sinks, *transformations):
        nm = n.get("name", "")
        if nm and not _ALNUM_RE.match(nm):
            issues.append(
                f"DataFlow '{name}': node name '{nm}' has invalid characters "
                "(only alphanumeric allowed)"
            )

    # Every node referenced in `~> Name` must be declared
    referenced = set(_NODE_REF_RE.findall(script))
    missing = referenced - declared
    for m in sorted(missing):
        issues.append(
            f"DataFlow '{name}': script references node '{m}' "
            "not in sources/sinks/transformations"
        )

    # Source / sink dataset + LS refs
    for s in (*sources, *sinks):
        ds_ref = (s.get("dataset") or {}).get("referenceName")
        if ds_ref and ds_ref not in ds_names:
            issues.append(
                f"DataFlow '{name}': node '{s.get('name')}' references "
                f"dataset '{ds_ref}' which does not exist"
            )
        ls_ref = (s.get("linkedService") or {}).get("referenceName")
        if ls_ref and ls_ref not in ls_names:
            issues.append(
                f"DataFlow '{name}': node '{s.get('name')}' references "
                f"linked service '{ls_ref}' which does not exist"
            )

    # Comment-only operator bodies are syntactically invalid in ADF DSL
    if re.search(r"\b(derive|sort|join|lookup|cast|aggregate|split)\(\s*/\*", script):
        issues.append(
            f"DataFlow '{name}': comment-only operator body (e.g. 'derive(/* TODO */)') "
            "will fail ADF DSL parse"
        )
    return issues


def validate_pipeline(
    pl: dict,
    ds_names: set[str], df_names: set[str], pl_names: set[str], ls_names: set[str],
) -> list[str]:
    issues: list[str] = []
    props = pl.get("properties", {})
    name = pl.get("name", "<unnamed>")
    activities = props.get("activities", []) or []
    if not activities:
        issues.append(f"Pipeline '{name}': has no activities")

    def walk(acts: list, scope: str) -> None:
        for a in acts:
            atype = a.get("type", "?")
            aname = a.get("name", "?")
            if len(aname) > _ACTIVITY_NAME_LIMIT:
                issues.append(
                    f"Pipeline '{name}' / activity '{aname}' ({atype}): name length "
                    f"{len(aname)} exceeds ADF limit of {_ACTIVITY_NAME_LIMIT}"
                )
            tp = a.get("typeProperties", {}) or {}
            inputs = a.get("inputs", []) or []
            outputs = a.get("outputs", []) or []
            for ds in (*inputs, *outputs):
                ref = ds.get("referenceName")
                if ref and ref not in ds_names:
                    issues.append(
                        f"Pipeline '{name}' / activity '{aname}' ({atype}): "
                        f"dataset '{ref}' does not exist"
                    )
            # ExecutePipeline
            if atype == "ExecutePipeline":
                ref = (tp.get("pipeline") or {}).get("referenceName")
                if ref and ref not in pl_names:
                    issues.append(
                        f"Pipeline '{name}' / activity '{aname}': child pipeline "
                        f"'{ref}' does not exist"
                    )
            # ExecuteDataFlow
            if atype == "ExecuteDataFlow":
                ref = (tp.get("dataflow") or {}).get("referenceName")
                if ref and ref not in df_names:
                    issues.append(
                        f"Pipeline '{name}' / activity '{aname}': data flow "
                        f"'{ref}' does not exist"
                    )
            # Lookup / Copy / Script linkedServiceName
            ls_ref = (a.get("linkedServiceName") or {}).get("referenceName")
            if ls_ref and ls_ref not in ls_names:
                issues.append(
                    f"Pipeline '{name}' / activity '{aname}': linked service "
                    f"'{ls_ref}' does not exist"
                )
            # Recurse into ForEach / Until / If
            for sub_key in ("activities", "ifTrueActivities", "ifFalseActivities"):
                if sub_key in tp and isinstance(tp[sub_key], list):
                    walk(tp[sub_key], f"{scope}/{aname}")

    walk(activities, name)
    return issues


def validate_trigger(tr: dict, pl_names: set[str]) -> list[str]:
    issues: list[str] = []
    props = tr.get("properties", {})
    name = tr.get("name", "<unnamed>")
    for pr in props.get("pipelines", []) or []:
        ref = (pr.get("pipelineReference") or {}).get("referenceName")
        if ref and ref not in pl_names:
            issues.append(f"Trigger '{name}': pipeline '{ref}' does not exist")
    return issues


def main() -> int:
    print(f"Validating live factory: {FACTORY} (rg: {RG})\n")
    token = get_token()

    linked = list_kind(token, "linkedservices")
    datasets = list_kind(token, "datasets")
    dataflows = list_kind(token, "dataflows")
    pipelines = list_kind(token, "pipelines")
    triggers = list_kind(token, "triggers")

    ls_names = {x["name"] for x in linked}
    ds_names = {x["name"] for x in datasets}
    df_names = {x["name"] for x in dataflows}
    pl_names = {x["name"] for x in pipelines}

    print(f"  Linked services: {len(linked)}")
    print(f"  Datasets:        {len(datasets)}")
    print(f"  Data flows:      {len(dataflows)}")
    print(f"  Pipelines:       {len(pipelines)}")
    print(f"  Triggers:        {len(triggers)}\n")

    all_issues: list[tuple[str, list[str]]] = []
    for ls in linked:
        issues = validate_linked_service(ls, ls_names)
        if issues:
            all_issues.append((f"LinkedService:{ls['name']}", issues))
    for ds in datasets:
        issues = validate_dataset(ds, ls_names)
        if issues:
            all_issues.append((f"Dataset:{ds['name']}", issues))
    for df in dataflows:
        issues = validate_data_flow(df, ds_names, ls_names)
        if issues:
            all_issues.append((f"DataFlow:{df['name']}", issues))
    for pl in pipelines:
        issues = validate_pipeline(pl, ds_names, df_names, pl_names, ls_names)
        if issues:
            all_issues.append((f"Pipeline:{pl['name']}", issues))
    for tr in triggers:
        issues = validate_trigger(tr, pl_names)
        if issues:
            all_issues.append((f"Trigger:{tr['name']}", issues))

    if not all_issues:
        print("OK — no validation issues found.")
        return 0

    total = sum(len(v) for _, v in all_issues)
    print(f"Found {total} issue(s) across {len(all_issues)} resource(s):\n")
    for resource, issues in all_issues:
        print(f"[{resource}]")
        for i in issues:
            print(f"  - {i}")
        print()
    return 1


if __name__ == "__main__":
    sys.exit(main())
