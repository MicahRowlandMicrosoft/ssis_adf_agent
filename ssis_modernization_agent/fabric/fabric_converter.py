"""
Top-level Fabric converter — reads an SSIS package, runs the proven ADF
generators in a temp directory, translates the output to Fabric Data
Pipelines format, and writes the Fabric artifacts to *output_dir*.

Output layout::

    output_dir/
        pipeline/
            <PipelineName>.DataPipeline/
                pipeline-content.json
                .platform
        notebook/
            NB_<Pkg>__<DFT>.Notebook/
                notebook-content.py
                .platform
        connections_required.json
        stubs/                 (Azure Function stubs from Script Tasks —
                                same as ADF target; reused as-is)
"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any

from ..generators.dataflow_generator import generate_data_flows
from ..generators.dataset_generator import generate_datasets
from ..generators.linked_service_generator import generate_linked_services
from ..generators.naming import pl_name as _pl_name
from ..generators.pipeline_generator import generate_pipeline
from ..parsers.models import DataFlowTask, SSISPackage
from .connection_resolver import ConnectionResolver
from .notebook_stub_generator import notebook_display_name, write_notebook_stub
from .pipeline_translator import translate_pipeline


def _collect_ls_references(obj: Any, sink: set[str] | None = None) -> set[str]:
    """Walk *obj* and collect every `linkedServiceName.referenceName` value."""
    if sink is None:
        sink = set()
    if isinstance(obj, dict):
        ls_ref = obj.get("linkedServiceName")
        if isinstance(ls_ref, dict):
            name = ls_ref.get("referenceName")
            if isinstance(name, str):
                sink.add(name)
        for v in obj.values():
            _collect_ls_references(v, sink)
    elif isinstance(obj, list):
        for item in obj:
            _collect_ls_references(item, sink)
    return sink


def _platform_metadata_pipeline(name: str, package_name: str) -> dict[str, Any]:
    return {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
        "metadata": {
            "type": "DataPipeline",
            "displayName": name,
            "description": (
                f"Generated from SSIS package '{package_name}' by "
                "ssis-modernization-agent (Fabric target)."
            ),
        },
        "config": {"version": "2.0", "logicalId": ""},
    }


def _make_notebook_placeholder_id(notebook_display: str) -> str:
    """Deterministic placeholder GUID for a Fabric notebook item."""
    # Reuse the connection-style placeholder shape but with a different prefix
    # so deployer code can tell them apart.
    import hashlib
    digest = hashlib.sha256(notebook_display.encode("utf-8")).hexdigest()
    return "00000000-0000-4000-9000-" + digest[:12]


def convert_package_to_fabric(
    package: SSISPackage,
    output_dir: Path,
    *,
    pipeline_prefix: str = "PL_",
    llm_translate: bool = False,
    on_prem_ir_name: str = "OnPremSHIR",
    auth_type: str = "ServicePrincipal",
    use_key_vault: bool = False,
    kv_ls_name: str = "AzureKeyVault",
    kv_url: str | None = None,
    schema_remap: dict[str, str] | None = None,
    name_overrides: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Convert *package* to Fabric Data Pipelines artifacts under *output_dir*.

    Strategy: run the existing ADF generators in a tempdir to get in-memory
    artifact dicts, then translate to Fabric shape and write to *output_dir*.
    The intermediate ADF JSON files are discarded — only the in-memory
    representations are used.

    Args mirror generate_pipeline / generate_linked_services where applicable;
    only options meaningful for Fabric are exposed.

    Returns a summary dict (artifact counts, paths, warnings).
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    pipeline_dir_root = output_dir / "pipeline"
    pipeline_dir_root.mkdir(parents=True, exist_ok=True)
    notebook_dir_root = output_dir / "notebook"
    notebook_dir_root.mkdir(parents=True, exist_ok=True)
    stubs_dir = output_dir / "stubs"

    # 1. Run ADF generators in a tempdir purely for the in-memory dicts.
    with tempfile.TemporaryDirectory(prefix="ssis_fabric_intermediate_") as tmp:
        intermediate = Path(tmp)

        linked_services, ls_name_map = generate_linked_services(
            package, intermediate,
            on_prem_ir_name=on_prem_ir_name,
            auth_type=auth_type,
            use_key_vault=use_key_vault,
            kv_ls_name=kv_ls_name,
            kv_url=kv_url,
            shared_artifacts_dir=None,
            name_overrides=name_overrides,
        )
        datasets = generate_datasets(
            package, intermediate,
            schema_remap=schema_remap,
            shared_artifacts_dir=None,
            ls_name_map=ls_name_map,
            name_overrides=name_overrides,
        )
        # Data flows are generated for completeness (script tasks may reference
        # them indirectly) but they get replaced by notebook stubs below.
        generate_data_flows(
            package, intermediate,
            ls_name_map=ls_name_map,
            name_overrides=name_overrides,
            substitution_registry=None,
        )
        adf_pipeline = generate_pipeline(
            package, intermediate,
            stubs_dir=stubs_dir,  # write Function stubs to the REAL output
            llm_translate=llm_translate,
            pipeline_prefix=pipeline_prefix,
            schema_remap=schema_remap,
            ls_name_map=ls_name_map,
            name_overrides=name_overrides,
        )

    # 2. Build the connection resolver from the package's CMs.
    resolver = ConnectionResolver()
    for cm in package.connection_managers:
        resolver.register(cm)

    # 2b. Scan the ADF pipeline for any linked-service references not backed
    # by an SSIS CM (e.g. the synthetic `LS_AzureFunction` injected by the
    # Script Task converter to wrap Function calls). Mint a synthetic
    # placeholder for each so the deployer knows it must be provisioned.
    known_ls_names = set(ls_name_map.values())
    referenced_ls_names = _collect_ls_references(adf_pipeline)
    referenced_ls_names |= _collect_ls_references(datasets)
    for ls_name in sorted(referenced_ls_names - known_ls_names):
        # Pick a Fabric connection type guess from the LS name suffix.
        fabric_type = "Web" if "function" in ls_name.lower() else "Unknown"
        resolver.register_synthetic(
            ls_name,
            fabric_type=fabric_type,
            note=(
                f"Synthetic connection for ADF linked service '{ls_name}' — "
                "no SSIS Connection Manager backing this. Provision the "
                "Fabric connection (e.g. Web/Function endpoint) before deploy."
            ),
        )
    # Splice synthetic entries into the ls_name_map so the translator finds
    # them. Use the LS name as both key and value (the inverter then maps
    # ls_name → ls_name as the cm_id, which matches our synthetic ssis_cm_id
    # convention via __synthetic__: prefix lookup below).
    augmented_ls_name_map = dict(ls_name_map)
    for ls_name in sorted(referenced_ls_names - known_ls_names):
        augmented_ls_name_map[f"__synthetic__:{ls_name}"] = ls_name

    # 3. Generate notebook stubs for every Data Flow Task.
    notebook_id_by_dataflow_name: dict[str, str] = {}
    notebook_paths: list[Path] = []
    for task in package.tasks:
        if isinstance(task, DataFlowTask):
            stub_path = write_notebook_stub(task, package.name, notebook_dir_root)
            notebook_paths.append(stub_path)
            display = notebook_display_name(task, package.name)
            notebook_id_by_dataflow_name[task.name] = _make_notebook_placeholder_id(display)
            # The dataflow generator's ADF activity references the dataflow
            # by `_df_name(package, task.name)`; keep both keys so the
            # translator finds it whichever it looks up.
            from ..generators.naming import df_name as _df_name
            notebook_id_by_dataflow_name[_df_name(package.name, task.name)] = (
                notebook_id_by_dataflow_name[task.name]
            )

    # 4. Translate the ADF pipeline dict to a Fabric pipeline dict.
    fabric_pipeline = translate_pipeline(
        adf_pipeline=adf_pipeline,
        datasets=datasets,
        ls_name_map=augmented_ls_name_map,
        resolver=resolver,
        notebook_id_by_dataflow_name=notebook_id_by_dataflow_name,
    )

    # 5. Write the Fabric pipeline (plus .platform sidecar) to output_dir.
    pipeline_name = _pl_name(package.name, pipeline_prefix, name_overrides=name_overrides)
    pipeline_item_dir = pipeline_dir_root / f"{pipeline_name}.DataPipeline"
    pipeline_item_dir.mkdir(parents=True, exist_ok=True)
    (pipeline_item_dir / "pipeline-content.json").write_text(
        json.dumps(fabric_pipeline, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    (pipeline_item_dir / ".platform").write_text(
        json.dumps(_platform_metadata_pipeline(pipeline_name, package.name), indent=2),
        encoding="utf-8",
    )

    # 6. Write the connections manifest.
    manifest_path = output_dir / "connections_required.json"
    manifest_path.write_text(
        json.dumps(resolver.manifest(), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    # 6b. Write notebook placeholder map (placeholder_id -> Fabric item dir name).
    # The deployer reads this to query fab for each notebook's real GUID after
    # import, so it can substitute into the pipeline's TridentNotebook activities.
    notebook_placeholder_map = {
        _make_notebook_placeholder_id(notebook_display_name(t, package.name)):
            f"{notebook_display_name(t, package.name)}.Notebook"
        for t in package.tasks if isinstance(t, DataFlowTask)
    }
    if notebook_placeholder_map:
        (output_dir / "notebook_placeholders.json").write_text(
            json.dumps(
                {"schema_version": "1.0", "placeholders": notebook_placeholder_map},
                indent=2,
            ),
            encoding="utf-8",
        )

    # 7. Stub files (from Script Tasks — ADF generator already wrote them)
    stub_files = list(stubs_dir.rglob("*.py")) if stubs_dir.exists() else []

    return {
        "package_name": package.name,
        "output_directory": str(output_dir),
        "artifacts_generated": {
            "pipelines": 1,
            "notebooks": len(notebook_paths),
            "connections_required": len(resolver),
            "function_stubs": len(stub_files),
        },
        "pipeline_path": str(pipeline_item_dir),
        "notebook_paths": [str(p) for p in notebook_paths],
        "connections_manifest": str(manifest_path),
        "linked_service_count_adf_intermediate": len(linked_services),
    }
