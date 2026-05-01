"""
Fabric deployer — pushes generated Fabric Data Pipelines artifacts to a
Microsoft Fabric workspace via the fabric-cli (`fab`) command-line tool.

Strategy: the agent does NOT call the Fabric REST APIs directly. Microsoft's
fabric-cli is the supported / documented surface for workspace + item CRUD,
handles auth (Entra DefaultAzureCredential / device-code), and stays in step
with the Fabric REST surface as it evolves. We shell out to `fab`.

Two-step deploy:

  1. Resolve placeholder Connection GUIDs — load
     `connections_required.json`, look up real Fabric Connection GUIDs for
     each placeholder (caller supplies a name→GUID mapping), then rewrite
     every pipeline-content.json file in place so `externalReferences.connection`
     points at the real GUID.

  2. Import each `<item>.DataPipeline` / `<item>.Notebook` directory into the
     target workspace via `fab import` (idempotent — `fab import` updates an
     existing item if it already exists).

For testability, all subprocess calls go through a `FabRunner` Protocol the
caller can substitute. The real implementation invokes `fab` via
`subprocess.run`. The agent never bypasses `fab` to talk to Fabric directly.
"""
from __future__ import annotations

import json
import logging
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Subprocess injection point
# ---------------------------------------------------------------------------

@dataclass
class FabResult:
    """Captured result of one `fab` invocation."""

    args: list[str]
    returncode: int
    stdout: str = ""
    stderr: str = ""

    @property
    def ok(self) -> bool:
        return self.returncode == 0


class FabRunner(Protocol):
    """Protocol the deployer uses to invoke `fab`. Substituted in tests."""

    def __call__(self, args: list[str], cwd: Path | None = None) -> FabResult: ...


def real_fab_runner(args: list[str], cwd: Path | None = None) -> FabResult:
    """Default FabRunner — invokes the real `fab` binary via subprocess."""
    fab_path = shutil.which("fab")
    if not fab_path:
        return FabResult(
            args=args, returncode=127,
            stderr=(
                "fab CLI not found on PATH. Install fabric-cli "
                "(pip install ms-fabric-cli) and run `fab auth login`."
            ),
        )
    full = [fab_path, *args]
    try:
        proc = subprocess.run(
            full,
            cwd=str(cwd) if cwd else None,
            capture_output=True,
            text=True,
            check=False,
        )
        return FabResult(
            args=full,
            returncode=proc.returncode,
            stdout=proc.stdout or "",
            stderr=proc.stderr or "",
        )
    except OSError as exc:
        return FabResult(args=full, returncode=1, stderr=str(exc))


# ---------------------------------------------------------------------------
# Connection placeholder substitution
# ---------------------------------------------------------------------------

def load_connections_manifest(artifacts_dir: Path) -> dict[str, Any]:
    """Read `connections_required.json` from *artifacts_dir*. Returns {} if missing."""
    path = artifacts_dir / "connections_required.json"
    if not path.exists():
        return {"schema_version": "1.0", "connections": []}
    return json.loads(path.read_text(encoding="utf-8"))


def resolve_connection_substitutions(
    manifest: dict[str, Any],
    name_to_guid: dict[str, str],
) -> tuple[dict[str, str], list[str]]:
    """For each entry in *manifest*, look up the real GUID by SSIS CM name.

    Returns ``(placeholder_to_guid, unresolved_names)``.

    The deployer supports two lookup keys for each manifest entry:
      - the SSIS connection-manager name (preferred)
      - the synthetic LS name (for adapter-injected connections like
        `LS_AzureFunction`)
    """
    sub: dict[str, str] = {}
    unresolved: list[str] = []
    for entry in manifest.get("connections", []):
        placeholder = entry.get("placeholder_id")
        ssis_name = entry.get("ssis_connection_manager_name", "")
        if not placeholder:
            continue
        guid = name_to_guid.get(ssis_name)
        if guid:
            sub[placeholder] = guid
        else:
            unresolved.append(ssis_name or placeholder)
    return sub, unresolved


def _substitute_in_obj(obj: Any, substitutions: dict[str, str]) -> int:
    """Walk *obj* and replace every `connection: <placeholder>` with the
    mapped GUID. Returns the count of substitutions made."""
    count = 0
    if isinstance(obj, dict):
        ext = obj.get("externalReferences")
        if isinstance(ext, dict):
            conn = ext.get("connection")
            if isinstance(conn, str) and conn in substitutions:
                ext["connection"] = substitutions[conn]
                count += 1
        # TridentNotebook activities carry a notebookId — substitute too if mapped.
        tp = obj.get("typeProperties")
        if isinstance(tp, dict) and obj.get("type") == "TridentNotebook":
            nb_id = tp.get("notebookId")
            if isinstance(nb_id, str) and nb_id in substitutions:
                tp["notebookId"] = substitutions[nb_id]
                count += 1
            ws_id = tp.get("workspaceId")
            if isinstance(ws_id, str) and ws_id in substitutions:
                tp["workspaceId"] = substitutions[ws_id]
                count += 1
        for v in obj.values():
            count += _substitute_in_obj(v, substitutions)
    elif isinstance(obj, list):
        for item in obj:
            count += _substitute_in_obj(item, substitutions)
    return count


def apply_substitutions_in_place(
    artifacts_dir: Path, substitutions: dict[str, str],
) -> dict[str, int]:
    """Rewrite every pipeline-content.json under *artifacts_dir* so placeholder
    GUIDs are replaced by the mapped real GUIDs.

    Returns a per-file count of substitutions made.
    """
    counts: dict[str, int] = {}
    pipeline_dir = artifacts_dir / "pipeline"
    if not pipeline_dir.exists():
        return counts
    for content in pipeline_dir.rglob("pipeline-content.json"):
        doc = json.loads(content.read_text(encoding="utf-8"))
        n = _substitute_in_obj(doc, substitutions)
        if n:
            content.write_text(
                json.dumps(doc, indent=2, ensure_ascii=False), encoding="utf-8",
            )
        counts[str(content)] = n
    return counts


# ---------------------------------------------------------------------------
# Deployer
# ---------------------------------------------------------------------------

@dataclass
class FabricDeployResult:
    item_name: str
    item_type: str  # "DataPipeline" | "Notebook"
    success: bool
    error: str | None = None
    fab_stdout: str = ""
    fab_stderr: str = ""


@dataclass
class FabricDeploySummary:
    workspace: str
    artifacts_dir: str
    substitutions_applied: int
    unresolved_connections: list[str]
    items_attempted: int
    items_succeeded: int
    items_failed: int
    results: list[FabricDeployResult] = field(default_factory=list)
    notebook_id_map: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "workspace": self.workspace,
            "artifacts_dir": self.artifacts_dir,
            "substitutions_applied": self.substitutions_applied,
            "unresolved_connections": list(self.unresolved_connections),
            "items_attempted": self.items_attempted,
            "items_succeeded": self.items_succeeded,
            "items_failed": self.items_failed,
            "notebook_id_map": dict(self.notebook_id_map),
            "results": [
                {
                    "item_name": r.item_name,
                    "item_type": r.item_type,
                    "success": r.success,
                    "error": r.error,
                    "fab_stdout_tail": r.fab_stdout[-500:] if r.fab_stdout else "",
                    "fab_stderr_tail": r.fab_stderr[-500:] if r.fab_stderr else "",
                }
                for r in self.results
            ],
        }


class FabricDeployer:
    """Deploys Fabric pipeline / notebook items to a workspace via fab CLI.

    Args:
        workspace: The Fabric workspace name as known to fab (`fab ls /workspaces/`).
        runner: Substitute the FabRunner for tests. Defaults to real_fab_runner.
        dry_run: If True, log every fab invocation but do not execute it.
    """

    def __init__(
        self,
        workspace: str,
        runner: FabRunner | None = None,
        dry_run: bool = False,
    ) -> None:
        if not workspace:
            raise ValueError("workspace must be non-empty")
        self.workspace = workspace
        self._runner = runner or real_fab_runner
        self.dry_run = dry_run

    # -- preflight ---------------------------------------------------------

    def check_fab_installed(self) -> FabResult:
        """Run `fab --version` to confirm the CLI is on PATH."""
        return self._runner(["--version"], cwd=None)

    def check_workspace_exists(self) -> FabResult:
        """Run `fab ls /workspaces/<workspace>` to confirm the workspace is visible."""
        return self._runner(["ls", f"/workspaces/{self.workspace}"], cwd=None)

    # -- notebook id discovery --------------------------------------------

    def discover_notebook_ids(self, artifacts_dir: Path) -> dict[str, str]:
        """For every entry in `notebook_placeholders.json`, query fab for the
        real Fabric notebook GUID. Returns ``{placeholder_id: real_guid}``.

        Notebooks not yet imported (or not visible to the authenticated user)
        are skipped silently — their placeholder will remain in the pipeline
        JSON and the deploy will be reported as failed via validator markers.
        """
        result: dict[str, str] = {}
        sidecar = artifacts_dir / "notebook_placeholders.json"
        if not sidecar.exists():
            return result
        try:
            doc = json.loads(sidecar.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return result
        placeholders = doc.get("placeholders", {})
        for placeholder, item_dir_name in placeholders.items():
            r = self._runner(
                ["get", "-q", "id", f"/workspaces/{self.workspace}/{item_dir_name}"],
                cwd=None,
            )
            if r.ok:
                real_id = (r.stdout or "").strip()
                if real_id:
                    result[placeholder] = real_id
        return result

    # -- import ------------------------------------------------------------

    def deploy_item(self, item_dir: Path) -> FabricDeployResult:
        """Import one .DataPipeline / .Notebook item directory."""
        if not item_dir.is_dir():
            return FabricDeployResult(
                item_name=item_dir.name, item_type=_item_type(item_dir),
                success=False, error=f"not a directory: {item_dir}",
            )
        item_type = _item_type(item_dir)
        target = f"/workspaces/{self.workspace}/{item_dir.name}"
        args = ["import", "--input", str(item_dir), target, "--force"]
        if self.dry_run:
            logger.info("[dry_run] would run: fab %s", " ".join(args))
            return FabricDeployResult(
                item_name=item_dir.name, item_type=item_type, success=True,
                fab_stdout="[dry_run]",
            )
        r = self._runner(args, cwd=item_dir.parent)
        return FabricDeployResult(
            item_name=item_dir.name,
            item_type=item_type,
            success=r.ok,
            error=None if r.ok else f"fab import returned {r.returncode}",
            fab_stdout=r.stdout,
            fab_stderr=r.stderr,
        )

    def deploy(
        self,
        artifacts_dir: Path,
        connection_name_to_guid: dict[str, str] | None = None,
        notebook_id_overrides: dict[str, str] | None = None,
    ) -> FabricDeploySummary:
        """Full deploy: resolve substitutions, push notebooks first, then pipelines.

        Args:
            artifacts_dir: Output directory of convert_ssis_to_fabric.
            connection_name_to_guid: Mapping from SSIS CM name (or synthetic
                LS name) to a real Fabric Connection GUID. Used to rewrite
                placeholder GUIDs before pipeline import.
            notebook_id_overrides: Mapping from placeholder notebook id to
                real Fabric notebook GUID. Lets the caller pass values they
                already have (e.g. from a prior `deploy` call) without
                re-running discovery.

        Returns: a FabricDeploySummary with per-item results.
        """
        # 1. Connection substitutions
        manifest = load_connections_manifest(artifacts_dir)
        substitutions, unresolved = resolve_connection_substitutions(
            manifest, connection_name_to_guid or {},
        )

        # 2. Push notebooks first (pipelines reference them)
        results: list[FabricDeployResult] = []
        nb_dir = artifacts_dir / "notebook"
        notebook_dirs = sorted(nb_dir.glob("*.Notebook")) if nb_dir.exists() else []
        for ndir in notebook_dirs:
            results.append(self.deploy_item(ndir))

        # 3. Discover real notebook ids and merge with caller-supplied overrides
        notebook_ids: dict[str, str] = dict(notebook_id_overrides or {})
        if not self.dry_run and notebook_dirs:
            discovered = self.discover_notebook_ids(artifacts_dir)
            # caller-supplied overrides win over discovered
            for k, v in discovered.items():
                notebook_ids.setdefault(k, v)
        substitutions.update(notebook_ids)

        # 4. Apply substitutions to pipeline JSONs in place
        sub_counts = apply_substitutions_in_place(artifacts_dir, substitutions)
        total_subs = sum(sub_counts.values())

        # 5. Push pipelines
        pl_dir = artifacts_dir / "pipeline"
        pipeline_dirs = sorted(pl_dir.glob("*.DataPipeline")) if pl_dir.exists() else []
        for pdir in pipeline_dirs:
            results.append(self.deploy_item(pdir))

        succeeded = sum(1 for r in results if r.success)
        failed = sum(1 for r in results if not r.success)
        return FabricDeploySummary(
            workspace=self.workspace,
            artifacts_dir=str(artifacts_dir),
            substitutions_applied=total_subs,
            unresolved_connections=unresolved,
            items_attempted=len(results),
            items_succeeded=succeeded,
            items_failed=failed,
            results=results,
            notebook_id_map=notebook_ids,
        )


def _item_type(item_dir: Path) -> str:
    suffix = item_dir.suffix
    if suffix == ".DataPipeline":
        return "DataPipeline"
    if suffix == ".Notebook":
        return "Notebook"
    return "Unknown"


# ---------------------------------------------------------------------------
# Workspace provisioning
# ---------------------------------------------------------------------------

@dataclass
class WorkspaceProvisionResult:
    workspace: str
    existed: bool
    created: bool
    capacity_id: str | None
    error: str | None = None
    fab_stdout: str = ""
    fab_stderr: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "workspace": self.workspace,
            "existed": self.existed,
            "created": self.created,
            "capacity_id": self.capacity_id,
            "error": self.error,
            "fab_stdout_tail": self.fab_stdout[-500:] if self.fab_stdout else "",
            "fab_stderr_tail": self.fab_stderr[-500:] if self.fab_stderr else "",
        }


def provision_workspace(
    workspace: str,
    capacity_id: str | None = None,
    runner: FabRunner | None = None,
    dry_run: bool = False,
) -> WorkspaceProvisionResult:
    """Ensure a Fabric workspace exists. Idempotent.

    Args:
        workspace: Workspace display name.
        capacity_id: Optional Fabric capacity id to bind the workspace to.
            Required when creating; ignored if the workspace already exists.
        runner: Substitute for tests.
        dry_run: Log but don't execute mutating fab commands.
    """
    if not workspace:
        raise ValueError("workspace must be non-empty")
    runner = runner or real_fab_runner

    # Check existence
    check = runner(["ls", f"/workspaces/{workspace}"], cwd=None)
    if check.ok:
        return WorkspaceProvisionResult(
            workspace=workspace, existed=True, created=False,
            capacity_id=capacity_id, fab_stdout=check.stdout, fab_stderr=check.stderr,
        )

    # Create
    if not capacity_id:
        return WorkspaceProvisionResult(
            workspace=workspace, existed=False, created=False, capacity_id=None,
            error=(
                "Workspace does not exist and capacity_id was not supplied. "
                "Pass capacity_id (`fab ls /capacities/` to discover) so the "
                "new workspace can be bound to a Fabric capacity."
            ),
        )
    create_args = [
        "create", f"/workspaces/{workspace}",
        "-P", f"capacityId={capacity_id}",
    ]
    if dry_run:
        logger.info("[dry_run] would run: fab %s", " ".join(create_args))
        return WorkspaceProvisionResult(
            workspace=workspace, existed=False, created=True, capacity_id=capacity_id,
            fab_stdout="[dry_run]",
        )
    cr = runner(create_args, cwd=None)
    return WorkspaceProvisionResult(
        workspace=workspace,
        existed=False,
        created=cr.ok,
        capacity_id=capacity_id,
        error=None if cr.ok else f"fab create returned {cr.returncode}",
        fab_stdout=cr.stdout,
        fab_stderr=cr.stderr,
    )
