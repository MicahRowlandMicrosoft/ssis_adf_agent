"""Pluggable runners for the behavioral parity harness.

Two protocols (:class:`SSISDataFlowRunner` and :class:`AdfDataFlowRunner`)
let callers swap real-environment runners (``dtexec``, ADF debug session)
for fakes in tests.

Three concrete runners ship today:

* :class:`DtexecRunner` — invokes ``dtexec`` against a .dtsx with a controlled
  Flat File source, captures the destination Flat File output rows.
* :class:`AdfDebugRunner` — starts an ADF Mapping Data Flow debug session via
  :class:`azure.mgmt.datafactory.DataFactoryManagementClient`, waits for the
  preview output, returns rows.
* :class:`CapturedOutputRunner` — replays previously-captured CSV rows.  Used
  by the worked example and by unit tests so the harness can be demonstrated
  without dtexec or live ADF.

Both real runners require external dependencies that are not part of the
test suite (``dtexec.exe`` from SSIS for one, an Azure subscription for the
other).  They are best-effort implementations intended as starting points
that customers will extend for their environment.
"""
from __future__ import annotations

import csv
import logging
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Protocol, runtime_checkable

logger = logging.getLogger(__name__)


@dataclass
class RunnerResult:
    """The output of one side of the comparison."""

    rows: list[dict[str, Any]]
    runner_name: str
    artifact_paths: list[str] = field(default_factory=list)
    log: str = ""


@runtime_checkable
class SSISDataFlowRunner(Protocol):
    """A callable that runs an SSIS Data Flow and returns its output rows."""

    name: str

    def run(
        self,
        *,
        package_path: Path,
        dataflow_task_name: str,
        input_dataset_path: Path,
        work_dir: Path,
    ) -> RunnerResult:  # pragma: no cover - protocol
        ...


@runtime_checkable
class AdfDataFlowRunner(Protocol):
    """A callable that runs an ADF Mapping Data Flow and returns its output rows."""

    name: str

    def run(
        self,
        *,
        adf_dataflow_path: Path,
        input_dataset_path: Path,
        work_dir: Path,
    ) -> RunnerResult:  # pragma: no cover - protocol
        ...


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------


def _read_csv(path: Path) -> list[dict[str, Any]]:
    """Read a CSV file into a list of dicts.  Empty cells become ``""``."""
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        return [dict(row) for row in reader]


def _write_csv(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    rows = list(rows)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames: list[str] = []
    seen: set[str] = set()
    for r in rows:
        for k in r.keys():
            if k not in seen:
                seen.add(k)
                fieldnames.append(k)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)


# ---------------------------------------------------------------------------
# Captured-output runner (works with fixtures, no external deps)
# ---------------------------------------------------------------------------


class CapturedOutputRunner:
    """Replay previously-captured rows from a CSV.

    Used by the worked example and by unit tests so the harness can be
    demonstrated without dtexec or live Azure.  Customers can capture real
    runs once and then replay them in CI for fast regression checks.
    """

    def __init__(self, captured_csv: Path, *, name: str = "captured") -> None:
        self.captured_csv = Path(captured_csv)
        self.name = name

    def run(self, **_: Any) -> RunnerResult:
        if not self.captured_csv.is_file():
            raise FileNotFoundError(
                f"CapturedOutputRunner: captured CSV not found: {self.captured_csv}"
            )
        rows = _read_csv(self.captured_csv)
        return RunnerResult(
            rows=rows,
            runner_name=self.name,
            artifact_paths=[str(self.captured_csv)],
            log=f"Replayed {len(rows)} rows from {self.captured_csv}",
        )


# ---------------------------------------------------------------------------
# dtexec runner
# ---------------------------------------------------------------------------


class DtexecRunner:
    """Runs an SSIS Data Flow via ``dtexec.exe`` and reads a Flat File destination.

    This is a *starter* runner.  It assumes:

    * ``dtexec.exe`` is on PATH (or supplied via ``dtexec_path``).
    * The package's source can be redirected at run-time to ``input_dataset_path``
      via the SSIS ``/SET`` command-line option, addressed by
      ``source_connection_path`` (e.g. the ConnectionString property of the
      controlled Flat File Connection Manager).
    * The package writes to a Flat File destination and the destination's
      ConnectionString is addressable via ``destination_connection_path``.

    Customers whose packages don't fit this shape should subclass and override
    :meth:`run` to assemble the appropriate ``/SET`` invocations or to
    pre-stage inputs in their environment.
    """

    name = "dtexec"

    def __init__(
        self,
        *,
        source_connection_path: str,
        destination_connection_path: str,
        destination_filename: str = "adf_parity_dest.csv",
        dtexec_path: str | None = None,
        extra_set_args: list[tuple[str, str]] | None = None,
        timeout_seconds: int = 600,
    ) -> None:
        self.source_connection_path = source_connection_path
        self.destination_connection_path = destination_connection_path
        self.destination_filename = destination_filename
        self.dtexec_path = dtexec_path
        self.extra_set_args = list(extra_set_args or [])
        self.timeout_seconds = timeout_seconds

    def run(
        self,
        *,
        package_path: Path,
        dataflow_task_name: str,
        input_dataset_path: Path,
        work_dir: Path,
    ) -> RunnerResult:
        del dataflow_task_name  # dtexec runs the whole package; caller controls scope
        exe = self.dtexec_path or shutil.which("dtexec")
        if not exe:
            raise FileNotFoundError(
                "dtexec not found.  Install SQL Server Integration Services or "
                "supply dtexec_path explicitly."
            )

        work_dir.mkdir(parents=True, exist_ok=True)
        dest_path = work_dir / self.destination_filename

        cmd: list[str] = [
            exe,
            "/File",
            str(package_path),
            "/Set",
            f"{self.source_connection_path};{input_dataset_path}",
            "/Set",
            f"{self.destination_connection_path};{dest_path}",
        ]
        for prop, value in self.extra_set_args:
            cmd.extend(["/Set", f"{prop};{value}"])

        logger.info("Running dtexec: %s", " ".join(cmd))
        completed = subprocess.run(  # noqa: S603 - intentional shell-out, paths controlled
            cmd,
            capture_output=True,
            text=True,
            timeout=self.timeout_seconds,
            check=False,
        )
        log = completed.stdout + "\n" + completed.stderr
        if completed.returncode != 0:
            raise RuntimeError(
                f"dtexec failed (exit {completed.returncode}).  Log:\n{log[-4000:]}"
            )
        if not dest_path.is_file():
            raise FileNotFoundError(
                f"dtexec completed but destination CSV was not produced at {dest_path}.\n"
                "Check that destination_connection_path matches a Flat File Connection "
                "Manager and that the package writes to it."
            )

        rows = _read_csv(dest_path)
        return RunnerResult(
            rows=rows,
            runner_name=self.name,
            artifact_paths=[str(dest_path)],
            log=log,
        )


# ---------------------------------------------------------------------------
# ADF debug runner
# ---------------------------------------------------------------------------


class AdfDebugRunner:
    """Runs an ADF Mapping Data Flow via a debug session, returns preview rows.

    *Starter* runner: requires an existing ADF instance reachable by the
    deploying identity, and assumes the dataflow has been deployed (or is
    deployed transparently here from ``adf_dataflow_path``).  It then starts
    a debug session, executes a preview command on a chosen output stream,
    and parses preview rows.

    The Azure SDK surface for Mapping Data Flow debug previews is verbose
    and version-dependent; this implementation includes the minimum needed
    to capture rows and is designed to be subclassed for customer-specific
    pre-stage / post-process steps.
    """

    name = "adf-debug"

    def __init__(
        self,
        *,
        subscription_id: str,
        resource_group: str,
        factory_name: str,
        compute_type: str = "General",
        core_count: int = 8,
        time_to_live_minutes: int = 10,
        output_stream_name: str | None = None,
        row_limit: int = 1000,
    ) -> None:
        self.subscription_id = subscription_id
        self.resource_group = resource_group
        self.factory_name = factory_name
        self.compute_type = compute_type
        self.core_count = core_count
        self.time_to_live_minutes = time_to_live_minutes
        self.output_stream_name = output_stream_name
        self.row_limit = row_limit

    def run(
        self,
        *,
        adf_dataflow_path: Path,
        input_dataset_path: Path,
        work_dir: Path,
    ) -> RunnerResult:
        del input_dataset_path, work_dir  # consumed by the debug session, not us
        try:
            from azure.mgmt.datafactory import DataFactoryManagementClient

            from ..credential import get_credential
        except ImportError as exc:  # pragma: no cover - import-time guard
            raise ImportError(
                "azure-mgmt-datafactory and azure-identity are required for AdfDebugRunner. "
                "pip install azure-mgmt-datafactory azure-identity"
            ) from exc

        credential = get_credential()
        client = DataFactoryManagementClient(credential, self.subscription_id)
        df_doc = _read_json(adf_dataflow_path)
        dataflow_name = df_doc.get("name") or adf_dataflow_path.stem

        # The exact debug-session payload shape changes between SDK versions.
        # We keep this method short and let the SDK raise if the running version
        # disagrees, so the failure is loud and actionable instead of silent.
        debug_session = client.data_flow_debug_session.begin_create(
            resource_group_name=self.resource_group,
            factory_name=self.factory_name,
            request={
                "computeType": self.compute_type,
                "coreCount": self.core_count,
                "timeToLive": self.time_to_live_minutes,
            },
        ).result()
        session_id = debug_session.session_id
        if not session_id:
            raise RuntimeError("ADF debug session create returned no session_id.")

        try:
            client.data_flow_debug_session.add_data_flow(
                resource_group_name=self.resource_group,
                factory_name=self.factory_name,
                request={
                    "sessionId": session_id,
                    "dataFlow": df_doc,
                },
            )
            preview = client.data_flow_debug_session.execute_command(
                resource_group_name=self.resource_group,
                factory_name=self.factory_name,
                request={
                    "sessionId": session_id,
                    "dataFlowName": dataflow_name,
                    "command": "previewOutput",
                    "commandPayload": {
                        "streamName": self.output_stream_name or "sink1",
                        "rowLimits": self.row_limit,
                    },
                },
            ).result()
            rows = _parse_preview_rows(preview)
        finally:
            try:
                client.data_flow_debug_session.delete(
                    resource_group_name=self.resource_group,
                    factory_name=self.factory_name,
                    request={"sessionId": session_id},
                )
            except Exception as exc:  # pragma: no cover - cleanup best-effort
                logger.warning("Failed to delete debug session %s: %s", session_id, exc)

        return RunnerResult(
            rows=rows,
            runner_name=self.name,
            artifact_paths=[],
            log=f"ADF debug session {session_id} returned {len(rows)} rows",
        )


def _read_json(path: Path) -> dict[str, Any]:
    import json

    return json.loads(Path(path).read_text(encoding="utf-8"))


def _parse_preview_rows(preview: Any) -> list[dict[str, Any]]:
    """Best-effort parser for the ADF debug preview payload.

    The SDK returns either a JSON string in ``preview.data`` or a typed object
    with ``data`` already deserialized.  Both shapes are handled.
    """
    data = getattr(preview, "data", None)
    if isinstance(data, str):
        import json

        try:
            data = json.loads(data)
        except json.JSONDecodeError:
            return []
    if isinstance(data, dict) and "rows" in data:
        # Newer SDK shape: {"schema": [...], "rows": [...]}
        rows = data.get("rows") or []
        schema = [c.get("name") for c in (data.get("schema") or [])]
        if schema and rows and isinstance(rows[0], list):
            return [dict(zip(schema, r)) for r in rows]
        if isinstance(rows, list) and rows and isinstance(rows[0], dict):
            return rows
    if isinstance(data, list):
        return [r for r in data if isinstance(r, dict)]
    return []
