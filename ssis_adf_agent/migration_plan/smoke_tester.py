"""Trigger a pipeline run in Azure Data Factory and return per-activity results.

This is the post-deployment smoke-test step of the migration copilot loop:
after ``deploy_to_adf`` has uploaded artifacts and (optionally)
``provision_adf_environment`` has set up infrastructure, this tool actually
*runs* a converted pipeline once and reports back what passed and what failed.

Authentication uses :class:`DefaultAzureCredential` (``az login`` for devs,
managed identity / service principal env vars for CI).
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger(__name__)

# ADF pipeline-run statuses that are terminal (no further polling needed).
_TERMINAL_STATUSES = {"Succeeded", "Failed", "Cancelled"}

_DEFAULT_TIMEOUT_SECONDS = 600
_DEFAULT_POLL_INTERVAL = 10


def smoke_test_pipeline(
    *,
    subscription_id: str,
    resource_group: str,
    factory_name: str,
    pipeline_name: str,
    parameters: dict[str, Any] | None = None,
    timeout_seconds: int = _DEFAULT_TIMEOUT_SECONDS,
    poll_interval_seconds: int = _DEFAULT_POLL_INTERVAL,
) -> dict[str, Any]:
    """Trigger one pipeline run, poll until terminal, and return activity-level results.

    :param subscription_id: Azure subscription containing the factory.
    :param resource_group: Resource group of the factory.
    :param factory_name: Data Factory name.
    :param pipeline_name: Pipeline to run (must already exist in the factory).
    :param parameters: Optional run-time parameter overrides for the pipeline.
    :param timeout_seconds: Hard cap on how long to wait for the run. Default 600s.
    :param poll_interval_seconds: Delay between status checks. Default 10s.

    :return: A dict with shape::

        {
          "run_id": "...",
          "pipeline_name": "...",
          "status": "Succeeded" | "Failed" | "Cancelled" | "TimedOut",
          "duration_seconds": 42.1,
          "started_at": "...", "ended_at": "...",
          "message": "...",                # ADF-supplied error message (failures)
          "activities": [
            {"name": "Copy_Sales", "type": "Copy", "status": "Succeeded",
             "duration_seconds": 12.3, "error": null},
            ...
          ]
        }

        On timeout the function returns ``status="TimedOut"`` with whatever
        activity status it could observe; it does NOT cancel the run.
    """
    try:
        from azure.mgmt.datafactory import DataFactoryManagementClient

        from ..credential import get_credential
        from azure.mgmt.datafactory.models import RunFilterParameters
    except ImportError as exc:  # pragma: no cover - import-time guard
        raise ImportError(
            "azure-mgmt-datafactory and azure-identity are required for smoke_test_pipeline. "
            "Install with: pip install azure-mgmt-datafactory azure-identity"
        ) from exc

    credential = get_credential()
    client = DataFactoryManagementClient(credential, subscription_id)

    logger.info(
        "Triggering pipeline run: %s/%s/%s", resource_group, factory_name, pipeline_name
    )
    create_response = client.pipelines.create_run(
        resource_group_name=resource_group,
        factory_name=factory_name,
        pipeline_name=pipeline_name,
        parameters=parameters or None,
    )
    run_id = create_response.run_id
    logger.info("Pipeline run started: run_id=%s", run_id)

    deadline = time.monotonic() + timeout_seconds
    last_run = None
    timed_out = False
    while True:
        last_run = client.pipeline_runs.get(
            resource_group_name=resource_group,
            factory_name=factory_name,
            run_id=run_id,
        )
        status = last_run.status or "Unknown"
        logger.debug("Run %s status=%s", run_id, status)
        if status in _TERMINAL_STATUSES:
            break
        if time.monotonic() >= deadline:
            timed_out = True
            break
        time.sleep(poll_interval_seconds)

    # Query activity runs over a window padded around the run start/end.
    started = last_run.run_start or datetime.now(timezone.utc)
    ended = last_run.run_end or datetime.now(timezone.utc)
    window_start = started - timedelta(minutes=5)
    window_end = ended + timedelta(minutes=5)

    activities_payload: list[dict[str, Any]] = []
    try:
        activity_query = client.activity_runs.query_by_pipeline_run(
            resource_group_name=resource_group,
            factory_name=factory_name,
            run_id=run_id,
            filter_parameters=RunFilterParameters(
                last_updated_after=window_start,
                last_updated_before=window_end,
            ),
        )
        for activity in activity_query.value or []:
            a_start = activity.activity_run_start
            a_end = activity.activity_run_end
            duration = (
                (a_end - a_start).total_seconds()
                if a_start and a_end
                else None
            )
            error_msg: str | None = None
            if activity.error:
                # `error` is a dict-like (Azure model). Extract message if present.
                error_msg = (
                    activity.error.get("message")
                    if isinstance(activity.error, dict)
                    else str(activity.error)
                )
            activities_payload.append(
                {
                    "name": activity.activity_name,
                    "type": activity.activity_type,
                    "status": activity.status,
                    "duration_seconds": duration,
                    "error": error_msg,
                }
            )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to query activity runs for %s: %s", run_id, exc)

    duration_seconds = (
        (ended - started).total_seconds() if last_run.run_start and last_run.run_end else None
    )

    return {
        "run_id": run_id,
        "pipeline_name": pipeline_name,
        "factory_name": factory_name,
        "resource_group": resource_group,
        "status": "TimedOut" if timed_out else (last_run.status or "Unknown"),
        "duration_seconds": duration_seconds,
        "started_at": last_run.run_start.isoformat() if last_run.run_start else None,
        "ended_at": last_run.run_end.isoformat() if last_run.run_end else None,
        "message": last_run.message,
        "parameters": parameters or {},
        "activities": activities_payload,
        "timed_out": timed_out,
    }
