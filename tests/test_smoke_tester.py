"""Tests for the smoke_test_pipeline helper.

We don't hit Azure here \u2014 all SDK clients are monkey-patched. The goal is to
verify the polling / terminal-status / activity-aggregation logic, not the
underlying SDK calls.
"""
from __future__ import annotations

import sys
import types as _pytypes
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest


def _install_fake_sdk(monkeypatch):
    """Inject minimal fake azure.identity + azure.mgmt.datafactory modules.

    Returns the (DataFactoryManagementClient_factory, captured_calls) tuple so
    tests can reach into the recorded call sequence.
    """
    captured: dict = {"client": None, "credential_init": 0}

    # azure.identity
    identity_mod = _pytypes.ModuleType("azure.identity")

    class _DefaultAzureCredential:
        def __init__(self, *a, **kw):
            captured["credential_init"] += 1

    class _AzureCliCredential:
        def __init__(self, *a, **kw):
            captured["credential_init"] += 1

    identity_mod.DefaultAzureCredential = _DefaultAzureCredential
    identity_mod.AzureCliCredential = _AzureCliCredential
    monkeypatch.setitem(sys.modules, "azure.identity", identity_mod)

    # azure.mgmt.datafactory + .models
    df_mod = _pytypes.ModuleType("azure.mgmt.datafactory")
    df_models = _pytypes.ModuleType("azure.mgmt.datafactory.models")

    class _RunFilterParameters:
        def __init__(self, last_updated_after=None, last_updated_before=None):
            self.last_updated_after = last_updated_after
            self.last_updated_before = last_updated_before

    df_models.RunFilterParameters = _RunFilterParameters

    class _DataFactoryManagementClient:
        def __init__(self, credential, subscription_id):
            self.credential = credential
            self.subscription_id = subscription_id
            captured["client"] = self
            self.pipelines = MagicMock()
            self.pipeline_runs = MagicMock()
            self.activity_runs = MagicMock()

    df_mod.DataFactoryManagementClient = _DataFactoryManagementClient
    monkeypatch.setitem(sys.modules, "azure.mgmt.datafactory", df_mod)
    monkeypatch.setitem(sys.modules, "azure.mgmt.datafactory.models", df_models)

    return captured


def _make_run(status, start, end, message=None):
    run = MagicMock()
    run.status = status
    run.run_start = start
    run.run_end = end
    run.message = message
    return run


def _make_activity(name, atype, status, start, end, error=None):
    a = MagicMock()
    a.activity_name = name
    a.activity_type = atype
    a.status = status
    a.activity_run_start = start
    a.activity_run_end = end
    a.error = error
    return a


def test_smoke_test_pipeline_succeeds_and_returns_activities(monkeypatch):
    captured = _install_fake_sdk(monkeypatch)
    # Reload to pick up new sys.modules entries.
    if "ssis_adf_agent.credential" in sys.modules:
        del sys.modules["ssis_adf_agent.credential"]
    if "ssis_adf_agent.migration_plan.smoke_tester" in sys.modules:
        del sys.modules["ssis_adf_agent.migration_plan.smoke_tester"]
    from ssis_adf_agent.migration_plan import smoke_tester  # noqa: WPS433

    start = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    end = start + timedelta(seconds=42)

    # Configure the fake client lazily \u2014 it gets created inside the call.
    monkeypatch.setattr(smoke_tester.time, "sleep", lambda *_: None)

    def _client_factory_side_effect(credential, subscription_id):
        client = MagicMock()
        client.pipelines.create_run.return_value = MagicMock(run_id="run-123")
        client.pipeline_runs.get.side_effect = [
            _make_run("InProgress", start, None),
            _make_run("Succeeded", start, end),
        ]
        client.activity_runs.query_by_pipeline_run.return_value = MagicMock(
            value=[
                _make_activity("Copy_Sales", "Copy", "Succeeded", start, start + timedelta(seconds=20)),
                _make_activity("Lookup_Cfg", "Lookup", "Succeeded", start, start + timedelta(seconds=2)),
            ]
        )
        return client

    monkeypatch.setattr(
        sys.modules["azure.mgmt.datafactory"],
        "DataFactoryManagementClient",
        _client_factory_side_effect,
    )

    result = smoke_tester.smoke_test_pipeline(
        subscription_id="sub",
        resource_group="rg",
        factory_name="adf",
        pipeline_name="PL_Test",
        poll_interval_seconds=0,
    )

    assert result["status"] == "Succeeded"
    assert result["run_id"] == "run-123"
    assert result["timed_out"] is False
    assert result["duration_seconds"] == 42.0
    assert len(result["activities"]) == 2
    names = {a["name"] for a in result["activities"]}
    assert names == {"Copy_Sales", "Lookup_Cfg"}
    assert all(a["status"] == "Succeeded" for a in result["activities"])


def test_smoke_test_pipeline_reports_timeout(monkeypatch):
    _install_fake_sdk(monkeypatch)
    if "ssis_adf_agent.credential" in sys.modules:
        del sys.modules["ssis_adf_agent.credential"]
    if "ssis_adf_agent.migration_plan.smoke_tester" in sys.modules:
        del sys.modules["ssis_adf_agent.migration_plan.smoke_tester"]
    from ssis_adf_agent.migration_plan import smoke_tester  # noqa: WPS433

    start = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(smoke_tester.time, "sleep", lambda *_: None)

    # Make monotonic jump past the deadline on the second call so we exit via timeout.
    times = iter([0.0, 9999.0, 9999.0, 9999.0])
    monkeypatch.setattr(smoke_tester.time, "monotonic", lambda: next(times))

    def _client_factory(credential, subscription_id):
        client = MagicMock()
        client.pipelines.create_run.return_value = MagicMock(run_id="run-timeout")
        client.pipeline_runs.get.return_value = _make_run("InProgress", start, None)
        client.activity_runs.query_by_pipeline_run.return_value = MagicMock(value=[])
        return client

    monkeypatch.setattr(
        sys.modules["azure.mgmt.datafactory"],
        "DataFactoryManagementClient",
        _client_factory,
    )

    result = smoke_tester.smoke_test_pipeline(
        subscription_id="sub",
        resource_group="rg",
        factory_name="adf",
        pipeline_name="PL_Slow",
        timeout_seconds=1,
        poll_interval_seconds=0,
    )

    assert result["status"] == "TimedOut"
    assert result["timed_out"] is True
    assert result["activities"] == []


def test_smoke_test_pipeline_captures_activity_errors(monkeypatch):
    _install_fake_sdk(monkeypatch)
    if "ssis_adf_agent.credential" in sys.modules:
        del sys.modules["ssis_adf_agent.credential"]
    if "ssis_adf_agent.migration_plan.smoke_tester" in sys.modules:
        del sys.modules["ssis_adf_agent.migration_plan.smoke_tester"]
    from ssis_adf_agent.migration_plan import smoke_tester  # noqa: WPS433

    start = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    end = start + timedelta(seconds=5)
    monkeypatch.setattr(smoke_tester.time, "sleep", lambda *_: None)

    def _client_factory(credential, subscription_id):
        client = MagicMock()
        client.pipelines.create_run.return_value = MagicMock(run_id="run-fail")
        client.pipeline_runs.get.return_value = _make_run(
            "Failed", start, end, message="Activity Copy_Sales failed"
        )
        client.activity_runs.query_by_pipeline_run.return_value = MagicMock(
            value=[
                _make_activity(
                    "Copy_Sales", "Copy", "Failed", start, end,
                    error={"message": "Source connection refused"},
                ),
            ]
        )
        return client

    monkeypatch.setattr(
        sys.modules["azure.mgmt.datafactory"],
        "DataFactoryManagementClient",
        _client_factory,
    )

    result = smoke_tester.smoke_test_pipeline(
        subscription_id="sub",
        resource_group="rg",
        factory_name="adf",
        pipeline_name="PL_Bad",
        poll_interval_seconds=0,
    )

    assert result["status"] == "Failed"
    assert result["message"] == "Activity Copy_Sales failed"
    assert result["activities"][0]["error"] == "Source connection refused"
    assert result["activities"][0]["status"] == "Failed"
