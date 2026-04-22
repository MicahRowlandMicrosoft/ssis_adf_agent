"""
H7: bulk trigger activation.

Tests the AdfDeployer.activate_triggers() business logic with a stubbed Azure
client — no real Azure calls. We exercise the four user-visible outcomes:

- already_started: trigger is in 'Started' state -> no API call, status reported
- would_activate: dry_run=True -> no begin_start call, status reported
- activated: dry_run=False -> begin_start called, status reported
- not_found: requested name does not exist in the factory
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from ssis_adf_agent.deployer.adf_deployer import AdfDeployer


def _make_trigger(name: str, runtime_state: str, type_name: str = "ScheduleTrigger"):
    """Build a fake trigger resource that quacks like azure-mgmt-datafactory's."""
    props = MagicMock()
    props.runtime_state = runtime_state
    # Force type(props).__name__ to look like the SDK class
    props.__class__.__name__ = type_name
    props.pipelines = []
    t = MagicMock()
    t.name = name
    t.properties = props
    return t


@pytest.fixture
def deployer_with_triggers(monkeypatch):
    """Construct an AdfDeployer with .client patched to return canned triggers."""
    monkeypatch.setattr(
        "ssis_adf_agent.deployer.adf_deployer.get_credential",
        lambda: MagicMock(),
    )
    d = AdfDeployer(
        subscription_id="sub-fake",
        resource_group="rg-fake",
        factory_name="adf-fake",
    )
    fake_client = MagicMock()
    fake_client.triggers.list_by_factory.return_value = [
        _make_trigger("trg_started", "Started"),
        _make_trigger("trg_stopped_a", "Stopped"),
        _make_trigger("trg_stopped_b", "Stopped"),
    ]
    poller = MagicMock()
    poller.result.return_value = None
    fake_client.triggers.begin_start.return_value = poller
    d._client = fake_client
    return d, fake_client


class TestListTriggers:
    def test_returns_each_trigger_with_runtime_state(self, deployer_with_triggers):
        d, _ = deployer_with_triggers
        rows = d.list_triggers()
        names = {r["name"] for r in rows}
        assert names == {"trg_started", "trg_stopped_a", "trg_stopped_b"}
        states = {r["name"]: r["runtime_state"] for r in rows}
        assert states["trg_started"] == "Started"
        assert states["trg_stopped_a"] == "Stopped"


class TestActivateTriggersDryRun:
    def test_default_is_dry_run(self, deployer_with_triggers):
        d, client = deployer_with_triggers
        results = d.activate_triggers()  # no kwargs -> dry_run defaults True
        assert all(r["dry_run"] for r in results)
        client.triggers.begin_start.assert_not_called()

    def test_dry_run_reports_would_activate_for_stopped(self, deployer_with_triggers):
        d, _ = deployer_with_triggers
        results = d.activate_triggers(dry_run=True)
        by_name = {r["name"]: r for r in results}
        assert by_name["trg_stopped_a"]["status"] == "would_activate"
        assert by_name["trg_stopped_a"]["before"] == "Stopped"
        assert by_name["trg_stopped_b"]["status"] == "would_activate"

    def test_dry_run_reports_already_started_no_op(self, deployer_with_triggers):
        d, _ = deployer_with_triggers
        results = d.activate_triggers(dry_run=True)
        by_name = {r["name"]: r for r in results}
        assert by_name["trg_started"]["status"] == "already_started"

    def test_unknown_trigger_name_is_not_found(self, deployer_with_triggers):
        d, _ = deployer_with_triggers
        results = d.activate_triggers(names=["does_not_exist"], dry_run=True)
        assert len(results) == 1
        assert results[0]["status"] == "not_found"
        assert results[0]["error"] is not None


class TestActivateTriggersLive:
    def test_actually_calls_begin_start_when_not_dry_run(self, deployer_with_triggers):
        d, client = deployer_with_triggers
        results = d.activate_triggers(
            names=["trg_stopped_a", "trg_started"], dry_run=False
        )
        # trg_stopped_a should have been started; trg_started should be a no-op.
        client.triggers.begin_start.assert_called_once_with(
            "rg-fake", "adf-fake", "trg_stopped_a"
        )
        by_name = {r["name"]: r for r in results}
        assert by_name["trg_stopped_a"]["status"] == "activated"
        assert by_name["trg_stopped_a"]["after"] == "Started"
        assert by_name["trg_started"]["status"] == "already_started"

    def test_failure_reports_error_and_does_not_throw(self, deployer_with_triggers):
        d, client = deployer_with_triggers
        client.triggers.begin_start.side_effect = RuntimeError("boom")
        results = d.activate_triggers(names=["trg_stopped_a"], dry_run=False)
        assert results[0]["status"] == "failed"
        assert "boom" in results[0]["error"]

    def test_explicit_name_filter_only_processes_listed(self, deployer_with_triggers):
        d, _ = deployer_with_triggers
        results = d.activate_triggers(names=["trg_stopped_b"], dry_run=True)
        assert {r["name"] for r in results} == {"trg_stopped_b"}
