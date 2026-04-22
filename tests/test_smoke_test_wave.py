"""N1 — smoke_test_wave aggregates per-pipeline smoke results."""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from ssis_adf_agent import mcp_server


def _run(args: dict) -> dict:
    import asyncio
    out = asyncio.run(mcp_server.call_tool("smoke_test_wave", args))
    return json.loads(out[0].text)


def _ok(name: str) -> dict:
    return {"pipeline_name": name, "status": "Succeeded", "run_id": "r-" + name, "activities": []}


def _fail(name: str) -> dict:
    return {"pipeline_name": name, "status": "Failed", "run_id": "r-" + name, "activities": []}


class TestSmokeTestWave:
    def test_aggregates_across_explicit_pipeline_names(self):
        with patch.object(mcp_server, "_smoke_test_wave", wraps=mcp_server._smoke_test_wave):
            with patch("ssis_adf_agent.migration_plan.smoke_test_pipeline", side_effect=[_ok("A"), _ok("B"), _fail("C")]):
                payload = _run({
                    "subscription_id": "00000000-0000-0000-0000-000000000000",
                    "resource_group": "rg",
                    "factory_name": "adf",
                    "pipeline_names": ["A", "B", "C"],
                })
        assert payload["summary"]["total"] == 3
        assert payload["summary"]["succeeded"] == 2
        assert payload["summary"]["failed"] == 1
        assert [r["pipeline_name"] for r in payload["results"]] == ["A", "B", "C"]

    def test_discovers_pipelines_from_artifacts_dir(self, tmp_path: Path):
        pipelines = tmp_path / "pipeline"
        pipelines.mkdir()
        (pipelines / "PL_one.json").write_text("{}")
        (pipelines / "PL_two.json").write_text("{}")

        with patch("ssis_adf_agent.migration_plan.smoke_test_pipeline", side_effect=[_ok("PL_one"), _ok("PL_two")]):
            payload = _run({
                "subscription_id": "00000000-0000-0000-0000-000000000000",
                "resource_group": "rg",
                "factory_name": "adf",
                "artifacts_dir": str(tmp_path),
            })
        assert payload["summary"]["total"] == 2
        assert payload["summary"]["succeeded"] == 2

    def test_stop_on_failure_skips_remaining(self):
        with patch("ssis_adf_agent.migration_plan.smoke_test_pipeline", side_effect=[_ok("A"), _fail("B"), _ok("C")]):
            payload = _run({
                "subscription_id": "00000000-0000-0000-0000-000000000000",
                "resource_group": "rg",
                "factory_name": "adf",
                "pipeline_names": ["A", "B", "C"],
                "stop_on_failure": True,
            })
        assert payload["summary"]["succeeded"] == 1
        assert payload["summary"]["failed"] == 1
        assert payload["summary"]["skipped"] == 1
        # Third pipeline should be skipped, not invoked
        assert payload["results"][2]["status"] == "skipped"

    def test_errored_pipeline_recorded(self):
        with patch("ssis_adf_agent.migration_plan.smoke_test_pipeline", side_effect=RuntimeError("boom")):
            payload = _run({
                "subscription_id": "00000000-0000-0000-0000-000000000000",
                "resource_group": "rg",
                "factory_name": "adf",
                "pipeline_names": ["only"],
            })
        assert payload["summary"]["errored"] == 1
        assert payload["results"][0]["status"] == "errored"
        assert "boom" in payload["results"][0]["error"]

    def test_requires_pipeline_names_or_artifacts_dir(self):
        import asyncio
        out = asyncio.run(mcp_server.call_tool("smoke_test_wave", {
            "subscription_id": "00000000-0000-0000-0000-000000000000",
            "resource_group": "rg",
            "factory_name": "adf",
        }))
        # Dispatcher catches the ValueError and returns a plain text "Error: ..." message.
        assert out[0].text.startswith("Error:")
        assert "pipeline_names" in out[0].text or "artifacts_dir" in out[0].text

    def test_artifacts_dir_without_pipeline_subdir_raises(self, tmp_path: Path):
        with pytest.raises(Exception):
            import asyncio
            asyncio.run(mcp_server._smoke_test_wave({
                "subscription_id": "00000000-0000-0000-0000-000000000000",
                "resource_group": "rg",
                "factory_name": "adf",
                "artifacts_dir": str(tmp_path),
            }))
