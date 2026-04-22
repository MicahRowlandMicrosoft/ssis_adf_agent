"""
M3: headless CLI exit-code semantics.

These tests don't assert the deep behavior of each tool (other tests cover
that). They check the wrapper does the right thing:

* prints the JSON body produced by the tool to stdout,
* returns rc=0 on success and rc>=1 on failure / issues_found,
* maps every CLI argument 1:1 onto the MCP tool's input dict.
"""
from __future__ import annotations

import json
from unittest.mock import patch

from mcp import types as mcp_types

from ssis_adf_agent import cli


def _ok(payload: dict) -> list[mcp_types.TextContent]:
    return [mcp_types.TextContent(type="text", text=json.dumps(payload))]


class TestArgsFor:
    def test_validate_args_forwarded_verbatim(self):
        ns = cli._build_parser().parse_args(["validate", "C:/some/dir"])
        name, args = cli._args_for(ns)
        assert name == "validate_adf_artifacts"
        assert args == {"artifacts_dir": "C:/some/dir"}

    def test_deploy_skip_if_exists_flag_propagates(self):
        ns = cli._build_parser().parse_args([
            "deploy", "out/",
            "--subscription-id", "s",
            "--resource-group", "r",
            "--factory-name", "f",
            "--skip-if-exists",
        ])
        name, args = cli._args_for(ns)
        assert name == "deploy_to_adf"
        assert args["skip_if_exists"] is True
        assert args["dry_run"] is False
        assert args["validate_first"] is True

    def test_activate_triggers_dry_run_default_true(self):
        ns = cli._build_parser().parse_args([
            "activate-triggers",
            "--subscription-id", "s",
            "--resource-group", "r",
            "--factory-name", "f",
        ])
        name, args = cli._args_for(ns)
        assert name == "activate_triggers"
        assert args["dry_run"] is True
        assert args["names"] is None

    def test_activate_triggers_no_dry_run_flips_to_live(self):
        ns = cli._build_parser().parse_args([
            "activate-triggers",
            "--subscription-id", "s",
            "--resource-group", "r",
            "--factory-name", "f",
            "--no-dry-run",
            "--names", "trgA", "trgB",
        ])
        _, args = cli._args_for(ns)
        assert args["dry_run"] is False
        assert args["names"] == ["trgA", "trgB"]


class TestExitCodes:
    def test_success_returns_zero(self, capsys):
        with patch.object(cli.mcp_server, "call_tool",
                          return_value=_ok({"status": "valid"})):
            rc = cli.main(["validate", "out/"])
        assert rc == 0
        printed = capsys.readouterr().out
        assert json.loads(printed)["status"] == "valid"

    def test_issues_found_returns_one(self, capsys):
        with patch.object(cli.mcp_server, "call_tool",
                          return_value=_ok({
                              "status": "issues_found",
                              "issue_count": 2,
                              "issues": [],
                          })):
            rc = cli.main(["validate", "out/"])
        assert rc == 1

    def test_failed_count_returns_one(self, capsys):
        with patch.object(cli.mcp_server, "call_tool",
                          return_value=_ok({
                              "total": 5, "succeeded": 3, "failed": 2,
                              "results": [],
                          })):
            rc = cli.main([
                "deploy", "out/",
                "--subscription-id", "s",
                "--resource-group",  "r",
                "--factory-name",    "f",
                "--dry-run",
            ])
        assert rc == 1

    def test_python_exception_returns_two(self, capsys):
        def _boom(name, args):
            raise RuntimeError("kaboom")
        with patch.object(cli.mcp_server, "call_tool", side_effect=_boom):
            rc = cli.main(["validate", "out/"])
        assert rc == 2
        body = json.loads(capsys.readouterr().out)
        assert "kaboom" in body["cli_error"]
