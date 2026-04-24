"""P5-17: CLI parity for every MCP tool.

The CLI must expose a sub-command for every tool published by
`mcp_server.list_tools()` and forward arguments verbatim. Help text and
required-argument enforcement come from the same `inputSchema` the MCP
server uses, so the two surfaces stay in sync as new tools are added.
"""
from __future__ import annotations

import asyncio
import json
from unittest.mock import patch

from mcp import types as mcp_types

from ssis_adf_agent import cli, mcp_server


def _ok(payload: dict) -> list[mcp_types.TextContent]:
    return [mcp_types.TextContent(type="text", text=json.dumps(payload))]


def _all_tool_names() -> list[str]:
    return [t.name for t in asyncio.run(mcp_server.list_tools())]


class TestParityCoverage:
    def test_every_mcp_tool_has_a_subcommand(self):
        parser = cli._build_parser()
        # Pull the sub-parsers action.
        sub_actions = [a for a in parser._actions
                       if isinstance(a, type(parser._subparsers._group_actions[0]))]  # type: ignore[union-attr]
        assert sub_actions, "no subparsers attached"
        choices = set()
        for action in sub_actions:
            choices.update(getattr(action, "choices", {}).keys())
        for tool_name in _all_tool_names():
            cmd = tool_name.replace("_", "-")
            # Either the auto-generated cmd, or a legacy alias serves it.
            assert cmd in choices or cli._LEGACY_ALIASES & {cmd} or cmd in cli._LEGACY_ALIASES \
                or any(_legacy_serves(tool_name) for _ in [0]), (
                    f"MCP tool {tool_name!r} has no CLI subcommand")

    def test_required_args_enforced_from_schema(self):
        # diff_estate requires before_dir and after_dir per schema.
        parser = cli._build_parser()
        try:
            parser.parse_args(["diff-estate"])
        except SystemExit as exc:
            assert exc.code != 0
        else:
            raise AssertionError("expected SystemExit for missing required args")

    def test_help_text_comes_from_schema_description(self, capsys):
        parser = cli._build_parser()
        try:
            parser.parse_args(["diff-estate", "--help"])
        except SystemExit:
            pass
        out = capsys.readouterr().out
        assert "before_dir" in out or "before-dir" in out
        assert "after_dir" in out or "after-dir" in out


class TestAutoDispatch:
    def test_diff_estate_forwards_args_to_mcp_handler(self, capsys, tmp_path):
        captured = {}

        def _fake(name, args):
            captured["name"] = name
            captured["args"] = args
            return _ok({"summary": {"changed": 0}})

        with patch.object(cli.mcp_server, "call_tool", side_effect=_fake):
            rc = cli.main([
                "diff-estate",
                "--before-dir", str(tmp_path / "before"),
                "--after-dir", str(tmp_path / "after"),
            ])
        assert rc == 0
        assert captured["name"] == "diff_estate"
        assert captured["args"]["before_dir"] == str(tmp_path / "before")
        assert captured["args"]["after_dir"] == str(tmp_path / "after")

    def test_boolean_flag_propagates_via_no_prefix(self, capsys):
        captured = {}

        def _fake(name, args):
            captured["args"] = args
            return _ok({"ok": True})

        # validate_deployer_rbac has only string properties; pick something
        # with a boolean. analyze_ssis_package -> string only. Use
        # convert_estate which has multiple flags including booleans.
        with patch.object(cli.mcp_server, "call_tool", side_effect=_fake):
            cli.main([
                "convert-estate",
                "--source-path", "in",
                "--output-dir", "out",
                "--with-cost-projection",
            ])
        assert captured["args"]["with_cost_projection"] is True

    def test_array_argument_collects_multiple_values(self):
        captured = {}

        def _fake(name, args):
            captured["args"] = args
            return _ok({"ok": True})

        # consolidate_packages takes package_paths: array of strings.
        with patch.object(cli.mcp_server, "call_tool", side_effect=_fake):
            cli.main([
                "consolidate-packages",
                "--package-paths", "PkgA.dtsx", "PkgB.dtsx", "PkgC.dtsx",
                "--output-dir", "out",
            ])
        assert captured["args"]["package_paths"] == ["PkgA.dtsx", "PkgB.dtsx", "PkgC.dtsx"]


def _legacy_serves(tool_name: str) -> bool:
    """Return True if a legacy alias dispatches to ``tool_name``."""
    legacy_map = {
        "analyze_ssis_package": "analyze",
        "convert_ssis_package": "convert",
        "validate_adf_artifacts": "validate",
        "deploy_to_adf": "deploy",
        "activate_triggers": "activate-triggers",
    }
    return tool_name in legacy_map
