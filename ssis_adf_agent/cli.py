"""
Headless CLI (M3).

Re-uses the same Python entry points as the MCP tools, so a CI job can call
``python -m ssis_adf_agent <command> ...`` without standing up an MCP client.

Goals:
* Every long-running tool that returns JSON via MCP also has a CLI here that
  prints the same JSON to stdout (so it can be piped into ``jq``, attached as
  a CI artifact, etc.).
* Exit code is 0 on success, non-zero on failure or validation issues — so
  CI pipelines fail loudly.
* Argument names match the MCP tool's ``inputSchema`` properties verbatim, so
  the CLI is just a 1:1 reflection.

Currently exposed:
  * ``analyze``           — wraps ``analyze_ssis_package``
  * ``convert``           — wraps ``convert_ssis_package``
  * ``validate``          — wraps ``validate_adf_artifacts``
  * ``deploy``            — wraps ``deploy_to_adf``
  * ``activate-triggers`` — wraps ``activate_triggers``

If you need another tool from CI, please open an issue — adding a new
sub-command is a 5-line change in this module.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from typing import Any

from . import mcp_server


def _run(coro_args: tuple[str, dict[str, Any]]) -> tuple[int, str]:
    """Dispatch a single tool call through the MCP server's call handler."""
    name, args = coro_args
    try:
        result = asyncio.run(mcp_server.call_tool(name, args))
    except Exception as exc:  # noqa: BLE001
        return 2, json.dumps({"cli_error": f"{type(exc).__name__}: {exc}"})
    # call_tool returns list[TextContent]; the agent always emits one TextContent.
    text = "\n".join(getattr(c, "text", str(c)) for c in result)
    # Try to detect failure inside the JSON body so CI can fail loudly.
    rc = 0
    try:
        body = json.loads(text)
        if isinstance(body, dict):
            if body.get("status") in ("issues_found", "error"):
                rc = 1
            if body.get("failed", 0):
                rc = 1
    except (ValueError, TypeError):
        pass
    return rc, text


def _add_common(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--json-only", action="store_true",
        help="Suppress non-JSON chatter; print only the JSON result.",
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ssis-adf-agent",
        description=(
            "Headless CLI for the SSIS -> ADF conversion agent. "
            "Each subcommand mirrors the corresponding MCP tool 1:1."
        ),
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    # ---- analyze --------------------------------------------------------
    a = sub.add_parser("analyze", help="Analyze a single .dtsx package.")
    a.add_argument("package_path")
    _add_common(a)

    # ---- convert --------------------------------------------------------
    c = sub.add_parser("convert", help="Convert a .dtsx to ADF JSON artifacts.")
    c.add_argument("package_path")
    c.add_argument("output_dir")
    c.add_argument("--no-trigger", action="store_true",
                   help="Skip ScheduleTrigger generation.")
    c.add_argument("--llm-translate", action="store_true",
                   help="Use the optional LLM expression translator.")
    c.add_argument("--auth-type", default="SystemAssignedManagedIdentity")
    c.add_argument("--use-key-vault", action="store_true")
    c.add_argument("--kv-url", default=None,
                   help="Required when --use-key-vault is set.")
    c.add_argument("--design-path", default=None)
    c.add_argument("--shared-artifacts-dir", default=None)
    _add_common(c)

    # ---- validate -------------------------------------------------------
    v = sub.add_parser("validate", help="Structurally validate an ADF artifacts directory.")
    v.add_argument("artifacts_dir")
    _add_common(v)

    # ---- deploy ---------------------------------------------------------
    d = sub.add_parser("deploy", help="Deploy ADF artifacts to an Azure Data Factory.")
    d.add_argument("artifacts_dir")
    d.add_argument("--subscription-id", required=True)
    d.add_argument("--resource-group", required=True)
    d.add_argument("--factory-name", required=True)
    d.add_argument("--dry-run", action="store_true")
    d.add_argument("--no-validate-first", action="store_true",
                   help="Skip the pre-deploy structural validation step.")
    d.add_argument("--skip-if-exists", action="store_true",
                   help="Non-destructive mode: leave existing artifacts untouched.")
    _add_common(d)

    # ---- activate-triggers ---------------------------------------------
    t = sub.add_parser(
        "activate-triggers",
        help="Activate (start) one or more triggers in a deployed factory.",
    )
    t.add_argument("--subscription-id", required=True)
    t.add_argument("--resource-group", required=True)
    t.add_argument("--factory-name", required=True)
    t.add_argument("--names", nargs="*", default=None,
                   help="Trigger names; default = every Stopped trigger.")
    t.add_argument("--no-dry-run", action="store_true",
                   help="Actually start the triggers (default is dry-run).")
    _add_common(t)

    return parser


def _args_for(ns: argparse.Namespace) -> tuple[str, dict[str, Any]]:
    cmd = ns.cmd
    if cmd == "analyze":
        return "analyze_ssis_package", {"package_path": ns.package_path}
    if cmd == "convert":
        out: dict[str, Any] = {
            "package_path": ns.package_path,
            "output_dir": ns.output_dir,
            "generate_trigger": not ns.no_trigger,
            "llm_translate": ns.llm_translate,
            "auth_type": ns.auth_type,
            "use_key_vault": ns.use_key_vault,
        }
        if ns.kv_url:
            out["kv_url"] = ns.kv_url
        if ns.design_path:
            out["design_path"] = ns.design_path
        if ns.shared_artifacts_dir:
            out["shared_artifacts_dir"] = ns.shared_artifacts_dir
        return "convert_ssis_package", out
    if cmd == "validate":
        return "validate_adf_artifacts", {"artifacts_dir": ns.artifacts_dir}
    if cmd == "deploy":
        return "deploy_to_adf", {
            "artifacts_dir": ns.artifacts_dir,
            "subscription_id": ns.subscription_id,
            "resource_group": ns.resource_group,
            "factory_name": ns.factory_name,
            "dry_run": ns.dry_run,
            "validate_first": not ns.no_validate_first,
            "skip_if_exists": ns.skip_if_exists,
        }
    if cmd == "activate-triggers":
        return "activate_triggers", {
            "subscription_id": ns.subscription_id,
            "resource_group": ns.resource_group,
            "factory_name": ns.factory_name,
            "names": ns.names,
            "dry_run": not ns.no_dry_run,
        }
    raise SystemExit(f"Unknown command: {cmd}")


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    ns = parser.parse_args(argv)
    rc, text = _run(_args_for(ns))
    print(text)
    return rc


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
