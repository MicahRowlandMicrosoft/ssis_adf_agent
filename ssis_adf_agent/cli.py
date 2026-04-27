"""
Headless CLI.

Two layers:
* **Legacy friendly aliases** (``analyze``, ``convert``, ``validate``,
  ``deploy``, ``activate-triggers``) that pre-date P5-17 and use a small
  curated argument surface (positional paths, mnemonic flags). Kept for
  backward compatibility with existing CI scripts.
* **Auto-generated parity** (P5-17): every MCP tool from
  ``mcp_server.list_tools()`` is exposed under a sub-command named after
  the tool itself (with underscores converted to hyphens), with one
  ``--<property>`` flag per ``inputSchema`` property. Help text and types
  come from the same schema the MCP server publishes, so the two surfaces
  stay synchronized as new tools are added.

Exit codes: 0 on success, 1 on validation/issues_found/failed>0, 2 on
unhandled Python exception.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from typing import Any

from . import mcp_server


# ---------------------------------------------------------------------------
# Shared dispatch
# ---------------------------------------------------------------------------

def _run(coro_args: tuple[str, dict[str, Any]]) -> tuple[int, str]:
    """Dispatch a single tool call through the MCP server's call handler."""
    name, args = coro_args
    try:
        result = asyncio.run(mcp_server.call_tool(name, args))
    except Exception as exc:  # noqa: BLE001
        return 2, json.dumps({"cli_error": f"{type(exc).__name__}: {exc}"})
    text = "\n".join(getattr(c, "text", str(c)) for c in result)
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


# ---------------------------------------------------------------------------
# Legacy friendly aliases (pre-P5-17)
# ---------------------------------------------------------------------------

_LEGACY_ALIASES = {
    "analyze", "convert", "validate", "deploy", "activate-triggers",
}


def _add_legacy_aliases(sub: argparse._SubParsersAction) -> None:
    a = sub.add_parser("analyze", help="Analyze a single .dtsx package.")
    a.add_argument("package_path")
    _add_common(a)

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

    v = sub.add_parser("validate", help="Structurally validate an ADF artifacts directory.")
    v.add_argument("artifacts_dir")
    _add_common(v)

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


def _args_for(ns: argparse.Namespace) -> tuple[str, dict[str, Any]]:
    """Translate a legacy-alias argparse Namespace into MCP (name, args)."""
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


# ---------------------------------------------------------------------------
# Auto-generated MCP parity (P5-17)
# ---------------------------------------------------------------------------

def _to_flag(name: str) -> str:
    return "--" + name.replace("_", "-")


def _coerce(value: Any, spec: dict[str, Any]) -> Any:
    typ = spec.get("type")
    if value is None:
        return None
    if typ == "object" and isinstance(value, str):
        return json.loads(value)
    if typ == "array" and isinstance(value, str):
        return json.loads(value)
    return value


def _add_property(
    parser: argparse.ArgumentParser, name: str, spec: dict[str, Any], required: bool
) -> None:
    typ = spec.get("type")
    flag = _to_flag(name)
    kwargs: dict[str, Any] = {
        "help": spec.get("description", ""),
        "dest": name,
    }
    if "default" in spec:
        kwargs["default"] = spec["default"]
    if spec.get("enum"):
        kwargs["choices"] = spec["enum"]

    if typ == "boolean":
        kwargs["action"] = argparse.BooleanOptionalAction
        if "default" not in kwargs:
            kwargs["default"] = None
    elif typ == "integer":
        kwargs["type"] = int
    elif typ == "number":
        kwargs["type"] = float
    elif typ == "array":
        items = spec.get("items") or {}
        item_type = items.get("type", "string")
        if item_type == "integer":
            kwargs["type"] = int
        elif item_type == "number":
            kwargs["type"] = float
        kwargs["nargs"] = "*"
    elif typ == "object":
        kwargs["type"] = str  # Pass JSON literal; coerced on dispatch.
    else:
        kwargs["type"] = str

    if required:
        kwargs["required"] = True
    parser.add_argument(flag, **kwargs)


def _ns_to_mcp_args(ns: argparse.Namespace, schema: dict[str, Any]) -> dict[str, Any]:
    properties = (schema or {}).get("properties", {}) or {}
    out: dict[str, Any] = {}
    for name, spec in properties.items():
        if not hasattr(ns, name):
            continue
        value = getattr(ns, name)
        if value is None:
            continue
        out[name] = _coerce(value, spec or {})
    return out


def _add_auto_tools(sub: argparse._SubParsersAction) -> None:
    """Add a sub-command for every MCP tool whose hyphen-name isn't already taken."""
    tools = asyncio.run(mcp_server.list_tools())
    for tool in tools:
        cmd_name = tool.name.replace("_", "-")
        if cmd_name in _LEGACY_ALIASES:
            continue  # Legacy alias already serves this tool with curated args.
        sub_p = sub.add_parser(
            cmd_name,
            help=(tool.description or "").splitlines()[0][:120],
            description=tool.description,
        )
        sub_p.set_defaults(_tool_name=tool.name, _schema=tool.inputSchema)
        schema = tool.inputSchema or {}
        properties = schema.get("properties", {}) or {}
        required = set(schema.get("required", []) or [])
        for name, spec in properties.items():
            _add_property(sub_p, name, spec or {}, required=name in required)


# ---------------------------------------------------------------------------
# Parser assembly + main
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ssis-adf-agent",
        description=(
            "Headless CLI for the SSIS -> ADF conversion agent. Legacy "
            "subcommands (analyze/convert/validate/deploy/activate-triggers) "
            "use a curated surface; every other MCP tool is reflected 1:1 "
            "from its inputSchema."
        ),
    )
    sub = parser.add_subparsers(dest="cmd", required=True)
    _add_legacy_aliases(sub)
    _add_auto_tools(sub)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    ns = parser.parse_args(argv)
    if ns.cmd in _LEGACY_ALIASES:
        rc, text = _run(_args_for(ns))
    else:
        name = ns._tool_name  # type: ignore[attr-defined]
        args = _ns_to_mcp_args(ns, ns._schema)  # type: ignore[attr-defined]
        rc, text = _run((name, args))
    print(text)
    return rc


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
