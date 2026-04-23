"""Catch the next time the MCP tool count drifts from the README.

P5-9: README tool-count + diagram inconsistencies.

The README header advertises a tool count in three places. They must all
agree with the actual number of tools the MCP server exposes
(``len(list_tools())``); otherwise procurement reviewers screenshot the
inconsistency.
"""
from __future__ import annotations

import asyncio
import re
from pathlib import Path

from ssis_adf_agent.mcp_server import list_tools

REPO_ROOT = Path(__file__).resolve().parent.parent
README = REPO_ROOT / "README.md"


def _actual_tool_count() -> int:
    return len(asyncio.run(list_tools()))


def test_readme_tool_count_matches_list_tools() -> None:
    text = README.read_text(encoding="utf-8")
    actual = _actual_tool_count()

    # All three references should be the actual number.
    patterns = [
        rf"\*\*{actual} tools\*\*",            # header callout
        rf"verify that the {actual} tools",    # install instructions
        rf"All {actual} tools are invoked",    # usage section
    ]
    missing = [p for p in patterns if not re.search(p, text)]
    assert not missing, (
        f"README has {actual} tools according to list_tools() but is missing "
        f"these references: {missing}. Update README.md to bump every "
        f"tool-count string when adding/removing tools."
    )


def test_readme_workflow_diagram_matches_six_step_path() -> None:
    """The README architecture diagram must show the six-step path
    (bulk_analyze \u2192 propose \u2192 convert \u2192 validate \u2192 deploy \u2192 activate)
    that WORKFLOW.md prescribes, not the older 5-step (scan \u2192 analyze \u2192
    convert \u2192 validate \u2192 deploy) path."""
    text = README.read_text(encoding="utf-8")
    # The six-step path MUST appear.
    assert "bulk_analyze" in text and "activate" in text, (
        "README architecture diagram should reference the six-step path "
        "from WORKFLOW.md (bulk_analyze \u2192 ... \u2192 activate)."
    )
    # The older five-step path MUST NOT appear in the diagram.
    assert "scan \u2192 analyze \u2192 convert" not in text, (
        "README architecture diagram still shows the older 5-step path; "
        "update it to the 6-step path that WORKFLOW.md documents."
    )
