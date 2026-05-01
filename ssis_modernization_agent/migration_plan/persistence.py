"""Save / load migration plans to JSON on disk.

Plans are versioned via ``schema_version``. Loaders accept any plan with the
same major version; minor mismatches log a warning but proceed. Future
migrations can be added when the schema changes incompatibly.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

from .models import PLAN_SCHEMA_VERSION, MigrationPlan

logger = logging.getLogger(__name__)


def save_plan(plan: MigrationPlan, path: str | Path) -> Path:
    """Write the plan to ``path`` as pretty-printed JSON. Returns the resolved path."""
    p = Path(path).expanduser().resolve()
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(plan.model_dump_json(indent=2), encoding="utf-8")
    return p


def load_plan(path: str | Path) -> MigrationPlan:
    """Load a plan from disk and validate it against the current schema."""
    p = Path(path).expanduser().resolve()
    raw = json.loads(p.read_text(encoding="utf-8"))
    found = str(raw.get("schema_version", "0.0"))
    if found.split(".")[0] != PLAN_SCHEMA_VERSION.split(".")[0]:
        raise ValueError(
            f"Plan at {p} has incompatible schema_version={found} "
            f"(expected {PLAN_SCHEMA_VERSION}). Migration required."
        )
    if found != PLAN_SCHEMA_VERSION:
        logger.warning(
            "Plan schema_version=%s differs from current %s; loading anyway.",
            found, PLAN_SCHEMA_VERSION,
        )
    return MigrationPlan.model_validate(raw)
