"""SSIS Migration Copilot — design proposal, plan persistence, and conversion-by-plan.

Public surface used by the MCP server:

- :func:`propose_design` — emit a recommended :class:`MigrationPlan` for a parsed package.
- :func:`save_plan` / :func:`load_plan` — round-trip plans to JSON on disk.
- :class:`MigrationPlan` and friends — the shared schema all consumers use.
"""
from __future__ import annotations

from .applier import PlanApplication, apply_plan
from .models import (
    PLAN_SCHEMA_VERSION,
    AuthMode,
    EffortEstimate,
    InfrastructureItem,
    LinkedServiceSpec,
    MigrationPlan,
    RbacAssignment,
    Risk,
    RiskSeverity,
    Simplification,
    SimplificationAction,
    StorageKind,
    TargetPattern,
)
from .persistence import load_plan, save_plan
from .proposer import detect_target_pattern, propose_design

__all__ = [
    "PLAN_SCHEMA_VERSION",
    "AuthMode",
    "EffortEstimate",
    "InfrastructureItem",
    "LinkedServiceSpec",
    "MigrationPlan",
    "PlanApplication",
    "RbacAssignment",
    "Risk",
    "RiskSeverity",
    "Simplification",
    "SimplificationAction",
    "StorageKind",
    "TargetPattern",
    "apply_plan",
    "detect_target_pattern",
    "load_plan",
    "propose_design",
    "save_plan",
]
