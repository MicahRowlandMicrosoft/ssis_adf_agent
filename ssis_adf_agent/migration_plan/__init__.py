"""SSIS Migration Copilot — design proposal, plan persistence, and conversion-by-plan.

Public surface used by the MCP server:

- :func:`propose_design` — emit a recommended :class:`MigrationPlan` for a parsed package.
- :func:`save_plan` / :func:`load_plan` — round-trip plans to JSON on disk.
- :class:`MigrationPlan` and friends — the shared schema all consumers use.
"""
from __future__ import annotations

from .applier import PlanApplication, apply_plan
from .bicep_generator import generate_bicep
from .estate_tools import (
    PlanEditError,
    edit_migration_plan,
    estimate_adf_costs,
    plan_migration_waves,
)
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
from .provisioner import BicepCompilerNotFound, deploy_bicep
from .smoke_tester import smoke_test_pipeline

__all__ = [
    "PLAN_SCHEMA_VERSION",
    "AuthMode",
    "BicepCompilerNotFound",
    "EffortEstimate",
    "InfrastructureItem",
    "LinkedServiceSpec",
    "MigrationPlan",
    "PlanApplication",
    "PlanEditError",
    "RbacAssignment",
    "Risk",
    "RiskSeverity",
    "Simplification",
    "SimplificationAction",
    "StorageKind",
    "TargetPattern",
    "apply_plan",
    "deploy_bicep",
    "detect_target_pattern",
    "edit_migration_plan",
    "estimate_adf_costs",
    "generate_bicep",
    "load_plan",
    "plan_migration_waves",
    "propose_design",
    "save_plan",
    "smoke_test_pipeline",
]
