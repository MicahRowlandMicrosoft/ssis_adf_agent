"""Estate-scale convert / wave-plan / cost-estimate / mutate-plan helpers.

These are all small, deterministic functions consumed by the MCP server.
They sit in the migration_plan package because they all operate on (or produce)
``MigrationPlan`` instances or ``bulk_analyze``-style estate reports.

Design-first workflow
---------------------
``plan_migration_waves``, ``estimate_adf_costs``, and the estate PDF report
**require saved MigrationPlans** as input.  This enforces the correct order:

1. scan → analyze → propose ADF design → customer reviews/edits → save plan
2. **then** estimate LOE, project costs, and sequence migration waves

Estimating before the architectural blueprints are agreed makes no sense —
these tools refuse to run without plans so the agent (and customer) are guided
to complete the design conversation first.
"""
from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any

from .models import (
    AuthMode,
    MigrationPlan,
    Simplification,
    SimplificationAction,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers — extract summary dicts from MigrationPlan for wave / cost logic
# ---------------------------------------------------------------------------

def _plan_to_pkg_summary(plan: MigrationPlan) -> dict[str, Any]:
    """Distil a MigrationPlan into the flat dict shape that wave/cost logic needs."""
    ri = plan.reasoning_input or {}
    return {
        "package_name": plan.package_name,
        "complexity_bucket": plan.effort.bucket,
        "complexity_score": ri.get("complexity_score", 0),
        "target_pattern": plan.target_pattern.value,
        "estimated_total_hours": plan.effort.total_hours,
        "estimated_low_hours": plan.effort.low_hours,
        "estimated_high_hours": plan.effort.high_hours,
        "mcp_automated_hours_saved": plan.effort.mcp_automated_hours_saved,
        "task_counts": ri.get("task_counts", {}),
        "simplifications": [s.action.value for s in plan.simplifications],
        "linked_service_count": len(plan.linked_services),
    }


# ---------------------------------------------------------------------------
# plan_migration_waves
# ---------------------------------------------------------------------------

def plan_migration_waves(
    plans: list[MigrationPlan],
    *,
    max_packages_per_wave: int = 10,
    estate_setup_hours: float = 0.0,
    apply_learning_curve: bool = False,
) -> dict[str, Any]:
    """Group an estate into ordered migration waves.

    **Requires saved MigrationPlans** — call ``propose_adf_design`` and
    ``save_migration_plan`` first so the estimates reflect the *agreed* design,
    not a preliminary analysis.

    Strategy (deliberately simple, deterministic, easy to override):

    * **Wave 1** — bulk-convertible (low / medium complexity), grouped by
      target_pattern so each wave shares linked services and reviewer context.
    * **Wave 2..N** — design-review-needed (high / very_high), grouped by
      target_pattern, each wave capped at ``max_packages_per_wave`` so a single
      reviewer can hold the design conversation in their head.

    Accuracy knobs (both default off to preserve backward-compatible totals):

    * ``estate_setup_hours`` — one-time hours added to Wave 1 to cover estate
      bring-up work that per-package estimates don't include: IR / Key Vault
      provisioning, naming conventions, CI/CD pipeline setup, RBAC, observability.
    * ``apply_learning_curve`` — when True, packages within a wave are
      progressively discounted (100%, 90%, 85%, 80%, ...) to reflect that later
      packages reuse design decisions, linked services, and reviewer context
      established by the first package in the wave.

    The output mirrors the bulk_analyze report shape so the agent can hand it
    straight to a customer.
    """
    packages = [_plan_to_pkg_summary(p) for p in plans]

    bulk = [p for p in packages if p["complexity_bucket"] in ("low", "medium")]
    review = [p for p in packages if p["complexity_bucket"] in ("high", "very_high")]

    def _wave_hours(chunk: list[dict]) -> float:
        if not apply_learning_curve:
            return round(sum(p["estimated_total_hours"] for p in chunk), 1)
        total = 0.0
        for i, p in enumerate(chunk):
            # 100%, 90%, 85%, 80%, 75%, 70%, 65%, 60%, ... floor 0.5
            discount = max(0.5, 1.0 - max(0, i) * 0.05 - (0.05 if i >= 1 else 0.0))
            total += p["estimated_total_hours"] * discount
        return round(total, 1)

    waves: list[dict[str, Any]] = []
    wave_num = 1

    # Wave 1: bulk-convertible, grouped by pattern, then chunked.
    by_pattern_bulk: dict[str, list[dict]] = defaultdict(list)
    for p in bulk:
        by_pattern_bulk[p.get("target_pattern", "custom")].append(p)
    for pattern, pkgs in sorted(by_pattern_bulk.items()):
        for chunk_start in range(0, len(pkgs), max_packages_per_wave):
            chunk = pkgs[chunk_start:chunk_start + max_packages_per_wave]
            waves.append({
                "wave": wave_num,
                "label": f"Bulk convert — {pattern}",
                "strategy": "bulk_convert",
                "package_count": len(chunk),
                "estimated_hours": _wave_hours(chunk),
                "target_pattern": pattern,
                "packages": [p["package_name"] for p in chunk],
            })
            wave_num += 1

    # Wave 2..N: design-review-needed, grouped by pattern, sorted hardest-first
    by_pattern_review: dict[str, list[dict]] = defaultdict(list)
    for p in review:
        by_pattern_review[p.get("target_pattern", "custom")].append(p)
    for pattern, pkgs in sorted(by_pattern_review.items()):
        pkgs_sorted = sorted(pkgs, key=lambda p: -p["complexity_score"])
        for chunk_start in range(0, len(pkgs_sorted), max_packages_per_wave):
            chunk = pkgs_sorted[chunk_start:chunk_start + max_packages_per_wave]
            waves.append({
                "wave": wave_num,
                "label": f"Design review — {pattern}",
                "strategy": "design_review",
                "package_count": len(chunk),
                "estimated_hours": _wave_hours(chunk),
                "target_pattern": pattern,
                "packages": [p["package_name"] for p in chunk],
            })
            wave_num += 1

    # Estate-setup surcharge is attached to Wave 1 as a separate line so
    # reviewers can see it explicitly.
    setup_applied = 0.0
    if estate_setup_hours and waves:
        setup_applied = round(float(estate_setup_hours), 1)
        waves[0]["setup_surcharge_hours"] = setup_applied
        waves[0]["estimated_hours"] = round(waves[0]["estimated_hours"] + setup_applied, 1)

    return {
        "wave_count": len(waves),
        "total_packages": len(packages),
        "total_estimated_hours": round(sum(w["estimated_hours"] for w in waves), 1),
        "estate_setup_hours": setup_applied,
        "learning_curve_applied": bool(apply_learning_curve),
        "waves": waves,
    }


# ---------------------------------------------------------------------------
# estimate_adf_costs
# ---------------------------------------------------------------------------

# Conservative US East/East 2 list-price assumptions (USD). Documented so the
# customer can challenge them. Update via the ``rates`` parameter for current
# pricing or other regions.
_DEFAULT_RATES = {
    "activity_run_per_1k": 1.00,             # Pipeline orchestration
    "azure_ir_diu_hour": 0.25,               # Azure-IR Copy DIU·hour
    "self_hosted_ir_run_per_1k": 0.25,       # Self-hosted IR pipeline activities
    "data_flow_vcore_hour": 0.274,           # General-purpose v-core·hour
    "storage_gb_month_hot": 0.0184,          # ADLS Gen2 hot LRS
    "key_vault_op_per_10k": 0.03,            # Std vault operations
}


def _activity_mix_from_plans(
    plans: list[MigrationPlan],
) -> dict[str, Any]:
    """Derive per-pipeline activity counts from saved plans.

    Returns aggregate counts the cost estimator can use instead of flat
    assumptions:

    * ``total_copy_activities``    — tasks that become Copy Activity
    * ``total_dataflow_activities`` — tasks that become Execute Data Flow
    * ``total_other_activities``    — remaining orchestration activities
    * ``total_activities``         — grand total
    * ``total_linked_services``    — distinct linked services across estate
    """
    total_copy = 0
    total_df = 0
    total_other = 0
    total_ls = 0

    for plan in plans:
        ri = plan.reasoning_input or {}
        task_counts: dict[str, int] = ri.get("task_counts", {})
        simps = {s.action.value for s in plan.simplifications}
        dropped_items = set()
        for s in plan.simplifications:
            if s.action == SimplificationAction.DROP:
                dropped_items.update(s.items)

        df_tasks = task_counts.get("DataFlowTask", 0)
        copy_like = 0

        # Simplifications that fold data flows into copies reduce DF count
        if "fold_to_copy_activity" in simps:
            folded = max(1, df_tasks)  # at least 1 gets folded
            copy_like += folded
            df_tasks = max(0, df_tasks - folded)

        total_df += df_tasks
        total_copy += max(1, copy_like)  # every pipeline has at least 1 copy

        # Everything else: ExecuteSQL, FileSystem, ForEachLoop, etc.
        other = sum(
            v for k, v in task_counts.items()
            if k != "DataFlowTask"
        )
        total_other += other
        total_ls += len(plan.linked_services)

    return {
        "total_copy_activities": total_copy,
        "total_dataflow_activities": total_df,
        "total_other_activities": total_other,
        "total_activities": total_copy + total_df + total_other,
        "total_linked_services": total_ls,
    }


def estimate_adf_costs(
    *,
    plans: list[MigrationPlan],
    runs_per_day: int = 1,
    avg_copy_diu: float = 4.0,
    avg_copy_minutes: float = 5.0,
    avg_dataflow_minutes: float = 10.0,
    avg_dataflow_vcores: int = 8,
    storage_gb: float = 100.0,
    rates: dict[str, float] | None = None,
) -> dict[str, Any]:
    """Coarse monthly Azure cost projection for the proposed estate.

    **Requires saved MigrationPlans** so the estimate reflects the *agreed*
    design (activity mix, linked-service count, simplifications applied).

    Instead of flat ``avg_activities_per_run`` the function introspects each
    plan's ``reasoning_input.task_counts`` and ``simplifications`` to derive
    per-pipeline Copy vs Data Flow vs orchestration activity counts.

    Returns a per-line-item breakdown plus a monthly total. Annual figures are
    a simple ×12 — sufficient for a budgeting conversation, not a quote.
    """
    rate_table = {**_DEFAULT_RATES, **(rates or {})}
    package_count = len(plans)
    mix = _activity_mix_from_plans(plans)

    runs_per_month = runs_per_day * 30 * max(package_count, 1)

    # Orchestration: all activities × runs
    activities_per_month = runs_per_month * max(mix["total_activities"] / max(package_count, 1), 1)
    orchestration_cost = (activities_per_month / 1000.0) * rate_table["activity_run_per_1k"]

    # Copy: only copy activities contribute DIU·hours
    copy_runs_per_month = runs_per_month * (mix["total_copy_activities"] / max(package_count, 1))
    copy_diu_hours = copy_runs_per_month * (avg_copy_minutes / 60.0) * avg_copy_diu
    copy_cost = copy_diu_hours * rate_table["azure_ir_diu_hour"]

    # Data Flows: only DF activities contribute v-core·hours
    df_runs_per_month = runs_per_month * (mix["total_dataflow_activities"] / max(package_count, 1))
    df_vcore_hours = df_runs_per_month * (avg_dataflow_minutes / 60.0) * avg_dataflow_vcores
    dataflow_cost = df_vcore_hours * rate_table["data_flow_vcore_hour"]

    storage_cost = storage_gb * rate_table["storage_gb_month_hot"]

    # Key Vault: ~2 secret reads per linked service per run
    kv_ops = runs_per_month * mix["total_linked_services"] * 2 / max(package_count, 1)
    kv_cost = (kv_ops / 10_000.0) * rate_table["key_vault_op_per_10k"]

    line_items = [
        {"name": "ADF orchestration (activity runs)", "monthly_usd": round(orchestration_cost, 2),
         "basis": f"{activities_per_month:,.0f} activity runs/mo"},
        {"name": "Copy activity (Azure IR DIU·hours)", "monthly_usd": round(copy_cost, 2),
         "basis": f"{copy_diu_hours:.1f} DIU·hours/mo"},
        {"name": "Mapping Data Flows (v-core·hours)", "monthly_usd": round(dataflow_cost, 2),
         "basis": f"{df_vcore_hours:.1f} v-core·hours/mo"},
        {"name": "ADLS Gen2 hot storage", "monthly_usd": round(storage_cost, 2),
         "basis": f"{storage_gb:.0f} GB"},
        {"name": "Key Vault operations", "monthly_usd": round(kv_cost, 2),
         "basis": f"{kv_ops:,.0f} ops/mo"},
    ]
    monthly_total = round(sum(li["monthly_usd"] for li in line_items), 2)

    return {
        "package_count": package_count,
        "activity_mix": mix,
        "assumptions": {
            "runs_per_day": runs_per_day,
            "avg_copy_diu": avg_copy_diu,
            "avg_copy_minutes": avg_copy_minutes,
            "avg_dataflow_minutes": avg_dataflow_minutes,
            "avg_dataflow_vcores": avg_dataflow_vcores,
            "storage_gb": storage_gb,
            "rates_usd": rate_table,
        },
        "line_items": line_items,
        "monthly_total_usd": monthly_total,
        "annual_total_usd": round(monthly_total * 12, 2),
        "currency": "USD",
        "note": (
            "List-price estimate (US East). Reservations, support tier, egress, "
            "and SHIR VM costs are not included. Override via the rates parameter. "
            "Activity counts derived from saved MigrationPlans."
        ),
    }


# ---------------------------------------------------------------------------
# edit_migration_plan
# ---------------------------------------------------------------------------

_VALID_AUTH = {a.value for a in AuthMode}


class PlanEditError(ValueError):
    """Raised when an edit operation is invalid."""


def edit_migration_plan(plan: MigrationPlan, edits: dict[str, Any]) -> MigrationPlan:
    """Apply structured mutations to a plan and return a new copy.

    Supported edit keys (any subset)::

        {
          "set_auth_mode": "ManagedIdentity",          # bulk update on all linked services
          "set_region": "eastus2",                     # all infrastructure_needed[*].location
          "set_summary": "...",
          "set_target_pattern": "scheduled_file_drop",
          "add_simplification": {"action": "...", "items": [...], "reason": "..."},
          "drop_simplification": "<action_value>",     # remove all matching that action
          "set_customer_decision": {"key": "value"}    # merged into customer_decisions
        }

    Unknown keys raise ``PlanEditError`` (so the agent gets a clear failure
    rather than a silently-ignored typo).
    """
    allowed = {
        "set_auth_mode", "set_region", "set_summary", "set_target_pattern",
        "add_simplification", "drop_simplification", "set_customer_decision",
        "set_name_override", "remove_name_override",
    }
    unknown = set(edits) - allowed
    if unknown:
        raise PlanEditError(f"Unknown edit keys: {sorted(unknown)}")

    # Pydantic v2: model_copy(deep=True) gives us a safe-to-mutate copy.
    new_plan = plan.model_copy(deep=True)

    if "set_auth_mode" in edits:
        mode = edits["set_auth_mode"]
        if mode not in _VALID_AUTH:
            raise PlanEditError(f"Invalid auth mode: {mode!r}. Valid: {sorted(_VALID_AUTH)}")
        for ls in new_plan.linked_services:
            ls.auth = AuthMode(mode)

    if "set_region" in edits:
        region = edits["set_region"]
        for infra in new_plan.infrastructure_needed:
            infra.location = region

    if "set_summary" in edits:
        new_plan.summary = str(edits["set_summary"])

    if "set_target_pattern" in edits:
        from .models import TargetPattern
        try:
            new_plan.target_pattern = TargetPattern(edits["set_target_pattern"])
        except ValueError as exc:
            raise PlanEditError(f"Invalid target_pattern: {edits['set_target_pattern']!r}") from exc

    if "add_simplification" in edits:
        spec = edits["add_simplification"]
        try:
            simp = Simplification(
                action=SimplificationAction(spec["action"]),
                items=list(spec.get("items", [])),
                reason=spec.get("reason", ""),
                confidence=float(spec.get("confidence", 0.8)),
            )
        except (KeyError, ValueError) as exc:
            raise PlanEditError(f"Invalid add_simplification payload: {exc}") from exc
        new_plan.simplifications.append(simp)

    if "drop_simplification" in edits:
        action = edits["drop_simplification"]
        new_plan.simplifications = [
            s for s in new_plan.simplifications if s.action.value != action
        ]

    if "set_customer_decision" in edits:
        decision = edits["set_customer_decision"]
        if not isinstance(decision, dict):
            raise PlanEditError("set_customer_decision must be a dict")
        new_plan.customer_decisions.update(decision)

    if "set_name_override" in edits:
        override = edits["set_name_override"]
        if not isinstance(override, dict):
            raise PlanEditError("set_name_override must be a dict, e.g. {'LS:MyConn': 'LS_Custom'}")
        _valid_prefixes = ("LS:", "DS:", "DF:", "PL", "TR")
        for key in override:
            if not any(key.upper().startswith(p) for p in _valid_prefixes):
                raise PlanEditError(
                    f"Invalid name_override key: {key!r}. "
                    f"Must start with one of: {', '.join(_valid_prefixes)}"
                )
        new_plan.name_overrides.update(override)

    if "remove_name_override" in edits:
        key_to_remove = edits["remove_name_override"]
        if isinstance(key_to_remove, str):
            new_plan.name_overrides.pop(key_to_remove, None)
        elif isinstance(key_to_remove, list):
            for k in key_to_remove:
                new_plan.name_overrides.pop(k, None)
        else:
            raise PlanEditError("remove_name_override must be a string or list of strings")

    return new_plan
