"""Estate-scale convert / wave-plan / cost-estimate / mutate-plan helpers.

These are all small, deterministic functions consumed by the MCP server.
They sit in the migration_plan package because they all operate on (or produce)
``MigrationPlan`` instances or ``bulk_analyze``-style estate reports.
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
# plan_migration_waves
# ---------------------------------------------------------------------------

def plan_migration_waves(
    estate_report: dict[str, Any],
    *,
    max_packages_per_wave: int = 10,
) -> dict[str, Any]:
    """Group an estate into ordered migration waves.

    Strategy (deliberately simple, deterministic, easy to override):

    * **Wave 1** — bulk-convertible (low / medium complexity), grouped by
      target_pattern so each wave shares linked services and reviewer context.
    * **Wave 2..N** — design-review-needed (high / very_high), grouped by
      target_pattern, each wave capped at ``max_packages_per_wave`` so a single
      reviewer can hold the design conversation in their head.
    * Failures from bulk_analyze (parse errors) become a final ``triage`` wave.

    The output mirrors the bulk_analyze report shape so the agent can hand it
    straight to a customer.
    """
    packages = list(estate_report.get("packages", []))
    failures = list(estate_report.get("failures", []))

    bulk = [p for p in packages if p.get("complexity_bucket") in ("low", "medium")]
    review = [p for p in packages if p.get("complexity_bucket") in ("high", "very_high")]

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
                "estimated_hours": round(sum(p.get("estimated_total_hours", 0) for p in chunk), 1),
                "target_pattern": pattern,
                "packages": [p["package_name"] for p in chunk],
            })
            wave_num += 1

    # Wave 2..N: design-review-needed, grouped by pattern, sorted hardest-first
    by_pattern_review: dict[str, list[dict]] = defaultdict(list)
    for p in review:
        by_pattern_review[p.get("target_pattern", "custom")].append(p)
    for pattern, pkgs in sorted(by_pattern_review.items()):
        pkgs_sorted = sorted(pkgs, key=lambda p: -p.get("complexity_score", 0))
        for chunk_start in range(0, len(pkgs_sorted), max_packages_per_wave):
            chunk = pkgs_sorted[chunk_start:chunk_start + max_packages_per_wave]
            waves.append({
                "wave": wave_num,
                "label": f"Design review — {pattern}",
                "strategy": "design_review",
                "package_count": len(chunk),
                "estimated_hours": round(sum(p.get("estimated_total_hours", 0) for p in chunk), 1),
                "target_pattern": pattern,
                "packages": [p["package_name"] for p in chunk],
            })
            wave_num += 1

    if failures:
        waves.append({
            "wave": wave_num,
            "label": "Triage — parse failures",
            "strategy": "triage",
            "package_count": len(failures),
            "estimated_hours": 0.0,
            "target_pattern": None,
            "packages": [f.get("path", "?") for f in failures],
        })

    return {
        "wave_count": len(waves),
        "total_packages": len(packages) + len(failures),
        "total_estimated_hours": round(sum(w["estimated_hours"] for w in waves), 1),
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


def estimate_adf_costs(
    *,
    estate_report: dict[str, Any] | None = None,
    plans: list[MigrationPlan] | None = None,
    runs_per_day: int = 1,
    avg_activities_per_run: int = 6,
    avg_copy_diu: float = 4.0,
    avg_copy_minutes: float = 5.0,
    avg_dataflow_minutes: float = 0.0,
    avg_dataflow_vcores: int = 8,
    storage_gb: float = 100.0,
    rates: dict[str, float] | None = None,
) -> dict[str, Any]:
    """Coarse monthly Azure cost projection for the proposed estate.

    Inputs come from ``bulk_analyze`` (estate_report) and/or a list of
    ``MigrationPlan``s. We avoid live pricing API calls so the tool stays
    offline-friendly; pass ``rates`` to override the documented defaults.

    Returns a per-line-item breakdown plus a monthly total. Annual figures are
    a simple ×12 — sufficient for a budgeting conversation, not a quote.
    """
    rate_table = {**_DEFAULT_RATES, **(rates or {})}

    if estate_report is not None:
        package_count = estate_report.get("package_count", 0)
    elif plans is not None:
        package_count = len(plans)
    else:
        package_count = 1

    runs_per_month = runs_per_day * 30 * max(package_count, 1)
    activities_per_month = runs_per_month * avg_activities_per_run

    orchestration_cost = (activities_per_month / 1000.0) * rate_table["activity_run_per_1k"]

    copy_diu_hours = runs_per_month * (avg_copy_minutes / 60.0) * avg_copy_diu
    copy_cost = copy_diu_hours * rate_table["azure_ir_diu_hour"]

    df_vcore_hours = runs_per_month * (avg_dataflow_minutes / 60.0) * avg_dataflow_vcores
    dataflow_cost = df_vcore_hours * rate_table["data_flow_vcore_hour"]

    storage_cost = storage_gb * rate_table["storage_gb_month_hot"]

    # Key Vault: assume ~2 secret reads per pipeline run for MI-bootstrapped LSes
    kv_ops = runs_per_month * 2
    kv_cost = (kv_ops / 10_000.0) * rate_table["key_vault_op_per_10k"]

    line_items = [
        {"name": "ADF orchestration (activity runs)", "monthly_usd": round(orchestration_cost, 2),
         "basis": f"{activities_per_month:,} activity runs/mo"},
        {"name": "Copy activity (Azure IR DIU·hours)", "monthly_usd": round(copy_cost, 2),
         "basis": f"{copy_diu_hours:.1f} DIU·hours/mo"},
        {"name": "Mapping Data Flows (v-core·hours)", "monthly_usd": round(dataflow_cost, 2),
         "basis": f"{df_vcore_hours:.1f} v-core·hours/mo"},
        {"name": "ADLS Gen2 hot storage", "monthly_usd": round(storage_cost, 2),
         "basis": f"{storage_gb:.0f} GB"},
        {"name": "Key Vault operations", "monthly_usd": round(kv_cost, 2),
         "basis": f"{kv_ops:,} ops/mo"},
    ]
    monthly_total = round(sum(li["monthly_usd"] for li in line_items), 2)

    return {
        "package_count": package_count,
        "assumptions": {
            "runs_per_day": runs_per_day,
            "avg_activities_per_run": avg_activities_per_run,
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
            "and SHIR VM costs are not included. Override via the rates parameter."
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

    return new_plan
