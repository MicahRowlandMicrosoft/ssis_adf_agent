"""Fabric Capacity Unit (CU) cost projection.

Microsoft Fabric bills compute as **Capacity Units** consumed per second by
running workloads. Unlike ADF's per-activity / per-DIU·hour / per-vCore·hour
model, Fabric is a *reserved-capacity* SKU: customers buy a fixed
F-skew (F2, F4, F8, F16, F32, F64, F128, F256, F512, F1024, F2048) and
their workload either fits within the purchased CUs or gets throttled.

This module projects the *peak CU·second consumption* of a converted estate
and translates it into:

  - The smallest F-SKU that comfortably fits the workload (sized at 70%
    headroom by default — Fabric throttles workloads that exceed 100% of
    purchased CUs sustained over the smoothing window).
  - The implied monthly cost of that SKU at list price.
  - A second-source comparison against pay-as-you-go (PAYG) hourly rates so
    customers can decide whether to commit to a reservation.

What this module does NOT do:
  - It does not model pause/resume schedules, OneLake storage, or per-item
    feature billing (Eventstream, Lakehouse compute beyond pipelines/notebooks).
  - It does not make a real Azure pricing-API call. List-price assumptions
    are documented in ``_DEFAULT_FABRIC_RATES`` and can be overridden via
    the ``rates`` argument; pricing changes faster than this code does.

The cost shape is intentionally similar to ``estimate_adf_costs`` so the
``estimate_costs`` MCP tool can dispatch by ``plan.target`` and return a
comparable shape regardless of platform.
"""
from __future__ import annotations

from typing import Any

from .models import (
    MigrationPlan,
    MigrationTarget,
    SimplificationAction,
)


# ---------------------------------------------------------------------------
# List-price assumptions — west US, USD, list price as of 2025
# ---------------------------------------------------------------------------

# F-SKU monthly list price (USD) at PAYG rates, computed as PAYG hourly × 730.
# Source: Microsoft Fabric pricing page; reservation savings ~40% are NOT
# applied here so the projection is conservative.
_FABRIC_F_SKU_MONTHLY_USD = {
    2: 262.80,       # F2:    $0.36/hr × 730
    4: 525.60,       # F4:    $0.72/hr × 730
    8: 1051.20,      # F8:    $1.44/hr × 730
    16: 2102.40,     # F16:   $2.88/hr × 730
    32: 4204.80,     # F32:   $5.76/hr × 730
    64: 8409.60,     # F64:   $11.52/hr × 730
    128: 16819.20,   # F128:  $23.04/hr × 730
    256: 33638.40,   # F256:  $46.08/hr × 730
    512: 67276.80,   # F512:  $92.16/hr × 730
    1024: 134553.60, # F1024
    2048: 269107.20, # F2048
}

_DEFAULT_FABRIC_RATES = {
    # CU·seconds per unit of work — coarse calibration that maps roughly to
    # Microsoft's published guidance: Pipelines are cheap orchestration
    # (~12 CU·s per activity), Notebooks dominate (driver + executor scaling).
    "cu_seconds_per_pipeline_activity": 12.0,
    "cu_seconds_per_notebook_minute_per_executor": 60.0,  # 1 CU·s per second per executor
    # Default executor count for a notebook stub — small Spark pool sized for
    # SSIS-replacement workloads (4 executors at 4 v-cores each).
    "default_notebook_executors": 4,
    # OneLake storage (delta tables, file landing). Cheap and meter-billed
    # separately from CU.
    "onelake_storage_gb_month": 0.023,
    # Smoothing window (minutes) — Fabric averages CU consumption over this
    # window. Sustained excess gets throttled.
    "smoothing_window_minutes": 5.0,
}


def _f_sku_for_cu(peak_cu: float, headroom_pct: float = 30.0) -> int | None:
    """Pick the smallest F-SKU whose CU capacity is ≥ peak_cu × (1 + headroom).

    Returns None if no SKU fits (workload exceeds F2048).
    """
    if peak_cu <= 0:
        return 2  # F2 minimum
    target = peak_cu * (1.0 + headroom_pct / 100.0)
    for sku in sorted(_FABRIC_F_SKU_MONTHLY_USD):
        if sku >= target:
            return sku
    return None


def _activity_mix_from_plans(plans: list[MigrationPlan]) -> dict[str, int]:
    """Aggregate per-pipeline activity counts across the estate.

    For Fabric, the relevant split is:
      - notebook_activities: Data Flow Tasks that became PySpark notebooks
      - other_activities: every other control-flow / Copy / SP activity
    Fabric does not bill separately for Copy vs orchestration the way ADF does.
    """
    notebooks = 0
    others = 0
    for plan in plans:
        ri = plan.reasoning_input or {}
        task_counts = ri.get("task_counts", {})
        # Folded data flows -> Copy (still counts as 'other' in Fabric)
        simps = {s.action.value for s in plan.simplifications}
        df_tasks = task_counts.get("DataFlowTask", 0)
        if "fold_to_copy_activity" in simps:
            df_tasks = max(0, df_tasks - max(1, df_tasks))
        notebooks += df_tasks
        others += sum(v for k, v in task_counts.items() if k != "DataFlowTask")
        others += max(0, task_counts.get("DataFlowTask", 0) - df_tasks)  # folded copies
    return {
        "notebook_activities": notebooks,
        "other_activities": others,
        "total_activities": notebooks + others,
    }


def estimate_fabric_costs(
    *,
    plans: list[MigrationPlan],
    runs_per_day: int = 1,
    avg_notebook_minutes: float = 10.0,
    notebook_executors: int | None = None,
    onelake_storage_gb: float = 100.0,
    headroom_pct: float = 30.0,
    rates: dict[str, float] | None = None,
) -> dict[str, Any]:
    """Coarse monthly Fabric Capacity Unit cost projection for the estate.

    Args:
        plans: Saved MigrationPlans (mixed targets allowed; non-Fabric plans
            are still costed because the workload — Copy + notebooks — still
            consumes CU). Caller should ensure plans match the intended target.
        runs_per_day: Number of times the estate runs daily.
        avg_notebook_minutes: Average notebook duration. The biggest unknown
            in Fabric pricing — a Spark notebook that runs 1 minute consumes
            far less CU than the same notebook running 30 minutes.
        notebook_executors: Spark pool executor count. Defaults to the rate
            table's ``default_notebook_executors`` (4 — sized for SSIS
            replacement workloads).
        onelake_storage_gb: OneLake storage assumption, GB.
        headroom_pct: Buffer above peak CU for SKU sizing. 30% means the
            recommended SKU has 30% room above peak (Fabric throttles at
            sustained 100%, so leave headroom for spikes).
        rates: Override the default rate table.

    Returns: a per-line-item breakdown plus a recommended F-SKU and
        monthly/annual totals. Shape is parallel to ``estimate_adf_costs`` so
        the MCP layer can present them side-by-side.
    """
    rate_table = {**_DEFAULT_FABRIC_RATES, **(rates or {})}
    executors = (
        notebook_executors
        if notebook_executors is not None
        else int(rate_table["default_notebook_executors"])
    )
    package_count = len(plans)
    mix = _activity_mix_from_plans(plans)

    runs_per_month = runs_per_day * 30 * max(package_count, 1)

    # Per-run CU·seconds consumption
    per_run_pipeline_cu_s = (
        mix["other_activities"] / max(package_count, 1)
    ) * rate_table["cu_seconds_per_pipeline_activity"]

    per_run_notebook_cu_s = (
        (mix["notebook_activities"] / max(package_count, 1))
        * avg_notebook_minutes
        * executors
        * rate_table["cu_seconds_per_notebook_minute_per_executor"]
    )

    per_run_total_cu_s = per_run_pipeline_cu_s + per_run_notebook_cu_s

    # Peak CU: assume one full run can land within the smoothing window.
    # peak_cu_per_second = total_cu_s / smoothing_window_seconds
    smoothing_seconds = rate_table["smoothing_window_minutes"] * 60.0
    peak_cu = per_run_total_cu_s / max(smoothing_seconds, 1.0)

    recommended_sku = _f_sku_for_cu(peak_cu, headroom_pct=headroom_pct)
    if recommended_sku is None:
        sku_cost = float("inf")
        sku_label = "exceeds-F2048"
    else:
        sku_cost = _FABRIC_F_SKU_MONTHLY_USD[recommended_sku]
        sku_label = f"F{recommended_sku}"

    # OneLake storage line item
    storage_cost = onelake_storage_gb * rate_table["onelake_storage_gb_month"]

    # PAYG comparison: total CU·s consumed in a month / 3600 = CU·hours
    # Then ÷ recommended_sku to get sku-hours, ÷ 730 to get fraction of month.
    monthly_cu_seconds = per_run_total_cu_s * runs_per_month
    monthly_cu_hours = monthly_cu_seconds / 3600.0
    payg_hourly_rate_per_cu = (
        _FABRIC_F_SKU_MONTHLY_USD[2] / 730.0 / 2.0  # F2 = 2 CUs, derive per-CU rate
    )
    payg_monthly_cost = monthly_cu_hours * payg_hourly_rate_per_cu

    line_items = [
        {
            "name": f"Fabric reserved capacity ({sku_label})",
            "monthly_usd": round(sku_cost, 2) if sku_cost != float("inf") else None,
            "basis": (
                f"{recommended_sku} CUs × $/hr × 730 hours/month — sized at "
                f"{headroom_pct:.0f}% headroom over peak {peak_cu:.2f} CU."
                if recommended_sku else "Workload exceeds F2048 — split capacity required."
            ),
        },
        {
            "name": "OneLake storage",
            "monthly_usd": round(storage_cost, 2),
            "basis": f"{onelake_storage_gb:.0f} GB",
        },
        {
            "name": "Pay-as-you-go alternative",
            "monthly_usd": round(payg_monthly_cost, 2),
            "basis": (
                f"{monthly_cu_hours:,.1f} CU·hours/mo × ${payg_hourly_rate_per_cu:.4f}/CU·hr "
                "(use this if workload is bursty enough that reservation underutilization "
                "exceeds reservation discount)."
            ),
        },
    ]
    monthly_total = (
        round(sku_cost + storage_cost, 2)
        if recommended_sku else None
    )

    return {
        "platform": "fabric",
        "package_count": package_count,
        "activity_mix": mix,
        "assumptions": {
            "runs_per_day": runs_per_day,
            "avg_notebook_minutes": avg_notebook_minutes,
            "notebook_executors": executors,
            "onelake_storage_gb": onelake_storage_gb,
            "headroom_pct": headroom_pct,
            "rates_usd": rate_table,
        },
        "peak_cu": round(peak_cu, 4),
        "recommended_sku": sku_label,
        "line_items": line_items,
        "monthly_total_usd": monthly_total,
        "annual_total_usd": (
            round(monthly_total * 12, 2) if monthly_total is not None else None
        ),
        "currency": "USD",
        "notes": [
            (
                "Fabric reservations save ~40% vs PAYG; this estimate uses PAYG "
                "list price as the conservative baseline. Apply customer-specific "
                "reservation discount to the reserved-capacity line item."
            ),
            (
                "Notebook cost is the largest unknown — actual CU·s depends on "
                "Spark pool size, transformation complexity, and shuffle volume. "
                "Re-run after first production run with measured executor minutes."
            ),
        ],
    }


def estimate_costs_dispatch(
    *,
    plans: list[MigrationPlan],
    target: MigrationTarget | None = None,
    **kwargs: Any,
) -> dict[str, Any]:
    """Dispatch to the appropriate platform cost projector.

    If ``target`` is None, infer from the first plan's ``target`` field. If
    plans target mixed platforms, the caller is responsible for splitting.
    """
    if target is None:
        target = plans[0].target if plans else MigrationTarget.ADF
    if target == MigrationTarget.FABRIC:
        # Filter only Fabric kwargs so estimate_adf_costs args don't leak in.
        fabric_kwargs = {
            k: v for k, v in kwargs.items()
            if k in {
                "runs_per_day", "avg_notebook_minutes", "notebook_executors",
                "onelake_storage_gb", "headroom_pct", "rates",
            }
        }
        return estimate_fabric_costs(plans=plans, **fabric_kwargs)
    # Default to ADF
    from .estate_tools import estimate_adf_costs
    adf_kwargs = {
        k: v for k, v in kwargs.items()
        if k in {
            "runs_per_day", "avg_copy_diu", "avg_copy_minutes",
            "avg_dataflow_minutes", "avg_dataflow_vcores", "storage_gb", "rates",
        }
    }
    return estimate_adf_costs(plans=plans, **adf_kwargs)
