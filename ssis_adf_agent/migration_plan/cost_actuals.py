"""
Cost-actuals join helper (P4-5).

Joins a converted estate's prediction (from :func:`estimate_adf_costs`) and
its deployment lineage (from M1's ``lineage.json``) against actual Azure
Cost Management spend, and emits a per-factory / per-pipeline variance
report.

What this module does **not** do:

* It does not call Azure. The caller is responsible for fetching the Cost
  Management data (REST query response *or* a portal CSV export) — every
  enterprise has different RBAC / data-residency rules around that call,
  so we keep the I/O boundary outside the helper. Tests pass captured
  fixtures.
* It does not claim *billed* per-pipeline accuracy. Cost Management does
  not break ADF spend down to the pipeline level — the smallest billing
  unit is the factory. The report includes an **estimated** per-pipeline
  allocation that splits the factory total proportionally to converted
  activity counts (from ``lineage.json``). Every per-pipeline row is
  flagged ``allocation: "estimated"`` so consumers do not confuse it with
  invoiced spend.

Inputs accepted for ``actuals_source``:

* ``dict`` — a Cost Management Query REST response (top-level keys
  ``properties.columns`` + ``properties.rows``).
* ``Path`` / ``str`` ending in ``.json`` — same response written to disk.
* ``Path`` / ``str`` ending in ``.csv`` — a portal Cost Analysis CSV
  export with at least ``ResourceId`` + ``Cost`` (or ``PreTaxCost``)
  columns.
* ``list[dict]`` — pre-normalized rows ``{"resource_id": ..., "cost": ...,
  "currency": ..., "service": ..., "meter": ...}``.
"""

from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Iterable


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ActualRow:
    """One normalized cost row joined back to a resource."""
    resource_id: str
    cost: float
    currency: str = "USD"
    service: str = ""
    meter: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class VarianceReport:
    """Per-factory variance plus optional per-pipeline allocation."""
    factory_resource_id: str
    period_label: str
    actuals_total_usd: float
    estimate_monthly_usd: float
    variance_usd: float
    variance_pct: float | None
    actuals_by_meter: dict[str, float] = field(default_factory=dict)
    pipelines: list[dict[str, Any]] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "factory_resource_id": self.factory_resource_id,
            "period_label": self.period_label,
            "actuals_total_usd": round(self.actuals_total_usd, 2),
            "estimate_monthly_usd": round(self.estimate_monthly_usd, 2),
            "variance_usd": round(self.variance_usd, 2),
            "variance_pct": (round(self.variance_pct, 2)
                             if self.variance_pct is not None else None),
            "actuals_by_meter": {
                k: round(v, 2) for k, v in self.actuals_by_meter.items()
            },
            "pipelines": self.pipelines,
            "notes": self.notes,
        }


# ---------------------------------------------------------------------------
# Loader for actuals
# ---------------------------------------------------------------------------


def _normalize_cost_management_rest(payload: dict[str, Any]) -> list[ActualRow]:
    """Normalize a Cost Management Query REST response into ActualRow list."""
    props = payload.get("properties") or payload  # tolerate already-unwrapped
    columns = props.get("columns") or []
    rows = props.get("rows") or []
    if not columns or not rows:
        return []

    # Build case-insensitive column index.
    idx: dict[str, int] = {}
    for i, col in enumerate(columns):
        name = (col.get("name") or "").lower()
        if name:
            idx[name] = i

    cost_key = next((k for k in ("pretaxcost", "cost", "costusd") if k in idx), None)
    rid_key = next((k for k in ("resourceid",) if k in idx), None)
    if cost_key is None or rid_key is None:
        raise ValueError(
            "Cost Management response missing required columns "
            "(need ResourceId + PreTaxCost/Cost; got "
            f"{[c.get('name') for c in columns]})"
        )
    cur_key = "currency" if "currency" in idx else None
    svc_key = next((k for k in ("servicename", "metercategory") if k in idx), None)
    meter_key = next((k for k in ("meter", "metername", "metersubcategory")
                      if k in idx), None)

    out: list[ActualRow] = []
    for row in rows:
        try:
            rid = str(row[idx[rid_key]] or "")
            cost = float(row[idx[cost_key]] or 0.0)
        except (IndexError, TypeError, ValueError):
            continue
        if not rid:
            continue
        out.append(ActualRow(
            resource_id=rid,
            cost=cost,
            currency=str(row[idx[cur_key]]) if cur_key is not None else "USD",
            service=str(row[idx[svc_key]]) if svc_key is not None else "",
            meter=str(row[idx[meter_key]]) if meter_key is not None else "",
        ))
    return out


def _normalize_csv(path: Path) -> list[ActualRow]:
    """Parse a portal Cost Analysis CSV export."""
    out: list[ActualRow] = []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        # Find columns case-insensitively.
        if reader.fieldnames is None:
            return []
        fmap = {(name or "").lower(): name for name in reader.fieldnames}
        rid_col = next((fmap[k] for k in ("resourceid", "resource id",
                                          "instanceid", "instance id")
                        if k in fmap), None)
        cost_col = next((fmap[k] for k in ("pretaxcost", "cost", "costusd",
                                           "cost in billing currency",
                                           "costinbillingcurrency")
                         if k in fmap), None)
        if not rid_col or not cost_col:
            raise ValueError(
                "CSV missing required columns (need ResourceId + Cost; got "
                f"{reader.fieldnames})"
            )
        cur_col = next((fmap[k] for k in ("currency", "billingcurrency",
                                          "billing currency") if k in fmap), None)
        svc_col = next((fmap[k] for k in ("servicename", "service name",
                                          "metercategory", "meter category")
                        if k in fmap), None)
        meter_col = next((fmap[k] for k in ("meter", "metername", "meter name",
                                            "metersubcategory")
                          if k in fmap), None)

        for row in reader:
            rid = (row.get(rid_col) or "").strip()
            if not rid:
                continue
            try:
                cost = float(row.get(cost_col) or 0.0)
            except ValueError:
                continue
            out.append(ActualRow(
                resource_id=rid,
                cost=cost,
                currency=(row.get(cur_col) or "USD") if cur_col else "USD",
                service=(row.get(svc_col) or "") if svc_col else "",
                meter=(row.get(meter_col) or "") if meter_col else "",
            ))
    return out


def load_actuals(source: Any) -> list[ActualRow]:
    """Coerce ``source`` into a list of :class:`ActualRow`.

    Accepts: dict (CM REST response), JSON path, CSV path, or pre-normalized
    list of dicts.
    """
    if isinstance(source, dict):
        return _normalize_cost_management_rest(source)
    if isinstance(source, list):
        rows: list[ActualRow] = []
        for r in source:
            if isinstance(r, ActualRow):
                rows.append(r)
                continue
            if not isinstance(r, dict):
                raise TypeError(f"Unsupported row type: {type(r).__name__}")
            rows.append(ActualRow(
                resource_id=str(r.get("resource_id") or r.get("ResourceId") or ""),
                cost=float(r.get("cost") or r.get("Cost") or 0.0),
                currency=str(r.get("currency") or "USD"),
                service=str(r.get("service") or ""),
                meter=str(r.get("meter") or ""),
            ))
        return [r for r in rows if r.resource_id]
    if isinstance(source, (str, Path)):
        path = Path(source)
        if not path.is_file():
            raise FileNotFoundError(f"Actuals source not found: {path}")
        suffix = path.suffix.lower()
        if suffix == ".json":
            payload = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                raise ValueError(
                    "JSON actuals source must be a Cost Management REST response object"
                )
            return _normalize_cost_management_rest(payload)
        if suffix == ".csv":
            return _normalize_csv(path)
        raise ValueError(f"Unsupported actuals file type: {suffix}")
    raise TypeError(f"Unsupported actuals source type: {type(source).__name__}")


# ---------------------------------------------------------------------------
# Lineage helpers
# ---------------------------------------------------------------------------


def _factory_id_from_pipeline_id(pipeline_resource_id: str) -> str:
    """Strip ``/pipelines/<name>`` (or any sub-resource) off a factory child id.

    Returns ``""`` if the input does not look like a factory child.
    """
    marker = "/providers/Microsoft.DataFactory/factories/"
    i = pipeline_resource_id.lower().find(marker.lower())
    if i < 0:
        return ""
    # Take everything up through the factory name itself.
    rest = pipeline_resource_id[i + len(marker):]
    factory_name = rest.split("/", 1)[0]
    return pipeline_resource_id[: i + len(marker)] + factory_name


def _extract_pipeline_rows(lineage: dict[str, Any]) -> list[dict[str, Any]]:
    """Return the pipeline-level rows from a lineage manifest (1+).

    Tolerates both the canonical ``pipeline: [ {...} ]`` shape and the
    legacy ``pipeline: {...}`` single-dict shape.
    """
    artifacts = (lineage.get("artifacts") or {})
    raw = artifacts.get("pipeline")
    if raw is None:
        return []
    if isinstance(raw, dict):
        return [raw]
    if isinstance(raw, list):
        return [r for r in raw if isinstance(r, dict)]
    return []


def _factory_id_from_lineage(lineage: dict[str, Any]) -> str:
    """Pull the parent factory resource id out of the manifest."""
    for row in _extract_pipeline_rows(lineage):
        rid = row.get("azure_resource_id") or ""
        fid = _factory_id_from_pipeline_id(rid)
        if fid:
            return fid
    # Try linked services / datasets too in case pipeline wasn't deployed.
    for key in ("linked_services", "datasets", "data_flows", "triggers"):
        for row in (lineage.get("artifacts") or {}).get(key) or []:
            fid = _factory_id_from_pipeline_id(row.get("azure_resource_id") or "")
            if fid:
                return fid
    return ""


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def compare_estimates_to_actuals(
    *,
    lineage_path: str | Path,
    actuals_source: Any,
    estimate: dict[str, Any] | None = None,
    period_label: str = "",
    factory_resource_id: str | None = None,
) -> dict[str, Any]:
    """Join a converted estate's prediction + lineage with Cost Management actuals.

    Args:
        lineage_path: Path to the ``lineage.json`` produced by the converter
            (post-deploy, so ``azure_resource_id`` is populated).
        actuals_source: See module docstring. dict / list / CSV / JSON file.
        estimate: Optional dict returned by :func:`estimate_adf_costs`. When
            present, its ``monthly_total_usd`` becomes the estimate baseline.
            If omitted, ``estimate_monthly_usd`` is reported as ``0.0`` and
            ``variance_pct`` is ``None``.
        period_label: Caller-supplied label for the cost period (e.g.
            ``"2026-03"``). Echoed back in the report; not interpreted.
        factory_resource_id: Override factory ARM id. When omitted, derived
            from the lineage manifest.

    Returns the :class:`VarianceReport` as a plain dict.
    """
    lineage_path = Path(lineage_path)
    if not lineage_path.is_file():
        raise FileNotFoundError(f"lineage.json not found: {lineage_path}")
    lineage = json.loads(lineage_path.read_text(encoding="utf-8"))

    factory_id = factory_resource_id or _factory_id_from_lineage(lineage)
    notes: list[str] = []
    if not factory_id:
        notes.append(
            "No factory resource id found in lineage.json — pass "
            "`factory_resource_id` explicitly or run deploy_to_adf first."
        )

    rows = load_actuals(actuals_source)

    # Filter actuals to this factory (case-insensitive, prefix match so
    # sub-resource ids like .../factories/foo/integrationruntimes/bar still
    # roll up).
    factory_rows: list[ActualRow] = []
    if factory_id:
        needle = factory_id.lower()
        for r in rows:
            if r.resource_id.lower().startswith(needle):
                factory_rows.append(r)
        if not factory_rows:
            notes.append(
                f"Actuals contained {len(rows)} rows, none matched factory id "
                f"{factory_id}."
            )

    actuals_total = sum(r.cost for r in factory_rows)
    actuals_by_meter: dict[str, float] = {}
    for r in factory_rows:
        key = r.meter or r.service or "unspecified"
        actuals_by_meter[key] = actuals_by_meter.get(key, 0.0) + r.cost

    # Currency check — refuse to mix.
    currencies = {r.currency.upper() for r in factory_rows if r.currency}
    if len(currencies) > 1:
        notes.append(
            f"Multiple currencies in actuals ({sorted(currencies)}); totals "
            "are summed without FX conversion."
        )

    estimate_monthly = 0.0
    if estimate is not None:
        try:
            estimate_monthly = float(estimate.get("monthly_total_usd") or 0.0)
        except (TypeError, ValueError):
            estimate_monthly = 0.0

    variance = actuals_total - estimate_monthly
    variance_pct: float | None = None
    if estimate_monthly > 0:
        variance_pct = (variance / estimate_monthly) * 100.0

    # Per-pipeline allocation (estimated, not billed).
    pipeline_rows = _extract_pipeline_rows(lineage)
    weights: list[tuple[str, int, str]] = []  # (name, activity_count, resource_id)
    for row in pipeline_rows:
        weights.append((
            str(row.get("name") or ""),
            int(row.get("activity_count") or 0),
            str(row.get("azure_resource_id") or ""),
        ))
    total_weight = sum(w for _, w, _ in weights)
    pipelines_out: list[dict[str, Any]] = []
    if total_weight > 0 and actuals_total > 0:
        for name, w, rid in weights:
            share = w / total_weight
            pipelines_out.append({
                "pipeline_name": name,
                "azure_resource_id": rid,
                "activity_count": w,
                "weight": round(share, 4),
                "allocated_actuals_usd": round(actuals_total * share, 2),
                "allocation": "estimated",
            })
        notes.append(
            "Per-pipeline allocation is estimated by activity-count weight; "
            "Cost Management does not invoice ADF spend below factory granularity."
        )
    elif pipeline_rows and total_weight == 0:
        notes.append(
            "Pipeline rows had zero combined activity_count — skipping "
            "per-pipeline allocation."
        )

    report = VarianceReport(
        factory_resource_id=factory_id,
        period_label=period_label,
        actuals_total_usd=actuals_total,
        estimate_monthly_usd=estimate_monthly,
        variance_usd=variance,
        variance_pct=variance_pct,
        actuals_by_meter=actuals_by_meter,
        pipelines=pipelines_out,
        notes=notes,
    )
    return report.to_dict()


__all__ = [
    "ActualRow",
    "VarianceReport",
    "compare_estimates_to_actuals",
    "load_actuals",
]
