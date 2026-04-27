"""Estate-level deduplication & consolidation analyzer.

Given a set of parsed packages (and optionally their proposed plans), identifies:

1. **Deduplication** — the safe, high-confidence case: linked services that
   appear in many packages and should be authored once and shared via
   ``shared_artifacts_dir``.  These savings are mechanical and do not require
   user judgment, so the analyzer reports them as a recommended action.

2. **Consolidation** — the trickier case: groups of packages that *could* be
   merged into a single parameterized pipeline (same shape, varying
   table/file names).  Consolidation is a customer-judgment call (it changes
   ownership, alerting, and lineage), so the analyzer reports candidates with
   tradeoffs and projected savings rather than recommending action.

This module is pure analysis — it never edits plans or files.
"""
from __future__ import annotations

from collections import defaultdict
from typing import Any, Iterable

from ..analyzers.similarity_analyzer import (
    PackageFingerprint,
    fingerprint_package,
    group_similar_packages,
)
from ..parsers.models import (
    ConnectionManagerType,
    SSISPackage,
)


# Hours of hand-authoring per duplicate linked service that would be saved by
# using ``shared_artifacts_dir`` (matches the per-LS estimate in the effort
# model: ~0.5h per LS).
_HOURS_SAVED_PER_DUPLICATE_LS = 0.5

# Hours saved per package that would be folded into a consolidated, parameterized
# pipeline.  Conservative because consolidation requires a parent driver
# pipeline + a parameter table, which is real work the first time.
_HOURS_SAVED_PER_CONSOLIDATED_PACKAGE = 4.0
_HOURS_TO_BUILD_CONSOLIDATED_PIPELINE = 8.0


# ---------------------------------------------------------------------------
# Deduplication: shared linked services across packages
# ---------------------------------------------------------------------------

def _cm_dedup_key(cm) -> tuple[str, str, str, str] | None:
    """Identity of a connection manager for dedup purposes.

    Two connection managers with the same key are safe to collapse into a
    single ADF linked service.  We deliberately ignore the SSIS-side `name`
    because the same physical endpoint often has slightly different friendly
    names across packages (`AdventureWorks2016`, `AdventureWorks 2016`, etc.).
    """
    cm_type = cm.type.value if hasattr(cm.type, "value") else str(cm.type or "")
    if not cm_type or cm_type == ConnectionManagerType.UNKNOWN.value:
        return None
    server = (cm.server or "").lower().strip()
    database = (cm.database or "").lower().strip()
    file_path = (cm.file_path or "").lower().strip()
    # SQL-flavoured connection: identity is server + database
    if cm.type in (
        ConnectionManagerType.OLEDB,
        ConnectionManagerType.ADO_NET,
        ConnectionManagerType.ODBC,
    ):
        if not server:
            return None
        return (cm_type, server, database, "")
    # File / Excel / Flat file: identity is the file path (or directory)
    if file_path:
        return (cm_type, "", "", file_path)
    # Otherwise (SMTP / FTP / HTTP / Cache): use the first non-empty hint
    fallback = server or database or ""
    if not fallback:
        return None
    return (cm_type, fallback, "", "")


def _human_label(key: tuple[str, str, str, str]) -> str:
    cm_type, server, database, file_path = key
    if file_path:
        return f"{cm_type} :: {file_path}"
    if server and database:
        return f"{cm_type} :: {server}/{database}"
    if server:
        return f"{cm_type} :: {server}"
    return cm_type


def find_dedup_candidates(packages: list[SSISPackage]) -> list[dict[str, Any]]:
    """Find linked services that are duplicated across 2+ packages.

    Returns a list of candidate records, each with the human label, the count,
    the list of package names, the connection type, and the projected
    hand-authoring savings.
    """
    by_key: dict[tuple, dict[str, Any]] = defaultdict(lambda: {
        "package_names": set(),
        "ssis_names": set(),
    })
    for pkg in packages:
        for cm in pkg.connection_managers:
            key = _cm_dedup_key(cm)
            if key is None:
                continue
            entry = by_key[key]
            entry["package_names"].add(pkg.name)
            entry["ssis_names"].add(cm.name)

    candidates: list[dict[str, Any]] = []
    for key, entry in by_key.items():
        n = len(entry["package_names"])
        if n < 2:
            continue
        cm_type, server, database, file_path = key
        candidates.append({
            "label": _human_label(key),
            "connection_type": cm_type,
            "server": server or None,
            "database": database or None,
            "file_path": file_path or None,
            "duplicate_count": n,
            "ssis_names": sorted(entry["ssis_names"]),
            "package_names": sorted(entry["package_names"]),
            "hours_saved_if_shared": round(
                (n - 1) * _HOURS_SAVED_PER_DUPLICATE_LS, 1
            ),
        })
    candidates.sort(key=lambda c: (-c["duplicate_count"], c["label"]))
    return candidates


# ---------------------------------------------------------------------------
# Consolidation: structurally similar packages that *could* share a pipeline
# ---------------------------------------------------------------------------

def find_consolidation_candidates(packages: list[SSISPackage]) -> list[dict[str, Any]]:
    """Find groups of packages with identical structural fingerprints.

    These are *candidates* — the report should surface tradeoffs rather than
    auto-applying consolidation.
    """
    result = group_similar_packages(packages)
    candidates: list[dict[str, Any]] = []
    for group in result.groups:
        n = len(group.packages)
        # Net hours saved = (per-package savings × n) − one-time consolidation
        # build cost.  Negative is possible for very small groups; we still
        # surface it so the user can see the tradeoff.
        savings = round(
            n * _HOURS_SAVED_PER_CONSOLIDATED_PACKAGE
            - _HOURS_TO_BUILD_CONSOLIDATED_PIPELINE,
            1,
        )
        candidates.append({
            "fingerprint": group.fingerprint.digest[:12],
            "shape": group.fingerprint.shape_summary,
            "package_count": n,
            "package_names": [pkg.name for pkg in group.packages],
            "varying_parameters": list(group.shared_parameter_names),
            "estimated_hours_saved": savings,
            "tradeoffs": [
                "Consolidation reduces N pipelines to 1 with parameters; pros: "
                "single deployment, single alerting target, less duplication.",
                "Cons: ownership/lineage for the original packages becomes "
                "implicit, debugging requires parameter context, "
                "per-package SLAs must be re-modelled.",
                "Recommended only when the originals share an owner and "
                "schedule; not recommended when packages have different "
                "stakeholders or run cadences.",
            ],
        })
    candidates.sort(key=lambda c: -c["package_count"])
    return candidates


# ---------------------------------------------------------------------------
# Top-level estate analysis
# ---------------------------------------------------------------------------

def analyze_estate_consolidation(packages: list[SSISPackage]) -> dict[str, Any]:
    """Return both dedup and consolidation findings for an estate."""
    dedup = find_dedup_candidates(packages)
    consol = find_consolidation_candidates(packages)
    total_dedup_savings = round(sum(c["hours_saved_if_shared"] for c in dedup), 1)
    total_consol_savings = round(
        sum(c["estimated_hours_saved"] for c in consol if c["estimated_hours_saved"] > 0),
        1,
    )
    return {
        "deduplication": {
            "candidate_count": len(dedup),
            "total_hours_saved": total_dedup_savings,
            "candidates": dedup,
            "recommended_action": (
                "Run convert_ssis_package (or convert_estate) with a single "
                "shared_artifacts_dir so duplicate linked services are "
                "authored once and reused."
                if dedup else
                "No duplicate linked services detected across packages."
            ),
        },
        "consolidation": {
            "candidate_group_count": len(consol),
            "potential_hours_saved": total_consol_savings,
            "candidates": consol,
            "guidance": (
                "Consolidation candidates are structurally identical packages "
                "that could become one parameterized pipeline. This is a "
                "judgment call — review tradeoffs before applying."
                if consol else
                "No structural consolidation groups detected (each package "
                "is structurally unique)."
            ),
        },
    }
