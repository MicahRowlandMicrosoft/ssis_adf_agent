"""Migration plan / design proposal models for the SSIS Migration Copilot.

These Pydantic models form the contract between three things:

1. ``propose_adf_design`` — emits a ``MigrationPlan`` describing a recommended
   target architecture for a single SSIS package (or an estate).
2. The agent / customer — reviews the plan, edits it, persists it via
   ``save_migration_plan`` / ``load_migration_plan``.
3. ``convert_ssis_package(... design_path=...)`` and
   ``provision_adf_environment(plan)`` — consume the (possibly edited) plan to
   generate ADF artifacts and Azure infrastructure that match the *agreed*
   target, not just the SSIS-faithful default.

The contract is intentionally permissive: every field has a sensible default so
older callers and partially-filled plans still work.
"""
from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


PLAN_SCHEMA_VERSION = "1.0"


# ---------------------------------------------------------------------------
# Enums — keep small and stable; agents will reference these by name
# ---------------------------------------------------------------------------

class TargetPattern(str, Enum):
    """Coarse pattern label for the target ADF design.

    Drives downstream simplification rules and the recommended activity
    inventory. Pattern detection is best-effort; ``CUSTOM`` is the always-safe
    fallback that means "convert SSIS-faithfully, no opinions applied".
    """

    SCHEDULED_FILE_DROP = "scheduled_file_drop"          # SQL → CSV/Parquet on ADLS
    INGEST_FILE_TO_SQL = "ingest_file_to_sql"            # File → staging table
    SQL_TO_SQL_COPY = "sql_to_sql_copy"                  # Plain table replication
    INCREMENTAL_LOAD = "incremental_load"                # Watermark-based ETL
    DIMENSIONAL_LOAD = "dimensional_load"                # SCD / star-schema build
    SCRIPT_HEAVY = "script_heavy"                        # Mostly Script Tasks → Functions
    CUSTOM = "custom"                                    # No simplification — SSIS-faithful


class AuthMode(str, Enum):
    MANAGED_IDENTITY = "ManagedIdentity"                 # Default, recommended
    SQL_AUTH = "SqlAuth"                                 # Username/password, secret in KV
    SERVICE_PRINCIPAL = "ServicePrincipal"
    SAS_TOKEN = "SasToken"
    ACCOUNT_KEY = "AccountKey"                           # Discouraged — flagged as risk


class StorageKind(str, Enum):
    ADLS_GEN2 = "AzureBlobFS"                            # HNS-enabled, recommended
    BLOB = "AzureBlobStorage"                            # Flat namespace
    AZURE_FILES = "AzureFileStorage"                     # Only for legacy SMB consumers


class SimplificationAction(str, Enum):
    DROP = "drop"                                        # Remove tasks entirely
    FOLD_TO_COPY_ACTIVITY = "fold_to_copy_activity"      # Mapping DF → Copy w/ source query
    FOLD_TO_STORED_PROC = "fold_to_stored_proc"          # Multi-statement → single SP call
    REPLACE_WITH_FUNCTION = "replace_with_function"      # Custom logic → Azure Function
    KEEP_AS_IS = "keep_as_is"                            # Explicit "no change"


class RiskSeverity(str, Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


# ---------------------------------------------------------------------------
# Plan sub-models
# ---------------------------------------------------------------------------

class Simplification(BaseModel):
    """A single recommended change vs. the SSIS-faithful baseline."""

    action: SimplificationAction
    items: list[str] = Field(
        default_factory=list,
        description="Task names (or component names) the action applies to.",
    )
    reason: str = Field(default="", description="Human-readable rationale.")
    confidence: float = Field(
        default=0.8, ge=0.0, le=1.0,
        description="Proposer's confidence this simplification is safe.",
    )


class LinkedServiceSpec(BaseModel):
    """Recommended ADF linked service (target, not SSIS-faithful)."""

    name: str
    type: str = Field(description="ADF linked service type, e.g. AzureSqlDatabase.")
    auth: AuthMode = AuthMode.MANAGED_IDENTITY
    target_resource: str | None = Field(
        default=None,
        description="Logical reference: 'sql://server/db' or 'storage://account/container'.",
    )
    secret_name: str | None = Field(
        default=None,
        description="Key Vault secret name if auth requires a credential.",
    )
    notes: str = ""


class InfrastructureItem(BaseModel):
    """One Azure resource the plan needs provisioned."""

    type: str = Field(description="ARM resource type, e.g. Microsoft.Storage/storageAccounts.")
    name_hint: str = Field(description="Suggested resource name (final name may add a suffix).")
    sku: str | None = None
    location: str | None = None
    properties: dict[str, Any] = Field(default_factory=dict)
    purpose: str = Field(default="", description="Why the plan needs this resource.")


class RbacAssignment(BaseModel):
    """A role assignment the agent must grant after provisioning."""

    principal: str = Field(description="Logical name, e.g. '<ADF MI>' — resolved at deploy time.")
    scope: str = Field(description="Logical scope, e.g. '<storage>' or '<sql>'.")
    role: str = Field(description="Built-in role name, e.g. 'Storage Blob Data Contributor'.")
    purpose: str = ""


class Risk(BaseModel):
    severity: RiskSeverity
    message: str
    mitigation: str = ""
    related_tasks: list[str] = Field(default_factory=list)


class EffortEstimate(BaseModel):
    """Coarse effort prediction in hours, for budgeting conversations.

    Hours are presented as a **range** (``low_hours`` / ``total_hours`` /
    ``high_hours``) because a point estimate implies a precision we don't
    have.  ``total_hours`` is the "likely" (P50-ish) value.  The range is
    a coarse ±30% / +60% envelope to reflect real-world variance in SSIS
    migration effort.

    ``script_porting_hours`` and ``dataflow_hours`` are informational
    breakdowns showing *which* content drove the estimate — useful when a
    customer pushes back on the number.
    """

    architecture_hours: float = 0.0
    development_hours: float = 0.0
    testing_hours: float = 0.0
    total_hours: float = 0.0
    low_hours: float = 0.0
    high_hours: float = 0.0
    script_porting_hours: float = 0.0
    dataflow_hours: float = 0.0
    mcp_automated_hours_saved: float = Field(
        default=0.0,
        description=(
            "Estimated hours of mechanical work (pipeline, linked service, "
            "dataset, data-flow scaffold, trigger, deployment wiring) that the "
            "MCP tools handle for this package — i.e. hours the human would "
            "have spent hand-authoring JSON without the converter. Informational; "
            "total_hours is already the post-MCP human estimate."
        ),
    )
    bucket: str = Field(default="medium", description="low | medium | high | very_high")
    notes: list[str] = Field(
        default_factory=list,
        description="Short human-readable notes explaining the major drivers.",
    )


# ---------------------------------------------------------------------------
# Top-level plan
# ---------------------------------------------------------------------------

class MigrationPlan(BaseModel):
    """An agreed target ADF design for a single SSIS package.

    Produced by ``propose_adf_design`` and (typically) edited by the agent /
    customer before being passed to ``convert_ssis_package`` and
    ``provision_adf_environment``.
    """

    schema_version: str = PLAN_SCHEMA_VERSION
    package_name: str
    package_path: str
    target_pattern: TargetPattern = TargetPattern.CUSTOM
    summary: str = Field(default="", description="One-paragraph plain-English summary.")

    # Core design decisions
    simplifications: list[Simplification] = Field(default_factory=list)
    linked_services: list[LinkedServiceSpec] = Field(default_factory=list)
    infrastructure_needed: list[InfrastructureItem] = Field(default_factory=list)
    rbac_needed: list[RbacAssignment] = Field(default_factory=list)
    risks: list[Risk] = Field(default_factory=list)
    effort: EffortEstimate = Field(default_factory=EffortEstimate)

    # Agent consumption hooks
    reasoning_input: dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Structured facts the LLM agent can use to refine the proposal "
            "(inventory counts, detected patterns, gap items, etc.)."
        ),
    )
    customer_decisions: dict[str, Any] = Field(
        default_factory=dict,
        description="Free-form bag for agent to record agreed deviations or notes.",
    )
    name_overrides: dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Optional artifact name overrides. Keys use prefixed identifiers: "
            "'LS:<cm_name>', 'DS:<component_name>', 'DF:<task_name>', 'PL', 'TR'. "
            "Values are the desired ADF artifact names (must be valid ADF names)."
        ),
    )

    def render_markdown(self) -> str:  # pragma: no cover - presentation
        """Render a human-readable Markdown summary for the agent to show."""
        lines = [
            f"# Migration plan: `{self.package_name}`",
            f"_Pattern: **{self.target_pattern.value}**_",
            "",
            self.summary or "_No summary._",
            "",
        ]
        if self.simplifications:
            lines.append("## Recommended simplifications")
            for s in self.simplifications:
                items = ", ".join(s.items) if s.items else "(none)"
                lines.append(f"- **{s.action.value}** → {items}")
                if s.reason:
                    lines.append(f"  - _why:_ {s.reason}")
            lines.append("")
        if self.linked_services:
            lines.append("## Linked services (target)")
            for ls in self.linked_services:
                lines.append(f"- `{ls.name}` — {ls.type} — auth: **{ls.auth.value}**")
            lines.append("")
        if self.infrastructure_needed:
            lines.append("## Infrastructure to provision")
            for r in self.infrastructure_needed:
                sku = f" ({r.sku})" if r.sku else ""
                lines.append(f"- **{r.type}**{sku} — `{r.name_hint}` — {r.purpose}")
            lines.append("")
        if self.rbac_needed:
            lines.append("## RBAC assignments needed")
            for r in self.rbac_needed:
                lines.append(f"- {r.principal} → **{r.role}** on {r.scope}")
            lines.append("")
        if self.risks:
            lines.append("## Risks")
            for r in self.risks:
                lines.append(f"- **[{r.severity.value}]** {r.message}")
                if r.mitigation:
                    lines.append(f"  - _mitigation:_ {r.mitigation}")
            lines.append("")
        e = self.effort
        lines.append(
            f"## Effort estimate\n"
            f"- Architecture: {e.architecture_hours}h\n"
            f"- Development: {e.development_hours}h"
            + (f" (script porting: {e.script_porting_hours}h, data flows: {e.dataflow_hours}h)"
               if (e.script_porting_hours or e.dataflow_hours) else "")
            + "\n"
            f"- Testing: {e.testing_hours}h\n"
            f"- **Likely total: {e.total_hours}h** ({e.bucket})"
            + (f"\n- Range: {e.low_hours}h (low) / {e.total_hours}h (likely) / {e.high_hours}h (high)"
               if (e.low_hours or e.high_hours) else "")
            + "\n"
        )
        if e.notes:
            lines.append("**Drivers:**")
            for n in e.notes:
                lines.append(f"- {n}")
            lines.append("")
        return "\n".join(lines)
