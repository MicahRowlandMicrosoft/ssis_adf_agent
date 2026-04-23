"""P4-2: tests for the vendor-curated substitution registries.

For each of the three curated registry files we:

1. Load it with ``load_registry`` (proves the JSON is well-formed and
   passes the registry's schema validator).
2. Confirm a representative entry is present and routes through
   ``convert_transformation`` to the expected ADF type.
3. Drive the loader with a captured component XML fragment representative
   of the vendor's on-disk shape, parse the component_type / creation_name
   from it, and confirm the registry returns a substitution.

Captured fragments are intentionally small — just enough to exercise the
parser-to-registry handoff. They are not full SSIS packages.
"""
from __future__ import annotations

from pathlib import Path
from xml.etree import ElementTree as ET

import pytest

from ssis_adf_agent.converters.data_flow.transformation_converter import (
    convert_transformation,
)
from ssis_adf_agent.converters.substitution_registry import (
    SubstitutionRegistry,
    load_registry,
)
from ssis_adf_agent.parsers.models import DataFlowComponent

REGISTRY_DIR = Path(__file__).parent.parent / "registries"


def _comp(component_type: str, name: str = "Vendor") -> DataFlowComponent:
    return DataFlowComponent(
        id="comp-vendor",
        name=name,
        component_class_id="{VENDOR-CLASS}",
        component_type=component_type,
    )


# ---------------------------------------------------------------------------
# All three files — smoke load + structural sanity
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "filename",
    ["cozyroc_salesforce.json", "kingswaysoft_dynamics.json", "pragmatic_works.json"],
)
def test_curated_registry_loads_cleanly(filename: str) -> None:
    reg = load_registry(REGISTRY_DIR / filename)
    assert isinstance(reg, SubstitutionRegistry)
    # Every curated registry must contribute at least one entry.
    assert len(reg.data_flow) + len(reg.control_flow) >= 1


@pytest.mark.parametrize(
    "filename",
    ["cozyroc_salesforce.json", "kingswaysoft_dynamics.json", "pragmatic_works.json"],
)
def test_every_data_flow_entry_has_review_marker_or_clean_mapping(filename: str) -> None:
    """Curated entries should either be safe-to-emit or carry a review flag."""
    reg = load_registry(REGISTRY_DIR / filename)
    for key, sub in reg.data_flow.items():
        # Either the entry has no extra props (pure rename) or it documents
        # a review item — never an undocumented config requiring manual
        # intervention.
        if sub.type_properties and "_review_required" not in sub.type_properties:
            # Allowed exception: if 'store' or 'broadcast' are the only props,
            # those are well-known native ADF fields and need no review note.
            allowed_silent = {"store", "broadcast"}
            extras = set(sub.type_properties) - allowed_silent
            assert not extras, (
                f"{filename}::{key} has type_properties {extras!r} but no "
                "_review_required marker"
            )


# ---------------------------------------------------------------------------
# COZYROC Salesforce — captured fragment + substitution routing
# ---------------------------------------------------------------------------

# Captured (sanitized) fragment of how COZYROC's Salesforce Source appears
# inside a .dtsx <component> element.  The parser-relevant attribute is
# componentClassID, which the SSIS parser surfaces as ``component_type``.
_COZYROC_SALESFORCE_SOURCE_FRAGMENT = """
<component
    refId="Package\\DFT - Load Accounts\\Salesforce Source"
    componentClassID="Cozyroc.SSISPlus.SalesforceSource"
    contactInfo="COZYROC SSIS+;www.cozyroc.com"
    description="Reads data from Salesforce"
    name="Salesforce Source"
    usesDispositions="true"
    version="14"
    xmlns="">
  <properties>
    <property name="ObjectAPIName">Account</property>
    <property name="SOQLQuery">SELECT Id, Name FROM Account WHERE IsActive = TRUE</property>
  </properties>
</component>
"""


def test_cozyroc_salesforce_source_substitutes_to_native_source() -> None:
    reg = load_registry(REGISTRY_DIR / "cozyroc_salesforce.json")

    # Parse the component_type out of the captured fragment, just like the
    # SSIS parser does (componentClassID attribute).
    root = ET.fromstring(_COZYROC_SALESFORCE_SOURCE_FRAGMENT)
    component_type = root.attrib["componentClassID"]
    assert component_type == "Cozyroc.SSISPlus.SalesforceSource"

    sub = reg.lookup_data_flow(component_type)
    assert sub is not None
    assert sub.adf_type == "Source"
    assert "Salesforce" in sub.notes

    # Drive convert_transformation end-to-end to confirm the substitution
    # actually short-circuits the dispatcher.
    result = convert_transformation(_comp(component_type), registry=reg)
    assert result is not None
    assert result["type"] == "Source"
    assert "REGISTRY SUBSTITUTION" in result["description"]


def test_cozyroc_salesforce_task_is_control_flow_entry() -> None:
    reg = load_registry(REGISTRY_DIR / "cozyroc_salesforce.json")
    cf = reg.lookup_control_flow("Cozyroc.SSISPlus.SalesforceTask")
    assert cf is not None
    assert cf.adf_activity_type == "WebActivity"
    assert "REST" in cf.notes or "SOAP" in cf.notes


# ---------------------------------------------------------------------------
# KingswaySoft Dynamics — captured fragment + substitution routing
# ---------------------------------------------------------------------------

_KINGSWAYSOFT_CRM_DESTINATION_FRAGMENT = """
<component
    refId="Package\\DFT - Sync Contacts\\CRM Destination"
    componentClassID="KingswaySoft.SSISCRMDestination"
    contactInfo="KingswaySoft;www.kingswaysoft.com"
    description="Writes data to Microsoft Dynamics CRM"
    name="CRM Destination"
    version="20"
    xmlns="">
  <properties>
    <property name="EntityName">contact</property>
    <property name="Action">Upsert</property>
    <property name="AlternateKeyName">emailaddress1</property>
  </properties>
</component>
"""


def test_kingswaysoft_crm_destination_substitutes_to_sink() -> None:
    reg = load_registry(REGISTRY_DIR / "kingswaysoft_dynamics.json")

    root = ET.fromstring(_KINGSWAYSOFT_CRM_DESTINATION_FRAGMENT)
    component_type = root.attrib["componentClassID"]
    assert component_type == "KingswaySoft.SSISCRMDestination"

    sub = reg.lookup_data_flow(component_type)
    assert sub is not None
    assert sub.adf_type == "Sink"
    assert "Dataverse" in sub.notes

    result = convert_transformation(_comp(component_type), registry=reg)
    assert result is not None
    assert result["type"] == "Sink"
    # Audit trail: the review checklist must surface in the generated JSON.
    assert "_review_required" in result["typeProperties"]


def test_kingswaysoft_premium_derived_column_routes_to_derived_column() -> None:
    reg = load_registry(REGISTRY_DIR / "kingswaysoft_dynamics.json")
    sub = reg.lookup_data_flow("KingswaySoft.PremiumDerivedColumn")
    assert sub is not None
    assert sub.adf_type == "DerivedColumn"


# ---------------------------------------------------------------------------
# Pragmatic Works — captured fragment + substitution routing
# ---------------------------------------------------------------------------

_PRAGMATIC_WORKS_UPSERT_FRAGMENT = """
<component
    refId="Package\\DFT - Merge Customers\\Upsert Destination"
    componentClassID="Pragmaticworks.TaskFactory.UpsertDestination"
    contactInfo="Pragmatic Works Task Factory;www.pragmaticworks.com"
    description="Performs an UPSERT against the destination"
    name="Upsert Destination"
    version="2024.1"
    xmlns="">
  <properties>
    <property name="BatchSize">1000</property>
    <property name="MatchColumns">customer_id</property>
  </properties>
</component>
"""


def test_pragmatic_works_upsert_substitutes_to_sink() -> None:
    reg = load_registry(REGISTRY_DIR / "pragmatic_works.json")

    root = ET.fromstring(_PRAGMATIC_WORKS_UPSERT_FRAGMENT)
    component_type = root.attrib["componentClassID"]
    assert component_type == "Pragmaticworks.TaskFactory.UpsertDestination"

    sub = reg.lookup_data_flow(component_type)
    assert sub is not None
    assert sub.adf_type == "Sink"
    assert "writeBehavior" in sub.notes  # Documents the upsert mapping.

    result = convert_transformation(_comp(component_type), registry=reg)
    assert result is not None
    assert result["type"] == "Sink"


def test_pragmatic_works_namespace_aliases_both_resolve() -> None:
    """Both `Pragmaticworks.*` and `PragmaticWorks.*` casings are mapped."""
    reg = load_registry(REGISTRY_DIR / "pragmatic_works.json")
    a = reg.lookup_data_flow("Pragmaticworks.TaskFactory.UpsertDestination")
    b = reg.lookup_data_flow("PragmaticWorks.TaskFactory.UpsertDestination")
    assert a is not None and b is not None
    assert a.adf_type == b.adf_type == "Sink"


def test_pragmatic_works_email_task_routes_to_web_activity() -> None:
    reg = load_registry(REGISTRY_DIR / "pragmatic_works.json")
    cf = reg.lookup_control_flow("Pragmaticworks.TaskFactory.AdvancedEMailTask")
    assert cf is not None
    assert cf.adf_activity_type == "WebActivity"
    assert "Logic App" in cf.notes


def test_pragmatic_works_dimension_merge_scd_routes_to_alter_row() -> None:
    reg = load_registry(REGISTRY_DIR / "pragmatic_works.json")
    sub = reg.lookup_data_flow("Pragmaticworks.TaskFactory.DimensionMergeSCD")
    assert sub is not None
    assert sub.adf_type == "AlterRow"
    # SCD Type 2 must be flagged as needing manual review.
    assert "_review_required" in sub.type_properties


# ---------------------------------------------------------------------------
# Cross-registry sanity: no key collisions across vendor files
# ---------------------------------------------------------------------------

def test_no_data_flow_key_collisions_across_curated_registries() -> None:
    """A customer who chains all three should not see one vendor stomp another."""
    seen: dict[str, str] = {}
    for filename in (
        "cozyroc_salesforce.json",
        "kingswaysoft_dynamics.json",
        "pragmatic_works.json",
    ):
        reg = load_registry(REGISTRY_DIR / filename)
        for key in reg.data_flow:
            assert key not in seen, (
                f"data_flow key {key!r} appears in both {seen[key]} and "
                f"{filename}; rename one to avoid silent overrides"
            )
            seen[key] = filename
