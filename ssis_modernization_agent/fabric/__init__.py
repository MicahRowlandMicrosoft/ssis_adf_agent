"""
Fabric target — translate SSIS packages to Microsoft Fabric Data Pipelines.

This package mirrors the existing ADF target (converters/ + generators/) but
emits Fabric Data Pipelines pipeline-content.json, Fabric Connection
placeholders + a manifest, and PySpark Notebook stubs for Data Flow Tasks
(Fabric Data Pipelines have no Mapping Data Flow equivalent).

Implementation strategy: rather than duplicating the SSIS task converters,
this package reuses the proven ADF dispatcher to produce activity dicts and
then translates the ADF JSON shape to Fabric's shape. The two formats are
~95% identical at the activity level; the differences are concentrated in
how connections, datasets, and Mapping Data Flow are referenced.

Public surface:
  - convert_package_to_fabric: SSISPackage → Fabric artifacts on disk
  - validate_fabric_artifacts: structural validation of Fabric pipeline JSON
  - ConnectionResolver: SSIS Connection Manager → Fabric Connection placeholder
"""
from .connection_resolver import ConnectionResolver
from .fabric_converter import convert_package_to_fabric
from .parity_validator import (
    render_fabric_parity_markdown,
    validate_fabric_conversion_parity,
)
from .validator import validate_fabric_artifacts

__all__ = [
    "ConnectionResolver",
    "convert_package_to_fabric",
    "render_fabric_parity_markdown",
    "validate_fabric_artifacts",
    "validate_fabric_conversion_parity",
]
