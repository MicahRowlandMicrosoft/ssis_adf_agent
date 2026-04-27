# Vendor-curated substitution registries

This directory ships **curated, vendor-specific substitution registry files**
that the agent can load via the `substitution_registry_path` argument on
`convert_ssis_package` (M7 — see [docs/SUBSTITUTION_REGISTRY.md](../docs/SUBSTITUTION_REGISTRY.md)
for the mechanism, the file format, and how to author your own).

The mechanism (M7) shipped without curated entries; this directory closes
the gap so customers using the most common third-party SSIS component
families do not have to author every entry from scratch before their first
conversion.

## Catalog

| File | Vendor | Coverage | Audit notes |
|---|---|---|---|
| [`cozyroc_salesforce.json`](cozyroc_salesforce.json) | COZYROC | Salesforce Source/Destination/Lookup, Bulk variants, Salesforce Task | Each entry expects a native ADF `Salesforce` (or `SalesforceServiceCloud`) linked service. Maps to MDF transformations, not Copy Activity. |
| [`kingswaysoft_dynamics.json`](kingswaysoft_dynamics.json) | KingswaySoft | Dynamics CRM Source/Destination/Lookup/OptionSet, Premium Derived Column, Premium Lookup, Retrieve Data Task | Each entry expects a `CommonDataServiceForApps` (Dataverse) linked service. ExecuteWorkflow action and Type 2 SCD logic are flagged as manual-review items. |
| [`pragmatic_works.json`](pragmatic_works.json) | Pragmatic Works | Task Factory: Upsert Destination, Dimension Merge SCD, Regex Replace, Advanced Derived Column, Data Validation, Aggregate, Advanced E-Mail Task, Secure FTP Task, Terminate Process Task, Compression Task, REST Source Task | Both the `Pragmaticworks` and `PragmaticWorks` namespace casings are mapped where applicable. Type 2 SCD always needs manual review. |

## Using a curated registry

```bash
mcp call convert_ssis_package '{
  "package_path": "C:/path/to/MyPackage.dtsx",
  "output_dir": "out/",
  "substitution_registry_path": "registries/cozyroc_salesforce.json"
}'
```

You can chain multiple registries by merging them ahead of time (the file
format is a plain JSON object — `jq -s '.[0] * .[1]' a.json b.json` works).

## What "curated" means here

- **Component-type strings** are taken from the vendor's public component
  catalog as of October 2024. Vendors occasionally rename components
  between major releases; always run `analyze_ssis_package` first and
  cross-check the keys against `component_class_id` values reported by the
  parser.
- **`adf_type` mappings** target the native ADF Mapping Data Flow / Pipeline
  Activity that gets the converted package closest to running. Where the
  vendor component has no pure-ADF analog (e.g. Pragmatic Works Terminate
  Process Task), the registry routes to `AzureFunctionActivity` and
  documents the rewrite in the `notes` field.
- **`_review_required`** keys inside `type_properties` flag the per-entry
  manual checklist for the reviewer (e.g. "set externalIdFieldName for
  Upsert"). They are passed through unchanged into the generated ADF JSON
  so the manual TODOs land in the file the reviewer is editing — not in a
  separate spreadsheet.

## Contributing additions

1. Add the new component / task to the matching JSON file (keep entries
   alphabetically sorted within each section).
2. Add a unit test in [`tests/test_vendor_registries.py`](../tests/test_vendor_registries.py)
   that asserts the registry loads cleanly and that `convert_transformation`
   (for data-flow entries) emits the expected `type` + `description`.
3. Cross-link from this README's catalog table.

## What this is **not**

- Not a license to redistribute the vendor's components. The third-party
  DLLs themselves remain under each vendor's own license; this directory
  only ships *mapping metadata* describing how the converted ADF artifact
  should be shaped.
- Not a substitute for a runtime test. The generated ADF JSON should still
  be put through `validate_adf_artifacts` and a `--pre-flight` deploy
  before going to production. See [PARITY.md](../PARITY.md) and
  [BEHAVIORAL_PARITY.md](../BEHAVIORAL_PARITY.md) for end-to-end checks.
