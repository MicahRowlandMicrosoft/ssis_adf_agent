---
agent: agent
tools:
  - analyze_ssis_package
  - convert_ssis_package
  - validate_adf_artifacts
  - validate_conversion_parity
description: Convert a single SSIS package to ADF JSON artifacts, validate them, and report any manual steps needed.
---

# Convert SSIS Package to ADF

Convert the specified SSIS package to Azure Data Factory JSON artifacts.

## Parameters

- **Package path**: ${input:package_path:Absolute path to the .dtsx file}
- **Output directory**: ${input:output_dir:Directory to write ADF artifacts (e.g. C:\adf_output\MyPackage)}

## Steps

### 1 — Pre-conversion analysis

Call `analyze_ssis_package` on the package path. Report:
- Complexity score
- Any `manual_required` gaps (these will need post-conversion work)

### 2 — Convert

Call `convert_ssis_package` with:
- `package_path` = the package path above
- `output_dir` = the output directory above
- `generate_trigger` = true

Read and report every item in `conversion_warnings`. Do not treat a successful
tool call as a warning-free conversion. Triggers are generated in Stopped state.

### 3 — Validate

Call `validate_adf_artifacts` on the output directory. Report any structural issues.

If structural validation succeeds, call `validate_conversion_parity` with the
package path and output directory. Keep the check local by omitting factory
arguments. Report errors and warnings separately; structural parity does not
prove data values or runtime behavior.

### 4 — Summary report

Produce a Markdown summary containing:
- List of generated files grouped by type (pipeline, linkedService, dataset, dataflow, trigger)
- Azure Function stubs generated (if any) and what manual work they need
- Structural and conversion-parity status
- Checklist of manual steps required before deployment:
  - [ ] For encrypted packages, decrypt a controlled copy with `dtutil`, use Key Vault references, and upload secrets through the documented encrypted-package workflow
  - [ ] Port Script Task logic from stubs to Python Azure Functions
  - [ ] Replace placeholder local paths with Azure Storage paths in File System Tasks
  - [ ] Activate triggers only after pipeline smoke-test

Remind the user: **run both validation tools again after any manual edits** before deploying.
