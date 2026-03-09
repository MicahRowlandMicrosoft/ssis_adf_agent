---
mode: agent
tools:
  - analyze_ssis_package
  - convert_ssis_package
  - validate_adf_artifacts
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

### 3 — Validate

Call `validate_adf_artifacts` on the output directory. Report any structural issues.

### 4 — Summary report

Produce a Markdown summary containing:
- List of generated files grouped by type (pipeline, linkedService, dataset, dataflow, trigger)
- Azure Function stubs generated (if any) and what manual work they need
- Validation status (valid / issues found)
- Checklist of manual steps required before deployment:
  - [ ] Fill in connection string passwords for encrypted packages
  - [ ] Port Script Task logic from stubs to Python Azure Functions
  - [ ] Replace placeholder local paths with Azure Storage paths in File System Tasks
  - [ ] Activate triggers only after pipeline smoke-test

Remind the user: **run `validate_adf_artifacts` again after any manual edits** before deploying.
