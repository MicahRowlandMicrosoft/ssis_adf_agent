---
mode: agent
tools:
  - scan_ssis_packages
  - analyze_ssis_package
description: Scan a source for SSIS packages, then analyze each one for complexity and conversion gaps.
---

# Analyze SSIS Packages

Scan the source below for SSIS packages, then produce a full analysis report for each package found.

## Step 1 — Discover packages

Use `scan_ssis_packages` with:
- `source_type`: ${input:source_type:local, git, or sql}
- `path_or_connection`: ${input:path_or_connection:Local directory path, git repo URL, or SQL Server connection string}

## Step 2 — Analyze each package

For every package found, call `analyze_ssis_package` with its path.

## Step 3 — Report

Produce a Markdown table summarizing:
- Package name
- Complexity score and label
- Number of gaps (manual_required / warning / info)
- Estimated effort
- Key issues (first 3 gaps from the `manual_required` category)

Then list full gap details for each "Very High" or "High" complexity package.

Conclude with a prioritized recommendation of which packages to convert first (start with lowest complexity).
