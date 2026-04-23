# Changelog

All notable changes to **ssis-adf-agent** will be recorded here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
While the package is still on the `0.x` line, the public surface (MCP tool
names + argument schemas, generated ADF JSON shape) may change between minor
versions. From `1.0.0` onward, breaking changes will only land in major bumps.

## [Unreleased]

### Added
- **P4-1** — Behavioral data-flow parity harness (`compare_dataflow_output`,
  MCP tool #27). Runs the same controlled input set through an SSIS Data Flow
  (via `dtexec.exe`) and through its converted ADF Mapping Data Flow (via an
  ADF debug session), and emits a row-and-column diff report. Supports three
  modes: `captured` (replay pre-captured CSVs — recommended for CI),
  `live` (real dtexec + ADF debug), and `mixed`. Pluggable runner protocols
  let customers wire their own SSIS environment in. Pure diff engine
  (`diff_rows`) with row-key matching, schema-drift detection, configurable
  ignore lists, numeric tolerance, and case/whitespace normalization. Worked
  example with a *seeded regression* under
  [tests/fixtures/dataflow_parity/](tests/fixtures/dataflow_parity/) plus
  documentation in [BEHAVIORAL_PARITY.md](BEHAVIORAL_PARITY.md). 27 new tests.

### Fixed
- **B1** — `convert_ssis_package` now derives a Copy activity's `source.type`
  and `sink.type` from the actual SSIS component (`OLE DB Source` →
  `AzureSqlSource`, `Flat File Destination` → `DelimitedTextSink`, etc.) instead
  of always emitting `AzureSqlSource` / `AzureSqlSink`. SQL-only sink properties
  are gated; OLE DB `sqlReaderQuery` is preserved.
- **B3** — Pipeline parameters / project parameters / variables whose **name**
  matches a credential keyword (password / userid / token / secret / login /
  account / sas / connectionstring / clientsecret / credential) **or** whose
  **value** matches a Windows-domain account or on-prem FQDN are now redacted
  from the generated pipeline JSON. The entry is kept (so callers / Key Vault
  references still bind) but `defaultValue` is stripped and replaced with a
  `[SENSITIVE]` description block. Azure cloud hostnames are intentionally not
  flagged.
- **H3** — Script Task source extraction now handles the SSIS 2017+ inline
  `<ProjectItem>` CDATA layout. Packages from the LNI estate (and any other
  modern VSTA-style package) now report the correct `script_language`
  (CSharp / VisualBasic) and the LLM Script Task translator receives the
  original source instead of `None`. The misleading "self-closing stub
  format" warning was rewritten.

### Added
- **H4** — `PARITY.md` documents every check `validate_conversion_parity`
  performs, with explicit boundaries (no row-level / performance comparison).
  A worked example is captured against the LNI ADDS-MIPS-TC sample.
- `BACKLOG.md` — buyer's-perspective evaluation findings, prioritised P0–P3
  with execution order. Each entry is updated as items land.
- `CHANGELOG.md`, `SECURITY.md` — repo metadata required for enterprise
  consumption.

## [0.1.0] — initial published shape

- 23 MCP tools spanning scan → analyze → bulk-analyze → propose → plan →
  convert → validate → parity → smoke-test → deploy → provision → estate
  reporting.
- SSIS parser supporting Control Flow + Data Flow, Script Tasks (binary
  `BinaryData` / `ProjectBytes` patterns), event handlers, sequence /
  ForEach / For Loop containers.
- ADF generators for pipeline, linkedService, dataset, dataflow, trigger
  (Stopped state by default), Azure Function stubs.
- Bicep-based ADF + Function App provisioner.
- `azure-mgmt-datafactory` SDK dry-run before deploy.
- Pre-deployment PDF report.
