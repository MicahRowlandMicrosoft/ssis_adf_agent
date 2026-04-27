# Roadmap

## Current version

`0.1.0` — pre-1.0. The semver pre-1.0 caveat applies: minor-version
bumps may include breaking changes to the MCP tool surface, the CLI
flag set, the on-disk artifact layout, the Pydantic IR models, and the
`lineage.json` schema. Users pinning a minor version is the supported
strategy until 1.0.

The current quarter's focus is **shipping the P4 buyer-followup
backlog** (see [BACKLOG.md](BACKLOG.md) §"P4 — Buyer follow-up backlog"),
which closes the remaining gaps a customer-facing review surfaced
against the 0.1.0 surface. Of the 14 P4 items, 12 are shipped at the
time of writing; P4-13 (this document) and P4-14 (SUPPORT.md) close
the set.

---

## What 1.0 means

A 1.0 release commits the project to **semver-strict compatibility
guarantees** on the four surfaces a downstream automation depends on:

| Surface | 1.0 commitment |
|---|---|
| **MCP tool inputs / outputs** | Tool names, argument names, argument types, and the shape of the JSON returned by each tool are stable within `1.x`. New tools may be added; existing tools may gain optional arguments and additional output keys. |
| **CLI** | `ssis-adf-agent` subcommand names, flag names, and exit-code conventions are stable within `1.x`. |
| **On-disk artifact layout** | The directory structure under `<output_dir>/` (`pipeline/`, `linkedService/`, `dataset/`, `dataflow/`, `trigger/`, `stubs/`), file naming convention, and the schemas of `lineage.json` and `migration_plan.json` are stable within `1.x`. |
| **Pydantic IR models** | The public `parsers.models` types (the IR consumed by every converter and generator) carry semver-style field-deprecation guarantees: a field removed in `2.x` will be deprecated for one full `1.y` minor cycle first. |

Things explicitly **not** covered by the 1.0 commitment:

- Internal converter / generator implementation details. Only the
  artifact they emit is part of the contract.
- The exact text of generated Python stubs for Script Tasks (the LLM
  translator output is non-deterministic by design).
- The exact wording of warning messages or error strings.
- Test-helper modules under `tests/`.
- The C# → Python translator under `translators/`.

---

## Engineering items required for 1.0

The list below is the *minimum* set required to flip the version. Each
line names the gap and either an existing tracked item (P-prefix) or
"NEW" if a new ticket is needed.

### Stability

| # | Item | Status |
|---|---|---|
| S1 | All P4 buyer follow-ups closed (BACKLOG.md §P4) | 12 of 14 done at time of writing; P4-13 and P4-14 close the set |
| S2 | Pydantic IR field-deprecation policy documented (one full minor cycle of `DeprecationWarning` before removal) | NEW (small doc add to README §Development) |
| S3 | `lineage.json` schema versioned (`schemaVersion: "1"` at top level) and a forward-compatibility test added | NEW |
| S4 | `migration_plan.json` schema versioned similarly | NEW |
| S5 | MCP tool output JSON schemas published as JSON Schema files under `schemas/` | NEW |

### Quality

| # | Item | Status |
|---|---|---|
| Q1 | All ✅ rows in COVERAGE.md backed by a unit-test fixture (not just the converter source) | Mostly done as of P4-12; remaining rows (Send Mail Task, Bulk Insert Task, Web Service Task, XML Task, Transfer SQL Server Objects Task, Excel destination, Excel source, Oracle/DB2/SAP source, FTP/SFTP/HTTP/File CMs) currently link to converter source — need dedicated tests. NEW |
| Q2 | Behavioral parity (`compare_dataflow_output`, P4-1) test coverage extended to at least 5 distinct Mapping Data Flow patterns (currently 1 worked example) | NEW |
| Q3 | At least 3 captured first-deploy failure case studies under `docs/case-studies/` (currently 1: P4-11 KV recovery) covering distinct failure modes | NEW |
| Q4 | The `provision_adf_environment` Bicep emits the OBSERVABILITY.md diagnostic-settings target as an *opt-in* parameter | NEW |

### Surface ergonomics

| # | Item | Status |
|---|---|---|
| E1 | `ssis-adf-agent` CLI surfaces every MCP tool (currently a subset) so the project is usable without the MCP server | NEW |
| E2 | `--dry-run` flag honored uniformly across every tool that touches Azure | Partial: `deploy_to_adf --pre-flight=true` (P4-6) and `activate_triggers --dry-run=true` exist; `provision_adf_environment` and `provision_function_app` do not. NEW |
| E3 | Single-binary distribution (`pip install ssis-adf-agent` plus `pipx run ssis-adf-agent` smoke-tested per release) | NEW |

### Operational

| # | Item | Status |
|---|---|---|
| O1 | SUPPORT.md naming the support channel and response-time commitment | P4-14 — last item in the P4 backlog |
| O2 | A captured estate-scale conversion run (≥ 100 packages) with timings, failure modes, and recovery, written up under `docs/case-studies/` | NEW |
| O3 | RBAC.md (P4-7) audited against the actual ARM-deployment activity log of one estate-scale run | NEW |

---

## Pre-1.0 → 1.0 transition window

The version that immediately precedes `1.0.0` will be `0.9.0` and will
carry an explicit **deprecation manifest**:

1. `0.9.0` is released *before* `1.0.0`. Anything that will change
   shape or be removed at `1.0.0` is emitted as a `DeprecationWarning`
   in `0.9.x` with the message text `removed-in-1.0`.
2. `0.9.x` is supported for a minimum of **30 days** (point-release
   support window) so that downstream automation can pin to `0.9` and
   migrate at its own cadence.
3. `1.0.0` ships only after the 30-day window closes and the deprecated
   surface is removed.

Concretely, the change log for `0.9.0` will list every breaking change
as `BREAKING (in 1.0):` followed by the migration recipe for each.

---

## Release cadence (pre-1.0)

- Patch releases (`0.1.x`): rolled as needed for bug fixes.
- Minor releases (`0.y.0`): cut roughly every 4–6 weeks, batching
  feature work and any breaking changes.
- No backports to older minor versions in the pre-1.0 era. Pin and
  upgrade.

After 1.0, the cadence is intentionally undefined — driven by
customer-facing need rather than calendar. Patch releases continue
indefinitely on the latest minor.

---

## See also

- [BACKLOG.md](BACKLOG.md) — full prioritized backlog including the
  P4 buyer follow-up set this roadmap closes out.
- [CHANGELOG.md](CHANGELOG.md) — what has actually shipped.
- [WORKFLOW.md](WORKFLOW.md) — the 6-tool minimum migration path that
  any 1.0 release must keep working.
