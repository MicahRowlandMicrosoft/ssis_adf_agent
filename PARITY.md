# Parity validation

## What it does

`validate_conversion_parity` is a pre-deployment gate that compares an SSIS source
package (`.dtsx`) to its converted ADF artifacts (the JSON files under
`pipeline/`, `linkedService/`, `dataset/`, `dataflow/`, `trigger/`, and `stubs/`)
and reports whether the conversion preserved the package's structure and
configuration.

It is **deterministic and offline by default**. It does not execute SSIS, does not
run the ADF pipeline, and does not require an Azure subscription unless you
explicitly pass one.

## What is compared

| Check | What it asserts | Source |
|---|---|---|
| **Task coverage** | Every SSIS task type appears in the ADF pipeline as an activity of an expected type (e.g. `EXECUTE_SQL` → `Lookup` / `SqlServerStoredProcedure` / `Script`; `SCRIPT` → `AzureFunctionActivity`; `DATA_FLOW` → `Copy` / `ExecuteDataFlow`). Counts must match. | `_check_task_coverage` |
| **Linked services** | Every SSIS connection manager produced a linked service. Linked services with placeholder values (e.g. `<server>`, `<database>`) are flagged so they cannot be deployed unintentionally. | `_check_linked_services` |
| **Parameters** | Every SSIS package parameter / project parameter has a corresponding pipeline parameter. Sensitive parameters that were stripped to a `[SENSITIVE]` description block (see B3) are tolerated and reported as info. | `_check_parameters` |
| **Data flows** | Each `DataFlowTask` is realised either as a `Copy` activity (simple 1-source / 1-sink) or as an `ExecuteDataFlow` referencing a generated mapping data flow. | `_check_data_flows` |
| **Event handlers** | Every SSIS `OnError` / `OnPostExecute` event handler is recorded; they are surfaced as warnings because they require manual review (ADF has no direct equivalent — they become success/failure dependsOn paths). | `_check_event_handlers` |
| **Script Tasks** | Every Script Task that wasn't auto-converted to a `SetVariable` produces a generated Function stub under `stubs/<FunctionName>/__init__.py`. Stubs must exist on disk. Missing stubs are errors. | `_check_script_tasks` |
| **SDK dry-run** *(opt-in, on by default)* | Each generated JSON is deserialized through `azure.mgmt.datafactory` SDK models. Catches schema-shape errors locally without deploying. | `validate_parity(dry_run=True)` |
| **Factory reachability** *(opt-in)* | When `subscription_id` + `resource_group` + `factory_name` are supplied, calls `client.factories.get` to confirm the target factory exists and the caller has read access. Pure RBAC / connectivity probe — does not deploy. | `_check_factory` |

## What it does NOT compare

- **Row-level data parity.** This validator is structural. To verify that a
  converted pipeline produces the same data as the original SSIS package, run
  the source SSIS package and the deployed ADF pipeline against the same input
  on the same day, then compare the two output sets with the customer's own
  data-quality framework. That is a separate exercise and is outside the scope
  of this tool.
- **Performance parity.** RU / DIU / SHIR sizing is not estimated.
- **Per-row transformation correctness.** Mapping data flow transformations
  (e.g. derived column expressions, lookup match rules) are emitted as JSON but
  the validator does not execute them.

## Output

Two artifacts are returned:

1. A **Markdown report** — human-readable, suitable for paste into a PR or a
   migration ticket.
2. A **JSON result** — machine-readable, with this top-level shape:

```jsonc
{
  "ok": true,
  "package_name": "ADDS-MIPS-TC",
  "output_dir": "...",
  "summary": { "ssis_tasks": 6, "adf_activities": 6, ... },
  "matches": [ "FileSystemTask: 3 SSIS task(s) → 4 ADF activity(ies) ..." ],
  "issues": [
    { "severity": "warning", "category": "linked_service",
      "message": "...placeholder values...", "detail": "..." }
  ],
  "artifact_dryrun": { "pipelines": 1, "linked_services": 2, ... },
  "factory_check": { ... }   // only if Azure identifiers were supplied
}
```

`ok` is `false` only when at least one issue has severity `error`. `warning`
issues do not fail the gate but should be reviewed before deployment. Build
pipelines should treat the JSON as the source of truth.

## Worked example — LNI ADDS-MIPS-TC

Captured against the LNI ADDS-MIPS-TC package:

- Markdown report: [PARITY_REPORT_ADDS_MIPS_TC.md](../test-lni-packages/PARITY_REPORT_ADDS_MIPS_TC.md)
- JSON result: [PARITY_REPORT_ADDS_MIPS_TC.json](../test-lni-packages/PARITY_REPORT_ADDS_MIPS_TC.json)

Headline findings on this sample:

- ✅ Task coverage matches: 6 SSIS tasks → 6 ADF activities. All three task
  types (`FileSystemTask`, `DataFlowTask`, `ScriptTask`) line up with the
  expected activity types.
- 🟡 Two linked services contain placeholder values that must be replaced with
  real connection strings (or, preferably, Key Vault references) before the
  factory can run anything. The validator caught these — they are exactly the
  kind of thing that would otherwise fail at deploy time with an opaque ADF
  error.
- 🟡 Two Script Tasks require manual porting. The validator does not refuse
  the conversion — it surfaces the work item so the migration team has it on
  their list before they go live.
- ✅ SDK dry-run passes for all 1 pipeline, 2 linked services, 2 datasets, and
  1 trigger.

## Catching a known defect

The parity validator was designed to catch the kind of regression we hit in
[BACKLOG.md](BACKLOG.md) item B1 (Copy activity emitting `AzureSqlSink` against
a `DelimitedText` dataset). When the SDK dry-run deserializes a Copy activity
whose `sink.type` does not match the dataset type referenced from the same
activity, `azure.mgmt.datafactory` raises a schema error that the validator
captures under `artifact_dryrun.errors`. That is the mechanism that converts
"silently broken JSON" into a hard pre-deploy stop.

## Running the report

From the MCP client (recommended):

```jsonc
{
  "tool": "validate_conversion_parity",
  "arguments": {
    "package_path": "C:/path/to/MyPackage.dtsx",
    "output_dir":   "C:/path/to/adf/MyPackage",
    "dry_run":      true,
    "pdf_report_path": "C:/path/to/MyPackage_PreDeployment.pdf"  // optional
  }
}
```

To additionally probe the target factory, pass `subscription_id`,
`resource_group`, and `factory_name`. The caller must have at least
`Microsoft.DataFactory/factories/read` permission.
