# Fabric Data Pipelines target

## What this page covers

The agent ships **two conversion targets** for SSIS packages:

| Target | Output | When to pick it |
|---|---|---|
| **ADF** (default) | Azure Data Factory pipeline / linkedService / dataset / dataflow / trigger JSON, plus Python Function stubs for Script Tasks | The customer is standardizing on Azure Data Factory; Mapping Data Flow is acceptable; per-DIU billing is fine. |
| **Fabric** | Microsoft Fabric Data Pipelines (`pipeline-content.json` + `.platform`), Fabric Notebook stubs (`notebook-content.py` + `.platform`) for every Data Flow Task, a `connections_required.json` manifest of unresolved Fabric Connections, and the same Python Function stubs for Script Tasks | The customer is standardizing on Microsoft Fabric; reserved-capacity (F-SKU) billing is preferred; the team is willing to hand-port Data Flow Tasks to PySpark. |

Both targets share the same parser, the same dispatcher, and the same Script
Task pipeline. The Fabric layer translates the produced ADF activity dicts
into the Fabric pipeline shape and substitutes Mapping Data Flow references
with `TridentNotebook` activities pointing at PySpark notebook stubs.

> **Conversion is structural; the data flow is yours.** Fabric Data Pipelines
> have no Mapping Data Flow equivalent. Every SSIS Data Flow Task becomes a
> generated PySpark notebook stub with `TODO` blocks marking each component
> (sources, transforms, destinations). The agent does not auto-translate
> SSIS transformations to PySpark — that is hand-port work, scoped by the
> Fabric Risk emitted on the migration plan.

---

## Picking the target

The target is set explicitly when you ask the agent to design or estimate:

* **`propose_adf_design`** — pass `target="fabric"`. The migration plan is
  saved with `target: "fabric"`, the summary calls out the PySpark hand-port
  consequence, and a `MEDIUM` (or `HIGH` if more than two DFTs) Risk is
  added per data flow so the porting effort is on the migration ticket.
* **`estimate_adf_costs`** — pass `target="fabric"`. The estimator returns
  a Fabric F-SKU sizing line item (peak CU + recommended SKU at 30%
  headroom by default), a OneLake storage line item, and a PAYG line item
  for bursty workloads. F-SKU monthly list prices for F2 through F2048 are
  documented inline in `migration_plan/fabric_costs.py`.
* **`convert_ssis_package`** does not currently take a `target` parameter
  for ADF vs. Fabric — Fabric conversion goes through the `fabric` package
  surface (`convert_package_to_fabric`) directly. This is intentional: the
  full estate-scale orchestration tooling (`convert_estate`, `validate_*`)
  is currently ADF-only and the Fabric path is exercised through the
  Python API + the per-package case study below.

---

## What the Fabric converter produces

For a package containing one or more Data Flow Tasks, two Script Tasks, and
two Connection Managers:

```
fabric/<PackageName>/
  connections_required.json              # one entry per CM + synthetic LSes
  notebook_placeholders.json             # GUID → notebook display name
  pipeline/
    <PipelineName>.DataPipeline/
      pipeline-content.json              # Fabric Data Pipeline activities
      .platform                          # Fabric workspace metadata sidecar
  notebook/
    NB_<PackageName>__<DFTName>.Notebook/
      notebook-content.py                # PySpark stub with TODO blocks
      .platform                          # Fabric workspace metadata sidecar
  stubs/                                 # Python Function stubs (unchanged from ADF)
    <ScriptTaskName>/
      __init__.py
      function.json
```

### Connections (`connections_required.json`)

Every SSIS Connection Manager is mapped to a **placeholder Fabric
Connection ID** (`00000000-0000-4000-8000-<sha256[:12]>`) and recorded in
`connections_required.json` with the inferred Fabric connection type, the
host / database / endpoint values lifted from the SSIS CM, and a `notes`
list calling out any deployment caveats — most importantly the **on-prem
detection note** that pins the requirement for an On-Premises Data Gateway
(OPDG) before the connection can be resolved at deploy time.

The deployer (`deployer/fabric_deployer.py`) reads this manifest and
substitutes the placeholders with real Fabric Connection IDs at deploy
time via `apply_substitutions_in_place`.

### Notebooks (`notebook_placeholders.json`)

Every PySpark notebook stub is registered with a placeholder GUID
(`00000000-0000-4000-9000-<sha256[:12]>`) so the pipeline JSON can refer to
notebooks before they exist in the workspace. The deployer's
`discover_notebook_ids` resolves the placeholders to real notebook IDs
once the notebooks have been deployed.

### PySpark notebook stubs

Each Data Flow Task becomes one `notebook-content.py` file with three
`TODO` cells: read sources, apply transformations, write destinations. The
stub lifts the SSIS component names verbatim into comments so the porter
sees exactly which DFT components they need to translate. There is no LLM
involved — the stub is a deterministic structural skeleton.

> **T-SQL notebooks are on the backlog (P6-1).** SSIS DFTs that are pure
> SQL (Execute SQL only, no transformations) currently produce a PySpark
> stub even though a T-SQL notebook would be a better fit. This is tracked
> as P6-1 in [backlog.md](../development/backlog.md).

---

## Deploying

The Fabric deployer (`deployer/fabric_deployer.py`) shells out to the
[`fabric-cli`](https://github.com/microsoft/fabric-cli) (`fab`) via a
`FabRunner` Protocol so the test suite can substitute a fake. The deployer:

1. Refuses to create a workspace without an explicit `capacity_id` —
   silently provisioning Fabric capacity is exactly the failure mode that
   creates surprise bills.
2. Imports notebooks first (so their IDs are knowable), then resolves the
   `notebook_placeholders.json` placeholders against the freshly-deployed
   notebooks, then imports the pipeline.
3. `apply_substitutions_in_place` rewrites every Connection placeholder
   (`externalReferences.connection`) and every notebook placeholder
   (`TridentNotebook.typeProperties.notebookId`) before the Fabric API ever
   sees the JSON.

> **There is no `validate_conversion_parity` for Fabric yet.** Structural
> validation of the generated Fabric JSON is provided by
> `validate_fabric_artifacts` (in the `fabric` package), but the SSIS↔Fabric
> structural diff (the equivalent of `validate_conversion_parity`) is not
> shipped. Use the **behavioral parity harness** below to validate
> end-to-end correctness once a Data Flow has been ported to PySpark.

---

## Behavioral parity for Fabric

The existing `compare_dataflow_output` MCP tool accepts a `target_label`
parameter. Pass `"Fabric"` to get a report whose headers, table columns,
and prose all read "Fabric" instead of "ADF". The underlying JSON diff
keys (`adf_row_count`, `missing_in_adf`, etc.) are **deliberately not
renamed** — keeping them stable protects existing CI assertions.

A Fabric notebook captured-output workflow looks like:

1. Run the ported PySpark notebook against a captured input dataset in a
   Fabric workspace, write the destination to OneLake, export it as CSV.
2. Pass that CSV as `adf_captured_csv` to `compare_dataflow_output`
   alongside the captured SSIS CSV. Set `target_label="Fabric"`.
3. The harness applies the same row-key matching, schema-drift detection,
   numeric tolerance, and ignore-column rules as the ADF path.

Live-mode comparison against a Fabric notebook is **not implemented** —
the `AdfDebugRunner` cannot drive a Fabric notebook session. Customers
needing live parity should subclass `AdfDataFlowRunner` and wire their
own runner that posts the input to a notebook job, polls the result, and
returns the rows. The pure `diff_rows` engine doesn't care where the rows
came from.

---

## Worked example

See [docs/case-studies/fabric_conversion_adds_mips_tc/](../case-studies/fabric_conversion_adds_mips_tc/README.md)
for an end-to-end walkthrough on the LNI ADDS-MIPS-TC sample. It captures:

* The exact input package (6 tasks, 2 CMs, 1 DFT, 2 Script Tasks).
* The exact generated artifacts (1 pipeline, 1 PySpark notebook stub,
  3 connection placeholders, 2 Function stubs).
* The on-prem OPDG warning the converter emits for the SQL Server CM.
* The PySpark notebook stub layout with its three TODO cells.
* The unresolved hand-port checklist a buyer needs to plan around.

The artifact counts in the case study are pinned by
[`tests/test_fabric_phase4.py`](../../tests/test_fabric_phase4.py)
(`test_case_study_lni_fabric_conversion_artifact_counts`) so the doc
cannot drift from reality.

---

## What this is NOT

* Not a one-click migration. Every Data Flow Task needs to be hand-ported
  to PySpark by an engineer who understands both sides.
* Not a guarantee that Fabric will be cheaper than ADF — `estimate_adf_costs`
  with `target="fabric"` returns the F-SKU list price, but reserved-capacity
  vs. PAYG vs. consolidating with an existing Fabric tenant is a customer
  decision.
* Not a replacement for the structural parity validator on the ADF side.
  Fabric structural parity (SSIS task → Fabric activity coverage) is not
  shipped and is tracked separately.
* Not officially-supported by Microsoft as a product. See
  [SUPPORT.md](../../SUPPORT.md) for the support model.
