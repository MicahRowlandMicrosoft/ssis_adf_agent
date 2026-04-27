# Behavioral Data-Flow Parity Harness (P4-1)

`validate_conversion_parity` (see [PARITY.md](PARITY.md)) checks **structural**
parity: task counts, linked services, parameters, schema-shape of generated
JSON.  It does *not* compare actual data.  A Conditional Split that routes
the wrong rows, a Derived Column with an off-by-one expression, or a Lookup
that no-matches when it should match — **none of these are caught** by
structural parity.

The `compare_dataflow_output` tool closes that gap.  It runs the *same*
controlled input set through:

1. The source SSIS Data Flow Task (via `dtexec.exe`), and
2. The converted ADF Mapping Data Flow (via an ADF debug session),

then emits a row-and-column diff report.

> ⚠️ **What this proves and does not prove.**  A green report means the
> Mapping Data Flow produced the same output as the SSIS Data Flow **for the
> input you supplied**.  Coverage is exactly as good as your input dataset.
> The harness does not prove behavior on inputs you did not test, performance
> parity, error-handling parity, or side-effect parity (e.g. file moves,
> audit-table writes).

---

## Three modes

| Mode | SSIS side | ADF side | When to use |
|---|---|---|---|
| **`captured`** (default) | Replay a pre-captured CSV | Replay a pre-captured CSV | Worked examples, regression tests, air-gapped reviews. No `dtexec` or live Azure required. |
| **`live`** | `dtexec.exe` | ADF debug session | One-shot evidence captures.  Slow; needs SSIS installed and an ADF reachable by the deploying identity. |
| **`mixed`** | Either | Either | Spot checks.  E.g. captured SSIS + live ADF when the original SSIS environment is gone but the converted MDF needs validating. |

Captured mode is the recommended day-to-day workflow:

1. Capture the SSIS output **once** by running the package against a curated
   input set in your existing SSIS environment.  Save the destination CSV.
2. Capture the ADF output **once** by running the converted Mapping Data Flow
   against the same input via the ADF debug-preview UI or `dtexec` mode of
   this harness.  Save the sink CSV.
3. Check both CSVs into source control alongside the input CSV.
4. Re-run `compare_dataflow_output` in `captured` mode in CI on every change.
   The diff engine is pure — sub-second on thousands of rows.

---

## Worked example

The repository ships a complete worked example under
[tests/fixtures/dataflow_parity/](tests/fixtures/dataflow_parity/):

| File | Purpose |
|---|---|
| [`sales_input.csv`](tests/fixtures/dataflow_parity/sales_input.csv) | Six rows fed into both sides.  Three regions, mixed discounts. |
| [`sales_ssis_output.csv`](tests/fixtures/dataflow_parity/sales_ssis_output.csv) | Captured SSIS Data Flow output (the expected truth). |
| [`sales_adf_output.csv`](tests/fixtures/dataflow_parity/sales_adf_output.csv) | Captured ADF Mapping Data Flow output (correct conversion).  Should match. |
| [`sales_adf_output_buggy.csv`](tests/fixtures/dataflow_parity/sales_adf_output_buggy.csv) | Captured ADF output with two seeded regressions: row 4 `net_amount` (discount mis-applied) and row 6 `tier` (classification regressed). |

Two tests in [tests/test_dataflow_parity_worked_example.py](tests/test_dataflow_parity_worked_example.py) drive the fixtures:

```bash
python -m pytest tests/test_dataflow_parity_worked_example.py -v
```

The first test asserts a green report when the conversion is correct.  The
second test asserts that the buggy conversion is caught with **exactly the
two value mismatches** above — proving the harness will surface real
regressions, not just shrug at them.

---

## Calling the MCP tool

```python
{
  "package_path": "C:/repo/sales/Package.dtsx",
  "dataflow_task_name": "DFT_Sales",
  "adf_dataflow_path": "C:/repo/sales/adf/dataflow/DF_Sales.json",
  "input_dataset_path": "C:/repo/sales/parity/inputs/sales_input.csv",
  "key_columns": ["id"],
  "ignore_columns": ["LoadDateTime"],
  "numeric_tolerance": 0.001,

  "mode": "captured",
  "ssis_captured_csv": "C:/repo/sales/parity/captured/sales_ssis_output.csv",
  "adf_captured_csv":  "C:/repo/sales/parity/captured/sales_adf_output.csv",

  "report_path":   "C:/repo/sales/parity/report.md",
  "diff_json_path":"C:/repo/sales/parity/diff.json"
}
```

For live mode replace the captured CSV paths with the `dtexec` and
`adf_debug` configuration blocks (see the tool schema in `mcp_server.py`).

---

## Output

The report contains:

* **Verdict** — `✅ PASS` only when row counts match, schemas match, and zero
  value mismatches.  `❌ FAIL` on any drift.
* **Row counts** — SSIS, ADF, and matched-row count.
* **Schema drift** — columns that exist on only one side.
* **Diff summary** — counts by kind: `value_mismatch`, `missing_in_adf`,
  `extra_in_adf`, `duplicate_count`.
* **First diffs** — up to 50 per-cell discrepancies with key, column, and the
  two values, suitable for opening a defect.

The same payload is also written as JSON for machine consumption (CI gates,
ticket auto-creation, dashboards).

---

## Comparison knobs

| Knob | Default | Purpose |
|---|---|---|
| `key_columns` | required | Identifies each row across the two sides. |
| `compare_columns` | all common non-key columns | Scope the comparison to just the columns you care about. |
| `ignore_columns` | `()` | Exclude non-deterministic fields (e.g. `LoadDateTime`, `RowGUID`). |
| `ignore_case` | `false` | Case-insensitive string comparison. |
| `strip_whitespace` | `true` | Normalize trailing/leading whitespace. |
| `numeric_tolerance` | `0.0` | Absolute tolerance for floats — set to e.g. `0.001` to absorb harmless rounding differences. |
| `max_diffs` | `1000` | Cap the diff list (the summary counts remain accurate). |

---

## Authoring your own runner

Both runners are Python protocols.  Customers whose SSIS environment doesn't
fit `dtexec` (encrypted packages, Project deployment, etc.) can subclass or
substitute their own:

```python
from ssis_adf_agent.parity import (
    compare_dataflow_output,
    SSISDataFlowRunner,
    AdfDataFlowRunner,
    RunnerResult,
)

class MyEnterpriseSsisRunner:
    name = "enterprise-ssis"

    def run(self, *, package_path, dataflow_task_name,
            input_dataset_path, work_dir):
        # ...invoke your build server, capture output...
        return RunnerResult(rows=my_rows, runner_name=self.name)
```

Pass that instance to `compare_dataflow_output(ssis_runner=...)`.  The diff
engine doesn't care where the rows came from.

---

## What this is *not*

* **Not a guarantee of conversion correctness.**  It tests the inputs you
  supply and nothing else.
* **Not a performance harness.**  Use ADF Monitoring + Cost Management for
  that (see [OBSERVABILITY.md](OBSERVABILITY.md)).
* **Not a side-effect harness.**  Pre/post `Execute SQL` audit writes, file
  moves, and stored-procedure calls are out of scope here — they belong in
  end-to-end smoke tests via `smoke_test_pipeline` / `smoke_test_wave`.
