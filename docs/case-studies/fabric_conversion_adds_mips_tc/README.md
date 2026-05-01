# Case study — converting LNI ADDS-MIPS-TC to Microsoft Fabric

This case study walks through converting a real SSIS package from the LNI
(Washington State Department of Labor and Industries) sample estate to
**Microsoft Fabric Data Pipelines**. It is the Fabric counterpart to the
ADF worked example in the parent project README.

The artifact counts and warnings below are pinned by
[`tests/test_fabric_phase4.py`](../../../tests/test_fabric_phase4.py)
(`test_case_study_lni_fabric_conversion_artifact_counts`) so this doc cannot
silently drift from what the converter actually emits.

---

## Source package

`ADDS-MIPS-TC.dtsx` from the LNI estate — a small but representative
package:

| | |
|---|---|
| Tasks | 6 |
| Connection Managers | 2 |
| Variables | several |

| Task | Type |
|---|---|
| Copy Template | `FileSystemTask` |
| Data Flow Task | `DataFlowTask` |
| Database Access Configuration | `ScriptTask` |
| Job Schedule Message Initialize | `ScriptTask` |
| Rename File | `FileSystemTask` |
| Set Attributes | `FileSystemTask` |

| Connection Manager | Type | Notes |
|---|---|---|
| `Database_Source_Connection_Manager` | OLE DB → SQL Server | On-prem host: `LNIsqTumSTGEX.lni.wa.lcl\INT,49377`, database `DAILYTRANS` |
| `Flat File Connection Manager` | Flat File | Destination CSV |

The Data Flow Task is structurally simple: one OLE DB source
(`SELECT … FROM [dbo].[activity]`) feeding one Flat File destination, no
transformations.

---

## Running the conversion

```python
from pathlib import Path
from ssis_modernization_agent.parsers.readers.local_reader import LocalReader
from ssis_modernization_agent.fabric import (
    convert_package_to_fabric,
    validate_fabric_artifacts,
)

src = Path(r"C:\source\test-lni-packages\ADDS-MIPS-TC.dtsx")
out = Path(r"C:\source\test-lni-packages\fabric\ADDS-MIPS-TC")
out.mkdir(parents=True, exist_ok=True)

package = LocalReader().read(src)
result = convert_package_to_fabric(package, out)
print(result["artifacts_generated"])
# {'pipelines': 1, 'notebooks': 1, 'connections_required': 3, 'function_stubs': 2}

print(validate_fabric_artifacts(out))
# {'valid': True, 'errors': [], 'warnings': [], 'pipelines': 1, 'notebooks': 1}
```

The full set of generated artifacts is checked in alongside this README under
[`artifacts/`](artifacts/) so a reader can inspect them without running the
agent.

---

## Generated artifact map

```
artifacts/
├── connections_required.json                       # 3 connection placeholders
├── notebook_placeholders.json                      # 1 notebook placeholder
├── pipeline/
│   └── PL_ADDS_MIPS_TC.DataPipeline/
│       ├── pipeline-content.json                   # 6 activities
│       └── .platform
├── notebook/
│   └── NB_ADDS-MIPS-TC__Data_Flow_Task.Notebook/
│       ├── notebook-content.py                     # PySpark stub, 3 TODO cells
│       └── .platform
└── stubs/
    ├── Database_Access_Configuration/              # Python Function stub
    │   ├── __init__.py
    │   └── function.json
    └── Job_Schedule_Message_Initialize/            # Python Function stub
        ├── __init__.py
        └── function.json
```

---

## Pipeline activities — what the converter chose

The 6 SSIS tasks become 6 Fabric Data Pipeline activities:

| SSIS task | Fabric activity (`type`) | Why |
|---|---|---|
| Copy Template (`FileSystemTask`) | `WebActivity` | Fabric pipelines have no FileSystem activity; the converter routes file ops through a Web call to a Function stub. |
| Set Attributes (`FileSystemTask`) | `WebActivity` | Same. |
| Rename File (`FileSystemTask`) | `WebActivity` | Same. |
| Database Access Configuration (`ScriptTask`) | `AzureFunction` | Script Tasks always become Function stubs (this is the same flow as the ADF target). |
| Job Schedule Message Initialize (`ScriptTask`) | `AzureFunction` | Same. |
| Data Flow Task (`DataFlowTask`) | **`Copy`** | The dispatcher classified this DFT as simple (1 source, 1 destination, no transforms), so it emits a Copy activity rather than a Mapping Data Flow. |

> **Honest finding — the notebook stub for this DFT is not wired in.**
> Because the DFT was classified as "simple" and converted to a `Copy`
> activity, the generated pipeline does **not** reference the
> `NB_ADDS-MIPS-TC__Data_Flow_Task.Notebook` stub. The converter emits the
> notebook stub anyway as a courtesy so the porter has a starting point if
> they later decide to reshape the DFT. A more sophisticated DFT (with
> transformations, multiple sources, or a fanout) would have produced a
> `TridentNotebook` activity pointing at this stub via
> `notebook_placeholders.json`.
>
> This is a real shape of the converter the buyer needs to see: the
> Fabric path inherits the ADF dispatcher's "simple Data Flow → Copy"
> classification, so the notebook stub is conditional. If you would
> prefer every DFT to become a notebook regardless, raise that as an
> issue — the dispatch can be policy-driven.

---

## Connection placeholders (`connections_required.json`)

Three entries — two for the SSIS Connection Managers and one synthetic
entry for the implicit Function host linked service:

```json
{
  "schema_version": "1.0",
  "connections": [
    {
      "placeholder_id": "00000000-0000-4000-8000-777fc457c8f0",
      "ssis_connection_manager_id": "0B15F767-4FE4-49D3-A092-AF91CE312F08",
      "ssis_connection_manager_name": "Database_Source_Connection_Manager",
      "fabric_connection_type": "SQL",
      "server": "LNIsqTumSTGEX.lni.wa.lcl\\INT,49377",
      "database": "DAILYTRANS",
      "notes": [
        "On-prem host detected — Fabric requires an On-Premises Data Gateway. Configure the OPDG before resolving this connection."
      ]
    },
    {
      "placeholder_id": "00000000-0000-4000-8000-99b106406332",
      "ssis_connection_manager_name": "Flat File Connection Manager",
      "fabric_connection_type": "AzureDataLakeStorage"
    },
    {
      "placeholder_id": "00000000-0000-4000-8000-5c509608a690",
      "ssis_connection_manager_name": "LS_AzureFunction",
      "fabric_connection_type": "Web",
      "notes": [
        "Synthetic connection for ADF linked service 'LS_AzureFunction' — no SSIS Connection Manager backing this. Provision the Fabric connection (e.g. Web/Function endpoint) before deploy."
      ]
    }
  ]
}
```

The **OPDG note on the SQL connection is the headline finding**. Without an
On-Premises Data Gateway provisioned and bound to the Fabric workspace, this
package cannot connect to its source database from Fabric — full stop. That
note is the kind of thing the customer wants surfaced *before* they start
doing the migration, not at deploy time.

The deployer's `apply_substitutions_in_place` rewrites these placeholder
GUIDs to real Fabric Connection IDs at deploy time. A `fab connection list`
output piped into `discover_connection_ids` (out of scope for this case
study) provides the substitution map.

---

## Notebook placeholder

```json
{
  "schema_version": "1.0",
  "placeholders": {
    "00000000-0000-4000-9000-59ae22c4317c": "NB_ADDS-MIPS-TC__Data_Flow_Task.Notebook"
  }
}
```

The notebook stub itself ([`notebook-content.py`](artifacts/notebook/NB_ADDS-MIPS-TC__Data_Flow_Task.Notebook/notebook-content.py))
is a Python script with three `TODO` cells — read sources, apply
transformations, write destinations — and a header comment lifting the
SSIS components verbatim:

```python
# Source SSIS package : ADDS-MIPS-TC
# Source Data Flow    : Data Flow Task
# SSIS components     : 2
#
# This stub MUST be hand-ported. The agent cannot translate Mapping
# Data Flow components to PySpark automatically. Treat each TODO
# block below as a checklist item.
```

The header explicitly names the components in declaration order
(`FlatFileDestination: CSV Output`, `OleDbSource: Data Source | table=[dbo].[activity]`)
so the porter can map them 1:1 to PySpark `read.format(...)` and
`write.format(...)` calls.

---

## What still needs human work

After running the converter, the migration ticket for this package contains:

1. **Provision an On-Premises Data Gateway** bound to the target Fabric
   workspace and resolve the SQL Connection placeholder against it.
2. **Provision the OneLake / Azure Data Lake Storage destination** the
   `Flat File Connection Manager` placeholder will resolve to. Decide
   whether the destination becomes a Lakehouse table (preferred) or stays
   a CSV-on-storage.
3. **Provision the Function host** for the two Script Task stubs and resolve
   the synthetic `LS_AzureFunction` Web connection placeholder against its
   SCM endpoint.
4. **Hand-port the two Script Tasks** — see the parent project's worked
   port at [docs/case-studies/script_task_port_database_access_configuration/](../script_task_port_database_access_configuration/README.md)
   for the canonical pattern (Key-Vault-backed, parameterized linked
   service). The Database Access Configuration script in this package is
   the *same* script that case study covers, so the port is reusable.
5. **Decide whether to reshape the Data Flow Task as a notebook.** The
   converter took the "simple DFT → Copy" path. A reviewer who would prefer
   the DFT to land as a PySpark notebook (for consistency, observability,
   or because they want the column lineage in OneLake) needs to:
   - Replace the `Copy` activity in `pipeline-content.json` with a
     `TridentNotebook` activity referencing the placeholder GUID
     `00000000-0000-4000-9000-59ae22c4317c`.
   - Hand-port the `TODO` cells in `notebook-content.py`.

Items 1–3 are roughly 30 minutes each in a prepared Fabric tenant. Item 4
is 3–4 hours per Script Task (see the worked port for the methodology).
Item 5 is optional and ranges from 1 hour (trivial Copy) to a half-day
(non-trivial reshape).

---

## How this differs from the ADF conversion of the same package

The ADF conversion of `ADDS-MIPS-TC.dtsx` produces:

* 1 ADF pipeline JSON (different shape but equivalent activity graph).
* 2 Linked Services (one per CM), in `linkedService/` — **not** in a
  `connections_required.json` manifest, because ADF resolves linked
  services at deploy time directly.
* 2 Datasets (one per data flow source / destination).
* 0 Mapping Data Flow JSONs — the simple DFT also became a `Copy`
  activity on the ADF side.
* 2 Function stubs (identical to the Fabric side; the Script Task
  pipeline is shared).
* 1 Stopped Trigger template.

The agent's [parity validator](../../conversion/parity.md) covers the ADF side
end-to-end. There is **no equivalent SSIS↔Fabric structural parity
validator** as of this case study — `validate_fabric_artifacts` checks
the generated JSON shape only, not its faithfulness to the SSIS source.
That gap is the headline reason a buyer cannot yet ship a Fabric
conversion at the same level of evidence as an ADF one.

---

## Reproducing this case study

1. Clone the repo and check out the `fabric` branch (until merged).
2. `pip install -e .` from the repo root.
3. Run the snippet in [Running the conversion](#running-the-conversion)
   above against any local copy of `ADDS-MIPS-TC.dtsx`.
4. Diff the resulting directory against [`artifacts/`](artifacts/).

Or just run the pinning test:

```bash
python -m pytest tests/test_fabric_phase4.py::test_case_study_lni_fabric_conversion_artifact_counts -v
```
