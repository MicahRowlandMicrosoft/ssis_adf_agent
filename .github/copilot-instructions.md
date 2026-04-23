---
applyTo: "**"
---

# SSIS → ADF Conversion Agent — Copilot Domain Knowledge

## Project Overview
This project exposes an MCP server (`ssis-adf-agent`) that reads SSIS packages (.dtsx)
and converts them to Azure Data Factory (ADF) JSON artifacts.  **28 tools** are
available via the MCP server; use them in the order that makes sense for the task.
The table below lists the most commonly used ones — see `mcp_server.py` for the
full set (it is the single source of truth).

---

## MCP Tools Available

| Tool | Purpose |
|---|---|
| `scan_ssis_packages` | Discover .dtsx files from local path, Git repo, or SQL Server |
| `analyze_ssis_package` | Complexity score, gap analysis, and dependency order for one package |
| `convert_ssis_package` | Full conversion to ADF JSON (pipeline, linked services, datasets, data flows, triggers) |
| `validate_adf_artifacts` | Structural validation of generated JSON before deploying |
| `deploy_to_adf` | Push artifacts to Azure Data Factory via SDK |
| `deploy_function_stubs` | Zip-deploy generated Azure Function stubs to an existing Function App |
| `provision_function_app` | Create Azure resources (Storage, App Insights, Plan, Function App) for hosting stubs |

---

## SSIS Component Mapping

| SSIS Component | ADF Equivalent | Notes |
|---|---|---|
| Execute SQL Task | Stored Procedure / Script / Lookup Activity | Depends on ResultSetType and SQL pattern |
| Data Flow Task (simple) | Copy Activity | Single source → single destination, no transformations |
| Data Flow Task (complex) | Execute Data Flow Activity (Mapping Data Flow) | Multiple sources, transformations, or fanout |
| Execute Package Task | Execute Pipeline Activity | Child pipeline must also be converted |
| Script Task (C# / VB) | Azure Function Activity | Stub generated at `stubs/<FunctionName>/__init__.py`; requires manual porting |
| ForEach Loop Container | ForEach Activity | Expression varies by enumerator type |
| For Loop Container | SetVariable (init) + Until Activity + SetVariable (increment) | Condition logic is inverted |
| Sequence Container | Flattened into parent with dependsOn chaining | No ADF equivalent |
| File System Task | Copy Activity (Azure paths) or Web Activity → Azure Function | Local paths need Azure-path substitution |
| Execute Process Task | Web Activity → Azure Function | Manual: wrap process call in Function |
| FTP Task | FTP connector via Copy Activity | Requires FTP linked service |
| Send Mail Task | Logic App / Web Activity | No native ADF equivalent |
| Event Handler (OnError) | Pipeline fails path / error handling | Converted to sub-pipeline reference |
| Event Handler (OnPostExecute) | Succeeded dependency path | Converted to sub-pipeline reference |

---

## ADF Artifact Structure

Output directories written by `convert_ssis_package`:

```
<output_dir>/
  pipeline/           ← <PackageName>.json
  linkedService/      ← one per ConnectionManager
  dataset/            ← one per data flow source / destination
  dataflow/           ← one per complex Data Flow Task
  trigger/            ← ScheduleTrigger template (Stopped state)
  stubs/              ← Azure Function stubs for Script Tasks
    <FunctionName>/
      __init__.py     ← Python stub with TODO blocks
      function.json
```

---

## Key Domain Rules

1. **Triggers are ALWAYS deployed in Stopped state** — activate them manually after validating pipelines.
2. **Script Task stubs require manual work** — the generated Python stub preserves the original C# variable interface as TODO comments.
3. **Encryption warnings** — packages with `EncryptAllWithPassword` protection level may have missing connection string passwords.
4. **SSIS variables syntax** — `@[User::VarName]` / `@(Ns::VarName)` → `@variables('VarName')` in ADF expressions.
5. **For Loop condition inversion** — SSIS loop `InitExpression` runs once; `EvalExpression` is true-while-looping (opposite of ADF Until's doWhileCondition).  The converter negates the expression.
6. **Sequence containers are flattened** — tasks inside are promoted to the parent with `dependsOn` chaining.
7. **Precedence constraints** — Expression-type constraints produce ADF expressions on `dependsOn`; conjunctions (AND) vs. disjunctions (OR) are resolved via the dependency graph.
8. **Re-deployment** — already-existing linked services / datasets are overwritten (put_or_update semantics).

---

## Authentication for Deployment

`deploy_to_adf` uses `DefaultAzureCredential` in this order:
1. Environment variables (`AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, `AZURE_TENANT_ID`)
2. Workload identity
3. Azure CLI (`az login`)
4. Azure PowerShell

Run `az login` before deploying if using a developer machine.

---

## Complexity Score Guide

| Score | Label | Typical Effort |
|---|---|---|
| 0–30 | Low | < 1 day |
| 31–55 | Medium | 1–3 days |
| 56–80 | High | 3–5 days |
| 81–100 | Very High | 1–3 weeks |

Score drivers: Script Tasks (+2 to +25, content-aware: trivial/simple/moderate/complex), Data Flow Tasks (+5 base, +1.5 per component), ForEach/ForLoop (+5 each), Event Handlers (+4 each), Nesting depth (+3 per level beyond 1), Unknown tasks (+10 each), Linked server references (+8 each), Cross-database references (+3 each).

---

## Python Package Layout

```
ssis_adf_agent/
  mcp_server.py          ← MCP entry point (this is the stdio server)
  parsers/
    models.py            ← Pydantic IR models
    ssis_parser.py       ← .dtsx XML parser
    readers/
      local_reader.py
      git_reader.py
      sql_reader.py
  analyzers/
    complexity_scorer.py
    gap_analyzer.py
    dependency_graph.py
  converters/
    dispatcher.py
    base_converter.py
    control_flow/        ← 8 converters
    data_flow/           ← 3 converters
  generators/            ← 5 generators (pipeline, linked_service, dataset, dataflow, trigger)
  deployer/
    adf_deployer.py
```
