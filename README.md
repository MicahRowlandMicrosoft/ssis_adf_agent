# SSIS → ADF Agent

![Python](https://img.shields.io/badge/python-3.11%2B-blue)
![License](https://img.shields.io/badge/license-MIT-green)

An MCP (Model Context Protocol) server that reads SSIS packages (`.dtsx`) and converts them to Azure Data Factory (ADF) JSON artifacts, exposed as tools directly inside **GitHub Copilot**.

```
.dtsx file(s)
      │
      ▼
┌─────────────────────────────┐
│      ssis-adf-agent         │  ← MCP stdio server
│                             │
│  scan → analyze → convert   │
│      → validate → deploy    │
└─────────────────────────────┘
      │
      ▼
ADF JSON artifacts
(pipeline / linkedService / dataset / dataflow / trigger)
      │
      ▼
Azure Data Factory
```

---

## Table of Contents

- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Registering as an MCP Server in VS Code](#registering-as-an-mcp-server-in-vs-code)
- [Trying It Out — Samples Directory](#trying-it-out--samples-directory)
- [Usage — End-to-End Walkthrough](#usage--end-to-end-walkthrough)
  - [1. Scan for packages](#1-scan-for-packages)
  - [2. Analyze a package](#2-analyze-a-package)
  - [3. Convert a package](#3-convert-a-package)
  - [4. Validate generated artifacts](#4-validate-generated-artifacts)
  - [5. Deploy to Azure Data Factory](#5-deploy-to-azure-data-factory)
- [Using the Built-in Prompt Files](#using-the-built-in-prompt-files)
- [Authentication](#authentication)
- [SSIS Component Mapping Reference](#ssis-component-mapping-reference)
- [Generated Artifact Structure](#generated-artifact-structure)
- [Manual Steps After Conversion](#manual-steps-after-conversion)
- [Development](#development)
- [License](#license)

---

## Prerequisites

| Requirement | Version / Notes |
|---|---|
| **Python** | 3.11 or later |
| **pip** | Included with Python; or use [uv](https://github.com/astral-sh/uv) / [hatch](https://hatch.pypa.io/) |
| **Git** | Required if scanning packages from a Git repository |
| **ODBC Driver for SQL Server** | 17 or later — required only when scanning packages from SQL Server (`msdb`) |
| **Azure CLI** | [`az`](https://learn.microsoft.com/cli/azure/install-azure-cli) — required for `az login` when deploying from a developer machine |
| **VS Code** | Latest stable |
| **GitHub Copilot extension** | With agent / MCP support enabled |

---

## Installation

Clone the repository and install in editable mode (recommended for development):

```bash
git clone https://github.com/chsimons_microsoft/ssis_adf_agent.git
cd ssis_adf_agent
pip install -e .
```

To also install development tools (pytest, ruff, mypy):

```bash
pip install -e ".[dev]"
```

Verify the installation:

```bash
ssis-adf-agent --help
```

> **Note:** When the package is published to PyPI, you can install it with `pip install ssis-adf-agent` without cloning the repository.

---

## Registering as an MCP Server in VS Code

Add the server to your VS Code `settings.json` so GitHub Copilot can discover it as a set of agent tools.

1. Open **Command Palette** (`Ctrl+Shift+P`) → **Preferences: Open User Settings (JSON)**
2. Add the following inside the root object:

```jsonc
{
  "github.copilot.chat.experimental.mcpServers": {
    "ssis-adf-agent": {
      "type": "stdio",
      "command": "ssis-adf-agent",
      "args": []
    }
  }
}
```

> If you installed into a virtual environment, replace `"command": "ssis-adf-agent"` with the full path to the script, e.g. `"C:\\path\\to\\.venv\\Scripts\\ssis-adf-agent.exe"` (Windows) or `"/path/to/.venv/bin/ssis-adf-agent"` (macOS/Linux).

3. Restart VS Code (or reload the window: `Ctrl+Shift+P` → **Developer: Reload Window**).
4. Open **Copilot Chat**, switch to **Agent** mode, and verify that the five tools appear:
   - `scan_ssis_packages`
   - `analyze_ssis_package`
   - `convert_ssis_package`
   - `validate_adf_artifacts`
   - `deploy_to_adf`

---

## Trying It Out — Samples Directory

The `samples/` directory is intended as a convenient drop zone for `.dtsx` files you want to experiment with locally.

1. Copy one or more `.dtsx` files into `samples/`:

   ```
   samples/
     MyETLPackage.dtsx
     LoadDimCustomer.dtsx
   ```

2. When using any tool that requires a `package_path` or `path_or_connection`, supply the **absolute path** to the file or directory. For example:

   - **Windows:** `C:\Users\you\ssis_adf_agent\samples\MyETLPackage.dtsx`
   - **macOS/Linux:** `/home/you/ssis_adf_agent/samples/MyETLPackage.dtsx`

3. For output, create a directory alongside `samples/` (e.g. `adf_output/`) to keep generated artifacts separate from source packages.

> The `samples/` directory is `.gitignore`-friendly — add your test packages there without worrying about committing proprietary SSIS files.

---

## Usage — End-to-End Walkthrough

All five tools are invoked from **GitHub Copilot Chat in Agent mode**. Type your request in natural language and Copilot will call the appropriate tool(s). The sections below show what each tool does and the key parameters it accepts.

---

### 1. Scan for packages

**Tool:** `scan_ssis_packages`

Discovers all `.dtsx` files from a local directory, a Git repository, or SQL Server (`msdb`).

**Example prompts:**

```
Scan C:\Projects\LegacyETL for all SSIS packages.
```

```
Scan the git repo at https://github.com/myorg/etl-packages for SSIS packages on the release branch.
```

```
List all SSIS packages stored in SQL Server at SERVER=MYSERVER;DATABASE=msdb.
```

**Key parameters:**

| Parameter | Required | Description |
|---|---|---|
| `source_type` | Yes | `local`, `git`, or `sql` |
| `path_or_connection` | Yes | Local directory path, Git repo URL, or SQL connection string |
| `recursive` | No | Search subdirectories (default: `true`) |
| `git_branch` | No | Branch to check out when `source_type` is `git` (default: `main`) |

---

### 2. Analyze a package

**Tool:** `analyze_ssis_package`

Produces a complexity score, gap analysis, and component inventory for a single package. Run this before converting to understand the scope of manual work required.

**Example prompt:**

```
Analyze the SSIS package at C:\Projects\LegacyETL\LoadFactSales.dtsx and tell me how complex it is.
```

**Complexity score guide:**

| Score | Label | Typical Effort |
|---|---|---|
| 0–25 | Low | < 1 day |
| 26–50 | Medium | 1–3 days |
| 51–75 | High | 3–5 days |
| 76–100 | Very High | 1+ weeks |

Score drivers: Script Tasks (+20 each), Data Flow Tasks (+8 each), ForEach/ForLoop containers (+5 each), unknown task types (+10 each).

**Key output:**
- Complexity score and effort label
- Component inventory (task types, connection managers, parameters, variables)
- Gap analysis grouped by severity: `manual_required` / `warning` / `info`
- Recommended execution order of tasks

---

### 3. Convert a package

**Tool:** `convert_ssis_package`

Converts a single `.dtsx` file to a complete set of ADF JSON artifacts.

**Example prompt:**

```
Convert C:\Projects\LegacyETL\LoadFactSales.dtsx to ADF artifacts and write them to C:\adf_output\LoadFactSales.
```

**Key parameters:**

| Parameter | Required | Description |
|---|---|---|
| `package_path` | Yes | Absolute path to the `.dtsx` file |
| `output_dir` | Yes | Directory to write artifacts into |
| `generate_trigger` | No | Emit a `ScheduleTrigger` template (default: `true`) |

Sub-folders are created automatically inside `output_dir`. See [Generated Artifact Structure](#generated-artifact-structure).

---

### 4. Validate generated artifacts

**Tool:** `validate_adf_artifacts`

Checks the generated JSON files for structural correctness (required fields, valid activity references) before touching Azure. Always validate before deploying.

**Example prompt:**

```
Validate the ADF artifacts in C:\adf_output\LoadFactSales.
```

**Key parameter:**

| Parameter | Required | Description |
|---|---|---|
| `artifacts_dir` | Yes | Directory containing the generated ADF JSON files |

Fix any reported issues in the JSON files, then validate again before proceeding to deployment.

---

### 5. Deploy to Azure Data Factory

**Tool:** `deploy_to_adf`

Deploys the validated artifacts to an existing Azure Data Factory instance. Deployment order is enforced automatically: linked services → datasets → data flows → pipelines → triggers.

> **Important:** Always run a dry run first to confirm what will be deployed without making any Azure API calls.

**Example prompt (dry run):**

```
Do a dry run deployment of C:\adf_output\LoadFactSales to my ADF instance named my-adf in resource group rg-data-prod, subscription 00000000-0000-0000-0000-000000000000.
```

**Example prompt (live deployment):**

```
Deploy C:\adf_output\LoadFactSales to ADF instance my-adf in resource group rg-data-prod, subscription 00000000-0000-0000-0000-000000000000.
```

**Key parameters:**

| Parameter | Required | Description |
|---|---|---|
| `artifacts_dir` | Yes | Directory containing generated ADF JSON artifacts |
| `subscription_id` | Yes | Azure subscription GUID |
| `resource_group` | Yes | Azure resource group name |
| `factory_name` | Yes | Azure Data Factory instance name |
| `dry_run` | No | `true` to log only without calling Azure APIs (default: `false`) |

> **Triggers are always deployed in Stopped state.** Activate them manually in the ADF Studio after validating pipeline runs.

---

## Using the Built-in Prompt Files

Three reusable prompt files are included in `.vscode/` and can be invoked directly from Copilot Chat to run the full workflow with guided inputs.

| Prompt File | Mode | Description |
|---|---|---|
| `analyze_packages.prompt.md` | Agent | Scan a source, then analyze every package found and produce a prioritized conversion report |
| `convert_package.prompt.md` | Agent | Analyze, convert, and validate a single package; produces a Markdown summary with a manual-steps checklist |
| `deploy_adf.prompt.md` | Agent | Validate artifacts and deploy to ADF with optional dry-run |

**To invoke from Copilot Chat:**

1. Open Copilot Chat (`Ctrl+Alt+I`)
2. Switch to **Agent** mode
3. Type `/` and select the prompt file from the list, or type the prompt name
4. Fill in the prompted inputs (package path, output directory, Azure details, etc.)

---

## Authentication

The `deploy_to_adf` tool uses `DefaultAzureCredential`, which tries the following in order:

| Priority | Method | When to use |
|---|---|---|
| 1 | Environment variables | CI/CD pipelines (service principal) |
| 2 | Workload Identity | Azure-hosted compute (AKS, etc.) |
| 3 | Azure CLI (`az login`) | Local developer machines |
| 4 | Azure PowerShell | Local developer machines |

**For local development,** the simplest approach is:

```bash
az login
```

**For CI/CD pipelines,** set these environment variables for a service principal:

| Variable | Description |
|---|---|
| `AZURE_CLIENT_ID` | Service principal application (client) ID |
| `AZURE_CLIENT_SECRET` | Service principal secret |
| `AZURE_TENANT_ID` | Azure Active Directory tenant ID |

The service principal must have the **Data Factory Contributor** role on the target ADF instance.

---

## SSIS Component Mapping Reference

| SSIS Component | ADF Equivalent | Notes |
|---|---|---|
| Execute SQL Task | Stored Procedure / Script / Lookup Activity | Depends on `ResultSetType` and SQL pattern |
| Data Flow Task (simple) | Copy Activity | Single source → single destination, no transformations |
| Data Flow Task (complex) | Execute Data Flow Activity (Mapping Data Flow) | Multiple sources, transformations, or fanout |
| Execute Package Task | Execute Pipeline Activity | Child pipeline must also be converted |
| Script Task (C# / VB) | Azure Function Activity | Stub generated at `stubs/<FunctionName>/__init__.py`; requires manual porting |
| ForEach Loop Container | ForEach Activity | Expression varies by enumerator type |
| For Loop Container | SetVariable (init) + Until Activity + SetVariable (increment) | Condition logic is inverted |
| Sequence Container | Flattened into parent with `dependsOn` chaining | No ADF equivalent |
| File System Task | Copy Activity (Azure paths) or Web Activity → Azure Function | Local paths need Azure-path substitution |
| Execute Process Task | Web Activity → Azure Function | Manual: wrap process call in a Function |
| FTP Task | Copy Activity via FTP connector | Requires FTP linked service |
| Send Mail Task | Logic App / Web Activity | No native ADF equivalent |
| Event Handler (`OnError`) | Pipeline fails path / error handling | Converted to sub-pipeline reference |
| Event Handler (`OnPostExecute`) | Succeeded dependency path | Converted to sub-pipeline reference |

---

## Generated Artifact Structure

`convert_ssis_package` writes the following directory structure under `output_dir`:

```
<output_dir>/
  pipeline/
    <PackageName>.json          ← Main ADF pipeline
  linkedService/
    <ConnectionName>.json       ← One per SSIS connection manager
  dataset/
    <DatasetName>.json          ← One per data flow source / destination
  dataflow/
    <DataFlowName>.json         ← One per complex Data Flow Task
  trigger/
    <PackageName>_trigger.json  ← ScheduleTrigger template (Stopped state)
  stubs/
    <FunctionName>/
      __init__.py               ← Python stub with TODO blocks
      function.json             ← Azure Function binding definition
```

---

## Manual Steps After Conversion

After running `convert_ssis_package`, review the following checklist before deploying:

- [ ] **Connection string passwords** — packages with `EncryptAllWithPassword` protection level may have missing passwords in linked service JSON files. Fill them in or reference Azure Key Vault secrets.
- [ ] **Script Task stubs** — each stub in `stubs/<FunctionName>/__init__.py` contains `TODO` comments marking where the original C# / VB.NET logic must be ported to Python. Deploy the Function to Azure Functions before running the pipeline.
- [ ] **Local file paths** — File System Tasks that reference local paths (e.g. `C:\Data\input.csv`) have placeholder Azure Storage paths. Replace them with valid `abfss://` or `https://` URLs.
- [ ] **Trigger schedules** — the generated `ScheduleTrigger` uses a placeholder cron schedule. Update it to match your production schedule before activating.
- [ ] **Re-validate** — run `validate_adf_artifacts` again after all manual edits.
- [ ] **Activate triggers** — triggers are deployed in **Stopped** state. Activate them in ADF Studio only after a successful pipeline smoke-test.

---

## Development

Install development dependencies:

```bash
pip install -e ".[dev]"
```

**Run tests:**

```bash
pytest
```

**Lint:**

```bash
ruff check .
```

**Type-check:**

```bash
mypy ssis_adf_agent/
```

The project targets Python 3.11+, uses `ruff` with `line-length = 100`, and enforces `mypy --strict`.

---

## License

This project is licensed under the [MIT License](LICENSE).

```
MIT License

Copyright (c) 2026 chsimons_microsoft

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```
