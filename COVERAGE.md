# SSIS coverage matrix

**Reading guide.** This matrix lists every SSIS construct the agent can
encounter and tells you up front what to expect when you call
`convert_ssis_package` against it.

| Status | Meaning |
|---|---|
| ✅ **Supported** | Auto-converted to a deterministic ADF artifact. No manual work to wire the construct up; you may still need to fill linked-service credentials or review business rules. |
| 🟡 **Partial** | Auto-converted to an ADF artifact that is structurally correct but requires human review or follow-up (e.g. a generated stub, a Web Activity wrapper, a TODO marker, or an inverted expression). The parity validator surfaces these as warnings. |
| 🔴 **Unsupported** | Cannot be auto-converted. The analyzer flags it as `manual_required`. The package will still convert (other tasks succeed) but the unsupported task is either omitted or replaced with a placeholder activity that fails fast at runtime. |
| ⚪ **Not yet observed** | Recognised in principle but no real-world sample has been tested. Treat as Partial until proven otherwise. |

The implementation status below is sourced directly from
[`converters/dispatcher.py`](ssis_adf_agent/converters/dispatcher.py) (what is
wired) and [`analyzers/gap_analyzer.py`](ssis_adf_agent/analyzers/gap_analyzer.py)
(what is flagged as `_UNSUPPORTED_DF_COMPONENTS` / manual-required).

---

## Control Flow tasks

| SSIS task | Status | ADF target | Notes / sample |
|---|---|---|---|
| Execute SQL Task | ✅ | `Lookup` / `SqlServerStoredProcedure` / `Script` | Routing depends on `ResultSetType` and the SQL pattern. |
| Data Flow Task (1 source → 1 sink, no transforms) | ✅ | `Copy` activity | See B1 fix in [BACKLOG.md](BACKLOG.md) — source/sink types now derive from the actual SSIS component. |
| Data Flow Task (transforms / fanout) | ✅ | `ExecuteDataFlow` + a generated mapping data flow JSON | Per-component mapping is in the data-flow section below. |
| Execute Package Task | ✅ | `ExecutePipeline` | Child pipeline must be converted as a separate package. |
| Sequence Container | ✅ | Flattened — children promoted with `dependsOn` chaining | No ADF equivalent; intentional. |
| ForEach Loop Container (File / ADO / Item enumerator) | 🟡 | `ForEach` activity | Items expression is mapped from the enumerator type but the analyzer flags it for review. ([gap_analyzer.py L216-218](ssis_adf_agent/analyzers/gap_analyzer.py)) |
| ForEach Loop Container (SMO / NodeList / Variable enumerator) | 🟡 | `ForEach` activity | Items array often needs manual rewrite. |
| For Loop Container | 🟡 | `SetVariable` (init) + `Until` + `SetVariable` (increment) | The `EvalExpression` is **negated** to match `Until.doWhileCondition` semantics. Always review the converted condition. |
| Script Task — trivial (variable assignment only) | ✅ | `SetVariable` activity, no Azure Function | Auto-classified by `analyzers/script_classifier.py`. |
| Script Task — simple (ADF-expressible) | ✅ | `SetVariable` with a converted expression | |
| Script Task — moderate / complex | 🟡 | `AzureFunctionActivity` + Python stub under `stubs/<FunctionName>/__init__.py` | Original C# / VB embedded as comments (see H3). LLM translator opt-in via `AZURE_OPENAI_ENDPOINT`. |
| Script Task — encrypted (`EncryptAllWithPassword`) | 🔴 | — | Source code unrecoverable without the package password; stub generated with TODO + warning surfaced. |
| File System Task — Azure-path-rewritable | ✅ | `Copy` activity | When source/destination map to Azure Blob via the supplied path-mapping. |
| File System Task — local path / OS operation (SetAttributes, Rename) | 🟡 | `WebActivity` → generated Azure Function stub | Stub written to `stubs/`; user implements Az SDK call. |
| FTP Task | 🟡 | `Copy` activity over FTP linked service | Requires manual FTP linked-service credential fill. |
| Send Mail Task | 🟡 | `WebActivity` calling Logic App / Communication Services | No native ADF equivalent — caller must stand up the Logic App. |
| Execute Process Task | 🟡 | `WebActivity` → generated Azure Function stub | The original `.exe` invocation must be ported manually. |
| Bulk Insert Task | 🟡 | `Copy` activity (BCP-style) | Format-file behavior may need rewriting. |
| Web Service Task | 🟡 | `WebActivity` | Headers / SOAP envelope may require manual templating. |
| XML Task | 🟡 | Generated stub under `stubs/` | XSL transforms / XPath queries don't have native ADF equivalents. |
| Transfer SQL Server Objects Task | 🟡 | `Script` activity (DDL emit) | Schema-only objects only; data movement still needs a Copy. |
| Event Handler — `OnError` | 🟡 | Failure dependency path | Converted as a sub-graph wired through `dependsOn: [{condition: Failed}]`. Surfaced as a warning by the parity validator. |
| Event Handler — `OnPostExecute` | 🟡 | Success dependency path | Same mechanism, success branch. |
| Event Handler — other (`OnPreExecute`, `OnWarning`, etc.) | ⚪ | — | Not currently wired; analyzer flags. |
| CDC Control / Source / Splitter | 🔴 | — | No ADF native equivalent; analyzer flags as `manual_required`. Recommended path: redesign with ADF Change Data Capture or Synapse Link. |
| Master Data Services (MDS) Task | 🔴 | — | No ADF equivalent. |
| Analysis Services Processing / Execute DDL Task | 🔴 | — | Use Azure Analysis Services REST API directly. |
| Message Queue Task | 🔴 | — | No equivalent; redesign onto Service Bus / Event Hubs. |
| WMI Data Reader / WMI Event Watcher Task | 🔴 | — | Windows-only; no Azure equivalent. |
| Custom 3rd-party tasks (`Microsoft.SqlServer.Dts.Tasks.<Other>` not in the registered set) | 🔴 | — | Surfaced as `Unknown task type` (+10 complexity per occurrence). |

## Data Flow components

### Sources

| Component | Status | Notes |
|---|---|---|
| OLE DB Source | ✅ | Maps to `AzureSqlSource` / `SqlServerSource` / `OdbcSource` based on the linked service. `sqlReaderQuery` preserved (B1). |
| ADO.NET Source | ✅ | Same family as OLE DB. |
| Flat File Source | ✅ | `DelimitedTextSource`. |
| Excel Source | 🟡 | `ExcelSource`; sheet/range parameters need confirmation. |
| XML Source | 🟡 | Generated as a Mapping Data Flow source; XSD validation not preserved. |
| Raw File Source | 🔴 | SSIS-binary format; not portable. |
| OLE DB Source against Oracle / DB2 / SAP | 🟡 | Requires the appropriate ADF connector + SHIR; linked service is generated as a placeholder. |

### Transformations

| Component | Status | Notes |
|---|---|---|
| Derived Column | ✅ | Mapping Data Flow `DerivedColumn` transformation. |
| Conditional Split | ✅ | Mapping Data Flow `ConditionalSplit`. |
| Lookup (cached / no-cache) | ✅ | Mapping Data Flow `Lookup`. Cache mode flagged for review. |
| Aggregate | 🟡 | Maps; verify grouping columns. |
| Sort | 🟡 | ADF Sort is memory-bound; confirm data volume. |
| Merge / Merge Join | 🟡 | Inputs must be sorted in ADF; Sort transforms may need to be added. |
| Union All | ✅ | `Union`. |
| Multicast | ✅ | `NewBranch`. |
| Pivot / Unpivot | ✅ | `Pivot` / `Unpivot`. |
| Row Count | ✅ | Mapping Data Flow surrogate via aggregate. |
| Percentage / Row Sampling | 🟡 | `Sampling` transformation; tunables differ. |
| Cache Transform | 🔴 | Listed in `_UNSUPPORTED_DF_COMPONENTS`. |
| Fuzzy Lookup / Fuzzy Grouping | 🔴 | Listed in `_UNSUPPORTED_DF_COMPONENTS`. No ADF equivalent — port to Cognitive Search / Synapse ML. |
| Term Extraction / Term Lookup | 🔴 | Listed in `_UNSUPPORTED_DF_COMPONENTS`. |
| Import Column / Export Column | 🔴 | Listed in `_UNSUPPORTED_DF_COMPONENTS`. |
| Slowly Changing Dimension (SCD) | 🟡 | Generated as a sub-flow with TODO; ADF SCD pattern documented but not auto-wired. |
| OLE DB Command | 🟡 | `AlterRow` transformation if pattern matches; otherwise per-row stored proc call (slow). |

### Destinations

| Component | Status | Notes |
|---|---|---|
| OLE DB / ADO.NET / SQL Server Destination | ✅ | `AzureSqlSink` / `SqlServerSink` / `OdbcSink` (B1). |
| Flat File Destination | ✅ | `DelimitedTextSink` with `storeSettings` + `formatSettings` (B1). |
| Excel Destination | 🟡 | `ExcelSink`. |
| Raw File Destination | 🔴 | Not portable. |
| Recordset Destination | 🔴 | Listed in `_UNSUPPORTED_DF_COMPONENTS`. Use a pipeline variable + Lookup. |
| SQL Server Destination (BCP) | 🔴 | Listed in `_UNSUPPORTED_DF_COMPONENTS`. Replaced with Copy Activity. |
| Script Component (any role) | 🔴 | Listed in `_UNSUPPORTED_DF_COMPONENTS`. Port to Azure Function or notebook. |

## Connection managers / linked services

| Connection manager | Status | ADF linked service |
|---|---|---|
| OLE DB / ADO.NET → Azure SQL / SQL Server / Synapse | ✅ | `AzureSqlDatabase` / `SqlServer` / `AzureSqlDW`. |
| OLE DB → Oracle / DB2 / SAP | 🟡 | `Oracle` / `Db2` / `SapHana` — connector reference emitted as placeholder; needs SHIR + credentials. |
| Flat File / Multiple Flat Files | ✅ | `AzureBlobStorage` + `DelimitedText` dataset (path mapping required). |
| Excel | 🟡 | `AzureBlobStorage` + `Excel` dataset. |
| FTP / SFTP | 🟡 | `Ftp` / `Sftp`. |
| HTTP | 🟡 | `HttpServer`. |
| File | 🟡 | `AzureFileStorage` (path mapping required). |
| SMTP | 🔴 | No ADF linked service; redirected through Logic App / ACS. |
| MSMQ | 🔴 | No ADF linked service. |
| WMI | 🔴 | Windows-only. |
| Cache (in-memory) | 🔴 | No ADF equivalent. |

## Package-level constructs

| Construct | Status | Notes |
|---|---|---|
| Package parameters | ✅ | Become pipeline parameters. Sensitive values stripped (B3). |
| Project parameters | ✅ | Same; sensitive values stripped (B3). |
| Package variables (User namespace) | ✅ | Become pipeline variables. Sensitive values stripped (B3). |
| System variables (`System::*`) | 🟡 | Read-only; mapped to ADF system variables where possible (`@pipeline().PipelineName`, `@utcnow()`). |
| Parent-package variables | 🟡 | Surfaced via `ExecutePipeline` arguments. |
| Package configurations — XML / SQL Server / Environment Variable | 🟡 | Configurations are read at parse time but the values are *not* re-emitted. Use ADF parameters / Key Vault references instead. |
| Project parameters via `.params` | ✅ | Read by parser; merged with package parameters. |
| `.ispac` deployment artifact | ⚪ | Parser accepts an extracted folder; binary `.ispac` extraction not yet wired. |
| Package parts (shared `.dtsxp` fragments) | ⚪ | Resolved if expanded by SSDT; not auto-resolved by the agent. |
| `EncryptAllWithPassword` / `EncryptAllWithUserKey` | 🟡 | Parser warns; sensitive properties (passwords, secrets) will be missing — caller must supply via Key Vault. |
| Windows / Kerberos / cert auth on linked services | 🟡 | Linked service emitted with `authenticationType` set; SHIR required at runtime. |
| Logging providers (text file / SQL Server) | ⚪ | Not auto-converted; ADF native diagnostic logging documented as the replacement. |
| Checkpoints (`CheckpointFileName`, `SaveCheckpoints`) | 🔴 | No ADF equivalent. |
| Transactions (`TransactionOption=Required`) | 🔴 | ADF activities are atomic per-activity; cross-activity transactions require redesign. |

## How to verify status for a given package

1. Run `analyze_ssis_package` (or `bulk_analyze` over a folder). The
   `gap_analysis.manual_required` array enumerates everything the analyzer
   flagged as 🔴 or 🟡 in this matrix for that specific package.
2. Run `convert_ssis_package` and read the `unresolved_objects` and
   `conversion_warnings` arrays in the response.
3. Run `validate_conversion_parity` ([PARITY.md](PARITY.md)) to confirm the
   structural conversion held together and to surface placeholder linked
   services / pending Script Task ports as warnings before deploy.
