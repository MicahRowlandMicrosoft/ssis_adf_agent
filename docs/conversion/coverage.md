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
[`converters/dispatcher.py`](ssis_modernization_agent/converters/dispatcher.py) (what is
wired) and [`analyzers/gap_analyzer.py`](ssis_modernization_agent/analyzers/gap_analyzer.py)
(what is flagged as `_UNSUPPORTED_DF_COMPONENTS` / manual-required).

---

## Control Flow tasks

> Evidence column links to a unit-test fixture (or generator source) that
> exercises the conversion end-to-end. 🔴 / ⚪ rows have no evidence link
> by design — the analyzer flags them as `manual_required`.

| SSIS task | Status | ADF target | Notes | Evidence |
|---|---|---|---|---|
| Execute SQL Task | ✅ | `Lookup` / `SqlServerStoredProcedure` / `Script` | Routing depends on `ResultSetType` and the SQL pattern. | [test_execute_sql_params.py](tests/test_execute_sql_params.py) |
| Data Flow Task (1 source → 1 sink, no transforms) | ✅ | `Copy` activity | See B1 fix in [backlog.md](../development/backlog.md) — source/sink types now derive from the actual SSIS component. | [test_realworld_fixes.py](tests/test_realworld_fixes.py) |
| Data Flow Task (transforms / fanout) | ✅ | `ExecuteDataFlow` + a generated mapping data flow JSON | Per-component mapping is in the data-flow section below. | [test_data_flow_transforms.py](tests/test_data_flow_transforms.py) |
| Execute Package Task | ✅ | `ExecutePipeline` | Child pipeline must be converted as a separate package. | [execute_package_converter.py](ssis_modernization_agent/converters/control_flow/execute_package_converter.py) |
| Sequence Container | ✅ | Flattened — children promoted with `dependsOn` chaining | No ADF equivalent; intentional. | [test_constraint_resolution.py](tests/test_constraint_resolution.py) |
| ForEach Loop Container (File / ADO / Item enumerator) | 🟡 | `ForEach` activity | Items expression is mapped from the enumerator type but the analyzer flags it for review. ([gap_analyzer.py L216-218](ssis_modernization_agent/analyzers/gap_analyzer.py)) | [test_foreach_prereq.py](tests/test_foreach_prereq.py) |
| ForEach Loop Container (SMO / NodeList / Variable enumerator) | 🟡 | `ForEach` activity | Items array often needs manual rewrite. | [foreach_converter.py](ssis_modernization_agent/converters/control_flow/foreach_converter.py) |
| For Loop Container | 🟡 | `SetVariable` (init) + `Until` + `SetVariable` (increment) | The `EvalExpression` is **negated** to match `Until.doWhileCondition` semantics. Always review the converted condition. | [for_loop_converter.py](ssis_modernization_agent/converters/control_flow/for_loop_converter.py) |
| Script Task — trivial (variable assignment only) | ✅ | `SetVariable` activity, no Azure Function | Auto-classified by `analyzers/script_classifier.py`. | [test_script_classifier.py](tests/test_script_classifier.py) |
| Script Task — simple (ADF-expressible) | ✅ | `SetVariable` with a converted expression | | [test_script_classifier.py](tests/test_script_classifier.py) |
| Script Task — moderate / complex | 🟡 | `AzureFunctionActivity` + Python stub under `stubs/<FunctionName>/__init__.py` | Original C# / VB embedded as comments (see H3). LLM translator opt-in via `AZURE_OPENAI_ENDPOINT`. **Worked port:** [Database_Access_Configuration (LNI)](docs/case-studies/script_task_port_database_access_configuration/README.md) — full VB → Python port with hours captured. | [test_script_task_port_database_access_configuration.py](tests/test_script_task_port_database_access_configuration.py) |
| Script Task — encrypted (`EncryptAllWithPassword`) | 🔴 | — | Source code unrecoverable without the package password; stub generated with TODO + warning surfaced. | — |
| File System Task — Azure-path-rewritable | ✅ | `Copy` activity | When source/destination map to Azure Blob via the supplied path-mapping. | [file_system_converter.py](ssis_modernization_agent/converters/control_flow/file_system_converter.py) |
| File System Task — local path / OS operation (SetAttributes, Rename) | 🟡 | `WebActivity` → generated Azure Function stub | Stub written to `stubs/`; user implements Az SDK call. | [file_system_converter.py](ssis_modernization_agent/converters/control_flow/file_system_converter.py) |
| FTP Task | 🟡 | `Copy` activity over FTP linked service | Requires manual FTP linked-service credential fill. | [linked_service_generator.py](ssis_modernization_agent/generators/linked_service_generator.py) |
| Send Mail Task | 🟡 | `WebActivity` calling Logic App / Communication Services | No native ADF equivalent — caller must stand up the Logic App. | [dispatcher.py](ssis_modernization_agent/converters/dispatcher.py) |
| Execute Process Task | 🟡 | `WebActivity` → generated Azure Function stub | The original `.exe` invocation must be ported manually. | [dispatcher.py](ssis_modernization_agent/converters/dispatcher.py) |
| Bulk Insert Task | 🟡 | `Copy` activity (BCP-style) | Format-file behavior may need rewriting. | [dispatcher.py](ssis_modernization_agent/converters/dispatcher.py) |
| Web Service Task | 🟡 | `WebActivity` | Headers / SOAP envelope may require manual templating. | [dispatcher.py](ssis_modernization_agent/converters/dispatcher.py) |
| XML Task | 🟡 | Generated stub under `stubs/` | XSL transforms / XPath queries don't have native ADF equivalents. | [dispatcher.py](ssis_modernization_agent/converters/dispatcher.py) |
| Transfer SQL Server Objects Task | 🟡 | `Script` activity (DDL emit) | Schema-only objects only; data movement still needs a Copy. | [dispatcher.py](ssis_modernization_agent/converters/dispatcher.py) |
| Event Handler — `OnError` | 🟡 | Failure dependency path | Converted as a sub-graph wired through `dependsOn: [{condition: Failed}]`. Surfaced as a warning by the parity validator. | [event_handler_converter.py](ssis_modernization_agent/converters/control_flow/event_handler_converter.py) |
| Event Handler — `OnPostExecute` | 🟡 | Success dependency path | Same mechanism, success branch. | [event_handler_converter.py](ssis_modernization_agent/converters/control_flow/event_handler_converter.py) |
| Event Handler — other (`OnPreExecute`, `OnWarning`, etc.) | ⚪ | — | Not currently wired; analyzer flags. | — |
| CDC Control / Source / Splitter | 🔴 | — | No ADF native equivalent; analyzer flags as `manual_required`. Recommended path: redesign with ADF Change Data Capture or Synapse Link. | — |
| Master Data Services (MDS) Task | 🔴 | — | No ADF equivalent. | — |
| Analysis Services Processing / Execute DDL Task | 🔴 | — | Use Azure Analysis Services REST API directly. | — |
| Message Queue Task | 🔴 | — | No equivalent; redesign onto Service Bus / Event Hubs. | — |
| WMI Data Reader / WMI Event Watcher Task | 🔴 | — | Windows-only; no Azure equivalent. | — |
| Custom 3rd-party tasks (`Microsoft.SqlServer.Dts.Tasks.<Other>` not in the registered set) | 🔴 | — | Surfaced as `Unknown task type` (+10 complexity per occurrence). | — |

## Data Flow components

### Sources

| Component | Status | Notes | Evidence |
|---|---|---|---|
| OLE DB Source | ✅ | Maps to `AzureSqlSource` / `SqlServerSource` / `OdbcSource` based on the linked service. `sqlReaderQuery` preserved (B1). | [source_converter.py](ssis_modernization_agent/converters/data_flow/source_converter.py) |
| ADO.NET Source | ✅ | Same family as OLE DB. | [source_converter.py](ssis_modernization_agent/converters/data_flow/source_converter.py) |
| Flat File Source | ✅ | `DelimitedTextSource`. | [source_converter.py](ssis_modernization_agent/converters/data_flow/source_converter.py) |
| Excel Source | 🟡 | `ExcelSource`; sheet/range parameters need confirmation. | [source_converter.py](ssis_modernization_agent/converters/data_flow/source_converter.py) |
| XML Source | 🟡 | Generated as a Mapping Data Flow source; XSD validation not preserved. | [source_converter.py](ssis_modernization_agent/converters/data_flow/source_converter.py) |
| Raw File Source | 🔴 | SSIS-binary format; not portable. | — |
| OLE DB Source against Oracle / DB2 / SAP | 🟡 | Requires the appropriate ADF connector + SHIR; linked service is generated as a placeholder. | [linked_service_generator.py](ssis_modernization_agent/generators/linked_service_generator.py) |

### Transformations

| Component | Status | Notes | Evidence |
|---|---|---|---|
| Derived Column | ✅ | Mapping Data Flow `DerivedColumn` transformation. | [test_data_flow_transforms.py](tests/test_data_flow_transforms.py) |
| Conditional Split | ✅ | Mapping Data Flow `ConditionalSplit`. | [test_data_flow_transforms.py](tests/test_data_flow_transforms.py) |
| Lookup (cached / no-cache) | ✅ | Mapping Data Flow `Lookup`. Cache mode flagged for review. | [test_data_flow_transforms.py](tests/test_data_flow_transforms.py) |
| Aggregate | 🟡 | Maps; verify grouping columns. | [transformation_converter.py](ssis_modernization_agent/converters/data_flow/transformation_converter.py) |
| Sort | 🟡 | ADF Sort is memory-bound; confirm data volume. | [transformation_converter.py](ssis_modernization_agent/converters/data_flow/transformation_converter.py) |
| Merge / Merge Join | 🟡 | Inputs must be sorted in ADF; Sort transforms may need to be added. | [transformation_converter.py](ssis_modernization_agent/converters/data_flow/transformation_converter.py) |
| Union All | ✅ | `Union`. | [test_data_flow_transforms.py](tests/test_data_flow_transforms.py) |
| Multicast | ✅ | `NewBranch`. | [test_data_flow_transforms.py](tests/test_data_flow_transforms.py) |
| Pivot / Unpivot | ✅ | `Pivot` / `Unpivot`. | [transformation_converter.py](ssis_modernization_agent/converters/data_flow/transformation_converter.py) |
| Row Count | ✅ | Mapping Data Flow surrogate via aggregate. | [transformation_converter.py](ssis_modernization_agent/converters/data_flow/transformation_converter.py) |
| Percentage / Row Sampling | 🟡 | `Sampling` transformation; tunables differ. | [transformation_converter.py](ssis_modernization_agent/converters/data_flow/transformation_converter.py) |
| Cache Transform | 🔴 | Listed in `_UNSUPPORTED_DF_COMPONENTS`. | — |
| Fuzzy Lookup / Fuzzy Grouping | 🔴 | Listed in `_UNSUPPORTED_DF_COMPONENTS`. No ADF equivalent — port to Cognitive Search / Synapse ML. | — |
| Term Extraction / Term Lookup | 🔴 | Listed in `_UNSUPPORTED_DF_COMPONENTS`. | — |
| Import Column / Export Column | 🔴 | Listed in `_UNSUPPORTED_DF_COMPONENTS`. | — |
| Slowly Changing Dimension (SCD) | 🟡 | Generated as a sub-flow with TODO; ADF SCD pattern documented but not auto-wired. | [transformation_converter.py](ssis_modernization_agent/converters/data_flow/transformation_converter.py) |
| OLE DB Command | 🟡 | `AlterRow` transformation if pattern matches; otherwise per-row stored proc call (slow). | [transformation_converter.py](ssis_modernization_agent/converters/data_flow/transformation_converter.py) |

### Destinations

| Component | Status | Notes | Evidence |
|---|---|---|---|
| OLE DB / ADO.NET / SQL Server Destination | ✅ | `AzureSqlSink` / `SqlServerSink` / `OdbcSink` (B1). | [destination_converter.py](ssis_modernization_agent/converters/data_flow/destination_converter.py) |
| Flat File Destination | ✅ | `DelimitedTextSink` with `storeSettings` + `formatSettings` (B1). | [destination_converter.py](ssis_modernization_agent/converters/data_flow/destination_converter.py) |
| Excel Destination | 🟡 | `ExcelSink`. | [destination_converter.py](ssis_modernization_agent/converters/data_flow/destination_converter.py) |
| Raw File Destination | 🔴 | Not portable. | — |
| Recordset Destination | 🔴 | Listed in `_UNSUPPORTED_DF_COMPONENTS`. Use a pipeline variable + Lookup. | — |
| SQL Server Destination (BCP) | 🔴 | Listed in `_UNSUPPORTED_DF_COMPONENTS`. Replaced with Copy Activity. | — |
| Script Component (any role) | 🔴 | Listed in `_UNSUPPORTED_DF_COMPONENTS`. Port to Azure Function or notebook. | — |

## Connection managers / linked services

| Connection manager | Status | ADF linked service | Evidence |
|---|---|---|---|
| OLE DB / ADO.NET → Azure SQL / SQL Server / Synapse | ✅ | `AzureSqlDatabase` / `SqlServer` / `AzureSqlDW`. | [test_linked_service_generation.py](tests/test_linked_service_generation.py) |
| OLE DB → Oracle / DB2 / SAP | 🟡 | `Oracle` / `Db2` / `SapHana` — connector reference emitted as placeholder; needs SHIR + credentials. | [linked_service_generator.py](ssis_modernization_agent/generators/linked_service_generator.py) |
| Flat File / Multiple Flat Files | ✅ | `AzureBlobStorage` + `DelimitedText` dataset (path mapping required). | [test_linked_service_generation.py](tests/test_linked_service_generation.py) |
| Excel | 🟡 | `AzureBlobStorage` + `Excel` dataset. | [linked_service_generator.py](ssis_modernization_agent/generators/linked_service_generator.py) |
| FTP / SFTP | 🟡 | `Ftp` / `Sftp`. | [linked_service_generator.py](ssis_modernization_agent/generators/linked_service_generator.py) |
| HTTP | 🟡 | `HttpServer`. | [linked_service_generator.py](ssis_modernization_agent/generators/linked_service_generator.py) |
| File | 🟡 | `AzureFileStorage` (path mapping required). | [linked_service_generator.py](ssis_modernization_agent/generators/linked_service_generator.py) |
| SMTP | 🔴 | No ADF linked service; redirected through Logic App / ACS. | — |
| MSMQ | 🔴 | No ADF linked service. | — |
| WMI | 🔴 | Windows-only. | — |
| Cache (in-memory) | 🔴 | No ADF equivalent. | — |

## Package-level constructs

| Construct | Status | Notes | Evidence |
|---|---|---|---|
| Package parameters | ✅ | Become pipeline parameters. Sensitive values stripped (B3). | [test_pipeline_sensitive_redaction.py](tests/test_pipeline_sensitive_redaction.py) |
| Project parameters | ✅ | Same; sensitive values stripped (B3). | [test_proposer_project_params.py](tests/test_proposer_project_params.py) |
| Package variables (User namespace) | ✅ | Become pipeline variables. Sensitive values stripped (B3). | [test_pipeline_sensitive_redaction.py](tests/test_pipeline_sensitive_redaction.py) |
| System variables (`System::*`) | 🟡 | Read-only; mapped to ADF system variables where possible (`@pipeline().PipelineName`, `@utcnow()`). | [test_expression_functions.py](tests/test_expression_functions.py) |
| Parent-package variables | 🟡 | Surfaced via `ExecutePipeline` arguments. | [execute_package_converter.py](ssis_modernization_agent/converters/control_flow/execute_package_converter.py) |
| Package configurations — XML / SQL Server / Environment Variable | 🟡 | Configurations are read at parse time but the values are *not* re-emitted. Use ADF parameters / Key Vault references instead. | [ssis_parser.py](ssis_modernization_agent/parsers/ssis_parser.py) |
| Project parameters via `.params` | ✅ | Read by parser; merged with package parameters. |
| `.ispac` deployment artifact | ⚪ | Parser accepts an extracted folder; binary `.ispac` extraction not yet wired. |
| Package parts (shared `.dtsxp` fragments) | ⚪ | Resolved if expanded by SSDT; not auto-resolved by the agent. |
| `EncryptAllWithPassword` / `EncryptAllWithUserKey` | 🟡 | Parser warns; sensitive properties (passwords, secrets) will be missing — caller must supply via Key Vault. |
| Windows / Kerberos / cert auth on linked services | 🟡 | Linked service emitted with `authenticationType` set; SHIR required at runtime. |
| Logging providers (text file / SQL Server) | ⚪ | Not auto-converted; ADF native diagnostic logging documented as the replacement. |
| Checkpoints (`CheckpointFileName`, `SaveCheckpoints`) | 🔴 | No ADF equivalent. |
| Transactions (`TransactionOption=Required`) | 🔴 | ADF activities are atomic per-activity; cross-activity transactions require redesign. |

## SSIS expression functions: mapped vs. unmapped

The control-flow expression translator
([`translators/control_flow_expression.py`](ssis_modernization_agent/translators/control_flow_expression.py)
`_FUNC_MAP`) is the source of truth — this table is derived from it.
✅ = direct ADF function. 🟡 = translated but emits a `/* TODO: ... */`
comment because no exact ADF equivalent exists or a manual review is
required (e.g. month diff, REVERSE, ISNUMERIC). 🔴 = not in the map;
the expression is passed through unchanged and will fail at deploy
time unless hand-edited.

| Category | SSIS function | Status | ADF equivalent / note |
|---|---|---|---|
| Date/time | `GETDATE()` / `GETUTCDATE()` | ✅ | `utcNow()` |
| Date/time | `DATEADD(part, n, dt)` | ✅ | `addDays` / `addMonths` / `addHours` / `addMinutes` / `addSeconds`; **year** uses `addToTime` placeholder. |
| Date/time | `DATEDIFF(part, a, b)` | 🟡 | `dayOfYear` for day; month / hour / minute / second / year all emit a TODO marker. |
| Date/time | `DATEPART(part, dt)`, `YEAR()`, `MONTH()`, `DAY()` | 🟡 | `dayOfMonth` / `dayOfWeek` / `dayOfYear` directly; month / year / hour / minute emit a `formatDateTime(...)` TODO. |
| String | `LEN`, `UPPER`, `LOWER`, `TRIM`, `LTRIM`, `RTRIM`, `REPLACE` | ✅ | `length`, `toUpper`, `toLower`, `trim`, `replace`. |
| String | `SUBSTRING`, `LEFT`, `RIGHT` | ✅ | Special-cased — 1-based SSIS indices rewritten to 0-based ADF indices. |
| String | `FINDSTRING`, `CHARINDEX` | ✅ | Special-cased — 1-based result rewritten to 0-based. |
| String | `PATINDEX` | 🟡 | Special-cased; LIKE-pattern handling is a partial port. |
| String | `REVERSE` | 🟡 | Emits `/* TODO: REVERSE — no direct ADF equivalent */`. |
| Null handling | `ISNULL(x)`, `REPLACENULL(x, y)` | ✅ | `empty(x)`, `coalesce(x, y)`. |
| Type checking | `ISNUMERIC` | 🟡 | Emits `/* TODO: ISNUMERIC — validate manually */`. |
| Math | `ABS`, `CEILING`, `FLOOR`, `ROUND`, `POWER`, `SQRT`, `SIGN` | ✅ | Direct ADF equivalents. |
| Type cast | `(DT_STR)`, `(DT_WSTR)`, `(DT_I4)`, `(DT_I8)`, `(DT_BOOL)`, `(DT_DECIMAL)` | ✅ | `string`, `int`, `bool`, `decimal`. |
| Type cast | `(DT_DBTIMESTAMP)`, `(DT_DATE)`, `(DT_DBDATE)`, other DT_ casts | 🟡 / 🔴 | `(DT_DBTIMESTAMP)` emits a TODO; uncatalogued DT_ casts pass through and fail at deploy. |

Functions **not** in `_FUNC_MAP` — examples include `HEX()`,
`SQUARE()`, `SLN()`, `TOKEN()`, `TOKENCOUNT()`, MAC-encoding casts,
and any vendor-specific function — fall through unchanged. They will
deploy successfully if ADF happens to recognise the name (rare) and
fail at run time otherwise. **Recommended response:** rewrite the
expression as an ADF native expression in the converted pipeline, or
move the logic into a Mapping Data Flow derived column.

The data-flow expression translator
([`translators/ssis_expression_translator.py`](ssis_modernization_agent/translators/ssis_expression_translator.py))
shares most of the catalogue above with one shape difference: ADF
Mapping Data Flow uses `currentTimestamp()` rather than `utcNow()`,
and `DATEPART` is approximated with `dayOfMonth` (no part argument);
where the part is not "day", the converter will still emit the call
but the operator should review the output for parity.

## How to verify status for a given package

1. Run `analyze_ssis_package` (or `bulk_analyze` over a folder). The
   `gap_analysis.manual_required` array enumerates everything the analyzer
   flagged as 🔴 or 🟡 in this matrix for that specific package.
2. Run `convert_ssis_package` and read the `unresolved_objects` and
   `conversion_warnings` arrays in the response.
3. Run `validate_conversion_parity` ([parity.md](parity.md)) to confirm the
   structural conversion held together and to surface placeholder linked
   services / pending Script Task ports as warnings before deploy.
