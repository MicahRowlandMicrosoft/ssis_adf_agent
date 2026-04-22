# SSIS → ADF Agent — Prioritized Backlog

Derived from the skeptical-buyer evaluation (April 2026) against the LNI ADDS sample
([test-lni-packages/](../test-lni-packages/)) and the README/SETUP/HOWTO docs.

Priority legend:
- **P0** — Blocker. Tool cannot be credibly demoed until fixed.
- **P1** — High. Must land before a Conditional-Go can become a Go.
- **P2** — Medium. Required for enterprise adoption but not for first-customer pilot.
- **P3** — Nice to have / polish.

---

## P0 — Blockers

### B1. Copy Activity emits wrong source/sink types for OLEDB → FlatFile — **DONE**
- **Fix:** `_COPY_SOURCE_BY_COMPONENT` / `_COPY_SINK_BY_COMPONENT` in `data_flow_converter.py`; SQL-only sink properties gated by `_SQL_SINK_TYPES`; `sqlReaderQuery` carried through.
- **Verified:** Regenerated [PL_ADDS_MIPS_TC.json](../test-lni-packages/adf/ADDS-MIPS-TC/pipeline/PL_ADDS_MIPS_TC.json) now emits `AzureSqlSource` + `DelimitedTextSink` matching the wired datasets.

### B2. End-to-end deploy + smoke-test on the LNI sample — **CUSTOMER-SIDE PROOF**
- **Status:** Cannot be executed in this environment (no Azure subscription / factory / SHIR available). Listed as a customer-side acceptance test.
- **Acceptance unchanged:** captured deploy + smoke-test logs against a real factory for all three LNI packages.

### B3. Credentials / on-prem identifiers in cleartext pipeline variables — **DONE**
- **Fix:** `_redact_sensitive_default()` in `pipeline_generator.py` strips defaultValues whose name matches a credential keyword OR whose value matches a Windows-domain account / on-prem FQDN regex. Stripped entries get a `description` instructing the deployer to inject via Key Vault reference, pipeline parameter, or env-specific override. Azure cloud hostnames (`*.windows.net` etc.) are intentionally not flagged.
- **Verified:** [PL_ADDS_MIPS_TC.json](../test-lni-packages/adf/ADDS-MIPS-TC/pipeline/PL_ADDS_MIPS_TC.json) — `DBUserID`, `DatabaseServer`, and the LNI service-account values no longer appear as cleartext defaults; replaced by `[SENSITIVE]` description blocks. Covered by [test_pipeline_sensitive_redaction.py](../tests/test_pipeline_sensitive_redaction.py).

---

## P1 — High

### H1. Doc/reality mismatch on tool count
- **Evidence:** README says **22 tools**; SETUP step 4 says "**five tools**"; copilot-instructions says 5–7.
- **Acceptance:** Single source of truth across README/SETUP/HOWTO and `.github/copilot-instructions.md`.

### H2. Repository ownership + version + changelog
- **Evidence:** README clone URL is `github.com/chsimons_microsoft/ssis_adf_agent.git`; no CHANGELOG, no version pin, no security contact.
- **Acceptance:** Verified repo URL, `CHANGELOG.md`, semver in `pyproject.toml`, SECURITY.md, ownership statement in README.

### H3. LLM Script Task translation silently no-ops on real input — **DONE**
- **Root cause:** the parser only recognised the SSIS 2008 / classic 2012 wrapper elements (`ScriptTaskProjectConfiguration` / `ScriptTask`) and only the `BinaryData` / `ProjectBytes` source patterns. Modern SSIS 2017+ packages (including the LNI estate) put a bare `<ScriptProject>` directly under `<ObjectData>` and embed the source as `<ProjectItem>` CDATA. Nothing matched, so the parser returned `script_language="CSharp"` (default) and `source_code=None`, and the LLM translator silently no-op'd.
- **Fix:** `_parse_script_task` in `ssis_parser.py` now recognises a bare `<ScriptProject>` as the config holder, reads its `Language` attribute (CSharp / VisualBasic), and `_extract_source_from_inline_project_items` concatenates `ScriptMain.{vb,cs}` and any other code-shaped `ProjectItem` CDATA. XML / project-metadata items are skipped. The LLM-skip warning in `script_task_converter.py` was rewritten to drop the "self-closing stub format" line and instead list real causes (unsupported VSTA layout, EncryptAllWithPassword, pre-2008 binary stub).
- **Verified:** [adf/ADDS-MIPS-TC/stubs/Database_Access_Configuration/__init__.py](../test-lni-packages/adf/ADDS-MIPS-TC/stubs/Database_Access_Configuration/__init__.py) regenerated — now reports `Original language: VisualBasic` and embeds the original `ScriptMain.vb` source as line comments. Covered by [test_script_task_inline_project_items.py](../tests/test_script_task_inline_project_items.py) (6 tests, including a smoke test against the real LNI sample).

### H4. Parity validation — define and demonstrate — **DONE**
- **Doc:** [PARITY.md](PARITY.md) — table-form definition of every check (`task coverage`, `linked services`, `parameters`, `data flows`, `event handlers`, `script tasks`, `SDK dry-run`, `factory reachability`), explicit list of what is *not* compared (row-level, performance, transform correctness), output schema, and a reproduction recipe.
- **Worked example:** [PARITY_REPORT_ADDS_MIPS_TC.md](../test-lni-packages/PARITY_REPORT_ADDS_MIPS_TC.md) + [PARITY_REPORT_ADDS_MIPS_TC.json](../test-lni-packages/PARITY_REPORT_ADDS_MIPS_TC.json) captured by running `validate_conversion_parity` against the LNI sample. Catches the two linked-service placeholder warnings and the two pending Script Task ports — exactly the kind of issues a buyer asked to see surfaced *before* deploy.
- **Defect-catching example:** PARITY.md "Catching a known defect" section explains how the SDK dry-run catches B1-class regressions (Copy sink/source type ≠ dataset type) before deploy.

### H5. Mark HOWTO transcripts as illustrative, not captured runs
- **Acceptance:** Each scripted dialogue in [HOWTO.md](HOWTO.md) prefaced as "*Example dialogue — output is illustrative, not a captured run.*" Real captured transcripts (with timings) added for at least one estate.

### H6. SSIS supported / partial / unsupported matrix
- **Acceptance:** New doc section listing each pattern (CDC, MDS, Fuzzy Lookup, Script Component, custom 3rd-party components, OLE DB→Oracle/DB2/SAP, package configurations XML/SQL/env-var, project params, package parts, parent-package variables, `EncryptAllWithPassword`, `.ispac`, Windows/Kerberos/cert auth) with status + sample link.

### H7. Bulk trigger activation
- **Acceptance:** New tool / documented script that activates all triggers in a factory or wave. Cited in HOWTO post-deploy section.

### H8. Non-destructive re-deploy mode
- **Evidence:** README states deploy is `put_or_update` (overwrites manual edits).
- **Acceptance:** Documented "preserve manual edits" mode (skip-if-exists, or generated-region marker), demonstrated on a hand-edited linked service.

---

## P2 — Medium

### M1. Lineage manifest
- **Acceptance:** Each conversion emits a manifest mapping `package_name → artifact_paths → deployed_resource_ids`. Surfaced in PDF.

### M2. ARM / azd export of ADF *content* (not just infra)
- **Acceptance:** Optional output that wraps generated ADF JSON for an ARM / Synapse-publish CI/CD model.

### M3. Headless CI recipe
- **Acceptance:** Documented `python -m ssis_adf_agent <command>` (or CLI) recipe per tool, with an Azure DevOps / GitHub Actions sample.

### M4. Cost estimate calibration
- **Acceptance:** One actual-vs-estimated cost comparison for a deployed pipeline running ≥30 days. Variance disclosed in cost report.

### M5. Effort range tightening / methodology disclosure
- **Evidence:** Per-package ranges like 36.8h–84.0h (2.3× spread) in [PreDeployment_Report.md](../test-lni-packages/PreDeployment_Report.md).
- **Acceptance:** Range methodology documented; calibration data from real migrations narrows the spread or explains drivers.

### M6. EncryptAllWithPassword end-to-end recipe
- **Acceptance:** Documented flow: extract → KV upload → linked service rewrite, with a working sample.

### M7. Custom-component / 3rd-party substitution registry
- **Acceptance:** Mechanism to declare "we use Cozyroc/KingswaySoft component X → use ADF activity Y" with a worked example.

### M8. Estate-scale evidence (≥100 packages)
- **Acceptance:** Captured `bulk_analyze` + `convert_estate` run on a public ≥100-package corpus; runtime, memory, dedup quality reported.

---

## P3 — Polish

### N1. Cross-pipeline regression harness
- **Acceptance:** Smoke-test multiple pipelines in a wave with a single call; aggregate report.

### N2. Rollback story
- **Acceptance:** Documented rollback flow (delete / soft-revert / branch-restore).

### N3. Naming-convention configurability
- **Acceptance:** Customer-tunable prefix/suffix patterns for generated artifacts.

---

## Suggested execution order

1. **B1** ✅ done (regenerated Copy types correctly on LNI).
2. **B3** ✅ done (sensitive defaults stripped from generated pipelines).
3. **B2** — customer-side proof; cannot be executed without an Azure factory.
4. **H3** (LLM translator on LNI dialect) — same sample, same regen pass.
5. **H4** (parity report) — produce on the deployed pipeline.
6. **H1**, **H5**, **H2** (doc/repo hygiene) — fast wins, can parallelize.
7. **H6, H7, H8** — flesh out the deliverable story.
8. **P2** items as adoption progresses.
