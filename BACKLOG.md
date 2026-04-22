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

### B1. Copy Activity emits wrong source/sink types for OLEDB → FlatFile
- **Evidence:** [adf/ADDS-MIPS-TC/pipeline/PL_ADDS_MIPS_TC.json](../test-lni-packages/adf/ADDS-MIPS-TC/pipeline/PL_ADDS_MIPS_TC.json) emits `AzureSqlSource` + `AzureSqlSink` against `AzureSqlTable` + `DelimitedText` datasets.
- **Acceptance:** Re-running `convert_ssis_package` on the three LNI packages produces Copy activities where `source.type` / `sink.type` match the wired dataset types and the SSIS source kind. JSON validates and deploys to a real ADF.

### B2. End-to-end deploy + smoke-test on the LNI sample
- **Evidence missing:** No `deploy_to_adf` log; no `smoke_test_pipeline` result.
- **Acceptance:** Captured logs for all three packages: deploy succeeds, smoke test runs, per-activity status visible.

### B3. Credentials / on-prem identifiers in cleartext pipeline variables
- **Evidence:** `DBUserID = LNI\svcOneWAWIP235`, `DatabaseServer = LNIsqTumSTGEX...` baked into pipeline variables in [PL_ADDS_MIPS_TC.json](../test-lni-packages/adf/ADDS-MIPS-TC/pipeline/PL_ADDS_MIPS_TC.json).
- **Acceptance:** Sensitive-looking variables (account names, FQDNs, passwords) are routed to Key Vault references or pipeline parameters by default; documented opt-out flag.

---

## P1 — High

### H1. Doc/reality mismatch on tool count
- **Evidence:** README says **22 tools**; SETUP step 4 says "**five tools**"; copilot-instructions says 5–7.
- **Acceptance:** Single source of truth across README/SETUP/HOWTO and `.github/copilot-instructions.md`.

### H2. Repository ownership + version + changelog
- **Evidence:** README clone URL is `github.com/chsimons_microsoft/ssis_adf_agent.git`; no CHANGELOG, no version pin, no security contact.
- **Acceptance:** Verified repo URL, `CHANGELOG.md`, semver in `pyproject.toml`, SECURITY.md, ownership statement in README.

### H3. LLM Script Task translation silently no-ops on real input
- **Evidence:** Stubs at [adf/ADDS-MIPS-TC/stubs/Database_Access_Configuration/__init__.py](../test-lni-packages/adf/ADDS-MIPS-TC/stubs/Database_Access_Configuration/__init__.py) carry "no C# source code was extracted from the DTSX (package may use self-closing stub format)" and contain only `NotImplementedError`.
- **Acceptance:** Either: (a) translator handles the LNI dialect and emits Python with original C# preserved as comments, or (b) docs add a clearly-flagged "known to fail when…" subsection AND the analyzer surfaces the dialect issue *before* convert.

### H4. Parity validation — define and demonstrate
- **Evidence:** `validate_conversion_parity` listed but no sample output.
- **Acceptance:** A captured parity report on at least one LNI package. Documented definition of what is compared (structure / schema / row counts / values). At least one worked example where it catches a known defect.

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

1. **B1** (regenerate Copy types correctly on LNI) — without this nothing else matters.
2. **B3** (credential leakage) — must land alongside B1 because the same regen run will rewrite pipeline JSON.
3. **B2** (deploy + smoke-test logs) — proves B1/B3 worked.
4. **H3** (LLM translator on LNI dialect) — same sample, same regen pass.
5. **H4** (parity report) — produce on the deployed pipeline.
6. **H1**, **H5**, **H2** (doc/repo hygiene) — fast wins, can parallelize.
7. **H6, H7, H8** — flesh out the deliverable story.
8. **P2** items as adoption progresses.
