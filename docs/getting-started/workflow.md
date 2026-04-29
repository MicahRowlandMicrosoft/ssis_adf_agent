# The Minimum Useful Workflow

The agent ships **29 MCP tools**. New engineers exploring all of them get
lost. This document names the **6-tool minimum path** from a fresh estate
of `.dtsx` files to live ADF pipelines, plus a short list of advanced /
optional tools for when the minimum isn't enough.

> **TL;DR — six tools, one straight line:**
>
> ```
> bulk_analyze → propose_adf_design → convert_estate
>             → validate_adf_artifacts → deploy_to_adf → activate_triggers
> ```
>
> Everything else is optional.

---

## The six tools, by wave milestone

| # | Tool | Wave milestone | What it produces | Decision gate |
|---|---|---|---|---|
| 1 | `bulk_analyze` | **Triage** | One JSON row per package: complexity score, gaps, dependency wave | Reviewer accepts the wave order, marks "do not migrate" packages |
| 2 | `propose_adf_design` | **Design** | A `MigrationPlan` per package: target shape, simplifications, hours estimate | Reviewer edits the plan (`edit_migration_plan` if needed), saves with `save_migration_plan` |
| 3 | `convert_estate` | **Convert** | ADF JSON artifacts (pipeline, linked services, datasets, data flows, triggers, stubs) under `out/<pkg>/` | Reviewer skims `linkedService/*.json`, decides on Key Vault wiring |
| 4 | `validate_adf_artifacts` | **Validate** | Pass/fail structural validation per package | Block deploy on fail |
| 5 | `deploy_to_adf` | **Deploy** | ARM resources created in the target factory | Run with `pre_flight=true` first to catch SHIR / KV / firewall issues *before* writing |
| 6 | `activate_triggers` | **Cut over** | Triggers flipped from Stopped → Started in the target factory | Run with `dry_run=true` first; flip only after a green smoke test |

That's it. Six commands cover triage, design, conversion, validation,
deploy, and cut-over for an entire estate.

---

## The straight-line path, end-to-end

Each block shows the natural-language Copilot prompt and what the tool
does behind the scenes. All six accept many more options — see the per-tool
docs in [README.md](README.md) — but the defaults below are deliberately
chosen to be safe.

### 1. Triage the estate (`bulk_analyze`)

> "Run bulk_analyze on `C:\estate\` and write the report to
> `out/triage.json`."

Output: complexity score per package (0–100), gap list, dependency wave
(0 = leaf packages with no `Execute Package` callers, 1 = packages that
call only wave 0, etc.). Use the `wave` column to drive parallel work
streams.

**Stop and decide:** any "do not migrate" packages are marked here, not
later. They are simply left out of the next step.

### 2. Propose a design per package (`propose_adf_design`)

> "Propose an ADF design for every package in wave 0 and save the plans
> under `plans/`."

Output: one `MigrationPlan` JSON per package — target shape, recommended
simplifications, hours estimate broken down by activity bucket and Script
Task tier (see [effort-methodology.md](../conversion/effort-methodology.md)).

**Reviewer edits welcome.** `edit_migration_plan` mutates a saved plan via
structured operations; the converter respects whatever the plan ends up
saying. The case study at
[docs/case-studies/script_task_port_database_access_configuration/](docs/case-studies/script_task_port_database_access_configuration/README.md)
shows how the hours numbers tie to a real worked port.

### 3. Convert the estate (`convert_estate`)

> "Convert every package whose plan is saved under `plans/` and write
> output to `out/`."

Output: per-package `out/<pkg>/{pipeline,linkedService,dataset,dataflow,trigger,stubs}/`.
Triggers are always emitted in **Stopped** state — never auto-started.

**Stop and decide:** open `out/<pkg>/linkedService/` and confirm Key Vault
wiring. For encrypted packages, run `upload_encrypted_secrets` (P4-4)
once per package to push the real secrets and rewrite the placeholder
`secretName` fields.

### 4. Validate before deploying (`validate_adf_artifacts`)

> "Validate every artifact set under `out/`."

Output: pass/fail per package with the specific JSON path that failed.
This is structural validation only — schema correctness, referential
integrity, naming rules. **It does not check Azure reachability or
permissions** — that's what `deploy_to_adf --pre-flight` is for in the
next step.

**Block on failure.** A failed validation here will deploy successfully
but blow up at first run.

### 5. Pre-flight, then deploy (`deploy_to_adf`)

> "Pre-flight the artifacts under `out/MyPackage/` against subscription
> `S` / RG `rg-data` / factory `adf-prod`."

Run *first* with `pre_flight=true`. The pre-flight resolves Key Vault
references, attempts a managed-identity token-fetch against each
linked-service host, and checks the deploying identity's roles, *without*
creating any ADF resources. Failure modes that consume migration weeks
(SHIR offline, KV permission gap, regional quota, host firewall) get
caught here.

Then deploy:

> "Deploy with `skip_if_exists=true` so we don't overwrite hand-edited
> linked services in the target factory."

`skip_if_exists=true` is the safe default for re-deploys onto an estate
that has any hand-edited resources.

### 6. Activate triggers, last (`activate_triggers`)

> "Activate every trigger named `Trigger_*Daily` across all packages,
> dry-run first."

`dry_run=true` (default) shows what *would* be flipped without touching
anything. Once the smoke test is green, run again with `dry_run=false`.

This is the **only** point in the workflow where pipelines start running
on their own schedule. Deferring it to last is intentional.

---

## Optional tools, by reason to reach for them

Use these only when you hit the specific need each one addresses. You can
ship a full migration without touching any of them.

### Estate-scale planning (after step 2, before step 3)

| Tool | Reach for it when… |
|---|---|
| `edit_migration_plan` | Reviewer needs to mutate a saved plan (rename pipeline, change trigger schedule, drop a Script Task, …) without re-running `propose_adf_design`. |
| `plan_migration_waves` | The default dependency-graph waves don't match how your team is organized; lets you regroup plans into business-driven waves. |
| `estimate_adf_costs` | You need a per-pipeline / per-resource USD projection before sign-off. |
| `compare_estimates_to_actuals` | (P4-5) Post-deploy: join the cost projection to a Cost Management export and report variance. |
| `build_estate_report` | You need a one-PDF estate deliverable for non-engineers. |

### Per-package deep dives

| Tool | Reach for it when… |
|---|---|
| `analyze_ssis_package` | One package is misbehaving and you want a deep-dive on just that one (skip the `bulk_*` overhead). |
| `convert_ssis_package` | Same as above for conversion — a one-off. **`convert_estate` calls this in a loop**, so prefer the estate version when there's more than one package. |
| `consolidate_packages` | Many near-duplicate packages. Folds them into one parameterized pipeline. Run *before* `propose_adf_design` if you want the consolidated version planned as a unit. |
| `explain_ssis_package` / `explain_adf_artifacts` | You're handing the package off to a reviewer who hasn't seen it before. |
| `build_predeployment_report` | Engineer-facing Markdown report (Mermaid diagrams + checklists) for a single package. |

### Validation & parity

| Tool | Reach for it when… |
|---|---|
| `validate_conversion_parity` | Reviewer wants explicit SSIS↔ADF mapping evidence (control-flow + data-flow diff). Structural, not behavioral. |
| `compare_dataflow_output` | (P4-1) Behavioral parity for one Data Flow Task: row-and-column diff against the converted MDF. Captured-mode runs in CI; `live` mode requires `dtexec` + Azure. See [behavioral-parity.md](../conversion/behavioral-parity.md). |
| `smoke_test_pipeline` | Single-pipeline post-deploy smoke test. |
| `smoke_test_wave` | Same, scaled to many pipelines with aggregation. **Use this instead of looping `smoke_test_pipeline`.** |

### Infrastructure & secrets

| Tool | Reach for it when… |
|---|---|
| `provision_adf_environment` | First-time setup of a target factory + RBAC + linked-service skeletons via Bicep. |
| `provision_function_app` | The estate has Script Tasks that need an Azure Function host (almost always, for non-trivial Script Tasks). Run once per environment. |
| `deploy_function_stubs` | Zip-deploy the generated Python stubs to an existing Function App. |
| `upload_encrypted_secrets` | (P4-4) The estate has `EncryptAllWithPassword` packages. Run after `convert_estate`, before `deploy_to_adf`. |
| `export_arm_template` | Customer ops team prefers ARM rollout over the SDK-based `deploy_to_adf`. |

### Discovery (only if your estate path isn't already known)

| Tool | Reach for it when… |
|---|---|
| `scan_ssis_packages` | The `.dtsx` files live in a Git repo or in SQL Server's SSISDB and you need to enumerate them first. If you already have a directory, skip this — `bulk_analyze` accepts a directory directly. |

---

## Tool overlap, signposted

These pairs of tools are easy to confuse:

| Choose this | Not this | Because |
|---|---|---|
| `convert_estate` | `convert_ssis_package` | Whenever there is more than one package — one call, one report. |
| `smoke_test_wave` | `smoke_test_pipeline` (in a loop) | The wave version aggregates results and respects fail-fast / max-parallel. |
| `bulk_analyze` | `analyze_ssis_package` (in a loop) | Same reason — produces one row per package in a single JSON. |
| `build_estate_report` | `build_predeployment_report` | The estate report is one PDF spanning all packages (for stakeholders); the predeployment report is one Markdown per package (for engineers). |
| `validate_adf_artifacts` | `validate_conversion_parity` | The first checks the JSON is valid ADF; the second checks the JSON faithfully represents the SSIS source. Run both. |
| `compare_dataflow_output` | `validate_conversion_parity` | Behavioral (rows match) vs. structural (shapes match). Both are evidence; the first is far more expensive. |

---

## What this doc does not cover

- The full per-tool argument schema — see [README.md](README.md).
- The migration-plan editor — see HOWTO.md.
- Encrypted-package handling — see [encrypted-packages.md](../operations/encrypted-packages.md).
- RBAC requirements per tool — see [rbac.md](../operations/rbac.md).
- Behavioral parity (data-level diff) — see [behavioral-parity.md](../conversion/behavioral-parity.md).
- Air-gapped / no-LLM operation — see [SECURITY.md](SECURITY.md) §"What
  the LLM translator sends, where, and how to disable".

---

## Why these six?

Every other tool exists either because (a) one team's workflow needed it
(consolidation, ARM export, prompt files), (b) a buyer's audit asked for
it (parity, cost variance, RBAC matrix), or (c) it makes a hard part of
one of the six tools easier (the migration-plan editor makes step 2
faster). None of them is *required* for a successful migration. The six
above are.
