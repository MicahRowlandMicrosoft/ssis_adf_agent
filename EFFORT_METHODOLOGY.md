# Effort Estimate Methodology

This document discloses **exactly how** the per-package and per-wave hour
ranges in the agent's reports are computed, so a customer can:

1. Verify they are not arbitrary (they're not — every term is rooted in code
   you can audit), and
2. Calibrate the formula against their own historical conversion data.

The source of truth for everything below is
[`ssis_adf_agent/migration_plan/proposer.py`](ssis_adf_agent/migration_plan/proposer.py)
(`_effort_from_package`).  If this document drifts from the code, the code
wins and this doc is wrong.

---

## What "hours" means here

A single number — `total_hours` — represents the **midpoint** of the
estimated work for *one experienced ADF/SSIS engineer* to:

| Phase | Default share of `total_hours` |
|---|---|
| Architecture / design review (`architecture_hours`) | 15% of bucket base, floor 2h |
| Development (`development_hours`) | bucket base + script/data-flow weighting |
| Testing — incl. parallel run + UAT (`testing_hours`) | 35% of dev, floor 2h |

The hours **do not** include:

* Customer SME time (data-domain validation, UAT sign-off).
* Estate-wide setup (IR, Key Vault, RBAC, CI/CD) — that lives separately as
  `estate_setup_hours` on `plan_migration_waves`.
* Production rollout / cutover communication.

---

## Per-package formula

```
bucket, base = bucket_for_score(complexity_score)
dev   = base + sum(script_porting_hours) + sum(dataflow_hours)
dev   = max(1.0, dev - 0.5 * n_simplifications)
arch  = max(2.0, base * 0.15) + 0.5 * n_simplifications
test  = max(2.0, dev * 0.35)
total = arch + dev + test
low   = total * 0.7   # –30%
high  = total * 1.6   # +60% (asymmetric — see "Why a range?")
```

### Bucket bases

| Complexity score | Bucket      | Base dev hours |
|------------------|-------------|----------------|
| 0–30             | `low`       | 4              |
| 31–55            | `medium`    | 10             |
| 56–80            | `high`      | 24             |
| 81+              | `very_high` | 56             |

### Script Task porting hours

Each Script Task contributes hours based on heuristic content classification
(`_script_porting_hours`). Trivial / simple / moderate / complex stubs each
add a different amount (the agent inspects the C# / VB body for loops, file
I/O, COM interop, etc. before classifying).

The exact formula is `max(tier_floor, LOC / tier_divisor)` capped at 40h:

| Tier      | Divisor | Floor | Typical range |
|-----------|---------|-------|---------------|
| trivial   | 120     | 0.25h | ≤ 0.5h        |
| simple    | 40      | 0.5h  | 0.5h – 1.5h   |
| moderate  | 25      | 2.0h  | 2h – 4h       |
| complex   | 15      | 6.0h  | 6h – 40h      |

**Worked example (anchors the `moderate` row to a real port):** see
[docs/case-studies/script_task_port_database_access_configuration/](docs/case-studies/script_task_port_database_access_configuration/README.md).
That LNI Script Task is 80 LOC (moderate tier) → predicted **3.2h**;
**actual time captured was 3.5h**, with the breakdown by phase (read,
design, code, test, wire, review) shown in the case-study README. The
0.3h overshoot reflects one-time decisions (Key Vault swap-in,
parameterized linked service) that the heuristic can't see; for the next
similar Script Task in the same estate the team budgeted **~1.5h**.

### Data Flow Task hours

`_dataflow_hours` walks every component in the data flow and weights them:

* **heavy** — Fuzzy Lookup, Fuzzy Grouping, Term Extraction, Slowly Changing
  Dimension, Script Component (and similar): high per-component weight.
* **medium** — Lookup, Conditional Split, Derived Column, Aggregate, Sort,
  Pivot/Unpivot, Merge Join, Union All: moderate weight.
* **light** — Copy Column, Row Count, Multicast, Audit, character-map: low
  weight.

The reported `notes` line shows the heavy / medium / light counts so the
output is auditable: *"Data flows: 1 heavy / 4 medium / 12 light → 9.5h"*.

### `n_simplifications`

Each rule the migration planner suggests (consolidate two equivalent control
flows, drop a sequence container, swap a pattern for an out-of-the-box
activity) shaves 0.5h off dev and adds 0.5h to arch — reflecting that the
work moves from coding to design.

---

## Why a range, not a point?

Every per-package estimate is reported as `low_hours` / `total_hours` /
`high_hours`. The envelope is **deliberately asymmetric**:

* `low = total * 0.7` (–30%) — the floor a well-prepared engineer can hit
  with no surprises.
* `high = total * 1.6` (+60%) — the realistic ceiling once you account for
  the most common overrun causes:
  * Source schema reality differs from the package's mapping
  * Connection auth differs in ADF (managed identity vs SQL auth)
  * Hidden Script-Task behavior surfaces only at runtime
  * Reviewer cycles (design + code) take longer than the optimistic plan

The 2.3× spread you may see in real reports (e.g.
[PreDeployment_Report.md](../test-lni-packages/PreDeployment_Report.md)) is
a feature, not a bug — it tells the customer *"this is a planning estimate,
not a quote"*. As more conversions complete and we collect calibration data,
we plan to narrow the envelope rather than tighten it artificially.

---

## Wave-level effort (`plan_migration_waves`)

Per-wave hours are the sum of per-package `estimated_total_hours` for the
packages in that wave, with two optional adjustments
(see `migration_plan/estate_tools.py`):

* **`apply_learning_curve=True`** — packages within a wave are progressively
  discounted (100%, 90%, 85%, 80%, 75%, 70%, 65%, 60%, … floor 50%) to
  reflect that later packages reuse design decisions, linked services, and
  reviewer context established by the first package. **Off by default** so
  totals stay backward-compatible.
* **`estate_setup_hours=N`** — a one-time block added to Wave 1 covering
  IR provisioning, Key Vault, naming conventions, CI/CD pipeline setup,
  RBAC, and observability — work that no per-package estimate captures.

---

## Calibration / proving us wrong

We will narrow the ±30/+60% envelope when we have hard data, not before.
If you run a wave and the actual hours come in *outside* the band, please
file an issue with:

* The package(s) (sanitized)
* The bucket the agent assigned
* The actual hours per phase (arch / dev / test)
* What drove the variance

That data feeds backlog item **M4** (cost calibration) and a future revision
of this document.
