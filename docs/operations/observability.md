# Pipeline-Execution Observability

After deploy, the migrated pipelines need to be **monitored as a
production workload** — not as a one-off cut-over event. This document
names:

1. The recommended diagnostic-settings target.
2. A sample KQL workbook for pipeline failures and duration trending.
3. Three suggested alert rules with thresholds.

It does **not** cover SSIS-side or SHIR-side monitoring; both are
documented in their respective Microsoft Learn pages and are unchanged
by the migration.

> The agent does not provision the workbook or the alerts — those are
> one-time-per-environment setup, easier to do via Bicep/portal than via
> the converter. This guide is what to *deploy*, not what to *write*.

---

## 1. Diagnostic-settings target

ADF emits structured logs through Azure Monitor Diagnostic Settings.
For the migrated factory, the **recommended target is a Log Analytics
workspace** (not Event Hubs, not Storage Account) for three reasons:

| Reason | Detail |
|---|---|
| Queryable | KQL on the same workspace where you'll keep the workbook and alerts. |
| Retention | Configurable 30–730 days; default 30 is enough for trend work, 90 days is comfortable for incident review. |
| Single-pane | If the same workspace already receives App Service / Function logs (the Script Task host), correlated queries become trivial. |

### Categories to enable

| Category | Why |
|---|---|
| `PipelineRuns` | Per-pipeline outcome, duration, parameters. **Required** for everything below. |
| `ActivityRuns` | Per-activity outcome, error, input/output. Needed to root-cause failures. |
| `TriggerRuns` | Trigger fire/skip/fail events. Needed for the "trigger silently stopped firing" alert. |
| `PipelineActivityRuns` | Joined view that some workbooks prefer. Optional. |
| `AllMetrics` | Numeric metrics (run duration, failed-run count). Required for metric-based alert rules. |

Anything else (sandbox, SSIS-IR, etc.) is optional and depends on whether
the migration also runs lift-and-shift packages on SSIS-IR.

### Bicep snippet (one-time, per factory)

```bicep
resource adf 'Microsoft.DataFactory/factories@2018-06-01' existing = {
  name: factoryName
}

resource diag 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = {
  scope: adf
  name: '${factoryName}-to-law'
  properties: {
    workspaceId: logAnalyticsWorkspaceId
    logs: [
      { categoryGroup: 'allLogs', enabled: true, retentionPolicy: { enabled: false, days: 0 } }
    ]
    metrics: [
      { category: 'AllMetrics', enabled: true, retentionPolicy: { enabled: false, days: 0 } }
    ]
  }
}
```

(Retention is set on the *workspace*, not the diagnostic setting, in
modern API versions — the per-setting retention fields are ignored.)

---

## 2. Sample KQL workbook

Two saved queries cover the everyday case. Drop both into an Azure
Workbook and pin to the factory's overview blade.

### Query A — Failed pipeline runs (last 24h)

```kusto
ADFPipelineRun
| where TimeGenerated > ago(24h)
| where Status == "Failed"
| project
    TimeGenerated,
    PipelineName,
    RunId,
    DurationMs = toint(End - Start) / 1000,
    FailureType,
    Parameters,
    Message = tostring(parse_json(Error).message)
| order by TimeGenerated desc
```

What it answers: *what failed, when, why, with which parameters*.
Click-through to Run ID gives the per-activity detail in the ADF portal.

### Query B — Duration trend (last 14 days, p50/p95 per pipeline)

```kusto
ADFPipelineRun
| where TimeGenerated > ago(14d)
| where Status == "Succeeded"
| extend DurationSec = toint(End - Start) / 1000
| summarize
    P50 = percentile(DurationSec, 50),
    P95 = percentile(DurationSec, 95),
    Runs = count()
    by PipelineName, bin(TimeGenerated, 1d)
| order by PipelineName asc, TimeGenerated asc
```

What it answers: *is anything getting slower over time?* Plot as a
line chart grouped by `PipelineName`. A creeping p95 with stable p50
usually means a downstream system (DB, file share, SHIR) is degrading.

### Workbook layout

A two-tab workbook is enough for BAU:

- **Tab 1 — Health:** Query A as a grid + a tile of `count()` of
  failures in the last 1h / 24h / 7d.
- **Tab 2 — Trends:** Query B as a multi-series line chart, plus a
  bar chart of "runs per pipeline last 24h" so you can see if a
  trigger silently stopped firing.

The full workbook JSON is intentionally not checked into this repo —
workspace IDs and resource IDs are deployment-specific. The two queries
above are the substance; the rest is layout.

---

## 3. Suggested alert rules

Three alerts catch the failure modes that have actually bitten migrated
estates. **Run all three on a 5-minute evaluation cadence** unless you
are confident your factory has bursty traffic, in which case extend to
15 minutes for alert C only.

### Alert A — Any pipeline failure (severity 2)

| Property | Value |
|---|---|
| Signal | Log query |
| Query | `ADFPipelineRun \| where TimeGenerated > ago(5m) \| where Status == "Failed"` |
| Threshold | `Count > 0` |
| Evaluation | every 5m, over the last 5m |
| Severity | 2 (Warning) |
| Action group | Page on-call |

Rationale: the buyer's biggest unknown post-cut-over is "are the
pipelines actually running cleanly". Any failure should land in chat /
on the pager immediately. Severity 2 (not 1) so a single transient
retry-recoverable failure does not wake someone up; tune to severity 1
once a baseline establishes which pipelines have the highest noise.

### Alert B — Pipeline duration regression (severity 3)

| Property | Value |
|---|---|
| Signal | Log query |
| Query | See below |
| Threshold | `Count > 0` |
| Evaluation | every 1h, over the last 1h |
| Severity | 3 (Informational) |

```kusto
let baseline = ADFPipelineRun
    | where TimeGenerated between (ago(14d) .. ago(1d))
    | where Status == "Succeeded"
    | extend DurationSec = toint(End - Start) / 1000
    | summarize P95 = percentile(DurationSec, 95) by PipelineName;
ADFPipelineRun
| where TimeGenerated > ago(1h)
| where Status == "Succeeded"
| extend DurationSec = toint(End - Start) / 1000
| join kind=inner baseline on PipelineName
| where DurationSec > P95 * 1.5
| project TimeGenerated, PipelineName, DurationSec, BaselineP95 = P95
```

Rationale: catches gradual degradation before it becomes a hard
failure. 1.5× the 14-day p95 is permissive enough to ignore single
slow runs but tight enough to flag a real shift. Severity 3 keeps it
out of the on-call queue while still surfacing in the daily review.

### Alert C — Trigger stopped firing (severity 2)

| Property | Value |
|---|---|
| Signal | Log query |
| Query | See below |
| Threshold | `Count > 0` |
| Evaluation | every 1h, over the last 6h |
| Severity | 2 (Warning) |

```kusto
let expected = datatable(TriggerName: string, MaxQuietHours: int) [
    "Trigger_Daily",  26,
    "Trigger_Hourly",  2,
    // ... one row per scheduled trigger
];
expected
| join kind=leftouter (
    ADFTriggerRun
    | where TimeGenerated > ago(48h)
    | where Status == "Succeeded"
    | summarize LastRun = max(TimeGenerated) by TriggerName
) on TriggerName
| extend QuietHours = datetime_diff('hour', now(), LastRun)
| where isnull(LastRun) or QuietHours > MaxQuietHours
| project TriggerName, LastRun, QuietHours, MaxQuietHours
```

Rationale: a trigger silently set to Stopped (operator click,
post-incident "let's pause this", deploy that didn't reactivate) is a
silent-failure mode that no per-run alert catches. The `expected`
table is per-environment configuration; keep it under version control
next to the workbook.

---

## What this guide does not promise

- **It does not replace SLO/SLI design.** Three alerts will catch the
  common failure modes; production-grade SLOs require per-pipeline
  business-criticality input the agent has no way to know.
- **It does not provision anything.** The Bicep snippet above is a
  pattern, not a deployable template — `provision_adf_environment`
  does not currently emit it (intentional: customer ops teams have
  strong opinions about Log Analytics ownership).
- **It does not cover Function-host observability.** The Script Task
  stubs run in Azure Functions; their App Insights story is the
  Functions defaults. See the
  [Functions monitoring docs](https://learn.microsoft.com/azure/azure-functions/monitor-functions)
  for that side.
- **It does not cover SHIR observability.** Self-hosted IR ships its
  own diagnostic logs and the
  [SHIR monitoring docs](https://learn.microsoft.com/azure/data-factory/monitor-integration-runtime)
  cover it directly.

---

## See also

- [workflow.md](../getting-started/workflow.md) — the 6-tool minimum migration path.
- [behavioral-parity.md](../conversion/behavioral-parity.md) — pre-cut-over data
  parity (this doc covers post-cut-over behavior).
- [rollback.md](rollback.md) — what to do *after* alert A fires.
