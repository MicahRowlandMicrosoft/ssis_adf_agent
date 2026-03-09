---
mode: agent
tools:
  - validate_adf_artifacts
  - deploy_to_adf
description: Validate ADF artifacts and deploy them to Azure Data Factory.
---

# Deploy ADF Artifacts

Validate and deploy generated ADF JSON artifacts to an Azure Data Factory instance.

## Parameters

- **Artifacts directory**: ${input:artifacts_dir:Directory containing generated ADF JSON artifacts}
- **Subscription ID**: ${input:subscription_id:Azure subscription GUID}
- **Resource group**: ${input:resource_group:Azure resource group name}
- **Factory name**: ${input:factory_name:Azure Data Factory name}
- **Dry run?**: ${input:dry_run:true to validate only (no Azure API calls), false to deploy}

## Steps

### 1 — Validate artifacts

Call `validate_adf_artifacts` with `artifacts_dir`.

If any structural issues are found, **stop and report them**. Ask the user to fix the issues
in the generated JSON before deploying.

### 2 — Dry run (if requested)

If `dry_run` = true, call `deploy_to_adf` with `dry_run: true`.
Report what would be deployed (type, name, count).
**Do not proceed to actual deployment.**

### 3 — Deploy

Call `deploy_to_adf` with the parameters above and `dry_run: false`.

Deployment order enforced by the tool: linked services → datasets → data flows → pipelines → triggers.

### 4 — Deployment report

Produce a Markdown summary:

| Artifact Type | Count | Status |
|---|---|---|
| Linked Services | N | ✅ / ❌ |
| Datasets | N | ✅ / ❌ |
| Data Flows | N | ✅ / ❌ |
| Pipelines | N | ✅ / ❌ |
| Triggers | N | ✅ (Stopped) |

List any failures with their error messages.

### 5 — Post-deployment checklist

Remind the user:
- [ ] **Triggers are deployed in Stopped state** — go to ADF Studio → Manage → Triggers to activate them.
- [ ] Validate linked service connections via "Test Connection" in ADF Studio.
- [ ] Run each pipeline once with debug mode before activating schedules.
- [ ] For pipelines that call Azure Functions: verify Function App URLs and keys in the linked services.
- [ ] Confirm that `az login` or service principal credentials are correctly set if deployment succeeded but subsequent runs fail.
