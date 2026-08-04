---
agent: agent
tools:
  - validate_adf_artifacts
  - validate_deployer_rbac
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
- **Dry run?**: ${input:dry_run:true to validate locally without Azure writes, false to deploy}
- **Preserve existing artifacts?**: ${input:skip_if_exists:true to skip existing artifacts, false to overwrite them}

## Steps

### 1 — Validate artifacts

Call `validate_adf_artifacts` with `artifacts_dir`.

If any structural issues are found, **stop and report them**. Ask the user to fix the issues
in the generated JSON before deploying.

### 2 - Dry run (if requested)

If `dry_run` = true, call `deploy_to_adf` with `dry_run: true`.
Report what would be deployed (type, name, count).
**Do not proceed to actual deployment.**

### 3 - Confirm Azure access and overwrite policy

Before any Azure call, show the subscription, resource group, factory, and
`skip_if_exists` value. Explain that `skip_if_exists=false` uses
`put_or_update` and can overwrite edits made in ADF Studio. Ask for explicit
confirmation to continue. Do not infer confirmation from invoking this prompt.

After confirmation, run `validate_deployer_rbac` for `deploy_to_adf`. Ask for
the deploying principal object ID if live RBAC inspection is needed. Stop if
required roles are missing.

### 4 - Dependency preflight

Call `deploy_to_adf` with `pre_flight: true`. This is a separate Azure-aware
check, not a deployment. It verifies an ARM token, referenced Key Vault secret
reads, and DNS resolution from this machine. It does not verify SHIR health,
ADF runtime network access, factory managed-identity RBAC, quota, or pipeline
runtime behavior. Stop on failed checks.

### 5 - Deploy

Ask for explicit confirmation for the live write, then call `deploy_to_adf`
with the parameters above, `dry_run: false`, and the selected
`skip_if_exists` value.

Deployment order enforced by the tool: linked services, datasets, data flows,
pipelines, then triggers. Triggers remain Stopped.

### 6 - Deployment report

Produce a Markdown summary:

| Artifact Type | Count | Status |
|---|---|---|
| Linked Services | N | succeeded / failed / skipped |
| Datasets | N | succeeded / failed / skipped |
| Data Flows | N | succeeded / failed / skipped |
| Pipelines | N | succeeded / failed / skipped |
| Triggers | N | succeeded (Stopped) / failed / skipped |

List any failures with their error messages.

### 7 - Post-deployment checklist

Remind the user:
- [ ] Run `smoke_test_pipeline` or `smoke_test_wave` only after separate confirmation; these create pipeline runs in Azure.
- [ ] **Triggers are deployed in Stopped state**. Activate them only after a successful smoke test and explicit confirmation.
- [ ] Validate linked service connections via "Test Connection" in ADF Studio.
- [ ] Run each pipeline once with debug mode before activating schedules.
- [ ] For pipelines that call Azure Functions: verify Function App URLs and keys in the linked services.
- [ ] Confirm that `az login` or service principal credentials are correctly set if deployment succeeded but subsequent runs fail.
