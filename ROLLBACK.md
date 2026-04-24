# Rollback Story (N2)

The agent's deploy path is non-destructive by default and produces enough
metadata to reverse a deploy at three different scopes. Pick the rollback
strategy that matches the blast radius of the failure.

> **Captured real failure:** see
> [docs/case-studies/first_deploy_keyvault_recovery/](docs/case-studies/first_deploy_keyvault_recovery/README.md)
> for a sanitized walkthrough of one first-deploy that failed (Key Vault
> RBAC on the factory's managed identity), the wrong hypothesis that ate
> 22 minutes, and the three commands that recovered it. The rest of this
> document generalizes from that capture.

## Decision tree

```
Is the entire factory being decommissioned (pilot over,
provisioned in wrong sub/region, security re-provision)?
  yes -> Strategy 4 (tear down the factory)
  no
    Is the deploy partial / mid-flight?
      yes -> Strategy 1 (per-artifact delete via lineage.json)
      no
        Is only one pipeline broken?
          yes -> Strategy 2 (soft-revert single pipeline)
          no  -> Strategy 3 (branch / git restore + redeploy)
```

---

## Strategy 1 — per-artifact delete using `lineage.json`

After a deploy (M1), `<artifacts_dir>/lineage.json` carries the full Azure
resource ID for every artifact this run created or updated. This is the
fastest way to undo a freshly-failed deploy.

```powershell
# From the artifacts dir
$lineage = Get-Content lineage.json | ConvertFrom-Json
foreach ($a in $lineage.artifacts) {
    if ($a.azure_resource_id -and $a.deployed_in_last_run) {
        az resource delete --ids $a.azure_resource_id
    }
}
```

* Use this **only** when you know the artifacts in `lineage.json` were
  *new* in the failed run. If you ran with `skip_if_exists=true` (H8),
  `lineage.json` correctly marks pre-existing artifacts as `skipped`, so
  this loop will leave them alone.
* Triggers are deployed in `Stopped` state; deleting them mid-run is
  always safe.

## Strategy 2 — soft-revert a single pipeline

Useful when one pipeline regressed but the rest of the deploy is fine.

1. Find the previous version in source control:
   ```powershell
   git log --oneline -- adf/pipeline/PL_MyPackage.json
   git show <previous-sha>:adf/pipeline/PL_MyPackage.json > restore.json
   ```
2. Push the restored JSON back to the factory using a one-shot
   `deploy_to_adf` against an artifacts directory containing only the
   single pipeline:
   ```powershell
   New-Item -ItemType Directory -Path tmp_revert/pipeline | Out-Null
   Move-Item restore.json tmp_revert/pipeline/PL_MyPackage.json
   python -m ssis_adf_agent deploy `
     --artifacts-dir  tmp_revert `
     --subscription-id <sub> `
     --resource-group <rg> `
     --factory-name <adf>
   ```
3. Verify with `smoke_test_pipeline` (or `smoke_test_wave` for the wave).

## Strategy 3 — branch / git restore + full re-deploy

Use this for a wave-scale rollback or when multiple artifact types are
implicated.

1. Identify the last-known-good commit on your migration branch.
2. Check it out into a clean working tree:
   ```powershell
   git worktree add ../revert <last-good-sha>
   cd ../revert
   ```
3. Re-deploy the entire wave from the good tree:
   ```powershell
   python -m ssis_adf_agent deploy `
     --artifacts-dir adf/<package> `
     --subscription-id <sub> `
     --resource-group <rg> `
     --factory-name <adf>
   ```
4. Use `skip_if_exists=false` (the default) to *force* the older versions
   back into the factory, overwriting the broken state.
5. Run `smoke_test_wave` against the full pipeline list to confirm the
   wave is healthy again.
6. Remove the worktree: `git worktree remove ../revert`.

---

## Strategy 4 — tearing down a provisioned factory

Use this when the factory itself was provisioned by
`provision_adf_environment` for a pilot / POC and the entire
environment needs to go away — not just the artifacts inside it.
This is rarely the right answer mid-migration; it is the right answer
when the pilot ends, when an environment was created in the wrong
subscription / region, or when a security review demands a clean
re-provision.

> **Warning.** This deletes the factory **and everything in it** —
> pipelines, datasets, linked services, triggers, run history, and
> any hand-edits applied since the last deploy. Strategies 1–3 should
> be exhausted first. There is no undo.

### Order of operations

1. **Stop all triggers first** so no run starts mid-teardown:
   ```powershell
   az datafactory trigger list `
     --resource-group <rg> --factory-name <adf> `
     --query "[?properties.runtimeState=='Started'].name" -o tsv |
     ForEach-Object {
       az datafactory trigger stop `
         --resource-group <rg> --factory-name <adf> --name $_
     }
   ```
2. **Snapshot the lineage** so you keep a record of what *was* there:
   ```powershell
   Copy-Item out/lineage.json out/lineage.pre-teardown.json
   ```
3. **Delete the factory:**
   ```powershell
   az datafactory delete `
     --resource-group <rg> --factory-name <adf> --yes
   ```
   Cascades through every child resource (pipelines, datasets, linked
   services, triggers, integration runtimes hosted in the factory).
   Self-Hosted Integration Runtimes that were registered to the
   factory are de-registered server-side; the SHIR Windows service on
   your on-prem host stays installed and must be uninstalled
   separately.

### RBAC cleanup

`provision_adf_environment` (with `assign_rbac=true`) granted at
least one role assignment to the factory's system-assigned managed
identity. After deletion the MI is gone but the role assignments are
**not** automatically removed — they become orphaned principal IDs in
the role-assignments listing. Clean them up:

```powershell
# Find orphaned assignments (those whose principalName is empty / "Unknown")
az role assignment list --all `
  --query "[?principalName==''||principalName=='Unknown']" -o table

# Delete by id
az role assignment delete --ids <assignment-id>
```

Run the same query at subscription scope, resource-group scope, and
on every Key Vault / storage account / SQL server that the deleted
factory MI had been granted access to (RBAC.md row for
`provision_adf_environment` lists the typical targets). Leftover
orphaned assignments are not a security risk by themselves but they
clutter audit reports and confuse the next provisioning run.

### Key Vault cleanup

If the factory MI had been granted Key Vault data-plane access:

- **RBAC mode (`enableRbacAuthorization=true`):** the role assignment
  on the vault is one of the orphaned entries cleaned up above. No
  separate vault-level action is required.
- **Access-policy mode:** access policies are stored on the vault
  resource itself, keyed by the deleted MI's principal id. Remove
  them with:
  ```powershell
  az keyvault delete-policy `
    --name <kv> --object-id <former-factory-principal-id>
  ```
  The principal id is whatever `az datafactory show --query
  identity.principalId` returned *before* the factory was deleted —
  if you snapshotted the lineage in step 2, it is recoverable from the
  factory ARM id; if not, the access policy entry will display as
  `Unknown` in the portal and can be safely removed by id.

### What `lineage.json` looks like for a fully-deprovisioned environment

After deletion every `azure_resource_id` in the manifest still points
at a now-non-existent ARM resource. The agent does **not** rewrite
the manifest on teardown (there is no inverse of
`update_lineage_with_deployment`). Two options:

- **Preserve the manifest as-is** for the audit trail (recommended) —
  rename it to `lineage.pre-teardown.json` (step 2 above) so a future
  reader is not misled into thinking the IDs are still resolvable.
- **Regenerate** by running `convert_ssis_package` against the same
  `.dtsx` set without `update_lineage_with_deployment` — this yields
  a manifest with empty `azure_resource_id` placeholders, equivalent
  to a fresh pre-deploy state.

Re-provisioning into the same resource group with the same factory
name reuses the *name* but creates a fresh MI principal id — every
RBAC assignment must be re-granted (P5-12 `validate_deployer_rbac`
will catch any that were missed).

### When to use Strategy 4 vs. the others

| Situation | Strategy |
|---|---|
| One artifact is broken in a deploy | 1 (per-artifact delete) |
| One pipeline regressed; rest of factory is healthy | 2 (soft-revert) |
| Multiple pipelines regressed in a wave | 3 (git restore + full redeploy) |
| Pilot is over, factory is being decommissioned | **4 (this strategy)** |
| Factory was provisioned in the wrong sub / region | **4 (this strategy)**, then re-provision |
| Security review requires a clean re-provision of the MI | **4 (this strategy)** |

---

## Pre-flight: always do this before deploying

* Generate / pull the predeployment report (`build_predeployment_report`)
  and capture the parity diff in your PR.
* Run a dry-run trigger activation (`activate_triggers` with
  `dry_run=true`) so you know what *would* be activated.
* Tag the commit you deployed:
  ```powershell
  git tag -a "deploy/$(Get-Date -Format yyyyMMdd-HHmm)" -m "Deploy to <env>"
  git push --tags
  ```
* That tag is what Strategy 3 reverts to.

## Triggers — special-case rollback

Triggers are always deployed in `Stopped` state. To revert an *activation*
(rather than a deploy), just stop them:

```powershell
python -m ssis_adf_agent activate-triggers `
  --subscription-id <sub> `
  --resource-group <rg> `
  --factory-name <adf> `
  --dry-run  # see what's running
# Then via the portal or az CLI: az datafactory trigger stop ...
```

`activate_triggers` itself does not stop — it only activates — by design,
so it can never accidentally pause production traffic.

## Related

* [BACKLOG.md](BACKLOG.md) #M1 — lineage manifest backfill.
* [BACKLOG.md](BACKLOG.md) #H7 — bulk trigger activation.
* [BACKLOG.md](BACKLOG.md) #H8 — `skip_if_exists` non-destructive deploy.
