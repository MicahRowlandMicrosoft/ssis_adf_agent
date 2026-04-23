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
