# Case study: First deploy that failed — Key Vault access policy

> **Sanitized capture.** Customer name, subscription IDs, factory
> name, and Key Vault name have been redacted; everything else (error
> message, RBAC role names, recovery commands, elapsed time) is the
> live record.

This is the case ROLLBACK.md generalizes from. It is one captured
real failure of a first-time `deploy_to_adf` run, written up so a new
buyer can see what failure recovery looks like in practice.

---

## Setting

- **Estate:** 47 SSIS packages, mid-complexity (one Script Task,
  several Execute SQL with parameter mappings, no Data Flow Tasks
  needing Mapping Data Flows).
- **Deploy target:** new ADF factory `adf-prod-eastus2-CUSTOMER` in
  RG `rg-data-platform-prod`, subscription `<sub-redacted>`.
- **Deployer:** Azure CLI service principal `sp-adf-deploy`, granted
  *Contributor* on the resource group an hour before the run.
- **Key Vault:** `kv-adf-secrets-CUSTOMER`, RBAC mode (not access
  policy mode), pre-populated by `upload_encrypted_secrets` for 6
  encrypted packages.
- **Time window:** First deploy attempted at 14:02 local; recovered
  and re-deployed cleanly at 14:48. **Elapsed: 46 minutes**, with 22
  minutes spent on the wrong hypothesis (next section).

---

## What happened

`deploy_to_adf` ran without `pre_flight=true` (P4-6 had not yet
shipped at the time of capture). The factory and 41 of 47 pipelines
deployed successfully. The remaining 6 — exactly the encrypted
packages whose linked services pointed at Key Vault references — failed
with a near-identical error per linked service:

```
ManagedServiceIdentityCredentialNotFound: Managed identity credential
not found. The Linked Service references Key Vault secret
'<vault-uri>/secrets/MyConn-password' but the Data Factory's managed
identity does not have permission to read secrets from
kv-adf-secrets-CUSTOMER. AdfManagementResponse: 403, Forbidden,
KeyVaultErrorException: Operation returned an invalid status code
'Forbidden'.
```

The deploy returned a non-zero exit; `lineage.json` correctly marked
the 41 successful artifacts as `deployed_in_last_run=true` and the 6
failed linked services as `failed`.

---

## The 22 wasted minutes (wrong hypothesis)

The first hypothesis was that the *deployer* (the service principal)
lacked Key Vault permission. We granted `sp-adf-deploy` the
*Key Vault Secrets User* role on the vault, waited for replication,
and re-ran. **Same error, same six linked services.**

The mistake: the error message said "the Data Factory's managed
identity", not "the deployer". ADF resolves Key Vault references at
*runtime* using the **factory's own system-assigned managed identity**,
not the identity that deployed the factory. Granting the deployer
permission was irrelevant.

This is exactly the failure mode `deploy_to_adf --pre-flight` (P4-6)
now catches before any resource is created.

---

## The recovery

Three commands, in order:

### 1. Confirm the factory's managed identity object ID

```powershell
az datafactory show `
    --resource-group rg-data-platform-prod `
    --name adf-prod-eastus2-CUSTOMER `
    --query identity.principalId -o tsv
# -> <factory-mi-object-id>
```

### 2. Grant *Key Vault Secrets User* on the vault to that identity

```powershell
$kvId = az keyvault show --name kv-adf-secrets-CUSTOMER --query id -o tsv
az role assignment create `
    --assignee <factory-mi-object-id> `
    --role "Key Vault Secrets User" `
    --scope $kvId
```

(Vault was in RBAC mode; if it had been in access-policy mode, the
equivalent would have been `az keyvault set-policy --secret-permissions get list`.)

### 3. Re-run the deploy with `skip_if_exists=true`

> "Re-deploy the artifacts under `out/` against the same factory with
> `skip_if_exists=true` so the 41 already-deployed pipelines are not
> overwritten."

The 41 successful artifacts were skipped (visible in `lineage.json` as
`skipped` with reason `already_exists`). The 6 failed linked services
were retried and succeeded. Total runtime for the second deploy: 4
minutes.

No `lineage.json`-driven cleanup was needed because the failure mode
left the failed linked services in a *not-created* state; ROLLBACK.md
Strategy 1 was unnecessary.

---

## What would have prevented this

| Control | Effect |
|---|---|
| **`deploy_to_adf` with `pre_flight=true` first** (P4-6) | Pre-flight attempts a managed-identity token-fetch against each linked-service host *and* reads the deploying identity's roles; it would have flagged "factory MI has no role on `kv-adf-secrets-CUSTOMER`" before any resource was created. **This is now the recommended first step in [WORKFLOW.md](../../WORKFLOW.md).** |
| **RBAC.md row for `deploy_to_adf`** (P4-7) | The matrix names *Key Vault Secrets User on the referenced vault, granted to the factory's system-assigned identity* as a required role for any deploy that includes Key Vault-backed linked services. Reading the matrix before the deploy would have prevented the wrong-identity hypothesis. |
| **Provisioning the role assignment in the factory's Bicep** | If the factory itself is provisioned via `provision_adf_environment`, the role assignment can be co-deployed. The customer in this capture had pre-existing factory resources, so this option was not available. |

---

## Lessons captured

1. **Read the failure-message subject carefully.** "The Data Factory's
   managed identity" ≠ "the deployer". Most ADF runtime resource
   access uses the factory MI, not the SP that deployed the factory.
2. **`skip_if_exists=true` makes recovery cheap.** A second deploy
   after fixing the root cause cost 4 minutes, not 46. Prefer it for
   any re-deploy.
3. **`lineage.json` is the source of truth for what got created.**
   It distinguished the 41 succeeded from the 6 failed cleanly, which
   is what enabled the targeted retry instead of a full teardown.
4. **Pre-flight (P4-6) is now mandatory for first deploys.**
   `WORKFLOW.md` step 5 explicitly calls out running with
   `pre_flight=true` first; this case is why.

---

## See also

- [ROLLBACK.md](../../ROLLBACK.md) — the generalized rollback decision tree.
- [WORKFLOW.md](../../WORKFLOW.md) §5 — why pre-flight is the default.
- [RBAC.md](../../RBAC.md) — required roles per tool, including the
  factory MI / Key Vault row.
- [ENCRYPTED_PACKAGES.md](../../ENCRYPTED_PACKAGES.md) — the upstream
  flow that put the Key Vault references in the linked services in the
  first place.
