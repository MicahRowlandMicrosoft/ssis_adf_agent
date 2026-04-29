# RBAC matrix — minimum permissions per tool

> **Audience:** the security reviewer signing off on the identity that will
> run the SSIS → ADF agent's MCP tools (developer, CI service principal, or
> automation managed identity).
>
> **Goal:** show the minimum Azure RBAC roles + Key Vault data-plane
> permissions required by each tool. "Owner on the resource group" is not
> on this page — and should not be on the change ticket either.

The matrix below is the *least-privilege* recommendation. Some customer
environments will require additional reads (subscription-level *Reader*,
*Cost Management Reader* for Cost API, etc.) — those are noted per-tool.

Authentication is always [`DefaultAzureCredential`](https://learn.microsoft.com/python/api/azure-identity/azure.identity.defaultazurecredential).
Run [`az login`](https://learn.microsoft.com/cli/azure/authenticate-azure-cli)
on a developer machine, set `AZURE_CLIENT_ID` / `AZURE_TENANT_ID` /
`AZURE_CLIENT_SECRET` for service principals, or assign a managed identity
to the host. The roles below apply to whichever principal that resolves to.

---

## Quick reference — minimum role per tool

| Tool                            | ARM role(s)                                         | Scope                              | Key Vault                                                      | Notes |
|---------------------------------|------------------------------------------------------|------------------------------------|----------------------------------------------------------------|-------|
| `scan_ssis_packages`            | none (local FS / Git) **or** `db_datareader`         | local / SQL DB                     | —                                                              | SQL reader path needs read on `[SSISDB].[catalog]` views. |
| `analyze_ssis_package`          | none                                                 | —                                  | —                                                              | Pure parse, no Azure calls. |
| `convert_ssis_package`          | none                                                 | —                                  | —                                                              | Local generation only. With `llm_translate=true`: needs **Cognitive Services OpenAI User** on the Azure OpenAI resource. Set `SSIS_ADF_NO_LLM=1` (P4-8) to forbid LLM calls regardless of args. |
| `validate_adf_artifacts`        | none                                                 | —                                  | —                                                              | Structural validation only. |
| `consolidate_packages`          | none                                                 | —                                  | —                                                              | Local refactor only. |
| `propose_adf_design`            | none                                                 | —                                  | —                                                              | Local LLM-assisted (optional, see [`SECURITY.md`](SECURITY.md)). |
| `save_migration_plan`           | none                                                 | —                                  | —                                                              | Writes JSON to disk. |
| `load_migration_plan`           | none                                                 | —                                  | —                                                              | Reads JSON from disk. |
| `edit_migration_plan`           | none                                                 | —                                  | —                                                              | In-memory edit. |
| `bulk_analyze`                  | as `analyze_ssis_package`                            | —                                  | —                                                              | |
| `convert_estate`                | as `convert_ssis_package`                            | —                                  | —                                                              | |
| `plan_migration_waves`          | none                                                 | —                                  | —                                                              | |
| `estimate_adf_costs`            | none                                                 | —                                  | —                                                              | List-price math; no Azure calls. |
| `build_estate_report`           | none                                                 | —                                  | —                                                              | PDF generation. |
| `build_predeployment_report`    | none                                                 | —                                  | —                                                              | Markdown generation. |
| `explain_ssis_package`          | none                                                 | —                                  | —                                                              | |
| `explain_adf_artifacts`         | none                                                 | —                                  | —                                                              | |
| `validate_conversion_parity`    | none                                                 | —                                  | —                                                              | Local diff. |
| `provision_adf_environment`     | **`Contributor`** + **`User Access Administrator`**  | target RG                          | —                                                              | UAA is required to *write* the RBAC assignments declared in the plan. If your reviewer disallows UAA, run `provision_adf_environment` with `assign_rbac=false` and have a separate operator with UAA grant the assignments out-of-band. |
| `deploy_to_adf` (deploy)        | **`Data Factory Contributor`**                       | target ADF                         | (read of any KV-backed linked services happens at *runtime* by the factory MI, not by the deployer) | Most-restrictive role that allows `Microsoft.DataFactory/factories/*/write`. |
| `deploy_to_adf` (`pre_flight=true`, P4-6) | **`Reader`** + KV `Get Secret`           | target ADF + each referenced KV    | `Get` on each referenced secret                                | Probes only; creates nothing. The MI-token probe needs a token for `https://management.azure.com/.default` — any signed-in identity satisfies it. |
| `validate_adf_artifacts` (with deployed factory introspection) | `Reader`     | target ADF                         | —                                                              | Read-only. |
| `provision_function_app`        | **`Contributor`** on RG                              | target RG                          | —                                                              | Creates Storage + App Insights + Plan + Function App. |
| `deploy_function_stubs`         | **`Website Contributor`** **or** `Contributor`       | target Function App                | —                                                              | Zip-deploy via SCM endpoint. |
| `activate_triggers` (H7)        | **`Data Factory Contributor`**                       | target ADF                         | —                                                              | `start` / `stop` are part of the same RBAC role family. |
| `export_arm_template` (M2)      | none                                                 | —                                  | —                                                              | Template generation is local. Re-deploying the template needs the deploy role above. |
| `smoke_test_pipeline`           | **`Data Factory Contributor`**                       | target ADF                         | —                                                              | Triggers a pipeline run + polls run history. |
| `smoke_test_wave` (N1)          | as `smoke_test_pipeline`                             | target ADF                         | —                                                              | |
| `compare_dataflow_output` (P4-1) | **`Storage Blob Data Reader`** **or** local FS      | source/target dataset stores       | —                                                              | Reads two datasets; never writes. |
| `upload_encrypted_secrets` (P4-4) | **`Key Vault Secrets Officer`** *or* `Set` access policy | target KV                     | `Set` (and `Get` if `overwrite=false`)                         | `Get` is needed only when the tool is asked to skip already-existing secrets — the default. |
| `compare_estimates_to_actuals` (P4-5) | **`Cost Management Reader`** *or* local CSV    | subscription / billing scope       | —                                                              | If you supply a CSV export, no Azure call is made. |

> **`Reader` rule of thumb.** Any tool that lists or reads ADF children
> works with subscription- or RG-scoped `Reader`. If you cannot grant
> `Reader` at the RG, scope it to the factory resource itself.

---

## Why the roles look the way they do

### Data Factory Contributor (built-in)

Allows everything under `Microsoft.DataFactory/factories/*` *except* RBAC
on the factory itself. This is the role to pick for the deploying identity
because:

* It can `create_or_update` linked services / datasets / data flows /
  pipelines / triggers — the only Microsoft.DataFactory write surface
  `deploy_to_adf` calls.
* It can list and read run history (used by `smoke_test_pipeline` and
  `smoke_test_wave`).
* It can `start` / `stop` triggers (`activate_triggers`).
* It cannot grant other principals — the *factory's* managed identity
  still needs its own role assignments granted by a separate operator with
  `User Access Administrator` (typically once, during
  `provision_adf_environment`).

### Key Vault data-plane permissions

`upload_encrypted_secrets` (P4-4) needs **`Set`** on the secret namespace.
`deploy_to_adf --pre_flight` (P4-6) needs **`Get`** on each secret it
checks. The simplest assignments are the two RBAC roles:

* `Key Vault Secrets Officer` — read + write secrets. Use for
  `upload_encrypted_secrets`.
* `Key Vault Secrets User` — read secrets. Use for the pre-flight identity.

If your environment still uses access policies instead of RBAC, grant the
matching `Get` / `Set` data-plane permissions; the data-plane API surface
the SDK calls is identical.

> **Factory's own MI** needs `Get` for the *runtime* path — when an ADF
> activity executes and dereferences `AzureKeyVaultSecret`. That assignment
> goes to the factory's managed identity, not the deploying identity. The
> Bicep template emitted by `provision_adf_environment` declares both.

### User Access Administrator — only for `provision_adf_environment`

The plan model (`MigrationPlan.rbac_assignments`) declares the role
assignments the factory needs (typically `Storage Blob Data Contributor` on
the source/sink ADLS, `Key Vault Secrets User` on the runtime KV, etc.).
Writing those assignments needs `Microsoft.Authorization/roleAssignments/write`,
which only `Owner` and `User Access Administrator` carry. We recommend the
latter so the deploying identity cannot grant *additional* roles outside
the plan.

If your reviewer disallows even time-bounded UAA on the deploy identity,
set the `assign_rbac=false` flag on `provision_adf_environment` (writes
infra only) and have a separate operator apply the role assignments
out-of-band, using the snippets included in the generated Bicep.

### Cost Management Reader — only for `compare_estimates_to_actuals`

Needed only if you ask the tool to fetch actuals via the REST API.
Supplying a CSV export from the portal makes the tool a pure-Python
join — no Azure call, no role required.

---

## Service-principal vs managed-identity vs developer machine

| Setting              | Identity                                         | Notes |
|----------------------|---------------------------------------------------|-------|
| Developer machine    | The user signed in via `az login`.                | Easiest. Roles below are granted to the user. |
| CI / GitHub Actions  | Service principal (federated credential preferred over secret). | Roles granted to the SP application id; scope to the *exact* RG. |
| Self-hosted automation host | The host's system-assigned MI.            | Roles granted to the MI's principal id. |
| Azure-hosted automation (Container Apps, Functions, AKS) | The workload identity / system MI. | Same — assign to the principal id. |

`DefaultAzureCredential` resolves these in turn; whichever lands first is
the one that needs the role.

---

## What we deliberately *don't* require

* **Owner.** Never. Every tool above is satisfied by a strictly narrower
  role.
* **Contributor on the subscription.** Likewise. The widest legitimate
  scope is the target RG, and even that is needed only for
  `provision_adf_environment` + `provision_function_app` (the tools that
  *create* resources).
* **Network Contributor / VNet Contributor.** The agent does not provision
  VNet resources. If your factory needs a private endpoint or a SHIR in a
  customer subnet, that is a separate, customer-owned change.
* **Storage Account Contributor.** The agent does not create storage
  accounts as part of `deploy_to_adf`; storage account creation lives in
  `provision_adf_environment` (RG-scoped Contributor covers it) and in
  `provision_function_app` (same).

---

## Granting the roles

The least-error-prone path is `az role assignment create`:

```bash
# Deploy identity gets Data Factory Contributor on the target factory only
az role assignment create \
  --assignee-object-id <PRINCIPAL_OBJECT_ID> \
  --assignee-principal-type {User|ServicePrincipal|Group} \
  --role "Data Factory Contributor" \
  --scope "/subscriptions/<SUB>/resourceGroups/<RG>/providers/Microsoft.DataFactory/factories/<FAC>"

# Same identity gets Key Vault Secrets User on the runtime vault only
az role assignment create \
  --assignee-object-id <PRINCIPAL_OBJECT_ID> \
  --assignee-principal-type {User|ServicePrincipal|Group} \
  --role "Key Vault Secrets User" \
  --scope "/subscriptions/<SUB>/resourceGroups/<RG>/providers/Microsoft.KeyVault/vaults/<KV>"
```

`provision_adf_environment` emits Bicep equivalents of these assignments;
you can dry-run with `assign_rbac=false` and have your platform team
review the Bicep output before applying.

---

## Auditing the principal

Before opening the change ticket, run:

```bash
az role assignment list \
  --assignee <PRINCIPAL_OBJECT_ID> \
  --all \
  --output table
```

The output should be a small list scoped tightly to the target RG / KV.
If you see `Owner` or `Contributor` at the subscription scope, the
principal is over-privileged for this workload and the ticket will (and
should) be rejected.

---

## Related

* [SECURITY.md](SECURITY.md) — secret-handling policy and threat model.
* [setup.md](../getting-started/setup.md) — bootstrap order: provision → assign roles →
  deploy.
* [encrypted-packages.md](encrypted-packages.md) — KV-side details for the
  one-shot `upload_encrypted_secrets` flow (P4-4).
* [rollback.md](rollback.md) — what to do if a deploy under these roles
  goes sideways.
