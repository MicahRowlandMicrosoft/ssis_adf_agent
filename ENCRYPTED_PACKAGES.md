# EncryptAllWithPassword end-to-end recipe (M6)

## Why this hurts

When SSIS packages use `ProtectionLevel = EncryptAllWithPassword` (or
`EncryptSensitiveWithPassword`), every secret inside — connection-manager
passwords, OData credentials, Script Task config, etc. — is wrapped in
DPAPI/AES output that is **only readable by SSIS itself when re-keyed with
the same password**. The agent cannot extract those secrets, so:

* Generated linked services have empty / placeholder password fields.
* Deploys succeed (ARM doesn't validate credentials), but the first pipeline
  run against the linked service fails authentication.

This recipe shows the full flow to land those secrets in Azure Key Vault
*before* deploying the converted artifacts.

## Prerequisites

* The original SSIS package(s) **and** the password used to encrypt them.
* An Azure Key Vault the deploying identity (SP / managed identity) can
  read with `get` permission on secrets.
* `dtutil.exe` (ships with SSIS / SSDT) on the machine extracting secrets.

---

## Step 1 — extract secrets from .dtsx

The agent does not exfiltrate secrets. You must do this with `dtutil` so
the act is auditable on the customer side. For each affected package:

```powershell
# Decrypt to a working copy with no protection, in a *clean* folder.
dtutil /FILE "src\MyPackage.dtsx" `
       /DECRYPT "<original-password>" `
       /COPY FILE;"work\MyPackage.unprotected.dtsx"

# Re-emit with no protection so secrets are inline (still in the working folder!).
dtutil /FILE "work\MyPackage.unprotected.dtsx" `
       /ENCRYPT FILE;"work\MyPackage.unprotected.dtsx";0 `
       /Q
```

`work\` should be on an encrypted volume, never committed, and deleted at
the end of this procedure.

Open `work\MyPackage.unprotected.dtsx` in a text editor and find each
`<DTS:Property DTS:Name="ConnectionString">…Password=…</DTS:Property>`
or `<DTS:Property DTS:Name="Password">…</DTS:Property>` element. Note the
connection-manager **DTS:ObjectName** so you can reference it later.

## Step 2 — push secrets to Key Vault

```powershell
# One secret per (package, connection-manager) pair to keep audit clean.
az keyvault secret set `
    --vault-name "kv-ssis-migration" `
    --name      "MyPackage-CMSrc-Password" `
    --value     "<extracted-password>"
```

The naming convention `<package>-<connection-manager>-Password` makes the
K-V entries discoverable when you have hundreds of secrets.

## Step 3 — convert with `use_key_vault=true`

Re-run the **original encrypted .dtsx** through the agent (do *not* convert
the unprotected copy):

```jsonc
// MCP convert_ssis_package args
{
  "package_path": "src/MyPackage.dtsx",
  "output_dir":   "out/MyPackage",
  "use_key_vault": true,
  "kv_url":        "https://kv-ssis-migration.vault.azure.net/",
  "kv_ls_name":    "LS_KeyVault"
}
```

The generator will:

1. Emit a `LS_KeyVault.json` linked service pointing at your vault, using
   `AzureKeyVaultLinkedService` with managed-identity auth.
2. Rewrite every other linked service that needs a secret to use a
   `secretReference` against `LS_KeyVault` instead of an inline password.

## Step 4 — patch secret names

The generator does *not* know the K-V secret names you chose in Step 2. By
default each rewritten linked service has a placeholder secret name
matching the connection manager. You have two options:

* **Match in Key Vault**: name your K-V secrets to match what the generator
  emits (visible in `out/MyPackage/linkedService/*.json`).
* **Edit JSON before deploy**: open each generated linked service and
  replace the placeholder `secretName` with the real one. This is a
  one-time edit per connection manager and is fully audit-trailed in your
  PR.

## Step 5 — deploy with non-destructive mode

If the customer has any *other* hand-edited linked services in the target
factory, deploy with `skip_if_exists=true` (H8) so you don't overwrite
their work:

```jsonc
// MCP deploy_to_adf args
{
  "artifacts_dir":    "out/MyPackage",
  "subscription_id":  "...",
  "resource_group":   "rg-data",
  "factory_name":     "adf-prod",
  "skip_if_exists":   true
}
```

## Step 6 — clean up

* `dtutil` working folder: `Remove-Item work -Recurse -Force`.
* Verify the unprotected copy is **not** in any git history
  (`git log --all -- "work\MyPackage.unprotected.dtsx"`).
* Rotate the original SSIS package password if it was reused for any other
  system.

## Quick checklist for reviewers

- [ ] Original `.dtsx` is the one converted; unprotected copy is throwaway.
- [ ] Every K-V secret name documented in a per-package map.
- [ ] `LS_KeyVault` exists in target factory and managed identity has
      `get` on secrets.
- [ ] `skip_if_exists=true` used on re-deploys.
- [ ] Working folder deleted; no plaintext secrets in PR.

## Related

* [SECURITY.md](SECURITY.md) — the agent's overall secret-handling policy.
* [BACKLOG.md](BACKLOG.md) #B3 — generator-side stripping of credential-shaped
  defaults.
* [BACKLOG.md](BACKLOG.md) #H8 — non-destructive deploy mode.
