# EncryptAllWithPassword end-to-end recipe (M6)

> **🧯 Real failure walkthrough:** A captured first-time deploy of 6
> encrypted packages failed with
> `ManagedServiceIdentityCredentialNotFound` because the *factory's*
> system-assigned MI (not the deployer SP) had no role on the vault.
> See [docs/case-studies/first_deploy_keyvault_recovery/](docs/case-studies/first_deploy_keyvault_recovery/README.md)
> for the verbatim error, the 22 minutes spent on the wrong hypothesis,
> and the three commands that recovered. Read it before your first
> encrypted-package deploy.

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

> **Automation available (P4-4).** Steps 2 + 4 below can be run as a single
> command via the [`upload_encrypted_secrets`](#automation-via-mcp) MCP tool
> (also importable as `ssis_adf_agent.deployer.keyvault_uploader.process_encrypted_package`).
> The manual instructions below are still the source of truth — the
> automation does the same thing programmatically with `dry_run=True`
> support so you can preview before applying.

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

## Failure modes and how to read them

The parser does **not** decrypt `EncryptAllWithPassword` /
`EncryptSensitiveWithPassword` packages. There is no `--password` flag
and no attempt to derive the encryption key. As a consequence the
classic SSIS "wrong password / missing password / key derivation
failed" error messages do **not** appear here — encrypted content is
simply absent from the parsed tree. The downstream symptoms are below;
each one points at the source-of-truth location in the codebase.

| Symptom | What it means | Where it surfaces |
|---|---|---|
| Gap entry: *"Package uses ProtectionLevel=EncryptAllWithPassword. Sensitive values may not be readable without the password. Connection strings and credentials were likely not exported."* | The package itself is encrypted. Nothing was decrypted. | [`analyzers/gap_analyzer.py` `analyze_gaps()`](ssis_adf_agent/analyzers/gap_analyzer.py) — emitted at package level when `protection_level` is one of the two password-protected enums. Severity `WARNING`, recommendation: re-export with `DontSaveSensitive` *or* run the recipe above. |
| Generated linked service has an empty `connectionString` placeholder | Connection-manager `ConnectionString` was a sensitive property and was stripped at export time. | [`generators/linked_service_generator.py`](ssis_adf_agent/generators/linked_service_generator.py) — placeholders need to be filled by Step 4 of the recipe (or by `upload_encrypted_secrets`). |
| Pipeline parameter declared but `defaultValue` is missing | The parameter was marked `Sensitive="1"` in the .dtsx; the value was stripped. | [`generators/pipeline_generator.py` `_redact_sensitive_default()`](ssis_adf_agent/generators/pipeline_generator.py) — by design, even unencrypted packages strip credential-shaped defaults (B3). For encrypted packages there *is* no value to strip in the first place. |
| Script Task warning: *"may be encrypted with a package password (EncryptAllWithPassword)"* | The Script Task's binary blob was unreadable. The LLM translator gets `source_code=None` and produces only the comment-stub form. | [`converters/control_flow/script_task_converter.py`](ssis_adf_agent/converters/control_flow/script_task_converter.py) — emitted when `task.source_code` is `None`; lists this as the third of three real causes (the other two: unsupported VSTA layout, pre-2008 binary stub). |
| Gap entry references a connection that does **not** appear in `linked_services/` at all | The `<DTS:ConnectionManager>` element existed but its `ObjectData` was encrypted; the parser walked the tag but produced no `ConnectionManager` record. | [`parsers/ssis_parser.py`](ssis_adf_agent/parsers/ssis_parser.py) `_parse_connection_manager()` — empty/encrypted ObjectData yields no entry; downstream gap analyzer flags the orphaned task reference. Recovery: run the recipe to decrypt the .dtsx or supply the connection out-of-band. |

If you see a parser exception (rather than one of the symptoms above)
when handling an encrypted package, **the encryption is not the
cause** — file a bug per [SUPPORT.md](SUPPORT.md) with a sanitized
fixture. The parser is supposed to silently skip unreadable encrypted
content, not crash.

## Related

* [SECURITY.md](SECURITY.md) — the agent's overall secret-handling policy.
* [backlog.md](../development/backlog.md) #B3 — generator-side stripping of credential-shaped
  defaults.
* [backlog.md](../development/backlog.md) #H8 — non-destructive deploy mode.

---

## Automation via MCP

`upload_encrypted_secrets` (P4-4) automates Steps 2 + 4 of the recipe above.
Customers still run `dtutil` manually (Step 1) — the act of decrypting their
package stays auditable on their side. The tool ingests the unprotected
`.dtsx` they produce.

```jsonc
// MCP upload_encrypted_secrets args
{
  "unprotected_dtsx_path": "work/MyPackage.unprotected.dtsx",
  "package_name":          "MyPackage",
  "kv_url":                "https://kv-ssis.vault.azure.net/",
  "linked_service_dir":    "out/MyPackage/linkedService",
  "dry_run":               true
}
```

What it does in one shot:

1. Walks the unprotected `.dtsx` and pulls every `Password` property,
   embedded `Password=...` substring, and `Sensitive="1"` package /
   project parameter (values **never logged**).
2. Builds `{package}-{cm}-{kind}` style Key Vault secret names (slugified
   to KV's `[a-zA-Z0-9-]` charset).
3. Uploads each secret via `azure-keyvault-secrets`'s `SecretClient`,
   honoring the deploying identity's `DefaultAzureCredential`.
   - `overwrite=false` (default) skips secrets that already exist.
   - `dry_run=true` previews without touching Key Vault.
4. Walks every `*.json` under the linked-service directory and rewrites
   the placeholder `secretName` fields to point at the real secret names.

Returns an `UploadReport` with the secrets uploaded, secrets skipped (with
reasons), linked-service files rewritten, and the total `secretName`
references updated.

The same module is importable directly:

```python
from ssis_adf_agent.deployer.keyvault_uploader import (
    process_encrypted_package,
)

report = process_encrypted_package(
    unprotected_dtsx_path="work/MyPackage.unprotected.dtsx",
    package_name="MyPackage",
    kv_url="https://kv-ssis.vault.azure.net/",
    linked_service_dir="out/MyPackage/linkedService",
    dry_run=True,
)
print(report.to_dict())
```

For testing, pass a custom `secret_client` that implements `get_secret` and
`set_secret` (the module exposes a `SecretClientProtocol` runtime-checkable
Protocol).
