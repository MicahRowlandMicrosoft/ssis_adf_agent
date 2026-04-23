"""P4-4 — Encrypted-package secret upload + linked-service rewrite helper.

The end-to-end recipe in [ENCRYPTED_PACKAGES.md](../../ENCRYPTED_PACKAGES.md)
is six manual steps; for an estate of 50 encrypted packages, doing those
steps by hand is error-prone and a security-review hot spot. This module
automates the two longest steps:

* **Step 2** (push secrets to Key Vault) — extract secrets from each
  unprotected ``.dtsx`` and upload them via ``azure-keyvault-secrets``.
* **Step 4** (patch secret names) — rewrite the placeholder ``secretName``
  fields inside generated linked-service JSON to match the real Key Vault
  secret names produced in Step 2.

Customers still run ``dtutil`` manually (Step 1) so the act of decrypting
their packages stays auditable on their side. This module ingests the
unprotected ``.dtsx`` they produce, never decrypts anything itself.

Pure-Python boundaries:

* ``extract_secrets_from_dtsx`` is pure I/O on a local file; no network.
* ``build_secret_map`` is pure data transformation.
* ``upload_secrets`` is the only function that talks to Azure; it accepts
  an already-constructed ``SecretClient`` so tests can pass a fake.
* ``rewrite_linked_services`` is pure I/O on a local directory; no network.

Together they make the recipe a single command::

    process_encrypted_package(
        unprotected_dtsx_path="work/MyPackage.unprotected.dtsx",
        package_name="MyPackage",
        kv_url="https://kv-ssis.vault.azure.net/",
        linked_service_dir="out/MyPackage/linkedService",
    )

Returns an :class:`UploadReport` with the secrets uploaded, the linked
services rewritten, any conflicts, and a ``dry_run`` flag so customers can
preview the action before it touches Key Vault.
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Protocol, runtime_checkable
from xml.etree import ElementTree as ET

logger = logging.getLogger(__name__)

# DTS XML namespace used by SSIS 2017+; older packages also use this.
_DTS_NS = "www.microsoft.com/SqlServer/Dts"
_NSMAP = {"DTS": _DTS_NS}

# Regex for ``Password=...`` (or ``Pwd=...``) inside a connection string.
# Captures the value up to the next ``;`` or end-of-string.
_CONN_STR_PASSWORD_RE = re.compile(
    r"(?:^|;)\s*(?:Password|Pwd)\s*=\s*([^;]+)",
    flags=re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ExtractedSecret:
    """A single secret pulled out of an unprotected ``.dtsx``.

    Attributes
    ----------
    connection_manager_name
        ``DTS:ObjectName`` of the connection manager (or ``""`` for
        package-level secrets like ProjectParam passwords).
    kind
        ``"password"`` (the only kind the agent surfaces today). Reserved
        for future expansion (e.g. ``"sas_token"``, ``"oauth_secret"``).
    value
        The plaintext secret. **Never logged.**
    source
        Free-text breadcrumb for auditing (e.g. "ConnectionString.Password",
        "Properties.Password", "ProjectParam.MySecret").
    """
    connection_manager_name: str
    kind: str
    value: str
    source: str

    def __repr__(self) -> str:  # avoid leaking the value into logs
        return (
            f"ExtractedSecret(connection_manager_name={self.connection_manager_name!r}, "
            f"kind={self.kind!r}, value=<redacted>, source={self.source!r})"
        )


@dataclass(frozen=True)
class SecretMapping:
    """One Key Vault upload + linked-service rewrite instruction."""
    secret_name: str
    value: str
    placeholder_secret_name: str  # what the generator emitted; used for rewrite

    def __repr__(self) -> str:
        return (
            f"SecretMapping(secret_name={self.secret_name!r}, "
            f"value=<redacted>, placeholder_secret_name={self.placeholder_secret_name!r})"
        )


@dataclass
class UploadReport:
    """End-to-end result of :func:`process_encrypted_package`."""
    package_name: str
    kv_url: str
    dry_run: bool
    secrets_uploaded: list[str] = field(default_factory=list)  # secret names
    secrets_skipped: list[str] = field(default_factory=list)  # name -> reason via parallel list below
    skip_reasons: dict[str, str] = field(default_factory=dict)
    linked_services_rewritten: list[str] = field(default_factory=list)  # file paths
    rewrite_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "package_name": self.package_name,
            "kv_url": self.kv_url,
            "dry_run": self.dry_run,
            "secrets_uploaded": list(self.secrets_uploaded),
            "secrets_skipped": list(self.secrets_skipped),
            "skip_reasons": dict(self.skip_reasons),
            "linked_services_rewritten": list(self.linked_services_rewritten),
            "rewrite_count": self.rewrite_count,
        }


# ---------------------------------------------------------------------------
# SecretClient protocol — the subset of azure-keyvault-secrets we need.
# Lets tests pass a fake without depending on the SDK.
# ---------------------------------------------------------------------------

@runtime_checkable
class SecretClientProtocol(Protocol):
    def get_secret(self, name: str) -> Any: ...
    def set_secret(self, name: str, value: str) -> Any: ...


# ---------------------------------------------------------------------------
# Step 1: extract secrets from an unprotected .dtsx
# ---------------------------------------------------------------------------

def extract_secrets_from_dtsx(dtsx_path: str | Path) -> list[ExtractedSecret]:
    """Walk an unprotected ``.dtsx`` and pull out every secret-shaped value.

    Looks at:

    1. ``DTS:Property[@DTS:Name="Password"]`` directly under each
       ``DTS:ConnectionManager`` (most common for OLE DB / ADO.NET CMs).
    2. ``Password=...`` / ``Pwd=...`` substrings inside the
       ``ConnectionString`` property of each connection manager.
    3. ``DTS:Property[@Sensitive="1"]`` under ``DTS:PackageParameters`` /
       ``DTS:ProjectParameters`` (if present in the unprotected copy).

    Returns an empty list (not an error) if the file has no secrets — that
    is the expected outcome for many converted packages whose secrets live
    in project params instead.
    """
    p = Path(dtsx_path)
    text = p.read_text(encoding="utf-8")
    try:
        root = ET.fromstring(text)
    except ET.ParseError as exc:
        raise ValueError(f"{p} is not valid XML: {exc}") from exc

    found: list[ExtractedSecret] = []

    # 1 + 2: walk DTS:ConnectionManager nodes
    # SSIS nests an inner <DTS:ConnectionManager> (without ObjectName) under
    # <DTS:ObjectData> for inline data-binding; the outer CM's recursive
    # iteration already covers the inner Properties, so process only CMs
    # that carry an ObjectName to avoid double-counting.
    for cm in root.iter(f"{{{_DTS_NS}}}ConnectionManager"):
        cm_name = cm.attrib.get(f"{{{_DTS_NS}}}ObjectName") or ""
        if not cm_name:
            continue

        # Direct Password property
        for prop in cm.iter(f"{{{_DTS_NS}}}Property"):
            pname = prop.attrib.get(f"{{{_DTS_NS}}}Name")
            if pname == "Password":
                value = (prop.text or "").strip()
                if value:
                    found.append(
                        ExtractedSecret(
                            connection_manager_name=cm_name,
                            kind="password",
                            value=value,
                            source="Properties.Password",
                        )
                    )
            elif pname == "ConnectionString":
                cs = (prop.text or "").strip()
                m = _CONN_STR_PASSWORD_RE.search(cs)
                if m and m.group(1).strip():
                    found.append(
                        ExtractedSecret(
                            connection_manager_name=cm_name,
                            kind="password",
                            value=m.group(1).strip(),
                            source="ConnectionString.Password",
                        )
                    )

    # 3: package + project parameters marked Sensitive="1"
    for params_tag in (
        f"{{{_DTS_NS}}}PackageParameters",
        f"{{{_DTS_NS}}}ProjectParameters",
    ):
        for params in root.iter(params_tag):
            for param in params.iter(f"{{{_DTS_NS}}}PackageParameter"):
                param_name = param.attrib.get(f"{{{_DTS_NS}}}ObjectName") or ""
                is_sensitive = False
                value = ""
                for prop in param.iter(f"{{{_DTS_NS}}}Property"):
                    pname = prop.attrib.get(f"{{{_DTS_NS}}}Name")
                    if pname == "Sensitive" and (prop.text or "").strip() == "1":
                        is_sensitive = True
                    elif pname == "Value":
                        value = (prop.text or "").strip()
                if is_sensitive and value:
                    found.append(
                        ExtractedSecret(
                            connection_manager_name="",
                            kind="password",
                            value=value,
                            source=f"Parameter.{param_name}",
                        )
                    )

    logger.info(
        "extract_secrets_from_dtsx: %s -> %d secret(s) (values redacted)",
        p,
        len(found),
    )
    return found


# ---------------------------------------------------------------------------
# Step 2a: build the {placeholder_name -> real_name} map
# ---------------------------------------------------------------------------

DEFAULT_SECRET_NAME_TEMPLATE = "{package}-{cm}-{kind}"
"""Default template: ``MyPackage-CMSrc-password``."""


def build_secret_map(
    extracted: Iterable[ExtractedSecret],
    *,
    package_name: str,
    secret_name_template: str = DEFAULT_SECRET_NAME_TEMPLATE,
    placeholder_template: str = "{cm}-password",
) -> list[SecretMapping]:
    """Project an ``ExtractedSecret`` list into ``SecretMapping`` instances.

    ``placeholder_template`` must match the convention used by
    ``linked_service_generator._kv_secret_ref`` (currently
    ``"{cm}-password"``). When the generator's convention changes, update
    the default here too.
    """
    out: list[SecretMapping] = []
    for s in extracted:
        cm = s.connection_manager_name or "package"
        secret_name = _slugify(secret_name_template.format(
            package=package_name, cm=cm, kind=s.kind
        ))
        # Placeholder MUST match the literal string the generator wrote
        # into the linked-service JSON (no slugification — the generator
        # uses cm.name verbatim, including underscores).
        placeholder = placeholder_template.format(cm=cm, kind=s.kind)
        out.append(SecretMapping(
            secret_name=secret_name,
            value=s.value,
            placeholder_secret_name=placeholder,
        ))
    return out


def _slugify(name: str) -> str:
    """Make a string safe to use as a Key Vault secret name.

    KV secret names allow ``[a-zA-Z0-9-]`` only and must be 1-127 chars.
    Replace any other character with ``-`` and collapse runs.
    """
    cleaned = re.sub(r"[^a-zA-Z0-9-]+", "-", name).strip("-")
    if not cleaned:
        raise ValueError(f"Cannot slugify {name!r} into a Key Vault secret name.")
    return cleaned[:127]


# ---------------------------------------------------------------------------
# Step 2b: upload to Key Vault
# ---------------------------------------------------------------------------

def upload_secrets(
    client: SecretClientProtocol,
    mappings: Iterable[SecretMapping],
    *,
    dry_run: bool = False,
    overwrite: bool = False,
) -> tuple[list[str], dict[str, str]]:
    """Push each mapping to the Key Vault behind ``client``.

    Returns ``(uploaded_secret_names, {skipped_name: reason})``.

    * ``dry_run=True`` does not call ``set_secret`` or ``get_secret``;
      every mapping is reported as uploaded for preview purposes.
    * ``overwrite=False`` (default) skips secrets that already exist in the
      vault — safer for re-runs. Pass ``overwrite=True`` to force.
    """
    uploaded: list[str] = []
    skipped: dict[str, str] = {}

    for m in mappings:
        if dry_run:
            uploaded.append(m.secret_name)
            continue

        if not overwrite:
            try:
                client.get_secret(m.secret_name)
                # Already exists -> skip
                skipped[m.secret_name] = "already exists in Key Vault (pass overwrite=True to replace)"
                continue
            except Exception as exc:
                # Treat any "not found" path as "ok to upload"; SDK raises
                # ResourceNotFoundError but we don't import it to keep this
                # module SDK-import-free for tests.
                if "not found" not in str(exc).lower() and exc.__class__.__name__ != "ResourceNotFoundError":
                    skipped[m.secret_name] = f"get_secret raised: {exc.__class__.__name__}"
                    continue

        try:
            client.set_secret(m.secret_name, m.value)
            uploaded.append(m.secret_name)
            logger.info("Uploaded secret %s (value redacted)", m.secret_name)
        except Exception as exc:
            skipped[m.secret_name] = f"set_secret raised: {exc.__class__.__name__}: {exc}"
            logger.error("Failed to upload secret %s: %s", m.secret_name, exc)

    return uploaded, skipped


# ---------------------------------------------------------------------------
# Step 4: rewrite generated linked-service JSON
# ---------------------------------------------------------------------------

def rewrite_linked_services(
    linked_service_dir: str | Path,
    *,
    name_map: dict[str, str],
    dry_run: bool = False,
) -> tuple[list[str], int]:
    """Walk every ``*.json`` under ``linked_service_dir`` and replace any
    ``AzureKeyVaultSecret.secretName`` value found in ``name_map``.

    ``name_map`` is ``{placeholder_secret_name: real_secret_name}`` — i.e.
    the rewrite from what the generator emitted to what's actually in
    Key Vault.

    Returns ``(rewritten_file_paths, total_rewrite_count)``. A file appears
    in ``rewritten_file_paths`` only if at least one ``secretName`` inside
    it was changed. Files with no relevant references are silently
    skipped.

    ``dry_run=True`` does the rewrite in memory and counts what *would*
    change but does not write the file back.
    """
    d = Path(linked_service_dir)
    if not d.is_dir():
        raise ValueError(f"Linked-service directory not found: {d}")

    rewritten_files: list[str] = []
    total = 0

    for ls_file in sorted(d.glob("*.json")):
        try:
            data = json.loads(ls_file.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError(f"{ls_file} is not valid JSON: {exc}") from exc

        changed = _rewrite_secret_refs_in_node(data, name_map)
        if changed > 0:
            total += changed
            rewritten_files.append(str(ls_file))
            if not dry_run:
                ls_file.write_text(
                    json.dumps(data, indent=2, ensure_ascii=False) + "\n",
                    encoding="utf-8",
                )
            logger.info(
                "Rewrote %d secretName reference(s) in %s%s",
                changed,
                ls_file.name,
                " (dry-run)" if dry_run else "",
            )

    return rewritten_files, total


def _rewrite_secret_refs_in_node(node: Any, name_map: dict[str, str]) -> int:
    """Recursively walk a JSON node, rewriting AzureKeyVaultSecret.secretName.

    Returns the number of substitutions applied.
    """
    count = 0
    if isinstance(node, dict):
        # Detect the AzureKeyVaultSecret shape and rewrite secretName.
        if node.get("type") == "AzureKeyVaultSecret" and "secretName" in node:
            current = node["secretName"]
            replacement = name_map.get(current)
            if replacement and replacement != current:
                node["secretName"] = replacement
                count += 1
        for v in node.values():
            count += _rewrite_secret_refs_in_node(v, name_map)
    elif isinstance(node, list):
        for item in node:
            count += _rewrite_secret_refs_in_node(item, name_map)
    return count


# ---------------------------------------------------------------------------
# Top-level orchestrator
# ---------------------------------------------------------------------------

def process_encrypted_package(
    *,
    unprotected_dtsx_path: str | Path,
    package_name: str,
    kv_url: str,
    linked_service_dir: str | Path,
    secret_client: SecretClientProtocol | None = None,
    secret_name_template: str = DEFAULT_SECRET_NAME_TEMPLATE,
    placeholder_template: str = "{cm}-password",
    dry_run: bool = False,
    overwrite: bool = False,
) -> UploadReport:
    """End-to-end: extract -> map -> upload -> rewrite.

    ``secret_client`` defaults to a real ``azure.keyvault.secrets.SecretClient``
    using ``DefaultAzureCredential`` if ``None`` is passed. Tests should
    always pass a fake.
    """
    extracted = extract_secrets_from_dtsx(unprotected_dtsx_path)
    mappings = build_secret_map(
        extracted,
        package_name=package_name,
        secret_name_template=secret_name_template,
        placeholder_template=placeholder_template,
    )

    if secret_client is None:
        secret_client = _default_secret_client(kv_url)

    uploaded, skipped = upload_secrets(
        secret_client, mappings, dry_run=dry_run, overwrite=overwrite,
    )

    name_map = {m.placeholder_secret_name: m.secret_name for m in mappings}
    rewritten, count = rewrite_linked_services(
        linked_service_dir, name_map=name_map, dry_run=dry_run,
    )

    return UploadReport(
        package_name=package_name,
        kv_url=kv_url,
        dry_run=dry_run,
        secrets_uploaded=uploaded,
        secrets_skipped=list(skipped),
        skip_reasons=skipped,
        linked_services_rewritten=rewritten,
        rewrite_count=count,
    )


def _default_secret_client(kv_url: str) -> SecretClientProtocol:
    """Lazy import so ``azure-keyvault-secrets`` is only required when used."""
    try:
        from azure.identity import DefaultAzureCredential
        from azure.keyvault.secrets import SecretClient
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "azure-keyvault-secrets and azure-identity are required to upload "
            "secrets. Install with `pip install azure-keyvault-secrets "
            "azure-identity`, or pass a custom secret_client."
        ) from exc
    return SecretClient(vault_url=kv_url, credential=DefaultAzureCredential())
