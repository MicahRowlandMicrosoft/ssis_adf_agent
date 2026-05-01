"""
Deeper deploy dry-run / pre-flight (P4-6).

The SDK's ``dry_run`` mode validates JSON shape only. Real customer
deployments fail on:

* Key Vault secret missing / spelled wrong / wrong vault.
* Deploying identity has no ``get`` permission on the vault.
* Linked-service host is unreachable from the SHIR / Azure IR (DNS or
  firewall).
* Managed identity not granted on the destination data store.
* Subscription / region quota for Mapping Data Flows exhausted.

This module walks the *generated* linked-service JSON (no Azure call
required to enumerate) and runs a configurable set of probes against
each external dependency. Network / Azure SDK calls are funneled through
small Protocols so the unit tests pass fakes — no real network in CI.

Public surface:

* :class:`PreflightCheck` — one finding (kind / target / status / message).
* :class:`PreflightReport` — collected findings + counts + ``to_dict()``.
* :func:`extract_dependencies` — pure-Python scan of an artifacts dir.
* :func:`run_preflight` — orchestrator. Accepts injectable
  ``secret_client_factory`` / ``dns_resolver`` / ``credential`` /
  ``factory_probe`` so callers (and tests) can swap any boundary.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable, Protocol, runtime_checkable

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PreflightCheck:
    """One probe result."""
    kind: str        # "kv_secret" | "host_dns" | "mi_token" | "factory_reach"
    target: str
    status: str      # "pass" | "fail" | "warn" | "skipped"
    message: str
    detail: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class PreflightReport:
    """Aggregate of all checks for one artifacts directory."""
    artifacts_dir: str
    factory_resource_id: str
    checks: list[PreflightCheck] = field(default_factory=list)
    counts: dict[str, int] = field(default_factory=dict)

    def add(self, check: PreflightCheck) -> None:
        self.checks.append(check)
        self.counts[check.status] = self.counts.get(check.status, 0) + 1

    @property
    def has_failures(self) -> bool:
        return self.counts.get("fail", 0) > 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifacts_dir": self.artifacts_dir,
            "factory_resource_id": self.factory_resource_id,
            "counts": dict(self.counts),
            "has_failures": self.has_failures,
            "checks": [c.to_dict() for c in self.checks],
        }


# ---------------------------------------------------------------------------
# Dependency types extracted from artifacts
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class KvSecretRef:
    """A ``{type:'AzureKeyVaultSecret', secretName: '...'}`` node, resolved."""
    vault_url: str             # Resolved baseUrl from the KV linked service
    secret_name: str
    referenced_by: str         # Linked-service file name doing the referencing
    kv_linked_service: str     # The AzureKeyVault linked-service name


@dataclass(frozen=True)
class HostTarget:
    """A host (FQDN / IP) parsed from a linked-service connection string."""
    host: str
    referenced_by: str
    raw_property: str  # which JSON property the host came from (debug aid)


@dataclass(frozen=True)
class ExtractedDependencies:
    kv_secrets: list[KvSecretRef]
    hosts: list[HostTarget]
    kv_linked_services: dict[str, str]  # name -> vault baseUrl


# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------


_HOST_PROPERTIES = (
    "server", "host", "endpoint", "url", "serverName", "accountEndpoint",
    "fullyQualifiedDomainName", "endPoint",
)
_HOST_FROM_CONNSTR_RE = re.compile(
    r"(?:Server|Data Source|Host|Endpoint)\s*=\s*(?:tcp:)?([^;,\s]+)",
    re.IGNORECASE,
)


def _walk_kv_refs(node: Any, owner: str, sink: list[KvSecretRef],
                  kv_index: dict[str, str]) -> None:
    if isinstance(node, dict):
        if (
            node.get("type") == "AzureKeyVaultSecret"
            and isinstance(node.get("store"), dict)
            and node.get("secretName")
        ):
            ls_name = node["store"].get("referenceName") or ""
            vault_url = kv_index.get(ls_name, "")
            sink.append(KvSecretRef(
                vault_url=vault_url,
                secret_name=str(node["secretName"]),
                referenced_by=owner,
                kv_linked_service=ls_name,
            ))
        for v in node.values():
            _walk_kv_refs(v, owner, sink, kv_index)
    elif isinstance(node, list):
        for v in node:
            _walk_kv_refs(v, owner, sink, kv_index)


def _walk_hosts(node: Any, owner: str, sink: list[HostTarget]) -> None:
    if isinstance(node, dict):
        for k, v in node.items():
            if k in _HOST_PROPERTIES and isinstance(v, str) and v.strip():
                sink.append(HostTarget(
                    host=_host_from_url_or_value(v),
                    referenced_by=owner, raw_property=k,
                ))
            elif k == "connectionString" and isinstance(v, str):
                m = _HOST_FROM_CONNSTR_RE.search(v)
                if m:
                    sink.append(HostTarget(
                        host=_host_from_url_or_value(m.group(1)),
                        referenced_by=owner, raw_property=k,
                    ))
            else:
                _walk_hosts(v, owner, sink)
    elif isinstance(node, list):
        for v in node:
            _walk_hosts(v, owner, sink)


def _host_from_url_or_value(raw: str) -> str:
    """Strip scheme + path + port from a connection-string value."""
    s = raw.strip()
    # Strip URL scheme.
    if "://" in s:
        s = s.split("://", 1)[1]
    # Strip path.
    s = s.split("/", 1)[0]
    # Strip port.
    s = s.split(":", 1)[0].split(",", 1)[0]
    # Strip surrounding angle brackets / placeholders.
    return s.strip().strip("<>")


def extract_dependencies(artifacts_dir: Path) -> ExtractedDependencies:
    """Walk every linked-service JSON under *artifacts_dir* and return the
    Key Vault refs + host targets we'd need to reach from the factory."""
    ls_dir = artifacts_dir / "linkedService"
    if not ls_dir.is_dir():
        return ExtractedDependencies(kv_secrets=[], hosts=[],
                                     kv_linked_services={})

    # Pass 1: index AzureKeyVault linked services so we can resolve baseUrl.
    kv_index: dict[str, str] = {}
    files: list[tuple[str, dict[str, Any]]] = []
    for f in sorted(ls_dir.glob("*.json")):
        try:
            payload = json.loads(f.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        files.append((f.name, payload))
        props = (payload.get("properties") or {})
        if props.get("type") == "AzureKeyVault":
            base = ((props.get("typeProperties") or {}).get("baseUrl") or "")
            kv_index[payload.get("name", f.stem)] = str(base)

    # Pass 2: walk for refs + hosts.
    kv_secrets: list[KvSecretRef] = []
    hosts: list[HostTarget] = []
    for name, payload in files:
        owner = payload.get("name") or name
        type_props = (payload.get("properties") or {}).get("typeProperties") or {}
        _walk_kv_refs(type_props, owner, kv_secrets, kv_index)
        # Skip host extraction for AzureKeyVault itself — its baseUrl is the
        # vault, already represented by the KV checks.
        if (payload.get("properties") or {}).get("type") != "AzureKeyVault":
            _walk_hosts(type_props, owner, hosts)

    return ExtractedDependencies(
        kv_secrets=kv_secrets,
        hosts=hosts,
        kv_linked_services=kv_index,
    )


# ---------------------------------------------------------------------------
# Probe protocols (kept small so tests can stub them)
# ---------------------------------------------------------------------------


@runtime_checkable
class _SecretGetter(Protocol):
    def get_secret(self, name: str) -> Any: ...


SecretClientFactory = Callable[[str], _SecretGetter]
"""Given a vault URL, return an object exposing ``get_secret(name)``."""

DnsResolver = Callable[[str], list[str]]
"""Given a host, return a list of resolved IPs (empty list = unresolved)."""


@runtime_checkable
class _Credential(Protocol):
    def get_token(self, *scopes: str) -> Any: ...


# ---------------------------------------------------------------------------
# Default real-Azure factories (lazy imports so the test path stays clean)
# ---------------------------------------------------------------------------


def _default_secret_client_factory(vault_url: str) -> _SecretGetter:
    from azure.identity import DefaultAzureCredential
    from azure.keyvault.secrets import SecretClient
    return SecretClient(vault_url=vault_url, credential=DefaultAzureCredential())


def _default_dns_resolver(host: str) -> list[str]:
    import socket
    try:
        infos = socket.getaddrinfo(host, None)
    except OSError:
        return []
    out: list[str] = []
    seen: set[str] = set()
    for info in infos:
        addr = info[4][0]
        if addr not in seen:
            seen.add(addr)
            out.append(addr)
    return out


def _default_credential() -> _Credential:
    from azure.identity import DefaultAzureCredential
    return DefaultAzureCredential()


# ---------------------------------------------------------------------------
# Probes (pure functions on the injected boundaries)
# ---------------------------------------------------------------------------


def _probe_kv_secret(
    ref: KvSecretRef,
    secret_client_factory: SecretClientFactory,
) -> PreflightCheck:
    if not ref.vault_url:
        return PreflightCheck(
            kind="kv_secret",
            target=f"{ref.kv_linked_service}::{ref.secret_name}",
            status="fail",
            message=(
                f"Linked service '{ref.kv_linked_service}' referenced from "
                f"{ref.referenced_by} could not be resolved to a Key Vault "
                f"baseUrl. Check the AzureKeyVault linked-service JSON exists "
                f"and has typeProperties.baseUrl set."
            ),
        )
    try:
        client = secret_client_factory(ref.vault_url)
        client.get_secret(ref.secret_name)
    except Exception as exc:  # noqa: BLE001 — surface to caller
        cls = exc.__class__.__name__
        msg = str(exc) or cls
        # Heuristic classification of forbidden vs missing.
        lower = msg.lower()
        if "forbidden" in lower or "denied" in lower or "401" in lower or "403" in lower:
            status = "fail"
            note = (
                "Deploying identity does not have 'get' permission on the "
                "vault. Grant the Key Vault Secrets User RBAC role or the "
                "data-plane 'get' permission via access policy."
            )
        elif "not found" in lower or "secretnotfound" in lower or cls == "ResourceNotFoundError":
            status = "fail"
            note = (
                "Secret does not exist in the vault. Run "
                "upload_encrypted_secrets to publish it, or correct the "
                "secretName in the linked-service JSON."
            )
        else:
            status = "warn"
            note = "Probe error did not match a known category — review manually."
        return PreflightCheck(
            kind="kv_secret",
            target=f"{ref.vault_url}/secrets/{ref.secret_name}",
            status=status,
            message=f"{cls}: {msg}. {note}",
            detail={"referenced_by": ref.referenced_by},
        )
    return PreflightCheck(
        kind="kv_secret",
        target=f"{ref.vault_url}/secrets/{ref.secret_name}",
        status="pass",
        message="Secret resolved.",
        detail={"referenced_by": ref.referenced_by},
    )


def _probe_host_dns(
    target: HostTarget,
    dns_resolver: DnsResolver,
) -> PreflightCheck:
    host = target.host
    if not host or "@" in host or "{" in host or "$" in host:
        return PreflightCheck(
            kind="host_dns",
            target=host or "(empty)",
            status="skipped",
            message=(
                "Host appears templated / parameterized — DNS check skipped. "
                "Resolve manually after parameter substitution."
            ),
            detail={"referenced_by": target.referenced_by,
                    "property": target.raw_property},
        )
    try:
        addrs = dns_resolver(host)
    except Exception as exc:  # noqa: BLE001
        return PreflightCheck(
            kind="host_dns",
            target=host,
            status="warn",
            message=f"DNS resolver raised {exc.__class__.__name__}: {exc}",
            detail={"referenced_by": target.referenced_by,
                    "property": target.raw_property},
        )
    if not addrs:
        return PreflightCheck(
            kind="host_dns",
            target=host,
            status="fail",
            message=(
                "Host did not resolve. Verify the FQDN is correct and that "
                "the SHIR / Azure IR can reach it (private DNS / firewall)."
            ),
            detail={"referenced_by": target.referenced_by,
                    "property": target.raw_property},
        )
    return PreflightCheck(
        kind="host_dns",
        target=host,
        status="pass",
        message=f"Resolved to {len(addrs)} address(es).",
        detail={"referenced_by": target.referenced_by,
                "property": target.raw_property,
                "addresses": addrs},
    )


def _probe_mi_token(credential: _Credential) -> PreflightCheck:
    """Try to fetch an ARM token; surface auth-config failures early."""
    try:
        token = credential.get_token("https://management.azure.com/.default")
    except Exception as exc:  # noqa: BLE001
        return PreflightCheck(
            kind="mi_token",
            target="https://management.azure.com/.default",
            status="fail",
            message=(
                f"{exc.__class__.__name__}: {exc}. The deploying identity "
                "could not acquire a token for ARM. Run `az login` "
                "interactively, or check workload-identity / service-principal "
                "env vars."
            ),
        )
    expires = getattr(token, "expires_on", None)
    return PreflightCheck(
        kind="mi_token",
        target="https://management.azure.com/.default",
        status="pass",
        message="Token acquired.",
        detail={"expires_on": expires},
    )


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def _factory_resource_id(
    subscription_id: str, resource_group: str, factory_name: str,
) -> str:
    return (
        f"/subscriptions/{subscription_id}"
        f"/resourceGroups/{resource_group}"
        f"/providers/Microsoft.DataFactory/factories/{factory_name}"
    )


def run_preflight(
    *,
    artifacts_dir: Path | str,
    subscription_id: str = "",
    resource_group: str = "",
    factory_name: str = "",
    secret_client_factory: SecretClientFactory | None = None,
    dns_resolver: DnsResolver | None = None,
    credential: _Credential | None = None,
    skip_kv: bool = False,
    skip_dns: bool = False,
    skip_mi_token: bool = False,
) -> PreflightReport:
    """Walk *artifacts_dir* and run the configured probes.

    Every probe boundary is injectable so tests pass stubs instead of hitting
    Azure / DNS. When a boundary is omitted, the corresponding lazy default
    will be used (and will require the matching SDK / network).

    Args:
        artifacts_dir: Directory of generated artifacts (must contain a
            ``linkedService`` subdir).
        subscription_id / resource_group / factory_name: Used only to build
            the factory ARM id surfaced in the report.
        secret_client_factory: ``vault_url -> SecretClient``. Defaults to a
            real ``azure-keyvault-secrets`` client.
        dns_resolver: ``host -> list[ip]``. Defaults to ``socket.getaddrinfo``.
        credential: Object exposing ``get_token(*scopes)``. Defaults to
            ``DefaultAzureCredential``.
        skip_*: Disable the matching probe class; useful in air-gapped envs.
    """
    artifacts_dir = Path(artifacts_dir)
    if not artifacts_dir.is_dir():
        raise FileNotFoundError(f"artifacts_dir not found: {artifacts_dir}")

    deps = extract_dependencies(artifacts_dir)

    report = PreflightReport(
        artifacts_dir=str(artifacts_dir),
        factory_resource_id=_factory_resource_id(
            subscription_id, resource_group, factory_name,
        ),
    )

    # MI token first — if this fails, all downstream Azure-bound checks will
    # too, and the message is much more actionable upstream.
    if not skip_mi_token:
        cred = credential or _default_credential()
        report.add(_probe_mi_token(cred))

    if not skip_kv:
        factory = secret_client_factory or _default_secret_client_factory
        for ref in deps.kv_secrets:
            report.add(_probe_kv_secret(ref, factory))

    if not skip_dns:
        resolver = dns_resolver or _default_dns_resolver
        # De-dupe identical hosts so we don't probe one host five times.
        seen: set[str] = set()
        for h in deps.hosts:
            if h.host in seen:
                continue
            seen.add(h.host)
            report.add(_probe_host_dns(h, resolver))

    return report


__all__ = [
    "ExtractedDependencies",
    "HostTarget",
    "KvSecretRef",
    "PreflightCheck",
    "PreflightReport",
    "extract_dependencies",
    "run_preflight",
]
