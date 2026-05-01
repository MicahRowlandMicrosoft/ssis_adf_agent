"""
Connection resolver — maps SSIS Connection Managers to Fabric Connection
placeholders, and emits a `connections_required.json` manifest the deployer
fills in with real Fabric Connection GUIDs at deploy time.

Fabric Connections are workspace- or tenant-scoped objects identified by
GUID. They do not exist until provisioned, and they cannot be created from
inside a pipeline JSON the way ADF linked services can. Strategy:

  1. At convert time: emit a deterministic placeholder GUID for each unique
     SSIS Connection Manager. The placeholder is shaped like a real GUID
     (`00000000-0000-4000-8000-<12-hex>`) but starts with a recognizable
     all-zero prefix and uses a hash of the CM ID as the variable tail so
     it's both clearly a placeholder AND stable across runs (so re-running
     the converter doesn't churn diffs).

  2. At deploy time: the deployer reads `connections_required.json`,
     resolves each placeholder to a real Connection GUID (either by name
     lookup against the target workspace or via an explicit name→GUID map
     supplied by the caller), then substitutes throughout the pipeline JSON.

This split lets the convert step stay 100% offline and lets the deploy step
own all the Azure / Fabric API calls.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any

from ..parsers.models import ConnectionManagerType, SSISConnectionManager


# ---------------------------------------------------------------------------
# Placeholder GUID shaping
# ---------------------------------------------------------------------------
#
# Real Fabric Connection GUIDs are random version-4 UUIDs. We deliberately
# build placeholders that:
#   - have all-zero first three groups (visually distinct from real GUIDs)
#   - retain valid version-4 / variant-1 nibbles in groups 3 and 4 so JSON
#     consumers that schema-validate against the GUID grammar still accept
#     the value
#   - encode a 12-hex-char hash of the CM ID in the final group (stable +
#     unique across CMs)
_PLACEHOLDER_PREFIX = "00000000-0000-4000-8000-"


def make_placeholder_connection_id(cm_id: str) -> str:
    """Return a deterministic, recognizable placeholder GUID for *cm_id*."""
    if not cm_id:
        raise ValueError("cm_id must be non-empty")
    digest = hashlib.sha256(cm_id.encode("utf-8")).hexdigest()
    return _PLACEHOLDER_PREFIX + digest[:12]


# Maps SSIS connection-manager type to a Fabric connection-type label used
# in the manifest. Fabric's connection catalog uses these short names.
_CM_TYPE_TO_FABRIC: dict[ConnectionManagerType, str] = {
    ConnectionManagerType.OLEDB: "SQL",
    ConnectionManagerType.ADO_NET: "SQL",
    ConnectionManagerType.ODBC: "ODBC",
    ConnectionManagerType.FLAT_FILE: "AzureDataLakeStorage",
    ConnectionManagerType.EXCEL: "AzureDataLakeStorage",
    ConnectionManagerType.FILE: "AzureDataLakeStorage",
    ConnectionManagerType.MULTIFILE: "AzureDataLakeStorage",
    ConnectionManagerType.SMTP: "SMTP",
    ConnectionManagerType.HTTP: "Web",
    ConnectionManagerType.FTP: "FTP",
    ConnectionManagerType.MSOLAP: "Unsupported",
    ConnectionManagerType.UNKNOWN: "Unknown",
}


@dataclass
class ConnectionEntry:
    """One entry in the `connections_required.json` manifest."""

    placeholder_id: str
    ssis_cm_id: str
    ssis_cm_name: str
    fabric_connection_type: str
    server: str | None = None
    database: str | None = None
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "placeholder_id": self.placeholder_id,
            "ssis_connection_manager_id": self.ssis_cm_id,
            "ssis_connection_manager_name": self.ssis_cm_name,
            "fabric_connection_type": self.fabric_connection_type,
        }
        if self.server:
            out["server"] = self.server
        if self.database:
            out["database"] = self.database
        if self.notes:
            out["notes"] = list(self.notes)
        return out


class ConnectionResolver:
    """Tracks the SSIS CM → Fabric Connection placeholder mapping for one package."""

    def __init__(self) -> None:
        # Indexed by CM ID (the SSIS DTSID) so we de-dup correctly when the
        # same CM is referenced from multiple tasks.
        self._entries: dict[str, ConnectionEntry] = {}

    def register(self, cm: SSISConnectionManager) -> str:
        """Register *cm* and return its placeholder Connection GUID.

        Idempotent — calling with the same CM twice returns the same id.
        """
        if cm.id in self._entries:
            return self._entries[cm.id].placeholder_id

        placeholder = make_placeholder_connection_id(cm.id)
        notes: list[str] = []

        # On-prem hostnames can't be reached by Fabric without an OPDG (on-prem
        # data gateway). Surface that in the manifest so the deployer knows.
        server = cm.server
        if server and _looks_on_prem(server):
            notes.append(
                "On-prem host detected — Fabric requires an On-Premises Data "
                "Gateway. Configure the OPDG before resolving this connection."
            )
        if cm.type == ConnectionManagerType.UNKNOWN:
            notes.append(
                "SSIS connection manager type was UNKNOWN; verify the Fabric "
                "connection type before deploying."
            )

        entry = ConnectionEntry(
            placeholder_id=placeholder,
            ssis_cm_id=cm.id,
            ssis_cm_name=cm.name,
            fabric_connection_type=_CM_TYPE_TO_FABRIC.get(cm.type, "Unknown"),
            server=cm.server,
            database=cm.database,
            notes=notes,
        )
        self._entries[cm.id] = entry
        return placeholder

    def register_synthetic(
        self, key: str, fabric_type: str, note: str,
    ) -> str:
        """Register a synthetic connection not backed by an SSIS CM.

        Used for adapter-injected linked services like the AzureFunction LS
        wrapping Script Task → Function activity calls. *key* is a stable
        identifier (typically the ADF linked-service name) used both as the
        registry key and as the input to the placeholder GUID hash.
        """
        if key in self._entries:
            return self._entries[key].placeholder_id
        placeholder = make_placeholder_connection_id(f"__synthetic__:{key}")
        entry = ConnectionEntry(
            placeholder_id=placeholder,
            ssis_cm_id=f"__synthetic__:{key}",
            ssis_cm_name=key,
            fabric_connection_type=fabric_type,
            notes=[note],
        )
        self._entries[key] = entry
        return placeholder

    def get_placeholder_for_cm_name(self, cm_name: str) -> str | None:
        """Look up a placeholder by CM *name* (not ID). Returns None if unknown."""
        for entry in self._entries.values():
            if entry.ssis_cm_name == cm_name:
                return entry.placeholder_id
        return None

    def manifest(self) -> dict[str, Any]:
        """Return the `connections_required.json` document as a dict."""
        return {
            "schema_version": "1.0",
            "connections": [e.to_dict() for e in self._entries.values()],
        }

    def __len__(self) -> int:
        return len(self._entries)


# ---------------------------------------------------------------------------

_AZURE_DOMAINS = (
    ".windows.net",
    ".azure.com",
    ".azuresynapse.net",
    ".azuredatabricks.net",
)
_ON_PREM_TLDS = (".lcl", ".local", ".corp", ".lan", ".intra", ".internal", ".prv", ".priv")


def _looks_on_prem(host: str) -> bool:
    h = host.lower()
    if any(h.endswith(suffix) for suffix in _AZURE_DOMAINS):
        return False
    return any(tld in h for tld in _ON_PREM_TLDS)
