"""
Centralized ADF artifact naming conventions.

Default naming patterns
-----------------------
- Linked Service : ``LS_{Package}_{TypeShort}_{Server}_{Database}``
- Dataset        : ``DS_{Package}_{ComponentName}``
- Data Flow      : ``DF_{Package}_{TaskName}``
- Pipeline       : ``PL_{Package}``
- Trigger        : ``TR_{Package}``

Customizing naming (N3)
-----------------------
Two layers of customization are supported through the ``name_overrides``
mapping accepted by every helper in this module:

1. **Per-artifact overrides** — exact-name replacement for one entity.
   * ``LS:<connection_manager_name>`` -> custom linked-service name
   * ``DS:<component_name>`` -> custom dataset name
   * ``DF:<task_name>`` -> custom data-flow name
   * ``PL`` -> custom pipeline name (one per package conversion)
   * ``TR`` -> custom trigger name

2. **Prefix overrides** — change the standard ``LS_`` / ``DS_`` / ``DF_`` /
   ``PL_`` / ``TR_`` prefix for *every* artifact of that kind:
   * ``LS_PREFIX``, ``DS_PREFIX``, ``DF_PREFIX``, ``PL_PREFIX``, ``TR_PREFIX``
   Use empty string ("") to drop the prefix entirely. Combine with
   per-artifact overrides for the rare exception.

When the server or database is a placeholder (TODO / Insert_*), the linked
service falls back to the SSIS Connection Manager name.

All generated names are passed through :func:`sanitize_adf_name` which strips
characters invalid in ADF identifiers (only ``[A-Za-z0-9_]`` allowed) and
collapses consecutive underscores.

Collision safety
----------------
If two connection managers in the same package produce the same base linked
service name, a short hash of the CM ID is appended to disambiguate.
"""
from __future__ import annotations

import hashlib
import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..parsers.models import ConnectionManagerType, SSISConnectionManager

# ---------------------------------------------------------------------------
# Core sanitizer
# ---------------------------------------------------------------------------

def sanitize_adf_name(raw: str) -> str:
    """Sanitize *raw* into a valid ADF artifact name.

    ADF names must contain only letters, digits, and underscores.
    Hyphens and other invalid characters are replaced with underscores,
    and consecutive underscores are collapsed.
    """
    cleaned = re.sub(r"[^A-Za-z0-9_]", "_", raw)
    cleaned = re.sub(r"_+", "_", cleaned)
    return cleaned.strip("_")


def _resolve_prefix(
    kind: str,
    default: str,
    name_overrides: dict[str, str] | None,
) -> str:
    """Return the configured prefix for *kind* (LS/DS/DF/PL/TR), or *default*.

    Looks up ``{kind}_PREFIX`` in *name_overrides* (case-insensitive). An
    explicit empty string drops the prefix entirely. The returned prefix is
    sanitized only for ADF-invalid characters; trailing underscores are
    preserved so callers control separator style.
    """
    if not name_overrides:
        return default
    target = f"{kind}_PREFIX".upper()
    for k, v in name_overrides.items():
        if k.upper() == target:
            # Allow explicit "" to mean "no prefix".
            if v == "":
                return ""
            # Sanitize but preserve a trailing underscore if the caller wrote one.
            trailing = "_" if v.endswith("_") else ""
            return sanitize_adf_name(v) + trailing
    return default


# ---------------------------------------------------------------------------
# Connection-manager type → short label
# ---------------------------------------------------------------------------

# Imported lazily to avoid circular imports at module level
_CM_TYPE_SHORT: dict[str, str] = {
    "OLEDB": "Sql",
    "ADO.NET": "Sql",
    "ODBC": "Sql",
    "FLATFILE": "FlatFile",
    "EXCEL": "Excel",
    "FTP": "Ftp",
    "HTTP": "Http",
    "SMTP": "Smtp",
    "FILE": "File",
    "MULTIFILE": "File",
    "MSOLAP100": "OLAP",
    "Unknown": "Unknown",
}

# Placeholder tokens in server / database fields that indicate the value
# hasn't been filled in yet.
_PLACEHOLDER_RE = re.compile(
    r"(^TODO|Insert_|Insert\s|_Here$|^unknown$)",
    re.IGNORECASE,
)


def _is_placeholder(value: str | None) -> bool:
    """Return True if *value* looks like a TODO placeholder."""
    if not value:
        return True
    return bool(_PLACEHOLDER_RE.search(value))


# ---------------------------------------------------------------------------
# Linked-service naming
# ---------------------------------------------------------------------------

def ls_name_for_cm(
    package_name: str,
    cm: SSISConnectionManager,
    *,
    name_overrides: dict[str, str] | None = None,
) -> str:
    """Build a human-readable linked-service name for *cm*.

    Pattern: ``{LS_PREFIX}{Package}_{TypeShort}_{Server}_{Database}``
    (default ``LS_PREFIX`` is ``LS_``).

    Falls back to the CM name when server/database are placeholders.
    """
    prefix = _resolve_prefix("LS", "LS_", name_overrides)
    pkg = sanitize_adf_name(package_name)
    type_short = _CM_TYPE_SHORT.get(cm.type.value, "Unknown")

    server_raw = cm.server or ""
    database_raw = cm.database or ""

    if _is_placeholder(server_raw) and _is_placeholder(database_raw):
        # Fallback: use connection-manager name
        semantic = sanitize_adf_name(cm.name)
    else:
        parts: list[str] = []
        if not _is_placeholder(server_raw):
            # Use just the hostname (strip domain suffix)
            host = server_raw.split(".")[0].split(",")[0]
            parts.append(sanitize_adf_name(host))
        if not _is_placeholder(database_raw):
            parts.append(sanitize_adf_name(database_raw))
        semantic = "_".join(parts) if parts else sanitize_adf_name(cm.name)

    return f"{prefix}{pkg}_{type_short}_{semantic}"


def build_ls_name_map(
    package_name: str,
    connection_managers: list[SSISConnectionManager],
    *,
    name_overrides: dict[str, str] | None = None,
) -> dict[str, str]:
    """Build a mapping from CM ID → canonical linked-service name.

    Handles collisions by appending a 4-char hash of the CM ID when two CMs
    in the same package produce the same base name.

    If *name_overrides* contains a key ``LS:<cm_name>`` (case-insensitive
    match on the connection-manager name), that value is used as-is
    (after sanitization) instead of the auto-generated name.
    """
    overrides = name_overrides or {}
    # Build case-insensitive lookup for LS: prefixed overrides
    _ls_overrides: dict[str, str] = {}
    for key, val in overrides.items():
        if key.upper().startswith("LS:"):
            cm_name_key = key[3:].strip().lower()
            _ls_overrides[cm_name_key] = sanitize_adf_name(val)

    name_map: dict[str, str] = {}
    # First pass: compute raw names (skip overridden CMs)
    raw_names: list[tuple[str, str]] = []  # (cm_id, proposed_name)
    for cm in connection_managers:
        override = _ls_overrides.get((cm.name or "").lower())
        if override:
            name_map[cm.id] = override
        else:
            raw_names.append((cm.id, ls_name_for_cm(package_name, cm, name_overrides=overrides)))

    # Detect collisions among non-overridden names
    from collections import Counter
    name_counts = Counter(name for _, name in raw_names)

    for cm_id, proposed in raw_names:
        if name_counts[proposed] > 1:
            # Disambiguate with short hash
            short_hash = hashlib.sha256(cm_id.encode()).hexdigest()[:4]
            final = f"{proposed}_{short_hash}"
        else:
            final = proposed
        name_map[cm_id] = final

    return name_map


def resolve_ls_name(
    cm_id: str,
    ls_name_map: dict[str, str] | None = None,
) -> str:
    """Look up the linked-service name for *cm_id*.

    When *ls_name_map* is provided, returns the mapped name.
    Falls back to ``LS_{sanitized_id}`` for backward compatibility.
    """
    if ls_name_map and cm_id in ls_name_map:
        return ls_name_map[cm_id]
    return f"LS_{sanitize_adf_name(cm_id)}"


# ---------------------------------------------------------------------------
# Dataset / Data Flow / Pipeline / Trigger naming
# ---------------------------------------------------------------------------

def ds_name(
    package_name: str,
    component_name: str,
    *,
    name_overrides: dict[str, str] | None = None,
) -> str:
    """Dataset name: ``DS_{Package}_{ComponentName}``.

    Honors ``DS:<component_name>`` in *name_overrides*.
    """
    if name_overrides:
        key = f"DS:{component_name}".lower()
        for k, v in name_overrides.items():
            if k.lower() == key:
                return sanitize_adf_name(v)
    prefix = _resolve_prefix("DS", "DS_", name_overrides)
    return f"{prefix}{sanitize_adf_name(package_name)}_{sanitize_adf_name(component_name)}"


def df_name(
    package_name: str,
    task_name: str,
    *,
    name_overrides: dict[str, str] | None = None,
) -> str:
    """Data Flow name: ``DF_{Package}_{TaskName}``.

    Honors ``DF:<task_name>`` in *name_overrides*.
    """
    if name_overrides:
        key = f"DF:{task_name}".lower()
        for k, v in name_overrides.items():
            if k.lower() == key:
                return sanitize_adf_name(v)
    prefix = _resolve_prefix("DF", "DF_", name_overrides)
    return f"{prefix}{sanitize_adf_name(package_name)}_{sanitize_adf_name(task_name)}"


def pl_name(
    package_name: str,
    prefix: str = "PL_",
    *,
    name_overrides: dict[str, str] | None = None,
) -> str:
    """Pipeline name: ``PL_{Package}``.

    Honors ``PL`` in *name_overrides*.
    """
    if name_overrides:
        for k, v in name_overrides.items():
            if k.upper() == "PL":
                return sanitize_adf_name(v)
        configured = _resolve_prefix("PL", prefix, name_overrides)
        return f"{configured}{sanitize_adf_name(package_name)}"
    return f"{prefix}{sanitize_adf_name(package_name)}"


def tr_name(
    package_name: str,
    *,
    name_overrides: dict[str, str] | None = None,
) -> str:
    """Trigger name: ``TR_{Package}``.

    Honors ``TR`` in *name_overrides*.
    """
    if name_overrides:
        for k, v in name_overrides.items():
            if k.upper() == "TR":
                return sanitize_adf_name(v)
    prefix = _resolve_prefix("TR", "TR_", name_overrides)
    return f"{prefix}{sanitize_adf_name(package_name)}"
