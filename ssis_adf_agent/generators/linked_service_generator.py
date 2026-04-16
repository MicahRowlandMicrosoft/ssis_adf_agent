"""
Linked Service generator — emits one ADF linkedService.json per SSIS Connection Manager.

Uses Microsoft Recommended version format for Azure SQL Database linked services
(discrete server/database/authenticationType properties, TLS 1.3 support) instead
of the legacy connectionString format.

Supports:
  - Self-Hosted IR detection for on-prem connections
  - Key Vault secret references
  - SystemAssignedManagedIdentity, ServicePrincipal, and SQL auth
  - Azure Blob Storage with connection string, account key, or SAS token
  - Cross-package deduplication via shared_artifacts_dir
  - Connection string component extraction
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from ..parsers.models import ConnectionManagerType, SSISConnectionManager, SSISPackage
from ..warnings_collector import warn


_DEFAULT_IR = "AutoResolveIntegrationRuntime"

# Cloud suffixes for Azure services — connections to these don't need SHIR
_AZURE_SUFFIXES = (
    ".database.windows.net",
    ".sql.azuresynapse.net",
    ".documents.azure.com",
    ".cosmos.azure.com",
    ".core.windows.net",
    ".blob.core.windows.net",
    ".dfs.core.windows.net",
    ".table.core.windows.net",
    ".queue.core.windows.net",
    ".file.core.windows.net",
    ".azurehdinsight.net",
    ".servicebus.windows.net",
    ".azuredatabricks.net",
    ".mysql.database.azure.com",
    ".postgres.database.azure.com",
    ".mariadb.database.azure.com",
    ".redis.cache.windows.net",
)


# ---------------------------------------------------------------------------
# Connection string parsing
# ---------------------------------------------------------------------------

def parse_connection_string(cs: str | None) -> dict[str, str]:
    """
    Parse a semicolon-delimited connection string into a dict of components.

    Handles OLE DB, ADO.NET, and Azure Storage connection strings.
    Keys are normalised to lower-case for consistent lookup.

    Correctly handles values wrapped in single or double quotes, so that
    semicolons embedded in quoted passwords (e.g. ``Password="a;b"``) are
    not treated as delimiters.

    Examples::

        "Server=mysvr;Database=mydb;User ID=sa;Password=secret"
        "Server=mysvr;Password=\"p;wd\";Database=mydb"
        "DefaultEndpointsProtocol=https;AccountName=act;AccountKey=k;..."
    """
    if not cs:
        return {}
    parts: dict[str, str] = {}
    # Tokenise respecting quoted segments
    segments = _split_connection_string(cs)
    for segment in segments:
        segment = segment.strip()
        if "=" not in segment:
            continue
        key, _, value = segment.partition("=")
        # Strip surrounding quotes from values if present
        value = value.strip()
        if len(value) >= 2 and value[0] in ('"', "'") and value[-1] == value[0]:
            value = value[1:-1]
        parts[key.strip().lower()] = value
    return parts


def _split_connection_string(cs: str) -> list[str]:
    """Split a connection string on semicolons, respecting quoted values.

    Quotes (single or double) protect embedded semicolons.  A backslash does
    **not** act as an escape character — OLE DB / ADO.NET connection strings
    use doubled quotes for escaping, which this routine handles transparently.
    """
    segments: list[str] = []
    current: list[str] = []
    in_quote: str | None = None

    for ch in cs:
        if in_quote is not None:
            current.append(ch)
            if ch == in_quote:
                in_quote = None
        elif ch in ('"', "'"):
            in_quote = ch
            current.append(ch)
        elif ch == ";":
            segments.append("".join(current))
            current = []
        else:
            current.append(ch)

    if current:
        segments.append("".join(current))
    return segments


# Key aliases in connection strings
_SERVER_KEYS = ("server", "data source", "host", "addr", "address", "network address")
_DB_KEYS = ("database", "initial catalog")
_USER_KEYS = ("user id", "uid", "user", "username")
_PWD_KEYS = ("password", "pwd")


def _extract_server(cs_parts: dict[str, str]) -> str | None:
    for k in _SERVER_KEYS:
        if k in cs_parts:
            return cs_parts[k]
    return None


def _extract_database(cs_parts: dict[str, str]) -> str | None:
    for k in _DB_KEYS:
        if k in cs_parts:
            return cs_parts[k]
    return None


def _extract_user(cs_parts: dict[str, str]) -> str | None:
    for k in _USER_KEYS:
        if k in cs_parts:
            return cs_parts[k]
    return None


def _extract_password(cs_parts: dict[str, str]) -> str | None:
    for k in _PWD_KEYS:
        if k in cs_parts:
            return cs_parts[k]
    return None


# ---------------------------------------------------------------------------
# IR selection heuristic
# ---------------------------------------------------------------------------

def _is_on_prem(cm: SSISConnectionManager) -> bool:
    """
    Heuristic: decide if this connection targets on-prem infrastructure.

    Returns False (= cloud / Azure-hosted) when:
      - server name contains any known Azure suffix
      - connection string contains any Azure suffix
      - connection string has ``Authentication=ActiveDirectoryManagedIdentity``
      - connection string has Azure Storage keys (AccountName, BlobEndpoint, etc.)

    Returns True otherwise (assumed on-prem, needs Self-Hosted IR).
    """
    server = (cm.server or "").lower()
    cs = (cm.connection_string or "").lower()
    combined = f"{server} {cs}"

    # Any Azure suffix → cloud
    for suffix in _AZURE_SUFFIXES:
        if suffix in combined:
            return False

    # Azure Storage connection string patterns
    if any(k in cs for k in ("accountname=", "blobendpoint=", "defaultendpointsprotocol=")):
        return False

    # Managed Identity auth marker
    if "activedirectorymanagedidentity" in cs or "activedirectorydefault" in cs:
        return False

    return True


def _base_ls(
    cm: SSISConnectionManager,
    ir_name: str = _DEFAULT_IR,
) -> dict[str, Any]:
    return {
        "name": f"LS_{cm.id}",
        "properties": {
            "description": f"Auto-generated from SSIS Connection Manager: {cm.name}",
            "annotations": ["ssis-adf-agent"],
            "connectVia": {
                "referenceName": ir_name,
                "type": "IntegrationRuntimeReference",
            },
            "type": "UNKNOWN",
            "typeProperties": {},
        },
    }


def _kv_secret_ref(
    kv_ls_name: str,
    secret_name: str,
) -> dict[str, Any]:
    """Build an Azure Key Vault secret reference."""
    return {
        "type": "AzureKeyVaultSecret",
        "store": {
            "referenceName": kv_ls_name,
            "type": "LinkedServiceReference",
        },
        "secretName": secret_name,
    }


def _oledb_ls(
    cm: SSISConnectionManager,
    ir_name: str,
    auth_type: str,
    use_key_vault: bool,
    kv_ls_name: str,
) -> dict[str, Any]:
    ls = _base_ls(cm, ir_name)
    on_prem = _is_on_prem(cm)

    # Extract components from connection string when model fields are empty
    cs_parts = parse_connection_string(cm.connection_string)
    server = cm.server or _extract_server(cs_parts) or ("TODO_SERVER" if on_prem else "TODO_SERVER.database.windows.net")
    database = cm.database or _extract_database(cs_parts) or "TODO_DATABASE"
    cs_user = _extract_user(cs_parts)
    cs_password = _extract_password(cs_parts)

    # Detect auth hints from the connection string
    cs_auth = cs_parts.get("authentication", "").lower()
    integrated = cs_parts.get("integrated security", "").lower() in ("sspi", "true", "yes")
    if cs_auth.startswith("activedirectoryserviceprincipal"):
        auth_type = "ServicePrincipal"
    elif cs_auth.startswith("activedirectorymanagedidentity") or cs_auth.startswith("activedirectorydefault"):
        auth_type = "SystemAssignedManagedIdentity"

    if on_prem:
        # On-prem SQL Server: use SqlServer connector type with SHIR
        ls["properties"]["type"] = "SqlServer"

        if integrated:
            ls["properties"]["typeProperties"] = {
                "server": server,
                "database": database,
                "encrypt": "mandatory",
                "trustServerCertificate": False,
                "authenticationType": "Windows",
                "userName": cs_user or "TODO_DOMAIN\\\\TODO_USER",
                "password": (
                    _kv_secret_ref(kv_ls_name, f"{cm.name}-password")
                    if use_key_vault
                    else {"type": "SecureString", "value": "TODO — store in Azure Key Vault"}
                ),
                "pooling": False,
            }
        elif use_key_vault:
            ls["properties"]["typeProperties"] = {
                "server": server,
                "database": database,
                "encrypt": "mandatory",
                "trustServerCertificate": False,
                "authenticationType": "SQL",
                "userName": cs_user or "TODO_USERNAME",
                "password": _kv_secret_ref(kv_ls_name, f"{cm.name}-password"),
                "pooling": False,
            }
        else:
            ls["properties"]["typeProperties"] = {
                "server": server,
                "database": database,
                "encrypt": "mandatory",
                "trustServerCertificate": False,
                "authenticationType": "SQL",
                "userName": cs_user or "TODO_USERNAME",
                "password": {"type": "SecureString", "value": "TODO — store in Azure Key Vault"},
                "pooling": False,
            }
    else:
        # Azure SQL Database: Recommended version format
        ls["properties"]["type"] = "AzureSqlDatabase"

        if auth_type == "SystemAssignedManagedIdentity":
            ls["properties"]["typeProperties"] = {
                "server": server,
                "database": database,
                "encrypt": "mandatory",
                "trustServerCertificate": False,
                "authenticationType": "SystemAssignedManagedIdentity",
            }
        elif auth_type == "ServicePrincipal":
            tenant = cs_parts.get("tenant id", cs_parts.get("tenantid", "TODO_TENANT_ID"))
            sp_id = cs_parts.get("client id", cs_parts.get("clientid", cs_user or "TODO_SERVICE_PRINCIPAL_ID"))
            ls["properties"]["typeProperties"] = {
                "server": server,
                "database": database,
                "encrypt": "mandatory",
                "trustServerCertificate": False,
                "authenticationType": "ServicePrincipal",
                "servicePrincipalId": sp_id,
                "tenant": tenant,
                "servicePrincipalCredentialType": "ServicePrincipalKey",
                "servicePrincipalCredential": (
                    _kv_secret_ref(kv_ls_name, f"{cm.name}-sp-secret")
                    if use_key_vault
                    else {"type": "SecureString", "value": "TODO — store in Azure Key Vault"}
                ),
            }
        elif use_key_vault:
            ls["properties"]["typeProperties"] = {
                "server": server,
                "database": database,
                "encrypt": "mandatory",
                "trustServerCertificate": False,
                "authenticationType": "SQL",
                "userName": cs_user or "TODO_USERNAME",
                "password": _kv_secret_ref(kv_ls_name, f"{cm.name}-password"),
            }
        else:
            ls["properties"]["typeProperties"] = {
                "server": server,
                "database": database,
                "encrypt": "mandatory",
                "trustServerCertificate": False,
                "authenticationType": "SQL",
                "userName": cs_user or "TODO_USERNAME",
                "password": {"type": "SecureString", "value": "TODO — store in Azure Key Vault"},
            }

    return ls


def _flat_file_ls(
    cm: SSISConnectionManager,
    ir_name: str,
    auth_type: str,
    use_key_vault: bool,
    kv_ls_name: str,
) -> dict[str, Any]:
    ls = _base_ls(cm, ir_name)
    ls["properties"]["type"] = "AzureBlobStorage"
    note = (
        f"Original flat file path: {cm.file_path or 'unknown'}. "
        "Replace with Azure Blob Storage or ADLS Gen2 connection string."
    )

    cs_parts = parse_connection_string(cm.connection_string)
    sas_token = cs_parts.get("sharedaccesssignature", "")
    account_name = cs_parts.get("accountname", "")
    account_key = cs_parts.get("accountkey", "")
    blob_endpoint = cs_parts.get("blobendpoint", "")

    if sas_token:
        # SAS token authentication
        sas_uri = blob_endpoint or f"https://{account_name}.blob.core.windows.net"
        # Combine endpoint and token: sasUri = endpoint + "?" + sas
        full_uri = f"{sas_uri}?{sas_token}" if "?" not in sas_uri else sas_uri
        if use_key_vault:
            ls["properties"]["typeProperties"] = {
                "sasUri": _kv_secret_ref(kv_ls_name, f"{cm.name}-sasuri"),
                "note": note,
            }
        else:
            ls["properties"]["typeProperties"] = {
                "sasUri": {"type": "SecureString", "value": full_uri},
                "note": note,
            }
    elif account_key:
        # Account key authentication
        if use_key_vault:
            ls["properties"]["typeProperties"] = {
                "connectionString": f"DefaultEndpointsProtocol=https;AccountName={account_name}",
                "accountKey": _kv_secret_ref(kv_ls_name, f"{cm.name}-accountkey"),
                "note": note,
            }
        else:
            ls["properties"]["typeProperties"] = {
                "connectionString": f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key}",
                "note": note,
            }
    elif account_name:
        # Known account but no key/SAS — use Managed Identity or Key Vault
        ls["properties"]["typeProperties"] = {
            "serviceEndpoint": f"https://{account_name}.blob.core.windows.net",
            "accountKind": "StorageV2",
            "note": note,
        }
    elif use_key_vault:
        ls["properties"]["typeProperties"] = {
            "connectionString": _kv_secret_ref(kv_ls_name, f"{cm.name}-connectionstring"),
            "note": note,
        }
    else:
        ls["properties"]["typeProperties"] = {
            "connectionString": "DefaultEndpointsProtocol=https;AccountName=TODO;AccountKey=TODO",
            "note": note,
        }
    return ls


def _ftp_ls(
    cm: SSISConnectionManager,
    ir_name: str,
    auth_type: str,
    use_key_vault: bool,
    kv_ls_name: str,
) -> dict[str, Any]:
    ls = _base_ls(cm, ir_name)
    ls["properties"]["type"] = "FtpServer"
    password: Any
    if use_key_vault:
        password = _kv_secret_ref(kv_ls_name, f"{cm.name}-password")
    else:
        password = {"type": "SecureString", "value": "TODO — store in Azure Key Vault"}
    ls["properties"]["typeProperties"] = {
        "host": cm.server or "TODO_FTP_HOST",
        "port": 21,
        "enableSsl": True,
        "authenticationType": "Basic",
        "userName": cm.username or "TODO",
        "password": password,
    }
    return ls


def _http_ls(
    cm: SSISConnectionManager,
    ir_name: str,
    auth_type: str,
    use_key_vault: bool,
    kv_ls_name: str,
) -> dict[str, Any]:
    ls = _base_ls(cm, ir_name)
    ls["properties"]["type"] = "HttpServer"
    ls["properties"]["typeProperties"] = {
        "url": cm.connection_string or "https://TODO",
        "authenticationType": "Anonymous",
    }
    return ls


def _smtp_ls(
    cm: SSISConnectionManager,
    ir_name: str,
    auth_type: str,
    use_key_vault: bool,
    kv_ls_name: str,
) -> dict[str, Any]:
    ls = _base_ls(cm, ir_name)
    ls["properties"]["type"] = "AzureFunction"
    ls["properties"]["description"] = (
        f"[MANUAL REVIEW] SMTP connection '{cm.name}' has no ADF linked service. "
        "Replace with Azure Communication Services or Logic App."
    )
    ls["properties"]["typeProperties"] = {
        "functionAppUrl": "https://TODO.azurewebsites.net",
        "functionKey": {"type": "SecureString", "value": "TODO"},
    }
    return ls


_BUILDERS: dict[ConnectionManagerType, Any] = {
    ConnectionManagerType.OLEDB: _oledb_ls,
    ConnectionManagerType.ADO_NET: _oledb_ls,
    ConnectionManagerType.FLAT_FILE: _flat_file_ls,
    ConnectionManagerType.EXCEL: _flat_file_ls,
    ConnectionManagerType.FTP: _ftp_ls,
    ConnectionManagerType.HTTP: _http_ls,
    ConnectionManagerType.SMTP: _smtp_ls,
    ConnectionManagerType.ODBC: _oledb_ls,
}


def _generate_kv_linked_service(
    kv_ls_name: str,
    kv_url: str,
    output_dir: Path,
) -> dict[str, Any]:
    """Generate an Azure Key Vault linked service."""
    ls: dict[str, Any] = {
        "name": kv_ls_name,
        "properties": {
            "description": "Azure Key Vault for storing connection secrets.",
            "annotations": ["ssis-adf-agent"],
            "type": "AzureKeyVault",
            "typeProperties": {
                "baseUrl": kv_url,
            },
        },
    }
    ls_dir = output_dir / "linkedService"
    ls_dir.mkdir(parents=True, exist_ok=True)
    (ls_dir / f"{kv_ls_name}.json").write_text(
        json.dumps(ls, indent=4, ensure_ascii=False),
        encoding="utf-8",
    )
    return ls


def generate_linked_services(
    package: SSISPackage,
    output_dir: Path,
    *,
    on_prem_ir_name: str = "SelfHostedIR",
    cloud_ir_name: str = _DEFAULT_IR,
    auth_type: str = "SystemAssignedManagedIdentity",
    use_key_vault: bool = False,
    kv_ls_name: str = "LS_KeyVault",
    kv_url: str = "https://TODO.vault.azure.net/",
    shared_artifacts_dir: Path | None = None,
) -> list[dict[str, Any]]:
    """
    Generate one ADF linked service JSON per unique Connection Manager in *package*.

    When *shared_artifacts_dir* is set, checks for existing linked service JSON files
    there before creating new ones (cross-package deduplication by server+database).

    Files are written to *output_dir*/linkedService/.
    Returns the list of linked service dicts.
    """
    ls_dir = output_dir / "linkedService"
    ls_dir.mkdir(parents=True, exist_ok=True)

    # Build index of existing shared linked services for dedup
    existing_ls: dict[str, str] = {}  # (server|database) → ls_name
    if shared_artifacts_dir:
        shared_ls_dir = shared_artifacts_dir / "linkedService"
        if shared_ls_dir.exists():
            for f in shared_ls_dir.glob("*.json"):
                try:
                    data = json.loads(f.read_text(encoding="utf-8"))
                    tp = data.get("properties", {}).get("typeProperties", {})
                    key = f"{tp.get('server', '')}|{tp.get('database', '')}".lower()
                    if key != "|":
                        existing_ls[key] = data["name"]
                except Exception as exc:
                    warn(
                        phase="generate", severity="warning",
                        source="linked_service_generator",
                        message=f"Failed to read shared linked service '{f.name}': {exc}",
                        detail="Deduplication may create a duplicate linked service",
                    )
                    continue

    seen_ids: set[str] = set()
    results: list[dict[str, Any]] = []
    generated_kv = False

    for cm in package.connection_managers:
        if cm.id in seen_ids:
            continue
        seen_ids.add(cm.id)

        # Check for cross-package dedup
        dedup_key = f"{(cm.server or '').lower()}|{(cm.database or '').lower()}"
        if dedup_key != "|" and dedup_key in existing_ls:
            # Reuse existing linked service — don't generate a new file
            continue

        on_prem = _is_on_prem(cm)
        ir_name = on_prem_ir_name if on_prem else cloud_ir_name

        builder = _BUILDERS.get(cm.type, _generic_ls)
        ls = builder(cm, ir_name, auth_type, use_key_vault, kv_ls_name)

        # Generate Key Vault linked service once if needed
        if use_key_vault and not generated_kv:
            kv_ls = _generate_kv_linked_service(kv_ls_name, kv_url, output_dir)
            results.append(kv_ls)
            generated_kv = True

        ls_name = ls["name"]
        file_path = ls_dir / f"{ls_name}.json"
        file_path.write_text(
            json.dumps(ls, indent=4, ensure_ascii=False),
            encoding="utf-8",
        )
        results.append(ls)

        # Track for future dedup
        if dedup_key != "|":
            existing_ls[dedup_key] = ls_name

    return results


def _generic_ls(
    cm: SSISConnectionManager,
    ir_name: str,
    auth_type: str,
    use_key_vault: bool,
    kv_ls_name: str,
) -> dict[str, Any]:
    ls = _base_ls(cm, ir_name)
    ls["properties"]["type"] = "AzureSqlDatabase"
    ls["properties"]["description"] = (
        f"[MANUAL REVIEW] Unknown connection type '{cm.type.value}' for '{cm.name}'. "
        "Update type and typeProperties."
    )
    server = cm.server or "TODO_SERVER"
    database = cm.database or "TODO_DATABASE"
    ls["properties"]["typeProperties"] = {
        "server": server,
        "database": database,
        "encrypt": "mandatory",
        "trustServerCertificate": False,
        "authenticationType": auth_type,
    }
    return ls
