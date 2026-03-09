"""
Linked Service generator — emits one ADF linkedService.json per SSIS Connection Manager.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ..parsers.models import ConnectionManagerType, SSISConnectionManager, SSISPackage


_IR_NAME = "AutoResolveIntegrationRuntime"


def _base_ls(cm: SSISConnectionManager) -> dict[str, Any]:
    return {
        "name": f"LS_{cm.id}",
        "properties": {
            "description": f"Auto-generated from SSIS Connection Manager: {cm.name}",
            "annotations": ["ssis-adf-agent"],
            "connectVia": {
                "referenceName": _IR_NAME,
                "type": "IntegrationRuntimeReference",
            },
            "type": "UNKNOWN",
            "typeProperties": {},
        },
    }


def _oledb_ls(cm: SSISConnectionManager) -> dict[str, Any]:
    ls = _base_ls(cm)
    ls["properties"]["type"] = "AzureSqlDatabase"
    ls["properties"]["typeProperties"] = {
        "connectionString": cm.connection_string or (
            f"Server=tcp:{cm.server or 'TODO'},1433;"
            f"Initial Catalog={cm.database or 'TODO'};"
            "Persist Security Info=False;MultipleActiveResultSets=False;"
            "Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;"
        ),
    }
    return ls


def _flat_file_ls(cm: SSISConnectionManager) -> dict[str, Any]:
    ls = _base_ls(cm)
    ls["properties"]["type"] = "AzureBlobStorage"
    ls["properties"]["typeProperties"] = {
        "connectionString": "DefaultEndpointsProtocol=https;AccountName=TODO;AccountKey=TODO",
        "note": (
            f"Original flat file path: {cm.file_path or 'unknown'}. "
            "Replace with Azure Blob Storage or ADLS Gen2 connection string."
        ),
    }
    return ls


def _ftp_ls(cm: SSISConnectionManager) -> dict[str, Any]:
    ls = _base_ls(cm)
    ls["properties"]["type"] = "FtpServer"
    ls["properties"]["typeProperties"] = {
        "host": cm.server or "TODO_FTP_HOST",
        "port": 21,
        "enableSsl": True,
        "authenticationType": "Basic",
        "userName": cm.username or "TODO",
        "password": {
            "type": "SecureString",
            "value": "TODO — store in Azure Key Vault",
        },
    }
    return ls


def _http_ls(cm: SSISConnectionManager) -> dict[str, Any]:
    ls = _base_ls(cm)
    ls["properties"]["type"] = "HttpServer"
    ls["properties"]["typeProperties"] = {
        "url": cm.connection_string or "https://TODO",
        "authenticationType": "Anonymous",
    }
    return ls


def _smtp_ls(cm: SSISConnectionManager) -> dict[str, Any]:
    ls = _base_ls(cm)
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


_BUILDERS = {
    ConnectionManagerType.OLEDB: _oledb_ls,
    ConnectionManagerType.ADO_NET: _oledb_ls,
    ConnectionManagerType.FLAT_FILE: _flat_file_ls,
    ConnectionManagerType.EXCEL: _flat_file_ls,
    ConnectionManagerType.FTP: _ftp_ls,
    ConnectionManagerType.HTTP: _http_ls,
    ConnectionManagerType.SMTP: _smtp_ls,
    ConnectionManagerType.ODBC: _oledb_ls,
}


def generate_linked_services(
    package: SSISPackage,
    output_dir: Path,
    ir_name: str = _IR_NAME,
) -> list[dict[str, Any]]:
    """
    Generate one ADF linked service JSON per unique Connection Manager in *package*.
    Files are written to *output_dir*/linkedService/.

    Returns the list of linked service dicts.
    """
    ls_dir = output_dir / "linkedService"
    ls_dir.mkdir(parents=True, exist_ok=True)

    seen_ids: set[str] = set()
    results: list[dict[str, Any]] = []

    for cm in package.connection_managers:
        if cm.id in seen_ids:
            continue
        seen_ids.add(cm.id)

        builder = _BUILDERS.get(cm.type, _generic_ls)
        ls = builder(cm)

        # Override IR name if customised
        if ir_name != _IR_NAME:
            ls["properties"]["connectVia"]["referenceName"] = ir_name

        ls_name = ls["name"]
        file_path = ls_dir / f"{ls_name}.json"
        file_path.write_text(
            json.dumps(ls, indent=4, ensure_ascii=False),
            encoding="utf-8",
        )
        results.append(ls)

    return results


def _generic_ls(cm: SSISConnectionManager) -> dict[str, Any]:
    ls = _base_ls(cm)
    ls["properties"]["type"] = "AzureSqlDatabase"
    ls["properties"]["description"] = (
        f"[MANUAL REVIEW] Unknown connection type '{cm.type.value}' for '{cm.name}'. "
        "Update type and typeProperties."
    )
    ls["properties"]["typeProperties"] = {
        "connectionString": cm.connection_string or "TODO",
    }
    return ls
