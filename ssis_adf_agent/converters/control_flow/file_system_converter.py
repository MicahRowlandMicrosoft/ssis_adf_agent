"""
File System Task → ADF Web Activity calling an Azure Function.

Most file-system operations (copy, move, delete, rename, mkdir) have no native
ADF activity equivalent.  The converter generates a Web Activity wired to an
Azure Function endpoint.  The function stub is generated separately by the
script_task_converter.

For simple copy/move between Azure Storage paths, a Copy Activity is preferred;
more exotic operations fall back to the Web+Function approach.
"""
from __future__ import annotations

from typing import Any

from ...parsers.models import FileSystemTask, PrecedenceConstraint, SSISTask
from ..base_converter import BaseConverter

_COPY_OPS = frozenset({"CopyFile", "MoveFile"})
_AZURE_STORAGE_PROTOCOLS = ("wasbs://", "abfss://", "https://", "adl://")


def _looks_like_azure_path(path: str | None) -> bool:
    if not path:
        return False
    return any(path.startswith(p) for p in _AZURE_STORAGE_PROTOCOLS)


class FileSystemConverter(BaseConverter):
    def convert(
        self,
        task: SSISTask,
        constraints: list[PrecedenceConstraint],
        task_by_id: dict[str, SSISTask],
    ) -> list[dict[str, Any]]:
        assert isinstance(task, FileSystemTask)
        depends_on = self._depends_on(task, constraints, task_by_id)

        # If both paths look like Azure Storage and operation is copy/move → Copy Activity
        if task.operation in _COPY_OPS and (
            _looks_like_azure_path(task.source_path) and
            _looks_like_azure_path(task.destination_path)
        ):
            return [self._copy_activity(task, depends_on)]

        # Default: delegate to Azure Function
        return [self._web_activity(task, depends_on)]

    def _copy_activity(self, task: FileSystemTask, depends_on: list) -> dict[str, Any]:
        return {
            "name": task.name,
            "description": task.description or "",
            "type": "Copy",
            "dependsOn": depends_on,
            "typeProperties": {
                "source": {
                    "type": "BinarySource",
                    "storeSettings": {
                        "type": "AzureBlobStorageReadSettings",
                        "recursive": False,
                    },
                },
                "sink": {
                    "type": "BinarySink",
                    "storeSettings": {"type": "AzureBlobStorageWriteSettings"},
                },
                "enableStaging": False,
                "deleteFilesAfterCompletion": task.operation == "MoveFile",
            },
            "inputs": [{"referenceName": f"DS_src_{task.name.replace(' ', '_')}", "type": "DatasetReference"}],
            "outputs": [{"referenceName": f"DS_dst_{task.name.replace(' ', '_')}", "type": "DatasetReference"}],
        }

    def _web_activity(self, task: FileSystemTask, depends_on: list) -> dict[str, Any]:
        return {
            "name": task.name,
            "description": (
                f"[CONVERTED FROM FileSystemTask — operation: {task.operation}] "
                + (task.description or "")
            ),
            "type": "WebActivity",
            "dependsOn": depends_on,
            "typeProperties": {
                "method": "POST",
                "url": "@pipeline().parameters.FileSystemFunctionUrl",
                "body": {
                    "operation": task.operation,
                    "source": task.source_path or "",
                    "destination": task.destination_path or "",
                    "overwrite": task.overwrite,
                },
                "authentication": {
                    "type": "MSI",
                    "resource": "https://management.azure.com/",
                },
            },
        }
