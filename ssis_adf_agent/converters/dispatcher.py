"""
Converter dispatcher — routes SSIS tasks to the right converter based on TaskType.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from ..parsers.models import PrecedenceConstraint, SSISTask, TaskType
from ..warnings_collector import warn
from .base_converter import BaseConverter
from .control_flow.data_flow_converter import DataFlowConverter
from .control_flow.execute_package_converter import ExecutePackageConverter
from .control_flow.execute_sql_converter import ExecuteSQLConverter
from .control_flow.file_system_converter import FileSystemConverter
from .control_flow.for_loop_converter import ForLoopConverter
from .control_flow.foreach_converter import ForEachConverter
from .control_flow.script_task_converter import ScriptTaskConverter


class ConverterDispatcher:
    """
    Selects and applies the appropriate converter for each SSIS task type.

    Usage::

        dispatcher = ConverterDispatcher(stubs_dir=Path("output/stubs"))
        activities = dispatcher.convert_task(task, constraints, task_by_id)
    """

    def __init__(
        self,
        stubs_dir: Path | None = None,
        llm_translate: bool = False,
        pipeline_prefix: str = "PL_",
    ) -> None:
        script_converter = ScriptTaskConverter(
            stubs_output_dir=stubs_dir, llm_translate=llm_translate,
        )

        # Pass self to loop converters so they can recursively convert inner tasks
        foreach_converter = ForEachConverter(child_converter=self)
        for_loop_converter = ForLoopConverter(child_converter=self)

        self._registry: dict[TaskType, BaseConverter] = {
            TaskType.EXECUTE_SQL: ExecuteSQLConverter(),
            TaskType.EXECUTE_PACKAGE: ExecutePackageConverter(pipeline_prefix=pipeline_prefix),
            TaskType.FILE_SYSTEM: FileSystemConverter(),
            TaskType.FTP: _FTPConverter(),
            TaskType.SEND_MAIL: _SendMailConverter(),
            TaskType.SCRIPT: script_converter,
            TaskType.EXECUTE_PROCESS: _ExecuteProcessConverter(),
            TaskType.DATA_FLOW: DataFlowConverter(),
            TaskType.SEQUENCE: _SequenceConverter(child_converter=self),
            TaskType.FOREACH_LOOP: foreach_converter,
            TaskType.FOR_LOOP: for_loop_converter,
            TaskType.BULK_INSERT: _BulkInsertConverter(),
            TaskType.WEB_SERVICE: _WebServiceConverter(),
            TaskType.XML: _XMLConverter(stubs_dir=stubs_dir),
            TaskType.TRANSFER_SQL: _TransferSQLConverter(),
        }
        self._fallback = _FallbackConverter()

    def convert_task(
        self,
        task: SSISTask,
        constraints: list[PrecedenceConstraint],
        task_by_id: dict[str, SSISTask],
    ) -> list[dict[str, Any]]:
        converter = self._registry.get(task.task_type, self._fallback)
        return converter.convert(task, constraints, task_by_id)


# ---------------------------------------------------------------------------
# Inline minimal converters for types that don't warrant standalone files
# ---------------------------------------------------------------------------

class _FTPConverter(BaseConverter):
    def convert(self, task, constraints, task_by_id):  # type: ignore[override]
        from ..parsers.models import FTPTask
        assert isinstance(task, FTPTask)
        depends_on = self._depends_on(task, constraints, task_by_id)
        return [{
            "name": task.name,
            "description": task.description or "",
            "type": "Copy",
            "dependsOn": depends_on,
            "typeProperties": {
                "source": {
                    "type": "BinarySource",
                    "storeSettings": {"type": "FtpReadSettings", "recursive": False},
                },
                "sink": {
                    "type": "BinarySink",
                    "storeSettings": {"type": "AzureBlobStorageWriteSettings"},
                },
                "enableStaging": False,
            },
            "inputs": [{"referenceName": f"DS_FTP_{task.name.replace(' ', '_')}", "type": "DatasetReference"}],
            "outputs": [{"referenceName": f"DS_Blob_{task.name.replace(' ', '_')}", "type": "DatasetReference"}],
        }]


class _SendMailConverter(BaseConverter):
    def convert(self, task, constraints, task_by_id):  # type: ignore[override]
        from ..parsers.models import SendMailTask
        assert isinstance(task, SendMailTask)
        depends_on = self._depends_on(task, constraints, task_by_id)
        return [{
            "name": task.name,
            "description": (
                "[MANUAL REVIEW] Send Mail has been mapped to a Web Activity. "
                "Configure a Logic App or Azure Communication Services endpoint."
            ),
            "type": "WebActivity",
            "dependsOn": depends_on,
            "typeProperties": {
                "method": "POST",
                "url": "@pipeline().parameters.SendMailFunctionUrl",
                "body": {
                    "to": task.to or "",
                    "cc": task.cc or "",
                    "from": task.from_address or "",
                    "subject": task.subject or "",
                    "message": task.message_source or "",
                },
                "authentication": {"type": "Anonymous"},
            },
        }]


class _ExecuteProcessConverter(BaseConverter):
    def convert(self, task, constraints, task_by_id):  # type: ignore[override]
        from ..parsers.models import ExecuteProcessTask
        assert isinstance(task, ExecuteProcessTask)
        depends_on = self._depends_on(task, constraints, task_by_id)
        return [{
            "name": task.name,
            "description": (
                f"[MANUAL REVIEW] Execute Process Task (executable: {task.executable!r}) "
                "mapped to Azure Batch Custom Activity. Configure batch pool and application."
            ),
            "type": "Custom",
            "dependsOn": depends_on,
            "linkedServiceName": {
                "referenceName": "LS_AzureBatch",
                "type": "LinkedServiceReference",
            },
            "typeProperties": {
                "command": f"{task.executable or 'TODO'} {task.arguments or ''}".strip(),
                "resourceLinkedService": {
                    "referenceName": "LS_AzureStorage",
                    "type": "LinkedServiceReference",
                },
                "folderPath": "custom-activity",
            },
        }]


class _SequenceConverter(BaseConverter):
    def __init__(self, child_converter: ConverterDispatcher) -> None:
        self._child = child_converter

    def convert(self, task, constraints, task_by_id):  # type: ignore[override]
        from ..parsers.models import SequenceContainer
        assert isinstance(task, SequenceContainer)
        activities = []
        inner_by_id = {t.id: t for t in task.tasks}
        for t in task.tasks:
            activities.extend(
                self._child.convert_task(t, task.constraints, inner_by_id)
            )
        return activities


class _FallbackConverter(BaseConverter):
    def convert(self, task, constraints, task_by_id):  # type: ignore[override]
        depends_on = self._depends_on(task, constraints, task_by_id)
        warn(
            phase="convert", severity="warning", source="dispatcher",
            message=f"No converter for task type '{task.task_type.value}'",
            task_name=task.name, task_id=task.id,
            detail="Emitting placeholder Wait activity — manual implementation required",
        )
        return [{
            "name": task.name,
            "description": (
                f"[UNSUPPORTED — TaskType: {task.task_type.value}] "
                "Manual implementation required. Original task could not be converted."
            ),
            "type": "Wait",
            "dependsOn": depends_on,
            "typeProperties": {"waitTimeInSeconds": 1},
        }]


# ---------------------------------------------------------------------------
# Task-type converters for previously unsupported types
# ---------------------------------------------------------------------------

class _BulkInsertConverter(BaseConverter):
    """BulkInsertTask → ADF Copy Activity (SQL source → SQL sink)."""

    def convert(self, task, constraints, task_by_id):  # type: ignore[override]
        depends_on = self._depends_on(task, constraints, task_by_id)
        safe = task.name.replace(" ", "_")
        return [{
            "name": task.name,
            "description": (
                "[MANUAL REVIEW] Bulk Insert mapped to Copy Activity. "
                "Verify source dataset and sink table."
            ),
            "type": "Copy",
            "dependsOn": depends_on,
            "typeProperties": {
                "source": {
                    "type": "DelimitedTextSource",
                    "storeSettings": {"type": "AzureBlobStorageReadSettings"},
                    "formatSettings": {"type": "DelimitedTextReadSettings"},
                },
                "sink": {
                    "type": "AzureSqlSink",
                    "writeBehavior": "insert",
                    "sqlWriterUseTableLock": True,
                    "tableOption": "autoCreate",
                },
                "enableStaging": False,
            },
            "inputs": [{
                "referenceName": f"DS_BulkSrc_{safe}",
                "type": "DatasetReference",
            }],
            "outputs": [{
                "referenceName": f"DS_BulkSink_{safe}",
                "type": "DatasetReference",
            }],
        }]


class _WebServiceConverter(BaseConverter):
    """WebServiceTask → ADF Web Activity."""

    def convert(self, task, constraints, task_by_id):  # type: ignore[override]
        depends_on = self._depends_on(task, constraints, task_by_id)
        url = getattr(task, "url", None) or getattr(task, "connection_string", None) or ""
        method = getattr(task, "http_method", None) or "POST"
        return [{
            "name": task.name,
            "description": (
                "[MANUAL REVIEW] Web Service Task mapped to Web Activity. "
                "Configure URL, authentication, and request body."
            ),
            "type": "WebActivity",
            "dependsOn": depends_on,
            "typeProperties": {
                "method": method.upper() if method else "POST",
                "url": url or "@pipeline().parameters.WebServiceUrl",
                "body": "{}",
                "authentication": {"type": "Anonymous"},
            },
        }]


class _XMLConverter(BaseConverter):
    """XMLTask → ADF Azure Function Activity with generated stub."""

    def __init__(self, stubs_dir: Path | None = None) -> None:
        self._stubs_dir = stubs_dir or Path("stubs")

    def convert(self, task, constraints, task_by_id):  # type: ignore[override]
        depends_on = self._depends_on(task, constraints, task_by_id)
        props = getattr(task, "properties", {}) or {}
        operation = props.get("OperationType") or getattr(task, "operation_type", None) or "Unknown"
        source = props.get("Source", "")
        second_operand = props.get("SecondOperand", "")
        xpath_op = props.get("XPathOperation", "")

        warn(
            phase="convert", severity="warning", source="dispatcher",
            message=f"XML Task '{task.name}' (operation: {operation}) requires manual review",
            task_name=task.name, task_id=task.id,
            detail="Azure Function stub generated with XML processing boilerplate",
        )

        func_name = task.name.replace(" ", "_").replace("-", "_")
        self._write_xml_stub(func_name, operation, source, second_operand, xpath_op)

        return [{
            "name": task.name,
            "description": (
                f"[MANUAL REVIEW] XML Task (operation: {operation}) → Azure Function. "
                f"Stub generated at stubs/{func_name}/__init__.py. "
                "Review and complete the XML processing logic."
            ),
            "type": "AzureFunction",
            "dependsOn": depends_on,
            "linkedServiceName": {
                "referenceName": "LS_AzureFunction",
                "type": "LinkedServiceReference",
            },
            "typeProperties": {
                "functionName": func_name,
                "method": "POST",
                "body": {
                    "operation": operation,
                    "source": source,
                    "second_operand": second_operand,
                    "xpath_operation": xpath_op,
                },
            },
        }]

    def _write_xml_stub(
        self, func_name: str, operation: str, source: str,
        second_operand: str, xpath_op: str,
    ) -> Path:
        import json
        import textwrap
        self._stubs_dir.mkdir(parents=True, exist_ok=True)
        stub_file = self._stubs_dir / func_name / "__init__.py"
        stub_file.parent.mkdir(parents=True, exist_ok=True)

        body = textwrap.dedent(f'''\
            """
            Azure Function stub for XML Task: {func_name}
            Operation: {operation}
            Source: {source}
            {"XPath: " + second_operand if operation == "XPATH" else "Second operand: " + second_operand}
            {"XPath operation: " + xpath_op if xpath_op else ""}

            Auto-generated by ssis-adf-agent. Implement the XML processing logic below.
            """
            import json
            import logging
            from lxml import etree

            import azure.functions as func


            def main(req: func.HttpRequest) -> func.HttpResponse:
                logging.info("XML Task function triggered: {func_name}")

                body = req.get_json()
                operation = body.get("operation", "{operation}")
                source_path = body.get("source", "")
                second_operand = body.get("second_operand", "")
                xpath_operation = body.get("xpath_operation", "")

        ''')

        if operation == "XPATH":
            body += textwrap.dedent(f'''\
                # --- XPATH operation ---
                # TODO: Load XML from Azure Blob Storage or inline
                # tree = etree.parse(source_path)
                # root = tree.getroot()
                #
                # xpath_expr = second_operand or "{second_operand}"
                # results = root.xpath(xpath_expr)
                #
                # if xpath_operation == "NodeList":
                #     output = [etree.tostring(r, encoding="unicode") for r in results]
                # elif xpath_operation == "Values":
                #     output = [r.text if hasattr(r, "text") else str(r) for r in results]
                # else:
                #     output = str(results)

                # TODO: Replace with actual implementation
                return func.HttpResponse(
                    json.dumps({{"status": "not_implemented", "operation": "XPATH"}}),
                    mimetype="application/json",
                )
            ''')
        elif operation == "Merge":
            body += textwrap.dedent('''\
                # --- Merge operation ---
                # TODO: Load two XML documents and merge them
                # tree1 = etree.parse(source_path)
                # tree2 = etree.parse(second_operand)
                # root1 = tree1.getroot()
                # for child in tree2.getroot():
                #     root1.append(child)
                # merged = etree.tostring(root1, encoding="unicode", pretty_print=True)

                # TODO: Replace with actual implementation
                return func.HttpResponse(
                    json.dumps({"status": "not_implemented", "operation": "Merge"}),
                    mimetype="application/json",
                )
            ''')
        elif operation == "Validate":
            body += textwrap.dedent('''\
                # --- Validate operation ---
                # TODO: Validate XML against XSD schema
                # schema = etree.XMLSchema(etree.parse(second_operand))
                # doc = etree.parse(source_path)
                # is_valid = schema.validate(doc)

                # TODO: Replace with actual implementation
                return func.HttpResponse(
                    json.dumps({"status": "not_implemented", "operation": "Validate"}),
                    mimetype="application/json",
                )
            ''')
        elif operation == "XSLT":
            body += textwrap.dedent('''\
                # --- XSLT operation ---
                # TODO: Apply XSLT transformation
                # xslt = etree.parse(second_operand)
                # transform = etree.XSLT(xslt)
                # doc = etree.parse(source_path)
                # result = transform(doc)
                # output = str(result)

                # TODO: Replace with actual implementation
                return func.HttpResponse(
                    json.dumps({"status": "not_implemented", "operation": "XSLT"}),
                    mimetype="application/json",
                )
            ''')
        elif operation == "Diff":
            body += textwrap.dedent('''\
                # --- Diff operation ---
                # TODO: Compare two XML documents
                # doc1 = etree.parse(source_path)
                # doc2 = etree.parse(second_operand)
                # Use xmldiff or custom comparison logic

                # TODO: Replace with actual implementation
                return func.HttpResponse(
                    json.dumps({"status": "not_implemented", "operation": "Diff"}),
                    mimetype="application/json",
                )
            ''')
        else:
            body += textwrap.dedent(f'''\
                # --- {operation} operation ---
                # TODO: Implement XML processing logic for operation: {operation}

                return func.HttpResponse(
                    json.dumps({{"status": "not_implemented", "operation": "{operation}"}}),
                    mimetype="application/json",
                )
            ''')

        stub_file.write_text(body, encoding="utf-8")

        # Write function.json
        func_json = stub_file.parent / "function.json"
        func_json.write_text(json.dumps({
            "scriptFile": "__init__.py",
            "bindings": [
                {"authLevel": "function", "type": "httpTrigger", "direction": "in",
                 "name": "req", "methods": ["post"]},
                {"type": "http", "direction": "out", "name": "$return"},
            ],
        }, indent=2), encoding="utf-8")

        return stub_file


class _TransferSQLConverter(BaseConverter):
    """TransferSQLServerObjectsTask → ADF Script Activity with migration script."""

    def convert(self, task, constraints, task_by_id):  # type: ignore[override]
        depends_on = self._depends_on(task, constraints, task_by_id)
        src_conn = getattr(task, "source_connection_id", None) or "source"
        dst_conn = getattr(task, "destination_connection_id", None) or "destination"
        objects = getattr(task, "transfer_objects", None) or "Tables"
        warn(
            phase="convert", severity="warning", source="dispatcher",
            message=f"Transfer SQL Server Objects Task '{task.name}' requires manual review",
            task_name=task.name, task_id=task.id,
            detail="Schema/data transfer between SQL Servers — use ADF Copy or database migration tools",
        )
        return [{
            "name": task.name,
            "description": (
                f"[MANUAL REVIEW] Transfer SQL Server Objects ({objects}) "
                f"from {src_conn} → {dst_conn}. "
                "Replace with Copy Activity pipeline or Azure Database Migration Service."
            ),
            "type": "Script",
            "dependsOn": depends_on,
            "linkedServiceName": {
                "referenceName": f"LS_{dst_conn}",
                "type": "LinkedServiceReference",
            },
            "typeProperties": {
                "scripts": [{
                    "type": "Query",
                    "text": (
                        f"-- TODO: Transfer {objects} from {src_conn} to {dst_conn}\n"
                        "-- Consider: Copy Activity, Azure Database Migration Service,\n"
                        "-- or dacpac/bacpac deployment\n"
                        "SELECT 1 AS placeholder"
                    ),
                }],
            },
        }]
