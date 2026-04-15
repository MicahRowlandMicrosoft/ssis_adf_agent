"""
Pydantic data models representing all SSIS component types extracted from .dtsx XML.

These models form the intermediate representation (IR) used across the parser,
analyzers, and converters. All converters consume these models rather than raw XML.
"""
from __future__ import annotations

from enum import Enum
from typing import Any, Literal
from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class PrecedenceEvalOp(str, Enum):
    CONSTRAINT = "Constraint"
    EXPRESSION = "Expression"
    EXPRESSION_AND_CONSTRAINT = "ExpressionAndConstraint"
    EXPRESSION_OR_CONSTRAINT = "ExpressionOrConstraint"


class PrecedenceValue(str, Enum):
    SUCCESS = "0"
    FAILURE = "1"
    COMPLETION = "2"


class ProtectionLevel(str, Enum):
    DONT_SAVE_SENSITIVE = "DontSaveSensitive"
    ENCRYPT_SENSITIVE_WITH_USER_KEY = "EncryptSensitiveWithUserKey"
    ENCRYPT_SENSITIVE_WITH_PASSWORD = "EncryptSensitiveWithPassword"
    ENCRYPT_ALL_WITH_PASSWORD = "EncryptAllWithPassword"
    ENCRYPT_ALL_WITH_USER_KEY = "EncryptAllWithUserKey"
    SERVER_STORAGE = "ServerStorage"


class ForEachEnumeratorType(str, Enum):
    FILE = "ForEachFileEnumerator"
    ITEM = "ForEachItemEnumerator"
    ADO = "ForEachADOEnumerator"
    ADO_NET_SCHEMA = "ForEachADONetSchemaRowsetEnumerator"
    VARIABLE = "ForEachFromVariableEnumerator"
    NODELIST = "ForEachNodeListEnumerator"
    SMO = "ForEachSMOEnumerator"


class TaskType(str, Enum):
    EXECUTE_SQL = "ExecuteSQLTask"
    DATA_FLOW = "DataFlowTask"
    SCRIPT = "ScriptTask"
    FILE_SYSTEM = "FileSystemTask"
    FTP = "FTPTask"
    SEND_MAIL = "SendMailTask"
    EXECUTE_PACKAGE = "ExecutePackageTask"
    EXECUTE_PROCESS = "ExecuteProcessTask"
    BULK_INSERT = "BulkInsertTask"
    WEB_SERVICE = "WebServiceTask"
    XML = "XMLTask"
    TRANSFER_SQL = "TransferSQLServerObjectsTask"
    SEQUENCE = "Sequence"
    FOREACH_LOOP = "ForEachLoop"
    FOR_LOOP = "ForLoop"
    UNKNOWN = "Unknown"


class ConnectionManagerType(str, Enum):
    OLEDB = "OLEDB"
    ADO_NET = "ADO.NET"
    FLAT_FILE = "FLATFILE"
    EXCEL = "EXCEL"
    FTP = "FTP"
    HTTP = "HTTP"
    SMTP = "SMTP"
    FILE = "FILE"
    MULTIFILE = "MULTIFILE"
    ODBC = "ODBC"
    MSOLAP = "MSOLAP100"
    UNKNOWN = "Unknown"


class IngestionPattern(str, Enum):
    FULL = "full"
    DELTA = "delta"
    MERGE = "merge"
    UNKNOWN = "unknown"


class CrossDbReferenceType(str, Enum):
    THREE_PART = "three_part"       # [database].[schema].[table]
    FOUR_PART = "four_part"         # [server].[database].[schema].[table]
    OPENQUERY = "openquery"         # OPENQUERY(...)
    OPENROWSET = "openrowset"       # OPENROWSET(...)


class DataType(str, Enum):
    INT8 = "i1"
    INT16 = "i2"
    INT32 = "i4"
    INT64 = "i8"
    UINT8 = "ui1"
    UINT16 = "ui2"
    UINT32 = "ui4"
    UINT64 = "ui8"
    FLOAT = "r4"
    DOUBLE = "r8"
    CURRENCY = "cy"
    DECIMAL = "numeric"
    BOOLEAN = "bool"
    STRING = "str"
    WSTRING = "wstr"
    BYTES = "bytes"
    DATE = "date"
    DBDATE = "dbDate"
    DBTIME = "dbTime"
    DBTIMESTAMP = "dbTimeStamp"
    GUID = "uniqueidentifier"
    EMPTY = "empty"


# ---------------------------------------------------------------------------
# Base components
# ---------------------------------------------------------------------------

class SSISVariable(BaseModel):
    name: str
    namespace: str = "User"
    data_type: str = "String"
    value: Any = None
    expression: str | None = None
    read_only: bool = False


class SSISParameter(BaseModel):
    name: str
    data_type: str = "String"
    value: Any = None
    required: bool = False
    sensitive: bool = False


class SSISConnectionManager(BaseModel):
    id: str
    name: str
    type: ConnectionManagerType = ConnectionManagerType.UNKNOWN
    connection_string: str | None = None
    server: str | None = None
    database: str | None = None
    username: str | None = None
    file_path: str | None = None
    provider: str | None = None
    properties: dict[str, Any] = Field(default_factory=dict)


class CrossDbReference(BaseModel):
    """A cross-database or linked-server reference detected in SQL text."""
    ref_type: CrossDbReferenceType
    server_name: str | None = None
    database_name: str | None = None
    schema_name: str | None = None
    table_name: str | None = None
    raw_match: str = ""  # original matched text


class SqlAgentSchedule(BaseModel):
    """SQL Agent job schedule metadata extracted from msdb."""
    job_name: str = ""
    schedule_name: str = ""
    frequency_type: int = 4          # 1=once, 4=daily, 8=weekly, 16=monthly, 32=monthly-relative
    freq_interval: int = 1           # depends on frequency_type
    freq_subday_type: int = 1        # 1=at specified time, 4=minutes, 8=hours
    freq_subday_interval: int = 0
    active_start_time: int = 0       # HHMMSS format, e.g. 60000 = 06:00:00
    active_end_time: int = 235959
    freq_recurrence_factor: int = 0  # weekly: every N weeks, monthly: every N months


class PrecedenceConstraint(BaseModel):
    id: str
    from_task_id: str
    to_task_id: str
    eval_op: PrecedenceEvalOp = PrecedenceEvalOp.CONSTRAINT
    value: PrecedenceValue = PrecedenceValue.SUCCESS
    expression: str | None = None
    logical_and: bool = True  # False = OR


# ---------------------------------------------------------------------------
# Task models
# ---------------------------------------------------------------------------

class SSISTask(BaseModel):
    """Base model for all control-flow tasks."""
    id: str
    name: str
    description: str = ""
    task_type: TaskType = TaskType.UNKNOWN
    disabled: bool = False
    delay_validation: bool = False
    properties: dict[str, Any] = Field(default_factory=dict)
    cross_db_references: list[CrossDbReference] = Field(default_factory=list)


class ExecuteSQLTask(SSISTask):
    task_type: TaskType = TaskType.EXECUTE_SQL
    connection_id: str | None = None
    sql_statement: str | None = None
    result_set_type: str = "None"  # None, SingleRow, FullResultSet, Xml
    result_bindings: list[dict[str, str]] = Field(default_factory=list)
    parameter_bindings: list[dict[str, Any]] = Field(default_factory=list)
    timeout: int = 0
    ingestion_pattern: IngestionPattern = IngestionPattern.UNKNOWN
    delta_column: str | None = None


class ExecutePackageTask(SSISTask):
    task_type: TaskType = TaskType.EXECUTE_PACKAGE
    package_path: str | None = None
    package_connection_id: str | None = None
    use_project_reference: bool = False
    project_package_name: str | None = None
    parameter_assignments: list[dict[str, str]] = Field(default_factory=list)


class FileSystemTask(SSISTask):
    task_type: TaskType = TaskType.FILE_SYSTEM
    operation: str = "CopyFile"  # CopyFile, MoveFile, DeleteFile, RenameFile, CreateDirectory, etc.
    source_path: str | None = None
    destination_path: str | None = None
    overwrite: bool = False
    operation_type: str | None = None


class FTPTask(SSISTask):
    task_type: TaskType = TaskType.FTP
    connection_id: str | None = None
    operation: str = "Send"  # Send, Receive
    local_path: str | None = None
    remote_path: str | None = None
    overwrite: bool = False


class SendMailTask(SSISTask):
    task_type: TaskType = TaskType.SEND_MAIL
    smtp_connection_id: str | None = None
    to: str | None = None
    cc: str | None = None
    bcc: str | None = None
    from_address: str | None = None
    subject: str | None = None
    message_source: str | None = None
    message_type: str = "DirectInput"


class ScriptTask(SSISTask):
    task_type: TaskType = TaskType.SCRIPT
    script_language: str = "CSharp"  # CSharp or VisualBasic
    entry_point: str = "Main"
    read_only_variables: list[str] = Field(default_factory=list)
    read_write_variables: list[str] = Field(default_factory=list)
    source_code: str | None = None  # Extracted C#/VB code if available (not binary-compressed)
    project_file_name: str | None = None


class ExecuteProcessTask(SSISTask):
    task_type: TaskType = TaskType.EXECUTE_PROCESS
    executable: str | None = None
    arguments: str | None = None
    working_directory: str | None = None
    standard_input_variable: str | None = None
    standard_output_variable: str | None = None
    standard_error_variable: str | None = None
    success_return_codes: list[int] = Field(default_factory=lambda: [0])


# ---------------------------------------------------------------------------
# Data Flow models
# ---------------------------------------------------------------------------

class DataFlowColumn(BaseModel):
    name: str
    data_type: DataType = DataType.WSTRING
    length: int = 0
    precision: int = 0
    scale: int = 0
    code_page: int = 0
    nullable: bool = True
    properties: dict[str, Any] = Field(default_factory=dict)


class DataFlowComponent(BaseModel):
    """Base model for a single Data Flow component (source, transform, destination)."""
    id: str
    name: str
    component_class_id: str  # GUID or class name
    component_type: str  # OLEDBSource, Lookup, DerivedColumn, etc.
    input_columns: list[DataFlowColumn] = Field(default_factory=list)
    output_columns: list[DataFlowColumn] = Field(default_factory=list)
    properties: dict[str, Any] = Field(default_factory=dict)
    connection_id: str | None = None
    key_columns: list[str] = Field(default_factory=list)


class DataFlowPath(BaseModel):
    id: str
    name: str
    start_id: str  # component output ID
    end_id: str    # component input ID


class DataFlowTask(SSISTask):
    task_type: TaskType = TaskType.DATA_FLOW
    components: list[DataFlowComponent] = Field(default_factory=list)
    paths: list[DataFlowPath] = Field(default_factory=list)
    default_buffer_max_rows: int = 10000
    default_buffer_size: int = 10485760
    ingestion_pattern: IngestionPattern = IngestionPattern.UNKNOWN


# ---------------------------------------------------------------------------
# Container models
# ---------------------------------------------------------------------------

class SequenceContainer(SSISTask):
    task_type: TaskType = TaskType.SEQUENCE
    tasks: list[SSISTask] = Field(default_factory=list)
    constraints: list[PrecedenceConstraint] = Field(default_factory=list)


class ForEachLoopContainer(SSISTask):
    task_type: TaskType = TaskType.FOREACH_LOOP
    enumerator_type: ForEachEnumeratorType = ForEachEnumeratorType.FILE
    enumerator_config: dict[str, Any] = Field(default_factory=dict)
    variable_mappings: list[dict[str, Any]] = Field(default_factory=list)
    tasks: list[SSISTask] = Field(default_factory=list)
    constraints: list[PrecedenceConstraint] = Field(default_factory=list)


class ForLoopContainer(SSISTask):
    task_type: TaskType = TaskType.FOR_LOOP
    init_expression: str | None = None
    eval_expression: str | None = None
    assign_expression: str | None = None
    tasks: list[SSISTask] = Field(default_factory=list)
    constraints: list[PrecedenceConstraint] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Event Handler
# ---------------------------------------------------------------------------

class EventHandler(BaseModel):
    event_name: str  # OnError, OnPreExecute, OnPostExecute, OnWarning, etc.
    parent_task_id: str | None = None  # None = package-level
    parent_task_name: str | None = None
    tasks: list[SSISTask] = Field(default_factory=list)
    constraints: list[PrecedenceConstraint] = Field(default_factory=list)
    variables: list[SSISVariable] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Top-level package model
# ---------------------------------------------------------------------------

class SSISPackage(BaseModel):
    """Complete representation of a .dtsx SSIS package."""
    id: str
    name: str
    source_file: str  # absolute path or identifier
    description: str = ""
    protection_level: ProtectionLevel = ProtectionLevel.DONT_SAVE_SENSITIVE
    creator_computer_name: str = ""
    creator_name: str = ""
    creation_date: str = ""
    package_format_version: int = 8

    # Top-level components
    connection_managers: list[SSISConnectionManager] = Field(default_factory=list)
    variables: list[SSISVariable] = Field(default_factory=list)
    parameters: list[SSISParameter] = Field(default_factory=list)
    tasks: list[SSISTask] = Field(default_factory=list)  # top-level control-flow items
    constraints: list[PrecedenceConstraint] = Field(default_factory=list)
    event_handlers: list[EventHandler] = Field(default_factory=list)

    # Source metadata
    raw_xml: str | None = None  # kept for debugging; not serialized to ADF

    # SQL Agent schedule (populated from msdb when available)
    sql_agent_schedule: SqlAgentSchedule | None = None


# ---------------------------------------------------------------------------
# Analysis result models (used by analyzers)
# ---------------------------------------------------------------------------

class GapItem(BaseModel):
    task_id: str
    task_name: str
    task_type: str
    severity: str  # "manual_required", "warning", "info"
    message: str
    recommendation: str = ""


class ConversionWarning(BaseModel):
    """Structured warning emitted during parsing, conversion, or generation."""
    phase: str  # "parse", "analyze", "convert", "generate", "deploy"
    severity: str  # "error", "warning", "info"
    source: str  # module/component that emitted the warning
    message: str
    task_name: str = ""
    task_id: str = ""
    detail: str = ""  # additional context (e.g. fallback value used)


class ComplexityScore(BaseModel):
    package_name: str
    total_tasks: int = 0
    script_task_count: int = 0
    data_flow_task_count: int = 0
    data_flow_component_count: int = 0
    loop_container_count: int = 0
    event_handler_count: int = 0
    nest_depth: int = 0
    unknown_task_count: int = 0
    cross_db_ref_count: int = 0
    linked_server_ref_count: int = 0
    score: int = 0  # 0-100 complexity score
    effort_estimate: str = ""  # "Low", "Medium", "High", "Very High"


class PackageAnalysisResult(BaseModel):
    package: SSISPackage
    complexity: ComplexityScore
    gaps: list[GapItem] = Field(default_factory=list)
    dependency_order: list[str] = Field(default_factory=list)  # task IDs in execution order
    warnings: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# ADF artifact reference (output of conversion)
# ---------------------------------------------------------------------------

class AdfArtifact(BaseModel):
    artifact_type: str  # "pipeline", "linkedService", "dataset", "dataFlow", "trigger"
    name: str
    file_path: str  # absolute path to generated JSON file
    payload: dict[str, Any] = Field(default_factory=dict)


class ConversionResult(BaseModel):
    package_name: str
    source_file: str
    artifacts: list[AdfArtifact] = Field(default_factory=list)
    script_task_stubs: list[str] = Field(default_factory=list)  # paths to Azure Function stubs
    warnings: list[str] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    success: bool = True
