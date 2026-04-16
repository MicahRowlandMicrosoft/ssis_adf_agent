"""Tests for the 5 medium-impact improvements."""
from __future__ import annotations

import pytest
from types import SimpleNamespace
from typing import Any

from ssis_adf_agent.converters.dispatcher import ConverterDispatcher
from ssis_adf_agent.converters.control_flow.event_handler_converter import EventHandlerConverter
from ssis_adf_agent.converters.control_flow.execute_sql_converter import apply_schema_remap
from ssis_adf_agent.generators.linked_service_generator import (
    generate_linked_services,
    _resolve_ir_name,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _task(task_type: str, name: str = "TestTask", **extra) -> SimpleNamespace:
    """Create a minimal task-like object for the dispatcher."""
    from ssis_adf_agent.parsers.models import TaskType
    tt = TaskType(task_type)
    defaults = dict(
        id=f"id_{name}",
        name=name,
        task_type=tt,
        description="",
        precedence_constraints=[],
    )
    defaults.update(extra)
    return SimpleNamespace(**defaults)


def _cm(name: str = "TestConn", **overrides) -> SimpleNamespace:
    """Minimal ConnectionManager-like object."""
    from ssis_adf_agent.parsers.models import ConnectionManagerType
    defaults = dict(
        id=name,
        name=name,
        type=ConnectionManagerType.OLEDB,
        server="mysvr",
        database="mydb",
        connection_string="Server=mysvr;Database=mydb",
        username=None,
        password=None,
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


# ===================================================================
# Area 1: New task type converters
# ===================================================================

class TestBulkInsertConverter:
    def test_emits_copy_activity(self):
        t = _task("BulkInsertTask", name="LoadCSV")
        dispatcher = ConverterDispatcher()
        acts = dispatcher.convert_task(t, [], {})
        assert len(acts) == 1
        assert acts[0]["type"] == "Copy"
        assert "MANUAL REVIEW" in acts[0]["description"]
        assert acts[0]["typeProperties"]["sink"]["type"] == "AzureSqlSink"

    def test_depends_on_carried(self):
        t = _task("BulkInsertTask", name="BulkLoad")
        dispatcher = ConverterDispatcher()
        acts = dispatcher.convert_task(t, [], {})
        assert acts[0]["dependsOn"] == []


class TestWebServiceConverter:
    def test_emits_web_activity(self):
        t = _task("WebServiceTask", name="CallAPI")
        dispatcher = ConverterDispatcher()
        acts = dispatcher.convert_task(t, [], {})
        assert len(acts) == 1
        assert acts[0]["type"] == "WebActivity"
        assert acts[0]["typeProperties"]["method"] == "POST"

    def test_url_from_attribute(self):
        t = _task("WebServiceTask", name="CallAPI", url="https://api.example.com")
        dispatcher = ConverterDispatcher()
        acts = dispatcher.convert_task(t, [], {})
        assert acts[0]["typeProperties"]["url"] == "https://api.example.com"


class TestXMLConverter:
    def test_emits_script_activity(self):
        t = _task("XMLTask", name="TransformXML", operation_type="XSLT")
        dispatcher = ConverterDispatcher()
        acts = dispatcher.convert_task(t, [], {})
        assert len(acts) == 1
        assert acts[0]["type"] == "Script"
        assert "XSLT" in acts[0]["description"]

    def test_default_operation_type(self):
        t = _task("XMLTask", name="ProcessXML")
        dispatcher = ConverterDispatcher()
        acts = dispatcher.convert_task(t, [], {})
        assert "Unknown" in acts[0]["description"]


class TestTransferSQLConverter:
    def test_emits_script_activity(self):
        t = _task("TransferSQLServerObjectsTask", name="MigrateSchema",
                   source_connection_id="SrcDB", destination_connection_id="DstDB",
                   transfer_objects="Tables,StoredProcedures")
        dispatcher = ConverterDispatcher()
        acts = dispatcher.convert_task(t, [], {})
        assert len(acts) == 1
        assert acts[0]["type"] == "Script"
        assert "SrcDB" in acts[0]["description"]
        assert "DstDB" in acts[0]["description"]

    def test_fallback_connection_ids(self):
        t = _task("TransferSQLServerObjectsTask", name="Transfer")
        dispatcher = ConverterDispatcher()
        acts = dispatcher.convert_task(t, [], {})
        assert "source" in acts[0]["description"]


# ===================================================================
# Area 2: ForEach NODELIST / ADO_NET_SCHEMA enumerators
# ===================================================================

class TestForEachSchemaEnumerator:
    def _make_foreach(self, enum_type, config=None):
        from ssis_adf_agent.parsers.models import ForEachLoopContainer, ForEachEnumeratorType
        return ForEachLoopContainer(
            id="fe1", name="SchemaLoop",
            enumerator_type=ForEachEnumeratorType(enum_type),
            enumerator_config=config or {},
        )

    def test_ado_net_schema_emits_lookup(self):
        from ssis_adf_agent.converters.control_flow.foreach_converter import ForEachConverter
        conv = ForEachConverter()
        task = self._make_foreach("ForEachADONetSchemaRowsetEnumerator", {"SchemaRowsetName": "Tables"})
        acts = conv.convert(task, [], {})
        # Should contain both the Lookup prerequisite and the ForEach
        names = [a["name"] for a in acts]
        assert any("Lookup_" in n for n in names)
        # The Lookup should query INFORMATION_SCHEMA
        lookup = [a for a in acts if a["type"] == "Lookup"][0]
        assert "INFORMATION_SCHEMA.TABLES" in lookup["typeProperties"]["source"]["sqlReaderQuery"]

    def test_ado_net_schema_columns(self):
        from ssis_adf_agent.converters.control_flow.foreach_converter import ForEachConverter
        conv = ForEachConverter()
        task = self._make_foreach("ForEachADONetSchemaRowsetEnumerator", {"SchemaRowsetName": "Columns"})
        acts = conv.convert(task, [], {})
        lookup = [a for a in acts if a["type"] == "Lookup"][0]
        assert "INFORMATION_SCHEMA.COLUMNS" in lookup["typeProperties"]["source"]["sqlReaderQuery"]

    def test_nodelist_emits_lookup_with_xpath_comment(self):
        from ssis_adf_agent.converters.control_flow.foreach_converter import ForEachConverter
        conv = ForEachConverter()
        task = self._make_foreach("ForEachNodeListEnumerator",
                                   {"OuterXPathString": "//item", "VariableName": "xmlDoc"})
        acts = conv.convert(task, [], {})
        lookup = [a for a in acts if a["type"] == "Lookup"][0]
        assert "//item" in lookup["typeProperties"]["source"]["sqlReaderQuery"]
        assert "xmlDoc" in lookup["typeProperties"]["source"]["sqlReaderQuery"]

    def test_nodelist_items_expression_references_lookup(self):
        from ssis_adf_agent.converters.control_flow.foreach_converter import ForEachConverter
        conv = ForEachConverter()
        task = self._make_foreach("ForEachNodeListEnumerator")
        acts = conv.convert(task, [], {})
        foreach = [a for a in acts if a["type"] == "ForEach"][0]
        assert "Lookup_SchemaLoop" in foreach["typeProperties"]["items"]["value"]


# ===================================================================
# Area 3: Event handler types
# ===================================================================

class TestEventHandlerTypes:
    def _handler(self, event_name: str) -> SimpleNamespace:
        return SimpleNamespace(event_name=event_name, tasks=[])

    def test_on_error(self):
        conv = EventHandlerConverter()
        result = conv.convert_handler(self._handler("OnError"), "MainPipeline")
        assert result["trigger_condition"] == "Failed"

    def test_on_task_failed(self):
        conv = EventHandlerConverter()
        result = conv.convert_handler(self._handler("OnTaskFailed"), "MainPipeline")
        assert result["trigger_condition"] == "Failed"

    def test_on_post_execute(self):
        conv = EventHandlerConverter()
        result = conv.convert_handler(self._handler("OnPostExecute"), "MainPipeline")
        assert result["trigger_condition"] == "Succeeded"

    def test_on_warning(self):
        conv = EventHandlerConverter()
        result = conv.convert_handler(self._handler("OnWarning"), "MainPipeline")
        assert result["trigger_condition"] == "Completed"

    def test_on_information(self):
        conv = EventHandlerConverter()
        result = conv.convert_handler(self._handler("OnInformation"), "MainPipeline")
        assert result["trigger_condition"] == "Succeeded"

    def test_on_progress(self):
        conv = EventHandlerConverter()
        result = conv.convert_handler(self._handler("OnProgress"), "MainPipeline")
        assert result["trigger_condition"] == "Succeeded"

    def test_unknown_event_defaults_completed(self):
        conv = EventHandlerConverter()
        result = conv.convert_handler(self._handler("CustomEvent"), "MainPipeline")
        assert result["trigger_condition"] == "Completed"

    def test_sub_pipeline_name_includes_event(self):
        conv = EventHandlerConverter()
        result = conv.convert_handler(self._handler("OnWarning"), "PL_Main")
        assert result["sub_pipeline_name"] == "PL_PL_Main_EH_OnWarning"


# ===================================================================
# Area 4: SQL schema_remap applied to all activity types
# ===================================================================

class TestSchemaRemapAllActivities:
    def test_remap_lookup_sql(self):
        remap = {"OldDB.dbo": "NewDB.staging"}
        result = apply_schema_remap("SELECT * FROM OldDB.dbo.Customers", remap)
        assert "[NewDB].[staging].Customers" in result
        assert "OldDB" not in result

    def test_remap_stored_proc_name(self):
        remap = {"LegacyDB.dbo": "ModernDB.app"}
        result = apply_schema_remap("LegacyDB.dbo.usp_GetData", remap)
        assert "[ModernDB].[app].usp_GetData" in result

    def test_remap_bracketted_names(self):
        remap = {"OldDB.dbo": "NewDB.staging"}
        result = apply_schema_remap("SELECT * FROM [OldDB].[dbo].Orders", remap)
        assert "[NewDB].[staging].Orders" in result

    def test_remap_none_sql_returns_none(self):
        assert apply_schema_remap(None, {"a.b": "c.d"}) is None

    def test_remap_none_mapping_returns_original(self):
        assert apply_schema_remap("SELECT 1", None) == "SELECT 1"

    def test_multiple_remaps_in_one_statement(self):
        remap = {"A.dbo": "B.stg", "C.dbo": "D.prd"}
        sql = "SELECT * FROM A.dbo.T1 JOIN C.dbo.T2 ON T1.id = T2.id"
        result = apply_schema_remap(sql, remap)
        assert "[B].[stg].T1" in result
        assert "[D].[prd].T2" in result


# ===================================================================
# Area 5: Multi-IR support
# ===================================================================

class TestMultiIRSupport:
    def test_ir_mapping_matches_glob(self):
        cm = _cm(name="CONN_EU_Sales")
        mapping = {"*_EU_*": "SHIR_Europe", "*_US_*": "SHIR_US"}
        ir = _resolve_ir_name(cm, True, "DefaultSHIR", "AutoResolve", mapping)
        assert ir == "SHIR_Europe"

    def test_ir_mapping_case_insensitive(self):
        cm = _cm(name="conn_us_data")
        mapping = {"*_US_*": "SHIR_US"}
        ir = _resolve_ir_name(cm, True, "DefaultSHIR", "AutoResolve", mapping)
        assert ir == "SHIR_US"

    def test_ir_mapping_first_match_wins(self):
        cm = _cm(name="CONN_EU_US_hybrid")
        mapping = {"*_EU_*": "SHIR_Europe", "*_US_*": "SHIR_US"}
        ir = _resolve_ir_name(cm, True, "DefaultSHIR", "AutoResolve", mapping)
        assert ir == "SHIR_Europe"

    def test_ir_mapping_fallback_to_default(self):
        cm = _cm(name="CONN_APAC_data")
        mapping = {"*_EU_*": "SHIR_Europe"}
        ir = _resolve_ir_name(cm, True, "DefaultSHIR", "AutoResolve", mapping)
        assert ir == "DefaultSHIR"

    def test_ir_mapping_cloud_fallback(self):
        cm = _cm(name="NoMatch")
        ir = _resolve_ir_name(cm, False, "DefaultSHIR", "CloudIR", {"*_EU_*": "SHIR_EU"})
        assert ir == "CloudIR"

    def test_ir_mapping_none_uses_default(self):
        cm = _cm(name="anything")
        ir = _resolve_ir_name(cm, True, "MySHIR", "CloudIR", None)
        assert ir == "MySHIR"

    def test_generate_with_ir_mapping(self, tmp_path):
        from ssis_adf_agent.parsers.models import ConnectionManagerType
        pkg = SimpleNamespace(
            connection_managers=[
                _cm(name="EU_Sales", server="10.0.0.1"),
                _cm(name="US_Orders", server="10.0.0.2", id="US_Orders"),
            ],
        )
        mapping = {"eu_*": "SHIR_EU", "us_*": "SHIR_US"}
        results = generate_linked_services(
            pkg, tmp_path,
            on_prem_ir_name="DefaultSHIR",
            ir_mapping=mapping,
        )
        ir_names = [
            r["properties"]["connectVia"]["referenceName"]
            for r in results
        ]
        assert "SHIR_EU" in ir_names
        assert "SHIR_US" in ir_names
