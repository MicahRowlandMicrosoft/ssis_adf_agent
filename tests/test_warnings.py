"""Tests for the structured warnings system.

Tests cover:
1. WarningsCollector context manager collects warnings
2. warn() outside a collector context (just logs, doesn't crash)
3. warn() inside a collector context populates warnings list
4. Parser emits warnings for unknown task types
5. Dispatcher emits warnings for unsupported task types
6. Source/destination converters emit warnings for missing connection IDs
7. Transformation converter emits warnings for unsupported components
"""
from __future__ import annotations

import logging

import pytest

from ssis_adf_agent.parsers.models import (
    ConversionWarning,
    SSISTask,
    TaskType,
    DataFlowComponent,
)
from ssis_adf_agent.warnings_collector import WarningsCollector, warn


# ---------------------------------------------------------------------------
# Core collector behaviour
# ---------------------------------------------------------------------------

class TestWarningsCollector:
    def test_collector_collects_warnings(self):
        with WarningsCollector() as wc:
            warn(phase="parse", severity="warning", source="test", message="test msg")
        assert len(wc.warnings) == 1
        assert wc.warnings[0].message == "test msg"
        assert wc.warnings[0].phase == "parse"

    def test_collector_clears_after_exit(self):
        with WarningsCollector() as wc:
            warn(phase="parse", severity="info", source="test", message="inside")
        # New collector should start empty
        with WarningsCollector() as wc2:
            pass
        assert len(wc2.warnings) == 0

    def test_warn_outside_collector_does_not_crash(self, caplog):
        with caplog.at_level(logging.WARNING, logger="ssis_adf_agent"):
            warn(phase="convert", severity="warning", source="test", message="no collector")
        assert "no collector" in caplog.text

    def test_warn_logs_regardless_of_collector(self, caplog):
        with caplog.at_level(logging.WARNING, logger="ssis_adf_agent"):
            with WarningsCollector() as wc:
                warn(phase="convert", severity="warning", source="test", message="logged and collected")
        assert "logged and collected" in caplog.text
        assert len(wc.warnings) == 1

    def test_multiple_warnings_collected(self):
        with WarningsCollector() as wc:
            warn(phase="parse", severity="warning", source="a", message="first")
            warn(phase="convert", severity="error", source="b", message="second")
            warn(phase="generate", severity="info", source="c", message="third")
        assert len(wc.warnings) == 3
        assert [w.source for w in wc.warnings] == ["a", "b", "c"]

    def test_warning_model_fields(self):
        with WarningsCollector() as wc:
            warn(
                phase="convert",
                severity="warning",
                source="dispatcher",
                message="No converter",
                task_name="My Task",
                task_id="GUID-123",
                detail="Using fallback",
            )
        w = wc.warnings[0]
        assert isinstance(w, ConversionWarning)
        assert w.phase == "convert"
        assert w.severity == "warning"
        assert w.source == "dispatcher"
        assert w.task_name == "My Task"
        assert w.task_id == "GUID-123"
        assert w.detail == "Using fallback"

    def test_warning_model_dump(self):
        with WarningsCollector() as wc:
            warn(phase="parse", severity="info", source="test", message="dump test")
        d = wc.warnings[0].model_dump()
        assert isinstance(d, dict)
        assert d["phase"] == "parse"
        assert d["message"] == "dump test"

    def test_severity_logging_levels(self, caplog):
        with caplog.at_level(logging.DEBUG, logger="ssis_adf_agent"):
            with WarningsCollector():
                warn(phase="p", severity="error", source="t", message="err msg")
                warn(phase="p", severity="warning", source="t", message="warn msg")
                warn(phase="p", severity="info", source="t", message="info msg")
        assert "err msg" in caplog.text
        assert "warn msg" in caplog.text
        assert "info msg" in caplog.text


# ---------------------------------------------------------------------------
# Parser warnings: unknown task type
# ---------------------------------------------------------------------------

class TestParserWarnings:
    def test_unknown_task_type_emits_warning(self):
        """When the parser encounters an unknown task type, a warning should be emitted."""
        from ssis_adf_agent.parsers.ssis_parser import SSISParser

        # Minimal .dtsx with an unknown task type inside <DTS:Executables>
        dtsx = """<?xml version="1.0"?>
<DTS:Executable xmlns:DTS="www.microsoft.com/SqlServer/Dts"
    DTS:ObjectName="TestPackage"
    DTS:DTSID="{PKG-GUID}"
    DTS:ExecutableType="Microsoft.Package">
  <DTS:Executables>
    <DTS:Executable
        DTS:ObjectName="WeirdTask"
        DTS:DTSID="{T1-GUID}"
        DTS:ExecutableType="SomeVendor.CustomTask" />
  </DTS:Executables>
</DTS:Executable>"""

        parser = SSISParser()
        with WarningsCollector() as wc:
            pkg = parser.parse_xml(dtsx, source_identifier="test.dtsx")

        # Parser should emit a warning about unknown task type
        unknown_warnings = [
            w for w in wc.warnings
            if "unknown" in w.message.lower() or "unrecognised" in w.message.lower()
            or w.source == "ssis_parser"
        ]
        assert len(unknown_warnings) >= 1, (
            f"Expected warning for unknown task type, got: {[w.message for w in wc.warnings]}"
        )


# ---------------------------------------------------------------------------
# Dispatcher warnings: fallback converter
# ---------------------------------------------------------------------------

class TestDispatcherWarnings:
    def test_fallback_converter_emits_warning(self):
        """When the dispatcher uses the fallback converter, a warning is emitted."""
        from ssis_adf_agent.converters.dispatcher import ConverterDispatcher

        task = SSISTask(
            id="T1",
            name="UnsupportedTask",
            task_type=TaskType.UNKNOWN,
            properties={"original_type": "SomeVendor.CustomTask"},
        )

        dispatcher = ConverterDispatcher()
        with WarningsCollector() as wc:
            activities = dispatcher.convert_task(task, [], {})

        # Should have emitted a warning about unsupported task type
        assert any("no converter" in w.message.lower() or "unsupported" in w.message.lower()
                    for w in wc.warnings), (
            f"Expected dispatcher warning, got: {[w.message for w in wc.warnings]}"
        )
        # Should still produce a placeholder activity
        assert len(activities) >= 1


# ---------------------------------------------------------------------------
# Source/destination converter: missing connection_id
# ---------------------------------------------------------------------------

class TestSourceDestinationWarnings:
    def _make_component(self, component_type: str, name: str = "TestComp") -> DataFlowComponent:
        return DataFlowComponent(
            id="C1",
            name=name,
            component_class_id="test-class-id",
            component_type=component_type,
            properties={},
            input_columns=[],
            output_columns=[],
            connection_id=None,
        )

    def test_source_converter_warns_on_missing_connection(self):
        from ssis_adf_agent.converters.data_flow.source_converter import convert_source

        comp = self._make_component("OleDbSource")
        with WarningsCollector() as wc:
            result = convert_source(comp)

        conn_warnings = [w for w in wc.warnings if "connection" in w.message.lower()]
        assert len(conn_warnings) >= 1, (
            f"Expected connection warning, got: {[w.message for w in wc.warnings]}"
        )

    def test_destination_converter_warns_on_missing_connection(self):
        from ssis_adf_agent.converters.data_flow.destination_converter import convert_destination

        comp = self._make_component("OleDbDestination")
        with WarningsCollector() as wc:
            result = convert_destination(comp)

        conn_warnings = [w for w in wc.warnings if "connection" in w.message.lower()]
        assert len(conn_warnings) >= 1, (
            f"Expected connection warning, got: {[w.message for w in wc.warnings]}"
        )


# ---------------------------------------------------------------------------
# Transformation converter: unsupported component
# ---------------------------------------------------------------------------

class TestTransformationWarnings:
    def test_unsupported_transform_emits_warning(self):
        from ssis_adf_agent.converters.data_flow.transformation_converter import convert_transformation

        comp = DataFlowComponent(
            id="T1",
            name="WeirdTransform",
            component_class_id="vendor-class",
            component_type="SomeVendor.CustomTransform",
            properties={},
            input_columns=[],
            output_columns=[],
        )

        with WarningsCollector() as wc:
            result = convert_transformation(comp)

        assert any(
            "unsupported" in w.message.lower()
            or "unknown" in w.message.lower()
            or "no adf equivalent" in w.message.lower()
            for w in wc.warnings
        ), f"Expected unsupported warning, got: {[w.message for w in wc.warnings]}"

    def test_script_component_emits_warning(self):
        from ssis_adf_agent.converters.data_flow.transformation_converter import convert_transformation

        comp = DataFlowComponent(
            id="T1",
            name="ScriptTransform",
            component_class_id="script-class",
            component_type="Script Component",
            properties={},
            input_columns=[],
            output_columns=[],
        )

        with WarningsCollector() as wc:
            result = convert_transformation(comp)

        assert any(
            "script" in w.message.lower() or "manual" in w.message.lower()
            for w in wc.warnings
        ), f"Expected script warning, got: {[w.message for w in wc.warnings]}"
