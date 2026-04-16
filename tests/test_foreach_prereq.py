"""Tests for ForEach converter — prerequisite activity generation."""
from __future__ import annotations

import pytest

from ssis_adf_agent.converters.control_flow.foreach_converter import ForEachConverter
from ssis_adf_agent.parsers.models import (
    ForEachEnumeratorType,
    ForEachLoopContainer,
    PrecedenceConstraint,
    SSISTask,
    TaskType,
)


def _make_foreach(
    enumerator_type: ForEachEnumeratorType,
    name: str = "Loop Files",
    config: dict | None = None,
) -> ForEachLoopContainer:
    return ForEachLoopContainer(
        id="foreach-1",
        name=name,
        task_type=TaskType.FOREACH_LOOP,
        enumerator_type=enumerator_type,
        enumerator_config=config or {},
        tasks=[],
        constraints=[],
    )


class TestFileEnumerator:
    """ForEachFileEnumerator should emit a GetMetadata prerequisite."""

    def test_emits_get_metadata_activity(self):
        task = _make_foreach(
            ForEachEnumeratorType.FILE,
            name="Loop Files",
            config={"Folder": "/data/inbox", "FileSpec": "*.csv"},
        )
        converter = ForEachConverter()
        activities = converter.convert(task, [], {})

        assert len(activities) == 2
        gm = activities[0]
        fe = activities[1]

        assert gm["type"] == "GetMetadata"
        assert gm["name"] == "GetMetadata_Loop_Files"
        assert "childItems" in gm["typeProperties"]["fieldList"]

    def test_foreach_depends_on_get_metadata(self):
        task = _make_foreach(ForEachEnumeratorType.FILE, name="Loop Files")
        converter = ForEachConverter()
        activities = converter.convert(task, [], {})

        fe = activities[1]
        assert fe["type"] == "ForEach"
        assert any(
            dep["activity"] == "GetMetadata_Loop_Files"
            for dep in fe["dependsOn"]
        )

    def test_items_expr_references_get_metadata(self):
        task = _make_foreach(ForEachEnumeratorType.FILE, name="Scan")
        converter = ForEachConverter()
        activities = converter.convert(task, [], {})

        fe = activities[1]
        expr = fe["typeProperties"]["items"]["value"]
        assert "GetMetadata_Scan" in expr
        assert "childItems" in expr

    def test_get_metadata_uses_folder_config(self):
        task = _make_foreach(
            ForEachEnumeratorType.FILE,
            config={"Folder": "/data/inbox", "FileSpec": "*.txt"},
        )
        converter = ForEachConverter()
        activities = converter.convert(task, [], {})

        gm = activities[0]
        ds_params = gm["typeProperties"]["dataset"]["parameters"]
        assert ds_params["FolderPath"] == "/data/inbox"
        assert ds_params["FileSpec"] == "*.txt"

    def test_get_metadata_default_folder(self):
        task = _make_foreach(ForEachEnumeratorType.FILE, config={})
        converter = ForEachConverter()
        activities = converter.convert(task, [], {})

        gm = activities[0]
        ds_params = gm["typeProperties"]["dataset"]["parameters"]
        assert "pipeline().parameters.FolderPath" in ds_params["FolderPath"]

    def test_get_metadata_has_retry_policy(self):
        task = _make_foreach(ForEachEnumeratorType.FILE)
        converter = ForEachConverter()
        activities = converter.convert(task, [], {})

        gm = activities[0]
        assert gm["policy"]["retry"] == 2

    def test_get_metadata_inherits_depends_on(self):
        """Parent dependsOn flows to GetMetadata, not the ForEach."""
        parent_task = SSISTask(id="parent-1", name="Previous Step")
        constraint = PrecedenceConstraint(
            id="pc-1", from_task_id="parent-1", to_task_id="foreach-1",
        )
        task = _make_foreach(ForEachEnumeratorType.FILE, name="Loop")
        converter = ForEachConverter()
        activities = converter.convert(
            task, [constraint], {"parent-1": parent_task}
        )

        gm = activities[0]
        fe = activities[1]
        # GetMetadata should have the original dependency
        assert any(dep["activity"] == "Previous Step" for dep in gm["dependsOn"])
        # ForEach should depend only on GetMetadata
        assert fe["dependsOn"][0]["activity"] == "GetMetadata_Loop"


class TestADOEnumerator:
    """ForEachADOEnumerator should emit a Lookup prerequisite."""

    def test_emits_lookup_activity(self):
        task = _make_foreach(
            ForEachEnumeratorType.ADO,
            name="Loop Records",
            config={"Query": "SELECT id, name FROM users"},
        )
        converter = ForEachConverter()
        activities = converter.convert(task, [], {})

        assert len(activities) == 2
        lookup = activities[0]
        fe = activities[1]

        assert lookup["type"] == "Lookup"
        assert lookup["name"] == "Lookup_Loop_Records"
        assert lookup["typeProperties"]["firstRowOnly"] is False

    def test_lookup_uses_query_from_config(self):
        task = _make_foreach(
            ForEachEnumeratorType.ADO,
            config={"Query": "SELECT * FROM orders"},
        )
        converter = ForEachConverter()
        activities = converter.convert(task, [], {})

        lookup = activities[0]
        assert lookup["typeProperties"]["source"]["sqlReaderQuery"] == "SELECT * FROM orders"

    def test_lookup_placeholder_when_no_query(self):
        task = _make_foreach(
            ForEachEnumeratorType.ADO,
            config={"VariableName": "User::rs"},
        )
        converter = ForEachConverter()
        activities = converter.convert(task, [], {})

        lookup = activities[0]
        query = lookup["typeProperties"]["source"]["sqlReaderQuery"]
        assert "TODO" in query
        assert "rs" in query

    def test_foreach_depends_on_lookup(self):
        task = _make_foreach(ForEachEnumeratorType.ADO, name="ADO Loop")
        converter = ForEachConverter()
        activities = converter.convert(task, [], {})

        fe = activities[1]
        assert fe["dependsOn"][0]["activity"] == "Lookup_ADO_Loop"

    def test_items_expr_references_lookup(self):
        task = _make_foreach(ForEachEnumeratorType.ADO, name="ADO Loop")
        converter = ForEachConverter()
        activities = converter.convert(task, [], {})

        fe = activities[1]
        expr = fe["typeProperties"]["items"]["value"]
        assert "Lookup_ADO_Loop" in expr
        assert "output.value" in expr

    def test_lookup_has_retry_policy(self):
        task = _make_foreach(ForEachEnumeratorType.ADO)
        converter = ForEachConverter()
        activities = converter.convert(task, [], {})

        assert activities[0]["policy"]["retry"] == 2


class TestNoPrerequisite:
    """ITEM, VARIABLE, and unknown enumerators should NOT emit prerequisites."""

    def test_item_enumerator_single_activity(self):
        task = _make_foreach(
            ForEachEnumeratorType.ITEM,
            config={"Items": '[{"a":1},{"a":2}]'},
        )
        converter = ForEachConverter()
        activities = converter.convert(task, [], {})

        assert len(activities) == 1
        assert activities[0]["type"] == "ForEach"

    def test_variable_enumerator_single_activity(self):
        task = _make_foreach(
            ForEachEnumeratorType.VARIABLE,
            config={"VariableName": "User::MyList"},
        )
        converter = ForEachConverter()
        activities = converter.convert(task, [], {})

        assert len(activities) == 1
        assert activities[0]["type"] == "ForEach"

    def test_unknown_enumerator_single_activity(self):
        task = _make_foreach(ForEachEnumeratorType.SMO)
        converter = ForEachConverter()
        activities = converter.convert(task, [], {})

        assert len(activities) == 1

    def test_item_expression_uses_json(self):
        task = _make_foreach(
            ForEachEnumeratorType.ITEM,
            config={"Items": '[1,2,3]'},
        )
        converter = ForEachConverter()
        activities = converter.convert(task, [], {})

        expr = activities[0]["typeProperties"]["items"]["value"]
        assert "@json(" in expr

    def test_variable_expression(self):
        task = _make_foreach(
            ForEachEnumeratorType.VARIABLE,
            config={"VariableName": "User::FileList"},
        )
        converter = ForEachConverter()
        activities = converter.convert(task, [], {})

        expr = activities[0]["typeProperties"]["items"]["value"]
        assert "variables('FileList')" in expr
