"""
Tests for data flow transformation improvements:
- Expression translator (SSIS → ADF)
- Transformation converters (DerivedColumn, Lookup, ConditionalSplit, Aggregate, Sort, MergeJoin)
- DSL script generation with topology
- Parser column-level property extraction
"""
from __future__ import annotations

import json
import pytest
from pathlib import Path

from ssis_adf_agent.parsers.models import (
    DataFlowColumn,
    DataFlowComponent,
    DataFlowPath,
    DataFlowTask,
    DataType,
    SSISPackage,
    TaskType,
)
from ssis_adf_agent.translators.ssis_expression_translator import translate_expression
from ssis_adf_agent.converters.data_flow.transformation_converter import convert_transformation
from ssis_adf_agent.generators.dataflow_generator import generate_data_flows, _build_dsl_script


# ==========================================================================
# Expression Translator Tests
# ==========================================================================


class TestExpressionTranslator:
    """Tests for SSIS → ADF expression translation."""

    def test_column_reference(self):
        assert translate_expression("[ColumnName]") == "ColumnName"

    def test_multiple_column_refs(self):
        result = translate_expression("[FirstName] + [LastName]")
        assert "FirstName" in result
        assert "LastName" in result
        assert "[" not in result

    def test_upper_function(self):
        result = translate_expression("UPPER([Name])")
        assert result == "upper(Name)"

    def test_lower_function(self):
        result = translate_expression("LOWER([City])")
        assert result == "lower(City)"

    def test_substring_function(self):
        result = translate_expression("SUBSTRING([Code], 1, 3)")
        assert result == "substring(Code, 1, 3)"

    def test_len_to_length(self):
        result = translate_expression("LEN([Name])")
        assert result == "length(Name)"

    def test_replace_function(self):
        result = translate_expression('REPLACE([Col], "a", "b")')
        assert result == 'replace(Col, "a", "b")'

    def test_ltrim_rtrim(self):
        assert translate_expression("LTRIM([Col])") == "ltrim(Col)"
        assert translate_expression("RTRIM([Col])") == "rtrim(Col)"

    def test_isnull_function(self):
        result = translate_expression("ISNULL([Col])")
        assert result == "isNull(Col)"

    def test_getdate(self):
        result = translate_expression("GETDATE()")
        assert result == "currentTimestamp()"

    def test_year_function(self):
        result = translate_expression("YEAR([DateCol])")
        assert result == "year(DateCol)"

    def test_month_function(self):
        result = translate_expression("MONTH([DateCol])")
        assert result == "month(DateCol)"

    def test_abs_function(self):
        result = translate_expression("ABS([Amount])")
        assert result == "abs(Amount)"

    def test_round_function(self):
        result = translate_expression("ROUND([Price], 2)")
        assert result == "round(Price, 2)"

    def test_cast_dt_i4(self):
        result = translate_expression("(DT_I4)[StringCol]")
        assert result == "toInteger(StringCol)"

    def test_cast_dt_wstr(self):
        result = translate_expression("(DT_WSTR)[IntCol]")
        assert result == "toString(IntCol)"

    def test_cast_dt_dbtimestamp(self):
        result = translate_expression("(DT_DBTIMESTAMP)[DateStr]")
        assert result == "toTimestamp(DateStr)"

    def test_cast_with_length(self):
        result = translate_expression("(DT_STR,50,1252)[Col]")
        assert result == "toString(Col)"

    def test_cast_dt_bool(self):
        result = translate_expression("(DT_BOOL)[Flag]")
        assert result == "toBoolean(Flag)"

    def test_typed_null(self):
        result = translate_expression("NULL(DT_I4)")
        assert result == "toInteger(null())"

    def test_typed_null_string(self):
        result = translate_expression("NULL(DT_WSTR)")
        assert result == "toString(null())"

    def test_dateadd_days(self):
        result = translate_expression('DATEADD("dd", 1, [DateCol])')
        assert result == "addDays(DateCol, 1)"

    def test_dateadd_months(self):
        result = translate_expression('DATEADD("mm", -3, [DateCol])')
        assert result == "addMonths(DateCol, -3)"

    def test_dateadd_hours(self):
        result = translate_expression('DATEADD("hh", 2, [StartTime])')
        assert result == "addHours(StartTime, 2)"

    def test_ternary_to_iif(self):
        result = translate_expression('[Status] == "A" ? "Active" : "Inactive"')
        assert "iif(" in result
        assert "Active" in result
        assert "Inactive" in result

    def test_empty_expression(self):
        assert translate_expression("") == ""
        assert translate_expression(None) == ""

    def test_complex_nested(self):
        expr = 'UPPER(LTRIM([Name]))'
        result = translate_expression(expr)
        assert "upper(" in result
        assert "ltrim(" in result

    def test_replacenull_to_coalesce(self):
        result = translate_expression('REPLACENULL([Col], "default")')
        assert result == 'coalesce(Col, "default")'


# ==========================================================================
# DerivedColumn Converter Tests
# ==========================================================================


class TestDerivedColumnConverter:
    """Tests for DerivedColumn transformation with column-level properties."""

    def _make_derived(self, output_cols: list[DataFlowColumn]) -> DataFlowComponent:
        return DataFlowComponent(
            id="dc1",
            name="DeriveFullName",
            component_class_id="test",
            component_type="DerivedColumn",
            output_columns=output_cols,
            properties={},
        )

    def test_reads_expression_from_column_properties(self):
        cols = [
            DataFlowColumn(
                name="FullName",
                data_type=DataType.WSTRING,
                properties={"Expression": '[FirstName] + " " + [LastName]'},
            ),
        ]
        result = convert_transformation(self._make_derived(cols))
        assert result["type"] == "DerivedColumn"
        col_expr = result["typeProperties"]["columns"][0]
        assert col_expr["name"] == "FullName"
        assert "FirstName" in col_expr["expression"]
        assert "LastName" in col_expr["expression"]
        assert "[" not in col_expr["expression"]  # brackets should be removed

    def test_friendly_expression_fallback(self):
        cols = [
            DataFlowColumn(
                name="Upper_Name",
                data_type=DataType.WSTRING,
                properties={"FriendlyExpression": "UPPER([Name])"},
            ),
        ]
        result = convert_transformation(self._make_derived(cols))
        expr = result["typeProperties"]["columns"][0]["expression"]
        assert expr == "upper(Name)"

    def test_no_expression_gives_todo(self):
        cols = [DataFlowColumn(name="Mystery", data_type=DataType.WSTRING)]
        result = convert_transformation(self._make_derived(cols))
        expr = result["typeProperties"]["columns"][0]["expression"]
        assert "TODO" in expr

    def test_multiple_columns(self):
        cols = [
            DataFlowColumn(name="Col1", properties={"Expression": "UPPER([A])"}),
            DataFlowColumn(name="Col2", properties={"Expression": "LOWER([B])"}),
        ]
        result = convert_transformation(self._make_derived(cols))
        columns = result["typeProperties"]["columns"]
        assert len(columns) == 2
        assert columns[0]["expression"] == "upper(A)"
        assert columns[1]["expression"] == "lower(B)"

    def test_cast_expression_in_column(self):
        cols = [
            DataFlowColumn(
                name="IntCol",
                data_type=DataType.INT32,
                properties={"Expression": "(DT_I4)[StringCol]"},
            ),
        ]
        result = convert_transformation(self._make_derived(cols))
        expr = result["typeProperties"]["columns"][0]["expression"]
        assert "toInteger" in expr


# ==========================================================================
# Lookup Converter Tests
# ==========================================================================


class TestLookupConverter:
    """Tests for Lookup transformation with join conditions."""

    def _make_lookup(
        self, input_cols: list[DataFlowColumn] | None = None
    ) -> DataFlowComponent:
        return DataFlowComponent(
            id="lk1",
            name="Lookup Country",
            component_class_id="test",
            component_type="Lookup",
            input_columns=input_cols or [],
            properties={},
        )

    def test_extracts_join_conditions_from_input_cols(self):
        cols = [
            DataFlowColumn(
                name="CountryCode",
                properties={"JoinToReferenceColumn": "Code"},
            ),
        ]
        result = convert_transformation(self._make_lookup(cols))
        conds = result["typeProperties"]["conditions"]
        assert len(conds) == 1
        assert conds[0]["leftColumn"] == "CountryCode"
        assert conds[0]["rightColumn"] == "Code"

    def test_multiple_join_keys(self):
        cols = [
            DataFlowColumn(name="CustID", properties={"JoinToReferenceColumn": "CustomerID"}),
            DataFlowColumn(name="Region", properties={"JoinToReferenceColumn": "RegionCode"}),
        ]
        result = convert_transformation(self._make_lookup(cols))
        conds = result["typeProperties"]["conditions"]
        assert len(conds) == 2

    def test_no_join_keys_gives_todo(self):
        result = convert_transformation(self._make_lookup())
        conds = result["typeProperties"]["conditions"]
        assert len(conds) == 1
        assert "TODO" in conds[0]["leftColumn"]

    def test_lookup_type_is_lookup(self):
        result = convert_transformation(self._make_lookup())
        assert result["type"] == "Lookup"

    def test_has_dataset_reference(self):
        result = convert_transformation(self._make_lookup())
        ref = result["typeProperties"]["lookupTable"]
        assert ref["type"] == "DatasetReference"
        assert "Lookup_Country" in ref["referenceName"]


# ==========================================================================
# ConditionalSplit Converter Tests
# ==========================================================================


class TestConditionalSplitConverter:
    """Tests for ConditionalSplit with output-level properties."""

    def _make_split(
        self, output_conditions: list[dict] | None = None
    ) -> DataFlowComponent:
        props: dict = {}
        if output_conditions:
            props["_output_conditions"] = output_conditions
        return DataFlowComponent(
            id="cs1",
            name="Split By Status",
            component_class_id="test",
            component_type="ConditionalSplit",
            properties=props,
            output_columns=[],
        )

    def test_reads_conditions_from_output_properties(self):
        conds = [
            {"output_name": "Active Records", "Expression": '[Status] == "A"', "EvaluationOrder": "0"},
            {"output_name": "Inactive Records", "Expression": '[Status] == "I"', "EvaluationOrder": "1"},
        ]
        result = convert_transformation(self._make_split(conds))
        split_conds = result["typeProperties"]["conditions"]
        assert len(split_conds) == 2
        # ADF Mapping Data Flow node names must be alphanumeric only.
        assert split_conds[0]["name"] == "ActiveRecords"
        assert "Status" in split_conds[0]["expression"]
        assert "[" not in split_conds[0]["expression"]

    def test_sorted_by_evaluation_order(self):
        conds = [
            {"output_name": "Second", "Expression": "[B] > 10", "EvaluationOrder": "1"},
            {"output_name": "First", "Expression": "[A] > 5", "EvaluationOrder": "0"},
        ]
        result = convert_transformation(self._make_split(conds))
        split_conds = result["typeProperties"]["conditions"]
        assert split_conds[0]["name"] == "First"
        assert split_conds[1]["name"] == "Second"

    def test_translates_ssis_expressions(self):
        conds = [
            {"output_name": "HasName", "Expression": "ISNULL([Name]) == FALSE"},
        ]
        result = convert_transformation(self._make_split(conds))
        expr = result["typeProperties"]["conditions"][0]["expression"]
        assert "isNull" in expr

    def test_no_conditions_fallback(self):
        result = convert_transformation(self._make_split())
        # Should not crash; conditions list may be empty
        assert result["type"] == "ConditionalSplit"


# ==========================================================================
# Aggregate Converter Tests
# ==========================================================================


class TestAggregateConverter:
    """Tests for Aggregate with AggregationType from column properties."""

    def _make_aggregate(self, cols: list[DataFlowColumn]) -> DataFlowComponent:
        return DataFlowComponent(
            id="agg1",
            name="Sum By Customer",
            component_class_id="test",
            component_type="Aggregate",
            output_columns=cols,
            properties={},
        )

    def test_group_by_detected(self):
        cols = [
            DataFlowColumn(name="CustID", properties={"AggregationType": "0"}),
            DataFlowColumn(name="TotalAmt", properties={"AggregationType": "4"}),
        ]
        result = convert_transformation(self._make_aggregate(cols))
        assert "CustID" in result["typeProperties"]["groupBy"]
        aggs = result["typeProperties"]["aggregations"]
        assert len(aggs) == 1
        assert aggs[0]["function"] == "sum"
        assert aggs[0]["column"] == "TotalAmt"

    def test_count_aggregation(self):
        cols = [
            DataFlowColumn(name="Region", properties={"AggregationType": "0"}),
            DataFlowColumn(name="OrderCount", properties={"AggregationType": "6"}),
        ]
        result = convert_transformation(self._make_aggregate(cols))
        aggs = result["typeProperties"]["aggregations"]
        assert aggs[0]["function"] == "count"

    def test_count_distinct(self):
        cols = [
            DataFlowColumn(name="UniqueCustomers", properties={"AggregationType": "7"}),
        ]
        result = convert_transformation(self._make_aggregate(cols))
        aggs = result["typeProperties"]["aggregations"]
        assert aggs[0]["function"] == "countDistinct"

    def test_multiple_group_by(self):
        cols = [
            DataFlowColumn(name="Year", properties={"AggregationType": "0"}),
            DataFlowColumn(name="Region", properties={"AggregationType": "0"}),
            DataFlowColumn(name="Revenue", properties={"AggregationType": "4"}),
        ]
        result = convert_transformation(self._make_aggregate(cols))
        assert len(result["typeProperties"]["groupBy"]) == 2
        assert len(result["typeProperties"]["aggregations"]) == 1

    def test_min_max_avg(self):
        cols = [
            DataFlowColumn(name="MinPrice", properties={"AggregationType": "1"}),
            DataFlowColumn(name="MaxPrice", properties={"AggregationType": "2"}),
            DataFlowColumn(name="AvgPrice", properties={"AggregationType": "5"}),
        ]
        result = convert_transformation(self._make_aggregate(cols))
        aggs = result["typeProperties"]["aggregations"]
        funcs = [a["function"] for a in aggs]
        assert "min" in funcs
        assert "max" in funcs
        assert "avg" in funcs

    def test_no_agg_info_defaults_to_group_by(self):
        cols = [DataFlowColumn(name="Col1")]
        result = convert_transformation(self._make_aggregate(cols))
        assert "Col1" in result["typeProperties"]["groupBy"]


# ==========================================================================
# Sort Converter Tests
# ==========================================================================


class TestSortConverter:
    """Tests for Sort with SortKeyPosition from column properties."""

    def _make_sort(self, cols: list[DataFlowColumn]) -> DataFlowComponent:
        return DataFlowComponent(
            id="sort1",
            name="Sort Output",
            component_class_id="test",
            component_type="Sort",
            output_columns=cols,
            properties={},
        )

    def test_ascending_sort(self):
        cols = [
            DataFlowColumn(name="LastName", properties={"SortKeyPosition": "1"}),
        ]
        result = convert_transformation(self._make_sort(cols))
        conds = result["typeProperties"]["sortConditions"]
        assert len(conds) == 1
        assert conds[0]["column"] == "LastName"
        assert conds[0]["order"] == "asc"

    def test_descending_sort(self):
        cols = [
            DataFlowColumn(name="Score", properties={"SortKeyPosition": "-1"}),
        ]
        result = convert_transformation(self._make_sort(cols))
        conds = result["typeProperties"]["sortConditions"]
        assert conds[0]["order"] == "desc"

    def test_multi_key_sort_ordered_by_position(self):
        cols = [
            DataFlowColumn(name="City", properties={"SortKeyPosition": "2"}),
            DataFlowColumn(name="State", properties={"SortKeyPosition": "1"}),
            DataFlowColumn(name="Zip"),  # not a sort key
        ]
        result = convert_transformation(self._make_sort(cols))
        conds = result["typeProperties"]["sortConditions"]
        assert len(conds) == 2
        assert conds[0]["column"] == "State"
        assert conds[1]["column"] == "City"

    def test_no_sort_keys_gives_todo(self):
        cols = [DataFlowColumn(name="Col1")]
        result = convert_transformation(self._make_sort(cols))
        conds = result["typeProperties"]["sortConditions"]
        assert "TODO" in conds[0]["column"]


# ==========================================================================
# MergeJoin Converter Tests
# ==========================================================================


class TestMergeJoinConverter:
    """Tests for MergeJoin transformation."""

    def _make_merge_join(
        self,
        input_cols: list[DataFlowColumn] | None = None,
        join_type: str = "inner",
    ) -> DataFlowComponent:
        return DataFlowComponent(
            id="mj1",
            name="Join Orders",
            component_class_id="test",
            component_type="MergeJoin",
            input_columns=input_cols or [],
            properties={"JoinType": join_type},
        )

    def test_join_type_from_properties(self):
        result = convert_transformation(self._make_merge_join(join_type="left"))
        assert result["typeProperties"]["joinType"] == "left"

    def test_join_type_is_join(self):
        result = convert_transformation(self._make_merge_join())
        assert result["type"] == "Join"

    def test_no_keys_gives_todo(self):
        result = convert_transformation(self._make_merge_join())
        conds = result["typeProperties"]["conditions"]
        assert "TODO" in conds[0]["leftColumn"]


# ==========================================================================
# Other Converter Tests
# ==========================================================================


class TestOtherTransformations:
    """Tests for UnionAll, Merge, DataConversion, RowCount, Multicast, etc."""

    def test_union_all(self):
        comp = DataFlowComponent(
            id="u1", name="Union All", component_class_id="t",
            component_type="UnionAll",
        )
        result = convert_transformation(comp)
        assert result["type"] == "Union"

    def test_merge_is_union_with_note(self):
        comp = DataFlowComponent(
            id="m1", name="Merge", component_class_id="t",
            component_type="Merge",
        )
        result = convert_transformation(comp)
        assert result["type"] == "Union"
        assert "pre-sorted" in result["description"]

    def test_multicast_returns_none(self):
        comp = DataFlowComponent(
            id="mc1", name="Multicast", component_class_id="t",
            component_type="Multicast",
        )
        assert convert_transformation(comp) is None

    def test_data_conversion(self):
        comp = DataFlowComponent(
            id="cv1", name="Convert Types", component_class_id="t",
            component_type="DataConversion",
            output_columns=[
                DataFlowColumn(name="IntCol", data_type=DataType.INT32, length=0, scale=0),
            ],
        )
        result = convert_transformation(comp)
        assert result["type"] == "Cast"
        assert result["typeProperties"]["columns"][0]["name"] == "IntCol"

    def test_row_count(self):
        comp = DataFlowComponent(
            id="rc1", name="Row Count", component_class_id="t",
            component_type="RowCount",
            properties={"VariableName": "User::RowCount"},
        )
        result = convert_transformation(comp)
        assert result["typeProperties"]["variableName"] == "RowCount"

    def test_script_component(self):
        comp = DataFlowComponent(
            id="sc1", name="Script Transform", component_class_id="t",
            component_type="ScriptComponent",
        )
        result = convert_transformation(comp)
        assert "MANUAL REVIEW" in result["description"]

    def test_unsupported_fuzzy_lookup(self):
        comp = DataFlowComponent(
            id="fl1", name="Fuzzy Lookup", component_class_id="t",
            component_type="FuzzyLookup",
        )
        result = convert_transformation(comp)
        assert "UNSUPPORTED" in result["description"]

    def test_unknown_type_gets_generic(self):
        comp = DataFlowComponent(
            id="x1", name="Custom Thing", component_class_id="t",
            component_type="NeverSeenBefore",
        )
        result = convert_transformation(comp)
        assert "Unknown component type" in result["description"]


# ==========================================================================
# DSL Script Generation Tests
# ==========================================================================


class TestDSLScriptGeneration:
    """Tests for _build_dsl_script with topology-aware chaining."""

    def _source(self, name: str) -> dict:
        return {"name": name, "description": "", "typeProperties": {}}

    def _sink(self, name: str) -> dict:
        return {"name": name, "description": "", "typeProperties": {}}

    def _transform(self, name: str, ttype: str, type_props: dict | None = None) -> dict:
        return {
            "name": name,
            "description": "",
            "type": ttype,
            "typeProperties": type_props or {},
        }

    def test_source_emits_correct_dsl(self):
        script = _build_dsl_script(
            [self._source("src1")], [], [self._sink("sink1")]
        )
        assert "source(" in script
        assert "~> src1" in script

    def test_sink_emits_correct_dsl(self):
        script = _build_dsl_script(
            [self._source("src1")], [], [self._sink("sink1")]
        )
        assert "sink(" in script
        assert "~> sink1" in script

    def test_derived_column_in_dsl(self):
        t = self._transform("derive1", "DerivedColumn", {
            "columns": [{"name": "FullName", "expression": "upper(Name)"}],
        })
        script = _build_dsl_script(
            [self._source("src1")], [t], [self._sink("sink1")]
        )
        assert "derive(" in script
        assert "FullName = upper(Name)" in script
        assert "~> derive1" in script

    def test_lookup_in_dsl(self):
        t = self._transform("lkp1", "Lookup", {
            "conditions": [{"leftColumn": "CustID", "rightColumn": "ID"}],
        })
        script = _build_dsl_script(
            [self._source("src1")], [t], [self._sink("sink1")]
        )
        assert "lookup(" in script
        assert "CustID == ID" in script

    def test_conditional_split_in_dsl(self):
        t = self._transform("split1", "ConditionalSplit", {
            "conditions": [
                {"name": "Active", "expression": "Status == 'A'"},
                {"name": "Inactive", "expression": "Status == 'I'"},
            ],
        })
        script = _build_dsl_script(
            [self._source("src1")], [t], [self._sink("sink1")]
        )
        assert "split(" in script
        assert "Active:" in script

    def test_aggregate_in_dsl(self):
        t = self._transform("agg1", "Aggregate", {
            "groupBy": ["Region"],
            "aggregations": [{"column": "Amount", "function": "sum"}],
        })
        script = _build_dsl_script(
            [self._source("src1")], [t], [self._sink("sink1")]
        )
        assert "aggregate(" in script
        assert "groupBy(Region)" in script
        assert "Amount = sum(Amount)" in script

    def test_sort_in_dsl(self):
        t = self._transform("sort1", "Sort", {
            "sortConditions": [{"column": "Name", "order": "asc"}],
        })
        script = _build_dsl_script(
            [self._source("src1")], [t], [self._sink("sink1")]
        )
        assert "sort(" in script
        assert "asc(Name)" in script

    def test_join_in_dsl(self):
        t = self._transform("join1", "Join", {
            "joinType": "left",
            "conditions": [{"leftColumn": "ID", "rightColumn": "CustID"}],
        })
        script = _build_dsl_script(
            [self._source("src1")], [t], [self._sink("sink1")]
        )
        assert "join(" in script
        assert "ID == CustID" in script
        assert "joinType: 'left'" in script

    def test_sink_with_key_columns(self):
        script = _build_dsl_script(
            [self._source("src1")], [], [self._sink("sink1")],
            key_columns=["CustomerID"],
        )
        assert "keys: ['CustomerID']" in script
        assert "upsertable: true" in script

    def test_linear_chain_fallback(self):
        """Without a task, components chain linearly."""
        t1 = self._transform("t1", "DerivedColumn", {"columns": []})
        t2 = self._transform("t2", "Sort", {"sortConditions": []})
        script = _build_dsl_script(
            [self._source("src1")], [t1, t2], [self._sink("sink1")]
        )
        # t1 should reference src1, t2 should reference src1 (flat fallback),
        # sink should reference t2
        assert "~> t1" in script
        assert "~> t2" in script
        assert "~> sink1" in script


# ==========================================================================
# DataFlowColumn properties test
# ==========================================================================


class TestDataFlowColumnProperties:
    """Tests that DataFlowColumn carries properties dict."""

    def test_default_empty_properties(self):
        col = DataFlowColumn(name="Col1", data_type=DataType.INT32)
        assert col.properties == {}

    def test_with_expression_property(self):
        col = DataFlowColumn(
            name="Derived",
            properties={"Expression": "UPPER([Name])", "FriendlyExpression": "UPPER([Name])"},
        )
        assert col.properties["Expression"] == "UPPER([Name])"

    def test_with_sort_key_position(self):
        col = DataFlowColumn(name="SortCol", properties={"SortKeyPosition": "1"})


# ==========================================================================
# Audit-driven improvements
# ==========================================================================

from ssis_adf_agent.converters.data_flow.source_converter import convert_source
from ssis_adf_agent.converters.data_flow.destination_converter import convert_destination


class TestSourceOutputColumns:
    """Source output schema should be populated in DSL from parsed columns."""

    def test_source_output_columns_in_dsl(self):
        comp = DataFlowComponent(
            id="src1", name="OLE_SRC", component_class_id="test",
            component_type="OleDbSource",
            output_columns=[
                DataFlowColumn(name="CustomerID", data_type=DataType.INT32),
                DataFlowColumn(name="Name", data_type=DataType.WSTRING),
            ],
        )
        source = convert_source(comp)
        assert "_output_columns" in source
        assert len(source["_output_columns"]) == 2

        # Build script and verify schema appears
        script = _build_dsl_script([source], [], [{"name": "sink1"}])
        assert "CustomerID as integer" in script
        assert "Name as string" in script
        assert "/* TODO: declare output schema */" not in script

    def test_source_no_columns_falls_back_to_todo(self):
        comp = DataFlowComponent(
            id="src1", name="OLE_SRC", component_class_id="test",
            component_type="OleDbSource",
            output_columns=[],
        )
        source = convert_source(comp)
        script = _build_dsl_script([source], [], [{"name": "sink1"}])
        # ADF DSL rejects comment-only operator bodies, so a placeholder column
        # is emitted instead of a bare TODO comment.
        assert "_ssis_todo as string" in script


class TestSinkColumnMapping:
    """Sink should emit select/mapColumn from input columns."""

    def test_sink_with_input_columns_does_not_emit_orphan_select(self):
        # Implicit `select(mapColumn(...)) ~> <sink>_mapped` would create a node
        # that is not declared in typeProperties.transformations, causing ADF
        # to reject the data flow with "Unable to parse". The sink relies on
        # allowSchemaDrift to pass columns through instead.
        sink_dict = {
            "name": "sink1",
            "_input_columns": [
                DataFlowColumn(name="ID", data_type=DataType.INT32),
                DataFlowColumn(name="Amount", data_type=DataType.DOUBLE),
            ],
            "_key_columns": [],
        }
        script = _build_dsl_script(
            [{"name": "src1"}], [], [sink_dict]
        )
        assert "select(mapColumn(" not in script
        assert "_mapped" not in script
        assert "allowSchemaDrift: true" in script

    def test_sink_without_input_columns_no_map(self):
        sink_dict = {"name": "sink1", "_input_columns": [], "_key_columns": []}
        script = _build_dsl_script([{"name": "src1"}], [], [sink_dict])
        assert "select(mapColumn(" not in script


class TestUpsertGuard:
    """allowUpsert should only be true when key columns are present."""

    def test_no_keys_disables_upsert(self):
        comp = DataFlowComponent(
            id="dst1", name="OLE_DST", component_class_id="test",
            component_type="OleDbDestination",
            key_columns=[],
        )
        sink = convert_destination(comp)
        assert sink["typeProperties"]["allowUpsert"] is False

    def test_with_keys_enables_upsert(self):
        comp = DataFlowComponent(
            id="dst1", name="OLE_DST", component_class_id="test",
            component_type="OleDbDestination",
            key_columns=["CustomerID"],
        )
        sink = convert_destination(comp)
        assert sink["typeProperties"]["allowUpsert"] is True


class TestSinkDslNoKeysNoUpsert:
    """DSL sink should emit upsertable: false when no key columns."""

    def test_no_keys_upsertable_false(self):
        script = _build_dsl_script(
            [{"name": "src1"}], [], [{"name": "sink1"}],
            key_columns=None,
        )
        assert "upsertable: false" in script
        assert "upsertable: true" not in script


class TestConditionalSplitDisjoint:
    """ConditionalSplit should emit disjoint: true (SSIS first-match semantics)."""

    def test_disjoint_true(self):
        t = {
            "name": "split1",
            "type": "ConditionalSplit",
            "typeProperties": {
                "conditions": [
                    {"name": "Active", "expression": "Status == 'A'"},
                ],
            },
        }
        script = _build_dsl_script(
            [{"name": "src1"}], [t], [{"name": "sink1"}]
        )
        assert "disjoint: true" in script


class TestDataFlowColumnProperties2:
    """Remaining column property assertions."""

    def test_with_sort_key_position(self):
        col = DataFlowColumn(name="SortCol", properties={"SortKeyPosition": "1"})
        assert col.properties["SortKeyPosition"] == "1"

    def test_with_aggregation_type(self):
        col = DataFlowColumn(name="AggCol", properties={"AggregationType": "4"})
        assert col.properties["AggregationType"] == "4"

    def test_with_join_reference(self):
        col = DataFlowColumn(
            name="JoinCol", properties={"JoinToReferenceColumn": "RefCol"}
        )
        assert col.properties["JoinToReferenceColumn"] == "RefCol"


# ==========================================================================
# Integration: full generate_data_flows with real components
# ==========================================================================


class TestDataFlowGeneratorIntegration:
    """Integration tests for generate_data_flows with populated properties."""

    def _make_package_with_derived_column(self) -> SSISPackage:
        """Package with a data flow that has source → DerivedColumn → destination."""
        src = DataFlowComponent(
            id="src1", name="OLE_SRC", component_class_id="test",
            component_type="OleDbSource",
            properties={"SqlCommand": "SELECT * FROM Customers"},
        )
        derived = DataFlowComponent(
            id="dc1", name="Derive FullName", component_class_id="test",
            component_type="DerivedColumn",
            output_columns=[
                DataFlowColumn(
                    name="FullName",
                    data_type=DataType.WSTRING,
                    length=200,
                    properties={"Expression": '[FirstName] + " " + [LastName]'},
                ),
            ],
        )
        dest = DataFlowComponent(
            id="dst1", name="OLE_DST", component_class_id="test",
            component_type="OleDbDestination",
        )
        task = DataFlowTask(
            id="df1", name="Load Customers", task_type=TaskType.DATA_FLOW,
            components=[src, derived, dest],
            paths=[
                DataFlowPath(id="p1", name="p1", start_id="src1", end_id="dc1"),
                DataFlowPath(id="p2", name="p2", start_id="dc1", end_id="dst1"),
            ],
        )
        return SSISPackage(
            id="pkg1", name="TestPkg", source_file="test.dtsx",
            tasks=[task],
        )

    def test_generates_data_flow_with_derived_columns(self, tmp_path):
        pkg = self._make_package_with_derived_column()
        results = generate_data_flows(pkg, tmp_path)
        assert len(results) == 1
        df = results[0]
        assert df["name"] == "DF_TestPkg_Load_Customers"

        # Transformations should include the DerivedColumn
        transforms = df["properties"]["typeProperties"]["transformations"]
        assert len(transforms) == 1
        # type/typeProperties are stripped (not valid ADF JSON); verify via description + script
        assert "DerivedColumn" in transforms[0]["description"]
        script = "\n".join(df["properties"]["typeProperties"]["scriptLines"])
        assert "FullName" in script
        assert "FirstName" in script

    def test_dsl_script_contains_derive(self, tmp_path):
        pkg = self._make_package_with_derived_column()
        results = generate_data_flows(pkg, tmp_path)
        script = "\n".join(results[0]["properties"]["typeProperties"]["scriptLines"])
        assert "derive(" in script
        assert "FullName" in script

    def test_json_file_written(self, tmp_path):
        pkg = self._make_package_with_derived_column()
        generate_data_flows(pkg, tmp_path)
        json_path = tmp_path / "dataflow" / "DF_TestPkg_Load_Customers.json"
        assert json_path.exists()
        data = json.loads(json_path.read_text())
        assert data["name"] == "DF_TestPkg_Load_Customers"

    def _make_package_with_aggregate(self) -> SSISPackage:
        src = DataFlowComponent(
            id="src1", name="Source", component_class_id="test",
            component_type="OleDbSource",
        )
        agg = DataFlowComponent(
            id="agg1", name="Aggregate Sales", component_class_id="test",
            component_type="Aggregate",
            output_columns=[
                DataFlowColumn(name="Region", properties={"AggregationType": "0"}),
                DataFlowColumn(name="TotalSales", properties={"AggregationType": "4"}),
            ],
        )
        dest = DataFlowComponent(
            id="dst1", name="Destination", component_class_id="test",
            component_type="OleDbDestination",
        )
        task = DataFlowTask(
            id="df1", name="Aggregate Flow", task_type=TaskType.DATA_FLOW,
            components=[src, agg, dest],
            paths=[],
        )
        return SSISPackage(
            id="pkg1", name="AggPkg", source_file="test.dtsx",
            tasks=[task],
        )

    def test_aggregate_in_generated_data_flow(self, tmp_path):
        pkg = self._make_package_with_aggregate()
        results = generate_data_flows(pkg, tmp_path)
        assert len(results) == 1
        transforms = results[0]["properties"]["typeProperties"]["transformations"]
        assert len(transforms) == 1
        # type/typeProperties are stripped (not valid ADF JSON); verify via description + script
        assert "Aggregate" in transforms[0]["description"]
        script = "\n".join(results[0]["properties"]["typeProperties"]["scriptLines"])
        assert "groupBy(" in script
        assert "Region" in script
        assert "sum(" in script

    def test_simple_copy_not_generated_as_dataflow(self, tmp_path):
        """Single source + single destination with no transforms → no data flow."""
        src = DataFlowComponent(
            id="s1", name="Src", component_class_id="t",
            component_type="OleDbSource",
        )
        dst = DataFlowComponent(
            id="d1", name="Dst", component_class_id="t",
            component_type="OleDbDestination",
        )
        task = DataFlowTask(
            id="df1", name="Simple Copy", task_type=TaskType.DATA_FLOW,
            components=[src, dst], paths=[],
        )
        pkg = SSISPackage(id="p1", name="P", source_file="t.dtsx", tasks=[task])
        results = generate_data_flows(pkg, tmp_path)
        assert len(results) == 0  # handled as Copy Activity, not data flow

    def test_dataflow_json_serializable_with_columns(self, tmp_path):
        """Generated dataflow JSON must serialize cleanly when components carry parsed columns.

        Regression: source/sink converters attach Pydantic ``DataFlowColumn`` objects
        as private metadata for DSL emission; those must be stripped before json.dumps.
        """
        src = DataFlowComponent(
            id="c1", name="Src", component_type="OleDbSource", component_class_id="x",
            connection_id="cm1",
            output_columns=[
                DataFlowColumn(name="id", data_type=DataType.INT32),
                DataFlowColumn(name="nm", data_type=DataType.WSTRING),
            ],
        )
        dst = DataFlowComponent(
            id="c2", name="Dest", component_type="OleDbDestination", component_class_id="x",
            connection_id="cm1",
            input_columns=[
                DataFlowColumn(name="id", data_type=DataType.INT32),
                DataFlowColumn(name="nm", data_type=DataType.WSTRING),
            ],
            key_columns=["id"],
        )
        trans = DataFlowComponent(
            id="c3", name="Tr", component_type="DerivedColumn", component_class_id="x",
            output_columns=[
                DataFlowColumn(
                    name="full_name",
                    data_type=DataType.WSTRING,
                    properties={"FriendlyExpression": "[nm] + ' suffix'"},
                ),
            ],
        )
        paths = [
            DataFlowPath(id="p1", name="p1", start_id="c1\\Output 0", end_id="c3\\Input 0"),
            DataFlowPath(id="p2", name="p2", start_id="c3\\Output 0", end_id="c2\\Input 0"),
        ]
        task = DataFlowTask(
            id="t1", name="DFT", task_type=TaskType.DATA_FLOW,
            components=[src, trans, dst], paths=paths,
        )
        pkg = SSISPackage(id="pk", name="P", source_file="p.dtsx", tasks=[task])
        results = generate_data_flows(pkg, tmp_path)
        assert len(results) == 1
        # File on disk must be valid JSON (would crash on Pydantic models)
        df = json.loads((tmp_path / "dataflow" / "DF_P_DFT.json").read_text())
        # Private fields must not leak into the JSON
        for s in df["properties"]["typeProperties"]["sources"]:
            assert "_output_columns" not in s
        for s in df["properties"]["typeProperties"]["sinks"]:
            assert "_input_columns" not in s
            assert "_key_columns" not in s
        # Script should include the typed source schema; no orphan _mapped node
        script = "\n".join(df["properties"]["typeProperties"]["scriptLines"])
        assert "id as integer" in script
        assert "nm as string" in script
        assert "_mapped" not in script
