"""Tests for the control-flow expression translator.

Tests cover:
1. Variable reference syntax variants (@[User::X], @(User::X), @User::X)
2. Comparison operators → ADF prefix functions
3. Boolean operators (&&, ||, !)
4. Arithmetic operators → add/sub/mul/div/mod
5. String concatenation → concat
6. Function calls (GETDATE, DATEADD, LEN, etc.)
7. Nested/compound expressions
8. For Loop integration (negate, init, assign)
9. Namespace stripping utility
10. Execute SQL / Execute Package namespace fix
"""
from __future__ import annotations

import pytest

from ssis_adf_agent.translators.control_flow_expression import (
    translate_control_flow_expr,
    strip_variable_namespace,
)


# ---------------------------------------------------------------------------
# Variable references
# ---------------------------------------------------------------------------

class TestVariableReferences:
    def test_bracket_syntax(self):
        result = translate_control_flow_expr("@[User::Counter]")
        assert result == "variables('Counter')"

    def test_paren_syntax(self):
        result = translate_control_flow_expr("@(User::Counter)")
        assert result == "variables('Counter')"

    def test_bare_syntax(self):
        result = translate_control_flow_expr("@User::Counter")
        assert result == "variables('Counter')"

    def test_system_variable(self):
        result = translate_control_flow_expr("@[System::PackageName]")
        assert result == "variables('PackageName')"

    def test_no_namespace(self):
        # Edge case: @[Counter] should still work
        result = translate_control_flow_expr("@[Counter]")
        assert result == "variables('Counter')"


# ---------------------------------------------------------------------------
# Comparison operators → prefix functions
# ---------------------------------------------------------------------------

class TestComparisonOperators:
    def test_equals(self):
        result = translate_control_flow_expr("@[User::X] == 10")
        assert result == "equals(variables('X'), 10)"

    def test_not_equals(self):
        result = translate_control_flow_expr("@[User::X] != 10")
        assert result == "not(equals(variables('X'), 10))"

    def test_less_than(self):
        result = translate_control_flow_expr("@[User::Counter] < 100")
        assert result == "less(variables('Counter'), 100)"

    def test_greater_than(self):
        result = translate_control_flow_expr("@[User::Counter] > 0")
        assert result == "greater(variables('Counter'), 0)"

    def test_less_or_equals(self):
        result = translate_control_flow_expr("@[User::X] <= 50")
        assert result == "lessOrEquals(variables('X'), 50)"

    def test_greater_or_equals(self):
        result = translate_control_flow_expr("@[User::X] >= 1")
        assert result == "greaterOrEquals(variables('X'), 1)"

    def test_equals_true(self):
        result = translate_control_flow_expr("@[User::Flag] == true")
        assert result == "equals(variables('Flag'), true)"

    def test_equals_string(self):
        result = translate_control_flow_expr('@[User::Status] == "Active"')
        assert result == "equals(variables('Status'), 'Active')"

    def test_variable_vs_variable(self):
        result = translate_control_flow_expr("@[User::Current] < @[User::Max]")
        assert result == "less(variables('Current'), variables('Max'))"


# ---------------------------------------------------------------------------
# Boolean operators
# ---------------------------------------------------------------------------

class TestBooleanOperators:
    def test_and(self):
        result = translate_control_flow_expr(
            "@[User::Counter] > 0 && @[User::Counter] < 100"
        )
        assert result == "and(greater(variables('Counter'), 0), less(variables('Counter'), 100))"

    def test_or(self):
        result = translate_control_flow_expr(
            "@[User::X] == 1 || @[User::Y] == 2"
        )
        assert result == "or(equals(variables('X'), 1), equals(variables('Y'), 2))"

    def test_not(self):
        result = translate_control_flow_expr("!@[User::Done]")
        assert result == "not(variables('Done'))"

    def test_complex_boolean(self):
        result = translate_control_flow_expr(
            "@[User::A] == 1 && (@[User::B] > 2 || @[User::C] < 3)"
        )
        assert result == "and(equals(variables('A'), 1), or(greater(variables('B'), 2), less(variables('C'), 3)))"


# ---------------------------------------------------------------------------
# Arithmetic operators
# ---------------------------------------------------------------------------

class TestArithmeticOperators:
    def test_add(self):
        result = translate_control_flow_expr("@[User::Counter] + 1")
        assert result == "add(variables('Counter'), 1)"

    def test_subtract(self):
        result = translate_control_flow_expr("@[User::Total] - @[User::Discount]")
        assert result == "sub(variables('Total'), variables('Discount'))"

    def test_multiply(self):
        result = translate_control_flow_expr("@[User::Price] * @[User::Qty]")
        assert result == "mul(variables('Price'), variables('Qty'))"

    def test_divide(self):
        result = translate_control_flow_expr("@[User::Total] / 2")
        assert result == "div(variables('Total'), 2)"

    def test_modulo(self):
        result = translate_control_flow_expr("@[User::Counter] % 10")
        assert result == "mod(variables('Counter'), 10)"

    def test_nested_arithmetic(self):
        result = translate_control_flow_expr("@[User::X] + @[User::Y] * 2")
        # Multiplication has higher precedence: add(X, mul(Y, 2))
        assert result == "add(variables('X'), mul(variables('Y'), 2))"

    def test_parens_override_precedence(self):
        result = translate_control_flow_expr("(@[User::X] + @[User::Y]) * 2")
        assert result == "mul(add(variables('X'), variables('Y')), 2)"

    def test_unary_minus(self):
        result = translate_control_flow_expr("-@[User::X]")
        assert result == "sub(0, variables('X'))"


# ---------------------------------------------------------------------------
# String concatenation
# ---------------------------------------------------------------------------

class TestStringConcat:
    def test_string_plus_variable(self):
        result = translate_control_flow_expr('"Hello " + @[User::Name]')
        assert result == "concat('Hello ', variables('Name'))"

    def test_variable_plus_string(self):
        result = translate_control_flow_expr('@[User::Prefix] + "_suffix"')
        assert result == "concat(variables('Prefix'), '_suffix')"

    def test_chained_concat(self):
        result = translate_control_flow_expr('"A" + "B" + "C"')
        assert result == "concat(concat('A', 'B'), 'C')"


# ---------------------------------------------------------------------------
# Function calls
# ---------------------------------------------------------------------------

class TestFunctionCalls:
    def test_getdate(self):
        result = translate_control_flow_expr("GETDATE()")
        assert result == "utcNow()"

    def test_len(self):
        result = translate_control_flow_expr("LEN(@[User::Name])")
        assert result == "length(variables('Name'))"

    def test_upper(self):
        result = translate_control_flow_expr("UPPER(@[User::Name])")
        assert result == "toUpper(variables('Name'))"

    def test_dateadd_days(self):
        result = translate_control_flow_expr('DATEADD("dd", 7, GETDATE())')
        assert result == "addDays(utcNow(), 7)"

    def test_dateadd_months(self):
        result = translate_control_flow_expr('DATEADD("mm", 1, @[User::StartDate])')
        assert result == "addMonths(variables('StartDate'), 1)"

    def test_dateadd_hours(self):
        result = translate_control_flow_expr('DATEADD("hh", -2, @[User::Timestamp])')
        assert result == "addHours(variables('Timestamp'), sub(0, 2))"

    def test_replace(self):
        result = translate_control_flow_expr('REPLACE(@[User::Path], "\\\\", "/")')
        assert result == "replace(variables('Path'), '\\\\', '/')"

    def test_replacenull(self):
        result = translate_control_flow_expr('REPLACENULL(@[User::Val], "default")')
        assert result == "coalesce(variables('Val'), 'default')"

    def test_isnull(self):
        result = translate_control_flow_expr("ISNULL(@[User::X])")
        assert result == "empty(variables('X'))"

    def test_substring(self):
        # SSIS SUBSTRING is 1-based; ADF substring is 0-based → sub(start, 1)
        result = translate_control_flow_expr("SUBSTRING(@[User::Name], 1, 5)")
        assert result == "substring(variables('Name'), sub(1, 1), 5)"


# ---------------------------------------------------------------------------
# Compound / real-world expressions
# ---------------------------------------------------------------------------

class TestCompoundExpressions:
    def test_for_loop_counter_check(self):
        """Typical SSIS for-loop eval: @[User::Counter] < @[User::MaxItems]"""
        result = translate_control_flow_expr("@[User::Counter] < @[User::MaxItems]")
        assert result == "less(variables('Counter'), variables('MaxItems'))"

    def test_for_loop_increment(self):
        """Typical SSIS for-loop assign RHS: @[User::Counter] + 1"""
        result = translate_control_flow_expr("@[User::Counter] + 1")
        assert result == "add(variables('Counter'), 1)"

    def test_len_comparison(self):
        result = translate_control_flow_expr("LEN(@[User::Str]) > 0")
        assert result == "greater(length(variables('Str')), 0)"

    def test_empty_expression(self):
        assert translate_control_flow_expr("") == ""
        assert translate_control_flow_expr(None) == ""

    def test_mixed_syntax_variables(self):
        """Expression mixing @[...] and @(...) syntax"""
        result = translate_control_flow_expr("@[User::A] + @(User::B)")
        assert result == "add(variables('A'), variables('B'))"

    def test_bool_with_function(self):
        result = translate_control_flow_expr(
            "LEN(@[User::Name]) > 0 && @[User::Active] == true"
        )
        assert result == "and(greater(length(variables('Name')), 0), equals(variables('Active'), true))"


# ---------------------------------------------------------------------------
# For Loop converter integration
# ---------------------------------------------------------------------------

class TestForLoopIntegration:
    def test_negate_eval_expression(self):
        from ssis_adf_agent.converters.control_flow.for_loop_converter import (
            _negate_ssis_expression,
        )

        result = _negate_ssis_expression("@[User::Counter] < 10")
        assert result == "@not(less(variables('Counter'), 10))"

    def test_negate_with_paren_syntax(self):
        from ssis_adf_agent.converters.control_flow.for_loop_converter import (
            _negate_ssis_expression,
        )

        result = _negate_ssis_expression("@(User::I) < @(User::Max)")
        assert result == "@not(less(variables('I'), variables('Max')))"

    def test_negate_with_bracket_syntax(self):
        from ssis_adf_agent.converters.control_flow.for_loop_converter import (
            _negate_ssis_expression,
        )

        result = _negate_ssis_expression("@[User::Done] == true")
        assert result == "@not(equals(variables('Done'), true))"

    def test_negate_none(self):
        from ssis_adf_agent.converters.control_flow.for_loop_converter import (
            _negate_ssis_expression,
        )

        result = _negate_ssis_expression(None)
        assert "TODO" in result

    def test_full_for_loop_conversion(self):
        """End-to-end For Loop with @[User::...] syntax"""
        from ssis_adf_agent.parsers.models import ForLoopContainer
        from ssis_adf_agent.converters.control_flow.for_loop_converter import ForLoopConverter

        task = ForLoopContainer(
            id="FL1",
            name="CountLoop",
            init_expression="@[User::Counter] = 0",
            eval_expression="@[User::Counter] < 10",
            assign_expression="@[User::Counter] = @[User::Counter] + 1",
            tasks=[],
        )

        converter = ForLoopConverter()
        activities = converter.convert(task, [], {})

        # Should produce Init + Until
        assert len(activities) == 2
        init_act = activities[0]
        until_act = activities[1]

        assert init_act["type"] == "SetVariable"
        assert init_act["typeProperties"]["variableName"] == "Counter"

        assert until_act["type"] == "Until"
        expr_val = until_act["typeProperties"]["expression"]["value"]
        assert "not(less(variables('Counter'), 10))" in expr_val

        # Inner should have increment activity
        inner = until_act["typeProperties"]["activities"]
        increment = [a for a in inner if "Increment" in a["name"]]
        assert len(increment) == 1
        inc_val = increment[0]["typeProperties"]["value"]["value"]
        assert "add(variables('Counter'), 1)" in inc_val


# ---------------------------------------------------------------------------
# Namespace stripping utility
# ---------------------------------------------------------------------------

class TestStripVariableNamespace:
    def test_user_namespace(self):
        assert strip_variable_namespace("User::Counter") == "Counter"

    def test_system_namespace(self):
        assert strip_variable_namespace("System::PackageName") == "PackageName"

    def test_no_namespace(self):
        assert strip_variable_namespace("Counter") == "Counter"

    def test_empty(self):
        assert strip_variable_namespace("") == ""


# ---------------------------------------------------------------------------
# Execute SQL / Execute Package namespace fix
# ---------------------------------------------------------------------------

class TestConverterNamespaceFix:
    def test_execute_sql_strips_namespace(self):
        from ssis_adf_agent.parsers.models import ExecuteSQLTask, TaskType
        from ssis_adf_agent.converters.control_flow.execute_sql_converter import ExecuteSQLConverter

        task = ExecuteSQLTask(
            id="T1",
            name="RunProc",
            task_type=TaskType.EXECUTE_SQL,
            connection_id="conn1",
            sql_statement="EXEC dbo.MyProc @Param1",
            result_set_type="None",
            parameter_bindings=[
                {"parameter_name": "Param1", "variable": "User::MyVar", "direction": "Input"},
            ],
        )

        converter = ExecuteSQLConverter()
        activities = converter.convert(task, [], {})
        assert len(activities) >= 1

        # Find the activity and check parameter binding
        act = activities[0]
        props = act.get("typeProperties", {})
        sp_params = props.get("storedProcedureParameters", {})
        if sp_params:
            for _name, param in sp_params.items():
                assert "User::" not in param["value"], (
                    f"Namespace not stripped: {param['value']}"
                )

    def test_execute_package_strips_namespace(self):
        from ssis_adf_agent.parsers.models import ExecutePackageTask, TaskType
        from ssis_adf_agent.converters.control_flow.execute_package_converter import ExecutePackageConverter

        task = ExecutePackageTask(
            id="T1",
            name="RunChild",
            task_type=TaskType.EXECUTE_PACKAGE,
            use_project_reference=True,
            project_package_name="Child.dtsx",
            parameter_assignments=[
                {"parameter": "TargetDB", "variable": "User::DatabaseName"},
            ],
        )

        converter = ExecutePackageConverter()
        activities = converter.convert(task, [], {})
        assert len(activities) >= 1

        act = activities[0]
        params = act["typeProperties"].get("parameters", {})
        for _name, param in params.items():
            assert "User::" not in param["value"], (
                f"Namespace not stripped: {param['value']}"
            )
