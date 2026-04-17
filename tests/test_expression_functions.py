"""Tests for expression translator — new functions, DATEDIFF, SUBSTRING, RIGHT/LEFT,
bitwise operators, DATEPART, YEAR/MONTH/DAY."""
from __future__ import annotations

import pytest

from ssis_adf_agent.translators.control_flow_expression import (
    translate_control_flow_expr,
)


# ---------------------------------------------------------------------------
# SUBSTRING (1-based → 0-based)
# ---------------------------------------------------------------------------

class TestSubstring:
    def test_substring_offset(self):
        result = translate_control_flow_expr('SUBSTRING(@[User::Name], 1, 5)')
        assert "substring(variables('Name'), sub(1, 1), 5)" == result

    def test_substring_variable_start(self):
        result = translate_control_flow_expr('SUBSTRING(@[User::S], @[User::Pos], 3)')
        assert "sub(variables('Pos'), 1)" in result
        assert "substring(variables('S')" in result


# ---------------------------------------------------------------------------
# RIGHT / LEFT
# ---------------------------------------------------------------------------

class TestRightLeft:
    def test_right(self):
        result = translate_control_flow_expr('RIGHT(@[User::Code], 4)')
        assert "substring(variables('Code'), sub(length(variables('Code')), 4), 4)" == result

    def test_left(self):
        result = translate_control_flow_expr('LEFT(@[User::Code], 3)')
        assert "substring(variables('Code'), 0, 3)" == result


# ---------------------------------------------------------------------------
# FINDSTRING / CHARINDEX
# ---------------------------------------------------------------------------

class TestFindString:
    def test_findstring(self):
        result = translate_control_flow_expr('FINDSTRING(@[User::Text], "ab")')
        assert "add(indexOf(variables('Text'), 'ab'), 1)" == result

    def test_charindex(self):
        result = translate_control_flow_expr('CHARINDEX(@[User::Text], "x")')
        assert "add(indexOf(variables('Text'), 'x'), 1)" == result


# ---------------------------------------------------------------------------
# DATEDIFF
# ---------------------------------------------------------------------------

class TestDateDiff:
    def test_datediff_day(self):
        result = translate_control_flow_expr(
            'DATEDIFF("dd", @[User::Start], @[User::End])'
        )
        assert "ticks(variables('End'))" in result
        assert "ticks(variables('Start'))" in result
        assert "864000000000" in result

    def test_datediff_hour(self):
        result = translate_control_flow_expr(
            'DATEDIFF("hh", @[User::S], @[User::E])'
        )
        assert "36000000000" in result

    def test_datediff_minute(self):
        result = translate_control_flow_expr(
            'DATEDIFF("mi", @[User::S], @[User::E])'
        )
        assert "600000000" in result

    def test_datediff_second(self):
        result = translate_control_flow_expr(
            'DATEDIFF("ss", @[User::S], @[User::E])'
        )
        assert "10000000" in result

    def test_datediff_unknown_part(self):
        result = translate_control_flow_expr(
            'DATEDIFF("yyyy", @[User::S], @[User::E])'
        )
        assert "TODO" in result


# ---------------------------------------------------------------------------
# DATEPART / YEAR / MONTH / DAY
# ---------------------------------------------------------------------------

class TestDatePart:
    def test_datepart_day(self):
        result = translate_control_flow_expr('DATEPART("dd", @[User::D])')
        assert "dayOfMonth(variables('D'))" == result

    def test_datepart_weekday(self):
        result = translate_control_flow_expr('DATEPART("dw", @[User::D])')
        assert "dayOfWeek(variables('D'))" == result

    def test_year_function(self):
        result = translate_control_flow_expr('YEAR(@[User::D])')
        assert "int(formatDateTime(variables('D'), 'yyyy'))" == result

    def test_month_function(self):
        result = translate_control_flow_expr('MONTH(@[User::D])')
        assert "int(formatDateTime(variables('D'), 'MM'))" == result

    def test_day_function(self):
        result = translate_control_flow_expr('DAY(@[User::D])')
        assert "int(formatDateTime(variables('D'), 'dd'))" == result


# ---------------------------------------------------------------------------
# Math functions (were TODOs, now mapped)
# ---------------------------------------------------------------------------

class TestMathFunctions:
    def test_abs(self):
        result = translate_control_flow_expr('ABS(@[User::Val])')
        assert result == "abs(variables('Val'))"

    def test_ceiling(self):
        result = translate_control_flow_expr('CEILING(@[User::X])')
        assert result == "ceil(variables('X'))"

    def test_floor(self):
        result = translate_control_flow_expr('FLOOR(@[User::X])')
        assert result == "floor(variables('X'))"

    def test_round(self):
        result = translate_control_flow_expr('ROUND(@[User::X], 2)')
        assert result == "round(variables('X'), 2)"


# ---------------------------------------------------------------------------
# REPLACENULL / ISNULL
# ---------------------------------------------------------------------------

class TestNullFunctions:
    def test_replacenull(self):
        result = translate_control_flow_expr('REPLACENULL(@[User::X], "default")')
        assert result == "coalesce(variables('X'), 'default')"

    def test_isnull(self):
        result = translate_control_flow_expr('ISNULL(@[User::X])')
        assert result == "empty(variables('X'))"


# ---------------------------------------------------------------------------
# Bitwise operators
# ---------------------------------------------------------------------------

class TestBitwiseOperators:
    def test_bitwise_and(self):
        result = translate_control_flow_expr('@[User::Flags] & 4')
        assert "TODO: bitwise AND" in result
        assert "variables('Flags')" in result
        assert "4" in result

    def test_bitwise_or(self):
        result = translate_control_flow_expr('@[User::A] | @[User::B]')
        assert "TODO: bitwise OR" in result

    def test_bitwise_xor(self):
        result = translate_control_flow_expr('@[User::A] ^ @[User::B]')
        assert "TODO: bitwise XOR" in result

    def test_bitwise_not(self):
        result = translate_control_flow_expr('~@[User::Mask]')
        assert "TODO: bitwise NOT" in result

    def test_bitwise_does_not_conflict_with_logical(self):
        """& should not be confused with &&"""
        result = translate_control_flow_expr(
            '@[User::A] > 0 && @[User::B] > 0'
        )
        assert "and(" in result
        assert "TODO" not in result


# ---------------------------------------------------------------------------
# Existing functionality still works
# ---------------------------------------------------------------------------

class TestRegressions:
    def test_basic_comparison(self):
        result = translate_control_flow_expr('@[User::X] == 10')
        assert result == "equals(variables('X'), 10)"

    def test_and_or(self):
        result = translate_control_flow_expr('@[User::A] > 0 && @[User::B] < 5')
        assert "and(greater(variables('A'), 0), less(variables('B'), 5))" == result

    def test_concat(self):
        result = translate_control_flow_expr('"Hello " + @[User::Name]')
        assert "concat('Hello ', variables('Name'))" == result

    def test_dateadd_days(self):
        result = translate_control_flow_expr('DATEADD("dd", 7, @[User::D])')
        assert "addDays(variables('D'), 7)" == result

    def test_getdate(self):
        result = translate_control_flow_expr('GETDATE()')
        assert result == "utcNow()"

    def test_unary_not(self):
        result = translate_control_flow_expr('!@[User::Flag]')
        assert result == "not(variables('Flag'))"

    def test_unary_minus(self):
        result = translate_control_flow_expr('-@[User::X]')
        assert result == "sub(0, variables('X'))"

    def test_nested_function(self):
        result = translate_control_flow_expr('LEN(@[User::Name])')
        assert result == "length(variables('Name'))"
