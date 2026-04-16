"""Tests for trigger generator — SQL Agent schedule mapping."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from ssis_adf_agent.generators.trigger_generator import (
    _schedule_from_agent,
    _start_time_iso,
    _end_time_iso,
    _hhmmss_to_parts,
    generate_triggers,
)
from ssis_adf_agent.parsers.models import SqlAgentSchedule, SSISPackage


def _make_schedule(**overrides) -> SqlAgentSchedule:
    defaults = dict(
        job_name="ETL_Job",
        schedule_name="Daily6AM",
        frequency_type=4,
        freq_interval=1,
        freq_subday_type=1,
        freq_subday_interval=0,
        active_start_time=60000,  # 06:00:00
        active_end_time=235959,
        freq_recurrence_factor=0,
    )
    defaults.update(overrides)
    return SqlAgentSchedule(**defaults)


def _make_package(schedule: SqlAgentSchedule | None = None) -> SSISPackage:
    return SSISPackage(
        id="pkg-1", name="TestPkg", source_file="test.dtsx",
        sql_agent_schedule=schedule,
    )


# ---------------------------------------------------------------------------
# _hhmmss_to_parts
# ---------------------------------------------------------------------------

class TestHhmmssParser:
    def test_midnight(self):
        assert _hhmmss_to_parts(0) == (0, 0, 0)

    def test_6am(self):
        assert _hhmmss_to_parts(60000) == (6, 0, 0)

    def test_23_59_59(self):
        assert _hhmmss_to_parts(235959) == (23, 59, 59)

    def test_14_30_15(self):
        assert _hhmmss_to_parts(143015) == (14, 30, 15)


# ---------------------------------------------------------------------------
# _start_time_iso / _end_time_iso
# ---------------------------------------------------------------------------

class TestTimeIso:
    def test_start_time_6am(self):
        sched = _make_schedule(active_start_time=60000)
        assert _start_time_iso(sched) == "2026-01-01T06:00:00Z"

    def test_start_time_midnight(self):
        sched = _make_schedule(active_start_time=0)
        assert _start_time_iso(sched) == "2026-01-01T00:00:00Z"

    def test_end_time_default_none(self):
        sched = _make_schedule(active_end_time=235959)
        assert _end_time_iso(sched) is None

    def test_end_time_custom(self):
        sched = _make_schedule(active_end_time=180000)
        assert _end_time_iso(sched) == "2026-12-31T18:00:00Z"


# ---------------------------------------------------------------------------
# _schedule_from_agent
# ---------------------------------------------------------------------------

class TestScheduleFromAgent:
    def test_daily(self):
        sched = _make_schedule(frequency_type=4, freq_interval=2, active_start_time=90000)
        rec = _schedule_from_agent(sched)
        assert rec["frequency"] == "Day"
        assert rec["interval"] == 2
        assert rec["schedule"]["hours"] == [9]
        assert rec["schedule"]["minutes"] == [0]
        assert rec["startTime"] == "2026-01-01T09:00:00Z"
        assert rec["timeZone"] == "UTC"

    def test_weekly_mon_wed_fri(self):
        # Monday(2) + Wednesday(8) + Friday(32) = 42
        sched = _make_schedule(
            frequency_type=8, freq_interval=42,
            freq_recurrence_factor=1, active_start_time=70000,
        )
        rec = _schedule_from_agent(sched)
        assert rec["frequency"] == "Week"
        assert set(rec["schedule"]["weekDays"]) == {"Monday", "Wednesday", "Friday"}
        assert rec["schedule"]["hours"] == [7]

    def test_monthly_15th(self):
        sched = _make_schedule(
            frequency_type=16, freq_interval=15,
            freq_recurrence_factor=1, active_start_time=120000,
        )
        rec = _schedule_from_agent(sched)
        assert rec["frequency"] == "Month"
        assert rec["schedule"]["monthDays"] == [15]
        assert rec["schedule"]["hours"] == [12]

    def test_monthly_relative(self):
        sched = _make_schedule(frequency_type=32, freq_recurrence_factor=1)
        rec = _schedule_from_agent(sched)
        assert rec["frequency"] == "Month"

    def test_once(self):
        sched = _make_schedule(frequency_type=1, active_start_time=153000)
        rec = _schedule_from_agent(sched)
        assert rec["frequency"] == "Day"
        assert rec["startTime"] == "2026-01-01T15:30:00Z"

    def test_subday_every_15_minutes(self):
        sched = _make_schedule(
            frequency_type=4, freq_interval=1,
            freq_subday_type=4, freq_subday_interval=15,
            active_start_time=80000,
        )
        rec = _schedule_from_agent(sched)
        assert rec["frequency"] == "Minute"
        assert rec["interval"] == 15

    def test_subday_every_2_hours(self):
        sched = _make_schedule(
            frequency_type=4, freq_interval=1,
            freq_subday_type=8, freq_subday_interval=2,
            active_start_time=60000,
        )
        rec = _schedule_from_agent(sched)
        assert rec["frequency"] == "Hour"
        assert rec["interval"] == 2

    def test_end_time_included(self):
        sched = _make_schedule(active_end_time=180000)
        rec = _schedule_from_agent(sched)
        assert "endTime" in rec
        assert rec["endTime"] == "2026-12-31T18:00:00Z"

    def test_end_time_not_included_for_default(self):
        sched = _make_schedule(active_end_time=235959)
        rec = _schedule_from_agent(sched)
        assert "endTime" not in rec


# ---------------------------------------------------------------------------
# generate_triggers end-to-end
# ---------------------------------------------------------------------------

class TestGenerateTriggers:
    def test_with_schedule(self, tmp_path):
        sched = _make_schedule(active_start_time=60000)
        pkg = _make_package(sched)
        triggers = generate_triggers(pkg, tmp_path)

        assert len(triggers) == 1
        rec = triggers[0]["properties"]["typeProperties"]["recurrence"]
        assert rec["startTime"] == "2026-01-01T06:00:00Z"
        assert "ETL_Job" in triggers[0]["properties"]["description"]

    def test_without_schedule_fallback(self, tmp_path):
        pkg = _make_package(None)
        triggers = generate_triggers(pkg, tmp_path)

        rec = triggers[0]["properties"]["typeProperties"]["recurrence"]
        assert rec["frequency"] == "Day"
        assert rec["schedule"]["hours"] == [0]
        assert rec["startTime"] == "2026-01-01T00:00:00Z"

    def test_cron_expression(self, tmp_path):
        pkg = _make_package(None)
        triggers = generate_triggers(pkg, tmp_path, cron_expression="0 0 6 ? * MON-FRI")

        rec = triggers[0]["properties"]["typeProperties"]["recurrence"]
        assert rec["schedule"]["quartz"] == "0 0 6 ? * MON-FRI"

    def test_stopped_state(self, tmp_path):
        pkg = _make_package(_make_schedule())
        triggers = generate_triggers(pkg, tmp_path)
        assert triggers[0]["properties"]["runtimeState"] == "Stopped"

    def test_once_description(self, tmp_path):
        sched = _make_schedule(frequency_type=1)
        pkg = _make_package(sched)
        triggers = generate_triggers(pkg, tmp_path)
        assert "Once" in triggers[0]["properties"]["description"]

    def test_subday_description(self, tmp_path):
        sched = _make_schedule(freq_subday_type=4, freq_subday_interval=15)
        pkg = _make_package(sched)
        triggers = generate_triggers(pkg, tmp_path)
        assert "15 minutes" in triggers[0]["properties"]["description"]

    def test_json_written(self, tmp_path):
        pkg = _make_package(_make_schedule())
        generate_triggers(pkg, tmp_path)
        files = list((tmp_path / "trigger").glob("*.json"))
        assert len(files) == 1
        payload = json.loads(files[0].read_text())
        assert "recurrence" in payload["properties"]["typeProperties"]
