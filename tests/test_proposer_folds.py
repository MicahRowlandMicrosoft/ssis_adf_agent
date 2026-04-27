"""Tests for the new fold/drop/replace detectors added to the proposer."""
from __future__ import annotations

from ssis_adf_agent.migration_plan import (
    SimplificationAction,
    propose_design,
)
from ssis_adf_agent.parsers.models import (  # type: ignore[attr-defined]
    ConnectionManagerType,
    DataFlowComponent,
    DataFlowTask,
    ExecuteSQLTask,
    SSISConnectionManager,
    SSISPackage,
    SSISTask,
    TaskType,
)


def _sql_cm(id_="cm-sql", name="SQL"):
    return SSISConnectionManager(
        id=id_, name=name, type=ConnectionManagerType.OLEDB, server="s", database="d",
    )


def _file_cm():
    return SSISConnectionManager(
        id="cm-file", name="Out", type=ConnectionManagerType.FLAT_FILE, file_path="/o.csv",
    )


def _exec_sql(name, stmt, conn="cm-sql", result="None"):
    return ExecuteSQLTask(
        id=f"sql-{name}", name=name, task_type=TaskType.EXECUTE_SQL,
        connection_id=conn, sql_statement=stmt, result_set_type=result,
    )


def _df(name, comps):
    return DataFlowTask(
        id=f"df-{name}", name=name, task_type=TaskType.DATA_FLOW, components=comps,
    )


def _comp(comp_type, conn=None, name=None):
    return DataFlowComponent(
        id=f"c-{name or comp_type}", name=name or comp_type,
        component_class_id="x", component_type=comp_type, connection_id=conn,
    )


def _pkg(tasks: list[SSISTask], cms=None) -> SSISPackage:
    return SSISPackage(
        id="pkg", name="P", source_file="p.dtsx",
        connection_managers=cms or [_sql_cm(), _file_cm()],
        tasks=tasks,
    )


# ---------------------------------------------------------------------------
# fold_to_stored_proc — TRUNCATE + MERGE/INSERT on same connection
# ---------------------------------------------------------------------------

def test_proposer_folds_truncate_then_merge_to_stored_proc():
    df = _df("DF", [
        _comp("OLEDBSource", conn="cm-sql"),
        _comp("OLEDBDestination", conn="cm-sql", name="Sink"),
    ])
    pkg = _pkg(
        [
            _exec_sql("Trunc", "TRUNCATE TABLE staging.Sales"),
            df,
            _exec_sql("Merge", "MERGE dbo.Sales USING staging.Sales ON ..."),
        ],
        cms=[_sql_cm()],
    )
    plan = propose_design(pkg)
    folds = [s for s in plan.simplifications if s.action == SimplificationAction.FOLD_TO_STORED_PROC]
    assert len(folds) == 1
    assert set(folds[0].items) == {"Trunc", "Merge"}


def test_proposer_does_not_fold_when_only_one_sql_task():
    pkg = _pkg(
        [_exec_sql("Trunc", "TRUNCATE TABLE staging.Sales")],
        cms=[_sql_cm()],
    )
    plan = propose_design(pkg)
    assert not any(
        s.action == SimplificationAction.FOLD_TO_STORED_PROC for s in plan.simplifications
    )


def test_proposer_does_not_fold_lookup_style_query():
    """Single-row / FullResultSet queries drive control flow — never silently fold."""
    pkg = _pkg(
        [
            _exec_sql("LookupConfig", "SELECT TOP 1 * FROM cfg", result="SingleRow"),
            _exec_sql("Merge", "MERGE dbo.Sales ..."),
        ],
        cms=[_sql_cm()],
    )
    plan = propose_design(pkg)
    assert not any(
        s.action == SimplificationAction.FOLD_TO_STORED_PROC for s in plan.simplifications
    )


# ---------------------------------------------------------------------------
# fold_to_copy_activity — Lookup-only data flow
# ---------------------------------------------------------------------------

def test_proposer_folds_lookup_only_dataflow_to_copy_activity():
    df = _df("DF", [
        _comp("OLEDBSource", conn="cm-sql"),
        _comp("Lookup"),
        _comp("OLEDBDestination", conn="cm-sql2"),
    ])
    pkg = _pkg([df], cms=[_sql_cm(), _sql_cm("cm-sql2", "SQL2")])
    plan = propose_design(pkg)
    folds = [
        s for s in plan.simplifications
        if s.action == SimplificationAction.FOLD_TO_COPY_ACTIVITY
        and "Lookup" in s.reason
    ]
    assert len(folds) == 1
    assert folds[0].items == ["DF"]


def test_proposer_does_not_fold_dataflow_with_heavy_transforms():
    df = _df("DF", [
        _comp("OLEDBSource", conn="cm-sql"),
        _comp("Aggregate"),
        _comp("OLEDBDestination", conn="cm-sql2"),
    ])
    pkg = _pkg([df], cms=[_sql_cm(), _sql_cm("cm-sql2", "SQL2")])
    plan = propose_design(pkg)
    folds = [s for s in plan.simplifications if s.action == SimplificationAction.FOLD_TO_COPY_ACTIVITY]
    assert folds == []


# ---------------------------------------------------------------------------
# drop — audit-only ExecuteSQL
# ---------------------------------------------------------------------------

def test_proposer_drops_audit_log_only_tasks():
    pkg = _pkg(
        [
            _exec_sql("LogStart", "INSERT INTO etl_log VALUES (...)"),
            _exec_sql("Real", "MERGE dbo.Sales ..."),
            _exec_sql("LogEnd", "INSERT INTO audit.run_history VALUES (...)"),
        ],
        cms=[_sql_cm()],
    )
    plan = propose_design(pkg)
    drops = [s for s in plan.simplifications if s.action == SimplificationAction.DROP]
    # All drops merged into one Simplification
    assert len(drops) == 1
    assert set(drops[0].items) == {"LogStart", "LogEnd"}
    assert "Real" not in drops[0].items


def test_proposer_does_not_drop_audit_when_lookup_query_present_on_same_connection():
    """If the connection is also used to read state (FullResultSet), be conservative."""
    pkg = _pkg(
        [
            _exec_sql("LogStart", "INSERT INTO etl_log VALUES (...)"),
            _exec_sql("LoadConfig", "SELECT * FROM cfg", result="FullResultSet"),
        ],
        cms=[_sql_cm()],
    )
    plan = propose_design(pkg)
    drops = [s for s in plan.simplifications if s.action == SimplificationAction.DROP]
    # No audit drops emitted (atomic-write rule may add unrelated drops, but not for these tasks)
    audit_dropped = {n for s in drops for n in s.items if n.startswith("Log")}
    assert audit_dropped == set()


# ---------------------------------------------------------------------------
# replace_with_function — Send Mail
# ---------------------------------------------------------------------------

class _SendMailTask(SSISTask):
    task_type: TaskType = TaskType.SEND_MAIL


def test_proposer_recommends_replacing_send_mail_tasks():
    mail = _SendMailTask(id="mail-1", name="NotifyOps")
    pkg = _pkg([mail], cms=[_sql_cm()])
    plan = propose_design(pkg)
    repl = [
        s for s in plan.simplifications
        if s.action == SimplificationAction.REPLACE_WITH_FUNCTION
    ]
    assert len(repl) == 1
    assert repl[0].items == ["NotifyOps"]
    assert "Logic App" in repl[0].reason


def test_proposer_emits_no_send_mail_simplification_when_none_present():
    pkg = _pkg([_exec_sql("Q", "SELECT 1")], cms=[_sql_cm()])
    plan = propose_design(pkg)
    assert not any(
        s.action == SimplificationAction.REPLACE_WITH_FUNCTION for s in plan.simplifications
    )
