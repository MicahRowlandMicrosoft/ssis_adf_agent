from __future__ import annotations

from ssis_adf_agent.parsers.models import (
    EventHandler,
    ForEachLoopContainer,
    PrecedenceConstraint,
    SequenceContainer,
    SSISPackage,
    SSISTask,
)
from ssis_adf_agent.parsers.task_traversal import iter_task_locations, iter_tasks


def _package() -> SSISPackage:
    nested_constraint = PrecedenceConstraint(
        id="nested-edge",
        from_task_id="nested",
        to_task_id="loop",
    )
    handler_constraint = PrecedenceConstraint(
        id="handler-edge",
        from_task_id="handler-sequence",
        to_task_id="handler-tail",
    )
    handler_sequence = SequenceContainer(
        id="handler-sequence",
        name="Handler sequence",
        tasks=[SSISTask(id="handler-child", name="Handler child")],
    )
    sequence = SequenceContainer(
        id="sequence",
        name="Sequence",
        disabled=True,
        constraints=[nested_constraint],
        tasks=[
            SSISTask(id="nested", name="Nested task"),
            ForEachLoopContainer(
                id="loop",
                name="Loop",
                tasks=[SSISTask(id="deep", name="Deep task")],
            ),
        ],
    )
    return SSISPackage(
        id="package",
        name="Package",
        source_file="Package.dtsx",
        tasks=[SSISTask(id="root", name="Root task"), sequence],
        event_handlers=[
            EventHandler(
                event_name="OnError",
                parent_task_id="nested",
                parent_task_name="Nested task",
                constraints=[handler_constraint],
                tasks=[handler_sequence, SSISTask(id="handler-tail", name="Handler tail")],
            )
        ],
    )


def test_traversal_yields_package_and_handler_tasks_in_preorder():
    locations = list(iter_task_locations(_package()))

    assert [location.task.id for location in locations] == [
        "root",
        "sequence",
        "nested",
        "loop",
        "deep",
        "handler-sequence",
        "handler-child",
        "handler-tail",
    ]


def test_traversal_records_owner_scope_depth_and_constraints():
    locations = {
        location.task.id: location for location in iter_task_locations(_package())
    }

    assert locations["root"].owner_kind == "package"
    assert locations["root"].owner_id == "package"
    assert locations["root"].scope_path == ("package:package",)
    assert locations["root"].depth == 0

    nested = locations["nested"]
    assert nested.owner_kind == "container"
    assert nested.owner_id == "sequence"
    assert nested.scope_path == ("package:package", "container:sequence")
    assert nested.depth == 1
    assert [constraint.id for constraint in nested.constraints] == ["nested-edge"]
    assert nested.inbound_constraints == ()
    assert [constraint.id for constraint in nested.outbound_constraints] == [
        "nested-edge"
    ]
    assert locations["sequence"].task.disabled is True

    loop = locations["loop"]
    assert [constraint.id for constraint in loop.inbound_constraints] == [
        "nested-edge"
    ]
    assert loop.outbound_constraints == ()

    handler_task = locations["handler-sequence"]
    assert handler_task.owner_kind == "event_handler"
    assert handler_task.owner_id == "handler:nested:OnError:0"
    assert handler_task.owner_name == "OnError"
    assert handler_task.scope_path == (
        "package:package",
        "container:sequence",
        "task:nested",
        "handler:nested:OnError:0",
    )
    assert handler_task.depth == 2
    assert handler_task.event_name == "OnError"
    assert handler_task.handler_parent_task_id == "nested"
    assert [constraint.id for constraint in handler_task.constraints] == ["handler-edge"]

    handler_child = locations["handler-child"]
    assert handler_child.owner_kind == "container"
    assert handler_child.owner_id == "handler-sequence"
    assert handler_child.scope_path == handler_task.scope_path + (
        "container:handler-sequence",
    )
    assert handler_child.depth == 3
    assert handler_child.event_name == "OnError"
    assert handler_child.handler_parent_task_id == "nested"


def test_traversal_can_exclude_event_handlers():
    package = _package()

    assert [task.id for task in iter_tasks(package, include_event_handlers=False)] == [
        "root",
        "sequence",
        "nested",
        "loop",
        "deep",
    ]


def test_package_handler_has_package_scope_and_stable_owner_id():
    package = SSISPackage(
        id="package",
        name="Package",
        source_file="Package.dtsx",
        event_handlers=[
            EventHandler(
                event_name="OnWarning",
                tasks=[SSISTask(id="warning-task", name="Warning task")],
            )
        ],
    )

    location = next(iter_task_locations(package))
    assert location.owner_id == "handler:package:OnWarning:0"
    assert location.scope_path == (
        "package:package",
        "handler:package:OnWarning:0",
    )
    assert location.depth == 0