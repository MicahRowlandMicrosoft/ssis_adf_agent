"""
Conversion warnings collector — thread-safe, context-manager-based collector
that accumulates ``ConversionWarning`` instances during a conversion run.

Modules emit warnings via the module-level ``warn()`` function.  The MCP tool
handler creates a collector context that gathers all warnings from that run.

Usage in a tool handler::

    with WarningsCollector() as wc:
        # ... call parsers, converters, generators ...
        all_warnings = wc.warnings

Usage in a converter/generator::

    from ssis_adf_agent.warnings_collector import warn

    warn(
        phase="convert",
        severity="warning",
        source="source_converter",
        message="Missing connection ID for component 'OLE_SRC'",
        detail="Falling back to 'LS_unknown'",
        task_name="Load Customers",
    )
"""
from __future__ import annotations

import logging
import threading
from typing import Any

from .parsers.models import ConversionWarning

logger = logging.getLogger("ssis_adf_agent")

# Thread-local storage for the active collector
_local = threading.local()


class WarningsCollector:
    """Context manager that collects conversion warnings from all modules."""

    def __init__(self) -> None:
        self.warnings: list[ConversionWarning] = []

    def __enter__(self) -> WarningsCollector:
        _local.collector = self
        return self

    def __exit__(self, *exc: Any) -> None:
        _local.collector = None

    def add(self, warning: ConversionWarning) -> None:
        self.warnings.append(warning)


def _get_collector() -> WarningsCollector | None:
    return getattr(_local, "collector", None)


def warn(
    *,
    phase: str,
    severity: str,
    source: str,
    message: str,
    task_name: str = "",
    task_id: str = "",
    detail: str = "",
) -> None:
    """Emit a conversion warning.

    If a ``WarningsCollector`` context is active, the warning is collected.
    The warning is always logged via the ``ssis_adf_agent`` logger regardless.
    """
    w = ConversionWarning(
        phase=phase,
        severity=severity,
        source=source,
        message=message,
        task_name=task_name,
        task_id=task_id,
        detail=detail,
    )

    # Always log
    log_msg = f"[{phase}/{severity}] {source}: {message}"
    if task_name:
        log_msg += f" (task: {task_name})"
    if detail:
        log_msg += f" — {detail}"

    if severity == "error":
        logger.error(log_msg)
    elif severity == "warning":
        logger.warning(log_msg)
    else:
        logger.info(log_msg)

    # Collect if inside a WarningsCollector context
    collector = _get_collector()
    if collector is not None:
        collector.add(w)
