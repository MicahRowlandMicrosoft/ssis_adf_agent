"""Path-safety utilities for the MCP server.

Provides :func:`safe_resolve` which validates and resolves user-supplied
filesystem paths, guarding against traversal attacks and enforcing an optional
``SSIS_ADF_ALLOWED_ROOT`` directory boundary.
"""
from __future__ import annotations

import os
from pathlib import Path

# Optional: set SSIS_ADF_ALLOWED_ROOT env var to restrict all path inputs to
# a directory tree.  When unset, only basic traversal checks are applied.
ALLOWED_ROOT: Path | None = (
    Path(os.environ["SSIS_ADF_ALLOWED_ROOT"]).resolve()
    if os.environ.get("SSIS_ADF_ALLOWED_ROOT")
    else None
)


def safe_resolve(raw: str, *, must_exist: bool = False, label: str = "path") -> Path:
    """Resolve *raw* to an absolute path and guard against traversal attacks.

    Rules enforced:
    1. Null bytes are rejected outright.
    2. The resolved path must not contain ``..`` components after resolution
       (defence-in-depth — ``Path.resolve()`` already normalises them, but we
       also reject raw inputs that *look* like traversal attempts).
    3. When ``SSIS_ADF_ALLOWED_ROOT`` is set the resolved path must be equal to
       or a child of that root.
    4. When *must_exist* is ``True`` the path must already exist on disk.
    """
    if "\x00" in raw:
        raise ValueError(f"Null byte in {label}: rejected")

    resolved = Path(raw).resolve()

    # Defence-in-depth: reject obvious traversal patterns in the raw input
    if ".." in Path(raw).parts:
        raise ValueError(
            f"Path traversal detected in {label}: {raw!r} — "
            "relative '..' components are not allowed"
        )

    if ALLOWED_ROOT is not None:
        try:
            resolved.relative_to(ALLOWED_ROOT)
        except ValueError:
            raise ValueError(
                f"{label} {str(resolved)!r} is outside the allowed root "
                f"{str(ALLOWED_ROOT)!r}"
            ) from None

    if must_exist and not resolved.exists():
        raise FileNotFoundError(f"{label} not found: {resolved}")

    return resolved
