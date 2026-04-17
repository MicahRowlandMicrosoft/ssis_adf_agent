"""Shared naming helper for Mapping Data Flow node names.

ADF Mapping Data Flow rejects source/sink/transformation names that contain
anything other than alphanumeric characters. Spaces, underscores, hyphens and
other punctuation in the original SSIS component name must be stripped. The
node name must also start with a letter.
"""
from __future__ import annotations

import re

_NON_ALNUM_RE = re.compile(r"[^A-Za-z0-9]+")


def safe_node_name(raw: str, fallback: str = "Node") -> str:
    """Return an ADF-compatible Mapping Data Flow node name.

    Strips every non-alphanumeric character from *raw*. If the result is empty
    or starts with a digit, prefixes ``N``.
    """
    if not raw:
        return fallback
    cleaned = _NON_ALNUM_RE.sub("", raw)
    if not cleaned:
        return fallback
    if not cleaned[0].isalpha():
        cleaned = "N" + cleaned
    return cleaned
