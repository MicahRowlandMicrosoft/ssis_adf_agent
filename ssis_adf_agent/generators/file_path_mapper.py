"""
File path mapper — rewrites local/UNC paths to Azure Storage URLs.

Accepts a mapping config of ``{prefix: azure_url}`` and applies longest-prefix-match
substitution across linked services, pipeline activities, and dataset references.

Example config::

    {
        "C:\\\\Data\\\\Input": "https://mystorage.blob.core.windows.net/input",
        "\\\\\\\\fileserver\\\\share": "abfss://container@account.dfs.core.windows.net/share"
    }
"""
from __future__ import annotations

from typing import Any


def apply_file_path_map(
    artifacts: dict[str, list[dict[str, Any]]],
    file_path_map: dict[str, str],
) -> int:
    """Apply file path substitutions across all artifact types.

    *artifacts* is a dict with keys ``"linked_services"``, ``"pipeline"``,
    ``"datasets"`` mapping to their JSON dicts/lists.

    Returns the number of substitutions made.
    """
    if not file_path_map:
        return 0

    # Sort by longest prefix first so more specific rules win
    sorted_prefixes = sorted(file_path_map.keys(), key=len, reverse=True)
    count = 0

    # Linked services
    for ls in artifacts.get("linked_services", []):
        count += _rewrite_dict(ls, sorted_prefixes, file_path_map)

    # Pipeline activities
    pipeline = artifacts.get("pipeline")
    if pipeline:
        for act in pipeline.get("properties", {}).get("activities", []):
            count += _rewrite_dict(act, sorted_prefixes, file_path_map)

    # Datasets
    for ds in artifacts.get("datasets", []):
        count += _rewrite_dict(ds, sorted_prefixes, file_path_map)

    return count


def _rewrite_dict(
    obj: Any,
    sorted_prefixes: list[str],
    file_path_map: dict[str, str],
) -> int:
    """Recursively walk a JSON-like structure and replace matching path prefixes."""
    count = 0
    if isinstance(obj, dict):
        for key in list(obj.keys()):
            val = obj[key]
            if isinstance(val, str):
                new_val, changed = _replace_path(val, sorted_prefixes, file_path_map)
                if changed:
                    obj[key] = new_val
                    count += 1
            else:
                count += _rewrite_dict(val, sorted_prefixes, file_path_map)
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            if isinstance(item, str):
                new_val, changed = _replace_path(item, sorted_prefixes, file_path_map)
                if changed:
                    obj[i] = new_val
                    count += 1
            else:
                count += _rewrite_dict(item, sorted_prefixes, file_path_map)
    return count


def _replace_path(
    value: str,
    sorted_prefixes: list[str],
    file_path_map: dict[str, str],
) -> tuple[str, bool]:
    """Replace the first matching prefix in *value*. Case-insensitive match."""
    value_lower = value.lower()
    for prefix in sorted_prefixes:
        prefix_lower = prefix.lower()
        if value_lower.startswith(prefix_lower):
            return file_path_map[prefix] + value[len(prefix):], True
        # Also try with normalised separators (\ → /)
        norm_value = value_lower.replace("\\", "/")
        norm_prefix = prefix_lower.replace("\\", "/")
        if norm_value.startswith(norm_prefix):
            return file_path_map[prefix] + value[len(prefix):], True
    return value, False
