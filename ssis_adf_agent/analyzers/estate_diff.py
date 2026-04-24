"""P5-16: diff_estate — compare two converted estates and surface what changed.

Compares two output directories produced by ``convert_estate`` (or any
matching layout: ``<root>/<package>/{pipeline,linkedService,dataset,
dataflow,trigger,stubs}``). Emits a focused report:

* Per-package classification: ``identical`` / ``changed`` / ``added`` /
  ``removed``.
* For changed packages, the per-artifact (per-file) classification with
  the unified diff for JSON / Python files.
* Aggregate counts so a downstream automation can decide whether to
  re-run ``validate_adf_artifacts`` and ``smoke_test_wave`` against the
  full estate or only the changed subset.

Pure stdlib — no Azure calls, no LLM calls, no network.
"""
from __future__ import annotations

import difflib
import hashlib
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


_TEXT_SUFFIXES = {".json", ".py", ".txt", ".md", ".yml", ".yaml", ".bicep"}
_MAX_DIFF_LINES = 200  # truncate diffs longer than this to keep reports readable


def _is_text(path: Path) -> bool:
    return path.suffix.lower() in _TEXT_SUFFIXES


def _file_digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _list_relative_files(root: Path) -> dict[str, Path]:
    """Map relative POSIX paths -> absolute Path under root."""
    out: dict[str, Path] = {}
    if not root.exists():
        return out
    for p in root.rglob("*"):
        if p.is_file():
            rel = p.relative_to(root).as_posix()
            out[rel] = p
    return out


def _list_packages(root: Path) -> list[str]:
    """Return immediate-child directory names under root."""
    if not root.exists():
        return []
    return sorted(p.name for p in root.iterdir() if p.is_dir())


@dataclass
class FileChange:
    path: str
    status: str  # "identical" | "changed" | "added" | "removed"
    diff: str | None = None  # unified diff (text files only)
    truncated: bool = False
    note: str | None = None  # e.g. "binary, byte-different"


@dataclass
class PackageDiff:
    package: str
    status: str  # "identical" | "changed" | "added" | "removed"
    file_changes: list[FileChange] = field(default_factory=list)


def _diff_text(a: Path, b: Path, rel: str) -> tuple[str, bool]:
    a_lines = a.read_text(encoding="utf-8", errors="replace").splitlines(keepends=True)
    b_lines = b.read_text(encoding="utf-8", errors="replace").splitlines(keepends=True)
    diff_iter = difflib.unified_diff(
        a_lines, b_lines,
        fromfile=f"a/{rel}", tofile=f"b/{rel}",
        n=3,
    )
    lines = list(diff_iter)
    truncated = False
    if len(lines) > _MAX_DIFF_LINES:
        lines = lines[:_MAX_DIFF_LINES] + [
            f"... [truncated: diff exceeded {_MAX_DIFF_LINES} lines] ...\n",
        ]
        truncated = True
    return "".join(lines), truncated


def _compare_package(name: str, before_dir: Path, after_dir: Path) -> PackageDiff:
    before_files = _list_relative_files(before_dir)
    after_files = _list_relative_files(after_dir)

    all_rels = sorted(set(before_files) | set(after_files))
    file_changes: list[FileChange] = []
    any_change = False

    for rel in all_rels:
        b = before_files.get(rel)
        a = after_files.get(rel)
        if b is None and a is not None:
            file_changes.append(FileChange(path=rel, status="added"))
            any_change = True
            continue
        if a is None and b is not None:
            file_changes.append(FileChange(path=rel, status="removed"))
            any_change = True
            continue
        # Both sides exist — compare digests first (cheap).
        if _file_digest(b) == _file_digest(a):  # type: ignore[arg-type]
            file_changes.append(FileChange(path=rel, status="identical"))
            continue
        any_change = True
        if _is_text(Path(rel)):
            diff, truncated = _diff_text(b, a, rel)  # type: ignore[arg-type]
            file_changes.append(FileChange(
                path=rel, status="changed", diff=diff, truncated=truncated,
            ))
        else:
            file_changes.append(FileChange(
                path=rel, status="changed", note="binary, byte-different",
            ))

    return PackageDiff(
        package=name,
        status="changed" if any_change else "identical",
        file_changes=file_changes,
    )


def diff_estates(before: Path, after: Path) -> dict[str, Any]:
    """Compare two estate output directories and return a structured report.

    Args:
        before: Root directory containing one subdirectory per package
            (e.g. an earlier convert_estate output).
        after: Root directory in the same shape (later convert_estate output).

    Returns:
        Dict with package-level and aggregate counts plus per-changed-package
        per-file diff. Suitable for ``json.dumps`` and for handing to a
        reviewer.
    """
    before = Path(before)
    after = Path(after)

    before_packages = set(_list_packages(before))
    after_packages = set(_list_packages(after))
    all_packages = sorted(before_packages | after_packages)

    pkg_diffs: list[PackageDiff] = []
    for name in all_packages:
        if name in before_packages and name not in after_packages:
            pkg_diffs.append(PackageDiff(package=name, status="removed"))
            continue
        if name in after_packages and name not in before_packages:
            pkg_diffs.append(PackageDiff(package=name, status="added"))
            continue
        pkg_diffs.append(_compare_package(name, before / name, after / name))

    counts = {
        "identical": sum(1 for p in pkg_diffs if p.status == "identical"),
        "changed":   sum(1 for p in pkg_diffs if p.status == "changed"),
        "added":     sum(1 for p in pkg_diffs if p.status == "added"),
        "removed":   sum(1 for p in pkg_diffs if p.status == "removed"),
    }

    return {
        "before_dir": str(before),
        "after_dir": str(after),
        "summary": {
            "total_packages": len(pkg_diffs),
            **counts,
        },
        "packages": [
            {
                "package": p.package,
                "status": p.status,
                "file_changes": [
                    {k: v for k, v in fc.__dict__.items() if v is not None or k == "status"}
                    for fc in p.file_changes
                ],
            }
            for p in pkg_diffs
        ],
    }
