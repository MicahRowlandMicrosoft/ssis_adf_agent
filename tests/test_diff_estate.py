"""P5-16: diff_estate unit tests against synthetic before/after fixtures."""
from __future__ import annotations

import asyncio
import json
from pathlib import Path

from ssis_adf_agent.analyzers.estate_diff import diff_estates
from ssis_adf_agent.mcp_server import _diff_estate


def _seed(root: Path, packages: dict[str, dict[str, str]]) -> None:
    """Materialize ``{pkg_name: {relative_path: content}}`` under root."""
    for pkg, files in packages.items():
        for rel, content in files.items():
            target = root / pkg / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content, encoding="utf-8")


def test_identical_estates_reported_as_identical(tmp_path: Path) -> None:
    before = tmp_path / "before"
    after = tmp_path / "after"
    layout = {
        "PkgA": {"pipeline/PkgA.json": '{"name": "PkgA"}'},
        "PkgB": {"pipeline/PkgB.json": '{"name": "PkgB"}'},
    }
    _seed(before, layout)
    _seed(after, layout)

    report = diff_estates(before, after)
    assert report["summary"] == {
        "total_packages": 2, "identical": 2, "changed": 0, "added": 0, "removed": 0,
    }
    for pkg in report["packages"]:
        assert pkg["status"] == "identical"


def test_changed_package_emits_unified_diff(tmp_path: Path) -> None:
    before = tmp_path / "before"
    after = tmp_path / "after"
    _seed(before, {"PkgA": {"pipeline/PkgA.json": '{"timeout": 30}\n'}})
    _seed(after, {"PkgA": {"pipeline/PkgA.json": '{"timeout": 60}\n'}})

    report = diff_estates(before, after)
    assert report["summary"]["changed"] == 1
    pkg = report["packages"][0]
    assert pkg["status"] == "changed"
    fc = pkg["file_changes"][0]
    assert fc["path"] == "pipeline/PkgA.json"
    assert fc["status"] == "changed"
    assert "-{\"timeout\": 30}" in fc["diff"]
    assert "+{\"timeout\": 60}" in fc["diff"]


def test_added_and_removed_packages_classified(tmp_path: Path) -> None:
    before = tmp_path / "before"
    after = tmp_path / "after"
    _seed(before, {"OldOnly": {"pipeline/OldOnly.json": "{}"}})
    _seed(after, {"NewOnly": {"pipeline/NewOnly.json": "{}"}})

    report = diff_estates(before, after)
    assert report["summary"] == {
        "total_packages": 2, "identical": 0, "changed": 0, "added": 1, "removed": 1,
    }
    by_pkg = {p["package"]: p["status"] for p in report["packages"]}
    assert by_pkg == {"OldOnly": "removed", "NewOnly": "added"}


def test_per_file_added_and_removed_within_changed_package(tmp_path: Path) -> None:
    before = tmp_path / "before"
    after = tmp_path / "after"
    _seed(before, {"PkgA": {
        "pipeline/PkgA.json": "{}",
        "linkedService/LS_Sql.json": "{}",
    }})
    _seed(after, {"PkgA": {
        "pipeline/PkgA.json": "{}",
        "linkedService/LS_Adls.json": "{}",
    }})

    report = diff_estates(before, after)
    pkg = report["packages"][0]
    assert pkg["status"] == "changed"
    by_path = {fc["path"]: fc["status"] for fc in pkg["file_changes"]}
    assert by_path["pipeline/PkgA.json"] == "identical"
    assert by_path["linkedService/LS_Sql.json"] == "removed"
    assert by_path["linkedService/LS_Adls.json"] == "added"


def test_diff_truncated_when_very_large(tmp_path: Path) -> None:
    before = tmp_path / "before"
    after = tmp_path / "after"
    big_before = "\n".join(f"line {i} before" for i in range(500)) + "\n"
    big_after = "\n".join(f"line {i} after" for i in range(500)) + "\n"
    _seed(before, {"PkgA": {"pipeline/PkgA.json": big_before}})
    _seed(after, {"PkgA": {"pipeline/PkgA.json": big_after}})

    report = diff_estates(before, after)
    fc = report["packages"][0]["file_changes"][0]
    assert fc["truncated"] is True
    assert "[truncated:" in fc["diff"]


def test_mcp_handler_writes_report_when_path_supplied(tmp_path: Path) -> None:
    before = tmp_path / "before"
    after = tmp_path / "after"
    _seed(before, {"PkgA": {"pipeline/PkgA.json": "{}"}})
    _seed(after, {"PkgA": {"pipeline/PkgA.json": '{"x": 1}'}})

    report_path = tmp_path / "diff.json"
    result = asyncio.run(_diff_estate({
        "before_dir": str(before),
        "after_dir": str(after),
        "report_path": str(report_path),
    }))
    payload = json.loads(result[0].text)

    assert payload["summary"]["changed"] == 1
    assert report_path.exists()
    saved = json.loads(report_path.read_text(encoding="utf-8"))
    assert saved["summary"]["changed"] == 1
