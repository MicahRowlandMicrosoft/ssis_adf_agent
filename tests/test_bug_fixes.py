"""Regression tests for the five bugs surfaced by the SSISDataFlowTraining run."""
from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# Bug #2: package name fallback — SSIS designer default 'Package' / 'Package1'
# should not survive into estate-scale outputs; use the .dtsx filename instead.
# ---------------------------------------------------------------------------

_DEFAULT_NAMED_DTSX = """<?xml version="1.0"?>
<DTS:Executable xmlns:DTS="www.microsoft.com/SqlServer/Dts"
    DTS:ExecutableType="Microsoft.Package"
    DTS:DTSID="{11111111-1111-1111-1111-111111111111}"
    DTS:ObjectName="Package1">
  <DTS:Executables />
</DTS:Executable>
"""


def test_default_package_name_falls_back_to_filename(tmp_path: Path) -> None:
    from ssis_adf_agent.parsers.readers.local_reader import LocalReader

    p = tmp_path / "MyMeaningfulName.dtsx"
    p.write_text(_DEFAULT_NAMED_DTSX, encoding="utf-8")
    pkg = LocalReader().read(p)
    assert pkg.name == "MyMeaningfulName"


def test_non_default_package_name_is_preserved(tmp_path: Path) -> None:
    from ssis_adf_agent.parsers.readers.local_reader import LocalReader

    custom = _DEFAULT_NAMED_DTSX.replace('"Package1"', '"LoadFactSales"')
    p = tmp_path / "any_filename.dtsx"
    p.write_text(custom, encoding="utf-8")
    pkg = LocalReader().read(p)
    assert pkg.name == "LoadFactSales"


# ---------------------------------------------------------------------------
# Bug #1: bulk_analyze.shared_on_prem_sql_servers should only include
# SQL-flavored connection managers, not Excel / Flat File / FTP paths.
# ---------------------------------------------------------------------------

_DTSX_WITH_MIXED_CMS = """<?xml version="1.0"?>
<DTS:Executable xmlns:DTS="www.microsoft.com/SqlServer/Dts"
    DTS:ExecutableType="Microsoft.Package"
    DTS:DTSID="{22222222-2222-2222-2222-22222222{n:04d}}"
    DTS:ObjectName="Pkg{n}">
  <DTS:ConnectionManagers>
    <DTS:ConnectionManager DTS:ObjectName="SqlSrc"
        DTS:CreationName="OLEDB"
        DTS:DTSID="{33333333-3333-3333-3333-33333333{n:04d}}">
      <DTS:ObjectData>
        <DTS:ConnectionManager DTS:ConnectionString="Data Source=prod-sql-01;Initial Catalog=DW;" />
      </DTS:ObjectData>
    </DTS:ConnectionManager>
    <DTS:ConnectionManager DTS:ObjectName="ExcelOut"
        DTS:CreationName="EXCEL"
        DTS:DTSID="{44444444-4444-4444-4444-44444444{n:04d}}">
      <DTS:ObjectData>
        <DTS:ConnectionManager DTS:ConnectionString="Data Source=C:\\\\Outputs\\\\Demo.xls;" />
      </DTS:ObjectData>
    </DTS:ConnectionManager>
  </DTS:ConnectionManagers>
  <DTS:Executables />
</DTS:Executable>
"""


def test_shared_sql_servers_excludes_excel_paths(tmp_path: Path) -> None:
    from ssis_adf_agent.mcp_server import _bulk_analyze

    for i in (1, 2):
        (tmp_path / f"pkg{i}.dtsx").write_text(
            _DTSX_WITH_MIXED_CMS.replace("{n:04d}", f"{i:04d}").replace("{n}", str(i)),
            encoding="utf-8",
        )

    result = asyncio.run(_bulk_analyze({"source_path": str(tmp_path)}))
    payload = json.loads(result[0].text)
    proj = payload["projects"][0]
    assert "prod-sql-01" in proj["shared_on_prem_sql_servers"]
    # Excel file paths must NOT show up here.
    assert not any(
        ".xls" in srv.lower() or "outputs" in srv.lower()
        for srv in proj["shared_on_prem_sql_servers"]
    )


# ---------------------------------------------------------------------------
# Bug #3: validate_adf_artifacts must skip migration_plan.json sidecars and
# anything outside the recognized ADF subfolders.
# ---------------------------------------------------------------------------

def test_validate_skips_migration_plan_sidecar(tmp_path: Path) -> None:
    from ssis_adf_agent.deployer.adf_deployer import AdfDeployer

    # Sidecar at the artifacts root — must be ignored.
    (tmp_path / "migration_plan.json").write_text(
        json.dumps({"schema_version": "1.0", "package_name": "X"}),
        encoding="utf-8",
    )
    # Random scratch JSON at the root — must also be ignored.
    (tmp_path / "scratch.json").write_text("{}", encoding="utf-8")
    # A real ADF pipeline file — must be checked and pass.
    (tmp_path / "pipeline").mkdir()
    (tmp_path / "pipeline" / "PL_Demo.json").write_text(
        json.dumps({"name": "PL_Demo", "properties": {"activities": []}}),
        encoding="utf-8",
    )

    deployer = AdfDeployer.__new__(AdfDeployer)
    issues = deployer.validate_artifacts(tmp_path)
    assert issues == []


def test_validate_still_catches_missing_pipeline_activities(tmp_path: Path) -> None:
    from ssis_adf_agent.deployer.adf_deployer import AdfDeployer

    (tmp_path / "pipeline").mkdir()
    (tmp_path / "pipeline" / "PL_Bad.json").write_text(
        json.dumps({"name": "PL_Bad", "properties": {}}),
        encoding="utf-8",
    )
    deployer = AdfDeployer.__new__(AdfDeployer)
    issues = deployer.validate_artifacts(tmp_path)
    assert any("activities" in issue["error"] for issue in issues)


# ---------------------------------------------------------------------------
# Bug #4: provision_adf_environment(dry_run=True) without subscription_id /
# resource_group should compile Bicep locally and return successfully.
# ---------------------------------------------------------------------------

@pytest.fixture
def saved_plan_path(tmp_path: Path) -> Path:
    from ssis_adf_agent.migration_plan import (
        AuthMode,
        InfrastructureItem,
        MigrationPlan,
        TargetPattern,
        save_plan,
    )

    plan = MigrationPlan(
        package_name="Demo",
        package_path="demo.dtsx",
        target_pattern=TargetPattern.CUSTOM,
        infrastructure_needed=[
            InfrastructureItem(
                type="Microsoft.DataFactory/factories",
                name_hint="adf",
                purpose="Workload factory.",
            ),
        ],
    )
    out = tmp_path / "demo.plan.json"
    save_plan(plan, out)
    return out


def test_provision_dry_run_offline_returns_bicep_only(saved_plan_path: Path, tmp_path: Path) -> None:
    """Offline dry_run must succeed without subscription_id / resource_group."""
    from ssis_adf_agent.mcp_server import _provision_adf_env

    bicep_out = tmp_path / "main.bicep"
    result = asyncio.run(_provision_adf_env({
        "plan_path": str(saved_plan_path),
        "output_bicep_path": str(bicep_out),
        "dry_run": True,
    }))
    payload = json.loads(result[0].text)
    # Bicep must always be written, regardless of compile outcome.
    assert bicep_out.exists()
    assert payload["bicep_saved_to"] == str(bicep_out)
    assert payload["mode"] == "offline_dry_run"
    # status will be 'bicep_compiled' if `az bicep build` is on PATH, else
    # 'bicep_compile_failed'. Either way the tool must NOT have raised.
    assert payload["status"] in {"bicep_compiled", "bicep_compile_failed"}


def test_provision_live_deploy_requires_azure_identifiers(saved_plan_path: Path) -> None:
    from ssis_adf_agent.mcp_server import _provision_adf_env

    with pytest.raises(ValueError, match="subscription_id and resource_group"):
        asyncio.run(_provision_adf_env({
            "plan_path": str(saved_plan_path),
            "dry_run": False,
        }))


# ---------------------------------------------------------------------------
# Bug #5: build_estate_report should honor max_packages_per_wave when
# deriving waves inline (so the PDF wave_count matches user expectations).
# ---------------------------------------------------------------------------

def test_build_estate_report_honors_max_packages_per_wave(tmp_path: Path) -> None:
    from ssis_adf_agent.migration_plan import (
        MigrationPlan,
        TargetPattern,
        save_plan,
    )

    # Load the real PDF builder lazily so reportlab errors surface in the test.
    pytest.importorskip("reportlab")
    from ssis_adf_agent.mcp_server import _build_estate_pdf

    plans_dir = tmp_path / "plans"
    plans_dir.mkdir()
    for i in range(6):
        plan = MigrationPlan(
            package_name=f"Pkg{i}",
            package_path=f"pkg{i}.dtsx",
            target_pattern=TargetPattern.SCHEDULED_FILE_DROP,
        )
        save_plan(plan, plans_dir / f"pkg{i}_plan.json")

    pdf_out = tmp_path / "estate.pdf"
    result = asyncio.run(_build_estate_pdf({
        "plans_dir": str(plans_dir),
        "output_pdf": str(pdf_out),
        "max_packages_per_wave": 2,
    }))
    payload = json.loads(result[0].text)
    # 6 packages with cap 2 → 3 waves.
    assert payload["wave_count"] == 3


# ---------------------------------------------------------------------------
# Subscription name → GUID resolution
# ---------------------------------------------------------------------------

def test_resolve_subscription_id_passes_guid_through() -> None:
    from ssis_adf_agent.credential import resolve_subscription_id

    guid = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
    assert resolve_subscription_id(guid) == guid.lower()


def test_resolve_subscription_id_passes_uppercase_guid() -> None:
    from ssis_adf_agent.credential import resolve_subscription_id

    guid = "A1B2C3D4-E5F6-7890-ABCD-EF1234567890"
    assert resolve_subscription_id(guid) == guid.lower()


def test_resolve_subscription_id_name_lookup(monkeypatch: pytest.MonkeyPatch) -> None:
    """When given a non-GUID string, resolve via SubscriptionClient."""
    from types import SimpleNamespace
    from ssis_adf_agent import credential

    fake_sub = SimpleNamespace(
        subscription_id="11111111-2222-3333-4444-555555555555",
        display_name="Sub1",
    )

    class FakeSubList:
        def list(self):
            return [fake_sub]

    class FakeSubClient:
        def __init__(self, cred):
            self.subscriptions = FakeSubList()

    monkeypatch.setattr(
        "ssis_adf_agent.credential.SubscriptionClient",
        FakeSubClient,
        raising=False,
    )
    # Ensure the import inside the function resolves to our fake
    import ssis_adf_agent.credential as cred_mod
    monkeypatch.setattr(cred_mod, "SubscriptionClient", FakeSubClient, raising=False)

    # Patch the lazy import path used by resolve_subscription_id
    import azure.mgmt.resource
    monkeypatch.setattr(azure.mgmt.resource, "SubscriptionClient", FakeSubClient, raising=False)

    result = credential.resolve_subscription_id("Sub1")
    assert result == "11111111-2222-3333-4444-555555555555"


def test_resolve_subscription_id_name_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    from types import SimpleNamespace
    from ssis_adf_agent import credential

    class FakeSubList:
        def list(self):
            return []

    class FakeSubClient:
        def __init__(self, cred):
            self.subscriptions = FakeSubList()

    import azure.mgmt.resource
    monkeypatch.setattr(azure.mgmt.resource, "SubscriptionClient", FakeSubClient, raising=False)

    with pytest.raises(ValueError, match="No accessible subscription"):
        credential.resolve_subscription_id("NonExistent")
