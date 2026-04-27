"""Tests for the cost-actuals join helper (P4-5)."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from ssis_adf_agent.migration_plan.cost_actuals import (
    ActualRow,
    compare_estimates_to_actuals,
    load_actuals,
)


# ---------------------------------------------------------------------------
# Captured fixtures (real-shape Cost Management responses, sanitized)
# ---------------------------------------------------------------------------

SUB_ID = "00000000-0000-0000-0000-000000000001"
RG = "rg-ssis-prod"
FACTORY = "adf-ssis-prod"
FACTORY_ID = (
    f"/subscriptions/{SUB_ID}/resourceGroups/{RG}"
    f"/providers/Microsoft.DataFactory/factories/{FACTORY}"
)
PIPELINE_ID = f"{FACTORY_ID}/pipelines/PL_LoadDimCustomer"
IR_ID = f"{FACTORY_ID}/integrationRuntimes/AutoResolveIntegrationRuntime"


COST_MGMT_REST_RESPONSE: dict = {
    "id": "/subscriptions/.../providers/Microsoft.CostManagement/query/abc",
    "name": "abc",
    "type": "Microsoft.CostManagement/query",
    "properties": {
        "nextLink": None,
        "columns": [
            {"name": "PreTaxCost",  "type": "Number"},
            {"name": "ResourceId",  "type": "String"},
            {"name": "ServiceName", "type": "String"},
            {"name": "Meter",       "type": "String"},
            {"name": "Currency",    "type": "String"},
        ],
        "rows": [
            [123.45, FACTORY_ID,  "Azure Data Factory v2", "Activity Run",            "USD"],
            [ 87.20, FACTORY_ID,  "Azure Data Factory v2", "Data Movement DIU-Hours", "USD"],
            [ 41.10, IR_ID,       "Azure Data Factory v2", "Pipeline Orchestration",  "USD"],
            [  9.99, "/subscriptions/X/resourceGroups/other/providers/"
                     "Microsoft.Storage/storageAccounts/sa",
             "Storage", "Hot LRS Data Stored", "USD"],
        ],
    },
}


def _lineage_for_two_pipelines(pipeline_factory_id: str) -> dict:
    """A lineage manifest with two deployed pipelines under the same factory."""
    return {
        "schema_version": "1.0",
        "generated_at": "2026-04-01T00:00:00Z",
        "agent_version": "0.1.0",
        "source": {
            "package_name": "Estate",
            "package_id": "{abc}",
            "source_file": "C:/src/Estate.dtsx",
            "sha256": "abc",
            "protection_level": "EncryptSensitiveWithUserKey",
            "ssis_top_level_task_count": 5,
            "connection_manager_count": 2,
            "variable_count": 1,
        },
        "artifacts": {
            "pipeline": [
                {
                    "name": "PL_LoadDimCustomer",
                    "file": "pipeline/PL_LoadDimCustomer.json",
                    "activity_count": 3,
                    "azure_resource_id": f"{pipeline_factory_id}/pipelines/PL_LoadDimCustomer",
                },
                {
                    "name": "PL_LoadFactSales",
                    "file": "pipeline/PL_LoadFactSales.json",
                    "activity_count": 7,
                    "azure_resource_id": f"{pipeline_factory_id}/pipelines/PL_LoadFactSales",
                },
            ],
            "linked_services": [],
            "datasets": [],
            "data_flows": [],
            "triggers": [],
        },
        "activity_origins": [],
    }


@pytest.fixture
def lineage_path(tmp_path: Path) -> Path:
    p = tmp_path / "lineage.json"
    p.write_text(json.dumps(_lineage_for_two_pipelines(FACTORY_ID)), encoding="utf-8")
    return p


@pytest.fixture
def cost_csv_path(tmp_path: Path) -> Path:
    """Portal Cost Analysis CSV export — sanitized columns."""
    csv_text = (
        "Date,ResourceId,ServiceName,MeterCategory,Meter,PreTaxCost,Currency\n"
        f"2026-03-31,{FACTORY_ID},Azure Data Factory v2,Azure Data Factory v2,Activity Run,123.45,USD\n"
        f"2026-03-31,{FACTORY_ID},Azure Data Factory v2,Azure Data Factory v2,Data Movement DIU-Hours,87.20,USD\n"
        f"2026-03-31,{IR_ID},Azure Data Factory v2,Azure Data Factory v2,Pipeline Orchestration,41.10,USD\n"
        f"2026-03-31,/subscriptions/X/resourceGroups/other/providers/Microsoft.Storage/storageAccounts/sa,Storage,Storage,Hot LRS Data Stored,9.99,USD\n"
    )
    p = tmp_path / "cost.csv"
    p.write_text(csv_text, encoding="utf-8")
    return p


# ---------------------------------------------------------------------------
# load_actuals
# ---------------------------------------------------------------------------


class TestLoadActuals:
    def test_rest_dict_normalizes_rows(self) -> None:
        rows = load_actuals(COST_MGMT_REST_RESPONSE)
        assert len(rows) == 4
        assert all(isinstance(r, ActualRow) for r in rows)
        first = rows[0]
        assert first.resource_id == FACTORY_ID
        assert first.cost == pytest.approx(123.45)
        assert first.currency == "USD"
        assert first.service == "Azure Data Factory v2"
        assert first.meter == "Activity Run"

    def test_rest_dict_unwrapped_properties_accepted(self) -> None:
        # Some pipelines pre-unwrap ``properties``.
        unwrapped = COST_MGMT_REST_RESPONSE["properties"]
        rows = load_actuals(unwrapped)
        assert len(rows) == 4

    def test_rest_dict_missing_columns_raises(self) -> None:
        bad = {"properties": {"columns": [{"name": "Foo"}], "rows": [[1]]}}
        with pytest.raises(ValueError, match="missing required columns"):
            load_actuals(bad)

    def test_rest_dict_empty_returns_empty_list(self) -> None:
        assert load_actuals({"properties": {"columns": [], "rows": []}}) == []

    def test_csv_path_normalizes_rows(self, cost_csv_path: Path) -> None:
        rows = load_actuals(cost_csv_path)
        assert len(rows) == 4
        rids = {r.resource_id for r in rows}
        assert FACTORY_ID in rids
        assert IR_ID in rids

    def test_csv_path_string_accepted(self, cost_csv_path: Path) -> None:
        rows = load_actuals(str(cost_csv_path))
        assert len(rows) == 4

    def test_csv_with_bom_handled(self, tmp_path: Path) -> None:
        text = "\ufeffResourceId,Cost\nrid-1,5.00\n"
        p = tmp_path / "boom.csv"
        p.write_text(text, encoding="utf-8")
        rows = load_actuals(p)
        assert rows == [ActualRow(resource_id="rid-1", cost=5.00)]

    def test_csv_missing_required_columns_raises(self, tmp_path: Path) -> None:
        p = tmp_path / "bad.csv"
        p.write_text("Date,Foo\n2026-01-01,bar\n", encoding="utf-8")
        with pytest.raises(ValueError, match="missing required columns"):
            load_actuals(p)

    def test_json_path_loads_rest(self, tmp_path: Path) -> None:
        p = tmp_path / "cm.json"
        p.write_text(json.dumps(COST_MGMT_REST_RESPONSE), encoding="utf-8")
        rows = load_actuals(p)
        assert len(rows) == 4

    def test_json_path_non_object_rejected(self, tmp_path: Path) -> None:
        p = tmp_path / "cm.json"
        p.write_text("[1, 2, 3]", encoding="utf-8")
        with pytest.raises(ValueError, match="REST response object"):
            load_actuals(p)

    def test_unsupported_extension_rejected(self, tmp_path: Path) -> None:
        p = tmp_path / "x.xlsx"
        p.write_text("ignored", encoding="utf-8")
        with pytest.raises(ValueError, match="Unsupported actuals file type"):
            load_actuals(p)

    def test_missing_path_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            load_actuals(tmp_path / "nope.json")

    def test_unsupported_source_type_rejected(self) -> None:
        with pytest.raises(TypeError, match="Unsupported actuals source"):
            load_actuals(42)

    def test_pre_normalized_list_accepted(self) -> None:
        rows = load_actuals([
            {"resource_id": "rid-1", "cost": 1.0, "currency": "USD"},
            {"ResourceId": "rid-2", "Cost": 2.5},
            ActualRow(resource_id="rid-3", cost=3.0),
        ])
        assert len(rows) == 3
        assert rows[1].resource_id == "rid-2"
        assert rows[1].cost == pytest.approx(2.5)

    def test_pre_normalized_list_drops_blank_resource_ids(self) -> None:
        rows = load_actuals([{"resource_id": "", "cost": 1.0}])
        assert rows == []


# ---------------------------------------------------------------------------
# compare_estimates_to_actuals
# ---------------------------------------------------------------------------


class TestCompare:
    def test_factory_total_and_meter_breakdown(self, lineage_path: Path) -> None:
        out = compare_estimates_to_actuals(
            lineage_path=lineage_path,
            actuals_source=COST_MGMT_REST_RESPONSE,
            period_label="2026-03",
        )
        # Factory + IR row roll up; storage row is filtered out.
        assert out["factory_resource_id"] == FACTORY_ID
        assert out["actuals_total_usd"] == pytest.approx(123.45 + 87.20 + 41.10, rel=1e-6)
        assert "Activity Run" in out["actuals_by_meter"]
        assert out["actuals_by_meter"]["Pipeline Orchestration"] == pytest.approx(41.10)
        assert "9.99" not in json.dumps(out)  # storage excluded

    def test_variance_against_estimate(self, lineage_path: Path) -> None:
        estimate = {"monthly_total_usd": 200.00}
        out = compare_estimates_to_actuals(
            lineage_path=lineage_path,
            actuals_source=COST_MGMT_REST_RESPONSE,
            estimate=estimate,
        )
        assert out["estimate_monthly_usd"] == pytest.approx(200.00)
        # Actuals 251.75 vs estimate 200 → +51.75 = +25.875%
        assert out["variance_usd"] == pytest.approx(51.75, rel=1e-3)
        assert out["variance_pct"] == pytest.approx(25.875, rel=1e-3)

    def test_no_estimate_yields_null_pct(self, lineage_path: Path) -> None:
        out = compare_estimates_to_actuals(
            lineage_path=lineage_path,
            actuals_source=COST_MGMT_REST_RESPONSE,
        )
        assert out["estimate_monthly_usd"] == 0.0
        assert out["variance_pct"] is None

    def test_per_pipeline_allocation_weighted_by_activity_count(
        self, lineage_path: Path,
    ) -> None:
        out = compare_estimates_to_actuals(
            lineage_path=lineage_path,
            actuals_source=COST_MGMT_REST_RESPONSE,
        )
        pipes = out["pipelines"]
        assert len(pipes) == 2
        # weights: 3 / 10 and 7 / 10.
        by_name = {p["pipeline_name"]: p for p in pipes}
        assert by_name["PL_LoadDimCustomer"]["weight"] == pytest.approx(0.3)
        assert by_name["PL_LoadFactSales"]["weight"] == pytest.approx(0.7)
        total = (by_name["PL_LoadDimCustomer"]["allocated_actuals_usd"]
                 + by_name["PL_LoadFactSales"]["allocated_actuals_usd"])
        assert total == pytest.approx(out["actuals_total_usd"], abs=0.02)
        assert all(p["allocation"] == "estimated" for p in pipes)

    def test_per_pipeline_allocation_carries_warning_note(
        self, lineage_path: Path,
    ) -> None:
        out = compare_estimates_to_actuals(
            lineage_path=lineage_path,
            actuals_source=COST_MGMT_REST_RESPONSE,
        )
        assert any("does not invoice ADF spend below factory" in n
                   for n in out["notes"])

    def test_no_matching_actuals_emits_note_and_zero_total(
        self, tmp_path: Path,
    ) -> None:
        # Lineage points at a different factory than the actuals.
        other = "/subscriptions/Y/resourceGroups/r/providers/Microsoft.DataFactory/factories/other"
        p = tmp_path / "lineage.json"
        p.write_text(
            json.dumps(_lineage_for_two_pipelines(other)),
            encoding="utf-8",
        )
        out = compare_estimates_to_actuals(
            lineage_path=p,
            actuals_source=COST_MGMT_REST_RESPONSE,
        )
        assert out["actuals_total_usd"] == 0.0
        assert out["pipelines"] == []  # no allocation when total is zero
        assert any("none matched factory id" in n for n in out["notes"])

    def test_factory_resource_id_override_wins(self, lineage_path: Path) -> None:
        # Override with bogus id → no rows match.
        out = compare_estimates_to_actuals(
            lineage_path=lineage_path,
            actuals_source=COST_MGMT_REST_RESPONSE,
            factory_resource_id="/subscriptions/zzz/resourceGroups/r"
                                 "/providers/Microsoft.DataFactory/factories/zzz",
        )
        assert out["actuals_total_usd"] == 0.0
        assert "factories/zzz" in out["factory_resource_id"]

    def test_unresolved_factory_emits_note(self, tmp_path: Path) -> None:
        # Lineage with no azure_resource_id values (pre-deploy state).
        manifest = _lineage_for_two_pipelines(FACTORY_ID)
        for row in manifest["artifacts"]["pipeline"]:
            row["azure_resource_id"] = ""
        p = tmp_path / "lineage.json"
        p.write_text(json.dumps(manifest), encoding="utf-8")
        out = compare_estimates_to_actuals(
            lineage_path=p,
            actuals_source=COST_MGMT_REST_RESPONSE,
        )
        assert out["factory_resource_id"] == ""
        assert any("No factory resource id" in n for n in out["notes"])

    def test_csv_actuals_match_rest_actuals(
        self, lineage_path: Path, cost_csv_path: Path,
    ) -> None:
        rest_out = compare_estimates_to_actuals(
            lineage_path=lineage_path,
            actuals_source=COST_MGMT_REST_RESPONSE,
        )
        csv_out = compare_estimates_to_actuals(
            lineage_path=lineage_path,
            actuals_source=cost_csv_path,
        )
        assert csv_out["actuals_total_usd"] == pytest.approx(
            rest_out["actuals_total_usd"]
        )

    def test_legacy_pipeline_dict_shape_supported(self, tmp_path: Path) -> None:
        # Older manifests put `pipeline` as a single dict, not a list.
        manifest = _lineage_for_two_pipelines(FACTORY_ID)
        manifest["artifacts"]["pipeline"] = manifest["artifacts"]["pipeline"][0]
        p = tmp_path / "lineage.json"
        p.write_text(json.dumps(manifest), encoding="utf-8")
        out = compare_estimates_to_actuals(
            lineage_path=p,
            actuals_source=COST_MGMT_REST_RESPONSE,
        )
        assert out["factory_resource_id"] == FACTORY_ID
        assert len(out["pipelines"]) == 1

    def test_zero_activity_count_skips_allocation(self, tmp_path: Path) -> None:
        manifest = _lineage_for_two_pipelines(FACTORY_ID)
        for row in manifest["artifacts"]["pipeline"]:
            row["activity_count"] = 0
        p = tmp_path / "lineage.json"
        p.write_text(json.dumps(manifest), encoding="utf-8")
        out = compare_estimates_to_actuals(
            lineage_path=p,
            actuals_source=COST_MGMT_REST_RESPONSE,
        )
        assert out["pipelines"] == []
        assert any("zero combined activity_count" in n for n in out["notes"])

    def test_missing_lineage_raises(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            compare_estimates_to_actuals(
                lineage_path=tmp_path / "nope.json",
                actuals_source=COST_MGMT_REST_RESPONSE,
            )

    def test_period_label_is_echoed(self, lineage_path: Path) -> None:
        out = compare_estimates_to_actuals(
            lineage_path=lineage_path,
            actuals_source=COST_MGMT_REST_RESPONSE,
            period_label="2026-03",
        )
        assert out["period_label"] == "2026-03"

    def test_multi_currency_emits_warning_note(
        self, tmp_path: Path, lineage_path: Path,
    ) -> None:
        rows = [
            {"resource_id": FACTORY_ID, "cost": 100.0, "currency": "USD"},
            {"resource_id": FACTORY_ID, "cost": 50.0,  "currency": "EUR"},
        ]
        out = compare_estimates_to_actuals(
            lineage_path=lineage_path,
            actuals_source=rows,
        )
        assert any("Multiple currencies" in n for n in out["notes"])
