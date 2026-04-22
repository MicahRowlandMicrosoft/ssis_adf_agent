"""N3 — configurable prefix overrides for naming helpers."""
from __future__ import annotations

from ssis_adf_agent.generators.naming import (
    df_name,
    ds_name,
    pl_name,
    tr_name,
)


class TestPrefixOverrides:
    def test_default_prefixes_unchanged(self):
        assert ds_name("Pkg", "Src").startswith("DS_")
        assert df_name("Pkg", "DFT").startswith("DF_")
        assert pl_name("Pkg").startswith("PL_")
        assert tr_name("Pkg").startswith("TR_")

    def test_dataset_prefix_override(self):
        # Trailing underscore preserved; hyphen sanitized to underscore.
        assert ds_name("Pkg", "Src", name_overrides={"DS_PREFIX": "ds_"}) == "ds_Pkg_Src"

    def test_dataflow_prefix_override(self):
        assert df_name("Pkg", "DFT", name_overrides={"DF_PREFIX": "MyDf_"}) == "MyDf_Pkg_DFT"

    def test_pipeline_prefix_override(self):
        assert pl_name("Pkg", name_overrides={"PL_PREFIX": "Pipe_"}) == "Pipe_Pkg"

    def test_trigger_prefix_override(self):
        assert tr_name("Pkg", name_overrides={"TR_PREFIX": "Trig_"}) == "Trig_Pkg"

    def test_empty_string_prefix_drops_prefix(self):
        assert ds_name("Pkg", "Src", name_overrides={"DS_PREFIX": ""}) == "Pkg_Src"
        assert pl_name("Pkg", name_overrides={"PL_PREFIX": ""}) == "Pkg"

    def test_per_artifact_override_wins_over_prefix(self):
        # PL exact override should beat PL_PREFIX
        out = pl_name("Pkg", name_overrides={"PL": "MyPipeline", "PL_PREFIX": "ignored_"})
        assert out == "MyPipeline"

    def test_prefix_case_insensitive(self):
        assert ds_name("Pkg", "Src", name_overrides={"ds_prefix": "x_"}) == "x_Pkg_Src"
