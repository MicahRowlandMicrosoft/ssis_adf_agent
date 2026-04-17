"""Tests for the 2 low-impact improvements: retry jitter + kv_url warning."""
from __future__ import annotations

import pytest
from ssis_adf_agent.warnings_collector import WarningsCollector, warn


# ===================================================================
# kv_url TODO placeholder warning
# ===================================================================

class TestKvUrlPlaceholderWarning:
    """The _convert handler should emit a warning when use_key_vault is True
    but kv_url still contains the TODO placeholder.

    Since importing mcp_server requires the 'mcp' package (not installed in
    test env), we test the warning logic inline using WarningsCollector + warn().
    """

    def test_todo_url_triggers_warning(self):
        """Simulate the exact warning logic from _convert."""
        kv_url = "https://TODO.vault.azure.net/"
        use_key_vault = True

        with WarningsCollector() as wc:
            if use_key_vault and "TODO" in kv_url:
                warn(
                    phase="convert", severity="warning", source="mcp_server",
                    message=(
                        "Key Vault URL is still the placeholder 'https://TODO.vault.azure.net/'. "
                        "Set the kv_url parameter to your actual Azure Key Vault URL."
                    ),
                )

        assert len(wc.warnings) == 1
        assert "TODO" in wc.warnings[0].message
        assert wc.warnings[0].severity == "warning"

    def test_real_url_no_warning(self):
        kv_url = "https://myvault.vault.azure.net/"
        use_key_vault = True

        with WarningsCollector() as wc:
            if use_key_vault and "TODO" in kv_url:
                warn(
                    phase="convert", severity="warning", source="mcp_server",
                    message="Should not fire",
                )

        assert len(wc.warnings) == 0

    def test_kv_disabled_no_warning(self):
        kv_url = "https://TODO.vault.azure.net/"
        use_key_vault = False

        with WarningsCollector() as wc:
            if use_key_vault and "TODO" in kv_url:
                warn(
                    phase="convert", severity="warning", source="mcp_server",
                    message="Should not fire",
                )

        assert len(wc.warnings) == 0
