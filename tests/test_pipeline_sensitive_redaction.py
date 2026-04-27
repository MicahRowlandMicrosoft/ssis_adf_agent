"""Tests for B3 — sensitive variable / parameter defaultValue redaction in
generated pipeline JSON.

The pipeline generator must not ship credential-shaped values (Windows-domain
account names, on-prem FQDNs, password-style names) as cleartext defaultValues.
"""
from __future__ import annotations

from ssis_adf_agent.generators.pipeline_generator import (
    _is_sensitive_name,
    _looks_like_credential_value,
    _redact_sensitive_default,
)


class TestSensitiveNameDetection:
    def test_password_variants(self):
        assert _is_sensitive_name("Password")
        assert _is_sensitive_name("DbPassword")
        assert _is_sensitive_name("PWD")
        assert _is_sensitive_name("dbPwd")

    def test_account_variants(self):
        assert _is_sensitive_name("DBUserID")
        assert _is_sensitive_name("user_id")
        assert _is_sensitive_name("ServiceAccount")
        assert _is_sensitive_name("LoginName")

    def test_secret_variants(self):
        assert _is_sensitive_name("ClientSecret")
        assert _is_sensitive_name("ApiKey")
        assert _is_sensitive_name("SasToken")
        assert _is_sensitive_name("ConnectionString")

    def test_non_sensitive(self):
        assert not _is_sensitive_name("BatchSize")
        assert not _is_sensitive_name("FilePath")
        assert not _is_sensitive_name("Region")
        assert not _is_sensitive_name("Database")  # database name alone is not a credential


class TestCredentialValueDetection:
    def test_domain_account(self):
        assert _looks_like_credential_value("LNI\\svcOneWAWIP235")
        assert _looks_like_credential_value("CONTOSO\\jdoe")

    def test_on_prem_fqdn(self):
        assert _looks_like_credential_value("LNIsqTumSTGEX.lni.wa.lcl\\INT,49377")
        assert _looks_like_credential_value("server01.corp.local")
        assert _looks_like_credential_value("hostname.internal")

    def test_azure_hostname_not_flagged(self):
        # Azure hosts are public and not credential-shaped.
        assert not _looks_like_credential_value("myacct.blob.core.windows.net")
        assert not _looks_like_credential_value("mydb.database.windows.net")

    def test_plain_strings(self):
        assert not _looks_like_credential_value("PROD")
        assert not _looks_like_credential_value("DailyTrans")
        assert not _looks_like_credential_value("temp")


class TestRedaction:
    def test_strips_value_when_name_sensitive(self):
        safe, note = _redact_sensitive_default("DBUserID", "LNI\\svcOneWAWIP235")
        assert safe is None
        assert note is not None and "credential pattern" in note

    def test_strips_value_when_value_sensitive(self):
        safe, note = _redact_sensitive_default("DatabaseServer", "host.lni.wa.lcl\\INT,49377")
        assert safe is None
        assert note is not None and "FQDN" in note

    def test_passes_through_safe_value(self):
        safe, note = _redact_sensitive_default("Environment", "PROD")
        assert safe == "PROD"
        assert note is None

    def test_passes_through_database_name(self):
        safe, note = _redact_sensitive_default("Database", "DailyTrans")
        assert safe == "DailyTrans"
        assert note is None
