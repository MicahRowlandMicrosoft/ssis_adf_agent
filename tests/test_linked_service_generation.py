"""Tests for linked service generation enhancements.

Covers:
1. Connection string parsing
2. Name override support
2. Service Principal auth
3. SAS token support for Blob Storage
4. Account key support for Blob Storage
5. IR selection heuristic (expanded Azure suffixes)
6. Connection string component extraction (server, database, user, password)
7. Integrated Security detection
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from ssis_adf_agent.parsers.models import (
    ConnectionManagerType,
    SSISConnectionManager,
    SSISPackage,
    ProtectionLevel,
)
from ssis_adf_agent.generators.linked_service_generator import (
    parse_connection_string,
    generate_linked_services,
    _is_on_prem,
    _extract_server,
    _extract_database,
    _extract_user,
    _extract_password,
)


# ---------------------------------------------------------------------------
# Connection string parsing
# ---------------------------------------------------------------------------

class TestParseConnectionString:
    def test_oledb_connection_string(self):
        cs = "Server=mysvr;Database=mydb;User ID=sa;Password=secret123"
        parts = parse_connection_string(cs)
        assert parts["server"] == "mysvr"
        assert parts["database"] == "mydb"
        assert parts["user id"] == "sa"
        assert parts["password"] == "secret123"

    def test_adonet_data_source(self):
        cs = "Data Source=mysvr.database.windows.net;Initial Catalog=mydb;Integrated Security=SSPI"
        parts = parse_connection_string(cs)
        assert parts["data source"] == "mysvr.database.windows.net"
        assert parts["initial catalog"] == "mydb"
        assert parts["integrated security"] == "SSPI"

    def test_azure_storage_connection_string(self):
        cs = "DefaultEndpointsProtocol=https;AccountName=mystorage;AccountKey=abc123=="
        parts = parse_connection_string(cs)
        assert parts["accountname"] == "mystorage"
        assert parts["accountkey"] == "abc123=="

    def test_sas_token_in_connection_string(self):
        cs = "BlobEndpoint=https://act.blob.core.windows.net;SharedAccessSignature=sv=2020-08-04&ss=b"
        parts = parse_connection_string(cs)
        assert parts["blobendpoint"] == "https://act.blob.core.windows.net"
        assert "sv" in parts["sharedaccesssignature"]

    def test_empty_connection_string(self):
        assert parse_connection_string(None) == {}
        assert parse_connection_string("") == {}

    def test_service_principal_auth_marker(self):
        cs = "Server=svr.database.windows.net;Database=db;Authentication=ActiveDirectoryServicePrincipal;User ID=app-id;Password=secret"
        parts = parse_connection_string(cs)
        assert parts["authentication"] == "ActiveDirectoryServicePrincipal"
        assert parts["user id"] == "app-id"

    def test_account_key_with_equals(self):
        """Account keys often contain = characters (base64)."""
        cs = "AccountName=act;AccountKey=abc+def/ghi=="
        parts = parse_connection_string(cs)
        assert parts["accountname"] == "act"
        # The key is split on first = only
        assert parts["accountkey"] == "abc+def/ghi=="


# ---------------------------------------------------------------------------
# Component extraction helpers
# ---------------------------------------------------------------------------

class TestExtractComponents:
    def test_extract_server_from_server_key(self):
        parts = parse_connection_string("Server=mysvr;Database=db")
        assert _extract_server(parts) == "mysvr"

    def test_extract_server_from_data_source(self):
        parts = parse_connection_string("Data Source=mysvr.database.windows.net")
        assert _extract_server(parts) == "mysvr.database.windows.net"

    def test_extract_database_from_initial_catalog(self):
        parts = parse_connection_string("Initial Catalog=mydb")
        assert _extract_database(parts) == "mydb"

    def test_extract_user_from_uid(self):
        parts = parse_connection_string("UID=myuser")
        assert _extract_user(parts) == "myuser"

    def test_extract_password_from_pwd(self):
        parts = parse_connection_string("PWD=mysecret")
        assert _extract_password(parts) == "mysecret"

    def test_returns_none_when_missing(self):
        parts = parse_connection_string("Foo=bar")
        assert _extract_server(parts) is None
        assert _extract_database(parts) is None
        assert _extract_user(parts) is None
        assert _extract_password(parts) is None

    # --- Quoted-value tests (semicolons inside passwords / values) ----------

    def test_double_quoted_password_with_semicolons(self):
        cs = 'Server=mysvr;Password="p;w;d";Database=mydb'
        parts = parse_connection_string(cs)
        assert parts["server"] == "mysvr"
        assert parts["password"] == "p;w;d"
        assert parts["database"] == "mydb"

    def test_single_quoted_password_with_semicolons(self):
        cs = "Server=mysvr;Password='a;b;c';Database=mydb"
        parts = parse_connection_string(cs)
        assert parts["password"] == "a;b;c"
        assert parts["database"] == "mydb"

    def test_quoted_value_with_equals_and_semicolons(self):
        cs = 'AccountName=act;AccountKey="abc=;xyz=";EndpointSuffix=core.windows.net'
        parts = parse_connection_string(cs)
        assert parts["accountname"] == "act"
        assert parts["accountkey"] == "abc=;xyz="
        assert parts["endpointsuffix"] == "core.windows.net"

    def test_unquoted_value_still_splits_on_semicolons(self):
        cs = "Server=svr;Database=db"
        parts = parse_connection_string(cs)
        assert parts["server"] == "svr"
        assert parts["database"] == "db"

# ---------------------------------------------------------------------------
# IR selection heuristic
# ---------------------------------------------------------------------------

class TestIsOnPrem:
    def test_azure_sql_database(self):
        cm = SSISConnectionManager(
            id="1", name="AzureSQL", server="mysvr.database.windows.net"
        )
        assert _is_on_prem(cm) is False

    def test_azure_synapse(self):
        cm = SSISConnectionManager(
            id="2", name="Synapse", server="mysvr.sql.azuresynapse.net"
        )
        assert _is_on_prem(cm) is False

    def test_azure_blob_storage(self):
        cm = SSISConnectionManager(
            id="3", name="Blob",
            connection_string="DefaultEndpointsProtocol=https;AccountName=act;AccountKey=k"
        )
        assert _is_on_prem(cm) is False

    def test_cosmos_db(self):
        cm = SSISConnectionManager(
            id="4", name="Cosmos", server="myacct.documents.azure.com"
        )
        assert _is_on_prem(cm) is False

    def test_dfs_endpoint(self):
        cm = SSISConnectionManager(
            id="5", name="ADLS",
            connection_string="BlobEndpoint=https://act.dfs.core.windows.net"
        )
        assert _is_on_prem(cm) is False

    def test_managed_identity_in_cs(self):
        cm = SSISConnectionManager(
            id="6", name="MIDB",
            connection_string="Server=svr;Authentication=ActiveDirectoryManagedIdentity"
        )
        assert _is_on_prem(cm) is False

    def test_on_prem_server(self):
        cm = SSISConnectionManager(id="7", name="OnPrem", server="SQLPROD01")
        assert _is_on_prem(cm) is True

    def test_on_prem_ip_address(self):
        cm = SSISConnectionManager(id="8", name="OnPremIP", server="192.168.1.50")
        assert _is_on_prem(cm) is True

    def test_on_prem_connection_string_only(self):
        cm = SSISConnectionManager(
            id="9", name="OnPremCS",
            connection_string="Data Source=SQLPROD01;Initial Catalog=mydb"
        )
        assert _is_on_prem(cm) is True

    def test_mysql_azure(self):
        cm = SSISConnectionManager(
            id="10", name="MySQL", server="mydb.mysql.database.azure.com"
        )
        assert _is_on_prem(cm) is False

    def test_postgres_azure(self):
        cm = SSISConnectionManager(
            id="11", name="PG", server="mydb.postgres.database.azure.com"
        )
        assert _is_on_prem(cm) is False

    def test_core_windows_net_blob_endpoint(self):
        cm = SSISConnectionManager(
            id="12", name="Blob2", server="act.blob.core.windows.net"
        )
        assert _is_on_prem(cm) is False


# ---------------------------------------------------------------------------
# Service Principal auth
# ---------------------------------------------------------------------------

class TestServicePrincipalAuth:
    def test_explicit_service_principal_auth_type(self, tmp_path):
        """auth_type='ServicePrincipal' emits SP properties."""
        pkg = _make_package([
            SSISConnectionManager(
                id="cm1", name="AzureSP",
                type=ConnectionManagerType.OLEDB,
                server="svr.database.windows.net",
                database="mydb",
            ),
        ])
        results, _ = generate_linked_services(
            pkg, tmp_path, auth_type="ServicePrincipal"
        )
        assert len(results) == 1
        tp = results[0]["properties"]["typeProperties"]
        assert tp["authenticationType"] == "ServicePrincipal"
        assert "servicePrincipalId" in tp
        assert "tenant" in tp
        assert "servicePrincipalCredential" in tp

    def test_sp_detected_from_connection_string(self, tmp_path):
        """Connection string with Authentication=ActiveDirectoryServicePrincipal triggers SP auth."""
        pkg = _make_package([
            SSISConnectionManager(
                id="cm1", name="AutoSP",
                type=ConnectionManagerType.ADO_NET,
                connection_string=(
                    "Server=svr.database.windows.net;Database=mydb;"
                    "Authentication=ActiveDirectoryServicePrincipal;"
                    "User ID=app-id;Tenant ID=my-tenant"
                ),
            ),
        ])
        results, _ = generate_linked_services(pkg, tmp_path)
        tp = results[0]["properties"]["typeProperties"]
        assert tp["authenticationType"] == "ServicePrincipal"
        assert tp["servicePrincipalId"] == "app-id"
        assert tp["tenant"] == "my-tenant"

    def test_sp_with_key_vault(self, tmp_path):
        pkg = _make_package([
            SSISConnectionManager(
                id="cm1", name="SpKV",
                type=ConnectionManagerType.OLEDB,
                server="svr.database.windows.net",
                database="mydb",
            ),
        ])
        results, _ = generate_linked_services(
            pkg, tmp_path,
            auth_type="ServicePrincipal",
            use_key_vault=True,
        )
        # KV linked service + actual LS
        actual_ls = [r for r in results if r["name"] != "LS_KeyVault"]
        assert len(actual_ls) == 1
        cred = actual_ls[0]["properties"]["typeProperties"]["servicePrincipalCredential"]
        assert cred["type"] == "AzureKeyVaultSecret"


# ---------------------------------------------------------------------------
# Blob Storage SAS token
# ---------------------------------------------------------------------------

class TestBlobStorageSAS:
    def test_sas_token_from_connection_string(self, tmp_path):
        pkg = _make_package([
            SSISConnectionManager(
                id="cm1", name="BlobSAS",
                type=ConnectionManagerType.FLAT_FILE,
                connection_string=(
                    "BlobEndpoint=https://act.blob.core.windows.net;"
                    "SharedAccessSignature=sv=2020-08-04&ss=b&srt=o&sp=r"
                ),
            ),
        ])
        results, _ = generate_linked_services(pkg, tmp_path)
        tp = results[0]["properties"]["typeProperties"]
        assert "sasUri" in tp
        # When not using KV, it should be a SecureString
        assert tp["sasUri"]["type"] == "SecureString"
        assert "sv=2020-08-04" in tp["sasUri"]["value"]

    def test_sas_token_with_key_vault(self, tmp_path):
        pkg = _make_package([
            SSISConnectionManager(
                id="cm1", name="BlobSASKV",
                type=ConnectionManagerType.FLAT_FILE,
                connection_string=(
                    "BlobEndpoint=https://act.blob.core.windows.net;"
                    "SharedAccessSignature=sv=2020-08-04&ss=b"
                ),
            ),
        ])
        results, _ = generate_linked_services(pkg, tmp_path, use_key_vault=True)
        actual_ls = [r for r in results if r["name"] != "LS_KeyVault"]
        tp = actual_ls[0]["properties"]["typeProperties"]
        assert tp["sasUri"]["type"] == "AzureKeyVaultSecret"

    def test_account_key_extraction(self, tmp_path):
        pkg = _make_package([
            SSISConnectionManager(
                id="cm1", name="BlobKey",
                type=ConnectionManagerType.FLAT_FILE,
                connection_string=(
                    "DefaultEndpointsProtocol=https;AccountName=mystorage;AccountKey=abc123=="
                ),
            ),
        ])
        results, _ = generate_linked_services(pkg, tmp_path)
        tp = results[0]["properties"]["typeProperties"]
        assert "connectionString" in tp
        assert "mystorage" in tp["connectionString"]
        assert "abc123==" in tp["connectionString"]

    def test_account_key_with_key_vault(self, tmp_path):
        pkg = _make_package([
            SSISConnectionManager(
                id="cm1", name="BlobKeyKV",
                type=ConnectionManagerType.FLAT_FILE,
                connection_string="DefaultEndpointsProtocol=https;AccountName=act;AccountKey=key==",
            ),
        ])
        results, _ = generate_linked_services(pkg, tmp_path, use_key_vault=True)
        actual_ls = [r for r in results if r["name"] != "LS_KeyVault"]
        tp = actual_ls[0]["properties"]["typeProperties"]
        assert "accountKey" in tp
        assert tp["accountKey"]["type"] == "AzureKeyVaultSecret"

    def test_account_name_only_uses_service_endpoint(self, tmp_path):
        """Account name with no key/SAS → service endpoint (Managed Identity)."""
        pkg = _make_package([
            SSISConnectionManager(
                id="cm1", name="BlobMI",
                type=ConnectionManagerType.FLAT_FILE,
                connection_string="DefaultEndpointsProtocol=https;AccountName=myacct",
            ),
        ])
        results, _ = generate_linked_services(pkg, tmp_path)
        tp = results[0]["properties"]["typeProperties"]
        assert "serviceEndpoint" in tp
        assert "myacct" in tp["serviceEndpoint"]

    def test_flat_file_no_cs_fallback(self, tmp_path):
        """Flat file with no connection string → TODO placeholder."""
        pkg = _make_package([
            SSISConnectionManager(
                id="cm1", name="LocalFile",
                type=ConnectionManagerType.FLAT_FILE,
                file_path="C:\\data\\export.csv",
            ),
        ])
        results, _ = generate_linked_services(pkg, tmp_path)
        tp = results[0]["properties"]["typeProperties"]
        assert "connectionString" in tp
        assert "TODO" in tp["connectionString"]


# ---------------------------------------------------------------------------
# Connection string component extraction in OLEDB
# ---------------------------------------------------------------------------

class TestConnectionStringExtraction:
    def test_server_extracted_from_cs_when_model_field_empty(self, tmp_path):
        pkg = _make_package([
            SSISConnectionManager(
                id="cm1", name="CSOnly",
                type=ConnectionManagerType.OLEDB,
                connection_string="Data Source=SQLPROD01;Initial Catalog=Sales;User ID=app;Password=pw",
            ),
        ])
        results, _ = generate_linked_services(pkg, tmp_path)
        tp = results[0]["properties"]["typeProperties"]
        assert tp["server"] == "SQLPROD01"
        assert tp["database"] == "Sales"
        assert tp["userName"] == "app"

    def test_model_fields_take_precedence(self, tmp_path):
        """Explicit model fields should not be overridden by connection string."""
        pkg = _make_package([
            SSISConnectionManager(
                id="cm1", name="Explicit",
                type=ConnectionManagerType.OLEDB,
                server="explicit-server",
                database="explicit-db",
                connection_string="Server=cs-server;Database=cs-db",
            ),
        ])
        results, _ = generate_linked_services(pkg, tmp_path)
        tp = results[0]["properties"]["typeProperties"]
        assert tp["server"] == "explicit-server"
        assert tp["database"] == "explicit-db"

    def test_integrated_security_produces_windows_auth(self, tmp_path):
        """Integrated Security=SSPI in connection string → Windows auth."""
        pkg = _make_package([
            SSISConnectionManager(
                id="cm1", name="WinAuth",
                type=ConnectionManagerType.OLEDB,
                connection_string="Server=SQLPROD01;Database=db;Integrated Security=SSPI",
            ),
        ])
        results, _ = generate_linked_services(pkg, tmp_path)
        tp = results[0]["properties"]["typeProperties"]
        assert tp["authenticationType"] == "Windows"

    def test_managed_identity_detected_from_cs(self, tmp_path):
        pkg = _make_package([
            SSISConnectionManager(
                id="cm1", name="MiAuth",
                type=ConnectionManagerType.ADO_NET,
                connection_string="Server=svr.database.windows.net;Database=db;Authentication=ActiveDirectoryManagedIdentity",
            ),
        ])
        results, _ = generate_linked_services(pkg, tmp_path, auth_type="SQL")
        tp = results[0]["properties"]["typeProperties"]
        # CS auth hint should override the passed-in auth_type
        assert tp["authenticationType"] == "SystemAssignedManagedIdentity"


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _make_package(cms: list[SSISConnectionManager]) -> SSISPackage:
    return SSISPackage(
        id="pkg1",
        name="TestPackage",
        source_file="test.dtsx",
        connection_managers=cms,
    )


# ---------------------------------------------------------------------------
# Name override tests
# ---------------------------------------------------------------------------

class TestNameOverrides:
    def test_ls_name_override_via_cm_name(self, tmp_path):
        """LS:<cm_name> override replaces the auto-generated LS name."""
        pkg = _make_package([
            SSISConnectionManager(
                id="cm1", name="MyConn",
                type=ConnectionManagerType.OLEDB,
                server="svr", database="db",
            ),
        ])
        results, ls_map = generate_linked_services(
            pkg, tmp_path,
            name_overrides={"LS:MyConn": "LS_Custom_Name"},
        )
        assert len(results) == 1
        assert results[0]["name"] == "LS_Custom_Name"
        assert ls_map["cm1"] == "LS_Custom_Name"

    def test_ls_name_override_case_insensitive(self, tmp_path):
        """LS override key matching is case-insensitive."""
        pkg = _make_package([
            SSISConnectionManager(
                id="cm1", name="Database Source Connection Manager",
                type=ConnectionManagerType.OLEDB,
                server="svr", database="db",
            ),
        ])
        results, ls_map = generate_linked_services(
            pkg, tmp_path,
            name_overrides={"ls:database source connection manager": "LS_MySql"},
        )
        assert results[0]["name"] == "LS_MySql"

    def test_override_sanitizes_value(self, tmp_path):
        """Override values are sanitized to valid ADF names."""
        pkg = _make_package([
            SSISConnectionManager(
                id="cm1", name="MyConn",
                type=ConnectionManagerType.OLEDB,
                server="svr", database="db",
            ),
        ])
        results, _ = generate_linked_services(
            pkg, tmp_path,
            name_overrides={"LS:MyConn": "LS-Has-Hyphens!"},
        )
        assert results[0]["name"] == "LS_Has_Hyphens"

    def test_non_overridden_cms_keep_auto_names(self, tmp_path):
        """CMs without an override keep their auto-generated names."""
        pkg = _make_package([
            SSISConnectionManager(
                id="cm1", name="ConnA",
                type=ConnectionManagerType.OLEDB,
                server="svr", database="db",
            ),
            SSISConnectionManager(
                id="cm2", name="ConnB",
                type=ConnectionManagerType.OLEDB,
                server="svr2", database="db2",
            ),
        ])
        results, ls_map = generate_linked_services(
            pkg, tmp_path,
            name_overrides={"LS:ConnA": "LS_Custom"},
        )
        assert ls_map["cm1"] == "LS_Custom"
        # cm2 should have auto-generated name
        assert ls_map["cm2"].startswith("LS_TestPackage_")


class TestNamingFunctionOverrides:
    """Test ds_name, df_name, pl_name, tr_name with name_overrides."""

    def test_ds_name_override(self):
        from ssis_adf_agent.generators.naming import ds_name
        result = ds_name("Pkg", "MySource", name_overrides={"DS:MySource": "DS_Custom"})
        assert result == "DS_Custom"

    def test_ds_name_no_override(self):
        from ssis_adf_agent.generators.naming import ds_name
        result = ds_name("Pkg", "MySource", name_overrides={"DS:Other": "DS_X"})
        assert result == "DS_Pkg_MySource"

    def test_df_name_override(self):
        from ssis_adf_agent.generators.naming import df_name
        result = df_name("Pkg", "LoadData", name_overrides={"DF:LoadData": "DF_Custom"})
        assert result == "DF_Custom"

    def test_pl_name_override(self):
        from ssis_adf_agent.generators.naming import pl_name
        result = pl_name("Pkg", name_overrides={"PL": "PL_MyPipeline"})
        assert result == "PL_MyPipeline"

    def test_tr_name_override(self):
        from ssis_adf_agent.generators.naming import tr_name
        result = tr_name("Pkg", name_overrides={"TR": "TR_MyTrigger"})
        assert result == "TR_MyTrigger"

    def test_overrides_are_case_insensitive(self):
        from ssis_adf_agent.generators.naming import pl_name
        result = pl_name("Pkg", name_overrides={"pl": "PL_Custom"})
        assert result == "PL_Custom"



class TestConnectViaStripping:
    """Linked services targeting the implicit default IR must omit connectVia.

    ADF rejects explicit `connectVia.referenceName=AutoResolveIntegrationRuntime`
    on factories where the default IR has not been materialized yet
    (HTTP 400: "Could not get integration runtime details for
    AutoResolveIntegrationRuntime"). Treat absence as the default.
    """

    def test_cloud_ls_omits_connectvia(self, tmp_path):
        # Azure SQL connection -> Azure-native LS -> default IR -> connectVia stripped
        pkg = _make_package([
            SSISConnectionManager(
                id="cm1", name="AzureSql",
                type=ConnectionManagerType.OLEDB,
                server="svr.database.windows.net",
                database="mydb",
            ),
        ])
        results, _ = generate_linked_services(pkg, tmp_path)
        assert len(results) == 1
        assert "connectVia" not in results[0]["properties"], (
            "Cloud LSes targeting AutoResolveIntegrationRuntime should omit connectVia"
        )

    def test_on_prem_ls_keeps_connectvia(self, tmp_path):
        # On-prem SQL -> SHIR -> connectVia retained
        pkg = _make_package([
            SSISConnectionManager(
                id="cm1", name="OnPrem",
                type=ConnectionManagerType.OLEDB,
                server=".\\sql2016",
                database="AdventureWorks2016",
            ),
        ])
        results, _ = generate_linked_services(pkg, tmp_path)
        assert results[0]["properties"]["connectVia"]["referenceName"] == "SelfHostedIR"
