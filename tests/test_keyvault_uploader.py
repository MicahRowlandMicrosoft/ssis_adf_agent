"""P4-4 — tests for the encrypted-package automation helper.

Covers:
* secret extraction from sample unprotected .dtsx fragments
* secret-name slugification + templating
* upload_secrets with a fake SecretClient (overwrite/skip semantics)
* linked-service JSON rewrite (recursive, dry-run, no-match)
* process_encrypted_package end-to-end orchestrator
* never-leak: __repr__ on data classes redacts the value
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from ssis_adf_agent.deployer.keyvault_uploader import (
    DEFAULT_SECRET_NAME_TEMPLATE,
    ExtractedSecret,
    SecretMapping,
    UploadReport,
    build_secret_map,
    extract_secrets_from_dtsx,
    process_encrypted_package,
    rewrite_linked_services,
    upload_secrets,
)

DTS_NS = "www.microsoft.com/SqlServer/Dts"


# ---------------------------------------------------------------------------
# Fake SecretClient for upload_secrets tests
# ---------------------------------------------------------------------------

class _ResourceNotFoundError(Exception):
    pass


class FakeSecretClient:
    """Minimal SecretClient stand-in used by upload tests."""

    def __init__(self, existing: dict[str, str] | None = None, *, raise_on_set: str | None = None) -> None:
        self.vault: dict[str, str] = dict(existing or {})
        self.set_calls: list[tuple[str, str]] = []
        self.get_calls: list[str] = []
        self._raise_on_set = raise_on_set

    def get_secret(self, name: str) -> Any:
        self.get_calls.append(name)
        if name in self.vault:
            class _Got:
                def __init__(self, v: str) -> None:
                    self.value = v
            return _Got(self.vault[name])
        # Mimic azure SDK error class name so the uploader's heuristic works.
        raise _ResourceNotFoundError(f"Secret not found: {name}")


_ResourceNotFoundError.__name__ = "ResourceNotFoundError"


class FakeSecretClientWritable(FakeSecretClient):
    def set_secret(self, name: str, value: str) -> Any:
        self.set_calls.append((name, value))
        if self._raise_on_set == name:
            raise RuntimeError(f"simulated upload failure for {name}")
        self.vault[name] = value
        return object()


# ---------------------------------------------------------------------------
# Fixtures: small .dtsx fragments
# ---------------------------------------------------------------------------

_DTSX_WITH_DIRECT_PASSWORD = f"""<?xml version="1.0"?>
<DTS:Executable xmlns:DTS="{DTS_NS}" DTS:ObjectName="Pkg">
  <DTS:ConnectionManagers>
    <DTS:ConnectionManager DTS:ObjectName="CM_Source" DTS:CreationName="OLEDB">
      <DTS:ObjectData>
        <DTS:ConnectionManager DTS:ConnectionString="Data Source=srv;Initial Catalog=db;Provider=SQLOLEDB;">
          <DTS:Property DTS:Name="ConnectionString">Data Source=srv;Initial Catalog=db;Provider=SQLOLEDB;</DTS:Property>
          <DTS:Property DTS:Name="Password">SuperSecret123!</DTS:Property>
        </DTS:ConnectionManager>
      </DTS:ObjectData>
    </DTS:ConnectionManager>
  </DTS:ConnectionManagers>
</DTS:Executable>
"""

_DTSX_WITH_CONNSTR_PASSWORD = f"""<?xml version="1.0"?>
<DTS:Executable xmlns:DTS="{DTS_NS}" DTS:ObjectName="Pkg">
  <DTS:ConnectionManagers>
    <DTS:ConnectionManager DTS:ObjectName="CM_Embedded">
      <DTS:ObjectData>
        <DTS:ConnectionManager>
          <DTS:Property DTS:Name="ConnectionString">Data Source=srv;User ID=u;Password=EmbeddedPw;Initial Catalog=db</DTS:Property>
        </DTS:ConnectionManager>
      </DTS:ObjectData>
    </DTS:ConnectionManager>
  </DTS:ConnectionManagers>
</DTS:Executable>
"""

_DTSX_WITH_SENSITIVE_PARAM = f"""<?xml version="1.0"?>
<DTS:Executable xmlns:DTS="{DTS_NS}" DTS:ObjectName="Pkg">
  <DTS:PackageParameters>
    <DTS:PackageParameter DTS:ObjectName="DbPassword">
      <DTS:Property DTS:Name="Sensitive">1</DTS:Property>
      <DTS:Property DTS:Name="Value">ParamSecret</DTS:Property>
    </DTS:PackageParameter>
    <DTS:PackageParameter DTS:ObjectName="NotSecret">
      <DTS:Property DTS:Name="Sensitive">0</DTS:Property>
      <DTS:Property DTS:Name="Value">visible</DTS:Property>
    </DTS:PackageParameter>
  </DTS:PackageParameters>
</DTS:Executable>
"""

_DTSX_NO_SECRETS = f"""<?xml version="1.0"?>
<DTS:Executable xmlns:DTS="{DTS_NS}" DTS:ObjectName="Pkg">
  <DTS:ConnectionManagers>
    <DTS:ConnectionManager DTS:ObjectName="CM_NoCreds">
      <DTS:ObjectData>
        <DTS:ConnectionManager>
          <DTS:Property DTS:Name="ConnectionString">Data Source=srv;Integrated Security=SSPI;</DTS:Property>
        </DTS:ConnectionManager>
      </DTS:ObjectData>
    </DTS:ConnectionManager>
  </DTS:ConnectionManagers>
</DTS:Executable>
"""


def _write(tmp_path: Path, name: str, content: str) -> Path:
    p = tmp_path / name
    p.write_text(content, encoding="utf-8")
    return p


# ---------------------------------------------------------------------------
# extract_secrets_from_dtsx
# ---------------------------------------------------------------------------

class TestExtractSecrets:
    def test_extracts_direct_password_property(self, tmp_path: Path) -> None:
        p = _write(tmp_path, "pkg.dtsx", _DTSX_WITH_DIRECT_PASSWORD)
        secrets = extract_secrets_from_dtsx(p)
        assert len(secrets) == 1
        s = secrets[0]
        assert s.connection_manager_name == "CM_Source"
        assert s.kind == "password"
        assert s.value == "SuperSecret123!"
        assert s.source == "Properties.Password"

    def test_extracts_password_embedded_in_connection_string(self, tmp_path: Path) -> None:
        p = _write(tmp_path, "pkg.dtsx", _DTSX_WITH_CONNSTR_PASSWORD)
        secrets = extract_secrets_from_dtsx(p)
        assert len(secrets) == 1
        assert secrets[0].connection_manager_name == "CM_Embedded"
        assert secrets[0].value == "EmbeddedPw"
        assert secrets[0].source == "ConnectionString.Password"

    def test_extracts_sensitive_package_parameter(self, tmp_path: Path) -> None:
        p = _write(tmp_path, "pkg.dtsx", _DTSX_WITH_SENSITIVE_PARAM)
        secrets = extract_secrets_from_dtsx(p)
        # Only the Sensitive=1 param should surface; the visible one must not.
        assert len(secrets) == 1
        assert secrets[0].source == "Parameter.DbPassword"
        assert secrets[0].value == "ParamSecret"

    def test_no_secrets_returns_empty_list(self, tmp_path: Path) -> None:
        p = _write(tmp_path, "pkg.dtsx", _DTSX_NO_SECRETS)
        assert extract_secrets_from_dtsx(p) == []

    def test_invalid_xml_raises(self, tmp_path: Path) -> None:
        p = _write(tmp_path, "broken.dtsx", "<not-xml>")
        with pytest.raises(ValueError, match="not valid XML"):
            extract_secrets_from_dtsx(p)

    def test_repr_redacts_value(self) -> None:
        s = ExtractedSecret(
            connection_manager_name="CM",
            kind="password",
            value="hunter2",
            source="x",
        )
        r = repr(s)
        assert "hunter2" not in r
        assert "<redacted>" in r


# ---------------------------------------------------------------------------
# build_secret_map
# ---------------------------------------------------------------------------

class TestBuildSecretMap:
    def test_default_template_produces_kebab_case(self) -> None:
        extracted = [
            ExtractedSecret("CM_Src", "password", "v1", "Properties.Password"),
            ExtractedSecret("CM_Dst", "password", "v2", "Properties.Password"),
        ]
        m = build_secret_map(extracted, package_name="MyPkg")
        names = [s.secret_name for s in m]
        # Secret names are slugified (KV does not allow underscores).
        assert names == ["MyPkg-CM-Src-password", "MyPkg-CM-Dst-password"]
        assert all("_" not in n for n in names)
        # Placeholders must match the literal generator output (underscores
        # preserved); otherwise the rewrite cannot find them.
        placeholders = [s.placeholder_secret_name for s in m]
        assert placeholders == ["CM_Src-password", "CM_Dst-password"]

    def test_package_level_secret_uses_package_token(self) -> None:
        extracted = [ExtractedSecret("", "password", "v", "Parameter.X")]
        m = build_secret_map(extracted, package_name="P")
        assert m[0].secret_name == "P-package-password"
        assert m[0].placeholder_secret_name == "package-password"

    def test_repr_redacts_value(self) -> None:
        m = SecretMapping(secret_name="n", value="hunter2", placeholder_secret_name="p")
        assert "hunter2" not in repr(m)

    def test_custom_template_honored(self) -> None:
        extracted = [ExtractedSecret("CM", "password", "v", "x")]
        m = build_secret_map(
            extracted,
            package_name="P",
            secret_name_template="kv-{cm}-{kind}",
        )
        assert m[0].secret_name == "kv-CM-password"

    def test_unsluggable_name_raises(self) -> None:
        extracted = [ExtractedSecret("///", "///", "v", "x")]
        with pytest.raises(ValueError, match="Cannot slugify"):
            build_secret_map(extracted, package_name="///",
                              secret_name_template="{package}")


# ---------------------------------------------------------------------------
# upload_secrets
# ---------------------------------------------------------------------------

class TestUploadSecrets:
    def _mappings(self) -> list[SecretMapping]:
        return [
            SecretMapping("Pkg-CM-Src-password", "v1", "CM-Src-password"),
            SecretMapping("Pkg-CM-Dst-password", "v2", "CM-Dst-password"),
        ]

    def test_uploads_new_secrets(self) -> None:
        client = FakeSecretClientWritable()
        uploaded, skipped = upload_secrets(client, self._mappings())
        assert uploaded == ["Pkg-CM-Src-password", "Pkg-CM-Dst-password"]
        assert skipped == {}
        assert client.set_calls == [
            ("Pkg-CM-Src-password", "v1"),
            ("Pkg-CM-Dst-password", "v2"),
        ]

    def test_skips_existing_when_overwrite_false(self) -> None:
        client = FakeSecretClientWritable(existing={"Pkg-CM-Src-password": "preexisting"})
        uploaded, skipped = upload_secrets(client, self._mappings())
        assert uploaded == ["Pkg-CM-Dst-password"]
        assert "Pkg-CM-Src-password" in skipped
        assert "already exists" in skipped["Pkg-CM-Src-password"]
        # The existing secret value must NOT have been overwritten.
        assert client.vault["Pkg-CM-Src-password"] == "preexisting"

    def test_overwrite_replaces_existing(self) -> None:
        client = FakeSecretClientWritable(existing={"Pkg-CM-Src-password": "old"})
        uploaded, skipped = upload_secrets(client, self._mappings(), overwrite=True)
        assert "Pkg-CM-Src-password" in uploaded
        assert client.vault["Pkg-CM-Src-password"] == "v1"
        assert skipped == {}

    def test_dry_run_does_not_call_client(self) -> None:
        client = FakeSecretClientWritable()
        uploaded, skipped = upload_secrets(client, self._mappings(), dry_run=True)
        assert uploaded == ["Pkg-CM-Src-password", "Pkg-CM-Dst-password"]
        assert skipped == {}
        assert client.set_calls == []
        assert client.get_calls == []

    def test_set_failure_routed_to_skipped(self) -> None:
        client = FakeSecretClientWritable(raise_on_set="Pkg-CM-Src-password")
        uploaded, skipped = upload_secrets(client, self._mappings())
        assert "Pkg-CM-Dst-password" in uploaded
        assert "Pkg-CM-Src-password" in skipped
        assert "RuntimeError" in skipped["Pkg-CM-Src-password"]


# ---------------------------------------------------------------------------
# rewrite_linked_services
# ---------------------------------------------------------------------------

def _ls_with_kv_ref(secret_name: str, password_field: str = "password") -> dict:
    return {
        "name": "LS_OleDb_Src",
        "properties": {
            "type": "OleDb",
            "typeProperties": {
                "connectionString": "Data Source=...",
                password_field: {
                    "type": "AzureKeyVaultSecret",
                    "store": {"referenceName": "LS_KV", "type": "LinkedServiceReference"},
                    "secretName": secret_name,
                },
            },
        },
    }


def _ls_without_kv_ref() -> dict:
    return {
        "name": "LS_File",
        "properties": {
            "type": "AzureBlobStorage",
            "typeProperties": {"connectionString": "..."},
        },
    }


class TestRewriteLinkedServices:
    def test_rewrites_matching_secret_name(self, tmp_path: Path) -> None:
        ls_dir = tmp_path / "linkedService"
        ls_dir.mkdir()
        (ls_dir / "LS_Src.json").write_text(
            json.dumps(_ls_with_kv_ref("CM_Src-password")), encoding="utf-8"
        )
        rewritten, count = rewrite_linked_services(
            ls_dir, name_map={"CM_Src-password": "Pkg-CM-Src-password"}
        )
        assert count == 1
        assert len(rewritten) == 1
        loaded = json.loads((ls_dir / "LS_Src.json").read_text(encoding="utf-8"))
        assert loaded["properties"]["typeProperties"]["password"]["secretName"] == "Pkg-CM-Src-password"

    def test_dry_run_does_not_persist(self, tmp_path: Path) -> None:
        ls_dir = tmp_path / "linkedService"
        ls_dir.mkdir()
        ls_path = ls_dir / "LS_Src.json"
        original = json.dumps(_ls_with_kv_ref("CM_Src-password"))
        ls_path.write_text(original, encoding="utf-8")

        rewritten, count = rewrite_linked_services(
            ls_dir, name_map={"CM_Src-password": "X"}, dry_run=True
        )
        assert count == 1
        assert ls_path.read_text(encoding="utf-8") == original  # unchanged on disk

    def test_files_with_no_matching_ref_are_left_alone(self, tmp_path: Path) -> None:
        ls_dir = tmp_path / "linkedService"
        ls_dir.mkdir()
        (ls_dir / "LS_File.json").write_text(json.dumps(_ls_without_kv_ref()), encoding="utf-8")
        (ls_dir / "LS_Src.json").write_text(
            json.dumps(_ls_with_kv_ref("Other-password")), encoding="utf-8"
        )
        rewritten, count = rewrite_linked_services(
            ls_dir, name_map={"CM_Src-password": "X"}
        )
        # Other-password isn't in the map; LS_File has no KV ref at all.
        assert count == 0
        assert rewritten == []

    def test_recursive_rewrite_inside_lists(self, tmp_path: Path) -> None:
        ls_dir = tmp_path / "linkedService"
        ls_dir.mkdir()
        # An array-of-secrets shape (less common but worth covering).
        nested = {
            "properties": {
                "extras": [
                    {"type": "AzureKeyVaultSecret", "secretName": "p1"},
                    {"type": "AzureKeyVaultSecret", "secretName": "p2"},
                ]
            }
        }
        (ls_dir / "LS_X.json").write_text(json.dumps(nested), encoding="utf-8")
        rewritten, count = rewrite_linked_services(
            ls_dir, name_map={"p1": "P1-final", "p2": "P2-final"}
        )
        assert count == 2
        loaded = json.loads((ls_dir / "LS_X.json").read_text(encoding="utf-8"))
        assert loaded["properties"]["extras"][0]["secretName"] == "P1-final"
        assert loaded["properties"]["extras"][1]["secretName"] == "P2-final"

    def test_missing_directory_raises(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="not found"):
            rewrite_linked_services(tmp_path / "nope", name_map={})

    def test_invalid_json_raises(self, tmp_path: Path) -> None:
        ls_dir = tmp_path / "linkedService"
        ls_dir.mkdir()
        (ls_dir / "bad.json").write_text("not json", encoding="utf-8")
        with pytest.raises(ValueError, match="not valid JSON"):
            rewrite_linked_services(ls_dir, name_map={})


# ---------------------------------------------------------------------------
# process_encrypted_package — end-to-end orchestrator
# ---------------------------------------------------------------------------

class TestProcessEncryptedPackage:
    def test_end_to_end_dry_run(self, tmp_path: Path) -> None:
        dtsx = _write(tmp_path, "MyPkg.dtsx", _DTSX_WITH_DIRECT_PASSWORD)
        ls_dir = tmp_path / "linkedService"
        ls_dir.mkdir()
        ls_path = ls_dir / "LS_Src.json"
        original = json.dumps(_ls_with_kv_ref("CM_Source-password"))
        ls_path.write_text(original, encoding="utf-8")

        client = FakeSecretClientWritable()
        report = process_encrypted_package(
            unprotected_dtsx_path=dtsx,
            package_name="MyPkg",
            kv_url="https://x.vault.azure.net/",
            linked_service_dir=ls_dir,
            secret_client=client,
            dry_run=True,
        )

        assert isinstance(report, UploadReport)
        assert report.dry_run is True
        assert report.secrets_uploaded == ["MyPkg-CM-Source-password"]
        assert client.set_calls == []  # dry-run
        assert ls_path.read_text(encoding="utf-8") == original  # unchanged

        d = report.to_dict()
        assert d["package_name"] == "MyPkg"
        assert d["rewrite_count"] == 1

    def test_end_to_end_applied(self, tmp_path: Path) -> None:
        dtsx = _write(tmp_path, "MyPkg.dtsx", _DTSX_WITH_DIRECT_PASSWORD)
        ls_dir = tmp_path / "linkedService"
        ls_dir.mkdir()
        (ls_dir / "LS_Src.json").write_text(
            json.dumps(_ls_with_kv_ref("CM_Source-password")), encoding="utf-8"
        )

        client = FakeSecretClientWritable()
        report = process_encrypted_package(
            unprotected_dtsx_path=dtsx,
            package_name="MyPkg",
            kv_url="https://x.vault.azure.net/",
            linked_service_dir=ls_dir,
            secret_client=client,
        )

        assert report.dry_run is False
        assert report.secrets_uploaded == ["MyPkg-CM-Source-password"]
        assert client.vault["MyPkg-CM-Source-password"] == "SuperSecret123!"

        loaded = json.loads((ls_dir / "LS_Src.json").read_text(encoding="utf-8"))
        assert loaded["properties"]["typeProperties"]["password"]["secretName"] == "MyPkg-CM-Source-password"

    def test_end_to_end_no_secrets_no_failure(self, tmp_path: Path) -> None:
        dtsx = _write(tmp_path, "MyPkg.dtsx", _DTSX_NO_SECRETS)
        ls_dir = tmp_path / "linkedService"
        ls_dir.mkdir()
        client = FakeSecretClientWritable()

        report = process_encrypted_package(
            unprotected_dtsx_path=dtsx,
            package_name="MyPkg",
            kv_url="https://x.vault.azure.net/",
            linked_service_dir=ls_dir,
            secret_client=client,
        )
        assert report.secrets_uploaded == []
        assert report.rewrite_count == 0


# ---------------------------------------------------------------------------
# Default template constant
# ---------------------------------------------------------------------------

def test_default_template_constant_is_stable() -> None:
    """The MCP tool's default schema documents this exact value."""
    assert DEFAULT_SECRET_NAME_TEMPLATE == "{package}-{cm}-{kind}"
