"""Tests for the deeper deploy dry-run / pre-flight helper (P4-6)."""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pytest

from ssis_adf_agent.deployer.preflight import (
    HostTarget,
    KvSecretRef,
    PreflightReport,
    extract_dependencies,
    run_preflight,
)


# ---------------------------------------------------------------------------
# Fakes — no Azure / DNS in tests
# ---------------------------------------------------------------------------


@dataclass
class _FakeToken:
    token: str = "fake-jwt"
    expires_on: int = 1_900_000_000


class _FakeCredOk:
    def get_token(self, *_scopes: str) -> _FakeToken:
        return _FakeToken()


class _FakeCredFail:
    def get_token(self, *_scopes: str) -> _FakeToken:
        raise RuntimeError("ManagedIdentityCredential authentication unavailable")


class ResourceNotFoundError(Exception):
    pass


class _FakeSecretClient:
    def __init__(self, vault_url: str, *, contents: dict[str, dict[str, str]] | None = None) -> None:
        self.vault_url = vault_url
        self._contents = (contents or {}).get(vault_url, {})

    def get_secret(self, name: str) -> dict[str, str]:
        if name not in self._contents:
            raise ResourceNotFoundError(
                f"SecretNotFound: A secret with (name/id) {name} was not found"
            )
        if self._contents[name] == "__forbidden__":
            raise PermissionError("Forbidden: caller does not have 'get' permission")
        return {"name": name, "value": "<redacted>"}


def _make_secret_factory(contents: dict[str, dict[str, str]]):
    def _factory(vault_url: str) -> _FakeSecretClient:
        return _FakeSecretClient(vault_url, contents=contents)
    return _factory


def _make_dns(host_to_addrs: dict[str, list[str]], *, raise_for: set[str] | None = None):
    def _resolve(host: str) -> list[str]:
        if raise_for and host in raise_for:
            raise OSError("getaddrinfo failed")
        return host_to_addrs.get(host, [])
    return _resolve


# ---------------------------------------------------------------------------
# Fixtures — synthetic linked-service tree
# ---------------------------------------------------------------------------


VAULT_URL = "https://kv-ssis.vault.azure.net/"


def _ls(name: str, type_: str, type_props: dict) -> dict:
    return {
        "name": name,
        "properties": {"type": type_, "typeProperties": type_props},
    }


def _kv_ref(secret_name: str, kv_ls_name: str = "LS_KV") -> dict:
    return {
        "type": "AzureKeyVaultSecret",
        "store": {"referenceName": kv_ls_name, "type": "LinkedServiceReference"},
        "secretName": secret_name,
    }


@pytest.fixture
def artifacts_dir(tmp_path: Path) -> Path:
    ls_dir = tmp_path / "linkedService"
    ls_dir.mkdir()

    # KV linked service.
    (ls_dir / "LS_KV.json").write_text(json.dumps(
        _ls("LS_KV", "AzureKeyVault", {"baseUrl": VAULT_URL})
    ), encoding="utf-8")

    # SQL with KV-backed password.
    (ls_dir / "LS_SqlSrc.json").write_text(json.dumps(
        _ls("LS_SqlSrc", "AzureSqlDatabase", {
            "connectionString": "Server=tcp:sql-src.database.windows.net,1433;Database=src;",
            "password": _kv_ref("sql-src-password"),
        })
    ), encoding="utf-8")

    # SFTP with KV-backed password and a host property.
    (ls_dir / "LS_Sftp.json").write_text(json.dumps(
        _ls("LS_Sftp", "Sftp", {
            "host": "sftp.example.com",
            "port": 22,
            "password": _kv_ref("sftp-password"),
        })
    ), encoding="utf-8")

    # Linked service whose host is parameterized — should be skipped, not failed.
    (ls_dir / "LS_Param.json").write_text(json.dumps(
        _ls("LS_Param", "AzureSqlDatabase", {
            "connectionString": "Server=@{linkedService().serverName};Database=foo;",
        })
    ), encoding="utf-8")

    return tmp_path


# ---------------------------------------------------------------------------
# extract_dependencies
# ---------------------------------------------------------------------------


class TestExtractDependencies:
    def test_finds_kv_refs_and_resolves_baseurl(self, artifacts_dir: Path) -> None:
        deps = extract_dependencies(artifacts_dir)
        assert deps.kv_linked_services == {"LS_KV": VAULT_URL}
        secret_names = sorted(r.secret_name for r in deps.kv_secrets)
        assert secret_names == ["sftp-password", "sql-src-password"]
        assert all(r.vault_url == VAULT_URL for r in deps.kv_secrets)

    def test_finds_hosts_from_connstr_and_property(self, artifacts_dir: Path) -> None:
        deps = extract_dependencies(artifacts_dir)
        hosts = sorted({h.host for h in deps.hosts})
        # parameterized host stays in the list — the probe is what skips it.
        assert "sql-src.database.windows.net" in hosts
        assert "sftp.example.com" in hosts

    def test_does_not_emit_host_for_kv_linked_service(
        self, artifacts_dir: Path,
    ) -> None:
        # LS_KV.baseUrl is a host but should not appear as a host_dns target —
        # the KV probe covers it.
        deps = extract_dependencies(artifacts_dir)
        assert all("vault.azure.net" not in h.host for h in deps.hosts)

    def test_unresolved_kv_baseurl_yields_empty_vault_url(
        self, tmp_path: Path,
    ) -> None:
        ls_dir = tmp_path / "linkedService"
        ls_dir.mkdir()
        # KV reference with no matching AzureKeyVault linked service.
        (ls_dir / "LS_Sql.json").write_text(json.dumps(
            _ls("LS_Sql", "AzureSqlDatabase", {
                "password": _kv_ref("orphan", kv_ls_name="LS_NotPresent"),
            })
        ), encoding="utf-8")
        deps = extract_dependencies(tmp_path)
        assert len(deps.kv_secrets) == 1
        assert deps.kv_secrets[0].vault_url == ""

    def test_missing_linkedservice_dir_returns_empty(self, tmp_path: Path) -> None:
        deps = extract_dependencies(tmp_path)
        assert deps.kv_secrets == []
        assert deps.hosts == []
        assert deps.kv_linked_services == {}

    def test_invalid_json_files_are_skipped(self, tmp_path: Path) -> None:
        ls_dir = tmp_path / "linkedService"
        ls_dir.mkdir()
        (ls_dir / "broken.json").write_text("{not json", encoding="utf-8")
        (ls_dir / "LS_KV.json").write_text(json.dumps(
            _ls("LS_KV", "AzureKeyVault", {"baseUrl": VAULT_URL})
        ), encoding="utf-8")
        deps = extract_dependencies(tmp_path)
        assert deps.kv_linked_services == {"LS_KV": VAULT_URL}


# ---------------------------------------------------------------------------
# run_preflight orchestrator
# ---------------------------------------------------------------------------


class TestRunPreflight:
    def test_happy_path_all_pass(self, artifacts_dir: Path) -> None:
        secret_factory = _make_secret_factory({
            VAULT_URL: {"sql-src-password": "x", "sftp-password": "x"},
        })
        dns = _make_dns({
            "sql-src.database.windows.net": ["10.0.0.1"],
            "sftp.example.com": ["192.0.2.5", "2001:db8::1"],
        })
        report = run_preflight(
            artifacts_dir=artifacts_dir,
            subscription_id="sub", resource_group="rg", factory_name="fac",
            secret_client_factory=secret_factory,
            dns_resolver=dns,
            credential=_FakeCredOk(),
        )
        assert isinstance(report, PreflightReport)
        assert report.has_failures is False
        # 1 mi_token + 2 kv + 2 dns pass + 1 dns skip (parameterized host)
        statuses = [c.status for c in report.checks]
        assert statuses.count("pass") == 5
        assert statuses.count("skipped") == 1

    def test_missing_secret_classified_as_fail_with_actionable_message(
        self, artifacts_dir: Path,
    ) -> None:
        secret_factory = _make_secret_factory({
            VAULT_URL: {"sql-src-password": "x"},  # sftp-password missing
        })
        report = run_preflight(
            artifacts_dir=artifacts_dir,
            subscription_id="s", resource_group="r", factory_name="f",
            secret_client_factory=secret_factory,
            dns_resolver=_make_dns({"sftp.example.com": ["1.1.1.1"],
                                    "sql-src.database.windows.net": ["1.1.1.2"]}),
            credential=_FakeCredOk(),
        )
        kv_fails = [c for c in report.checks
                    if c.kind == "kv_secret" and c.status == "fail"]
        assert len(kv_fails) == 1
        assert "sftp-password" in kv_fails[0].target
        assert "upload_encrypted_secrets" in kv_fails[0].message

    def test_forbidden_classified_as_fail_with_rbac_hint(
        self, artifacts_dir: Path,
    ) -> None:
        secret_factory = _make_secret_factory({
            VAULT_URL: {
                "sql-src-password": "__forbidden__",
                "sftp-password": "x",
            },
        })
        report = run_preflight(
            artifacts_dir=artifacts_dir,
            subscription_id="s", resource_group="r", factory_name="f",
            secret_client_factory=secret_factory,
            dns_resolver=_make_dns({"sftp.example.com": ["1.1.1.1"],
                                    "sql-src.database.windows.net": ["1.1.1.2"]}),
            credential=_FakeCredOk(),
        )
        forbidden = [c for c in report.checks
                     if c.kind == "kv_secret" and "Forbidden" in c.message]
        assert forbidden
        assert "Key Vault Secrets User" in forbidden[0].message

    def test_unresolved_host_fails_with_actionable_message(
        self, artifacts_dir: Path,
    ) -> None:
        secret_factory = _make_secret_factory({
            VAULT_URL: {"sql-src-password": "x", "sftp-password": "x"},
        })
        # sftp host returns no addresses.
        dns = _make_dns({"sql-src.database.windows.net": ["1.1.1.1"]})
        report = run_preflight(
            artifacts_dir=artifacts_dir,
            subscription_id="s", resource_group="r", factory_name="f",
            secret_client_factory=secret_factory,
            dns_resolver=dns,
            credential=_FakeCredOk(),
        )
        dns_fails = [c for c in report.checks
                     if c.kind == "host_dns" and c.status == "fail"]
        assert len(dns_fails) == 1
        assert dns_fails[0].target == "sftp.example.com"
        assert "SHIR" in dns_fails[0].message or "private DNS" in dns_fails[0].message

    def test_dns_resolver_exception_warns_not_fails(
        self, artifacts_dir: Path,
    ) -> None:
        secret_factory = _make_secret_factory({
            VAULT_URL: {"sql-src-password": "x", "sftp-password": "x"},
        })
        dns = _make_dns({}, raise_for={"sftp.example.com",
                                       "sql-src.database.windows.net"})
        report = run_preflight(
            artifacts_dir=artifacts_dir,
            subscription_id="s", resource_group="r", factory_name="f",
            secret_client_factory=secret_factory,
            dns_resolver=dns,
            credential=_FakeCredOk(),
        )
        dns_warns = [c for c in report.checks
                     if c.kind == "host_dns" and c.status == "warn"]
        assert len(dns_warns) == 2

    def test_parameterized_host_is_skipped(self, artifacts_dir: Path) -> None:
        secret_factory = _make_secret_factory({
            VAULT_URL: {"sql-src-password": "x", "sftp-password": "x"},
        })
        report = run_preflight(
            artifacts_dir=artifacts_dir,
            subscription_id="s", resource_group="r", factory_name="f",
            secret_client_factory=secret_factory,
            dns_resolver=_make_dns({"sftp.example.com": ["1.1.1.1"],
                                    "sql-src.database.windows.net": ["1.1.1.2"]}),
            credential=_FakeCredOk(),
        )
        skipped = [c for c in report.checks if c.status == "skipped"]
        assert any("templated" in c.message for c in skipped)

    def test_mi_token_failure_is_fail(self, artifacts_dir: Path) -> None:
        report = run_preflight(
            artifacts_dir=artifacts_dir,
            subscription_id="s", resource_group="r", factory_name="f",
            secret_client_factory=_make_secret_factory({
                VAULT_URL: {"sql-src-password": "x", "sftp-password": "x"},
            }),
            dns_resolver=_make_dns({"sftp.example.com": ["1.1.1.1"],
                                    "sql-src.database.windows.net": ["1.1.1.2"]}),
            credential=_FakeCredFail(),
        )
        mi = [c for c in report.checks if c.kind == "mi_token"]
        assert len(mi) == 1
        assert mi[0].status == "fail"
        assert "az login" in mi[0].message

    def test_unresolved_kv_baseurl_fails_kv_probe_without_calling_client(
        self, tmp_path: Path,
    ) -> None:
        ls_dir = tmp_path / "linkedService"
        ls_dir.mkdir()
        (ls_dir / "LS_Sql.json").write_text(json.dumps(
            _ls("LS_Sql", "AzureSqlDatabase", {
                "password": _kv_ref("orphan", kv_ls_name="LS_NotPresent"),
            })
        ), encoding="utf-8")

        called: list[str] = []
        def factory(vault_url: str):
            called.append(vault_url)
            return _FakeSecretClient(vault_url)

        report = run_preflight(
            artifacts_dir=tmp_path,
            secret_client_factory=factory,
            dns_resolver=_make_dns({}),
            credential=_FakeCredOk(),
        )
        assert called == []  # short-circuit before constructing a client
        kv = [c for c in report.checks if c.kind == "kv_secret"]
        assert len(kv) == 1 and kv[0].status == "fail"
        assert "baseUrl" in kv[0].message

    def test_skip_flags_disable_probe_classes(self, artifacts_dir: Path) -> None:
        report = run_preflight(
            artifacts_dir=artifacts_dir,
            subscription_id="s", resource_group="r", factory_name="f",
            skip_kv=True, skip_dns=True, skip_mi_token=True,
        )
        assert report.checks == []
        assert report.has_failures is False

    def test_factory_resource_id_is_built(self, artifacts_dir: Path) -> None:
        report = run_preflight(
            artifacts_dir=artifacts_dir,
            subscription_id="00000000-0000-0000-0000-000000000001",
            resource_group="rg-x",
            factory_name="adf-x",
            skip_kv=True, skip_dns=True, skip_mi_token=True,
        )
        assert report.factory_resource_id.endswith("/factories/adf-x")
        assert "/resourceGroups/rg-x" in report.factory_resource_id

    def test_to_dict_structure(self, artifacts_dir: Path) -> None:
        report = run_preflight(
            artifacts_dir=artifacts_dir,
            secret_client_factory=_make_secret_factory({
                VAULT_URL: {"sql-src-password": "x", "sftp-password": "x"},
            }),
            dns_resolver=_make_dns({"sftp.example.com": ["1.1.1.1"],
                                    "sql-src.database.windows.net": ["1.1.1.2"]}),
            credential=_FakeCredOk(),
        )
        d = report.to_dict()
        assert set(d.keys()) >= {"artifacts_dir", "factory_resource_id",
                                 "counts", "has_failures", "checks"}
        # round-trips through JSON cleanly.
        json.dumps(d)

    def test_missing_artifacts_dir_raises(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            run_preflight(artifacts_dir=tmp_path / "nope")

    def test_de_duplicates_repeated_hosts(self, tmp_path: Path) -> None:
        ls_dir = tmp_path / "linkedService"
        ls_dir.mkdir()
        # Two linked services pointing at the same host.
        (ls_dir / "LS_A.json").write_text(json.dumps(
            _ls("LS_A", "AzureSqlDatabase", {
                "connectionString": "Server=shared.example.com;Database=a;",
            })
        ), encoding="utf-8")
        (ls_dir / "LS_B.json").write_text(json.dumps(
            _ls("LS_B", "AzureSqlDatabase", {
                "connectionString": "Server=shared.example.com;Database=b;",
            })
        ), encoding="utf-8")
        calls: list[str] = []
        def dns(host: str) -> list[str]:
            calls.append(host)
            return ["1.1.1.1"]
        report = run_preflight(
            artifacts_dir=tmp_path,
            dns_resolver=dns,
            skip_kv=True, skip_mi_token=True,
        )
        assert calls == ["shared.example.com"]  # de-duped
        host_checks = [c for c in report.checks if c.kind == "host_dns"]
        assert len(host_checks) == 1
