"""Tests for the worked Script Task port (P4-3).

Drives the ported Database_Access_Configuration Function as a regular Python
module (no Functions runtime needed) by faking ``func.HttpRequest`` with the
minimal interface the handler uses.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

# Ensure the case-study folder is importable as a stand-alone module.
CASE_STUDY_DIR = (
    Path(__file__).parent.parent
    / "docs"
    / "case-studies"
    / "script_task_port_database_access_configuration"
)
sys.path.insert(0, str(CASE_STUDY_DIR))

# Stub ``azure.functions`` if it isn't installed in the test env.  The real
# module is only needed at Functions-runtime; the port doesn't use any class
# features beyond ``HttpRequest`` (read-only) and ``HttpResponse`` (passthrough).
# IMPORTANT: do not replace ``sys.modules["azure"]`` — it's a namespace
# package providing ``azure.identity``, ``azure.mgmt.*`` etc., and clobbering
# it breaks dozens of unrelated tests.
try:
    import azure.functions as func  # type: ignore  # noqa: F401
except ImportError:  # pragma: no cover
    import types

    import azure  # real namespace package — do NOT replace

    func_mod = types.ModuleType("azure.functions")

    class HttpRequest:  # minimal shape used by tests
        def __init__(self, body: dict | None) -> None:
            self._body = body

        def get_json(self) -> dict:
            if self._body is None:
                raise ValueError("no body")
            return self._body

    class HttpResponse:
        def __init__(self, body: str, *, mimetype: str = "text/plain", status_code: int = 200) -> None:
            self.body = body
            self.mimetype = mimetype
            self.status_code = status_code

        def get_body(self) -> bytes:
            return self.body.encode("utf-8")

    func_mod.HttpRequest = HttpRequest
    func_mod.HttpResponse = HttpResponse
    sys.modules["azure.functions"] = func_mod
    azure.functions = func_mod  # type: ignore[attr-defined]

import azure.functions as func  # noqa: E402

# Import after stubbing so the case-study module finds azure.functions.
import importlib.util  # noqa: E402

_module_path = CASE_STUDY_DIR / "__init__.py"
_spec = importlib.util.spec_from_file_location(
    "database_access_configuration_port", _module_path
)
assert _spec and _spec.loader
_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_module)
main = _module.main


def _make_request(body: dict | None) -> func.HttpRequest:
    class _Req:
        def __init__(self, b: dict | None) -> None:
            self._b = b

        def get_json(self) -> dict:
            if self._b is None:
                raise ValueError("no body")
            return self._b

    return _Req(body)  # type: ignore[return-value]


def _decode(resp: func.HttpResponse) -> dict:
    return json.loads(resp.get_body().decode("utf-8"))


@pytest.fixture
def valid_body() -> dict:
    return {
        "database": "ADDS",
        "database_server": "sql.contoso.com",
        "db_user_id": "svc_adds",
        "environment": "PROD",
        "key_vault_url": "https://kv-adds-prod.vault.azure.net",
    }


def test_happy_path_returns_connection_settings(valid_body: dict) -> None:
    resp = main(_make_request(valid_body))
    assert resp.status_code == 200
    payload = _decode(resp)
    cs = payload["connection_settings"]
    assert cs["server"] == "sql.contoso.com"
    assert cs["database"] == "ADDS"
    assert cs["user_name"] == "svc_adds"
    assert cs["password_secret_uri"] == "https://kv-adds-prod.vault.azure.net/secrets/PW-LNI"
    assert payload["environment_resolved"] == "PROD"
    assert payload["package_run_time"]


def test_environment_dev_uses_pw_wads_secret(valid_body: dict) -> None:
    valid_body["environment"] = "DEV"
    resp = main(_make_request(valid_body))
    payload = _decode(resp)
    assert payload["connection_settings"]["password_secret_uri"].endswith("/PW-WADS")


def test_environment_test_uses_pw_wads_secret(valid_body: dict) -> None:
    valid_body["environment"] = "TEST"
    resp = main(_make_request(valid_body))
    payload = _decode(resp)
    assert payload["connection_settings"]["password_secret_uri"].endswith("/PW-WADS")


def test_environment_preprod_uses_pw_lni_secret(valid_body: dict) -> None:
    valid_body["environment"] = "PREPROD"
    resp = main(_make_request(valid_body))
    payload = _decode(resp)
    assert payload["connection_settings"]["password_secret_uri"].endswith("/PW-LNI")


def test_environment_unknown_returns_400(valid_body: dict) -> None:
    valid_body["environment"] = "STAGING"
    resp = main(_make_request(valid_body))
    assert resp.status_code == 400
    payload = _decode(resp)
    assert payload["code"] == "UNKNOWN_ENVIRONMENT"


def test_environment_override_extends_map(valid_body: dict) -> None:
    valid_body["environment"] = "STAGING"
    valid_body["environment_password_overrides"] = {"STAGING": "PW-STAGING"}
    resp = main(_make_request(valid_body))
    payload = _decode(resp)
    assert resp.status_code == 200
    assert payload["connection_settings"]["password_secret_uri"].endswith("/PW-STAGING")


def test_missing_required_fields_returns_400() -> None:
    resp = main(
        _make_request(
            {"database": "ADDS", "environment": "PROD", "key_vault_url": "https://x.vault.azure.net"}
        )
    )
    assert resp.status_code == 400
    payload = _decode(resp)
    assert payload["code"] == "MISSING_INPUT"
    assert "database_server" in payload["error"]
    assert "db_user_id" in payload["error"]


def test_missing_key_vault_url_returns_400(valid_body: dict, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("KEY_VAULT_URI", raising=False)
    monkeypatch.delenv("KEY_VAULT_URL", raising=False)
    valid_body.pop("key_vault_url")
    resp = main(_make_request(valid_body))
    assert resp.status_code == 400
    payload = _decode(resp)
    assert payload["code"] == "MISSING_KV_URL"


def test_key_vault_url_from_env(valid_body: dict, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KEY_VAULT_URI", "https://kv-from-env.vault.azure.net")
    valid_body.pop("key_vault_url")
    resp = main(_make_request(valid_body))
    payload = _decode(resp)
    assert resp.status_code == 200
    assert payload["connection_settings"]["password_secret_uri"].startswith(
        "https://kv-from-env.vault.azure.net/secrets/"
    )


def test_invalid_json_returns_400() -> None:
    class _Req:
        def get_json(self) -> dict:
            raise ValueError("bad json")

    resp = main(_Req())  # type: ignore[arg-type]
    assert resp.status_code == 400
    payload = _decode(resp)
    assert payload["code"] == "BAD_JSON"


def test_password_value_never_in_response(valid_body: dict) -> None:
    """Sanity: the function returns a Key Vault URI, never a password."""
    resp = main(_make_request(valid_body))
    body = resp.get_body().decode("utf-8").lower()
    for forbidden in ("password=", '"password":', "pw_lni", "pw_wads"):
        assert forbidden not in body
