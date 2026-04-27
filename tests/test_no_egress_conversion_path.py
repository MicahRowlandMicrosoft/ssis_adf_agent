"""P5-8: the conversion path makes zero outbound network connections.

This test is the regression guard for the "What the agent talks to" audit
in SECURITY.md. The conversion path (scan -> analyze -> propose -> convert
-> validate without sub/RG) must complete with **all** socket / HTTP
machinery monkey-patched to raise on any connection attempt.

If a new outbound call is added to any module touched by the conversion
path, this test fails and forces an explicit code-review decision.
"""
from __future__ import annotations

import asyncio
import json
import socket
from pathlib import Path

import pytest


_DTSX_TEMPLATE = """<?xml version="1.0"?>
<DTS:Executable xmlns:DTS="www.microsoft.com/SqlServer/Dts"
    DTS:ExecutableType="Microsoft.Package"
    DTS:DTSID="{11111111-1111-1111-1111-1111111111{n}}"
    DTS:ObjectName="Pkg{n}">
  <DTS:Executables>
    <DTS:Executable DTS:refId="Package\\T1"
        DTS:ExecutableType="Microsoft.FileSystemTask"
        DTS:DTSID="{AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAA{n}}"
        DTS:ObjectName="T1" />
  </DTS:Executables>
</DTS:Executable>
"""


class _NoEgressViolation(AssertionError):
    """Raised when the conversion path attempts an outbound connection."""


def _seed_packages(tmp_path: Path, count: int = 2) -> Path:
    src = tmp_path / "src"
    src.mkdir()
    for i in range(1, count + 1):
        (src / f"pkg{i}.dtsx").write_text(
            _DTSX_TEMPLATE.replace("{n}", str(i)), encoding="utf-8",
        )
    return src


@pytest.fixture
def block_all_egress(monkeypatch):
    """Monkey-patch the socket layer to forbid any outbound connection."""
    real_socket = socket.socket
    real_create_connection = socket.create_connection

    def _is_local(addr):
        if not addr:
            return False
        host = addr[0] if isinstance(addr, tuple) else None
        return host in ("127.0.0.1", "::1", "localhost", "0.0.0.0", "")

    def _trip(*args, **kwargs):
        # Allow loopback (asyncio's self-pipe on Windows uses socketpair via
        # localhost). Anything else is a real outbound call and is rejected.
        target = args[0] if args else kwargs.get("address")
        if _is_local(target):
            return real_create_connection(*args, **kwargs)
        raise _NoEgressViolation(
            "Conversion path attempted an outbound network connection to "
            f"{target!r}. If this is intentional, update SECURITY.md 'What the "
            "agent talks to' and amend tests/test_no_egress_conversion_path.py."
        )

    monkeypatch.setattr(socket, "create_connection", _trip)

    class _BlockingSocket(real_socket):  # type: ignore[misc]
        def connect(self, address, *args, **kwargs):  # type: ignore[override]
            if _is_local(address):
                return super().connect(address, *args, **kwargs)
            raise _NoEgressViolation(
                f"Conversion path attempted socket.connect({address!r})."
            )

        def connect_ex(self, address, *args, **kwargs):  # type: ignore[override]
            if _is_local(address):
                return super().connect_ex(address, *args, **kwargs)
            raise _NoEgressViolation(
                f"Conversion path attempted socket.connect_ex({address!r})."
            )

    monkeypatch.setattr(socket, "socket", _BlockingSocket)

    # Belt-and-braces: also fail any httpx call (even though the conversion
    # path doesn't import httpx).
    try:
        import httpx  # noqa: F401

        def _httpx_trip(*args, **kwargs):
            _trip()

        monkeypatch.setattr(
            "httpx.HTTPTransport.handle_request", _httpx_trip, raising=False
        )
    except ImportError:
        pass

    yield


def test_convert_estate_makes_no_outbound_calls(
    tmp_path: Path, block_all_egress, monkeypatch
) -> None:
    """convert_estate end-to-end must not open any network connection."""
    # Hard-disable LLM translation regardless of operator env.
    monkeypatch.setenv("SSIS_ADF_NO_LLM", "1")
    monkeypatch.delenv("AZURE_OPENAI_ENDPOINT", raising=False)
    monkeypatch.delenv("AZURE_OPENAI_API_KEY", raising=False)

    from ssis_adf_agent.mcp_server import _convert_estate

    src = _seed_packages(tmp_path, count=2)
    out = tmp_path / "out"

    result = asyncio.run(_convert_estate({
        "source_path": str(src),
        "output_dir": str(out),
    }))
    payload = json.loads(result[0].text)

    # If any socket call had been attempted, _NoEgressViolation would have been
    # raised inside _convert_estate and we would never get here.
    assert payload["succeeded_count"] == 2
    assert payload["failed_count"] == 0


def test_bulk_analyze_makes_no_outbound_calls(
    tmp_path: Path, block_all_egress
) -> None:
    """bulk_analyze must also be fully offline."""
    from ssis_adf_agent.mcp_server import _bulk_analyze

    src = _seed_packages(tmp_path, count=3)

    result = asyncio.run(_bulk_analyze({"source_path": str(src)}))
    payload = json.loads(result[0].text)

    assert payload["package_count"] == 3
    assert payload["failure_count"] == 0
