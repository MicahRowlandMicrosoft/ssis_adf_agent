"""Tests for MCP server path traversal protection."""
from __future__ import annotations

import os
import pytest
from pathlib import Path
from unittest.mock import patch

from ssis_adf_agent.path_safety import safe_resolve, ALLOWED_ROOT


# ---------------------------------------------------------------------------
# safe_resolve — basic behaviour
# ---------------------------------------------------------------------------

class TestSafeResolveBasic:
    def test_resolves_absolute_path(self, tmp_path):
        f = tmp_path / "pkg.dtsx"
        f.touch()
        result = safe_resolve(str(f), must_exist=True)
        assert result == f.resolve()

    def test_must_exist_raises_when_missing(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="not found"):
            safe_resolve(str(tmp_path / "nope.dtsx"), must_exist=True, label="pkg")

    def test_must_exist_false_allows_missing(self, tmp_path):
        p = safe_resolve(str(tmp_path / "future_dir"), must_exist=False)
        assert p == (tmp_path / "future_dir").resolve()


# ---------------------------------------------------------------------------
# safe_resolve — traversal rejection
# ---------------------------------------------------------------------------

class TestTraversalRejection:
    def test_rejects_null_byte(self):
        with pytest.raises(ValueError, match="Null byte"):
            safe_resolve("/tmp/\x00evil", label="test")

    def test_rejects_dotdot_in_parts(self, tmp_path):
        with pytest.raises(ValueError, match="Path traversal"):
            safe_resolve(str(tmp_path / ".." / "etc" / "passwd"), label="test")

    def test_rejects_leading_dotdot(self):
        with pytest.raises(ValueError, match="Path traversal"):
            safe_resolve("../../etc/shadow", label="test")

    def test_accepts_dotdot_free_path(self, tmp_path):
        f = tmp_path / "safe.dtsx"
        f.touch()
        assert safe_resolve(str(f), must_exist=True) == f.resolve()


# ---------------------------------------------------------------------------
# safe_resolve — ALLOWED_ROOT enforcement
# ---------------------------------------------------------------------------

class TestAllowedRoot:
    def test_allowed_root_accepts_child(self, tmp_path):
        child = tmp_path / "sub" / "pkg.dtsx"
        child.parent.mkdir(parents=True, exist_ok=True)
        child.touch()
        with patch("ssis_adf_agent.path_safety.ALLOWED_ROOT", tmp_path.resolve()):
            result = safe_resolve(str(child), must_exist=True)
            assert result == child.resolve()

    def test_allowed_root_accepts_root_itself(self, tmp_path):
        with patch("ssis_adf_agent.path_safety.ALLOWED_ROOT", tmp_path.resolve()):
            result = safe_resolve(str(tmp_path))
            assert result == tmp_path.resolve()

    def test_allowed_root_rejects_outside(self, tmp_path):
        outside = tmp_path.parent / "other"
        with patch("ssis_adf_agent.path_safety.ALLOWED_ROOT", tmp_path.resolve()):
            with pytest.raises(ValueError, match="outside the allowed root"):
                safe_resolve(str(outside), label="test")
