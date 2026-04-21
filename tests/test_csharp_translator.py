"""Tests for csharp_to_python.py safety guards."""
from __future__ import annotations

import pytest
from types import SimpleNamespace
from unittest.mock import patch, MagicMock

from ssis_adf_agent.translators.csharp_to_python import (
    CSharpToPythonTranslator,
    TranslationError,
)


def _make_task(**overrides):
    defaults = dict(
        name="TestTask",
        script_language="CSharp",
        read_only_variables=["Var1"],
        read_write_variables=["Out1"],
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


class TestTranslatorSafetyGuards:
    """Verify that the translator gracefully handles malformed LLM responses."""

    def _make_mock_response(self, *, choices=None, content=None, message_none=False):
        """Build a mock ChatCompletion-like response."""
        if choices is not None:
            return SimpleNamespace(choices=choices)
        if message_none:
            msg = None
        else:
            msg = SimpleNamespace(content=content)
        choice = SimpleNamespace(message=msg)
        return SimpleNamespace(choices=[choice])

    @patch.dict("os.environ", {
        "AZURE_OPENAI_ENDPOINT": "https://test.openai.azure.com/",
        "AZURE_OPENAI_API_KEY": "fake-key",
    })
    @patch("ssis_adf_agent.translators.csharp_to_python.AzureOpenAI", create=True)
    def test_empty_choices_raises(self, mock_cls):
        """response.choices == [] should raise TranslationError."""
        # The import inside translate() uses a local `from openai import AzureOpenAI`
        # so we patch the class *after* it's imported in the module.
        import ssis_adf_agent.translators.csharp_to_python as mod

        client = MagicMock()
        client.chat.completions.create.return_value = self._make_mock_response(choices=[])

        with patch.object(mod, "AzureOpenAI", create=True):
            # We need to patch at the import point inside translate()
            with patch.dict("sys.modules", {"openai": MagicMock()}):
                with patch("builtins.__import__", side_effect=_openai_import_factory(client)):
                    translator = CSharpToPythonTranslator()
                    with pytest.raises(TranslationError, match="no choices"):
                        translator.translate("int x = 1;", _make_task())

    @patch.dict("os.environ", {
        "AZURE_OPENAI_ENDPOINT": "https://test.openai.azure.com/",
        "AZURE_OPENAI_API_KEY": "fake-key",
    })
    def test_none_message_raises(self):
        """choices[0].message == None should raise TranslationError."""
        translator = CSharpToPythonTranslator()
        resp = self._make_mock_response(message_none=True)

        with _patch_openai_call(resp):
            with pytest.raises(TranslationError, match="no message"):
                translator.translate("int x = 1;", _make_task())

    @patch.dict("os.environ", {
        "AZURE_OPENAI_ENDPOINT": "https://test.openai.azure.com/",
        "AZURE_OPENAI_API_KEY": "fake-key",
    })
    def test_empty_content_raises(self):
        """choices[0].message.content == '' should raise TranslationError."""
        translator = CSharpToPythonTranslator()
        resp = self._make_mock_response(content="")

        with _patch_openai_call(resp):
            with pytest.raises(TranslationError, match="empty response"):
                translator.translate("int x = 1;", _make_task())

    @patch.dict("os.environ", {
        "AZURE_OPENAI_ENDPOINT": "https://test.openai.azure.com/",
        "AZURE_OPENAI_API_KEY": "fake-key",
    })
    def test_valid_content_returned(self):
        """Normal response should return stripped content."""
        translator = CSharpToPythonTranslator()
        resp = self._make_mock_response(content="x = 1\nreturn x")

        with _patch_openai_call(resp):
            result = translator.translate("int x = 1;", _make_task())
            assert result == "x = 1\nreturn x"

    @patch.dict("os.environ", {
        "AZURE_OPENAI_ENDPOINT": "https://test.openai.azure.com/",
        "AZURE_OPENAI_API_KEY": "fake-key",
    })
    def test_content_with_code_fences_stripped(self):
        """Markdown code fences should be removed."""
        translator = CSharpToPythonTranslator()
        resp = self._make_mock_response(content="```python\nx = 1\n```")

        with _patch_openai_call(resp):
            result = translator.translate("int x = 1;", _make_task())
            assert result == "x = 1"


class TestTranslatorConfig:
    def test_not_configured_without_env(self):
        with patch.dict("os.environ", {}, clear=True):
            t = CSharpToPythonTranslator()
            assert not t.is_configured()

    def test_configured_with_env(self):
        with patch.dict("os.environ", {
            "AZURE_OPENAI_ENDPOINT": "https://x.openai.azure.com/",
            "AZURE_OPENAI_API_KEY": "key",
        }):
            t = CSharpToPythonTranslator()
            assert t.is_configured()

    def test_configured_with_endpoint_only_for_entra_id(self):
        # Entra ID auth: only endpoint is required; credentials come from
        # DefaultAzureCredential at translate time.
        with patch.dict("os.environ", {
            "AZURE_OPENAI_ENDPOINT": "https://x.openai.azure.com/",
        }, clear=True):
            t = CSharpToPythonTranslator()
            assert t.is_configured()

    def test_translate_without_config_raises(self):
        with patch.dict("os.environ", {}, clear=True):
            t = CSharpToPythonTranslator()
            # Will hit either missing openai package or missing env vars
            with pytest.raises(TranslationError):
                t.translate("int x;", _make_task())


# ---------------------------------------------------------------------------
# Helpers for patching the openai import inside translate()
# ---------------------------------------------------------------------------

class _FakeOpenAIModule:
    """Minimal stand-in for the `openai` module."""

    def __init__(self, client_instance):
        self._client = client_instance
        self.APIError = Exception

    def AzureOpenAI(self, **kwargs):
        return self._client


def _openai_import_factory(client):
    """Return an __import__ side-effect that intercepts `openai` imports."""
    real_import = __builtins__.__import__ if hasattr(__builtins__, "__import__") else __import__

    def _import(name, *args, **kwargs):
        if name == "openai":
            return _FakeOpenAIModule(client)
        return real_import(name, *args, **kwargs)

    return _import


class _patch_openai_call:
    """Context manager that patches the local `openai` import in translate()."""

    def __init__(self, response):
        self._response = response

    def __enter__(self):
        client = MagicMock()
        client.chat.completions.create.return_value = self._response
        fake_module = MagicMock()
        fake_module.AzureOpenAI.return_value = client
        fake_module.APIError = Exception
        self._patcher = patch.dict("sys.modules", {"openai": fake_module})
        self._patcher.__enter__()
        return self

    def __exit__(self, *exc_info):
        self._patcher.__exit__(*exc_info)


class TestEntraIdAuth:
    """When AZURE_OPENAI_API_KEY is unset, the translator must use
    DefaultAzureCredential via azure_ad_token_provider, not api_key.
    """

    def test_entra_path_passes_token_provider(self):
        from types import SimpleNamespace
        captured: dict = {}

        client = MagicMock()
        msg = SimpleNamespace(content="x = 1")
        choice = SimpleNamespace(message=msg)
        client.chat.completions.create.return_value = SimpleNamespace(choices=[choice])

        fake_openai = MagicMock()
        def _factory(**kwargs):
            captured.update(kwargs)
            return client
        fake_openai.AzureOpenAI.side_effect = _factory
        fake_openai.APIError = Exception

        fake_identity = MagicMock()
        sentinel_provider = object()
        fake_identity.get_bearer_token_provider.return_value = sentinel_provider

        env = {"AZURE_OPENAI_ENDPOINT": "https://x.openai.azure.com/"}
        with patch.dict("os.environ", env, clear=True), \
             patch.dict("sys.modules", {"openai": fake_openai, "azure.identity": fake_identity}):
            translator = CSharpToPythonTranslator()
            result = translator.translate("int x = 1;", _make_task())

        assert result == "x = 1"
        assert "api_key" not in captured, "api_key must NOT be passed in Entra mode"
        assert captured.get("azure_ad_token_provider") is sentinel_provider
        # And confirm we asked for the Cognitive Services audience scope
        scope = fake_identity.get_bearer_token_provider.call_args.args[1]
        assert scope == "https://cognitiveservices.azure.com/.default"
