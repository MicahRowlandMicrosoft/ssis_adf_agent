"""Shared Azure credential factory.

Centralises credential creation so every module in the agent uses the same
strategy.  By default uses ``AzureCliCredential`` which is fast and reliable
in local-dev / MCP-server contexts.  Falls back to ``DefaultAzureCredential``
(with slow providers excluded) when the CLI is not available.

Set the environment variable ``SSIS_ADF_CREDENTIAL=default`` to force
``DefaultAzureCredential`` (e.g. in CI/CD with service-principal env vars).
"""
from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)


def get_credential():
    """Return an Azure credential suitable for the current environment.

    Selection order:
    1. ``SSIS_ADF_CREDENTIAL=default`` → ``DefaultAzureCredential`` (slow
       providers excluded).
    2. ``AZURE_CLIENT_ID`` present → ``DefaultAzureCredential`` (auto-detects
       service-principal env vars for CI/CD).
    3. Otherwise → ``AzureCliCredential`` (fast, no timeout).
    """
    from azure.identity import AzureCliCredential, DefaultAzureCredential

    strategy = os.environ.get("SSIS_ADF_CREDENTIAL", "").lower()

    if strategy == "default" or os.environ.get("AZURE_CLIENT_ID"):
        reason = (
            "SSIS_ADF_CREDENTIAL=default"
            if strategy == "default"
            else "AZURE_CLIENT_ID detected"
        )
        logger.debug("Using DefaultAzureCredential (%s)", reason)
        return DefaultAzureCredential(
            exclude_managed_identity_credential=True,
            exclude_shared_token_cache_credential=True,
        )

    logger.debug("Using AzureCliCredential")
    return AzureCliCredential()
