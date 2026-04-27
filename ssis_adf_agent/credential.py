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


# ── Subscription name → ID resolver ─────────────────────────────────────────

_UUID_RE = __import__("re").compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    __import__("re").IGNORECASE,
)


def resolve_subscription_id(value: str) -> str:
    """Accept a subscription ID (GUID) *or* display name and return the GUID.

    If *value* already looks like a UUID it is returned as-is (lower-cased).
    Otherwise the Azure SubscriptionClient is used to list accessible
    subscriptions and find one whose ``display_name`` matches
    (case-insensitive).  Raises ``ValueError`` if no match is found.
    """
    if _UUID_RE.match(value):
        return value.lower()

    from azure.mgmt.resource import SubscriptionClient

    credential = get_credential()
    client = SubscriptionClient(credential)

    matches: list[tuple[str, str]] = []
    for sub in client.subscriptions.list():
        if sub.display_name and sub.display_name.lower() == value.lower():
            matches.append((sub.subscription_id, sub.display_name))

    if not matches:
        raise ValueError(
            f"No accessible subscription found with display name '{value}'. "
            "Pass a subscription GUID instead, or check `az account list`."
        )
    if len(matches) > 1:
        ids = ", ".join(m[0] for m in matches)
        raise ValueError(
            f"Multiple subscriptions match '{value}': {ids}. "
            "Pass the subscription GUID to disambiguate."
        )

    resolved_id = matches[0][0]
    logger.info(
        "Resolved subscription name '%s' → %s", value, resolved_id
    )
    return resolved_id
