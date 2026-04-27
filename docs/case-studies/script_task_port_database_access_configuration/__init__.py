"""Database Access Configuration — Azure Function port of an SSIS Script Task.

PORT NOTES
==========
The original VB Script Task (see ``original_script.vb``) ran inside SSIS and
mutated a **Connection Manager's** properties at run time:

    cm.Properties("UserName").SetValue(cm, strDBUserID)
    cm.Properties("Password").SetValue(cm, pPW)
    cm.Properties("ServerName").SetValue(cm, pDBServerName)
    cm.Properties("InitialCatalog").SetValue(cm, strDatabase)

ADF linked services are **static** at run time — there is no equivalent to
mutating a Connection Manager mid-pipeline.  The idiomatic ADF port has two
parts:

1. **This Function** resolves the four connection settings (server, db, user,
   password) from inputs + Key Vault and returns them in the JSON response.
2. The **ADF pipeline** uses those response values to bind a *parameterized*
   linked service (e.g. ``LS_DatabaseSource_Parameterized``) on the
   subsequent activities via the ``parameters`` block on the linked-service
   reference.

Other behavioral changes vs. the original:

* **Password resolution.**  The original VB pulled ``PW_LNI`` from a pipeline
  variable that was itself sourced from an SSIS Configuration.  In ADF the
  password comes from Azure Key Vault using managed identity — never from a
  pipeline variable in cleartext.  The ``Environment`` switch (DEV/TEST →
  ``PW_WADS`` vs. PREPROD/PROD → ``PW_LNI``) was commented out in the
  original; we surface it as the ``environment_password_overrides`` body
  field so the behavior is selectable per-environment without editing code.
* **Logging.**  ``Dts.Events.FireInformation`` becomes ``logging.info``;
  ``Dts.Events.FireError`` becomes ``logging.error`` + an HTTP 400 response
  (the pipeline can branch on this with an ``If Condition`` activity).
* **MsgBox debug shims.**  Removed entirely — ADF + Application Insights
  replace the SSIS debugging limitation the original was working around.
* **package_run_time.**  Returned in the response body so the pipeline can
  set a pipeline variable from it via a ``Set Variable`` activity.

INPUTS (JSON body)
==================
  database               (str, required)  — initial catalog
  database_server        (str, required)  — server hostname
  db_user_id             (str, required)  — SQL login name
  environment            (str, required)  — DEV / TEST / PREPROD / PROD / LOCAL
  key_vault_url          (str, optional)  — overrides KEY_VAULT_URL env var
  pw_secret_name_lni     (str, optional)  — defaults to ``PW-LNI``
  pw_secret_name_wads    (str, optional)  — defaults to ``PW-WADS``
  environment_password_overrides
                         (dict[str,str], optional) — explicit env→secret map.
                         Default: {DEV: PW-WADS, TEST: PW-WADS, others: PW-LNI}.

RESPONSE (JSON body)
====================
  package_run_time       (ISO-8601 string, UTC) — set as a pipeline variable
  connection_settings    (dict)                  — pass to a parameterized LS
    server               (str)
    database             (str)
    user_name            (str)
    password_secret_uri  (str)  — full Key Vault secret URI; the
                                  parameterized linked service should read
                                  this via @pipeline().parameters.X
  environment_resolved   (str)  — echoed back for diagnostics

ON ERROR the function returns HTTP 400 with::

    { "error": "<message>", "code": "<short_code>" }

The pipeline should use an ``If Condition`` on the Function activity output
to branch on success / failure.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import azure.functions as func

# Default mapping mirrors the LNI production rule: DEV / TEST share PW_WADS,
# everything else uses PW_LNI.  This was commented out in the original VB but
# documented in the team runbook; the port reinstates it as the default.
_DEFAULT_ENV_TO_SECRET: dict[str, str] = {
    "DEV": "PW-WADS",
    "TEST": "PW-WADS",
    "LOCAL": "PW-WADS",
    "PREPROD": "PW-LNI",
    "PROD": "PW-LNI",
}


def main(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP trigger entry point.  See module docstring for I/O contract."""
    logging.info("Database_Access_Configuration: invocation started")

    try:
        body: dict[str, Any] = req.get_json()
    except ValueError:
        return _error("Request body is not valid JSON.", "BAD_JSON", status=400)

    # --- Validate required inputs -----------------------------------------
    missing = [
        f for f in ("database", "database_server", "db_user_id", "environment")
        if not body.get(f)
    ]
    if missing:
        return _error(
            f"Missing required field(s): {', '.join(missing)}",
            "MISSING_INPUT",
            status=400,
        )

    database: str = body["database"]
    database_server: str = body["database_server"]
    db_user_id: str = body["db_user_id"]
    environment: str = body["environment"].strip().upper()

    # --- Resolve which secret to fetch for this environment ---------------
    overrides = body.get("environment_password_overrides") or {}
    env_to_secret = {**_DEFAULT_ENV_TO_SECRET, **{k.upper(): v for k, v in overrides.items()}}
    secret_name = env_to_secret.get(environment)
    if not secret_name:
        return _error(
            f"No password secret mapping for environment '{environment}'. "
            f"Pass environment_password_overrides to extend the map.",
            "UNKNOWN_ENVIRONMENT",
            status=400,
        )

    # Allow per-call override of the default name pulled from the env-map
    # (lets pipelines force a specific secret without editing the map).
    if environment == "DEV" and body.get("pw_secret_name_wads"):
        secret_name = body["pw_secret_name_wads"]
    elif environment in {"PREPROD", "PROD"} and body.get("pw_secret_name_lni"):
        secret_name = body["pw_secret_name_lni"]

    key_vault_url = (
        body.get("key_vault_url") or os.environ.get("KEY_VAULT_URI")
        or os.environ.get("KEY_VAULT_URL")
    )
    if not key_vault_url:
        return _error(
            "Key Vault URL not provided (set KEY_VAULT_URI app setting or "
            "pass key_vault_url in the request body).",
            "MISSING_KV_URL",
            status=400,
        )

    # Build the secret URI without versioning so the linked service always
    # picks up the latest version on each pipeline run.
    password_secret_uri = f"{key_vault_url.rstrip('/')}/secrets/{secret_name}"

    # --- Compose response --------------------------------------------------
    package_run_time = datetime.now(timezone.utc).isoformat()
    response_body = {
        "package_run_time": package_run_time,
        "environment_resolved": environment,
        "connection_settings": {
            "server": database_server,
            "database": database,
            "user_name": db_user_id,
            "password_secret_uri": password_secret_uri,
        },
    }
    logging.info(
        "Database_Access_Configuration: env=%s server=%s database=%s "
        "user=%s secret=%s (password value never logged)",
        environment,
        database_server,
        database,
        db_user_id,
        secret_name,
    )
    return func.HttpResponse(
        json.dumps(response_body),
        mimetype="application/json",
        status_code=200,
    )


def _error(message: str, code: str, *, status: int) -> func.HttpResponse:
    logging.error("Database_Access_Configuration error: %s (%s)", message, code)
    return func.HttpResponse(
        json.dumps({"error": message, "code": code}),
        mimetype="application/json",
        status_code=status,
    )
