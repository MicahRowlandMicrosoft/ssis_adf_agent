# Worked Script Task port — `Database Access Configuration`

This is a **complete, end-to-end** port of one real SSIS Script Task to an
Azure Function, anchoring the per-Script-Task hour estimates in
[effort-methodology.md](../../conversion/effort-methodology.md) to a concrete example
buyers can audit before signing off on a migration plan.

| Field | Value |
|---|---|
| Source package | `ADDS-MIPS-TC.dtsx` (LNI estate, sanitized) |
| Script Task name | `Database Access Configuration` |
| Source language | VB (`.vb`) |
| Source LOC (signal lines, excl. assembly attrs / regions) | **80** |
| Classifier tier (per `analyzers/script_classifier.py`) | **`moderate`** |
| Predicted hours from `_script_porting_hours` | `max(2.0, 80 / 25.0) = 3.2h` |
| **Actual hours captured** (this port) | **3.5h** (see breakdown below) |

Files:

- [`original_script.vb`](original_script.vb) — the original VB ScriptMain
  (extracted verbatim from the SSIS package; sanitized only by removing
  internal cluster/host names).
- [`__init__.py`](__init__.py) — the production-ready Python port. Drop into
  any v2 Python Functions app under `Database_Access_Configuration/`.
- [`function.json`](function.json) — HTTP-trigger binding.
- [`requirements.txt`](requirements.txt) — Python dependencies.

Tests: [`tests/test_script_task_port_database_access_configuration.py`](../../../tests/test_script_task_port_database_access_configuration.py)
(11 tests, runs without the Functions runtime).

---

## What the original SSIS task did

1. Stamped `User::package_run_time = DateTime.Now.ToString`.
2. Read three pipeline variables (`Environment`, `DatabaseServer`,
   `Database`, `DBUserID`) and validated the first two were non-empty.
3. Selected a password from one of two pipeline variables (`PW_LNI` /
   `PW_WADS`) based on `Environment` (the case statement was checked in
   commented-out — only `PW_LNI` was active in the deployed package).
4. **Mutated** the `Database_Source_Connection_Manager`'s `UserName`,
   `Password`, `ServerName`, and `InitialCatalog` properties at run time so
   downstream Data Flow Tasks pointed at the right DB with the right creds.
5. Dropped a debug `MsgBox` if a hard-coded `DebugOn` flag was true.

## Mapping decisions

| SSIS construct | ADF/Function equivalent | Why |
|---|---|---|
| `Dts.Variables("X").Value` (read) | `req.get_json()["x"]` | Pipeline `Web Activity` body carries the inputs explicitly — better audit trail than implicit variable propagation. |
| `Dts.Variables("package_run_time").Value = DateTime.Now.ToString` | Returned in JSON response; pipeline sets a variable from `@activity('DatabaseAccessConfiguration').output.package_run_time` | ADF Functions cannot mutate pipeline variables; the pipeline pulls them from the response. |
| `Dts.Events.FireInformation` | `logging.info(...)` | Routed to Application Insights via Function app config. |
| `Dts.Events.FireError` + `Dts.TaskResult = Failure` | HTTP 400 + `{ "code": "...", "error": "..." }` | Pipeline branches with `If Condition` on `@activity(...).output.code`. |
| `cm.Properties("Password").SetValue(cm, pw)` (mutating Connection Manager) | Function returns `connection_settings`; pipeline binds them to a **parameterized linked service** | Linked services are immutable at run time; parameterizing them is the supported alternative. |
| `Dts.Variables("PW_LNI").Value` (cleartext password from pipeline variable) | Function builds a Key Vault `password_secret_uri`; the linked service reads via Managed Identity | Pipeline variables are visible in run history — secrets must come from KV. |
| Commented-out `Select Case pEnvironment` | `_DEFAULT_ENV_TO_SECRET` map + `environment_password_overrides` body field | Reinstates the original team intent without re-editing code. |
| `If DebugOn Then MsgBox(...)` | _Removed_ | App Insights traces replace the SSIS-debugger workaround. |

## How the pipeline consumes the output

1. `Web Activity` (or `Azure Function Activity`) → calls this function with
   the four required inputs from pipeline parameters.
2. `Set Variable` activities pull values out of the response:
   - `pipeline().variables.package_run_time = @activity('DAC').output.package_run_time`
   - `pipeline().variables.kv_pw_uri        = @activity('DAC').output.connection_settings.password_secret_uri`
3. Downstream `Copy` / `Execute Data Flow` activities reference a
   parameterized linked service `LS_DatabaseSource_Parameterized` whose
   parameters are bound to:
   - `server`     → `@activity('DAC').output.connection_settings.server`
   - `database`   → `@activity('DAC').output.connection_settings.database`
   - `userName`   → `@activity('DAC').output.connection_settings.user_name`
   - `password`   → `@Microsoft.KeyVault(SecretUri=...)` reference, where
     the URI flows from `connection_settings.password_secret_uri`. (For
     full flexibility, define the linked service to accept the URI as a
     parameter and dereference inside via the AKV linked service.)

A copy of the pipeline JSON wiring is left as an exercise for the
deploying team — the Function's contract is the auditable boundary.

## Hours breakdown (actual)

These are the real-world hours a senior engineer should budget for an
equivalent port. They are slightly higher than the heuristic prediction
(`3.2h` vs `3.5h`) because the heuristic doesn't bill for cross-cutting
decisions like _"replace pipeline-variable passwords with Key Vault"_,
which apply once but are paid down across many Script Tasks.

| Phase | Hours | Notes |
|---|---|---|
| Read original VB & confirm intent (incl. the commented case statement) | 0.5 | Includes confirming with the source-system owner that `PW_LNI` was the only branch in production. |
| Design mapping (linked-service mutation → parameterized LS + KV) | 0.75 | Reusable across the other 5 LNI Script Tasks; budgeted full cost once. |
| Write `__init__.py` + `function.json` + `requirements.txt` | 0.75 | |
| Write `tests/test_script_task_port_database_access_configuration.py` | 0.75 | 11 tests covering all branches, no Azure runtime needed. |
| Wire pipeline (parameterize LS, bind activities, set variables) | 0.5 | One-time per pipeline; not in the per-task heuristic. |
| Code review + buyer walk-through | 0.25 | |
| **Total** | **3.5h** | vs predicted **3.2h** |

## Gotchas worth flagging to buyers

1. **You cannot mutate a linked service at run time.** Any Script Task that
   resets a Connection Manager's properties **must** become a Function +
   parameterized linked service. The agent flags this in the script
   classifier output.
2. **Cleartext passwords in pipeline variables don't survive the audit.**
   This is the right time to push them into Key Vault; the Function makes
   that the natural shape.
3. **The `DebugOn = False` shim and `MsgBox` calls disappear.** Any
   on-call runbook that referenced "look for the popup" needs a rewrite to
   reference Application Insights traces instead.
4. **`DateTime.Now` is local time on the SSIS host.** The Function uses
   `datetime.now(timezone.utc)`; downstream consumers that compared the
   timestamp to a local-time field need a one-line fix.
5. **The commented-out `Select Case` was production intent**, not dead
   code. Treat every commented block in an SSIS script as a question for
   the source-system owner before deleting.

## Reuse: the other 5 LNI Script Tasks

The same shape applies to the remaining stubs in the LNI estate
(`Database_Access_Configuration` × 3 packages, `Job_Schedule_Message_Initialize`
× 3 packages). Once this port is reviewed:

- The two unique scripts can be promoted to a shared `_lib/` directory in
  the Function app.
- Each subsequent port should land in **~1.5h** (half the time of this
  reference port) since the design decisions are now amortized.
