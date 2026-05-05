# Script Task Translation Modes

SSIS Script Tasks contain C# / VB code that must be ported to Python before
the generated Azure Function stubs do anything useful. The converter offers
**three translation modes** so you can pick the right trade-off between
determinism, AI assistance, and air-gap compliance.

| Mode | Who translates | Default | When to use |
|------|----------------|---------|-------------|
| `none` | Nobody — deterministic stub only | ✅ yes | Air-gapped / regulated tenants; CI; review-only conversions |
| `host` | The calling agent (Copilot CLI, Copilot Chat, Claude Code, etc.) using its own LLM session | — | Interactive use; you're already in an AI agent session and want translation to happen there (no extra AOAI cost or call) |
| `aoai` | In-process Azure OpenAI call from the converter | — | Headless pipelines (CI, batch convert) where no host agent is present, you have AOAI provisioned, and you want translation to land on disk during `convert_ssis_package` |

> ⚠️  All three modes still produce the **same deterministic stub on disk**.
> Translation, when it happens, only replaces the body of a clearly-marked
> region inside that stub (see [Region markers](#region-markers)).

---

## Selecting a mode

### From the MCP tool / SDK

```jsonc
// convert_ssis_package or convert_estate
{
  "package_path": "C:/path/to/Pkg.dtsx",
  "output_dir":   "C:/out/Pkg",
  "translation_mode": "host"   // "none" | "host" | "aoai"
}
```

### From the CLI

```bash
ssis-adf-agent convert \
  --package-path C:/path/to/Pkg.dtsx \
  --output-dir   C:/out/Pkg \
  --translation-mode host
```

### Precedence (highest wins)

1. `no_llm: true` arg **or** `SSIS_ADF_NO_LLM` env var → forces `none`.
2. Explicit `translation_mode` argument.
3. Legacy `llm_translate: true` → maps to `aoai` (deprecated; emits a warning).
4. Default → `none`.

When two signals conflict (e.g. `llm_translate=true` + `translation_mode="host"`)
the resolver returns a note that surfaces in the MCP response under
`translation.notes` and as a Python warning.

---

## Region markers

The deterministic stub at `<output_dir>/stubs/<FunctionName>/__init__.py`
wraps its placeholder logic in a stable, machine-recognisable region:

```python
# BEGIN SSIS_SCRIPT_TRANSLATION: ScriptTaskName
# Replace the lines between these two markers with the real translation.
# Imports, request parsing, response shape, and error handling are
# *outside* the region and must be preserved as-is.
raise NotImplementedError(
    "Translate the original SSIS Script Task body here. See translation_manifest.json."
)
# END SSIS_SCRIPT_TRANSLATION: ScriptTaskName
```

The host agent (or any tooling) **must replace only the bytes between the
two marker lines**, leaving:

* All imports above
* The HTTP entry-point signature
* Request parsing / variable rehydration
* Response serialisation / status codes
* Error handling

…unchanged. This guarantees the stub keeps its function-app contract no
matter how the body is rewritten.

The marker strings are exported from
`ssis_adf_agent.translators.translation_manifest` as
`REGION_BEGIN_MARKER` / `REGION_END_MARKER`. **Do not change them** — they
are part of the stable contract.

---

## Translation manifest

Whenever the effective mode is **not** `none`, the converter writes:

```
<output_dir>/stubs/translation_manifest.json
```

…and `convert_estate` aggregates the per-package manifests into:

```
<output_dir>/translation_index.json
```

The estate-level index records *paths and counts only* — it never
duplicates source code.

### Per-entry schema

| Field | Type | Description |
|-------|------|-------------|
| `task_name` | string | SSIS Script Task name |
| `function_name` | string | Sanitised Function App handler name |
| `stub_path` | string | Absolute path to the stub `__init__.py` |
| `function_json_path` | string | Path to the sibling `function.json` |
| `language` | `csharp` \| `vb` \| `unknown` | Source language detected |
| `source_code` | string \| null | Original code (only when `mode=host`; omitted for `aoai`) |
| `read_variables` | string[] | SSIS variables read by the task |
| `read_write_variables` | string[] | SSIS variables written by the task |
| `region_begin_marker` / `region_end_marker` | string | Stable marker strings for the agent to grep |
| `status` | enum | See below |
| `error` | string \| null | Failure message when `status = aoai_failed_pending_manual` |
| `created_at` | ISO 8601 | Manifest write time |

### Status lifecycle

| Status | Meaning |
|--------|---------|
| `pending_host_translation` | `mode=host`. Stub is in place, marker region needs to be replaced by the calling agent. |
| `aoai_translated_needs_review` | `mode=aoai`. AOAI succeeded; the marker region already contains Python. **Human review still recommended** — LLM output is not authoritative. |
| `aoai_failed_pending_manual` | `mode=aoai`. AOAI threw or returned unusable output; falls back to deterministic stub; manual port required. `error` field populated. |
| `skipped_no_source` | Original `SourceCode` was empty (encrypted package or binary-only). |
| `deterministic_stub_only` | Rare — emitted when manifest is forced on but mode resolved to `none`. |

---

## Recommended workflow per mode

### `host` (interactive in-agent)

1. Run `convert_ssis_package` (or `convert_estate`) with `translation_mode: "host"`.
2. Read the response's `translation.next_steps` and `translation.manifest_path`.
3. Open the manifest, iterate entries with `status = pending_host_translation`.
4. For each entry, open `stub_path`, locate the `BEGIN/END SSIS_SCRIPT_TRANSLATION`
   region, and replace **only** that region with translated Python.
5. Update the entry status (or simply re-run `convert_ssis_package` later — the
   converter is idempotent and will overwrite the manifest).

### `aoai` (headless / CI)

1. Provision Azure OpenAI; set `AZURE_OPENAI_ENDPOINT` and `AZURE_OPENAI_API_KEY`.
2. Run conversion with `translation_mode: "aoai"`.
3. Inspect the manifest after the run:
   * `aoai_translated_needs_review` → schedule a human review pass
   * `aoai_failed_pending_manual` → port by hand using the `error` hint
4. Translation lands on disk during the converter run; no second pass needed
   for happy-path entries.

### `none` (deterministic / regulated)

* The stub raises `NotImplementedError` from inside the region.
* No manifest is written.
* `SSIS_ADF_NO_LLM=1` makes this enforceable in policy-locked environments.

---

## Backwards compatibility

* The deprecated `llm_translate=true` argument still works and is silently
  promoted to `translation_mode="aoai"`.
* `SSIS_ADF_NO_LLM` (env) and `no_llm=true` (arg) continue to be a hard
  off-switch that overrides everything else.
* Existing stubs generated before this feature are unaffected — they simply
  lack the region markers and must be ported manually.
