# Headless CI Recipes (M3)

The agent ships a CLI that mirrors every MCP tool. Use it from any CI system
without an MCP host.

## Install

```bash
pip install -e path/to/ssis_adf_agent
# Sanity check
python -m ssis_adf_agent --help
```

The console-script `ssis-adf-agent` continues to launch the **MCP stdio
server** for back-compat. Headless callers should always use
`python -m ssis_adf_agent <subcommand>` instead.

## Subcommands

| Command            | Wraps MCP tool          |
|--------------------|-------------------------|
| `analyze`          | `analyze_ssis_package`  |
| `convert`          | `convert_ssis_package`  |
| `validate`         | `validate_adf_artifacts`|
| `deploy`           | `deploy_to_adf`         |
| `activate-triggers`| `activate_triggers`     |

Each prints the tool's JSON result to stdout and exits 0 on success, 1 if
the JSON body indicates issues, 2 on a Python exception.

---

## GitHub Actions — convert-and-validate on every PR

```yaml
# .github/workflows/ssis-convert.yml
name: SSIS → ADF conversion check

on:
  pull_request:
    paths:
      - "ssis/**"

jobs:
  convert:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.12" }

      - name: Install agent
        run: pip install -e ./tools/ssis_adf_agent

      - name: Convert all packages
        run: |
          mkdir -p out
          for pkg in ssis/*.dtsx; do
            python -m ssis_adf_agent convert "$pkg" "out/$(basename "${pkg%.dtsx}")" \
              | tee "out/$(basename "${pkg%.dtsx}").convert.json"
          done

      - name: Validate generated ADF JSON
        run: |
          for d in out/*/; do
            python -m ssis_adf_agent validate "$d"
          done

      - uses: actions/upload-artifact@v4
        with:
          name: adf-artifacts
          path: out/
```

Key points:

* `convert` exits non-zero if the agent flags a hard error; warnings are
  surfaced in stdout but do not fail the job. Add `| jq` if you want to
  fail on a specific warning class.
* `validate` exits 1 when any artifact has a structural issue, so the job
  fails before anything reaches Azure.

---

## Azure DevOps — convert + dry-run deploy on `main`

```yaml
# azure-pipelines.yml
trigger:
  branches: { include: [main] }

pool: { vmImage: ubuntu-latest }

variables:
  - group: adf-secrets       # provides AZURE_*

steps:
  - task: UsePythonVersion@0
    inputs: { versionSpec: "3.12" }

  - script: pip install -e tools/ssis_adf_agent
    displayName: Install agent

  - script: |
      python -m ssis_adf_agent convert \
        ssis/MyPackage.dtsx out/MyPackage \
        --auth-type SystemAssignedManagedIdentity
    displayName: Convert

  - script: python -m ssis_adf_agent validate out/MyPackage
    displayName: Validate

  - script: |
      az login --service-principal \
        -u $(AZURE_CLIENT_ID) -p $(AZURE_CLIENT_SECRET) \
        --tenant $(AZURE_TENANT_ID)
    displayName: Login to Azure

  - script: |
      python -m ssis_adf_agent deploy out/MyPackage \
        --subscription-id $(AZURE_SUBSCRIPTION_ID) \
        --resource-group  $(ADF_RG) \
        --factory-name    $(ADF_NAME) \
        --skip-if-exists  \
        --dry-run
    displayName: Deploy (dry-run)
```

Recommended pattern: **always** dry-run first in CI, then trigger an
approval gate for the real deploy. Pair with `--skip-if-exists` once the
factory has hand-edited artifacts (see [BACKLOG.md](BACKLOG.md) H8).

---

## Exit codes

| Code | Meaning |
|------|---------|
| 0    | Success |
| 1    | Tool ran but flagged issues / failed deploys (non-zero `failed` count, or `status == "issues_found"`). |
| 2    | Python-level exception (bad arguments, unreadable file, missing credentials). The `cli_error` field of the printed JSON has the full message. |

---

## Where to file CLI feedback

If you find yourself adding a wrapper script around `python -m ssis_adf_agent`
to compensate for a missing flag, file an issue. The CLI is intended to be
strict-superset of every MCP tool's surface, no more and no less.
