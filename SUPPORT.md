# Support

> **Community-supported, best-effort.** This is an open-source project,
> not an officially supported Microsoft product. There is no SLA, no
> on-call rotation, and no guaranteed response time. Use the issue
> tracker for bugs, feature requests, and questions.

---

## How to get help

| What | Where | Response-time |
|---|---|---|
| Bugs, feature requests, questions | [GitHub Issues](https://github.com/MicahRowlandMicrosoft/ssis_adf_agent/issues) | Best-effort. Typically 2–5 business days for a first response; longer for non-trivial requests. No SLA. |
| Security vulnerabilities | See [SECURITY.md](SECURITY.md) for the private disclosure path. **Do not file public issues** for vulnerabilities. | Per SECURITY.md (target: 5 business days for triage). |

What you should expect:

- A maintainer will read your issue.
- Reproducible bugs with a sanitized `.dtsx` fixture get prioritized.
- Pull requests are welcome; see the contribution notes in the README.
- Critical regressions in a tagged release will get a patch release;
  non-critical fixes ship in the next minor.

What you should **not** expect:

- A response within hours.
- Phone or chat support.
- Custom feature work scoped to your migration.
- Anyone on call overnight or on weekends.

---

## Reporting a bug

Good bug reports get fixed faster. The structure that works:

1. **What you ran** — the exact MCP tool call (or CLI command), with
   sensitive values redacted.
2. **What you expected** — one sentence.
3. **What happened** — the verbatim error / output, with sensitive
   values redacted. **Especially:** any `lineage.json` from the failed
   run.
4. **Repro fixture** — the smallest sanitized `.dtsx` (and any
   referenced `.dtproj` / `.params` / `.conmgr`) that reproduces the
   bug. Use the `convert_ssis_package` redaction defaults; if the
   problem only repros with sensitive data, redact and *describe* the
   sensitive shape rather than including it.
5. **Environment** — `python --version`, `pip show ssis-adf-agent`,
   OS, and (if a deploy issue) the target ADF region and
   integration-runtime type (Auto-Resolve / SHIR / Azure-SSIS).

A template is auto-populated when you click *New Issue* in GitHub.

### Sanitization checklist (before filing)

This is non-negotiable:

- [ ] No connection strings.
- [ ] No Key Vault names that identify the customer.
- [ ] No Azure subscription IDs / resource group names that identify
      the customer (replace with `<sub-redacted>` / `<rg-redacted>`).
- [ ] No SQL Server names that identify the customer.
- [ ] No real table or column names if they reveal business sensitive
      domain (e.g. customer names in a `Customers` table). Rename to
      `Table_A`, `col_1`.
- [ ] No package passwords, even for packages that have been rotated.

The maintainers will refuse a bug report that includes any of the
above and ask you to re-file. This is the customer's protection, not
the maintainers' inconvenience.

---

## See also

- [SECURITY.md](SECURITY.md) — vulnerability disclosure (separate from
  this document; do **not** file vulnerabilities as bugs).
- [workflow.md](docs/getting-started/workflow.md) — the 6-tool minimum migration path; if
  you are deviating from it, that's often the source of the issue.
- [rollback.md](docs/operations/rollback.md) — what to do when a deploy goes wrong.
- [docs/case-studies/](docs/case-studies/) — captured real failures
  and their resolutions; check here before filing.
