# Support

> **Active engagements only.** This document distinguishes the OSS
> support model (best-effort, GitHub-only) from the **engaged-customer**
> support model (named channel + response-time commitment). Migrations
> are time-pressured; the engaged-customer model exists because GitHub
> issues is not a support channel for an estate cut-over at 11 p.m.

---

## Tier 1 — OSS users

You're using the agent off `pip install` / a `git clone` without an
engagement contract.

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

If your migration is genuinely time-pressured and you need any of the
above, you want Tier 2.

---

## Tier 2 — Engaged customers

You're running an active migration under an engagement (FastTrack,
Solutions Architect, paid Premier / Unified, or named-Microsoft-team
support).

The engagement contract names the support channel and the response-time
commitment for the duration of the migration. The defaults below are
the ones the maintainers recommend; your engagement may have stricter
or looser terms.

### Default response-time commitments (engaged)

| Severity | First response | Resolution path |
|---|---|---|
| **Sev 1 — Migration blocked.** Production cut-over imminent (≤ 48h) and a single failure is blocking go-live. | **2 business hours** during the engagement support window. | Direct line to the engaged engineer (Teams chat, email, or phone per the engagement). |
| **Sev 2 — Migration impaired.** A defect is consuming significant engineering time, but a workaround exists. | **1 business day.** | Issue tracked in the engagement-shared GitHub issue queue, labeled `engaged-sev2`. |
| **Sev 3 — Question / minor defect.** Anything that would otherwise be a Tier 1 issue. | **3 business days.** | Same as Tier 1: GitHub issue. |

"Business hours" defaults to the engaged customer's local business
hours unless the engagement contract says otherwise.

### Named channel template

For the engagement to provide the response times above, the customer
and the engaged team should agree on **at least one** of:

- A **shared Microsoft Teams channel** (preferred — async-friendly,
  full thread history, file sharing).
- A **shared email distribution list** with at least 2 recipients on
  the agent side (single-recipient channels do not survive PTO).
- An **on-call rotation roster** named in the engagement contract,
  with a backup primary.

Single-engineer phone numbers are explicitly *not* a supported channel
even at Sev 1 — they create a single point of failure that violates
the response commitment the moment the engineer is unreachable.

### What's covered (engaged)

- Defects in the agent that block your migration.
- Help interpreting `bulk_analyze` / `propose_adf_design` /
  `validate_conversion_parity` output.
- Help debugging deploy failures (especially the failure modes
  documented in [docs/case-studies/](docs/case-studies/)).
- Sanitized walkthroughs of the [WORKFLOW.md](WORKFLOW.md) path
  against your specific estate.

### What's not covered (engaged or otherwise)

- Custom converter work for your in-house third-party SSIS components.
  Vendor-curated registries (P4-2) are the supported extension path;
  contributing your own registry is welcome.
- Code review or hand-porting of Script Tasks the
  [LLM translator](SECURITY.md) cannot handle. The
  [Database_Access_Configuration case study](docs/case-studies/script_task_port_database_access_configuration/README.md)
  documents the methodology so your team can do these yourselves.
- Azure RBAC / Key Vault / SHIR setup on your tenant. The
  [RBAC matrix](RBAC.md) documents what's required; provisioning is
  your team's responsibility.

---

## Reporting a bug (either tier)

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

This is non-negotiable for both tiers:

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
- [WORKFLOW.md](WORKFLOW.md) — the 6-tool minimum migration path; if
  you are deviating from it, that's often the source of the issue.
- [ROLLBACK.md](ROLLBACK.md) — what to do *while* waiting for support
  on a sev 1.
- [docs/case-studies/](docs/case-studies/) — captured real failures
  and their resolutions; check here before filing.
