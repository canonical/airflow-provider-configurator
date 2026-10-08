# ADR 0001: Scope of the Layer 2 provider configuration denylist

- **Date:** 2026-10-07
- **Spec:** WF029 sections 1.3.1, 3.1, 3.2.2 and 3.4
- **Affects:** airflow-provider-configurator (`src/denylist.yaml`, `src/denylist.py`)

## Context

This charm relays an unreviewed `.ini` file from git into the coordinator's
`airflow.cfg`. WF029 section 3 guards it in two layers: **Layer 1** drops keys
the coordinator currently renders (dynamic), **Layer 2** is this charm's short
static denylist for what is out of bounds regardless.

WF029 3.2.2 seeds Layer 2 with one entry, `core.dags_folder`, and asks the team
to review the list before implementation. This ADR is that review.

The boundary is narrower than "providers only touch their own sections". The
spec's own example `.ini` (§1.3.1) writes `[secrets] backend` and `[logging]
remote_logging`, and §3.1 defends that as providers' documented integration
mechanism. `[secrets] backend` is an import path, so this charm accepts
code-execution-shaped configuration *by design*. Layer 2 therefore guards only
the sections Airflow's own machinery owns.

Layer 2 constrains **whoever writes the `.ini`**, not the Juju admin. The
denylist is a file in the charm's own install directory, so the `.ini` author
has no way to reach it. An admin with `juju ssh` can edit it, and that is not
worth preventing: the same admin can `juju refresh` onto a patched charm, so any
in-charm defence would be circular.

## Decision

### 1. Deny whole sections, not individual options

| Entry | Why |
|---|---|
| `core` | DAG and plugin paths, `executor`, deserialization allow-lists, `auth_manager`, `fernet_key` |
| `api` | API/UI exposure, `expose_config`, session signing |
| `dag_processor` | where DAG code is fetched from |
| `webserver` | 2.x alias Airflow 3.x still honours: `[webserver] secret_key` reads as `[api] secret_key` |
| `default` | inherited by every section, including those above |
| `fab.auth_backends` | can switch the UI to an unauthenticated mode |

Sections rather than options, because an option-level list ages badly: entries
need re-verifying on every Airflow bump, and an option added later lands in an
already-sensitive section uncovered. `[core]` is `[core]` across 3.x. The seed
entry `core.dags_folder` is subsumed by `core`.

This also denies benign tuning like `core.parallelism`, which is intended — per
WF022 those are coordinator charm config options, not provider configuration.

Two entries need justifying. `webserver`, because denying a section does *not*
deny its deprecated alias: Airflow resolves the old spelling after this charm
has seen the file. `fab.auth_backends` is the one option-level entry, because
`auth_backends` moved to the FAB provider in 3.x and a provider may have
legitimate `[fab]` settings.

### 2. Layer 2 does not restate Layer 1

`core.fernet_key`, `core.executor`, `api.secret_key` and
`database.sql_alchemy_conn` are rendered by the coordinator, so Layer 1 already
drops them. Denying them twice adds no defence and removes the only signal that
would reveal a Layer 1 regression.

### 3. Matching stays liberal, so the list can stay small

Three rules in `denylist.py` do work the list would otherwise do by enumeration:

- **Case-insensitive, for section names as much as option names.** Airflow
  lowercases both, so `[CORE] DAGS_FOLDER` is the same setting. `configparser`
  preserves case on output (`optionxform = str`), so folding happens at
  comparison time.
- **`_cmd` / `_secret` variants are implied** by the base option — Airflow
  resolves `_cmd` by running its value as a shell command. Layer 1 cannot cover
  these: the coordinator renders the bare option, so the variant collides with
  nothing.
- **`[DEFAULT]` options are removed at source.** `configparser` cannot remove an
  inherited default from one section — `remove_option()` reports success while
  `write()` re-emits the value.

Drops stay non-blocking (3.4), which is what makes a liberal list tolerable: an
over-broad entry costs a warning, not a blocked unit.

### 4. An unloadable denylist blocks, per 1.3 rather than 3.4

If `denylist.yaml` is missing, unreadable, invalid YAML, or has no non-empty
`deny` list, the charm blocks instead of publishing.

The file is static and ships inside the charm, so this is not reachable by an
operator or by whoever writes the `.ini` — it is **not** a security control. The
only way to produce it is a packaging regression that leaves the file out of the
built charm. A bad edit is caught earlier, by the unit test that loads the real
file.

That case is worth guarding because it is silent: every key would pass Layer 2,
the unit would go Active, and nothing would indicate the validation had been
skipped. Blocking is also not an exception to 3.4 — that section governs
*violations*, and there is no offending key here. It is the class of a missing
`file_path` (1.1), an undiscoverable `.ini` (1.3) or a secret collision (3.3),
all of which block.

### 5. Changing a denied section is a charm release, not a runtime override

A provider that genuinely needs a `[core]` option has three routes, preferred
first: expose it as coordinator charm config (the WF022 pattern); or narrow the
denylist entry to the options that actually matter, which is a `denylist.yaml`
edit rather than a code change — the reason the list ships as data; or nothing,
since a denied key is dropped, not fatal, and the deployment keeps running
meanwhile.

A config option to override the denylist is deliberately not offered: it would
hand the bypass to whoever writes the `.ini`, the party Layer 2 constrains.

## Consequences

- Six entries, reviewable at a glance, as 3.2.2 assumes. No update needed when
  Airflow adds options to `[core]`, `[api]` or `[dag_processor]`.
- One-sentence boundary: *Airflow's own configuration goes through the
  coordinator; everything else is yours.*
- **A provider cannot set anything in `[core]`, `[api]`, `[dag_processor]` or
  `[webserver]`.** Recovery is a charm release (decision 5); non-fatal meanwhile.
- **A denied section can drop many keys at once**, so Juju may truncate the
  status message. The log is complete; the status may not be.

## Open question

**Executor sections — needs a decision before Layer 2 ships.**

`[kubernetes_executor] pod_template_file`, `worker_container_repository` and
`worker_container_tag` decide which image runs worker pods. The section is not
denied, on the argument that `core.executor` is denied and that while the
executor is in use the coordinator renders the section, so Layer 1 covers it.

That rests on another charm's implementation detail, and holds only if the
coordinator renders the *complete* set of image-determining options. It is also
shaped by today's support matrix and does not generalise — every executor added
later brings the same question.

Two ways to close it:

1. **Deny executor sections as a class.** Unilateral and cheap, answering it
   once instead of per executor. Cost: moves `[celery]` out of the "shared
   sections a provider may write to" set, which needs stating explicitly.
2. **Verify and pin the coordinator's render set**, so Layer 1 provably covers
   them. Correct, but a cross-team dependency that leaves the gap open until it
   lands.

Until one is done, the gap is live.

## Alternatives considered

- **Enumerate individual options,** as the seed entry does. Open-ended,
  release-specific, duplicates Layer 1. Rejected.
- **Deny every section the coordinator renders, derived from WF022.** That is
  Layer 1, computed statically and so permanently at risk of drifting. Rejected.
- **Allow-list the sections a provider may write to.** Already rejected by the
  spec (§3.1): the set grows with every provider installed, and scoping it to a
  provider's own sections breaks the pattern §1.3.1 documents.

## Out of scope

- **Validating option *values*.** Layer 2 decides whether a key may be set, never
  what it may be set to.
- **Remaining deprecated aliases.** `webserver` covers the one with real
  consequences; a full alias table would need tracking across releases.
