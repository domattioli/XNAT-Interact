# Feature Specification: Operational Hardening — 009 Deploy, Postgres Path, Local-XNAT Boot (011)

**Feature Branch**: `development`
**Created**: 2026-06-09
**Status**: Draft (spec only — not yet planned/built)
**Input**: User backlog (carried across sessions): (1) operational deploy of the 009 identity/dedup
layer — provision `XNAT_IDENTITY_SALT`, encrypt the crosswalk store at rest, migrate the live
`ConfigTables` JSON into the SQLite registry; (2) Postgres migration
([#34](https://github.com/domattioli/XNAT-Interact/issues/34)); (3) file/ship a reproducible
`boot-and-verify-local-xnat` capability (6th-session recurrence of manual local-XNAT bring-up pain).

## Overview

009 (`specs/009-data-identity-dedup`) **built the identity substrate** — `src/services/identity.py`
(HMAC pseudonym + date-hash, `load_identity_salt`), `src/services/registry.py` (SQLite registry,
`migrate_from_configtables`, `CrosswalkStore`) — but left three things as **deployment concerns,
explicitly out of scope of the code modules**:

1. The salt is read from env `XNAT_IDENTITY_SALT` and the loader raises if absent — but nothing
   **provisions** it (generation, storage, rotation, librarian-only access).
2. `CrosswalkStore` persists a **plaintext JSON** file holding the only copy of real HawkIDs; its
   own docstring says "Encryption is a deployment concern… out of scope for this module."
3. `migrate_from_configtables` exists and is parity-checked, but has **never been run against the
   live `ConfigTables` JSON** — the operational DB is still the JSON pseudo-database in practice.

Separately, two adjacent operability gaps recur:

4. **#34 Postgres**: the registry is SQLite-only. Multi-writer / server deployment needs a Postgres
   backend without changing identity/dedup behavior.
5. **Local-XNAT boot**: every dev/test session re-discovers how to stand up + health-check a
   localhost XNAT (the integration/roundtrip target). This has recurred ~6 sessions.

This feature is **operational hardening**: it does not change the *science* (009 already corrected
identity); it makes the 009 layer **safely deployable**, opens a **Postgres path**, and makes
**local-XNAT bring-up reproducible**. All work is localhost / synthetic-data only — it never touches
UIowa production (`rpacs.iibi.uiowa.edu`) or real PHI.

## Constitution alignment

- **Principle I (PHI Safety)**: US1 strengthens it — the crosswalk (only real-HawkID copy) moves
  from plaintext to encrypted-at-rest; the salt gets a real provisioning + non-logging contract.
  No PHI/HawkID/salt in logs, tests, argv, or repo.
- Migrations are **parity-checked + reversible** (already the 009 contract); US2 inherits it.
- Local-XNAT (US3) is synthetic-data only; it must refuse to point at any non-localhost host.

## Clarifications

### Session 2026-06-09

1. **Crosswalk encryption key (FR-003)**: **passphrase + scrypt KDF**. Librarian supplies a
   passphrase; the at-rest key is derived via `scrypt`. No new infra, works headless/localhost, the
   passphrase is never persisted. (Rejected: OS keychain — platform-specific, awkward in headless
   agent runs; external KMS — overkill for single-librarian localhost.)
2. **Postgres backend (US2/#34)**: **SQLAlchemy Core**. Schema + queries expressed once; both SQLite
   and Postgres derive from it, eliminating dual-dialect drift. Accept the new dependency.
3. **`boot-and-verify-local-xnat` home (US3)**: **repo script** (`scripts/`), versioned with the code
   that needs it. (DomI-skill promotion deferred — can extract later if the cross-repo recurrence
   justifies it.)
4. **Salt rotation**: **out of scope** for 011 — provisioning generates-if-absent; rotation is manual
   re-provision only. A rotation flow that re-derives existing pseudonyms is a separate future feature.

## User Scenarios & Testing *(mandatory)*

### User Story 1 — Deploy the 009 identity layer safely (Priority: P1)

The data librarian can stand up the 009 identity substrate for real: a salt is **provisioned**
(generated once, stored librarian-only, never logged), the **crosswalk is encrypted at rest**, and
the **live `ConfigTables` JSON is migrated** into the SQLite registry with parity + rollback — after
which the operational reads/writes go through the registry, not the JSON blob.

**Why this priority**: Until this runs, 009's correctness fixes are inert in production — the system
still uses the JSON pseudo-DB and stores HawkIDs in plaintext. This is the gating deliverable.

**Independent Test**: With a synthetic `ConfigTables` JSON + a provisioned test salt, run the deploy
path end-to-end; assert (a) salt loads from env and is absent from all output, (b) crosswalk file is
unreadable as plaintext (decrypts only with the librarian key), (c) post-migration registry row
counts match source and the JSON is retired/archived, (d) a forced parity-mismatch rolls back
cleanly leaving the registry untouched.

**Acceptance Scenarios**:

1. **Given** no `XNAT_IDENTITY_SALT` in env, **When** the deploy/provision step runs, **Then** it
   generates a CSPRNG salt, writes it to the librarian-only secret location (mode `0600`), and the
   value never appears in stdout/stderr/logs.
2. **Given** a provisioned salt and a librarian key, **When** a pseudonym↔HawkID pair is stored,
   **Then** the on-disk crosswalk is ciphertext (plaintext `grep` of the HawkID fails) and `get()`
   returns the HawkID only after decryption.
3. **Given** a live-shaped `ConfigTables` JSON, **When** the migration runs, **Then** registry row
   counts equal source counts, the audit log records the migration (actor/action/target, no PHI),
   and the JSON is archived (not deleted in place) for rollback.
4. **Given** a deliberately corrupted source row, **When** migration parity fails, **Then** all
   migrated rows roll back and the registry is byte-unchanged.

---

### User Story 2 — Postgres backend path (#34) (Priority: P2)

An operator can point the registry at **Postgres** instead of SQLite for a multi-writer / server
deployment, with **identical identity + dedup behavior** and a parity-checked data move.

**Why this priority**: Needed for the eventual shared-server deployment, but SQLite is sufficient for
the single-librarian workflow today — so it follows US1.

**Independent Test**: Run the existing 009 registry contract suite against both a SQLite backend and
a Postgres backend (containerized, localhost) and assert identical results; migrate a populated
SQLite registry into Postgres and assert row-for-row parity.

**Acceptance Scenarios**:

1. **Given** a backend selector (env/config), **When** set to `postgres` with a localhost DSN,
   **Then** all registry operations (record/dedup-lookup/audit) behave identically to SQLite.
2. **Given** a populated SQLite registry, **When** the SQLite→Postgres migration runs, **Then**
   counts match per table and a parity failure rolls back.
3. **Given** UNIQUE/index constraints from the 009 schema, **When** applied on Postgres, **Then**
   the same constraints are enforced (dup insert rejected). Schema + queries are expressed once via
   **SQLAlchemy Core**; both backends derive from it.

---

### User Story 3 — Reproducible local-XNAT boot + verify (Priority: P3)

A developer/agent can bring up a **localhost XNAT** and get a single **green/red health verdict**
with one command, instead of re-deriving the steps every session.

**Why this priority**: Pure ergonomics/time-save (recurs ~6 sessions) — valuable but never blocks
correctness; lowest priority of the three.

**Independent Test**: From a clean checkout, run the boot-and-verify entrypoint; assert it brings up
(or detects an already-running) localhost XNAT, runs health probes (auth, project list, a synthetic
round-trip), and exits `0` HEALTHY / non-zero with a specific failed-probe line — and **refuses** any
non-localhost host.

**Acceptance Scenarios**:

1. **Given** no XNAT running, **When** boot-and-verify runs, **Then** it starts a localhost XNAT and
   reports each probe (up / auth / project-list / synthetic round-trip) with a final HEALTHY line.
2. **Given** an XNAT already running locally, **When** it runs, **Then** it skips boot, verifies, and
   reports HEALTHY without double-starting.
3. **Given** a configured host that is not `localhost`/`127.0.0.1`, **When** it runs, **Then** it
   refuses with a safety error and starts nothing (no prod contact).
4. **Given** a failing probe (e.g. auth), **When** it runs, **Then** it exits non-zero naming the
   failed probe. Shipped as a **repo script** (`scripts/`), versioned with this codebase;
   DomI-skill extraction deferred.

---

## Requirements *(mandatory)*

### Functional Requirements

**US1 — 009 deploy**
- **FR-001**: A provisioning step MUST generate a CSPRNG salt when absent, persist it to a
  librarian-only location (`0600`), and load it via the existing `load_identity_salt` contract.
- **FR-002**: The salt MUST NEVER be written to logs, telemetry, error messages, argv, tests, or the
  repo (extends the 009 security invariant to the deploy path).
- **FR-003**: `CrosswalkStore` MUST encrypt the HawkID mapping at rest; plaintext search of the
  backing file for a known HawkID MUST fail. The at-rest key is **derived from a librarian passphrase
  via `scrypt`**; the passphrase is never persisted, logged, or committed.
- **FR-004**: A deploy command MUST run `migrate_from_configtables` against the live JSON, assert
  parity, record an audit entry, and archive (not in-place delete) the source JSON.
- **FR-005**: Parity failure MUST roll back fully (registry unchanged) — reuse the 009 guarantee.
- **FR-006**: After deploy, operational identity/dedup reads/writes MUST target the registry, with
  the JSON path retired behind the registry (no dual source of truth).

**US2 — Postgres (#34)**
- **FR-007**: The registry MUST support a backend selector (SQLite default, Postgres opt-in) without
  changing the public registry API.
- **FR-008**: The 009 schema (tables, UNIQUE indexes, audit) MUST be expressed for Postgres with
  equivalent constraint enforcement.
- **FR-009**: A SQLite→Postgres data move MUST be parity-checked + reversible.
- **FR-010**: The full 009 registry contract suite MUST pass against both backends.

**US3 — local-XNAT boot**
- **FR-011**: A single entrypoint MUST bring up (or detect) a localhost XNAT and run ordered health
  probes (up → auth → project-list → synthetic round-trip), emitting a HEALTHY/FAILED verdict.
- **FR-012**: The entrypoint MUST refuse any non-localhost target and start nothing in that case.
- **FR-013**: It MUST be idempotent (no double-start) and exit non-zero naming the first failed probe.
- **FR-014**: It MUST use synthetic data only for the round-trip probe; no real PHI, no prod host.

### Key Entities

- **Identity salt**: librarian-only secret bytes; env `XNAT_IDENTITY_SALT`; provisioned + rotated.
- **Crosswalk store**: encrypted-at-rest pseudonym↔HawkID map; only copy of real HawkIDs.
- **Registry**: SQLite-or-Postgres schema-enforced operational DB (replaces ConfigTables JSON).
- **Local-XNAT environment**: containerized localhost XNAT + ordered health probes.

## Success Criteria *(mandatory)*

- **SC-001**: Post-deploy, a plaintext search of the crosswalk backing file for any real HawkID
  returns nothing, and the salt appears in zero log/stdout/stderr bytes across the deploy run.
- **SC-002**: Live `ConfigTables` JSON migrates with 100% row parity; a corrupted-row trial rolls
  back to a byte-identical pre-migration registry.
- **SC-003**: The 009 registry contract suite passes identically on SQLite and Postgres (0 diffs);
  SQLite→Postgres move is row-for-row parity.
- **SC-004**: `boot-and-verify-local-xnat` yields a HEALTHY verdict from a clean checkout in one
  command, refuses non-localhost, and names the failed probe on any failure.
- **SC-005**: No PHI/HawkID/salt/credential appears in any test, log, commit, or repo artifact across
  all three stories (Constitution Principle I).

## Out of Scope

- Real-data / production deployment against UIowa XNAT (this spec is localhost + synthetic only).
- Salt **rotation** beyond manual re-provision (re-deriving existing pseudonyms is a future feature).
- The advanced pixel-PHI de-id (shipped separately as `010-pixel-deid`).
- Arthroscopy / simulation sibling tracks.

## Dependencies

- 009 substrate (`identity.py`, `registry.py`, `CrosswalkStore`, `migrate_from_configtables`) — built.
- A localhost XNAT container image; `scrypt` KDF + an AEAD cipher (FR-003); SQLAlchemy Core + a
  localhost Postgres container (US2).
