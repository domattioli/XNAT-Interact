# Feature Specification: Real-XNAT Round-Trip Correctness (Phase 7)

**Feature Branch**: `007-real-xnat-roundtrip-correctness`
**Created**: 2026-06-05
**Status**: ✅ Complete (all five defects fixed, 742 tests green, offline verified; real-XNAT fidelity verification deferred to Phase 6 contract test)
**Input**: Real-XNAT integration round-trip (PR #23 harness,
`tests/integration/run_roundtrip_{push,pull}.py` + logs) run against a live XNAT
1.9.3 with synthetic no-PHI data. Surfaced five correctness defects invisible to
the 523-test offline FakeXNAT suite. Issues: **#27, #28, #29, #25, #30**.

**Completed**: Commit d0d80ae. All five defects fixed in `src/xnat_experiment_data.py`, `src/utilities.py`, and `app/logic/download.py`. FakeXNAT fidelity extended (post-create datatype cache, N-file resources, label vs ID). Offline suite: 742 passed, 7 xfailed.

## Overview

The offline suite is green, but the tool **cannot complete a single real
push+pull round-trip**. The push dies on call #1 (`attrs.mset`), fresh-project
bootstrap can never self-initialize, browse never lists RF experiments, and the
download leg fetches one synthesized file per scan instead of the real series.
Root meta-cause: the FakeXNAT double diverged from pyxnat's real behavior, so the
full green suite masks every server-only failure.

This phase makes the **real round-trip actually pass** — the smallest correct
fixes for the five defects, each guarded by a test the old FakeXNAT could not
catch. It is the urgent-correctness slice; the deeper structural rehoming of
these call sites behind a gateway is **Phase 6** (`006-xnat-alignment`).

### Relationship to Phase 6

- Phase 6 = structural alignment (gateway ABC + conventions module). It also
  lists the #25 download fix as its proof-of-value.
- Phase 7 = correctness NOW. Direct minimal fixes so the round-trip works,
  landable independently of the refactor.
- **Overlap (#25 download, #29 browse)**: Phase 7 lands the direct fix; Phase 6
  later re-homes that exact logic behind `XnatGateway` / `conventions` with no
  behavior change. Phase 7's new tests carry forward as the gateway's contract
  tests. If Phase 6 lands first, Phase 7's #25/#29 tasks collapse to "verify
  through the gateway" instead of patching raw call sites.

## Clarifications

### Session 2026-06-05 (speckit-clarify)

- Q: #28 — who may self-initialize ConfigTables on a fresh project (replacing the
  hardcoded `dmattioli/domattioli/stelong` whitelist)? → A: Project member/owner
  **OR** a config-listed allowlist (not code) — backward-compatible escape hatch
  for service accounts (CI/admin) not on the project roster.
- Q: #25 — default on-disk result of a whole-surgery download? → A: **Always a
  zip**, but the user chooses what it includes (source images only, a subset or
  all scans, derived data, etc.) — selectable contents, single zip artifact.
- Q: #27 — re-publish behavior when a prior failed push left orphaned empty
  subjects? → A: **Reuse existing, fill in missing** (idempotent upsert) — detect
  the orphaned subject/experiment, reuse it, create only missing children; no
  duplicates, no manual cleanup.

### Pre-existing scope clarifications (2026-06-05)

1. **Scope = the five filed defects only.** No new user features beyond what #25
   already specifies (whole-surgery download). No refactor — minimal targeted
   fixes at the named file:line.
2. **Fidelity is the deliverable, not just the patch.** Each fix ships with a
   test that fails on today's code against a higher-fidelity fake (or real-XNAT
   marker) — otherwise the same blind spot returns.
3. **#27 fix = unblock, minimally.** Set the datatype on the pyxnat handle right
   after `create()` (or set attrs in the same `create` call). Full gateway
   contract test is Phase 6; Phase 7 ships the targeted regression test.
4. **#30 is cleanup.** Guard the one unguarded tag; fix-or-remove the stale
   bootstrap script. No behavior change for valid full-tag DICOMs.
5. **Synthetic/localhost only.** Real-XNAT tests run against a local throwaway
   (xnat4tests / the PR #23 `tests/integration/xnat_local/` image), never the
   UIowa production server, never real PHI, no creds in argv/logs.

## User Scenarios & Testing *(mandatory)*

### User Story 1 — A real push completes (Priority: P1, #27)

A librarian uploads a de-identified RF session to a real XNAT. The experiment,
subject, and scan are created, their attributes set, and the DICOM zip lands —
no `TypeError` on the first attribute write.

**Why first**: This is the primary "does the tool work" blocker. Nothing
downstream (browse, download) can be exercised end-to-end until push succeeds.

**Independent Test**: Against a fake that reproduces pyxnat's post-`create()`
empty datatype-cache (or a real local XNAT), a full `publish_to_xnat` run sets
experiment/subject/scan attrs and uploads the zip; the run that hits today's
`attrs.mset` path raises `TypeError: quote_from_bytes() expected bytes` and the
fixed path does not.

**Acceptance Scenarios**:
1. **Given** a freshly `create()`d experiment handle, **When** `attrs.mset` runs,
   **Then** the xsiType is resolved (datatype set on the handle) and no
   `TypeError` is raised.
2. **Given** a complete `SourceRFSession` publish, **When** it finishes, **Then**
   the server holds 1 experiment + 1 subject + scan `0` + `SRC` with all files
   (no orphaned empty subjects).

---

### User Story 2 — A fresh project bootstraps its config (Priority: P1, #28)

A first run against a brand-new project (or by a non-whitelisted user such as
`admin`/CI) self-initializes `ConfigTables` instead of crashing.

**Independent Test**: `ConfigTables.__init__` against a project with no
`database_config.json` triggers self-initialization (does not propagate
`pyxnat.core.errors.DataError`); init succeeds for a user not named
`dmattioli/domattioli/stelong`.

**Acceptance Scenarios**:
1. **Given** a project with no config file, **When** `ConfigTables` initializes,
   **Then** the pyxnat `DataError` ("Cannot get file: does not exists") is treated
   as first-run and the config is created.
2. **Given** a connecting user not in the legacy hardcoded list, **When** init
   runs, **Then** it is authorized via project-membership/owner lookup (not a
   `PermissionError` from a name whitelist).

---

### User Story 3 — Browse lists RF experiments (Priority: P1, #29)

Browsing a project surfaces its real RF sessions by human-readable subject label,
so the download UI can reach a valid selection.

**Independent Test**: With a staged `xnat:rfSessionData` experiment, the fake/real
listing returns it (was `[]`); subject column shows the label
(`ITEST_SUBJ_0001`), not an internal ID (`Xnat4Tests_S00003`).

**Acceptance Scenarios**:
1. **Given** a staged RF experiment, **When** `list_downloadable(project)` runs,
   **Then** it appears in the rows (type-agnostic enumeration surfaces RF/CT/US).
2. **Given** the browse table, **When** it renders the Subject column, **Then** it
   shows resolved labels and downstream queries use labels, not internal IDs.

---

### User Story 4 — Whole-surgery download fetches the full series (Priority: P2, #25)

Selecting a surgery (experiment) downloads **every** file across **all** its
scans, count-verified against the server's reported `# Files`.

**Independent Test**: A scan seeded with N files downloads N intact files (was: 1
synthesized `{subject}_{exp}_{scan}.dcm`); a whole-experiment selection enumerates
every scan; a count mismatch raises a friendly error; an empty resource is a
friendly no-op.

**Acceptance Scenarios**:
1. **Given** a scan with N files, **When** downloaded, **Then** N files land and
   the count matches the server.
2. **Given** a one-click whole-surgery selection with a chosen content scope
   (source-only / subset / all / + derived), **When** downloaded, **Then** every
   selected scan's resource is enumerated, fetched, and delivered as one zip whose
   contents match the selection (default = full source-image set).

---

### User Story 5 — Cleanup: no unguarded tag, no dead bootstrap (Priority: P3, #30)

A valid DICOM lacking `InstanceNumber` does not crash session build; the stale
`initialize_basic_metatable_items.py` is fixed or removed.

**Independent Test**: `_mine_session_metadata` on a DICOM with no `InstanceNumber`
derives/defaults an index instead of `AttributeError`; importing the bootstrap
script no longer fails on `from Utilities import MetaTables`.

**Acceptance Scenarios**:
1. **Given** a DICOM with no `InstanceNumber`, **When** session metadata is mined,
   **Then** it is guarded like its sibling tags (default/derived index).
2. **Given** the bootstrap script, **When** imported, **Then** it either references
   current `src.utilities.ConfigTables` correctly or is removed.

---

### Edge Cases
- `subj_inst`/`scan_inst` hit the **same** post-`create()` datatype bug as
  `exp_inst` — all three create+mset sites must be fixed, not just the experiment.
- After a previously-failed push, orphaned/partial subjects may exist — fix must not
  assume a clean project; re-publish reuses the existing subject/experiment and
  fills in missing children (idempotent upsert), never duplicating.
- A project where `database_config.json` exists but is malformed → still distinct
  from "does not exist"; do not silently re-initialize over a real (corrupt) file.
- Download of a scan whose server `# Files` is 0 → friendly no-op, not a crash.
- Mixed-modality project (RF + CT) → browse must surface all, not only RF.

## Requirements *(mandatory)*

### Functional Requirements
- **FR-001 (#27)**: After each `create()` in `publish_to_xnat`
  (`src/xnat_experiment_data.py` ~L257), the pyxnat handle's datatype MUST be set
  before `attrs.mset` so `_get_datatype()` returns the xsiType (not `None`). Applies
  to experiment, subject, and scan handles.
- **FR-002 (#27)**: A real push of a `SourceRFSession` MUST create
  experiment+subject+scan+`SRC` and upload the DICOM zip with zero `TypeError`.
  Re-publish MUST be an **idempotent upsert**: if a prior failed run left an
  orphaned/partial subject or experiment, reuse it and create only the missing
  children — no duplicate subject/experiment, no manual cleanup required.
- **FR-003 (#28)**: `ConfigTables.__init__` first-run detection
  (`src/utilities.py` ~L634) MUST treat pyxnat's `DataError` ("file does not
  exist") as first-run and self-initialize, in addition to
  `FileNotFoundError`/`KeyError`/`ValueError`.
- **FR-004 (#28)**: The accessor authorization (`src/utilities.py` ~L704) MUST NOT
  rely on a hardcoded username list. It MUST authorize a connecting user who is a
  project **member or owner** (via the `_verify_login` users()/owner lookup) **OR**
  who appears in a **config-listed allowlist** (config/env, not code) — the latter
  being the escape hatch for service accounts (CI/admin) not on the project roster.
- **FR-005 (#29)**: Browse enumeration MUST resolve and use human-readable subject
  **labels** (not internal `*_S#####` IDs) for display and downstream queries.
- **FR-006 (#29)**: Experiment enumeration MUST be type-agnostic (surface
  `xnat:rfSessionData` and other session types), so RF experiments appear in
  `list_downloadable`.
- **FR-007 (#25)**: The download leg MUST enumerate a scan resource's **real**
  files and fetch each (or zip-download + unpack) instead of synthesizing one
  filename (`app/logic/download.py` ~L222); downloaded count MUST be verified
  against the server's reported `# Files`.
- **FR-008 (#25)**: A one-click **whole-surgery** (experiment) selection MUST
  auto-expand to all its scans. The result MUST be delivered as a **single zip**
  whose **contents are user-selectable** — e.g. source images only, a subset or
  all scans, and/or derived data — defaulting to the full source-image set when
  no selection is made. Fail-soft, cross-platform paths, offline-testable.
- **FR-009 (#30)**: `dicom_obj.metadata.InstanceNumber`
  (`src/xnat_experiment_data.py` ~L506) MUST be `hasattr`-guarded like its sibling
  tags, defaulting/deriving an instance index when absent.
- **FR-010 (#30)**: `src/initialize_basic_metatable_items.py` MUST be fixed to
  reference `src.utilities.ConfigTables` or removed if `ConfigTables`
  self-initializes.
- **FR-011 (fidelity)**: Each fix MUST ship with a test that FAILS on current code
  — either a higher-fidelity fake reproducing the real pyxnat behavior, or a
  real-XNAT-marked integration test (synthetic/localhost only). The existing
  offline suite MUST still pass.
- **FR-012**: All paths remain PHI-free, fail-soft, no creds in argv/logs; tests
  never touch the UIowa production server.

### Key Entities
- **publish_to_xnat path**: experiment/subject/scan create + attrs + zip upload.
- **ConfigTables bootstrap**: first-run detection + accessor authorization.
- **browse / list_downloadable**: subject-label resolution + type-agnostic
  experiment enumeration.
- **download leg**: real file enumeration + count-verify + whole-surgery expansion.
- **fidelity harness**: the higher-fidelity fake / real-XNAT markers that catch
  each defect.

### Success Criteria *(mandatory)*
- **SC-001 (#27)**: A scripted real (local) push completes: server shows 1 exp + 1
  subj + scan `0` + `SRC` with all files; the pre-fix code raises the documented
  `TypeError` and the post-fix code does not. A re-publish over a pre-seeded
  orphaned/partial subject reuses it and fills missing children — final state has
  exactly one subject/experiment (no duplicates).
- **SC-002 (#28)**: `ConfigTables` initializes on a fresh project AND for a
  non-whitelisted user, with no manual pre-seeding of `database_config.json`.
- **SC-003 (#29)**: A staged RF experiment appears in `list_downloadable` and the
  Subject column shows labels, not internal IDs.
- **SC-004 (#25)**: A scan with N files downloads N files (was 1); a one-click
  whole-surgery selection produces a single zip whose contents match the chosen
  scope (default = all source images), count-verified per scan; empty resource is
  a friendly no-op.
- **SC-005 (#30)**: A DICOM without `InstanceNumber` builds without crashing; the
  bootstrap script imports (or is gone).
- **SC-006 (fidelity)**: Every one of SC-001..SC-004 has a test that FAILS at the
  parent commit (proving the old FakeXNAT blind spot is closed); full offline suite
  still green.

### Assumptions
- A local throwaway XNAT (PR #23 `tests/integration/xnat_local/` image or
  xnat4tests) is available for real-XNAT-marked tests; CI may skip-mark them if no
  server, but the higher-fidelity-fake tests run everywhere.
- The five defects are independent fixes (different files/functions); they can land
  in separate commits and be verified in isolation.
- pyxnat remains the client this phase (no xnatpy swap — that is Phase 6's spike).
- Phase 6, when built, re-homes #25/#29 behind the gateway without changing the
  behavior these criteria lock in.
