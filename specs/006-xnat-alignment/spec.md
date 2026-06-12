# Feature Specification: XNAT Alignment — Gateway Abstraction & Conventions (Phase 6)

**Feature Branch**: `006-xnat-alignment`
**Created**: 2026-06-05
**Status**: Draft (planning only — not yet built)
**Input**: Findings in [`docs/XNAT_MODEL.md`](../../docs/XNAT_MODEL.md). Align the
codebase to the now-understood XNAT model: collapse the scattered raw-pyxnat surface
behind one gateway, centralize the path/label conventions, fix the download leg to
use the real API, and open the door to derived-data-as-assessor + an xnatpy swap.

## Overview

We now know (Phase-0 research, `docs/XNAT_MODEL.md`) that the entire XNAT
dependency is a **16-call pyxnat surface** scattered across 6 source files, that
our path/label conventions (`scan='0'`, `SOURCE_DATA-{uid}`, resource labels) are
inlined as f-strings in multiple places, that the download leg **synthesizes**
filenames instead of enumerating the API, and that derived results (consensus) are
stored as loose resources rather than XNAT **assessors**.

This phase **does not add user features**. It is a structural alignment so that
every *future* feature is built against a stable seam:

1. **Gateway ABC** — one `XnatGateway` interface wrapping the 16 calls;
   `PyxnatGateway` implements it; the test fake conforms. All call sites go
   through it.
2. **Conventions module** — one place that builds query strings + labels, so the
   `scan='0'` / experiment-label / resource-label rules live once.
3. **Download alignment** — gateway exposes `list_files` / `download_resource`
   (real `CObject` enumeration / `Resource.get`); fixes the #25 gap.
4. **Derived-data seam** — gateway can write an **assessor** (XNAT's searchable
   derived-data slot) so consensus/STAPLE output lands correctly when built.
5. **xnatpy readiness** — the ABC makes a client swap a single new implementation,
   evaluated via spike, not a repo-wide rewrite.

Constraints unchanged: offline-testable (no server/PHI), fail-soft, no creds in
argv/logs, no behavior change visible to users (pure refactor + one bug fix).

## Clarifications (2026-06-05)

1. **Scope = refactor + the #25 download fix.** No new user-facing capability.
   The download fix is included because it is the one place current behavior is
   *wrong* against the API, and it is the proof the gateway earns its keep.
2. **Backward-compatible storage.** Existing XNAT layouts (resource labels,
   ConfigTables JSON, annotation blobs) are NOT migrated. The gateway reproduces
   today's writes byte-for-byte; only the *call path* changes.
3. **Assessor support = seam, not migration.** Add the gateway method + a test;
   do NOT move existing `ANNOTATIONS`-resource data into assessors this phase.
   Wiring consensus output to it happens when STAPLE is built (Phase-5 seam).
4. **xnatpy = spike only.** Implement nothing in production; a throwaway
   `XnatpyGateway` behind the ABC + a comparison note is the deliverable.
5. **Real-XNAT verification = contract test, not one-off harness.** Phase 7 
   offline verification is complete (742 tests green). Phase 6 ships a durable
   contract-test framework (grad-student workflow sim) that locks in FakeXNAT ≈ 
   real XNAT parity and replaces one-off integration harnesses. See 
   [`contract-test.md`](contract-test.md).

## User Scenarios & Testing *(mandatory)*

### User Story 1 — All XNAT access goes through one seam (Priority: P1)

A developer adding a feature that touches XNAT codes against `XnatGateway`, never
raw pyxnat. They can read the whole XNAT contract in one file and test against the
fake without a server.

**Why first**: Every other item depends on the seam existing.

**Independent Test**: Grep shows no `import pyxnat` / `Interface(` / `.put_zip(`
outside `src/services/xnat_gateway.py` and the test fake. The full existing suite
(718 tests) passes unchanged against a `FakeGateway` that implements the ABC.

**Acceptance Scenarios**:
1. **Given** the refactored repo, **When** you grep for raw pyxnat calls in
   `src/` outside the gateway module, **Then** there are none.
2. **Given** a test, **When** it injects `FakeGateway`, **Then** publish /
   config / annotation paths run offline with identical recorded calls.

---

### User Story 2 — Conventions live in one place (Priority: P1)

The `scan='0'` simplification, the `SOURCE_DATA-{uid}` experiment label, and the
resource-label set are defined once. Changing the single-scan assumption (future
multi-series) is a one-file edit.

**Independent Test**: A `paths`/`conventions` module returns every query string +
label; call sites import it; no inline `'/project/' + ...` f-strings remain in the
object classes.

**Acceptance Scenarios**:
1. **Given** the conventions module, **When** a call site needs a subject path,
   **Then** it calls `conventions.subject_qs(uid)` not a local f-string.
2. **Given** the resource-label registry, **When** code references `ANNOTATIONS`,
   **Then** it uses the named constant, not a string literal.

---

### User Story 3 — Download enumerates the real API (Priority: P1)

Downloading a surgery fetches the scan's **actual** files (count-verified), not one
synthesized filename per row. Closes #25.

**Independent Test**: FakeGateway seeded with a scan holding N files; download
returns all N, decoded/intact; a count mismatch raises FriendlyError.

**Acceptance Scenarios**:
1. **Given** a scan with N files, **When** downloaded, **Then** N files land and
   the manifest/count matches.
2. **Given** a whole surgery (experiment) selection, **When** downloaded, **Then**
   every scan's resource is enumerated and fetched (one-click), optionally zipped.

---

### User Story 4 — Derived results can be stored as assessors (Priority: P2)

A future consensus (STAPLE) output is writable as an XNAT **assessor** (searchable
derived data), not a loose resource. This phase ships only the seam + test.

**Independent Test**: `gateway.create_assessor(...)` + `put` against FakeGateway
records the assessor path; no production caller required yet.

**Acceptance Scenarios**:
1. **Given** the gateway, **When** `create_assessor` is called, **Then** the fake
   records an assessor-level write distinct from a scan-resource write.

---

### User Story 5 — A client swap is one implementation (Priority: P3)

Evaluate `xnatpy` by implementing the ABC once; no other code changes.

**Independent Test**: A spike `XnatpyGateway` (kept out of the default import path)
satisfies the ABC's type contract; a short note records the A/B comparison.

---

### Edge Cases
- Empty resource (no files) on download → friendly "nothing to download", not a crash.
- A call site that used a pyxnat behavior the ABC doesn't model → ABC grows a
  method (documented), never a raw-pyxnat escape hatch in a caller.
- Lost-update guard in ConfigTables must keep working through the gateway (it relies
  on a re-fetch + fingerprint — gateway must expose the read it needs).
- Singleton `XNATConnection` lifecycle unchanged from the caller's view.

## Requirements *(mandatory)*

### Functional Requirements
- **FR-001**: A single `XnatGateway` ABC MUST declare every XNAT operation the
  repo performs (the 16-call surface in `docs/XNAT_MODEL.md §3`): connect/disconnect,
  liveness, project label/users, select, exists, create, set_attrs, resource
  handle, put_zip, file put/insert/get_copy/delete, plus list_files /
  download_resource / create_assessor (new).
- **FR-002**: `PyxnatGateway(XnatGateway)` MUST implement it via pyxnat, producing
  writes **byte-identical** to today (same labels, formats, tags, overwrite flags).
- **FR-003**: The test double MUST implement the SAME ABC (rename/extend
  `tests/fakes/fake_xnat.py` → a `FakeGateway`), so tests bind to the contract.
- **FR-004**: All `src/` call sites (`utilities.py`, `xnat_experiment_data.py`,
  `xnat_resource_data.py`, `annotations/io_xnat.py`, `delete_contents_of_server.py`)
  MUST call the gateway; NO raw pyxnat outside the gateway module.
- **FR-005**: A conventions module MUST own every query string + label + resource
  name as functions/constants; call sites MUST use it.
- **FR-006**: `download_resource` / `list_files` MUST enumerate the resource's real
  files (pyxnat `CObject` / `Resource.get`) and MUST count-verify; the GUI download
  path MUST use them (replacing `app/logic/download.py` synthesized filename).
- **FR-007**: Whole-experiment (surgery) download MUST iterate scans/resources via
  the gateway and fetch all files (closes #25), fail-soft, cross-platform paths.
- **FR-008**: `create_assessor` MUST exist on the ABC + fake (seam for derived
  data); no existing data migrated.
- **FR-009**: The refactor MUST be behavior-preserving — the full existing suite
  passes unchanged (plus new tests), and the e2e sim still passes.
- **FR-010**: A spike `XnatpyGateway` MAY be added outside the default import path;
  it MUST NOT be wired into production this phase.
- **FR-011**: All paths remain offline-testable, PHI-free, fail-soft. No creds in
  argv/logs. Lost-update guard preserved.

### Key Entities
- **XnatGateway (ABC)**: the XNAT contract — every operation, no more.
- **PyxnatGateway**: production impl over pyxnat.
- **FakeGateway**: offline test impl (recorder + byte round-trip + failure inject).
- **conventions**: query-string + label + resource-name authority.
- **(spike) XnatpyGateway**: throwaway alt impl for evaluation.

### Success Criteria *(mandatory)*
- **SC-001**: Zero raw-pyxnat references in `src/` outside `xnat_gateway.py`
  (grep-enforced test).
- **SC-002**: 100% of the existing test suite passes unchanged against the
  ABC-bound fake; e2e sim still green.
- **SC-003**: All XNAT path strings + labels resolve from the conventions module
  (no inline `/project/...` f-strings in object classes).
- **SC-004**: Download of a scan with N files yields N files (was: 1 synthesized);
  whole-surgery download enumerates every scan (closes #25), with a count-verify test.
- **SC-005**: `create_assessor` seam has a passing fake test; a one-page xnatpy
  spike note exists with a go/no-go recommendation.
- **SC-006**: No change to on-server data layout for existing features (byte-diff
  test on representative writes).

### Assumptions
- The 16-call surface in `docs/XNAT_MODEL.md` is complete (verified by the codebase
  inventory). New methods (list/download/assessor) are additive.
- pyxnat stays the production client this phase; xnatpy is evaluation-only.
- FakeXNAT already covers the surface, so the refactor is guarded by existing tests.
