# Feature Specification: Reliability & Safe Failure (Phase 1)

**Feature Branch**: `001-phase-1-reliability-and-safe-failure`
**Created**: 2026-06-04
**Status**: Draft
**Input**: `docs/IMPROVEMENT_PLAN.md` Phase 1 + Safety/Security/Reliability refinements

## Overview

Make the existing terminal tool stop failing in confusing ways, close the most
important PHI-safety gap, and separate the core upload/download/de-identify logic
from the terminal so it is (a) testable offline and (b) reusable by the future
GUI. **No GUI in this phase.** This is the backend foundation every later phase
sits on.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - A failure tells me what to do next (Priority: P1)

A student hits a problem (no VPN, wrong password, bad folder, expired server
cert, malformed date). Instead of a Python traceback or a silent hang, they see a
plain-language explanation and concrete options: try again, skip this item, save
progress, or copy a diagnostic to send the Data Librarian.

**Why this priority**: Confusing crashes are the top reason students abandon the
tool. This is the core of the "human-in-the-loop" mission and unblocks trust in
everything else.

**Independent Test**: Drive each known failure mode (via the fake-XNAT layer and
monkeypatched input) and assert a friendly message + recourse is produced, and
that no bare traceback escapes. Fully offline.

**Acceptance Scenarios**:

1. **Given** the VPN is down, **When** the student tries to connect, **Then** they
   see "Can't reach the XNAT server — are you on the UIowa VPN?" with a Retry
   option, not an SSL/connection traceback.
2. **Given** a wrong password, **When** login is attempted, **Then** they are told
   the credentials failed and offered to re-enter, without the process exiting.
3. **Given** the server certificate has expired, **When** connecting, **Then** the
   message names the expiry and says to contact the Data Librarian.
4. **Given** any unexpected error, **When** it occurs, **Then** a diagnostic log
   (PHI-free) is written and its path is shown with "send this to the Librarian."

---

### User Story 2 - The server logic is testable without the server (Priority: P1)

A developer can run and test every upload/download/connect path against a
**fake XNAT** stand-in — no VPN, no live server, no PHI — in CI.

**Why this priority**: This is the enabling seam. Stories 1, 3, 4, 5 each touch
the server; none can be safely built or verified without a fake to test against.
It is the bridge that makes the rest of Phase 1 (and Phase 2) buildable in bulk.

**Independent Test**: A `FakeXNAT` (implementing the subset of the `pyxnat`
Interface the app uses) is injected; existing flows run end-to-end against it and
assertions check behavior. CI runs these with no network.

**Acceptance Scenarios**:

1. **Given** the fake-XNAT layer, **When** an upload flow runs in a test, **Then**
   it completes without any real network call.
2. **Given** the fake records calls, **When** a test publishes a subject, **Then**
   the test can assert the expected create/put calls were made.

---

### User Story 3 - Burned-in pixel PHI is reviewed before upload (Priority: P1)

Before any image is uploaded, the student is shown what will be sent and must
confirm there is no visible patient name/date burned into the pixels, with a
simple way to redact (black-box) a region.

**Why this priority**: Closes the single most important safety gap (Finding D):
de-identification currently scrubs metadata only. Constitution Principle I makes
this mandatory for any data leaving the machine.

**Independent Test**: Feed a synthetic image with simulated burned-in text; assert
the flow blocks upload until confirmation, and that an applied redaction modifies
the pixels (the existing `known_issue` test flips to asserting redaction).

**Acceptance Scenarios**:

1. **Given** images staged for upload, **When** the student proceeds, **Then** they
   must explicitly confirm "no visible PHI" before the upload starts.
2. **Given** a redaction is applied to a region, **When** the image is uploaded,
   **Then** the uploaded pixels have that region masked.

---

### User Story 4 - One bad row doesn't sink the batch (Priority: P2)

A batch spreadsheet with one malformed row uploads every good row, reports the
failures clearly (row #, column, reason), and lets the student re-run only the
failed rows.

**Why this priority**: Today one bad cell aborts the whole batch
(`batch_upload.py:530`) — a major source of lost work — but it affects the batch
path specifically, so it ranks below the universal failure/safety stories.

**Independent Test**: Build a synthetic xlsx with mixed valid/invalid rows; run
upload against fake-XNAT; assert valid rows succeed, invalid are reported, and a
"failed-only" re-run targets just those.

**Acceptance Scenarios**:

1. **Given** a 10-row batch with 2 bad rows, **When** uploaded, **Then** 8 succeed
   and a summary lists the 2 failures with reasons.
2. **Given** a completed run with failures, **When** the student chooses re-run,
   **Then** only the previously failed rows are attempted.

---

### User Story 5 - Safe credentials, config, and destructive actions (Priority: P2)

Credentials are never passed on the command line; server URL/project live in
config; the project-wipe script requires explicit confirmation and a dry-run.

**Why this priority**: Removes a credential-leak vector and a footgun, and makes
test-vs-prod switchable. Important hardening, lower day-to-day visibility than
P1.

**Independent Test**: Assert `--password` is gone; config is read from a file;
the delete script refuses to run without a typed confirmation and supports
`--dry-run` (tested with the fake-XNAT layer).

**Acceptance Scenarios**:

1. **Given** the CLI, **When** invoked, **Then** there is no `--password` option
   and no credential is read from argv.
2. **Given** the delete script, **When** run without confirmation, **Then** it
   performs no deletion; **When** run with `--dry-run`, **Then** it lists what it
   *would* delete and deletes nothing.

---

### Edge Cases

- VPN drops **mid-upload**: partial subject on server → the tool detects, explains,
  and offers cleanup/resume rather than leaving silent partial state.
- Concurrent config edits: two students editing `MetaTables.json` → detect the
  conflict and avoid a silent lost update (full fix may extend into a follow-up;
  at minimum, do not clobber silently).
- A file with no extension handed to the uploader: classified by content (DICM
  magic bytes), not by filename.
- Empty/whitespace inputs and unparseable dates: re-prompt with guidance, never
  crash.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST route all errors through a single helper that emits a
  plain-language message + recourse (retry / skip / save / contact Librarian) and
  writes a PHI-free diagnostic log.
- **FR-002**: System MUST replace every bare `except:` (6 sites) with specific
  handling that preserves the original error.
- **FR-003**: System MUST NOT use `assert` for user-facing validation; foreseeable
  problems raise friendly, recoverable errors.
- **FR-004**: System MUST run **preflight checks** (server reachable, credentials
  valid, paths exist) before long operations and fail early with a clear message.
- **FR-005**: System MUST provide a `FakeXNAT` test double implementing the subset
  of the `pyxnat` interface used by the app, injectable in place of the real
  connection.
- **FR-006**: System MUST, before uploading any image, require explicit
  confirmation that no visible (burned-in) PHI is present, and MUST support
  applying a redaction mask that is reflected in the uploaded pixels.
- **FR-007**: System MUST delete local PHI artifacts (intake forms, zips,
  downloaded config) after a successful upload, or clearly state where they remain.
- **FR-008**: Batch upload MUST process all rows, continue past failures, produce a
  per-row summary (row, column, reason), and support a failed-only re-run.
- **FR-009**: System MUST NOT accept credentials via command-line arguments; the
  `--password` flag MUST be removed.
- **FR-010**: Server URL and project name MUST be read from configuration, not
  hardcoded in source.
- **FR-011**: `delete_contents_of_server.py` MUST require an explicit typed
  confirmation, support `--dry-run`, and MUST NOT swallow deletion failures.
- **FR-012**: File-type detection MUST identify DICOM by content (DICM marker), not
  by file extension.
- **FR-013**: SSL-certificate expiry MUST surface as a dated, actionable message
  naming the Data Librarian as the contact.
- **FR-014**: Every server-touching change MUST ship with tests that run offline
  against `FakeXNAT`; every image change MUST ship a PHI-removal test.

### Key Entities

- **FriendlyError**: a problem expressed for a non-developer — title, plain
  explanation, list of recourse actions, optional diagnostic-log path.
- **FakeXNAT**: in-memory stand-in for the XNAT server; records calls, returns
  canned project/subject/experiment data, simulates failures (timeout, bad auth,
  expired cert).
- **PendingUpload**: the staged data for one case (images + intake metadata) plus
  its de-identification/redaction review state.
- **BatchRunResult**: per-row outcomes (succeeded / skipped / failed + reason),
  enabling a failed-only re-run.
- **AppConfig**: server URL, project name, environment — loaded from a config file.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Zero raw tracebacks reach the user for the catalogued foreseeable
  failures (VPN, auth, cert, path, malformed input) — verified by tests.
- **SC-002**: 100% of server-touching code paths have an offline test against
  `FakeXNAT`; CI runs them with no network.
- **SC-003**: No image can be uploaded without passing the burned-in-PHI review
  step (enforced by test).
- **SC-004**: A batch with N bad rows uploads the other rows and reports exactly N
  failures with reasons; failed-only re-run attempts exactly those N.
- **SC-005**: No credential is readable from argv/process list; no server
  URL/project name remains hardcoded in `src/`.
- **SC-006**: Zero bare `except:` remain in `src/`.

## Assumptions

- The `pyxnat` surface the app uses is small enough to fake faithfully (select,
  project/subject/experiment, get/put, where-queries).
- Pixel redaction in this phase is **manual** (human-in-the-loop); OCR-assisted
  detection is a later enhancement, not required here.
- A full distributed-lock fix for the config race may be split into a follow-up;
  this phase must at least prevent *silent* lost updates.
- The maintainer/Data Librarian remains the only runner of the delete script.
