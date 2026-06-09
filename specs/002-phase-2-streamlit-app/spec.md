# Feature Specification: Streamlit "See-Your-Data" App (Phase 2)

**Feature Branch**: `002-phase-2-streamlit-app`
**Created**: 2026-06-04
**Status**: Draft
**Input**: `docs/IMPROVEMENT_PLAN.md` Phase 2 (Streamlit "human-factors view of XNAT") + walking-skeleton note + cross-cutting refinements

## Overview

Give students a friendly, **browser-like local app** to see and manage their
XNAT data, so the terminal becomes optional. The app runs on the student's own
VPN-connected machine and opens in their browser; **PHI never leaves the machine
except to the XNAT server.** The GUI is a thin front-end over the **already-tested
Phase 1 service layer** (`src/services/`) — no upload/download/de-identify logic
is re-implemented here. Built as a **walking skeleton**: ship login → browse →
single upload (with the burned-in-PHI review step) first, validate with one real
student, then add batch, download, and the optional terminal panel. This is the
biggest skill-floor reduction in the plan (Constitution Principle III).

## User Scenarios & Testing *(mandatory)*

### User Story 1 - I log in, see my data, and upload one case (Priority: P1)

A student opens the app in their browser, gets a clear "are you on the VPN? are
you set up?" preflight, logs in with a secure field, sees a searchable table of
their cases on the server with thumbnail previews, then fills the intake form as a
real web form (dropdowns, date pickers, file/folder picker), reviews exactly what
will be de-identified and uploaded, confirms no burned-in PHI, and watches a
progress bar to completion. This is the **walking skeleton** — the full thin
slice that proves the whole shape works.

**Why this priority**: This is the core skill-floor win and the de-risking slice
from the plan. It must be validated end-to-end with one real student before any
other screen is built. It exercises login, browse, the intake form, and the
mandatory PHI review in one path.

**Independent Test**: Drive the login → browse → single-upload flow against
`FakeXNAT` with the UI logic factored into pure functions (form→`PendingUpload`,
table builder, preview builder); assert a case is published, the PHI-review gate
blocks upload until confirmed, and no real network call occurs. Fully offline.

**Acceptance Scenarios**:

1. **Given** the login screen, **When** the student is not on the VPN, **Then** a
   friendly preflight panel says "Can't reach the XNAT server — are you on the
   UIowa VPN?" with a Retry action, never a stack trace.
2. **Given** valid credentials, **When** the student logs in, **Then** the Browse
   screen shows a searchable, filterable table of their server data with thumbnail
   previews.
3. **Given** the single-upload form, **When** the student picks surgeon/site/
   procedure, **Then** the choices are dropdowns sourced from `ConfigTables`, not
   free-text fields, and dates use date pickers.
4. **Given** a staged upload, **When** the student proceeds, **Then** a "what will
   be uploaded / what PHI is removed" preview appears and the student MUST confirm
   "no visible PHI" before the upload starts.
5. **Given** a confirmed upload, **When** it runs, **Then** a progress bar advances
   and a success summary (subject, image count) is shown on completion.

---

### User Story 2 - Friendly errors everywhere, never a crash (Priority: P1)

Any failure on any screen (VPN drop, bad password, expired cert, bad file,
malformed form field) renders as a **friendly panel** with recourse — retry, skip,
save progress, or copy a diagnostic to send the Data Librarian — never a Python
traceback and never Streamlit's red exception box.

**Why this priority**: Constitution Principle II. A confusing crash in the new UI
would defeat the adoption mission as badly as the terminal does today. It is a
cross-screen guarantee, so it ranks alongside the skeleton.

**Independent Test**: Inject each failure (via `FakeXNAT` failure simulation and
invalid form inputs) into the UI's pure handler functions; assert each returns a
`FriendlyError` rendered as a panel with recourse, and that no exception propagates
to Streamlit's default handler.

**Acceptance Scenarios**:

1. **Given** any screen, **When** the `FakeXNAT` layer raises (timeout, bad auth,
   expired cert), **Then** the screen shows the Phase 1 `FriendlyError` as a panel,
   not the default Streamlit exception view.
2. **Given** a foreseeable input error (bad date, empty required field), **When**
   the student submits, **Then** the field is flagged inline with guidance and the
   form is not lost.

---

### User Story 3 - Browse: I can search, filter, and preview my cases (Priority: P1)

The Browse screen renders the server's data as a searchable, filterable table
(reusing the `print_preview_of_xnat_data` logic), with an image/thumbnail preview
so students *see* their cases instead of imagining them — the "human-factors view
of XNAT."

**Why this priority**: Seeing the data is the headline value of Phase 2 and the
selection surface that Download (P2) builds on. It pairs with the skeleton.

**Independent Test**: Build the table from canned `FakeXNAT` data via a pure
table-builder function; assert search/filter narrow rows correctly and that a
selected row yields a thumbnail-preview request. Offline.

**Acceptance Scenarios**:

1. **Given** server data, **When** the Browse screen loads, **Then** a table lists
   the student's subjects/experiments with the same columns as the CLI preview.
2. **Given** a search term or filter, **When** applied, **Then** the table narrows
   to matching rows without a page reload error.
3. **Given** a selected case, **When** chosen, **Then** a thumbnail/image preview is
   shown for that case.

---

### User Story 4 - Batch upload with per-row validation before sending (Priority: P2)

A student drags an `.xlsx` into the app, sees a **per-row validation table before
any upload**, fixes problems inline, then uploads with live progress and an
end-of-run summary (succeeded / skipped / failed + why), reusing Phase 1's
continue-on-error batch path.

**Why this priority**: High value but builds on the skeleton and the Phase 1 batch
resilience; sequenced after the thin slice is validated.

**Independent Test**: Feed a synthetic mixed valid/invalid xlsx; assert the
validation table flags bad rows pre-upload, an inline fix clears the flag, and the
run (vs `FakeXNAT`) reports exactly the expected succeeded/failed counts.

**Acceptance Scenarios**:

1. **Given** a dragged-in spreadsheet, **When** loaded, **Then** a validation table
   shows each row's status (OK / problem + which column) **before** any upload.
2. **Given** a flagged row, **When** the student edits the cell inline, **Then** the
   row re-validates and clears if fixed.
3. **Given** a validated batch, **When** uploaded, **Then** progress advances per
   row and a summary lists succeeded / skipped / failed with reasons.

---

### User Story 5 - Download selected cases to a folder (Priority: P2)

A student selects cases from the Browse table, picks a destination folder, and
watches progress as the data downloads, reusing Phase 1's reworked download path.

**Why this priority**: Rounds out the round-trip, but upload is the more common
need; download follows the skeleton and batch.

**Independent Test**: Select rows from canned `FakeXNAT` data, run the download via
the service layer against the fake, assert files land in the chosen folder and
progress completes; no network.

**Acceptance Scenarios**:

1. **Given** the Browse table, **When** the student selects one or more cases and a
   folder, **Then** download starts with a progress indicator.
2. **Given** a download in progress, **When** it completes, **Then** a summary shows
   what was retrieved and where it was saved.

---

### User Story 6 - Optional Terminal / Learn mode (Priority: P3)

A curious student can open an optional embedded terminal/"Learn" panel that shows
the equivalent CLI command for what they just did in the UI, so they can learn the
command-line way **by choice**. Default users never need it.

**Why this priority**: Pure enhancement, last in the walking skeleton; the
constitution requires the CLI stay *optional*, so this must never become a
required path.

**Independent Test**: Assert the panel is hidden by default, opt-in to show, and
that for a given UI action it renders the matching CLI command string (a pure
action→command mapping) without executing anything unprompted.

**Acceptance Scenarios**:

1. **Given** the app, **When** a default student uses it, **Then** no terminal is
   required or shown unless explicitly opened.
2. **Given** Learn mode is open, **When** the student performs an upload, **Then**
   the panel displays the equivalent CLI command for reference.

---

### Edge Cases

- **VPN drops mid-upload/download**: the progress panel shows a friendly "partial
  upload" message with cleanup/resume recourse (reusing Phase 1 detection), not a
  frozen bar or a traceback.
- **Browser tab closed / app restarted mid-session**: in-progress form data is not
  silently lost where it can be preserved; the student is not forced to re-enter a
  completed intake form from scratch.
- **Session-state stale after re-login**: switching users/credentials clears cached
  server data so one student never sees another's cached cases.
- **Large image folder / many cases**: thumbnails and the table load without
  blocking the whole UI (lazy/paged where needed); no PHI is cached to disk by the
  browser layer.
- **Not-yet-onboarded student**: login preflight surfaces the onboarding checklist
  (account / added-to-project / VPN) rather than failing with an auth error.
- **Drag-in of a non-xlsx or malformed spreadsheet**: friendly panel explaining the
  expected format, not a parser traceback.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The app MUST run **locally** on the student's machine and open in a
  browser; no PHI MAY be sent anywhere except the configured XNAT server.
- **FR-002**: The app MUST reuse the Phase 1 service layer (`src/services/`) for all
  upload, download, de-identification, config, and error handling; it MUST NOT
  re-implement that logic.
- **FR-003**: The Login screen MUST run a **VPN/server reachability preflight** and
  show an **onboarding-status hint** (account / added-to-project / VPN) before
  attempting authentication.
- **FR-004**: Credentials MUST be entered in a secure session-only field; the app
  MUST NOT accept credentials via command-line arguments, config files, or URLs,
  and MUST NOT log them (Constitution V; preserves Phase 1 FR-009).
- **FR-005**: Every error MUST render as a **friendly panel** built from the Phase 1
  `FriendlyError` (title, plain explanation, recourse actions: retry / skip / save /
  contact Librarian); the default Streamlit exception view MUST NOT reach the user.
- **FR-006**: The Browse screen MUST present server data as a **searchable,
  filterable table** (reusing `print_preview_of_xnat_data` logic) with an
  **image/thumbnail preview** for a selected case.
- **FR-007**: The single-upload screen MUST render the 34-question intake form as a
  web form with **dropdowns** for surgeon/site/procedure sourced from
  `ConfigTables`, **date pickers** for dates, and a **file/folder picker or
  drag-and-drop** for image input — no hand-typed paths.
- **FR-008**: Before any upload, the app MUST show a **"what will be uploaded / what
  PHI is removed" preview** and MUST require the Phase 1 **burned-in-PHI
  confirmation** (no visible patient name/date) before the upload starts (preserves
  Phase 1 FR-006; Constitution I).
- **FR-009**: Uploads and downloads MUST display a **progress indicator** and a
  clear end summary; failures during a transfer MUST surface as friendly panels with
  recourse.
- **FR-010**: The batch screen MUST show a **per-row validation table before any
  upload**, allow **inline fixes**, then run the Phase 1 continue-on-error batch
  path with a succeeded / skipped / failed summary.
- **FR-011**: The download screen MUST let the student select cases from the table,
  choose a destination folder, and run the Phase 1 download path with progress and a
  summary.
- **FR-012**: The optional Terminal / Learn panel MUST be **hidden by default**,
  opt-in only, and MUST NOT be required for any default workflow (Constitution III —
  CLI stays optional).
- **FR-013**: All Streamlit UI logic MUST be factored into **testable pure
  functions** (form→model, table builder, preview builder, action→command mapping),
  not buried in callbacks, and MUST be tested **offline against `FakeXNAT`**
  (Constitution IV).
- **FR-014**: The app MUST NOT cache PHI to disk via the browser/UI layer; staging
  and local PHI cleanup remain governed by the Phase 1 service layer (Constitution
  I).
- **FR-015**: Server URL and project name MUST come from the Phase 1 `AppConfig`
  (config file/env), not be hardcoded in the app (Constitution V).

### Key Entities

- **AppSession**: per-browser-session state — authenticated gateway handle, current
  user, cached server table, and current screen; cleared on logout/re-login.
- **PreflightStatus**: result of the login preflight — VPN/server reachable?
  onboarding steps (account / project / VPN) satisfied? — rendered as hints.
- **BrowseTable**: the searchable/filterable view-model of server data (rows +
  columns mirroring the CLI preview) plus selected-case thumbnail reference.
- **IntakeFormState**: the 34-question form values mapped to a Phase 1
  `PendingUpload`, with per-field validation status.
- **UploadPreview**: the pre-flight summary shown before sending — subject, image
  count, what-is-removed, and the PHI-review confirmation state.
- **BatchValidationTable**: per-row status (OK / problem + column + reason) shown
  before upload, editable inline, feeding the Phase 1 batch path.
- **FriendlyPanel**: the rendered form of a Phase 1 `FriendlyError` (title,
  explanation, recourse buttons, optional diagnostic-log path).

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A new student completes login → browse → single upload (with PHI
  review) **without opening a terminal or typing a file path by hand** — validated
  with one real student before later screens are built.
- **SC-002**: Zero raw tracebacks and zero default Streamlit exception views reach
  the user for the catalogued failures (VPN, auth, cert, bad path, malformed input,
  bad spreadsheet) — verified by tests.
- **SC-003**: No image can be uploaded through the UI without passing the burned-in-
  PHI review step (enforced by test, reusing the Phase 1 gate).
- **SC-004**: 100% of UI decision logic lives in pure functions tested offline
  against `FakeXNAT`; CI runs them with no network and no PHI.
- **SC-005**: All surgeon/site/procedure inputs are dropdowns sourced from
  `ConfigTables`; the single-upload form contains zero free-text path fields.
- **SC-006**: The batch screen flags every bad row **before** upload; a batch with N
  bad rows uploads the rest and reports exactly N failures with reasons.
- **SC-007**: The Terminal/Learn panel is absent from every default workflow; a
  student can complete all P1/P2 tasks without it.

## Assumptions

- **Phase 1 is a hard prerequisite**: `src/services/` (errors, config, preflight,
  deidentify, xnat_gateway) and `tests/fakes/fake_xnat.py` exist and are tested.
  Phase 2 builds strictly on top of them.
- **Streamlit** is the chosen tech (minimal code, maps to the existing pandas tables
  and forms); native desktop (PySide/Tkinter) was considered and rejected for higher
  UI-code cost.
- Streamlit's file/folder picker covers the student's machine only; on managed
  machines folder selection may fall back to a path field constrained to allowed
  locations — still no free-text *server* paths.
- The 34-question intake form's field set and the `ConfigTables` dropdown sources are
  unchanged from the CLI; the UI re-renders them, it does not redefine them.
- Validation-with-one-real-student is a process gate the maintainer runs after the
  skeleton lands, before batch/download/terminal are built.
- The app is launched however delivery lands (dev `pip`, signed installer, or ITS
  Software Center package per Phase 3); Phase 2 does not depend on the delivery
  mechanism.
