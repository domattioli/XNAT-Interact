# Feature Specification: Guided Novice-Safe UI (013)

**Feature Branch**: `013-guided-novice-ui`
**Created**: 2026-06-12
**Status**: Draft (spec + flagship implementation slice)
**Input**: Operator directive 2026-06-12 — "research, develop and implement a UI based on an
existing one that helps novices avoid confusing content and not make mistakes with complex data
handling and verification." Research basis: the **guided-wizard pattern** (Nielsen Norman Group,
PatternFly, ui-patterns.com), the **TurboTax interview pattern**, and **REDCap** clinical-research
data-capture conventions (the operator's own domain). Supersedes the flat-sidebar layout shipped
in spec 002 (Phase 2 Streamlit app). Builds on the live-app defects found in the PR #38 UI
screenshot pass (launcher, double-render, browse column mismatch — all fixed).

## Overview

The Phase-2 app presents eight equal sidebar buttons (Browse, Upload, Batch, Download,
Annotations, Learn, Onboarding, Log out) and a standalone tool behind each. For the target user —
a research student with no programming background, handling de-identified surgical imaging
infrequently and at high stakes — this is three failure modes at once: **decision paralysis**
(eight equal choices, no "what do I do first"), **wrong-tool risk** (nothing stops a download-
intent user from opening Upload), and **exposed complexity** (XNAT jargon, accession IDs, and an
all-fields-at-once intake form where a single mistake corrupts the archive or leaks PHI).

This feature replaces the tool-first layout with a **task-first guided flow** grounded in three
established UIs:

- **Wizard pattern** (NN/g): one subtask per step so the user "is less likely to miss important
  aspects and commits fewer errors"; an always-visible step rail; validation that gates advance;
  a review step before any commit.
- **TurboTax interview**: the complex intake form becomes a plain-language question sequence —
  "surgery" not "rfSessionData experiment", "images" not "DICOM scans" — asking only what is
  relevant at each step.
- **REDCap**: categorical inputs are dropdowns (no free-text typos), required fields block save,
  branching hides irrelevant fields, and values are range/consistency-checked at entry.

The redesign is **additive**: a new guided layer (`app/guided/`) and a new entrypoint
(`streamlit_guided.py`) reuse the tested, Streamlit-free `app/logic/*` functions. The existing
`app/` and its 1290 passing tests are untouched, so the two UIs can be compared side by side and
the guided one promoted once validated.

## Clarifications (design rulings, 2026-06-12)

1. **Task-first home, not tool-first sidebar.** The landing screen asks "What do you want to do?"
   and offers a small number of plain-language task cards (Add a surgery / Find & get surgeries /
   Work with annotations), each a single sentence. The eight-tool sidebar is removed from the
   novice's path; advanced tools remain reachable but are not the default surface.
2. **Every task is a wizard with a step rail.** Each task is a linear sequence of steps with a
   persistent progress indicator (step N of M, named steps). The user always knows where they
   are and what remains.
3. **One concern per step.** No screen asks for more than one coherent thing. The upload intake
   that was one dense form becomes: identify → details → images → privacy check → review →
   confirm.
4. **Forward motion is validated.** "Next" is disabled until the current step is valid; the block
   is explained inline in plain language at the point of entry (REDCap required-field model), not
   as a post-submit traceback.
5. **Irreversible and PHI-bearing actions get an explicit verification gate.** Before any upload,
   a dedicated **privacy-check step** presents the de-identification result as an affirmative
   checklist the user must actively confirm (extends the existing T014 pixel-review gate into the
   wizard); before the final commit, a **review step** shows exactly what will be sent. No
   irreversible action happens without a deliberate, informed click.
6. **Plain language and progressive disclosure.** Domain jargon is hidden by default; an optional
   "show technical detail" affordance reveals XNAT specifics for advanced users without imposing
   them on novices.
7. **Persistent context and status.** A header always shows who is logged in, which project is
   active, and a connection-OK indicator, so the user never acts against the wrong target or a
   dead connection.

## User Scenarios & Testing *(mandatory)*

### User Story 1 — A novice knows what to do within five seconds (Priority: P1)

A first-time student logs in and sees a short list of plain-language tasks, not eight tools, and
picks the one matching their intent without guessing.

**Independent Test**: render the home with an authenticated session; exactly the task cards are
present (no raw tool list); each card label is a plain-language sentence; selecting one enters
that task's wizard at step 1.

**Acceptance Scenarios**:
1. **Given** an authenticated session, **When** the home renders, **Then** the primary surface is
   the task cards, not the eight-button tool nav.
2. **Given** a task card, **When** selected, **Then** the corresponding wizard opens at step 1
   with the step rail visible.

### User Story 2 — The upload wizard prevents the common mistakes (Priority: P1)

The highest-stakes flow (de-identify + publish) is a guided wizard that makes the known failure
modes structurally impossible: no all-fields-at-once form, no advancing past a missing required
field, no upload without an affirmed privacy check and a reviewed summary.

**Independent Test** (offline, reusing `app/logic/upload` + the FakeXNAT seam): drive the wizard
step by step; advancing is blocked at each step until its required inputs are valid; the privacy
step cannot be passed without an explicit affirmation; the final commit is reachable only after
the review step; the underlying publish call receives exactly the assembled intake.

**Acceptance Scenarios**:
1. **Given** the details step with a required field empty, **When** the user clicks Next,
   **Then** advance is blocked with an inline plain-language reason and no exception.
2. **Given** the privacy-check step, **When** the affirmation control is unset, **Then** the
   commit step is unreachable.
3. **Given** a completed review step, **When** the user confirms, **Then** `app/logic/upload`'s
   publish runs once with the assembled form, and a success state (not a raw server response) is
   shown.
4. **Given** a publish failure, **When** it returns, **Then** a FriendlyError panel with recourse
   is shown (no traceback), and the user can retry without re-entering data.

### User Story 3 — Categorical inputs cannot be mistyped (Priority: P1)

Surgeon, institution, and procedure are chosen from the server registry, never free-typed, so the
class of typo errors that corrupt joins and dedup keys is eliminated (REDCap dropdown model).

**Acceptance Scenario**: **Given** the details step, **When** the registry-backed fields render,
**Then** they are selection controls populated from the project registry, with no free-text path
for a categorical value.

### User Story 4 — The user always knows where they are and what they are acting on (Priority: P2)

A persistent header shows identity, active project, and connection status on every screen; the
wizard shows step N of M with named steps.

**Acceptance Scenario**: **Given** any wizard step, **When** rendered, **Then** the header shows
username + project + connection indicator and the step rail shows the current named step.

### User Story 5 — Advanced detail is available but never imposed (Priority: P3)

Jargon and technical identifiers are hidden by default behind a "show technical detail" toggle.

**Acceptance Scenario**: **Given** a wizard step with underlying XNAT specifics, **When** the
toggle is off, **Then** only plain-language content shows; **When** on, **Then** the technical
identifiers appear.

### Edge Cases

- Connection drops mid-wizard → header indicator flips; the next validated action surfaces a
  FriendlyError, entered data is retained.
- A registry with zero surgeons/sites/procedures → the details step explains the project is not
  yet configured and routes to the Data Librarian, rather than showing empty dropdowns.
- Back-navigation never loses entered data (wizard state persists across steps).
- A novice who truly needs the raw tools → an explicit "advanced tools" affordance, off the
  default path.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: A task-first home replaces the eight-button sidebar as the post-login default;
  plain-language task cards map to wizards.
- **FR-002**: Each task is a step-rail wizard; the rail shows step N of M with named steps and is
  visible on every step.
- **FR-003**: No step presents more than one coherent concern; the upload intake is decomposed
  into identify → details → images → privacy → review → confirm.
- **FR-004**: "Next" is disabled until the current step validates; the reason is shown inline in
  plain language at point of entry; no validation surfaces as a traceback.
- **FR-005**: Categorical fields (surgeon, institution, procedure) are registry-backed selection
  controls with no free-text path.
- **FR-006**: A privacy-check step requires an explicit affirmation of the de-identification
  result before the commit step is reachable (wizard-integrated T014 gate).
- **FR-007**: A review step shows the full assembled submission before any commit; the commit is
  a single deliberate action with no irreversible effect before it.
- **FR-008**: All failures render as FriendlyError panels with recourse; entered data survives a
  failure for retry.
- **FR-009**: A persistent header shows username, active project, and connection status on every
  screen.
- **FR-010**: Technical/jargon detail is hidden by default behind a per-step "show technical
  detail" toggle.
- **FR-011**: The guided layer reuses `app/logic/*` only (no Streamlit in logic); the existing
  `app/` UI and its tests remain unchanged; a new entrypoint `streamlit_guided.py` launches the
  guided app with the same repo-root `sys.path` + page-discovery fix as `streamlit_app.py`.

### Key Entities

- **Task** — a plain-language user intent (add surgery / find surgeries / work with annotations)
  mapping to one wizard.
- **Wizard** — ordered named steps with per-step validators, persisted cross-step state, and a
  terminal commit.
- **Step** — one concern: a renderer, a validator returning ok|reason, and an optional technical-
  detail block.
- **VerificationGate** — a step whose validator requires an explicit affirmation (privacy) or a
  reviewed summary (commit).

### Success Criteria *(mandatory)*

- **SC-001**: A usability walkthrough (operator or proxy) reaches the correct task from the home
  without reading documentation.
- **SC-002**: In the upload wizard, every known mistake class is structurally blocked: cannot
  advance past a missing required field; cannot reach commit without affirming privacy; cannot
  commit without seeing the review — each covered by an offline test that fails on the old form.
- **SC-003**: Categorical inputs admit no free-text value (dropdown-only), verified by test.
- **SC-004**: The guided app launches via `streamlit run streamlit_guided.py` and renders the
  home + a full upload wizard against the live XNAT, proven by screenshots (the same Playwright
  harness used for the Phase-2 gallery).
- **SC-005**: Existing `app/` tests stay green (no regression in the parallel UI); guided-layer
  logic has its own offline tests.

### Assumptions

- `app/logic/*` (auth, upload, browse, download, onboarding) is the stable, tested seam; the
  guided UI is a presentation change, not a logic change.
- Streamlit remains the runtime; the step rail and cards are built from native Streamlit controls
  (no new heavy frontend dependency).
- The registry-backed dropdowns already exist in the Phase-2 upload page and are reused.

## Out of Scope (this feature)

- Replacing or deleting the Phase-2 `app/` UI (kept until the guided UI is validated and
  promoted).
- The batch-upload and annotation wizards beyond a shared shell + the upload flagship (follow-on
  slices once the pattern is ratified).
- A non-Streamlit frontend rewrite.
- Live interactive hosting (Streamlit Community Cloud / Spaces) — a deployment question tracked
  separately (#37); this feature ships the local guided app + static proof.
