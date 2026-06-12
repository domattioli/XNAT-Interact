# Implementation Plan: Streamlit “See-Your-Data” App (Phase 2)

**Branch**: `002-phase-2-streamlit-app` | **Date**: 2026-06-04 | **Spec**: [spec.md](spec.md)

## Summary

Build a **local Streamlit app** that runs on the student's VPN-connected machine
and opens in a browser, giving a friendly **login → browse → upload → download**
experience over the **already-tested Phase 1 service layer** (`src/services/`).
The GUI renders existing logic; it does not re-implement it. Deliver as a
**walking skeleton**: the single-upload slice (with the burned-in-PHI review)
first, validated with one real student, then batch, download, and an optional
Terminal/Learn panel. All screen logic lives in **plain, testable functions**
exercised against `FakeXNAT` so the app is offline-testable with no network and
no PHI.

## Technical Context

**Language/Version**: Python 3.11 (CI); same interpreter as Phase 1.
**Primary Dependencies**: `streamlit` (NEW), plus the Phase 1 stack reused via
`src/services/` (`pyxnat`, `pydicom`, `opencv-python`, `pandas`, `pytest`). No
other heavy deps. Streamlit chosen for minimal code over pandas tables/forms;
native desktop (PySide/Tkinter) considered and rejected (more UI code).
**Storage**: XNAT server (remote) + Phase 1 `AppConfig` for URL/project; local
temp for staged uploads/downloads (PHI cleaned up post-upload).
**Testing**: `pytest`, offline, against `tests/synthetic_data.py` + the Phase 1
`FakeXNAT`. Streamlit logic factored into pure functions so it is unit-testable
without launching the server or a browser.
**Target Platform**: Windows + macOS desktops (UIowa managed + BYOD); runs as a
**local browser app** (`streamlit run`, or the Phase 3 packaged launcher).
**Project Type**: Single-project; a thin GUI layer (`app/`) over `src/services/`.
**Constraints**: No network in tests; no PHI in repo/logs; no credentials in
argv; PHI leaves the machine only to the configured XNAT server.
**Scale/Scope**: 5 screens (Login, Browse, Upload-single, Upload-batch,
Download) + 1 optional Terminal/Learn panel; one shared session/auth state.

## Constitution Check

*GATE: must pass before and after design.*

- **I — PHI Safety**: FR-007 (pre-upload “what will be uploaded / PHI-removed”
  preview + mandatory burned-in-PHI confirmation, reusing Phase 1), FR-015 (local
  PHI cleanup), thumbnails rendered from de-identified data only. ✅
- **II — Fail Softly**: FR-011 (every error → friendly panel with recourse,
  never a traceback), FR-003 (onboarding hints), edge cases (VPN-drop, empty
  server). ✅ Central.
- **III — Lower Skill Floor**: The whole phase — browser app, dropdowns from
  `ConfigTables`, file/folder picker, date pickers; terminal becomes optional
  (FR-012). This is the headline win. ✅ Central.
- **IV — Offline Testable**: FR-013 (screen logic in plain functions tested vs
  `FakeXNAT`), SC-004. No live server/VPN/PHI in CI. ✅
- **V — Config/No Secrets**: FR-004 (secure field, never argv/log), FR-014
  (URL/project from `AppConfig`). ✅
- **VI — Data Integrity**: FR-008 (progress + verified summary), FR-009 (batch
  continue-on-error + failed-only re-run via Phase 1 engine), FR-010
  (cross-platform download), edge case (config lost-update guard surfaced). ✅

No violations. No complexity-tracking exceptions needed. **Hard prerequisite:**
Phase 1 (`src/services/`, `FakeXNAT`, PHI review) must be shipped first.

## Project Structure

```text
specs/002-phase-2-streamlit-app/
├── spec.md
├── plan.md      # this file
└── tasks.md

app/                          # NEW: thin Streamlit front-end over src/services/
├── main.py                   # page router (launched from repo-root streamlit_app.py)
├── session.py                # AppSession state, auth handle, screen routing helpers
├── components/
│   ├── error_panel.py        # renders a Phase 1 FriendlyError as a friendly panel
│   ├── data_table.py         # searchable/filterable table (wraps preview data)
│   └── progress.py           # progress-bar + summary helpers
└── pages/
    ├── login.py              # VPN preflight + onboarding hints + secure login
    ├── browse.py             # server-data table + thumbnail preview
    ├── upload_single.py      # 34-Q intake form, PHI-review gate, progress
    ├── upload_batch.py       # xlsx drop, per-row validation table, fix, run
    ├── download.py           # pick cases, choose folder, progress
    └── terminal.py           # OPTIONAL Terminal / Learn panel (P3)

src/services/                 # REUSED from Phase 1 (NOT modified by this phase)
├── errors.py  config.py  preflight.py  deidentify.py  xnat_gateway.py

tests/
├── fakes/fake_xnat.py        # REUSED Phase 1 double
├── synthetic_data.py         # REUSED (burned-in image, mixed-validity xlsx)
├── test_app_login_onboarding.py     # NEW: VPN/auth/cert/onboarding panels
├── test_app_browse.py               # NEW: table filter/search + thumbnail
├── test_app_upload_single.py        # NEW: form options, PHI gate, progress
├── test_app_upload_batch.py         # NEW: pre-upload validation + re-run
├── test_app_download.py             # NEW: select → folder → cross-platform
└── test_app_terminal_optional.py    # NEW: hidden by default, never required
```

## Approach (walking-skeleton order)

1. **Foundation**: `app/main.py` router, `app/session.py` shared auth/screen
   state, `app/components/error_panel.py` (wraps Phase 1 `FriendlyError`). Wire
   the app to `src/services/` via the gateway — no screen logic yet.
2. **US1 + US2 (the skeleton)**: Login (VPN preflight + onboarding hints + secure
   field), Browse (table + thumbnail), Upload-single (intake form with
   `ConfigTables` dropdowns + date pickers + picker, the **PHI-review gate**,
   progress). Every error rendered through the friendly panel. **Demo this slice
   to one real student before continuing.**
3. **US3**: Upload-batch — drop xlsx, per-row validation table *before* upload,
   inline fix, run via the Phase 1 continue-on-error engine, summary +
   failed-only re-run.
4. **US4**: Download — select from the Browse table, choose folder, cross-platform
   paths, progress + verified summary.
5. **US5**: Optional Terminal / Learn panel — hidden by default; shows the
   equivalent CLI command for a GUI action.

Each step factors its logic into plain functions in `app/pages/*` (or helpers)
and lands with offline tests against `FakeXNAT` before the next begins.

## Risks & Mitigations

- *Logic buried in Streamlit callbacks (untestable)*: enforce FR-013 — every
  screen splits into a pure `*_logic()` function (takes gateway + inputs, returns
  a `ScreenResult`) and a thin `render()` that only draws. Tests target the logic.
- *Phase 1 not actually ready*: Phase 2 is **blocked** on Phase 1; if the service
  seam shifts, the GUI breaks — pin to the Phase 1 gateway/`FriendlyError`/config
  interfaces and treat their contract tests as a prerequisite gate.
- *Streamlit session-state surprises on refresh/re-run*: keep all state in
  `app/session.py` with explicit keys; test the half-filled-form and
  no-double-upload paths.
- *Thumbnail PHI leak*: render previews only from de-identified pixels (post Phase
  1 de-id); never preview raw source images.
- *Scope creep past the skeleton*: gate batch/download/terminal behind the
  one-real-student validation of the single-upload slice (SC-007).
