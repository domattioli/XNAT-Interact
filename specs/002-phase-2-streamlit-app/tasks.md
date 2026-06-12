# Tasks: Streamlit “See-Your-Data” App (Phase 2)

**Input**: [spec.md](spec.md), [plan.md](plan.md)
**Prerequisites**: **Phase 1 shipped** — `src/services/` (errors, config,
preflight, deidentify, xnat_gateway), `tests/fakes/fake_xnat.py`, and the
burned-in-PHI review step. Tests REQUIRED (Constitution IV).

## Format: `[ID] [P?] [Story] Description`

- **[P]**: can run in parallel (different files, no dependency)
- **[Story]**: US1–US5 (see spec) or FN = foundational

## Phase A: Foundational — app scaffold & wiring (BLOCKS everything)

**⚠️ No user-story screen work begins until A is complete.**

- [ ] T001 [FN] Create `app/__init__.py`, `app/components/__init__.py`,
  `app/pages/__init__.py`; add `streamlit` to deps/`environment.yml`.
- [ ] T002 [FN] Build `app/session.py`: `AppSession` (authenticated gateway
  handle, current screen, staged `UploadDraft`, selected cases), with explicit
  Streamlit session-state keys and no credential retained after login.
- [ ] T003 [FN] Build `app/main.py`: `streamlit run` entrypoint + page router
  that dispatches to `app/pages/*` based on `AppSession.current_screen`.
- [ ] T004 [FN] Build `app/components/error_panel.py`: render a Phase 1
  `FriendlyError` (title, message, recourse[], diagnostic path) as a friendly
  panel — the single sink so **no screen ever shows a traceback** (FR-011).
- [ ] T005 [FN] [P] Build `app/components/data_table.py` (searchable/filterable
  table over preview data) and `app/components/progress.py` (progress + summary).
- [ ] T006 [FN] Wire the app to `src/services/` only (gateway + `AppConfig`); the
  app layer imports no hardcoded URL/project and no `pyxnat` directly (FR-002,
  FR-014). Smoke: app boots against `FakeXNAT` with no network.

## Phase B: US1 + US2 — the walking skeleton (P1)

**⚠️ Demo this slice to one real student before Phase C (SC-007).**

- [ ] T007 [US2] Build `app/pages/login.py` `login_logic(gateway, creds)`: VPN/
  server preflight (Phase 1 `preflight`), secure credential field only (no argv/
  log, FR-004), returns a `ScreenResult` (success handle or `FriendlyError`).
- [ ] T008 [US2] Add `OnboardingStatus` to login: account / project-access / VPN
  checks + optional pre-drafted Librarian access-request email (FR-003); render
  hints panel.
- [ ] T009 [US2] Map expired-cert / VPN-down / bad-auth to friendly panels via
  `error_panel` (reusing Phase 1 `FriendlyError`); no exception reaches render.
- [ ] T010 [US1] Build `app/pages/browse.py` `browse_logic(gateway, filters)`:
  build the searchable/filterable case table reusing the
  `print_preview_of_xnat_data` logic (FR-005); empty-server friendly state.
- [ ] T011 [US1] Add selected-case **thumbnail preview** in `browse.py`, rendered
  from **de-identified** image data only; lazy/on-selection load (FR-005, edge).
- [ ] T012 [US1] Build `app/pages/upload_single.py` form: 34-Q intake as a web
  form with **`ConfigTables` dropdowns** (surgeon/site/procedure), **date
  pickers**, **file/folder picker or drag-drop** (FR-006).
- [ ] T013 [US1] Add the **PHI-review gate**: “what will be uploaded / what PHI
  was removed” preview + mandatory burned-in-PHI confirmation (Phase 1 review) —
  upload blocked until confirmed (FR-007). Build an `UploadDraft` → Phase 1
  `PendingUpload`.
- [ ] T014 [US1] Run the single upload through the Phase 1 gateway with a
  **progress bar** + success/failure summary (FR-008); cleanup local PHI on
  success (FR-015).
- [ ] T015 [US1] [P] `tests/test_app_login_onboarding.py`: VPN-down / bad-auth /
  expired-cert / missing-project via `FakeXNAT` → friendly panel + recourse, no
  traceback (SC-002).
- [ ] T016 [US1] [P] `tests/test_app_browse.py`: table search/filter renders the
  fake's cases; thumbnail appears on selection; empty-server state.
- [ ] T017 [US1] `tests/test_app_upload_single.py`: form options come from
  `ConfigTables`; PHI gate **blocks upload until confirmed** (SC-003); confirmed
  upload records expected gateway calls; local PHI cleaned up.

## Phase C: US3 — batch upload with pre-upload validation (P2)

- [ ] T018 [US3] Build `app/pages/upload_batch.py`: drag-in xlsx →
  `BatchValidationTable` (row, column, reason, fixed?) shown **before** any upload
  (FR-009).
- [ ] T019 [US3] Add **inline fix**: editing a flagged cell re-validates and
  clears the flag without re-uploading the sheet.
- [ ] T020 [US3] Run validated rows through the **Phase 1 continue-on-error
  engine** with live progress, end-of-run summary, and **failed-only re-run**.
- [ ] T021 [US3] [P] `tests/test_app_upload_batch.py` (vs `FakeXNAT`, synthetic
  mixed-validity xlsx): exactly the bad rows flagged pre-upload; inline fix clears
  a flag; good rows succeed; summary + failed-only re-run target the failures
  (SC-005).

## Phase D: US4 — download to a folder (P2)

- [ ] T022 [US4] Build `app/pages/download.py`: select cases from the Browse
  table, choose a destination folder, run the Phase 1 download with
  **cross-platform paths** and progress (FR-010, FR-008).
- [ ] T023 [US4] [P] `tests/test_app_download.py` (vs `FakeXNAT`, temp folder):
  selected cases land as files; paths correct on Windows + macOS; progress →
  verified summary.

## Phase E: US5 — optional Terminal / Learn panel (P3)

- [ ] T024 [US5] Build `app/pages/terminal.py`: hidden by default; when enabled,
  shows the **equivalent CLI command** for a completed GUI action (FR-012).
- [ ] T025 [US5] [P] `tests/test_app_terminal_optional.py`: panel hidden by
  default and **never required** by any flow; toggled panel renders the right
  command string.

## Phase F: Polish & cross-cutting

- [ ] T026 [P] Edge case: VPN drop mid-upload → friendly “connection lost” panel
  with resume/cleanup recourse (test via `FakeXNAT` failure injection).
- [ ] T027 [P] Session-state robustness: browser refresh / re-run mid-flow does
  not lose a half-filled form or double-fire an upload (test the logic functions).
- [ ] T028 [P] Surface the Phase 1 config lost-update guard when a registration
  via the form would clobber concurrent `MetaTables.json` edits.
- [ ] T029 Add a short “how to launch the app” section to `README.md`
  (`streamlit run streamlit_app.py`); keep it terminal-light, pending Phase 3 packaging.
- [ ] T030 Run full suite; ensure SC-001…SC-007 met — no traceback reaches the
  browser, all screen logic covered offline against `FakeXNAT`, PHI gate enforced.

## Dependencies

- **Phase 1 (whole)** blocks Phase A — `src/services/` + `FakeXNAT` + PHI review
  must exist first.
- Phase A blocks B–F (scaffold, session, router, error panel, gateway wiring).
- T010–T011 (Browse table) precede T022 (Download selects from that table).
- T012–T013 (form + PHI gate) precede T014 (upload run).
- **Phase B (US1+US2) must be validated with one real student before Phase C**
  (SC-007) — the walking-skeleton gate.
- `[P]` tasks within a phase touch different files and may run concurrently.
