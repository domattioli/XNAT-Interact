# Tasks: Reliability & Safe Failure (Phase 1)

**Input**: [spec.md](spec.md), [plan.md](plan.md)
**Prerequisites**: Phase 0 test harness (shipped). Tests REQUIRED (Constitution IV).

## Format: `[ID] [P?] [Story] Description`

- **[P]**: can run in parallel (different files, no dependency)
- **[Story]**: US1–US5 (see spec) or FN = foundational

## Phase A: Foundational — the test seam (BLOCKS everything)

**⚠️ No user-story work begins until A is complete.**

- [ ] T001 [FN] Create `src/services/__init__.py` and `tests/fakes/__init__.py`.
- [ ] T002 [FN] Build `src/services/xnat_gateway.py`: a thin wrapper exposing the
  exact `pyxnat` operations the app uses (connect, select project/subject/
  experiment, get/put, where-query). All real XNAT calls route through it.
- [ ] T003 [FN] Build `tests/fakes/fake_xnat.py` (`FakeXNAT`): in-memory double of
  the gateway surface; records calls; canned project/subject data; can simulate
  timeout, bad auth, expired cert.
- [ ] T004 [FN] [P] Add a contract checklist comment in `fake_xnat.py` enumerating
  the gateway methods it must mirror.
- [ ] T005 [FN] `tests/test_xnat_gateway_fake.py`: characterization tests proving a
  representative upload/download/connect path runs end-to-end against FakeXNAT
  with no network.

## Phase B: US1 — Friendly failures (P1)

- [ ] T006 [US1] Create `src/services/errors.py`: `FriendlyError` (title, message,
  recourse[], diagnostic_log_path) + `handle()` that writes a **PHI-free** log to
  a temp path and returns the user-facing text.
- [ ] T007 [US1] Create `src/services/preflight.py`: checks for server
  reachability, valid credentials, and path existence; each returns a
  `FriendlyError` on failure.
- [ ] T008 [US1] Replace all 6 bare `except:` in `src/` with specific handlers
  that preserve the original error and route through `errors.handle()`
  (`utilities.py:497`, `batch_upload.py:191,275`, `delete_contents_of_server.py:15,23`,
  `xnat_experiment_data.py:190`).
- [ ] T009 [US1] Replace user-facing `assert`s (login, paths, dates, inputs) with
  friendly raises; keep `assert` only for true invariants/bugs.
- [ ] T010 [US1] Map SSL-cert-expiry exception → dated, "contact the Data
  Librarian" `FriendlyError` (`XNATConnection`).
- [ ] T011 [US1] [P] `tests/test_friendly_errors.py` + `tests/test_preflight.py`:
  drive VPN-down, bad-auth, expired-cert, bad-path, malformed-date via FakeXNAT /
  monkeypatched input; assert friendly message + recourse, no traceback escapes.

## Phase C: US3 — Burned-in PHI review (P1)

- [ ] T012 [US3] Extract a pure `deidentify_dataset(ds, redacted_string)` into
  `src/services/deidentify.py`; have `SourceDicomDeIdentified` call it. Run the
  existing de-id tests before/after to prove no change in metadata scrubbing.
- [ ] T013 [US3] Add a pixel-review API in `deidentify.py`:
  `needs_pixel_review(img)` (always true for now) and `apply_redaction(img, boxes)`.
- [ ] T014 [US3] Insert a mandatory pre-upload confirmation gate in the upload flow:
  no image uploads until "no visible PHI" is confirmed; applied redactions modify
  the pixels that get uploaded.
- [ ] T015 [US3] Extend `tests/synthetic_data.py` with a burned-in-text image
  generator (cv2.putText into the pixel array).
- [ ] T016 [US3] Convert `test_burned_in_pixel_phi_is_not_removed` (currently
  `known_issue`) into `test_redaction_masks_burned_in_phi`: assert a redaction
  changes the pixels and the gate blocks upload without confirmation.

## Phase D: US4 — Batch continue-on-error (P2)

- [ ] T017 [US4] Refactor `batch_upload.upload_sessions` to process every row,
  catch per-row failures, and NOT abort on first error (remove the all-or-nothing
  gate at `batch_upload.py:530`).
- [ ] T018 [US4] Produce a `BatchRunResult` summary (row, column, reason per
  outcome) and persist it so a **failed-only re-run** can target just those rows.
- [ ] T019 [US4] [P] Extend `tests/synthetic_data.py` with a mixed valid/invalid
  batch xlsx generator.
- [ ] T020 [US4] `tests/test_batch_continue_on_error.py` (vs FakeXNAT): N bad rows →
  others succeed, exactly N reported, failed-only re-run attempts exactly those N.

## Phase E: US5 — Credentials, config, destructive ops (P2)

- [ ] T021 [US5] Remove the `--password` argument and all argv credential reads
  from `main.py`; prompt only (existing `pwinput`/secure prompt).
- [ ] T022 [US5] Create `src/services/config.py` (`AppConfig`): load server URL +
  project name from a config file/env; replace hardcoded values in
  `utilities.py:85-86` and `main.py:264+`.
- [ ] T023 [US5] [P] `tests/test_config.py`: config drives URL/project; switching
  test↔prod requires no code edit.
- [ ] T024 [US5] Harden `delete_contents_of_server.py`: require typed confirmation,
  add `--dry-run` (lists, deletes nothing), stop swallowing failures.
- [ ] T025 [US5] [P] `tests/test_delete_guard.py` (vs FakeXNAT): no-confirm = no
  deletion; `--dry-run` lists only.
- [ ] T026 [US5] Replace `is_dicom` extension check with DICM-magic-byte detection
  (`xnat_scan_data.py:67`); flip `test_extensionless_file_is_treated_as_dicom_footgun`.
- [ ] T027 [US5] Add local PHI cleanup after successful upload (intake forms, zips,
  downloaded config); test that temp PHI is gone post-upload.

## Phase F: Polish & cross-cutting

- [ ] T028 [P] Edge case: detect VPN drop mid-upload → friendly "partial upload"
  message with cleanup/resume offer (test via FakeXNAT failure injection).
- [ ] T029 [P] Config race: detect concurrent `MetaTables.json` edits and refuse to
  silently clobber; open a follow-up issue if full locking is deferred.
- [ ] T030 Update `README.md` (short, friendly) and retire/fix `update_and_test.py`
  (correct default branch; stop claiming it runs tests).
- [ ] T031 Run full suite; ensure zero bare `except:` remain in `src/` and
  SC-001…SC-006 are met.

## Dependencies

- Phase A blocks B–F (the seam).
- T012 (extract de-id) precedes T013–T016.
- T021–T022 precede T030 (README reflects new invocation).
- `[P]` tasks within a phase touch different files and may run concurrently.
