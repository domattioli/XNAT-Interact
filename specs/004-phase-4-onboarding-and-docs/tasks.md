# Tasks: Onboarding & Static Docs Site (Phase 4)

**Input**: [spec.md](spec.md), [plan.md](plan.md)
**Prerequisites**: Phase 0 test harness (shipped). Phase 1 `src/services/`
(`preflight`, `errors`, `config`, `xnat_gateway`, `FakeXNAT`) and Phase 2 login
screen are the seams reused here. Tests REQUIRED (Constitution IV).

## Format: `[ID] [P?] [Story] Description`

- **[P]**: can run in parallel (different files, no dependency)
- **[Story]**: US1–US3 (see spec) or FN = foundational

## Phase A: Foundational — onboarding-status + metrics modules (BLOCKS stories)

**⚠️ No user-story work begins until A is complete.**

- [ ] T001 [FN] Create `src/services/onboarding.py` and `src/services/metrics.py`
  module skeletons (with `OnboardingStatus`, `AccessRequest`, `MetricEvent`
  dataclasses) and `tests/` placeholders.
- [ ] T002 [FN] In `onboarding.py`, define the three status values
  (done / not-done / unknown) and the `OnboardingStatus` shape (per-item status +
  next-step string); no probe logic yet, just the data model the UI binds to.
- [ ] T003 [FN] In `metrics.py`, define the **field allowlist** (event kind, coarse
  timestamp, success/fail outcome, duration bucket) and an opt-in flag; the write
  boundary MUST reject any record carrying a non-allowlisted key.
- [ ] T004 [FN] [P] `tests/test_onboarding_status.py` skeleton: assert the model
  enumerates all three states and exposes a next-step for not-done/unknown.
- [ ] T005 [FN] [P] `tests/test_metrics_phi_free.py` skeleton: assert a record with
  a disallowed key (e.g. `patient_name`, `path`, `password`) is **rejected**, not
  silently dropped.

## Phase B: US1 — In-app onboarding checklist + access-request draft (P1)

- [ ] T006 [US1] Implement the three checks in `onboarding.py`:
  ① account exists, ② added to project, ③ VPN/server reachable — reusing Phase 1
  `preflight` + `xnat_gateway`; account/project queried via the gateway,
  reachability via `preflight`.
- [ ] T007 [US1] Make the VPN/reachability check **fail soft**: an offline or
  timed-out probe yields the `unknown` state ("couldn't check — are you on the
  VPN?") via the Phase 1 `errors` helper, never a traceback, never a false `done`.
- [ ] T008 [US1] Build `AccessRequest` drafting: produce a Librarian-addressed
  message with the student's HawkID + project name prefilled; **draft/copy** is the
  default, a send-capable mail path is an optional extension (kept
  implementation-agnostic).
- [ ] T009 [US1] No-email-client fallback: when no mail app is available, surface a
  ready-to-copy recipient + body so the step never dead-ends (Constitution II).
- [ ] T010 [US1] Wire the checklist into the Phase 2 login screen so it renders
  before upload; all-green → point to the next step (upload) without nagging an
  already-onboarded student.
- [ ] T011 [US1] [P] `tests/test_onboarding_status.py`: drive the full 3-check
  status matrix against `FakeXNAT` + a stubbed reachability check; assert correct
  per-item status + concrete next step for every combination, offline.
- [ ] T012 [US1] [P] `tests/test_access_request_draft.py`: assert the draft is
  addressed to the Data Librarian, HawkID + project are filled, nothing is sent
  without explicit action, and the no-email-client fallback returns copyable text.

## Phase C: US2 — Static onboarding site (P2)

- [ ] T013 [US2] Create `docs/site/index.md`: the ≤2-minute "what is this / how do
  I get started" explainer + the download/setup link to the Phase 3 deliverable.
- [ ] T014 [US2] [P] Add the remaining required content blocks to the site: UIowa
  VPN reminder, annual SSL-cert renewal caveat, and the Data Librarian "who to
  contact" section.
- [ ] T015 [US2] Choose the lightest build path (raw Markdown or a thin generator,
  e.g. MkDocs/Jekyll) and add config under `docs/site/`; assets are static only —
  **no** forms, **no** JS analytics, **no** server endpoint.
- [ ] T016 [US2] Add `.github/workflows/pages.yml` to build `docs/site/` and publish
  to GitHub Pages on push.
- [ ] T017 [US2] [P] `tests/test_site_content.py`: against the built output, assert
  all five blocks are present, the download link resolves to the Phase 3 release,
  and the build contains **no** form / analytics tag / endpoint (fails otherwise).

## Phase D: US3 — Adoption success-metrics logging (P3)

- [ ] T018 [US3] Implement the opt-in, append-only **local JSON** event log in
  `metrics.py`: opt-out (default) writes **nothing**; opt-in is revocable and the
  existing log is clearable.
- [ ] T019 [US3] Emit aggregate events at the right points (first upload, upload
  succeeded/failed, "contact Librarian" action) carrying only allowlisted fields —
  no patient/subject id, no path, no credential.
- [ ] T020 [US3] Add a maintainer **summary** that derives the four metrics from the
  log — time-to-first-upload, upload error rate, active uploaders per term, and the
  support-ticket proxy — with no analytics service.
- [ ] T021 [US3] [P] `tests/test_metrics_phi_free.py`: assert across all event kinds
  that only allowlisted non-PHI fields appear, a disallowed/PHI field is rejected,
  and opt-out yields **zero** writes (no file created).
- [ ] T022 [US3] [P] `tests/test_metrics_summary.py`: from a synthetic event log,
  assert time-to-first-upload and upload error rate match ground truth and
  active-uploader/ticket-proxy counts are derivable — all offline.

## Phase E: Polish & cross-cutting

- [ ] T023 [P] Edge case: already-fully-onboarded student → checklist shows
  all-green and steps aside immediately (test: no nag, points to upload).
- [ ] T024 [P] Opt-in consent UX: a clear, revocable opt-in prompt for metrics;
  test that declining (default) leaves the app behaving exactly as without metrics.
- [ ] T025 Run the full suite; confirm SC-001…SC-006 hold, all Phase 4 tests run
  in CI with no network and no PHI, and the site build collects zero user data.

## Dependencies

- Phase A blocks B–D (the shared data models + metrics allowlist).
- US1 (Phase B) depends on Phase 1 services (`preflight`, `errors`,
  `xnat_gateway`, `FakeXNAT`) and the Phase 2 login screen.
- US2 (Phase C) and US3 (Phase D) are independent of each other and may proceed in
  parallel once Phase A is done.
- T013 (index content) precedes T016 (Pages publish) and T017 (content check).
- T018 (log writer) precedes T019–T022 (emit + summary + tests).
- `[P]` tasks within a phase touch different files and may run concurrently.
