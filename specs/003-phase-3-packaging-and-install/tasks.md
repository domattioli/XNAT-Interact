# Tasks: Packaging & Install (Phase 3)

**Input**: [spec.md](spec.md), [plan.md](plan.md)
**Prerequisites**: Phase 0 test harness (shipped); Phase 1 `AppConfig` (URL/project
from config, not source). Tests REQUIRED (Constitution IV).

## Format: `[ID] [P?] [Story] Description`

- **[P]**: can run in parallel (different files, no dependency)
- **[Story]**: US1–US3 (see spec), FN = foundational, OPS = operational (maintainer / ITS)

## Phase A: Foundational — the detection seam (BLOCKS the build steps)

**⚠️ No build/sign/ITS work begins until A is complete.**

- [ ] T001 [FN] Create `packaging/launcher/__init__.py`, `packaging/update/__init__.py`,
  and `tests/fakes/__init__.py` (if absent).
- [ ] T002 [FN] Build `packaging/launcher/python_detect.py` (`PythonDetector`): pure
  logic that, given **injected** PATH entries, Windows-registry entries, and common
  install locations, returns a compatible interpreter or "none found". Verifies
  required major/minor version, bitness, and a usability probe (can create a venv /
  import the app's deps). No global state read directly — all inputs injected.
- [ ] T003 [FN] Build `tests/fakes/fake_env.py`: fakes for PATH listing, Windows
  registry lookup, common-location probing, and interpreter version/bitness/import
  probes — feeds `PythonDetector` deterministic inputs (compatible, too-old,
  wrong-bitness, broken/unimportable, multiple-pythons, none-found).
- [ ] T004 [FN] Build `packaging/launcher/launcher.py` (`Launcher`): calls
  `PythonDetector`; **reuses** an existing compatible interpreter when found, else
  **falls back to the bundled runtime**; records the chosen path in a **PHI-free**
  launch log; on failure emits a `FriendlyError` (reuse Phase 1 `errors.py`), never a
  traceback.
- [ ] T005 [FN] [P] `tests/test_python_detect.py`: drive every branch via `fake_env`
  — compatible-found → reuse; too-old / wrong-bitness / broken → rejected as
  not-found; multiple-pythons → first compatible chosen deterministically;
  none-found → fallback signaled. Fully offline.
- [ ] T006 [FN] [P] `tests/test_launcher_selection.py`: assert reuse-existing vs
  fall-back-to-bundled decisions and that the chosen path is logged PHI-free; assert a
  corrupt/missing bundled runtime yields a friendly "installation incomplete" message.

## Phase B: US1 — Bundled runtime + Windows Software Center build (P1)

- [ ] T007 [US1] Author `packaging/windows/xnat_interact.spec`: PyInstaller build spec
  that bundles the Python runtime + the Phase 1/2 app and its deps; entry point is
  `launcher.py`. No server URL / project name / secrets baked in (reads Phase 1
  config at runtime).
- [ ] T008 [US1] Write `packaging/README_PACKAGING.md`: maintainer runbook — build →
  (sign) → hand-off to ITS; states the compatible Python version range detection
  targets and where the bundled runtime comes from.
- [ ] T009 [US1] Write `packaging/windows/mecm_package.md`: Software Center / MECM
  packaging notes **for ITS** — artifact, silent install/uninstall behavior, no-admin
  expectation, primary-user + security-group requirement, detection method summary.
- [ ] T010 [US1] [P] `tests/test_built_artifact_smoke.py` (marked `integration`):
  build the Windows artifact; on a clean runner with **no Python**, launch and assert
  the **bundled runtime** starts the app; on a runner with a **compatible Python**,
  assert the existing interpreter is **reused**. Document a manual smoke checklist for
  runners that can't build the artifact in CI.

## Phase C: US2 — macOS build + signing/notarization + self-served installer (P2)

- [ ] T011 [US2] Author `packaging/macos/xnat_interact.spec`: macOS build spec
  (PyInstaller or Briefcase/py2app) bundling the runtime + app; entry point
  `launcher.py`; no secrets/endpoints baked in.
- [ ] T012 [US2] Write `packaging/windows/sign.md`: **signtool** signing steps;
  signing cert + password come from **CI secrets**, never the repo. Includes a
  signature-verification command.
- [ ] T013 [US2] Write `packaging/macos/sign_notarize.md`: **codesign + notarytool**
  steps; Apple Developer ID + notary credentials from CI secrets. Includes a
  `spctl` / notarization-staple verification command.
- [ ] T014 [US2] Assemble the **self-served signed installer** for each OS (user-profile
  install, no admin); wire signing into the build/release pipeline reading CI secrets.
- [ ] T015 [US2] Write `packaging/macos/jamf_package.md`: Jamf Self Service packaging
  notes **for ITS** — artifact, install behavior, no-admin expectation, primary-user +
  security-group requirement.
- [ ] T016 [US2] [P] `tests/test_built_artifact_smoke.py` (extend, `integration`):
  add a signature/notarization **verification** step (signtool verify / spctl) against
  the built self-served artifact; assert reuse-vs-bundle behavior holds on macOS.

## Phase D: US3 — README, update mechanism, retire the footgun updater (P3)

- [ ] T017 [US3] Build `packaging/update/update_check.py` (`UpdateChecker`): compares
  the running version to a published "latest" version from an **injectable** source;
  returns a **non-blocking** "new version available" notice; **fails soft** (friendly
  message, no crash, no block) when offline. Never touches git or local state.
- [ ] T018 [US3] [P] `tests/test_update_check.py`: faked "latest" source — newer →
  notice; same/older → silent; source-unreachable → friendly no-op. Fully offline.
- [ ] T019 [US3] **Retire/replace `update_and_test.py`**: remove the `git reset --hard
  origin/master` and the false "runs the test suite" claim. Either delete it (and
  update any references) or replace it with a thin script that does only what its name
  honestly claims (and against the correct default branch). Assert no destructive
  reset remains.
- [ ] T020 [US3] Rewrite `README.md` into a short, friendly quick-start: **primary
  path = install from ITS Software Center → open → log in**; fallback = signed
  installer; **no git / Python-install / venv / `pip` step** in the end-user path.
  Note Software Center handles updates; self-served shows the in-app version notice.

## Phase E: Operational prerequisites (maintainer / Data Librarian + ITS)

**These unblock the signed-artifact and Software-Center delivery; engineering (A–D)
lands without waiting on them.**

- [ ] T021 [OPS] **Data Librarian: open the ITS Software Center packaging request**
  (MECM Windows + Jamf macOS), citing the maintainer runbook and the per-OS packaging
  notes (T009, T015); confirm primary-user + security-group setup with ITS.
- [ ] T022 [OPS] **Data Librarian: procure code-signing materials** — a Windows
  Authenticode (OV/EV) cert and an Apple Developer ID + notarization credentials —
  and load them into **CI secrets** (never the repo).
- [ ] T023 [OPS] [P] **Data Librarian: confirm UIowa policy specifics with ITS**
  (least-privilege / no student admin; JTA does not apply to desktops; Software Center
  primary-user rules) per the IMPROVEMENT_PLAN.md Sources note, before deployment.

## Phase F: Polish & cross-cutting

- [ ] T024 [P] Verify **no secrets / endpoints in source**: build specs, launcher,
  signing docs, and installer carry no credentials, signing keys, server URL, or
  project name (Constitution V; reuse Phase 1 `AppConfig`).
- [ ] T025 [P] Confirm every launch failure path (missing/corrupt runtime,
  incompatible Python, blocked/unsigned binary, offline update check) yields a
  **friendly message + next step**, not a traceback (Constitution II) — test via the
  launcher with faked failures.
- [ ] T026 Run the full suite; ensure SC-001…SC-008 are met (detection branches
  covered offline; signature/notarization verified where the runner allows; README
  has zero git/Python/venv/`pip` prerequisites; `update_and_test.py` has no
  destructive reset).

## Dependencies

- Phase A (detection seam) blocks B–D's build/launch behavior.
- T002 (`PythonDetector`) precedes T004–T006.
- T007 (Windows spec) precedes T010 (Windows smoke); T011 (macOS spec) precedes
  T016 (macOS smoke).
- T012–T013 (signing docs) + T022 (OPS: certs) precede T014 (signed installer) and
  T016 (signature verification).
- T009/T015 (packaging notes) feed T021 (OPS: ITS request).
- T017 (`UpdateChecker`) precedes T018; T019 + T020 (retire updater + README) land
  after the launcher/update path is fixed so docs match reality.
- OPS tasks (T021–T023) are **operational** and run in parallel with engineering;
  only the final signed-artifact (T014/T016) and Software-Center delivery gate on them.
- `[P]` tasks within a phase touch different files and may run concurrently.
