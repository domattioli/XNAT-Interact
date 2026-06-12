# Implementation Plan: Packaging & Install (Phase 3)

**Branch**: `003-phase-3-packaging-and-install` | **Date**: 2026-06-04 | **Spec**: [spec.md](spec.md)

## Summary

Deliver XNAT-Interact to UIowa **least-privilege managed machines** with as few
student steps as possible. **Primary channel = ITS Software Center** (MECM on
Windows, Jamf on macOS); **fallback = a code-signed self-served installer** for
BYOD. Because **Python can't be assumed**, ship a **bundled runtime**, but make a
**Launcher detect a compatible existing Python first and reuse it**, only falling
back to the bundle when none is found. Rewrite the README to a short quick-start,
retire the footgun `update_and_test.py`, and add a per-channel **update mechanism**
(Software Center for packaged; in-app version check for self-served). No app
behavior changes — this is delivery, launch, and update plumbing.

## Technical Context

**Language/Version**: Python 3.11 target for the bundled runtime; detection accepts
the compatible major/minor range the app supports.
**Primary Dependencies**: **PyInstaller** (or equivalent: Briefcase/py2app on mac)
for the bundled runtime; `pytest` for tests. No new app-runtime deps.
**Packaging/Delivery**: **MECM** (Windows Software Center) + **Jamf** (macOS Self
Service) — packaged and deployed by **ITS**. Self-served installer signed with
**signtool** (Windows) and **codesign + notarytool** (macOS).
**Python detection**: PATH scan, **Windows registry** (`HKLM/HKCU\Software\Python`),
common install locations; version + bitness + importability verification.
**Storage**: PHI-free launch log (local temp); no new persistent state. Endpoint
config reuses the Phase 1 `AppConfig` file — not embedded in the build.
**Testing**: `pytest`, **offline**, with **faked PATH / registry / version** inputs
for detection; a built-artifact **smoke test** on a clean runner where feasible.
**Target Platform**: Windows + macOS desktops (UIowa managed + BYOD).
**Project Type**: Packaging around the existing single-project app (CLI + Phase 2 GUI).
**Constraints**: No local admin assumed; no network in unit tests; no PHI in logs;
no signing secrets / endpoints in source.
**Scale/Scope**: 2 OSes, 1 launcher, 1 detection module, 2 build specs, 1 Software
Center packaging request per OS, 1 signing pipeline, 1 update checker, 1 README,
1 retired script.

## Constitution Check

*GATE: must pass before and after design.*

- **I — PHI Safety**: This phase moves **no image data**; it delivers/launches the
  app. The launch log is **PHI-free** (FR-004). No de-identification path is
  changed. ✅ (no PHI surface introduced).
- **II — Fail Softly**: FR-012 — launch failures (missing/corrupt runtime,
  incompatible Python, blocked binary, offline update check) produce a plain message
  + next step (reinstall / contact Librarian), never a traceback. ✅
- **III — Lower Skill Floor**: **Central.** Going from "nothing installed" to
  "running" with ≤3 steps and **no admin / no terminal / no git / no `pip`** is the
  entire purpose (SC-001, SC-006, FR-008). ✅ Biggest skill-floor win of the project.
- **IV — Offline Testable**: FR-013 — detection unit-tested with **faked
  PATH/registry/version** inputs; FR-011 — update check tested against an injectable
  faked "latest" source; both offline, no network, no real install. ✅
- **V — Config/No Secrets**: FR-014 — **no signing secrets, credentials, server
  URLs, or project names in source**; signing materials live in CI secrets, endpoints
  in the Phase 1 config file. ✅
- **VI — Data Integrity**: No shared-state writes, no destructive ops, no uploads in
  this phase. The **update mechanism** replaces the `git reset --hard` footgun
  (FR-009) with a non-destructive path. ✅ (no concurrency surface introduced).

No violations. No complexity-tracking exceptions needed.

## Project Structure

```text
specs/003-phase-3-packaging-and-install/
├── spec.md
├── plan.md      # this file
└── tasks.md

packaging/
├── launcher/
│   ├── launcher.py            # NEW: detect-or-bundle entry; starts app; friendly fails
│   └── python_detect.py       # NEW: PythonDetector (pure; PATH/registry/locations)
├── update/
│   └── update_check.py        # NEW: UpdateChecker (self-served; injectable source)
├── windows/
│   ├── xnat_interact.spec     # NEW: PyInstaller build spec (Windows)
│   ├── sign.md                # NEW: signtool signing steps (no secrets in repo)
│   └── mecm_package.md        # NEW: Software Center / MECM packaging notes for ITS
├── macos/
│   ├── xnat_interact.spec     # NEW: PyInstaller/Briefcase build spec (macOS)
│   ├── sign_notarize.md       # NEW: codesign + notarytool steps (no secrets in repo)
│   └── jamf_package.md        # NEW: Jamf Self Service packaging notes for ITS
└── README_PACKAGING.md        # NEW: maintainer build/release runbook (build → sign → ITS)

tests/
├── fakes/
│   └── fake_env.py            # NEW: fake PATH / registry / interpreter probes
├── test_python_detect.py      # NEW: detection w/ faked inputs (compatible/old/bitness/broken)
├── test_launcher_selection.py # NEW: reuse-existing vs fall-back-to-bundled decisions
├── test_update_check.py       # NEW: newer→notice, current→silent, offline→friendly
└── test_built_artifact_smoke.py  # NEW: smoke the built artifact where the runner allows

README.md                      # MODIFIED: short friendly quick-start (Software Center first)
update_and_test.py             # RETIRED/REPLACED: no git reset --hard; no false test claim
```

## Approach (phased within the phase)

1. **Detection first** (US1 core): build `python_detect.py` as **pure logic** over
   injected PATH/registry/location probes, and `launcher.py` runtime selection.
   Unit-test exhaustively with faked inputs (compatible, too-old, wrong-bitness,
   broken). Nothing else is verifiable until the seam exists.
2. **Bundled runtime + Windows build** (US1): author the Windows PyInstaller spec;
   produce an artifact; smoke it on a clean runner (no Python) and on one with a
   compatible Python; assert the launcher's reuse-vs-bundle choice.
3. **macOS build + signing/notarization** (US2): mac build spec; wire
   signtool/codesign+notarize via CI secrets; verify signature/notarization.
4. **Self-served installer + ITS packaging notes** (US2): assemble the signed
   self-served installer; write the MECM/Jamf packaging notes ITS will use.
5. **README + update mechanism** (US3): rewrite README; retire `update_and_test.py`;
   implement the self-served `UpdateChecker` behind an injectable source; document
   that Software Center owns updates for the packaged build.

Each step lands with its tests green; the build/sign/ITS steps depend on the
detection seam and on the **operational** cert/packaging prerequisites (flagged in
Risks and as checkboxes in `tasks.md`).

## Risks & Mitigations

- **Signing-cert procurement lead time**: code-signing certs (Windows EV/OV; Apple
  Developer ID + notarization) take time to obtain through ITS/department →
  *Mitigation*: land build specs + launcher + detection **before** certs arrive;
  gate only the final signed-artifact tasks on the cert; track cert procurement as
  an explicit Data-Librarian/ITS checkbox.
- **ITS packaging lead time / requirements**: Software Center deployment needs an
  ITS relationship, primary-user + security-group setup → *Mitigation*: open the ITS
  packaging request early as a Data-Librarian checkbox; ship the self-served signed
  installer as an interim channel so adoption isn't blocked on ITS turnaround.
- **Detection false-positives**: reusing an interpreter that *looks* compatible but
  can't import deps/create a venv → broken launch → *Mitigation*: detection verifies
  **version + bitness + a real importability/venv probe**, rejects on any failure,
  and falls back to bundled; covered by faked-input unit tests.
- **Unsigned-binary blocks (SmartScreen/AV/app-control, Gatekeeper)** on the
  self-served path → *Mitigation*: **sign + (mac) notarize**; README routes blocked
  users to the Software Center path; treat an unsigned-block as expected, not a bug.
- **Bundled-runtime bloat / startup cost** → *Mitigation*: reuse-existing-Python-first
  keeps the common managed-machine case light; bundle is the fallback only.
- **Artifact smoke testability in CI**: signing/notarization and clean-VM launch may
  exceed CI capability → *Mitigation*: run pure detection/update unit tests in CI
  always; mark the built-artifact smoke test to run where the runner allows
  (`integration` marker), document a manual smoke checklist otherwise.
- **README drift vs actual invocation** → *Mitigation*: write the README **after**
  the launcher/update path is fixed so the documented primary path matches reality.
