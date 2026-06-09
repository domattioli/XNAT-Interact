# Feature Specification: Packaging & Install (Phase 3)

**Feature Branch**: `003-phase-3-packaging-and-install`
**Created**: 2026-06-04
**Status**: Draft
**Input**: `docs/IMPROVEMENT_PLAN.md` Phase 3 ("Remove the install cliff") + "Your install question, answered" (UIowa managed-machine policy) + Sources

## Overview

Take a student from "nothing installed" to "the app is open" with as few steps as
possible, on a UIowa **IT-managed machine** where they have **no local admin**.
The reliable channel is the **ITS Software Center** (MECM on Windows, Jamf on
macOS); the app is delivered as a package ITS deploys. Because **we cannot assume
Python is installed**, the deliverable ships its own runtime — **but the launcher
MUST detect a compatible existing Python first and reuse it**, only falling back
to the bundled runtime when none is found. A **code-signed self-served installer**
is the fallback for BYOD/non-managed machines. This phase also rewrites the README
into a short quick-start and retires the misleading `update_and_test.py`, replacing
it with a real update mechanism. **No app behavior changes** — this is delivery,
launch, and update plumbing around the Phase 1/2 code.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Install from Software Center and launch (Priority: P1)

A student on a managed UIowa laptop opens the **ITS Software Center**, finds
XNAT-Interact, clicks install, and launches it — no admin password, no terminal,
no Python setup. On first launch the app **detects a compatible Python already on
the machine and reuses it**; if none exists, it **uses its own bundled runtime**.
Either way the app window opens.

**Why this priority**: This is the whole point of the phase and the only path that
is *reliable* on least-privilege managed machines — it sidesteps the admin-rights
and unsigned-binary blocks by design. The detect-or-bundle launcher is what makes
"nothing installed" reach "running" without assuming Python.

**Independent Test**: Build the package artifact; on a clean VM with no Python,
launch and assert the bundled runtime is used and the app starts. On a VM with a
compatible Python on PATH, launch and assert the existing interpreter is reused
(no bundled runtime invoked). Detection logic is unit-tested separately with faked
PATH / registry / version inputs — fully offline, no real install.

**Acceptance Scenarios**:

1. **Given** a managed machine with no Python, **When** the student installs from
   Software Center and launches, **Then** the app opens using the **bundled
   runtime** and never asks for admin rights.
2. **Given** a machine with a compatible Python (correct major/minor) on PATH,
   **When** the student launches, **Then** the launcher **reuses that interpreter**
   and does not invoke the bundled runtime.
3. **Given** launch begins, **When** the launcher selects a runtime, **Then** it
   records which path it chose (existing vs bundled) in a PHI-free launch log.

---

### User Story 2 - Signed self-served install on a non-managed machine (Priority: P2)

A student on a personal/BYOD laptop (or where Software Center isn't an option)
downloads a **code-signed installer**, runs it from their user profile **without
admin**, and the OS does not block it as an unsigned/untrusted binary. The same
detect-or-bundle launcher runs.

**Why this priority**: The fallback channel for machines outside ITS management. It
matters, but it is the backup to the primary Software Center path, and an unsigned
binary is *not guaranteed* to run — code-signing materially improves the odds, so
this story is explicitly the **signed** build.

**Independent Test**: Produce the signed artifact; verify the signature/notarization
on both OSes (signtool/codesign + notary check). On a test machine, run the
installer from a non-admin user profile and assert it completes and launches. The
launcher's runtime selection reuses the US1 detection tests.

**Acceptance Scenarios**:

1. **Given** a signed installer, **When** verified, **Then** the signature is valid
   and (macOS) notarization is present.
2. **Given** a non-admin user profile, **When** the student runs the signed
   installer, **Then** it installs to the user profile and launches without an
   admin prompt.
3. **Given** the same machine has no Python, **When** the app launches, **Then** the
   bundled runtime is used, exactly as in US1.

---

### User Story 3 - Friendly README, real updates, no footgun updater (Priority: P3)

A student reading the README sees a **short, friendly quick-start** ("install from
Software Center → open → log in"), not 140 lines of git/Python/venv steps. The
misleading `update_and_test.py` (which does `git reset --hard origin/master`
against possibly the wrong default branch and falsely claims to run tests) is
**retired**, replaced by a defined update path: Software Center handles updates for
the packaged app; the self-served build shows an in-app "a new version is
available" notice.

**Why this priority**: Documentation and update mechanism — high value for adoption
and safety, but they sit on top of the delivery work in US1/US2 and don't block a
student from installing today.

**Independent Test**: README renders and contains no terminal/git prerequisites in
the primary path. `update_and_test.py` is removed (or replaced) — assert it no
longer runs `git reset --hard`. The in-app update check is unit-tested against a
faked "latest version" source (no network): newer version → notice shown; same/older
→ silent.

**Acceptance Scenarios**:

1. **Given** the new README, **When** a non-technical student reads the quick-start,
   **Then** the primary path requires no git, no Python install, no venv, no `pip`.
2. **Given** the packaged app, **When** ITS publishes a new version, **Then** the
   update arrives via Software Center with no student action beyond the managed
   update.
3. **Given** the self-served build and a newer published version, **When** the app
   starts, **Then** it shows a non-blocking "new version available" notice with a
   link; **Given** the version is current, **Then** no notice appears.
4. **Given** the repo, **When** inspected, **Then** `update_and_test.py` no longer
   performs `git reset --hard` and no longer claims to run a test suite.

---

### Edge Cases

- **No local admin**: install and launch MUST complete with zero admin prompts on
  the Software Center path; if any step would require elevation, it is a defect.
- **App-control / SmartScreen / AV blocks the unsigned exe**: the self-served build
  MUST be code-signed; an unsigned binary blocked by policy is an expected failure
  the fallback is designed to avoid, and the README directs blocked users to the
  Software Center path.
- **Existing Python is present but too old / wrong build** (e.g. 3.7, 32-bit, or a
  venv that can't import deps): detection MUST reject it and fall back to the
  bundled runtime rather than reuse an incompatible interpreter.
- **Offline machine at first launch**: the app MUST start (bundled runtime + local
  code need no network); only the update check and the server login require
  connectivity, and their absence yields a friendly message, not a crash.
- **macOS Gatekeeper / quarantine**: the macOS artifact MUST be signed **and
  notarized** so Gatekeeper does not block it; an un-notarized app is an expected
  block the signed build avoids.
- **Multiple Pythons on PATH**: detection MUST pick the first *compatible* one
  deterministically and record which it chose; ambiguity MUST NOT crash launch.
- **Corrupt/partial bundled runtime**: if the bundled runtime fails to start, the
  launcher MUST surface a friendly "installation looks incomplete — reinstall from
  Software Center / contact the Data Librarian" message, not a traceback.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The app MUST be packaged as an **ITS Software Center deployable**
  artifact for Windows (MECM) and macOS (Jamf), installable on a managed machine
  **without local admin**.
- **FR-002**: The deliverable MUST be able to run on a machine with **no Python
  installed** by shipping its own Python runtime (a bundled runtime / one-file
  build).
- **FR-003**: On launch, the launcher MUST **first detect an existing compatible
  Python** (correct major/minor version; able to create a venv or import the app's
  deps) by checking PATH, the Windows registry, and common install locations, and
  MUST **reuse it when found**.
- **FR-004**: The launcher MUST **fall back to the bundled runtime** only when no
  compatible existing Python is found, and MUST record which runtime was selected
  in a **PHI-free** launch log.
- **FR-005**: Python detection MUST **reject incompatible interpreters** (too old,
  wrong bitness, broken/unimportable) and treat them as "not found".
- **FR-006**: A **code-signed** self-served installer MUST be provided as the
  fallback channel: **signtool**-signed on Windows, **codesign + notarized** on
  macOS.
- **FR-007**: The self-served installer MUST be installable to the **user profile
  without admin** on a non-managed machine.
- **FR-008**: The README MUST be rewritten to a short, friendly quick-start whose
  **primary path requires no git, no Python install, no venv, and no `pip`**.
- **FR-009**: `update_and_test.py` MUST be **retired or fixed** so it no longer runs
  `git reset --hard origin/master` and no longer claims to run a test suite it does
  not run.
- **FR-010**: The update mechanism MUST be defined and implemented per channel:
  **Software Center handles updates** for the packaged app; the **self-served build
  MUST perform a non-blocking in-app "new version available" check**.
- **FR-011**: The in-app update check MUST be **offline-testable** behind a seam
  (the "latest version" source is injectable/faked) and MUST fail softly when the
  network is unavailable (no crash, no blocking).
- **FR-012**: Failures during launch (missing/corrupt runtime, incompatible Python,
  blocked binary) MUST produce a **plain-language message + next step** (reinstall /
  contact the Data Librarian), never a raw traceback (Constitution II).
- **FR-013**: The Python-detection and runtime-selection logic MUST be **unit-tested
  offline** with **faked PATH / registry / version inputs**; no real Python install
  or network is required to run the tests.
- **FR-014**: The build/packaging process MUST **embed no credentials, signing
  secrets, server URLs, or project names in source**; signing materials and any
  endpoint config come from CI secrets / the Phase 1 config file (Constitution V).

### Key Entities

- **Launcher**: the first thing that runs on start; decides which Python runtime to
  use (existing vs bundled), then starts the app; emits the PHI-free launch log and
  friendly failures.
- **PythonDetector**: pure logic that, given PATH / registry / install-location
  inputs, returns a compatible interpreter or "none found"; the unit-tested seam.
- **RuntimeBundle**: the shipped Python interpreter + dependencies used when no
  compatible existing Python is found.
- **SoftwareCenterPackage**: the ITS-deployable artifact (MECM/Jamf) — the primary
  delivery; carries install/launch metadata, no admin required.
- **SignedInstaller**: the code-signed (Windows) / signed+notarized (macOS)
  self-served artifact — the fallback delivery.
- **UpdateChecker**: for the self-served build, compares the running version to a
  published "latest" version and surfaces a non-blocking notice; injectable source,
  offline-testable.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A student with **nothing installed** (no Python) reaches **first app
  launch** in **≤3 student-visible steps** on the Software Center path (find →
  install → open) with **zero admin prompts**.
- **SC-002**: On a clean machine with **no Python**, the app launches successfully
  using the bundled runtime in **100%** of clean-VM smoke runs.
- **SC-003**: On a machine with a **compatible Python**, the launcher **reuses** the
  existing interpreter (bundled runtime not invoked) in **100%** of such runs.
- **SC-004**: Python detection correctly **rejects** every catalogued incompatible
  interpreter (too old, wrong bitness, broken) and falls back to bundled — verified
  by unit tests with faked inputs.
- **SC-005**: The self-served installer is **validly signed** (Windows signtool) and
  **signed + notarized** (macOS), confirmed by an automated verification step.
- **SC-006**: The README primary path contains **zero** git / Python-install / venv
  / `pip` prerequisites.
- **SC-007**: `update_and_test.py` performs **no** `git reset --hard` and makes
  **no** false claim of running tests (verified by inspection/test).
- **SC-008**: The self-served update check shows the "new version" notice for a
  newer published version and stays silent for current/older — verified offline
  against a faked source; network-absent yields a friendly message, not a crash.

## Assumptions

- The **Data Librarian / departmental IT will open and shepherd the ITS packaging
  request** and procure code-signing certificates — these are **operational
  prerequisites** outside the repo and are tracked as explicit checkboxes in
  `tasks.md`. Engineering can land the build specs, launcher, and detection logic
  before ITS engagement completes.
- ITS Software Center can deploy the artifact to a student who is the device's
  **primary user** and in the **right security group** (per UIowa policy research);
  confirming the security-group/primary-user setup is part of the ITS request.
- The bundled-runtime approach (PyInstaller-style or equivalent) can package the
  Phase 1/2 code with its dependencies on both Windows and macOS; exact tool choice
  is an implementation detail of `plan.md`.
- The GUI (Phase 2) and core logic (Phase 1) run **unchanged** regardless of which
  runtime the launcher selects or which channel delivered the app.
- UIowa **Just-in-Time Admin does not apply to student desktops**, so self-elevation
  is not a delivery route; least-privilege (no student admin) is assumed throughout.
- Signing-cert procurement and ITS packaging have **lead time**; the phase is
  sequenced so code lands without blocking on them, and the operational steps are
  flagged for the maintainer.
