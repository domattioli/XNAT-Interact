# Implementation Plan: Reliability & Safe Failure (Phase 1)

**Branch**: `001-phase-1-reliability-and-safe-failure` | **Date**: 2026-06-04 | **Spec**: [spec.md](spec.md)

## Summary

Introduce a thin **service layer** that separates core logic from the terminal,
funnel all failures through a **friendly-error helper**, add a **FakeXNAT** test
double so server paths are testable offline, close the **burned-in-PHI** gap with
a mandatory review step, make **batch upload continue-on-error**, and harden
**credentials/config/destructive ops**. No GUI; this is the foundation Phase 2
reuses.

## Technical Context

**Language/Version**: Python 3.11 (CI); keep runtime ≥3.8-compatible where cheap.
**Primary Dependencies**: `pyxnat`, `pydicom`, `opencv-python`, `pandas`,
`pytest` (existing). No new heavy deps.
**Storage**: XNAT server (remote) + `MetaTables.json` config on the server; local
temp for staging. Config file (new) for URL/project.
**Testing**: `pytest`, offline, against `tests/synthetic_data.py` + new `FakeXNAT`.
**Target Platform**: Windows + macOS desktops (UIowa managed + BYOD).
**Project Type**: Single-project CLI (GUI added in Phase 2).
**Constraints**: No network in tests; no PHI in repo/logs; no credentials in argv.
**Scale/Scope**: ~6 bare-except sites, ~60 asserts on user paths, 1 batch path, 1
delete script, 1 download path.

## Constitution Check

*GATE: must pass before and after design.*

- **I — PHI Safety**: FR-006 (burned-in review + redaction), FR-007 (local
  cleanup), FR-014 (PHI-removal tests). ✅ Central to this phase.
- **II — Fail Softly**: FR-001–FR-004, FR-013 (friendly errors, preflight, no
  user-facing asserts, no bare except). ✅ Central.
- **III — Lower Skill Floor**: Phase is backend, but it removes traceback-walls
  and is a *prerequisite* for the GUI; introduces no new terminal burden. ✅
- **IV — Offline Testable**: FR-005 (FakeXNAT), FR-014. ✅ Core deliverable.
- **V — Config/No Secrets**: FR-009 (no `--password`), FR-010 (config file). ✅
- **VI — Data Integrity**: FR-008 (batch resilience), FR-011 (delete confirm +
  dry-run), edge cases (mid-upload partial, config race). ✅ (full config-lock
  may extend to a follow-up — flagged in spec Assumptions).

No violations. No complexity-tracking exceptions needed.

## Project Structure

```text
specs/001-phase-1-reliability-and-safe-failure/
├── spec.md
├── plan.md      # this file
└── tasks.md

src/
├── services/                 # NEW: terminal-free core (reused by Phase 2 GUI)
│   ├── errors.py             # FriendlyError + handle()/diagnostic-log helper
│   ├── config.py             # AppConfig (URL/project from file/env)
│   ├── preflight.py          # reachability / auth / path checks
│   ├── deidentify.py         # extracted pure deidentify_dataset() + pixel review
│   └── xnat_gateway.py       # thin wrapper over pyxnat (the seam FakeXNAT replaces)
├── utilities.py              # MODIFIED: remove bare excepts, asserts→raises
├── xnat_scan_data.py         # MODIFIED: is_dicom by magic bytes; deidentify hook
├── batch_upload.py           # MODIFIED: continue-on-error + summary + resume
├── delete_contents_of_server.py  # MODIFIED: confirm + --dry-run, no swallow
└── main.py                   # MODIFIED: drop --password; route via services

tests/
├── fakes/
│   └── fake_xnat.py          # NEW: FakeXNAT test double
├── synthetic_data.py         # EXTEND: burned-in-text image; mixed-validity xlsx
├── test_friendly_errors.py   # NEW
├── test_preflight.py         # NEW
├── test_xnat_gateway_fake.py # NEW
├── test_deidentify_pixel_review.py  # NEW (flips the known_issue)
├── test_batch_continue_on_error.py  # NEW
├── test_config.py            # NEW
└── test_delete_guard.py      # NEW
```

## Approach (phased within the phase)

1. **Seam first** (US2): build `xnat_gateway.py` + `FakeXNAT`, characterization
   tests pinning current behavior through the gateway. Nothing else is safely
   testable until this exists.
2. **Friendly errors + preflight** (US1): `errors.py`, replace bare excepts and
   user-facing asserts, add preflight + SSL-cert message.
3. **PHI review** (US3): extract `deidentify_dataset()` pure function, add the
   burned-in review/redaction step, flip the `known_issue` test.
4. **Batch resilience** (US4): continue-on-error + summary + failed-only re-run.
5. **Hardening** (US5): drop `--password`, add `config.py`, guard delete script,
   `is_dicom` by magic bytes, local PHI cleanup.

Each step lands as its own commit with tests green; later steps depend on the
seam from step 1.

## Risks & Mitigations

- *Faithful fake*: FakeXNAT could drift from real pyxnat → keep it to the minimal
  used surface; add a thin contract checklist in `tests/fakes/fake_xnat.py`.
- *Refactor regressions*: extracting `deidentify_dataset()` must not change
  scrubbing → the existing de-id tests guard it; run before/after.
- *Config race full-fix scope*: if locking proves large, ship "detect + refuse to
  clobber" now and open a follow-up issue for true locking.
