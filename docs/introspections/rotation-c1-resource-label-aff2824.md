---
session_type: maintenance-rotation
repo: domattioli/XNAT-Interact
date: 2026-06-16
slot: UTC-hour rotation (XNAT-Interact maintenance track; not in MADMESHing#48 mesh-overhaul roster)
branch: claude/jolly-hopper-safmi0
pr: "#43"
issues_filed: ["#44"]
introspect_round: R5
---

# Session Handoff — domattioli/XNAT-Interact · 2026-06-16 maintenance rotation

**Task:** Rotating-harness maintenance slice. XNAT-Interact is outside the #48 mesh-overhaul roster
(DomI/QuADMESH/ADMESH/CHILmesh/Valence) → maintenance track = top of issue queue.

**Picked:** #33 correctness-audit CRITICAL item **C1** (download data-loss). C2 was already fixed
(`18c9ac3`); C1 was the next-highest triage item and fully offline-testable.

## What shipped

- **PR #43** (`fix: parameterize download resource label (#33 C1)`):
  `app/logic/download.py::download_selection` hardcoded the `SRC` resource label in both the query
  string and the `.resource()` call, so any scan whose resource label ≠ `SRC` (e.g. `DERIVED`)
  silently returned no files → fell through to the legacy synthesized-filename path → downloaded a
  nonexistent file. Added a `resource_label` param (default `"SRC"`, backward compatible) + a per-row
  `resource_label` override, mirroring `assemble_zip`'s existing `scope → label` handling. Fixed an
  inaccurate docstring sentence (`scan_type` is the *scan* label, not the resource label).
- 2 offline FakeXNAT regression tests (DERIVED resource + per-row override). Download suite:
  `35 passed, 1 xfailed`. Full suite locally: `1156 passed, 34 skipped, 7 xfailed`. flake8
  `E9/F63/F7/F82` clean.
- Code edit dispatched to a Haiku subagent per the binding coding-dispatch policy; orchestrator
  reviewed the diff + ran the suite before commit.

## Pains / friction (matrix rows — no new request:skill per #203)

1. **CI logs unreadable from the cloud session.** GitHub MCP `get_job_logs` returns HTTP 404 for job
   artifacts and the signed Azure blob URL is network-blocked in the sandbox. Could not confirm the
   `tests.yml` failing step from logs — had to infer from local reproduction. *Matrix row:* "remote
   session cannot retrieve Actions job logs → CI triage blind; needs a log-proxy or gh-equipped pass."
2. **Pre-existing CI red blocks PR merge.** Both workflows fail across all branches (filed #44). A
   maintenance PR lands green-locally but red-CI through no fault of its own; merge is gated on an
   unrelated infra break. *Matrix row:* "base-branch CI red ⇒ every downstream PR inherits a red check;
   rotation should detect base-CI state before judging its own PR."
3. **No `send_later` in this session** → cannot self-schedule the ~1h PR re-check the subscription
   model expects; rely on webhook events only (which don't deliver CI-success/merge transitions).

## What worked

- Reproducing the full suite locally (after `pip install -r requirements.txt -r requirements-dev.txt`)
  was the only reliable signal once CI logs were unavailable — proved C1 fix green and isolated the CI
  red as pre-existing (3bb125b, unrelated branch, both workflows red 8h prior).
- Verify-first on the subagent: re-read the actual diff + re-ran tests rather than trusting the
  subagent's report.

## Next / open

- #44: confirm CI failing step from a real log; candidate causes = missing `libgl1` in
  `python-package.yml` (opencv `import cv2`) and/or heavy 011 deps (`presidio*`, `onnxruntime`) in base
  `requirements.txt` failing to install on a clean runner.
- #33 remaining: C2 done, C1 done; H1/H2 (DICOM identity), H3/H4/H5 (concurrency/state), H6 (assessor
  guard), and the M/L items remain.
