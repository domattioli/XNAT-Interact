# Cross-Artifact Analysis: 010-pixel-deid

**Date**: 2026-06-09 · spec ✓ plan ✓ tasks ✓ · grounded against current `src/`.

## Coverage matrix (FR/SC/US → task)

| Req | Task(s) | Status |
|---|---|---|
| FR-001 verdict replaces placeholder | T013,T014,T018 | ✓ |
| FR-002 device-profile registry + starter profiles | T008,T009 | ✓ |
| FR-003 cross-frame variance consensus | T010 | ✓ |
| FR-004 multipass + ONNX CRAFT detector | T004,T005 | ✓ |
| FR-005 PHI-vs-benign classification | T012 | ✓ |
| FR-006 fail-closed → quarantine | T014 | ✓ |
| FR-007 dilate boxes | T006 | ✓ |
| FR-008 irreversible fill + audit (no raw PHI) | T013,T015 | ✓ (see F1) |
| FR-009 preserve benign markers | T012 | ✓ |
| FR-010 CPU-only | T005,T022 assert no-GPU | ✓ |
| FR-011 batch ≤200 parallel | T021 | ✓ |
| FR-012 human only at quarantine | T019 | ✓ (see F2) |
| FR-013 upload gate | T019 | ✓ (see F2) |
| FR-014 synthetic fixtures | T003 | ✓ |
| FR-015 quarantine store + evidence | T017 | ✓ |
| SC-001 FN=0 holdout | T023 | ✓ (hard gate) |
| SC-002 quarantine <10% | T024 | ✓ |
| SC-003 benign preserve ≥90% | T024 | ✓ |
| SC-004 no raw PHI in audit | T015,T016 | ✓ (see F1) |
| SC-005 ≤15 min/200 | T022 | ✓ |
| SC-006 offline green + CPU CI | T025 | ✓ (see F3) |
| US1 profiled auto-redact | T008,T009,T017,T018,T019 | ✓ |
| US2 unprofiled/faint catch-all | T004,T005,T010,T014 | ✓ |
| US3 benign preserve | T012,T016 | ✓ |
| US4 batch throughput | T021,T022 | ✓ |

No orphan tasks; no uncovered requirement.

## Findings (grounded in source)

- **F1 — audit seam is a fixed 3-column API.** `registry.record_audit(actor, action, target)`
  (`src/services/registry.py:351`) — no structured evidence column. **Resolution**: T015 encodes the
  evidence (region count, tiers fired, PHI categories) as a compact JSON string in `target`,
  guaranteed **no raw PHI text** (categories/counts only). Fold into T015. Severity: low.

- **F2 — `ReviewDecision` has no quarantine member.** `src/xnat_experiment_data.py:32` defines only
  `CONFIRMED` + `REDACT` (review returns one of these; the gate raises `FriendlyError` on the
  no-confirm path). The verdict engine produces `clean|redacted|quarantine`. **Resolution**: add a
  `QUARANTINE` member to `ReviewDecision` and map verdict→decision in the gate; `quarantine` blocks
  upload via the existing `FriendlyError` recourse path (Principle II preserved). Fold into T019.
  Severity: med.

- **F3 — CI weight: holdout needs Tesseract-5 binary + spaCy `en_core_web_lg` (~400 MB) +
  onnxruntime + the vendored CRAFT model.** Loading all of that into the default CI lane is heavy
  and could break the light `python-tests` lane. **Resolution**: the FN=0 holdout + throughput tests
  carry a `pixeldeid`/`slow` marker and **skip gracefully when deps/model are absent**; a dedicated
  opt-in CPU lane installs the heavy deps and runs them. Core modules import the heavy deps lazily so
  the rest of the suite stays light. Fold into T002 (lazy import note) + T025 (lane + skip markers).
  Severity: med.

- **F4 — `data/`/`models/` gitignore.** `.gitignore` has scoped `/data/...` allow/deny rules but no
  blanket ignore of `data/device_profiles/` or any `models/` rule. **Resolution**: confirm the new
  dirs are tracked (add explicit `!` allow or `.gitkeep` if a parent rule catches them); CRAFT
  `.onnx` is binary → push via git CLI direct (T026), never MCP. Severity: low.

- **F5 — detector graceful-degrade is load-bearing for FN=0.** If the CRAFT model is absent, the
  detector tier falls to Tesseract-multipass only. FN=0 must still hold via the contrast-independent
  tiers (profiles + consensus) **+ quarantine** — i.e. a faint, unprofiled, single-frame case with no
  CRAFT MUST quarantine, not pass clean. T023 holdout MUST include exactly that cell. Fold the
  no-CRAFT cell into T023. Severity: med (it's the safety invariant).

- **F6 — `deidentify.py` back-compat.** `needs_pixel_review(pixel_array)` currently takes a bare
  array. The verdict engine needs the dataset (device id, multi-frame). **Resolution**: keep the
  old signature working (single-array → conservative review) and add the richer dataset-aware entry
  the gate calls; don't break existing callers/tests. Fold into T018. Severity: low.

## Decisions folded into tasks
T015 (audit JSON-in-target, no raw PHI) · T019 (`ReviewDecision.QUARANTINE` + gate mapping) ·
T002+T025 (lazy heavy imports + marked/skippable heavy CI lane) · T023 (no-CRAFT faint-unprofiled
→ quarantine cell) · T018 (back-compat `needs_pixel_review`). No spec/plan changes required —
findings are implementation-level. Build may proceed.
