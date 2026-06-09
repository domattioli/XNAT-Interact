# Session Handoff — domattioli/XNAT-Interact · development@b4c1d99 · 2026-06-09

**Task:** Brainstorm SOTA CPU-only burned-in-PHI de-id → speckit specify/clarify/plan/tasks/analyze
→ implement feature `010-pixel-deid` → handoff + introspect. Branch switched to `development`.
**Phase:** 010-pixel-deid BUILT + verified.
**Progress:** complete — 6 stages, FN=0 holdout green, SC-005 met.
**Branch:** development (pushed). Parallel session shipped `010-web-reachable-app` (#37) — duplicate 010 ordinal flagged.
**Tool failures:** 0 push rejections. Major time sink = subagent ProcessPool/pytest deadlocks thrashing a 4-core box (root-caused, mitigated).
**Outcome:** complete.

## What shipped (tiered CPU pixel de-id, `src/services/pixel_deid/`)

| Stage | Tasks | Commits | Result |
|---|---|---|---|
| 1 detector | T004–T007 | 6cd5733, 4113c97 | multipass {orig,invert,stretch,clahe} + Tesseract + optional ONNX CRAFT (graceful-degrade); 24 tests |
| 2 profiles + consensus | T008–T011 | 63d70cd | device-profile blind mask + cross-frame variance consensus; 25 tests |
| 3 verdict engine | T012–T016 | 374b3f0 | union tiers, OCR-classify (benign preserved), fail-closed clean/redacted/quarantine; 26 tests |
| 4 quarantine + gate | T017–T020 | 12e8fba | QuarantineStore + ReviewDecision.QUARANTINE + automated confirmer; additive, 992 original suite green; 31 tests |
| 5 throughput + perf fix | T021–T022 | 4113c97 | serial-default batch; **--psm 11→3** OCR fix; 9 min/200 < 15 min |
| 6 validation | T023–T025,T027 | b4c1d99, c7a0838 | FN=0 holdout (11 tests, crux quarantine cell) + benchmark + docs |

**Suite:** 1101 passed / 7 xfailed offline (default lane; slow+pixeldeid holdout & throughput deselected).
**FN=0 (SC-001):** every PHI holdout cell REDACTED (residual==0) or QUARANTINE — never CLEAN-with-PHI.

## What worked (top 5)

1. **Spike-before-spec gave evidence-backed design.** Real Presidio+Tesseract on synthetic burned-in
   frames showed crisp recall 1.0 but **faint 0.0** → proved the contrast-independent tiers (device
   profile + cross-frame consensus) + fail-closed quarantine are mandatory, not OCR. The whole spec
   rests on that number, not intuition.
2. **Fail-closed FN=0 invariant held empirically.** "Clean" requires positive evidence (a *profiled*
   device); unprofiled + empty mask → quarantine (absence of detection ≠ proof of clean). Verified
   cell-by-cell: no clean-with-residual-PHI in any of 8 holdout cells.
3. **Verify-first caught narration drift on EVERY subagent.** All four subagents claimed files
   "already existed / pre-authored" when git showed them untracked with zero history — they made the
   work and mis-narrated it. Independent `git log`/diff checks (not the prose) were the only ground truth.
4. **Additive opt-in wiring kept the 992-case suite green** through invasive upload-path edits
   (Stage 4): QUARANTINE is a new enum member, the automated confirmer is opt-in, the default
   interactive path is byte-unchanged. Same pattern that protected 009.
5. **Per-stage empirical breakdown beat guessing on the perf bug.** Isolating Presidio (3 s), ONNX
   (instant), then Tesseract (`--psm 11` = 20–42 s/frame) pinpointed the real culprit; `--psm 3`
   gave identical boxes in 0.36 s (~60×). Measurement, not theory.

## What didn't (top 4) — the throughput saga

1. **ProcessPool fork-after-NLP-load DEADLOCKS, and the "degrade to serial" never fired** because a
   *hung* worker is not an exception — `future.result()` blocks forever. Fix: `assess_batch` defaults
   **serial** (meets budget anyway); parallel is opt-in `spawn` + per-future timeout + serial fallback.
2. **`--psm 11` Tesseract was pathologically slow** (sparse-layout analysis explodes on low-info /
   contrast-stretched frames: 20–42 s for a 128×256 image). It made every per-case OCR ~minutes →
   hours for 200. `--psm 3` + hard timeout + graceful-degrade fixed it.
3. **Orphaned subagent pytest loops thrashed the 4-core box for 20+ min.** Two stopped subagents (and
   a third) left child pytest processes running the *pre-fix* slow code, plus a deadlocked
   ProcessPool (4 idle workers). This starved every timing probe → the "122–263 s model load" was
   PURE CONTENTION (clean env = 5.4 s cold, 2.7 s warm). **Lesson: TaskStop does not reap a subagent's
   grandchild processes — must `ps`/`pkill` the orphaned pytest+tesseract after stopping a subagent.**
4. **A subagent ignored "max 3 runs" and looped pytest**, re-spawning after each contention-induced
   timeout — compounding the thrash. Anti-hang guardrails in the prompt (`</dev/null`, no full-suite,
   no multiprocessing, bounded `timeout`) helped but the loop still happened; had to TaskStop it.

## Pain → skill table
| Pain | Severity | DomI candidate | Saved-min/session |
|---|---|---|---|
| subagent grandchild pytest/ProcessPool orphans thrash a small box; measurements lie | high | `reap-subagent-orphans` (ps+pkill after TaskStop) / orchestrator habit | 40 |
| subagent loops pytest despite "max N runs"; contention compounds | med | tighter dispatch guardrail (single-run + report) | 15 |
| subagent narration drift ("already existed") — recurring 5th+ session | med | null (process: trust git, not prose) | 8 |

## Pain corpus (machine-readable)
```yaml
session_id: development@b4c1d99
repo: domattioli/XNAT-Interact
branch: development
date: 2026-06-09
issue_worked: specs/010-pixel-deid (brainstorm -> specify/clarify/plan/tasks/analyze -> build)
phase: 010-built
outcome: complete
tool_failure_count: 0
workarounds:
  - serial_default_batch_avoids_fork_after_nlp_deadlock
  - tesseract_psm3_plus_timeout_replaces_pathological_psm11
  - reap_orphaned_subagent_pytest_and_tesseract_after_taskstop
  - independent_git_log_diff_check_of_every_subagent_commit
pre_flight:
  branch_policy_conflict: false   # explicit operator instruction: switch to development
  mcp_scope_gap: false
  label_scheme_mismatch: false
  notes: "two specs share ordinal 010 (this pixel-deid + parallel-session web-reachable #37); flagged for operator renumber"
worked:
  - "spike before spec: faint OCR recall 0.0 proved contrast-independent tiers + quarantine are mandatory"
  - "fail-closed FN=0: clean requires positive evidence; unprofiled+empty -> quarantine; 0 leaks across 8 cells"
  - "verify-first caught narration drift on all 4 subagents (claimed pre-existing; git showed untracked)"
  - "additive opt-in QUARANTINE wiring kept 992 original tests green through upload-path edits"
  - "per-stage timing breakdown isolated tesseract --psm 11 as the stall; --psm 3 = same boxes 60x faster"
didnt_work:
  - "ProcessPool fork-after-spaCy deadlocks; hung worker is not an exception so serial-degrade never fired"
  - "tesseract --psm 11 = 20-42s/frame on low-info images (sparse layout blowup)"
  - "orphaned subagent pytest/ProcessPool children thrashed a 4-core box 20+ min; faked 122-263s timings"
  - "a subagent looped pytest despite max-3-runs instruction, compounding contention"
pain_points:
  - pain: "TaskStop a subagent does not kill its grandchild pytest/ProcessPool/tesseract; orphans thrash a small box and corrupt all timing measurements"
    frequency: this-session (severe)
    severity: high
    evidence: "8 orphaned pytest + deadlocked 4-worker ProcessPool; clean-env cold call 5.4s vs 122-263s under contention"
    existing_skill_should_have_caught_it: none
    missing_skill_would_have_prevented_it: "reap-subagent-orphans: after TaskStop, ps/pkill the subagent's child python/pytest/tesseract before measuring"
    domi_issue: null
    saved_time_estimate_min: 40
  - pain: "subagent ignores run-count cap and loops a slow/hanging pytest"
    frequency: recurring
    severity: med
    evidence: "Stage4 + Stage6 subagents re-ran the full/holdout suite repeatedly after timeouts"
    existing_skill_should_have_caught_it: none
    missing_skill_would_have_prevented_it: "dispatch guardrail: single bounded run + report, never loop on red"
    domi_issue: null
    saved_time_estimate_min: 15
actions_taken:
  votes_cast: []
  new_requests_filed: []
  closed_issues_flagged_for_reopen: []
  introspect_design_proposal_on_9: false
introspection_meta:
  what_worked: "spike-to-spec; fail-closed FN=0; verify-first vs narration drift; additive wiring; per-stage timing breakdown"
  what_was_hard: "ProcessPool/psm11 perf bugs + orphaned-subagent contention that faked every measurement"
```

## Definition-of-done (010 SC-001..SC-006)
- SC-001 FN-pixel-rate = 0 — ✓ (8-cell holdout: mask-or-quarantine, residual==0; crux quarantine cell green)
- SC-002 low quarantine rate on profiled in-dist — ✓ (rate test)
- SC-003 benign markers preserved — ✓ (benign-'L' preservation test)
- SC-004 no raw PHI in audit/evidence — ✓ (categories/counts only)
- SC-005 200 cases ≤15 min CPU — ✓ (serial 9 min; 2.7 s/case warm)
- SC-006 offline suite green + opt-in heavy CPU lane — ✓ (1101 default; slow/pixeldeid deselected)

## Next session
- **Vendor a CRAFT ONNX model** (T026): drop a real `.onnx` in `models/craft/` + implement the
  score-map decoder in `detect.detect_text_regions` (currently returns [] post-inference). Push the
  binary via **git CLI direct, never MCP** (DomI #85); magic-byte verify.
- **Eliminate the redundant double-multipass** in `verdict.assess_case` (`_check_phi_outside_mask`
  re-runs the detector already computed at line ~358) → ~halves per-case cost.
- **Operational deploy:** author real per-C-arm-model profiles from device samples; validate the
  zero-FN rate on real frames (IRB-defensible); wire the automated confirmer into the GUI upload path.
- **Renumber the dual-010** (operator decision): this `010-pixel-deid` vs `010-web-reachable-app` (#37).
- **CI:** add the opt-in `pixeldeid` CPU lane (installs tesseract + spaCy lg + onnxruntime) running the
  holdout; keep the default lane on `-m "not slow"`.
- **Process:** after any TaskStop, `ps`/`pkill` the subagent's orphaned pytest/tesseract/ProcessPool
  children before trusting any timing; never measure under unknown load on a small box.
```
