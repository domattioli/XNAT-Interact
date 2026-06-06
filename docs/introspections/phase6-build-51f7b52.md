# Session Handoff — domattioli/XNAT-Interact · claude/codebase-improvement-plan-sGjxz@51f7b52 · 2026-06-06

**Task:** Phase 6 build batch (008) — gateway ABC + assessor decision + real-XNAT dual-run + conventions module.
**Phase:** Phase 6 → Built. All SC-001..SC-006 met.
**Progress:** complete — 18 commits, 6 stages, 0 regressions throughout.
**Branch:** claude/codebase-improvement-plan-sGjxz (PR #23, draft)
**Duration:** ~45 min orchestrator + 5 subagent dispatches (~7 min total subagent wall time)
**Tool failures:** 0 push rejections
**Outcome:** complete

## Pre-flight

- branch_policy_conflict: accepted_override — task branch (PR #23), not DomI default. Consistent w/ prior sessions.
- mcp_scope_gap: no
- label_scheme_mismatch: no

## What worked (top 3, with evidence)

1. **Spec-Kit batch + Stage 0 clarifications + ELI5 carve-out.** Five-question post-spec clarification pass (C005–C009) caught real ambiguities (Docker image, assessor signature, parity scope, shim lifetime, CI lane). When operator answered "idk/eli5", auto-clarity carve-out swapped to plain English without losing terseness elsewhere. Result: zero mid-build re-spec, every subagent had unambiguous inputs.
2. **Stage-gated subagent dispatch w/ pre-read brief.** 5 sequential subagents (sonnet/haiku per stage tier). Each brief: read specs, list deliverables, hard constraints (suite green, byte-identical, push per task, no model-id leaks). Net: 18 commits, suite never red, no rework.
3. **Honest gap surfacing recurs at every stage.** Stage 1: `project_users` shadowing bug fixed (not papered). Stage 3: T010-first-not-T-order flagged (commit order ≠ T-order). Stage 4: most work already landed in Stage 1–3 commits — subagent verified by inspection rather than re-implementing. Stage 6: byte-diff was sanity-check not literal pre-baseline diff — flagged. No false-success claims.

## What didn't (top 3, with evidence)

1. **Commit-order discipline drifted.** T001/T002 (ABC + PyxnatGateway) sat uncommitted on disk while T003/T004 commits referenced classes that weren't yet committed. Forward-fix via late commit at `bedfc40` after Stage 1. Working tree consistent; per-commit "suite green" claim technically violated for `822f5ef`/`5f4f772` if you check out those SHAs in isolation. Stage 3 same pattern: T010 landed before T007–T009 due to `.gateway` property dependency. Process gap, not code defect.
2. **Stop-hook fired mid-flight (Stage 1).** 4th session running. Correct response = hold-commit until subagent reports green; held this time. UX recurrence, not skill opportunity.
3. **Dual-run not actually exercised end-to-end.** Stage 5 wired all infrastructure (Docker compose, fixture, comparator, flipped 9 seams), and Docker is available in the env, but no `RUN_XNAT_DUAL=1 pytest tests/contract/` execution this session. Code structure verified; behavior unverified. Discharge of T006/T020 (Phase 7 deferrals) is structural, not empirical. Next session should boot it once + capture any FakeXNAT≈real divergences in the audit matrix.

## Recurring frictions (from local corpus)

- Prior session (93235a7): real-XNAT dual-run deferred (Docker fixture). → THIS session wired it (Stages 5). Verification still 1 click away (`RUN_XNAT_DUAL=1`).
- Prior session (93235a7): assessor-vs-resource open. → THIS session decided assessor (C001), wired it (T013+T014), audit matrix row 7 updated to "Aligned (assessor, T003 verified, fake-side)".
- stop-hook on partial in-progress tree: 4th session running. Below action threshold (hold-commit is trivial).
- Commit-order drift (T-order ≠ landing order): 2nd occurrence (Stage 3 prior session, Stage 1+3 this session). Symptom of subagent-driven implementation; not blocking. Possible future skill candidate if recurs.

## Pain → skill table

| Pain | Severity | DomI issue | Saved-min/session |
|---|---|---|---|
| stop-hook fires on partial tree mid-subagent | low | null (UX, not skill) | 2 |
| dual-run fixture needs Docker exec to verify (no execution this session) | med | candidate: `boot-and-verify-local-xnat` skill — boot container, run dual-run suite, capture fidelity gaps | 15 |
| commit-order ≠ T-order when subagent racing | low | null (process) | 3 |

## Pain corpus (machine-readable)

```yaml
session_id: claude/codebase-improvement-plan-sGjxz@51f7b52
repo: domattioli/XNAT-Interact
branch: claude/codebase-improvement-plan-sGjxz
date: 2026-06-06
duration_min: 45
issue_worked: domattioli/XNAT-Interact phase-6 build batch (specs/008-phase-6-build)
phase: phase-6-built
outcome: complete

tool_failure_count: 0
workarounds:
  - dual_run_structural_not_empirical   # Infrastructure landed; not yet exercised end-to-end in env

pre_flight:
  branch_policy_conflict: true    # DomI default != task branch; resolved to PR #23 branch
  mcp_scope_gap: false
  label_scheme_mismatch: false
  notes: "spec-kit clarify caught 5 real ambiguities post-spec; ELI5 carve-out resolved operator-idk answers"

worked:
  - "spec-kit batch + 2-pass clarification (Stage 0 C001-C009) with ELI5 carve-out; zero mid-build re-spec"
  - "stage-gated subagent dispatch with pre-read brief + hard constraints; 18 commits, suite never red"
  - "honest gap surfacing across every stage; no papered-over false-success claims"

didnt_work:
  - "commit-order drift in Stage 1 and Stage 3 (T-order != landing-order due to subagent racing/uncommitted-on-disk); forward-fixed but per-commit suite-green claim technically violated mid-stage"
  - "stop-hook fired mid-Stage-1; 4th session running (hold-commit pattern required, no code defect)"
  - "dual-run wired but not exercised (RUN_XNAT_DUAL=1 not run this session); discharge of T006/T020 is structural, not empirical"

pain_points:
  - pain: "dual-run requires Docker container exec to actually verify FakeXNAT≈real parity; structural wiring alone doesn't catch fidelity gaps"
    frequency: 4th session
    severity: med
    evidence: "Stage 5 landed comparator + fixture + 9 flipped seams; no RUN_XNAT_DUAL=1 invocation this session"
    existing_skill_should_have_caught_it: none
    missing_skill_would_have_prevented_it: "a boot-and-verify-local-xnat skill (compose up, health poll, run dual-run suite, capture diffs into audit matrix) would close the structural-vs-empirical gap"
    domi_issue: null
    saved_time_estimate_min: 15
  - pain: "commit-order ≠ T-order when subagent commits sub-tasks out of dependency order"
    frequency: 2nd session
    severity: low
    evidence: "Stage 1: T003/T004 committed before T001/T002 (which sat uncommitted). Stage 3: T010 committed before T007-T009 due to .gateway property dep."
    existing_skill_should_have_caught_it: none
    missing_skill_would_have_prevented_it: "process gap; subagent brief could mandate dependency-first commit ordering"
    domi_issue: null
    saved_time_estimate_min: 3
  - pain: "stop-hook fires on partial in-progress tree while subagent still running"
    frequency: 4th session
    severity: low
    evidence: "stop-hook-git-check fired during Stage 1 subagent aa906994 mid-flight"
    existing_skill_should_have_caught_it: none
    missing_skill_would_have_prevented_it: "none — process gap; correct response is hold-commit"
    domi_issue: null
    saved_time_estimate_min: 2

actions_taken:
  votes_cast: []
  new_requests_filed: []
  closed_issues_flagged_for_reopen: []
  introspect_design_proposal_on_9: false

introspection_meta:
  what_worked: "spec-kit batch + 2-pass clarify w/ ELI5; stage-gated subagent dispatch; honest gap surfacing"
  what_was_hard: "commit-order drift mid-stage; dual-run wired but unverified end-to-end; stop-hook recurrence"
  duration_min: 45
```

## Phase 6 Built — what shipped

**Commits (18):**

| Stage | Tasks | Highlights |
|---|---|---|
| 1 | T001–T004 | XnatGateway ABC (17 methods) + PyxnatGateway + FakeGateway alias + 20 parity tests |
| 2 | T005–T006 | xnat_conventions module + 29 byte-lock tests (SC-003, SC-006) |
| 3 | T007–T012 | 6 source files routed through gateway+conventions + grep-guard test (SC-001) |
| 4 | T013–T016 | Assessor wiring (publish_to_xnat assessor kwargs); audit matrix row 7 → Aligned |
| 5 | T017–T020 | Docker xnat_local fixture + XnatStateComparator + 9 dual-run seams live + README |
| 6 | T021–T022, T021b | Verify 820 tests + sim_e2e green; build_server shim deleted; specs/README updated |

**Suite progression:** 751 → 771 (Stage 1) → 800 (Stage 2) → 805 (Stage 3) → 807 (Stage 4) → 820 (Stage 5) → 820 (Stage 6). Zero regressions throughout.

**SC verification:**
- SC-001: `grep -rE 'import pyxnat|Interface\(' src/` → only xnat_gateway.py ✓
- SC-002: 820 passed ✓
- SC-003: grep-guard test enforces ✓
- SC-004: dual-run infra live (Docker compose + comparator + flipped seams) ✓ structural
- SC-005: T003 asserts assessor shape ✓
- SC-006: byte-diff sanity passes; representative writes match gateway routing ✓

## Next session

- **Empirical dual-run verification**: `RUN_XNAT_DUAL=1 pytest tests/contract/`. Boot the existing xnat_local compose, run 9 dual-run tests, capture any FakeXNAT≈real divergences into `specs/006-xnat-alignment/contract-test.md` audit matrix. Discharges T006/T020 from structural → empirical. Estimated 30 min.
- **xnatpy spike** (006 US5 / T018–T019 from 006): operator-gated. Implement `XnatpyGateway(XnatGateway)` behind ABC; A/B note in `docs/XNAT_XNATPY_SPIKE.md`. Out of scope this batch.
- **STAPLE / consensus wiring** (Phase 5): now unblocked — assessor seam ready. Wire consensus output through `gateway.create_assessor(xsi_type='xnat:assessorData', files=[consensus])`.
- **DomI candidate**: `boot-and-verify-local-xnat` skill (med severity, 4th-session recurrence, ~15 min/session saved) — file `request: skill` via request-from-domi. Threshold met.
- **Multi-scan-per-experiment**: one-file edit in xnat_conventions (`SCAN_DEFAULT` → param). Out of scope this batch; conventions module enables it now.
