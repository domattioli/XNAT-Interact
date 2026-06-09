# Session Handoff — domattioli/XNAT-Interact · claude/codebase-improvement-plan-sGjxz@93235a7 · 2026-06-05

**Task:** Phase 7 close-out + pivot real-XNAT verification (T006/T020) into a durable Phase 6 contract-test framework (grad-student workflow sim).
**Phase:** Phase 7 done → Phase 6 contract-test framework built (gateway ABC still pending).
**Progress:** complete — Phase 7 marked Built, contract-test design + 9 passing tests landed, fidelity audit matrix populated.
**Branch:** claude/codebase-improvement-plan-sGjxz (PR #23, draft)
**Duration:** ~40 min
**Tool failures:** 0 push rejections
**Outcome:** complete

## Pre-flight

- branch_policy_conflict: accepted_override — task branch (PR #23), not DomI default. Consistent with prior sessions.
- mcp_scope_gap: no
- label_scheme_mismatch: no

## What worked (top 3, with evidence)

1. **Reframe T006/T020 (one-off real-XNAT harness, blocked on env) → durable contract test.** Operator's grad-student-workflow ask mapped cleanly onto Phase 6's gateway-contract need. Result: a permanent FakeXNAT≈real-XNAT parity net instead of a throwaway verification. (contract-test.md + tests/contract/, commits 83cbfe2→93235a7)
2. **Subagent dispatch with tight spec + existing-infra inventory.** Pre-read synthetic_data.py, fake_xnat.py, conftest.py, test_publish_real_contract.py BEFORE dispatch; handed the haiku subagent exact file:line seams + reuse mandate. Net: 9 contract tests composing the Phase 7 per-defect tests into end-to-end workflow, 0 regressions (742→751). (task a768de9, 84 tool calls, 6.4 min)
3. **Honest gap surfacing.** Contract build exposed that publish_to_xnat has NO derived/assessor upload path. Subagent flagged it in T003 + audit matrix as a genuine Phase 6 design decision rather than papering over. Verified by spot-reading T003 before commit.

## What didn't (top 3, with evidence)

1. **Real-XNAT dual-run still deferred.** The `real_xnat` fixture is skip-marked (no Docker dep this pass); parity assertions are commented seams, not live. The framework LOCKS the workflow model but does not yet prove fake==real. Unblock = wire tests/integration/xnat_local/ image into the fixture.
2. **publish_to_xnat derived-upload gap.** T003 tests resource-level API directly because there's no `publish_to_xnat(assessor=...)` path. Assessor-vs-resource for derived data is unresolved — Phase 6 gateway must decide.
3. **stop-hook-git-check fired mid-flight again** (uncommitted changes while subagent ran). Same process gap as prior session; correct response = hold commit until subagent reports green. Recurred but is a known UX gap, not a code defect.

## Recurring frictions (from local corpus)

- Prior session (0bf43b4): FakeXNAT fidelity gap + THROWAWAY real-XNAT harness awaiting T006/T020. → THIS session converted that pending verification into a durable contract test; fidelity gap now tracked in an explicit audit matrix. Real-XNAT proof still pending (Docker wiring).
- stop-hook on partial in-progress tree: recurred 3rd session running. Process gap, not skill opportunity. Below action threshold (correct response is trivial: hold-commit).

## Pain → skill table

| Pain | Severity | DomI issue | Saved-min/session |
|---|---|---|---|
| stop-hook fires on partial tree mid-subagent | low | null (UX, not skill) | 2 |
| real-XNAT fixture needs Docker wiring (manual, repeated across sessions) | med | candidate: `bootstrap-vm`-adjacent local-XNAT fixture skill | 15 |

## Pain corpus (machine-readable)

```yaml
session_id: claude/codebase-improvement-plan-sGjxz@93235a7
repo: domattioli/XNAT-Interact
branch: claude/codebase-improvement-plan-sGjxz
date: 2026-06-05
duration_min: 40
issue_worked: domattioli/XNAT-Interact#25,#27,#28,#29,#30 (Phase 7 close) + Phase 6 contract-test
phase: phase-7-closeout + phase-6-contract-test
outcome: complete

tool_failure_count: 0
workarounds:
  - real_xnat_fixture_skip_marked   # Docker dual-run deferred; fake-side only this pass

pre_flight:
  branch_policy_conflict: true    # DomI default != task branch; resolved to PR #23 branch
  mcp_scope_gap: false
  label_scheme_mismatch: false
  notes: "github MCP server flapped (disconnect/reconnect) mid-session; no impact — PR read succeeded on retry"

worked:
  - "reframed blocked one-off real-XNAT verification into durable Phase 6 contract test; operator workflow ask mapped onto gateway-contract need"
  - "subagent dispatch with pre-read infra inventory + file:line seams; 9 contract tests, 0 regressions (742->751)"
  - "honest gap surfacing: publish_to_xnat has no assessor path; flagged in T003 + audit matrix, not papered over"

didnt_work:
  - "real-XNAT dual-run still deferred (real_xnat fixture skip-marked); framework locks workflow model but does not yet prove fake==real"
  - "publish_to_xnat derived/assessor upload path absent; assessor-vs-resource unresolved (Phase 6 gateway decision)"
  - "stop-hook-git-check fired mid-subagent-flight (3rd session); hold-commit pattern required"

pain_points:
  - pain: "real-XNAT integration fixture needs manual Docker (tests/integration/xnat_local/) wiring; recurs every session that wants real verification"
    frequency: 3rd session
    severity: med
    evidence: "T006/T020 blocked on env setup across two prior sessions; this session skip-marked rather than wire Docker fixture"
    existing_skill_should_have_caught_it: none
    missing_skill_would_have_prevented_it: "a local-XNAT-fixture skill (boot xnat_local image, health-poll, inject project+user, yield connection) would remove the repeated manual setup"
    domi_issue: null
    saved_time_estimate_min: 15
  - pain: "stop-hook fires on partial in-progress tree while subagent still running"
    frequency: 3rd session
    severity: low
    evidence: "stop-hook-git-check fired during subagent a768de9 mid-flight"
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
  what_worked: "reframe blocked verification into durable contract test; tight subagent dispatch; honest gap surfacing"
  what_was_hard: "real-XNAT proof still deferred (Docker); publish_to_xnat assessor gap; stop-hook on partial tree"
  duration_min: 40
```

## Next session

- **Phase 6 gateway ABC** (`src/services/xnat_gateway.py`): `XnatGateway` interface wrapping the 16-call pyxnat surface; `PyxnatGateway` impl; `FakeGateway` conforms. All call sites routed through it. Contract tests become the gateway regression net.
- **Resolve assessor-vs-resource** for derived data (T003 gap). Gateway must expose a derived-upload method; decide `xnat:assessorData` vs. labeled resource. Update publish_to_xnat / download to route through it.
- **Wire real-XNAT dual-run**: replace the skip-marked `real_xnat` fixture with a Docker `tests/integration/xnat_local/` boot+seed fixture; flip the commented parity-assert seams live. This finally discharges T006/T020.
- **Conventions module**: centralize `scan='0'`, `SOURCE_DATA-{uid}`, resource-label f-strings (Phase 6 US2).
- **DomI candidate**: local-XNAT-fixture skill (med severity, 3rd-session recurrence, ~15 min/session saved) — file a `request: skill` via request-from-domi if it recurs a 4th time, or now given threshold met. Operator call.
```
