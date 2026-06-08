# Session Handoff — domattioli/XNAT-Interact · claude/codebase-improvement-plan-sGjxz@ca21135 · 2026-06-08

**Task:** Phase 6 close-out → live GAP-001 fix → correctness audit → bug fixes → real-XNAT
real-world testing → data-identity/dedup grill → documentation → `009` spec/plan/tasks/analyze.
**Phase:** Phase 6 built (prior) + GAP-001 discharged live; `009-data-identity-dedup` specced &
build-ready (not yet built).
**Progress:** complete for the session's goals.
**Branch:** claude/codebase-improvement-plan-sGjxz (PR #23, draft). 38 commits this session.
**Duration:** long multi-arc session.
**Tool failures:** 0 push rejections. GitHub Pages workflow 422 (repo-settings, not code).
**Outcome:** complete.

## Pre-flight
- branch_policy_conflict: accepted_override — task branch (PR #23), not DomI default. Consistent.
- mcp_scope_gap: no. label_scheme_mismatch: no.

## What worked (top 5, with evidence)

1. **Live real-XNAT testing caught what the green suite couldn't.** Booting a real XNAT 1.9.3 and
   driving the actual `PyxnatGateway` through upload/download/derived round-trips surfaced **two
   real bugs the 844-test suite passed over**: `download_resource` returned one un-extracted zip
   instead of N files (M7), and the GAP-001 assessor `file.put` 404. Both reproduced, root-caused,
   fixed, re-verified live (`fa319ae`, `da74633`).
2. **GAP-001 root-caused by hypothesis-elimination against the live server.** Killed two wrong
   hypotheses (abstract xsiType; missing resource container) with targeted probes, then proved the
   fix: assessors are stored as experiments → PUT to the assessor's **direct** `/data/experiments/
   <id>/resources/<label>/files/<name>` URI works (200) where pyxnat's nested path 404s. Dual-run
   21/3 → **24/0**.
3. **Verify-first on the audit prevented regressions.** An independent Opus reviewer + the area
   audit reported many HIGH/CRITICAL bugs; verifying each against current source showed **most were
   already fixed** (H3/H4/H5, `:518`, C1/C2/H8 all struck). Only 3 were genuinely new (0x1006 tag
   collision, M2 VR, H6 non-functional guard). Verify-first stopped us "fixing" correct code into
   regressions. The reviewer itself rubber-stamped already-fixed code — source is the only ground
   truth.
4. **The grill turned a vague domain into a complete, scientifically-grounded spec.** Interrogating
   data/purpose one question at a time (grounding in code + the Thomas/Anderson/Long papers)
   produced the full identity/de-id/dedup/persistence model — and reframed deferred "code smells"
   (H1 SOPInstanceUID, StudyUID clobber) as **mission-critical** (they corrupt learning curves +
   leak ML splits). Documented in `DATA_MODEL.md` + `METADATA.md`.
5. **Chained boot-guard pattern beat the ephemeral environment.** The sandbox reaps Docker/XNAT
   between turns; running boot + health-poll + tests/probes in ONE background job (never handing a
   "healthy ✓" back across turns) made live work reliable.

## What didn't (top 4, with evidence)

1. **Ephemeral Docker bit repeatedly.** XNAT/daemon died between turns ~5×; each live step needed a
   fresh ~1-3 min reboot. Mitigated by the chained boot-guard, but it's friction. A persistent
   daemon or a longer-lived sidecar would remove it.
2. **Real-only test branches hide bugs.** A bogus 5-arg `experiment_qs()` call (`TypeError`) and
   the assessor 404 lived in `if real_pair:` branches that never run offline → green offline, red
   live. The dual-run lane is the only thing that exercises them; they rot silently otherwise.
3. **My own cwd bug cost two cycles.** Twice I left a chained command in `tests/integration/
   xnat_local` and ran `pytest tests/contract/` from there → "not found." Self-inflicted; fixed by
   an explicit `cd` before pytest.
4. **Commit-order / subagent-narration drift recurred.** Subagents occasionally mis-narrated
   ("test file existed" when they created it; "already fixed" when partially) — net outcomes were
   correct + test-verified, but the narration needed independent source checks.

## Recurring frictions (from local corpus)
- Real-XNAT verification: prior sessions deferred it (structural only). THIS session made it
  **empirical** (live boots, real round-trips, GAP-001 discharged 24/0). The Docker-ephemerality
  is the residual friction → the `boot-and-verify-local-xnat` skill candidate (now 5th-session).
- Audit/review false-positive rate: confirmed-then-already-fixed pattern → verify-first is the
  durable mitigation.

## Pain → skill table
| Pain | Severity | DomI issue | Saved-min/session |
|---|---|---|---|
| ephemeral Docker reaped between turns; manual reboot each live step | med | candidate: `boot-and-verify-local-xnat` (chained boot+health+test) | 15 |
| real-only test branches green offline / red live (rot) | med | null (process: run dual lane before trusting real branches) | 10 |
| audit/review reports already-fixed code as live bugs | low | null (process: verify-first against source) | 8 |

## Pain corpus (machine-readable)
```yaml
session_id: claude/codebase-improvement-plan-sGjxz@ca21135
repo: domattioli/XNAT-Interact
branch: claude/codebase-improvement-plan-sGjxz
date: 2026-06-08
issue_worked: phase-6 close-out + GAP-001 + audit (#33) + data-identity grill (#32) + specs/009
phase: phase-6-built + 009-specced
outcome: complete
tool_failure_count: 0
workarounds:
  - chained_boot_guard_for_ephemeral_docker   # boot+health+tests in one job, never hand back healthy across turns
pre_flight:
  branch_policy_conflict: true
  mcp_scope_gap: false
  label_scheme_mismatch: false
  notes: "github Pages workflow 422 (repo-settings: Pages not enabled / env unconfigured) — not a code defect"
worked:
  - "live real-XNAT testing caught 2 bugs the 844-green suite missed (M7 download-zip, GAP-001 assessor-404)"
  - "GAP-001 root-caused by hypothesis-elimination on the live server; fix = assessor-as-experiment direct URI; dual 21/3 -> 24/0"
  - "verify-first struck most audit HIGH/CRITICAL as already-fixed; only 3 genuinely new; prevented regressions"
  - "grill produced complete scientifically-grounded identity/de-id/dedup/persistence model (DATA_MODEL + METADATA)"
  - "chained boot-guard pattern made ephemeral-XNAT live work reliable"
didnt_work:
  - "ephemeral docker reaped ~5x between turns; reboot friction"
  - "real-only test branches (if real_pair) hide bugs from the offline suite"
  - "self-inflicted cwd bug cost two cycles (pytest from wrong dir)"
  - "subagent narration drift (mis-stated already-fixed/created); outcomes correct but needed source checks"
pain_points:
  - pain: "ephemeral Docker/XNAT reaped between turns; every live step needs a fresh reboot"
    frequency: 5th session
    severity: med
    evidence: "rebooted xnat_local ~5x this session via chained boot-guard"
    existing_skill_should_have_caught_it: none
    missing_skill_would_have_prevented_it: "boot-and-verify-local-xnat: one chained job boots+health-polls+runs the live suite, idempotent"
    domi_issue: null
    saved_time_estimate_min: 15
  - pain: "real-only test branches pass offline (skipped) but fail live; rot undetected without the dual lane"
    frequency: 2nd session
    severity: med
    evidence: "5-arg experiment_qs TypeError + assessor 404 both in if-real_pair branches; green offline"
    existing_skill_should_have_caught_it: none
    missing_skill_would_have_prevented_it: "a smoke that imports/compiles real branches, or a scheduled dual-run"
    domi_issue: null
    saved_time_estimate_min: 10
actions_taken:
  votes_cast: []
  new_requests_filed: []   # opened repo issues #32 #33 #34 (XNAT-Interact), not DomI skill-requests
  closed_issues_flagged_for_reopen: []
  introspect_design_proposal_on_9: false
introspection_meta:
  what_worked: "live testing > green suite; hypothesis-elimination root-cause; verify-first; grill-to-spec; chained boot-guard"
  what_was_hard: "ephemeral docker; real-only-branch rot; self-inflicted cwd; subagent narration drift"
```

## What shipped this session (38 commits)

- **Phase 6 built** (gateway ABC + conventions + assessor + dual-run + shim delete): `5c47bfe`..`51f7b52`.
- **Dual-run made real + repaired:** comparator fixes (`ec74928`), auth health-poll (`81c9e4d`),
  daemon-down→skip (`42bc078`).
- **Bug fixes (verify-first):** H2 0x1006 tag collision + M2 VR (`b02b28c`,`09d44d6`), H6 broken
  guard→`GatewayError` (`9ba1e6d`), H3/H4/H5 regression-tests (`a39af9b`); **struck as already-fixed:**
  C1/C2/H8, `:518`.
- **Live-found + fixed:** M7 download-extract (`fa319ae`), **GAP-001 assessor direct-URI** (`da74633`)
  + test-parity (`eb9df7d`,`e5efadd`) → dual **24/0**.
- **Documentation:** `DATA_MODEL.md` (`ecbe7ab`,`4e79a54`), `METADATA.md` + README (`0c0bdf2`).
- **Spec 009:** specify (`5c0d78f`) + plan/tasks/analyze (`ca21135`).
- **Issues:** #32 (dedup/identity grill), #33 (audit), #34 (Postgres migration).

**Final test state:** 844 passed / 7 xfailed offline · 24 passed / 0 failed live dual-run.

## Next session
- **Build `009`** off the build-ready plan/tasks (Stage 1 identity primitives → SQLite registry →
  dedup → ingest UID fix → pseudonym/versioning → verify). Highest risk = the Stage-4
  `xnat_experiment_data.py` metadata-block surgery (verify-first; seed-set dedup + live dual-run are
  the net). Honor analyze findings: T001b salt provisioning, T006 5-file ConfigTables shim
  (incl. `batch_upload.py`), T015 verify-first for patient de-id.
- **Advanced pixel de-id** (DATA_MODEL §4.2) — separate feature; device-profile mask + OCR quarantine.
- **GitHub Pages 422** — repo Settings → Pages (enable / configure environment); not a code fix.
- **DomI candidate:** `boot-and-verify-local-xnat` skill (med, 5th-session recurrence, ~15 min/session)
  — threshold met; file via request-from-domi if it recurs.
