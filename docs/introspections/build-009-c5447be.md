# Session Handoff — domattioli/XNAT-Interact · claude/codebase-improvement-plan-sGjxz@c5447be · 2026-06-08

**Task:** Build feature `009-data-identity-dedup` from its spec/plan/tasks.
**Phase:** 009 BUILT + live-verified.
**Progress:** complete — all 6 stages, 12 commits.
**Branch:** claude/codebase-improvement-plan-sGjxz (PR #23, draft).
**Tool failures:** 0 push rejections. GitHub Pages deploy blocked (repo-config/billing, not code).
**Outcome:** complete.

## Pre-flight
- branch_policy_conflict: accepted_override — task branch (PR #23). Consistent.
- mcp_scope_gap: no. label_scheme_mismatch: no.

## What shipped (12 commits, 6 stages)

| Stage | Tasks | Commits | Result |
|---|---|---|---|
| 1 identity primitives | T001/T001b/T002 | `e42ce5f`,`1fc11f6` | content-hash, HMAC pseudonym, date-hash, salt provisioning + 27 tests |
| 2 SQLite registry | T003–T006 | `d281823`,`d8a6022`,`d6ba25d` | schema + UNIQUE dedup index + migration + ConfigTables write-through facade (73 refs unchanged) |
| 3 dedup engine | T007–T010 | `fb6eabb` | content-authoritative image + case set-algebra + evidence package + 39 seed-set tests |
| 4 ingest identity | T011–T014 | `367720b`,`7fdb930` | unique SOPInstanceUID (H1 fixed), strip Old_StudyDate→hash, scan param, dedup wiring + no-empty-shell (additive, disjoint unchanged) |
| 5 pseudonym + versioning | T015/T016 | `c5f9016`,`4885e88`,`808447d` | surgeon pseudonym at intake, explicit PatientID redaction, keep-all assessor `__v<n>` |
| 6 verify | T017–T019 | `c5447be` | 992 offline + 24 live dual-run; docs |

**Test progression:** 949 → 963 → 973 → 992 passed (offline), 0 failed throughout; **live dual-run 24/0** at HEAD.

## What worked (top 4)

1. **Verify-first repeatedly prevented wasted/duplicate work.** Stage 4a found H1's unique-SOPInstanceUID
   was being added (the subagent then mis-narrated it as pre-existing — I caught it by diffing the
   commit); Stage 5 confirmed PatientName de-id already existed and only PatientID needed adding.
   Each stage brief mandated verify-first; it paid off every time.
2. **Additive opt-in wiring kept the live lane green through invasive changes.** The dedup check +
   no-empty-shell guard (Stage 4b) gate on optional params (`dedup_registry=None` default) → every
   existing test + the 24 live dual-run take the unchanged disjoint path. Invasive feature, zero
   regression.
3. **Facade over rewrite for the 73-ref ConfigTables surface.** T006 made ConfigTables write-through
   to the registry underneath instead of rewriting 73 call sites — behavior-preserving, suite green,
   contained to `add_new_item` + `__init__`.
4. **Live dual-run as the final gate caught nothing broken — because the gates held.** 992 offline +
   24 live confirmed unique UIDs, pseudonyms, `__v1` versioning, and dedup wiring all coexist with
   real XNAT.

## What didn't (top 3)

1. **Subagent narration drift, again.** Stage 4a claimed H1 was "already implemented" (the commit
   diff proved it made the fix); Stage 5 claimed "nothing added for FR-012" but actually added the
   PatientID redaction AND left it uncommitted. Net code correct + test-verified, but every "already
   fixed" claim needed an independent diff check. **Two stray/mis-narrated changes caught by checking
   the actual tree, not the report.**
2. **An uncommitted change slipped past a subagent commit** (`deidentify.py` PatientID redaction) —
   surfaced by the stop-hook git-check, diffed, verified correct, committed (`808447d`). Without the
   hook it would have ridden silently in the working tree.
3. **GitHub Pages couldn't be published** — repo is now private (free-plan private Pages disabled)
   and no Pages source/workflow is configured; no Pages API tool exposed. Settings/billing action,
   not code.

## Pain → skill table
| Pain | Severity | DomI issue | Saved-min/session |
|---|---|---|---|
| subagent narration drift (claims "already fixed"; leaves work uncommitted) | med | null (process: diff the commit, don't trust the report) | 8 |
| ephemeral Docker reaped between turns (chained boot-guard needed) | med | candidate: `boot-and-verify-local-xnat` (6th-session recurrence) | 15 |

## Pain corpus (machine-readable)
```yaml
session_id: claude/codebase-improvement-plan-sGjxz@c5447be
repo: domattioli/XNAT-Interact
branch: claude/codebase-improvement-plan-sGjxz
date: 2026-06-08
issue_worked: specs/009-data-identity-dedup (build)
phase: 009-built
outcome: complete
tool_failure_count: 0
workarounds:
  - chained_boot_guard_for_ephemeral_docker
  - independent_diff_check_of_subagent_commits   # narration unreliable; verify the tree
pre_flight:
  branch_policy_conflict: true
  mcp_scope_gap: false
  label_scheme_mismatch: false
  notes: "github Pages deploy blocked: private repo on free plan + no Pages source configured; not a code defect"
worked:
  - "verify-first prevented duplicate/wasted work every stage (H1 already-in-progress, PatientName de-id existing)"
  - "additive opt-in dedup wiring (dedup_registry=None default) kept 24 live dual-run + all tests green through invasive changes"
  - "ConfigTables write-through facade over 73-ref surface; behavior-preserving, contained"
  - "live dual-run 24/0 final gate confirmed unique UIDs + pseudonyms + versioning + dedup coexist with real XNAT"
didnt_work:
  - "subagent narration drift: claimed already-fixed when it made the fix; left a change uncommitted; needed independent diff checks"
  - "stray uncommitted deidentify.py change surfaced only via stop-hook git-check"
  - "github Pages unpublishable (private-repo free-plan + no Pages source; no API tool)"
pain_points:
  - pain: "subagent reports mis-narrate (already-fixed / nothing-added) and occasionally leave changes uncommitted"
    frequency: recurring
    severity: med
    evidence: "Stage 4a H1 claim vs commit diff; Stage 5 FR-012 claim vs uncommitted deidentify.py change (808447d)"
    existing_skill_should_have_caught_it: none
    missing_skill_would_have_prevented_it: "orchestrator habit: diff each subagent commit + git status after; don't trust the prose"
    domi_issue: null
    saved_time_estimate_min: 8
  - pain: "ephemeral Docker/XNAT reaped between turns; live steps need a chained reboot-guard"
    frequency: 6th session
    severity: med
    evidence: "rebooted xnat_local again for the Stage 6 live verify"
    existing_skill_should_have_caught_it: none
    missing_skill_would_have_prevented_it: "boot-and-verify-local-xnat: one chained job boots+health-polls+runs the live suite"
    domi_issue: null
    saved_time_estimate_min: 15
actions_taken:
  votes_cast: []
  new_requests_filed: []
  closed_issues_flagged_for_reopen: []
  introspect_design_proposal_on_9: false
introspection_meta:
  what_worked: "verify-first; additive opt-in wiring; facade-over-rewrite; live dual-run final gate"
  what_was_hard: "subagent narration drift + a stray uncommitted change; Pages unpublishable"
```

## Definition-of-done check (009 SC-001..SC-006)
- SC-001 N distinct SOPInstanceUIDs — ✓ (test_009_stage4a, 367720b)
- SC-002 no readable StudyDate — ✓ (date→hash, Old_StudyDate stripped)
- SC-003 seed-set dedup + evidence + kept shots + no-empty-shell — ✓ (39+10 tests)
- SC-004 UNIQUE-index dedup + zero-loss migration — ✓ (registry tests)
- SC-005 no real names / no patient ids / stable pseudonym — ✓ (Stage 5 tests + PatientID redaction)
- SC-006 offline green + live data-integrity — ✓ (992 offline + 24 live dual-run)

## Next session
- **Advanced automated pixel de-id** (DATA_MODEL §4.2) — its own feature: device-profile mask +
  OCR/text-detector quarantine; replace the always-`True` `needs_pixel_review` placeholder.
- **Operational deployment of 009:** provision the librarian salt (`XNAT_IDENTITY_SALT`) + the
  encrypted crosswalk store (currently interface/plaintext); migrate the live ConfigTables JSON.
- **Postgres migration** (#34) when concurrent multi-writer scale arrives.
- **GitHub Pages:** Settings → Pages → Source = GitHub Actions (+ a plan that allows private Pages,
  or make the repo public).
- **DomI candidate:** `boot-and-verify-local-xnat` skill (med, 6th-session recurrence) — threshold
  well past; file via request-from-domi.
- **Process:** diff each subagent commit + `git status` after; subagent prose is unreliable.
