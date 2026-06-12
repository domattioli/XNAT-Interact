# Session Handoff — domattioli/XNAT-Interact · claude/codebase-improvement-plan-sGjxz@d082364 · 2026-06-05

**Task:** execute docs/INTEGRATION_TEST_RUNBOOK.md — real-XNAT push+pull round-trip vs original src/ code, synthetic no-PHI data
**Phase:** validation
**Progress:** complete — round-trip driven, original code assessed, not fixed (per instruction)
**Branch:** claude/codebase-improvement-plan-sGjxz
**Duration:** ~75 min
**Tool failures:** 1 push-rejection (remote diverged, resolved by rebase)
**Outcome:** complete (research/assessment goal met; original code found broken)

## Pre-flight

- branch_policy_conflict: accepted_override — DomI dev-branch directive named `claude/dazzling-albattani-Vwb59`; task context named `claude/codebase-improvement-plan-sGjxz` (where runbook lives). Worked on task-named branch (PR #23).
- mcp_scope_gap: no — XNAT-Interact in MCP scope
- label_scheme_mismatch: no

## What worked (top 3, with evidence)

1. Egress diagnosis by URL-path → found proxy blocks `/alpine/`+`/debian/` (403), allows `/ubuntu/`. Rebuilt xnat4tests image on `tomcat:9-jdk8-temurin-jammy` → real XNAT 1.9.3 @ localhost:8080 (commit 5ae39b8, `tests/integration/xnat_local/`).
2. Subagent dispatch per DomI policy — 4 agents (sonnet infra+2 roundtrip, haiku fallback-research); main session orchestrate/read/report only. No inline coding.
3. Round-trip confirmed #25 + surfaced 5 more real-only bugs invisible to 523 offline FakeXNAT tests (commits 3152b8d, f714b75 + logs).

## What didn't (top 3, with evidence)

1. xnat4tests stock build DOA in this env — `apk add supervisor` → `unsatisfiable constraints: supervisor (missing)` (alpine CDN 403). Runbook assumed open egress = sufficient; reality = path-segment denylist.
2. Original push code fatal on call #1: `attrs.mset` xnat_experiment_data.py:258 → `TypeError: quote_from_bytes() expected bytes` (pyxnat `_get_datatype()` None post-`create()`).
3. Original pull returns zero files (#25): synth filename `{s}_{e}_{scan}.dcm` ≠ real `generate_source_image_file_name`; scan label = `row['scan_type']` not `'0'`.

## Recurring frictions (from local corpus)

- none matched prior entry (73083e24) — first real-XNAT integration session

## Pain → skill table

| Pain | Severity | DomI issue | Saved-min/session |
|---|---|---|---|
| Egress probed by host not path → wasted build cycle on apt/apk repos | medium | null (one-time) | 10 |
| FakeXNAT fidelity gap hides real pyxnat-server failures from full green suite | high | null (repo-internal, not skill) | 30 |

## Pain corpus (machine-readable)

```yaml
session_id: claude/codebase-improvement-plan-sGjxz@d082364
repo: domattioli/XNAT-Interact
branch: claude/codebase-improvement-plan-sGjxz
date: 2026-06-05
duration_min: 75
issue_worked: domattioli/XNAT-Interact#25
phase: validation
outcome: complete

tool_failure_count: 1
workarounds:
  - other   # raw-REST staging for pull leg because pyxnat create() hits same mset TypeError

pre_flight:
  branch_policy_conflict: true    # DomI default branch != task branch; resolved to task branch (PR #23)
  mcp_scope_gap: false
  label_scheme_mismatch: false
  notes: "egress proxy blocks /alpine/ + /debian/ paths (403), allows /ubuntu/; discovered mid-build"

worked:
  - "ubuntu-base XNAT rebuild bypassed alpine egress block (commit 5ae39b8)"
  - "subagent dispatch per DomI policy; main session orchestrate-only"
  - "round-trip confirmed #25 + 5 more real-only bugs (commits 3152b8d, f714b75)"

didnt_work:
  - "xnat4tests stock build: apk add supervisor → unsatisfiable constraints (alpine CDN 403)"
  - "push fatal xnat_experiment_data.py:258 attrs.mset TypeError quote_from_bytes expected bytes"
  - "pull returns 0 files: download.py:222 synth filename + download.py:165 wrong scan label (#25)"

pain_points:
  - pain: "egress probed by host not URL-path; alpine/debian package paths 403 while ubuntu ok — cost a build cycle to learn"
    frequency: once
    severity: medium
    evidence: "RuntimeError Building 'xnat4tests' ... apk add ... supervisor (missing)"
    existing_skill_should_have_caught_it: none
    missing_skill_would_have_prevented_it: "none — process gap (probe egress by path before docker build)"
    domi_issue: null
    saved_time_estimate_min: 10
  - pain: "FakeXNAT test double diverged from pyxnat: 523 green offline tests, real push dies on call #1; 4 real-only failures (datatype-cache None, ID-vs-label, real filenames, DataError type) all invisible to double"
    frequency: once
    severity: high
    evidence: "xnat_experiment_data.py:258 TypeError; download.py #25; browse ID-vs-label"
    existing_skill_should_have_caught_it: none
    missing_skill_would_have_prevented_it: "none — repo-internal fix (gateway ABC + contract test per XNAT_MODEL.md §6.1)"
    domi_issue: null
    saved_time_estimate_min: 30
  - pain: "publish_to_xnat fatal: pyxnat _get_datatype() returns None after create() → quote(None) TypeError; blocks all uploads"
    frequency: once
    severity: critical
    evidence: "src/xnat_experiment_data.py:258 -> pyxnat/core/attributes.py:101"
    existing_skill_should_have_caught_it: none
    missing_skill_would_have_prevented_it: "none — src bug"
    domi_issue: null
    saved_time_estimate_min: 0
  - pain: "ConfigTables fresh-project bootstrap broken: utilities.py:634 catches only FileNotFoundError/KeyError/ValueError not pyxnat DataError; utilities.py:704 hardcodes user whitelist excluding admin"
    frequency: once
    severity: high
    evidence: "pyxnat.core.errors.DataError: Cannot get file: does not exists"
    existing_skill_should_have_caught_it: none
    missing_skill_would_have_prevented_it: "none — src bug"
    domi_issue: null
    saved_time_estimate_min: 0
  - pain: "#25 pull gap: download.py:222 synth filename != real generate_source_image_file_name; download.py:165 uses scan_type as scan label not '0'; browse enumerates internal IDs not labels"
    frequency: once
    severity: high
    evidence: "tests/integration/roundtrip_pull.log; download.py:222,165,193"
    existing_skill_should_have_caught_it: none
    missing_skill_would_have_prevented_it: "none — src bug (issue #25)"
    domi_issue: null
    saved_time_estimate_min: 0

actions_taken:
  votes_cast: []
  new_requests_filed: []
  closed_issues_flagged_for_reopen: []
  introspect_design_proposal_on_9: false

introspection_meta:
  what_worked: "path-based egress probe → ubuntu rebuild; honest no-fix assessment driven by real server"
  what_was_hard: "stock xnat4tests undeployable in restricted egress; pyxnat datatype-cache bug blocks push at line 1"
  duration_min: 75
```

## Next session

- File issues for: push `attrs.mset` blocker (line 258), ConfigTables bootstrap (634/704), browse ID-vs-label, line-506 guard, stale initialize_basic_metatable_items.py. Confirm evidence on existing #25.
- Highest-leverage fix: gateway ABC + pyxnat contract test (XNAT_MODEL.md §6.1) — collapses the FakeXNAT-fidelity pain class.
- Frequency gate: all pains one-time this corpus → no new DomI skill request filed (rule 11). Re-evaluate if real-XNAT friction recurs.
