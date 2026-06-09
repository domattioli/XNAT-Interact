```yaml
session_id: development@f826974
repo: domattioli/XNAT-Interact
branch: development
date: 2026-06-09
duration_min: 120
issue_worked: domattioli/XNAT-Interact#37
phase: planning
outcome: complete

tool_failure_count: 2
workarounds:
  - mcp-push-fallback   # tag push blocked by env git proxy -> pushed baseline branch instead

pre_flight:
  branch_policy_conflict: false
  mcp_scope_gap: false
  label_scheme_mismatch: false
  notes: >
    Session began as ADMESH hour-10 routine (PR #139, merged), then operator
    pivoted to XNAT-Interact (not a DomI consumer) for site/plan/CI work.

worked:
  - FakeXNAT + connect_factory DI seam already in repo -> prototyping plan grounded in real code (tests/fakes/fake_xnat.py, app/logic/auth.py).
  - conda->pip CI migration went green after 3 real failures diagnosed in turn (PR #31).
  - Plan re-homed from a handoff doc into specs/010-web-reachable-app/spec.md on operator steer.

didnt_work:
  - Annotated AND lightweight tag pushes both fail "send-pack: unexpected disconnect while reading sideband packet"; env git proxy blocks refs/tags/* (branch pushes fine). No MCP create-tag/ref/release tool. Worked around with branch baseline-v0.1.0@1ea13e0.
  - caveman plugin not loaded at container start -> Skill caveman:caveman = "Unknown skill" all session; settings.json fix only takes effect next container.

pain_points:
  - pain: Env git proxy refuses all tag pushes; no MCP tool to create a tag/ref/release, so a requested "tagged baseline" cannot be created from a cloud session.
    frequency: once
    severity: medium
    evidence: "send-pack: unexpected disconnect while reading sideband packet; ls-remote shows tag absent; branch push to same proxy succeeds"
    existing_skill_should_have_caught_it: none
    missing_skill_would_have_prevented_it: tag-via-api-fallback
    domi_issue: null
    saved_time_estimate_min: 10
    tokens_wasted: ~8 tool calls retrying 3 transport variants (http.version, postBuffer, protocol.version) + lightweight fallback before concluding proxy blocks the tags namespace
  - pain: caveman style mode unavailable because consumer .claude/settings.json omitted caveman@caveman from enabledPlugins; only ADMESH (and XNAT, no settings.json at all) were affected, not the others.
    frequency: recurring-across-sessions
    severity: low
    evidence: "Skill caveman:caveman -> Unknown skill; /caveman -> Unknown command; DomI #168 reopened"
    existing_skill_should_have_caught_it: none
    missing_skill_would_have_prevented_it: none — process gap (per-repo enabledPlugins audit)
    domi_issue: "#168"
    saved_time_estimate_min: 5
    tokens_wasted: low

actions_taken:
  votes_cast: []            # probation: no skill votes
  new_requests_filed: []    # probation: routed to corpus instead
  closed_issues_flagged_for_reopen: ["#168"]   # DomI #168 reopened with fresh evidence
  introspect_design_proposal_on_9: false

introspection_meta:
  what_worked: Grounding the web-app prototyping plan in the repo's existing FakeXNAT + DI seam made the spec concrete, not hand-wavy.
  what_was_hard: Cloud env's git proxy silently blocks tag pushes (looks like a flaky disconnect, not a policy block) — cost several retries to diagnose.
  duration_min: 120
```

## Narrative (this session)

- **ADMESH hour-10 routine** (first half): fixed #133 demo custom-domain crash (spec 028), migrated branch `daily-maintenance`→`development` per branching.md #196, opened PR #139 (merged), de-flaked the octree perf test (slow-marker → `tests-slow` lane). Corpus for that half: `ADMESH/docs/introspections/2026-06-06-demo-133-fix.md`.
- **XNAT-Interact** (second half, operator-directed): clarified Pages-site-vs-interface; wrote `specs/010-web-reachable-app/spec.md`; filed brainstorm issue #37; migrated CI conda→pip + fixed pytest collection (PR #31, green); enabled caveman/DomI plugins in `.claude/settings.json` (ADMESH + XNAT); reopened DomI #168 with fresh caveman-not-loaded evidence; attempted a `v0.1.0` baseline tag (blocked → `baseline-v0.1.0` branch).

## Next-session pickups
1. Resolve b1-vs-b2 hosting on #37; then spec 010 `plan.md`/`tasks.md`.
2. Land L1 walking skeleton: onboarding-page `AppTest` vs FakeXNAT in CI (spec 010 SC-001).
3. Operator: cut real `v0.1.0` tag at `1ea13e0`; delete `baseline-v0.1.0`.
4. Confirm Pages Source = "GitHub Actions" (keeps `pages.yml` green).
