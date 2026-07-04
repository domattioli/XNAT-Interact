# Introspection — xnat-fable@ac5718f (canonical handoff)

```yaml
session_id: xnat-fable@ac5718f
repo: domattioli/XNAT-Interact
branch: xnat-fable
date: 2026-06-12
duration_min: ~900 (07:00–22:00 UTC, one container restart mid-session)
issue_worked: XNAT-Interact#32 #33 #3 #10 #12 #21 #22 #26 #37 #39(filed) + PR#38
phase: implementation
outcome: complete

tool_failure_count: 12+
workarounds:
  - mcp-push-fallback        # git proxy HTTP 403 on receive-pack ALL session; reads fine
  - registry-mirror          # docker hub 429 -> mirror.gcr.io pull + retag
  - playwright-pin           # cdn.playwright.dev blocked -> pin playwright==1.56 to preinstalled chromium-1194

pre_flight:
  branch_policy_conflict: true               # operator mandated xnat-fable; overrides claude/* + development policy
  mcp_scope_gap: false
  label_scheme_mismatch: true                # XNAT-Interact absent from DomI sync-labels REPOS (commented #26)
  notes: "XNAT_IDENTITY_SALT + nibabel undocumented env prereqs; now in plan doc + requirements"

worked:
  - live XNAT 1.9.3 boot from repo harness; hub-429 beaten via mirror.gcr.io retag (STRESS_TEST_PLAN env facts)
  - verify-don't-trust on subagents: 3 Haiku 'fixed' assessor ops w/ stub-green; live gate caught all 3 (B/N kwarg, _exec out=, CObject)
  - blob-SHA mirror verification after same-size-different-bytes corruption caught on utilities.py line 916
  - operator clone+run as live gate: surfaced requirements CMake abort, dead streamlit launcher, missing demo backend
  - seed-set factory + lanes (volume 10x20 1.71s mean, concurrent, malformed, dedup) per #32 Q2/Q4

didnt_work:
  - git push 403 entire session -> every text artifact via MCP push_files solo-file + blob-SHA loop; binaries (17 PNGs) stuck local permanently
  - MCP push_files corrupts: smart-quote substitution when batching prose files; \uXXXX emoji escapes -> "no low surrogate" 400; fix = solo pushes + literal UTF-8
  - offline suite fake-green x9: build_server missing, xsiType ignored, scan_type-as-id, list_files absent, numeric-resource 404, label-404 assessors, dedup dead-wired, launcher ModuleNotFoundError, browse 6v5 columns
  - Haiku agents over-claimed success 4x (reverted own fix, deleted own tests, stub-verified); escalated to Sonnet w/ mandatory verbatim live gates

pain_points:
  - pain: git proxy receive-pack 403 while reads work; no in-session credential refresh path
    frequency: recurring-this-session
    severity: high
    evidence: "error: RPC failed; HTTP 403 curl 22" every push 07:34->22:00; PR#38 body documents
    existing_skill_should_have_caught_it: git-push-fallback (mode 4 worked for text; no binary path)
    missing_skill_would_have_prevented_it: binary-safe push fallback (e.g. release-asset or LFS route)
    saved_time_estimate_min: 90
  - pain: MCP push_files mutates content (smart quotes, emoji surrogates); byte-size check insufficient
    frequency: recurring-this-session
    severity: high
    evidence: utilities.py same-size-wrong-bytes line 916; "no low surrogate" 400; fixed via blob-SHA verify loops
    existing_skill_should_have_caught_it: mcp-binary-push (covers binary only, not text mutation)
    missing_skill_would_have_prevented_it: mcp-text-push verify (blob-SHA loop as skill)
    saved_time_estimate_min: 60
  - pain: FakeXNAT shape-drift vs pyxnat produced 9 fake-green production bugs
    frequency: recurring-across-sessions   # #33 M7 predicted it; this session confirmed at scale
    severity: critical
    evidence: PR#38 table; dual-run lane existed but fake fidelity gaps unmodeled
    existing_skill_should_have_caught_it: none
    missing_skill_would_have_prevented_it: contract-fidelity audit (fake API surface diffed vs real lib)
    saved_time_estimate_min: 240

actions_taken:
  votes_cast: []
  new_requests_filed: []                     # frequency gate: pains 1-2 first corpus occurrence; pain 3 tracked in #33/PR#38
  closed_issues_flagged_for_reopen: []
  introspect_design_proposal_on_9: false

introspection_meta:
  what_worked: live-server verification gates after every subagent claim
  what_was_hard: push path dead all session; every artifact hand-mirrored w/ encoding traps
  duration_min: 900
```

## Handoff — resume here

**State (everything blob-verified on `origin/xnat-fable` except 17 PNGs + 1 byte-drift JSON, both cosmetic):**
- Campaign P0–P8 complete: 9 live bugs fixed, suite 1154→1337 green, lanes green, dedup wired+verified, GAP-001/002/003 fixed in gateway.
- Guided UI (spec 013) + demo mode shipped: `XNAT_DEMO_MODE=1 streamlit run streamlit_guided.py` → auto-login, 3 synthetic surgeries browseable. Streamlit-Cloud-ready (entrypoint `streamlit_guided.py`, env `XNAT_DEMO_MODE=1`).
- Spec 012 (derived-data hierarchy) drafted; plan/tasks pending.
- PR #38 open (draft) = campaign ledger. Issue #39 filed (mr-typed historical data audit — operator action).

**Next session picks up:**
1. `specs/013` remaining slices: download/annotations wizards (stubs say "coming soon"), wire wizard image-upload to st.file_uploader (currently dir-path text input).
2. `specs/012` /plan + /tasks, then sharded-manifest implementation (fixes #33-M6 live-confirmed orphaning).
3. Open findings in STRESS_TEST_PLAN log: H4 lost-update guard misses live contention (0 LostUpdateError under race); JSESSION leak; esvSessionData server-type risk (#39).
4. Operator: enable Pages (Settings→Pages→GitHub Actions); deploy Streamlit Cloud; push PNGs from any healthy checkout (`git push` fast-forwards).

**Env recipe (ephemeral container):** `sudo dockerd &` → `tests/integration/xnat_local` compose up (hub 429 → `mirror.gcr.io` pull+retag tomcat) → `pip install nibabel` → env `XNAT_SERVER_URL=http://localhost:8080 XNAT_PROJECT_NAME=<P> XNAT_IDENTITY_SALT=<hex>` → lanes via `python -m tests.stress.lane_*`. Push = MCP push_files, ONE file per call, literal UTF-8 emoji, blob-SHA verify loop.
```
