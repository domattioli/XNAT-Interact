---
session: 2026-07-14-rotation
repo: XNAT-Interact
model: claude-opus-4-8
dispatches_used: 2
slice_outcome: net-zero (both picks already-done/in-flight; H8 commit reverted to avoid PR #54 collision)
branch: development
rolling_pr: "#49"
actions_minutes_used: 0
---

# Rotation introspection — 2026-07-14 (UTC hour 11 → XNAT-Interact)

## R5 pains

- **check-done ran against the wrong base.** Scanned `git log` on the local
  branch recreated from `main`; `main` lags `origin/development` by 118
  commits, so already-shipped `#33` fixes (e.g. C1 `aff2824`) read as open.
  → Always diff against `origin/development` before picking.
- **Open draft PRs not scanned before picking.** H8 was implemented + tested +
  committed to `development` before discovering open draft **PR #54** already
  owns H8 (+ M4/L1/S4/L5). Reverted (`b039b46`) to avoid a merge conflict on
  `app/logic/download.py`. → Scan open PRs (esp. #54) as part of check-done.
- Both dead-ends are the same class: state read on the wrong surface. Two
  Haiku dispatches spent on work that was redundant.

## State snapshot (for next session)

- **Offline `#33`/`#32` backlog is drained on `development` or in-flight on PR
  #54.** Fixed-with-test on `development`: C1 C2 H1 H2 H3 H4 H6 H7 M1 M2 M3 M5
  M6 M9 M10 L2 L3 L4 L6 S1 S2 S3. On PR #54 (not yet on `development`): H8 M4
  L1 S4 L5.
- No clean, offline, not-in-flight slice remained. Remaining open work is
  operator-gated or big-ticket: #26 label *apply* (needs `gh`), #44 CI billing,
  #34 Postgres, #24 trauma batch, #37 web app, #39 production audit (VPN/PHI).

## Next-session guidance

1. `git fetch origin development && git checkout -B development origin/development`
   (the harness `claude/*` branch sits at `main` = pre-fix; do not read state there).
2. Run check-done against `origin/development` AND `gh`/MCP open-PR list before
   picking any `#33` finding.
3. If PR #54 has merged, its H8/M4/L1/S4/L5 are on `development` — do not redo.
