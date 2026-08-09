# Contract: Disposition Ledger (014)

File: `specs/014-validate-fix-backlog/ledger.md`. Append-only (supersede rows, never rewrite).

## Header

```
Baseline (pre-fix) commit: <sha>   # merge-base with development at feature start (D4)
Reproduce a pre-fix failure: git worktree add /tmp/prefix <sha> && cd /tmp/prefix && pytest <test_path> -x
```

## Findings table

| finding | severity | disposition | test | baseline evidence | fix commit | rationale |
|---|---|---|---|---|---|---|

- Every #33 finding gets exactly one live row — **all ~29**, not just the active-scope subset (SC-001, rescoped 2026-07-07).
- Disposition vocabulary (post-audit): `fixed-with-test` \| `fixed-no-test` (backfill pending) \| `still-open` (fix pending) \| `not-a-bug` \| `unclear` (investigation pending).
- `fixed-with-test` rows MUST fill test + baseline evidence + fix commit. For findings already fixed before this feature, `fix commit` is the historical commit (found via `git log`), not a new commit.
- `wont-fix` / `not-a-bug` rows MUST fill rationale (1–3 sentences).
- `still-open`/`unclear` rows transition to `fixed-with-test` as this feature's tasks land.

## Fix-candidates table

| PR | findings claimed | terminal state | pointer |
|---|---|---|---|

- Every PR in {#38, #40–#43, #45–#48, #50} gets a row (SC-004).
- Duplicate claims (M1 ×3, M1/M9 overlap) resolve to one `fixed-with-test` finding row; losing variants get `superseded-by-consolidation` + rationale for the chosen resolution.
