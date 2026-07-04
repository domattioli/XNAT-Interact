# Rotation introspection — #33 S3 + M5

- session: 2026-06-23T15Z, repo XNAT-Interact, maintenance track (MADMESHing#48 closed; repo not on mesh roster).
- model: claude-opus-4-8 orchestrator; code edits dispatched to haiku subagents per coding-dispatch policy.

## Shipped
- **S3** (#33): `AnnotationSet.from_manifest` bypassed `add()` validation -> annotator_id unvalidated on load. Fix routes load through `add()`. +4 offline tests. Pushed via contents API (commit 1b96bb9), byte-verified vs local. Rolls into PR #49 (development->main).

## Done locally, push-blocked
- **M5** (#33): `create_backup` dead local write -> CWD litter on every push. Fix removes dead read+write. +2 offline tests. Lives in `src/utilities.py` (~1420 lines). UNSHIPPABLE this session: git push 403 (receive-pack) AND contents-API can't round-trip a 90kB file. Patch handed to operator on #33.

## Pains (matrix rows; no new request:skill per #203)
1. **Large-file push wedge.** Cloud session can't ship any fix in a file too big for the contents-API round-trip once git push is 403. Small files OK via push_files. -> large-file fixes (esp. the core `utilities.py`) are operator-deferred until receive-pack is restored. Sharpens #44. ~6 wasted tool calls confirming both channels dead before pivoting.
2. **Picking the slice.** Half the #33 MEDIUM/LOW findings were already merged on `development` (C1/M1/M2/M3/L4/M9/L6/C2) or moot (M4). Cost: several greps to avoid duplicate work. A live audit-status checklist on #33 would cut this; the snapshot posted this session is a partial substitute.

## Worked well
- Verify-don't-assume on findings: M2/C2/M4 turned out already-fixed/moot before touching them -> no wasted edits.
- `git diff FETCH_HEAD` byte-verify after MCP push = cheap, definitive correctness gate.
