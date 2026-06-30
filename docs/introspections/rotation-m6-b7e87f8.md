# Rotation introspection — #33 M6 + M5

- session: 2026-06-30T00Z (UTC slot 23), repo XNAT-Interact, maintenance track (MADMESHing#48 closed; repo not on mesh roster).
- model: claude-opus-4-8 orchestrator; code edits dispatched to haiku (M5) / sonnet (M6) subagents per coding-dispatch + AGENTS.md tiering; orchestrator verified before commit.

## Shipped to `development` (rolls into PR #49)
- **M6** (#33): `upload_annotation_set` rebuilt `manifest.json` from only the current upload (`overwrite=True`) while blobs are keep-all (`overwrite=False`) -> re-upload orphaned prior-version blobs; `download_annotation_set` silently dropped history. Fix fetches the existing manifest, merges by versioned `blob_filename` (existing-first), re-indexes sequentially; missing/corrupt prior manifest -> empty. +14 offline tests; existing `test_annotations_io.py` stays green. Source + test pushed via `push_files` (commit b7e87f8), both **byte-verified** vs local (`git diff FETCH_HEAD` empty).
- **M5 test** (#33): `tests/test_create_backup_no_cwd_litter_m5.py` staged (commit 9f7a827), byte-verified. The 5-line `src/utilities.py` source hunk is documented on #33 for a git-equipped apply (see push-wedge below).

## Pains (matrix rows; no new request:skill per #203)
1. **Large-file push wedge — narrowed.** Correction to the 2026-06-23 read: the GitHub API write path (`push_files`) DOES round-trip medium text files byte-perfect (io_xnat.py, 530 lines, landed clean). The real limiter is not an API size cap on *writes* — it is that the whole file must be hand-re-emitted into the tool call, which is disproportionate/risky for a truly-large critical module (utilities.py ~1426 lines) for a 5-line fix. So: small/medium fixes ship via API + byte-verify even while git push is 403; only truly-large-file fixes stay operator-deferred. Sharpens #44.
2. **git push 403 persists.** receive-pack still HTTP 403 to `development` this session (reads fine). Every rotation re-confirms it. Operator-only (#44 billing/proxy). ~3 tool calls re-confirming before pivoting to the API path.
3. **Slice picking.** Most #33 findings already merged on `development` or moot; needed the full comment thread + git log scan to find the 2 genuinely-open offline items (M5, M6). A live audit-status checklist pinned on #33 would cut this each rotation.

## Worked well
- `git diff FETCH_HEAD` byte-verify after every MCP push = cheap, definitive corruption gate; let me confidently ship a 530-line source file via the API.
- Verify-first on triage: read the whole #33 thread before touching anything -> avoided re-doing C1/M1/M2/M3/L4/M9/L6/C2 (already landed) and M4 (moot).
- Parallel subagent dispatch (M5 haiku + M6 sonnet, disjoint files) -> two verified fixes in one pass.
