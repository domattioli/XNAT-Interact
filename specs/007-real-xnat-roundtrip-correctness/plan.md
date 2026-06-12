# Implementation Plan: Real-XNAT Round-Trip Correctness (Phase 7)

**Branch**: `007-real-xnat-roundtrip-correctness` | **Date**: 2026-06-05 | **Spec**: [spec.md](spec.md)

## Summary

Five targeted correctness fixes so a real (local, synthetic) XNAT push+pull
round-trip actually completes. Each fix is minimal (patch at the named file:line),
independent, and ships with a test that FAILS on current code — closing the
FakeXNAT blind spot that let 523 offline tests stay green while every server-only
path was broken. No refactor; the structural rehoming behind a gateway is Phase 6
(`006-xnat-alignment`), which later absorbs the #25/#29 fixes unchanged.

## Technical Context

**Language**: Python 3.11. **Deps**: pyxnat (prod), pydicom/numpy/opencv
(unchanged); no new runtime dep. **Testing**: pytest, offline FakeXNAT + a
higher-fidelity fake variant (reproduces pyxnat post-`create()` empty
datatype-cache) and `@pytest.mark.real_xnat` integration tests (synthetic/local
only, skip-marked when no server). **Reuses**: PR #23 `tests/integration/`
harness + `tests/integration/xnat_local/` image, `tests/fakes/fake_xnat.py`,
`scripts/simulate_e2e.py`.

## Constitution Check

- **I PHI**: no new PHI surface; de-id + annotator paths untouched; all tests use
  synthetic no-PHI DICOMs against a localhost throwaway. Production server never
  contacted. ✓
- **II Fail-soft**: #28 widens first-run catch (more graceful, not less); #25 adds
  count-verify + friendly empty-resource no-op; #30 guards a tag. All raise/handle
  friendly. ✓
- **III Skill floor**: fixes make currently-broken flows work; only user-visible
  change is push/browse/download functioning + whole-surgery download — strictly
  friendlier. ✓
- **IV Offline-testable**: every fix has an offline higher-fidelity-fake test; the
  real-XNAT tests are an additional marked layer, not the only proof. ✓
- **V Config/secrets**: #28 replaces a hardcoded username list with
  membership/owner lookup — removes a code-embedded identity assumption; no creds
  in argv/logs. ✓
- **VI Integrity/efficiency**: #27 stops orphaned empty subjects; #25 count-verifies
  downloads; #29 surfaces real labels. Net correctness gain. ✓

## Project Structure

```text
# Patched source (minimal, at named file:line):
src/xnat_experiment_data.py   # #27 set datatype post-create() (exp/subj/scan); #30 guard InstanceNumber (~L506)
src/utilities.py              # #28 widen first-run except for pyxnat DataError (~L634); membership-based auth (~L704)
app/logic/download.py         # #25 enumerate real files + count-verify (~L222)
app/pages/download.py         # #25 one-click whole-surgery selection
src/initialize_basic_metatable_items.py  # #30 fix imports to src.utilities.ConfigTables OR remove

# Test fidelity (the deliverable that closes the blind spot):
tests/fakes/fake_xnat.py             # add post-create() datatype-cache behavior (reproduce #27); label-vs-ID enumeration (#29); N-file resources (#25)
tests/test_publish_real_contract.py  # NEW: #27 regression (fails pre-fix)
tests/test_configtables_bootstrap.py # NEW: #28 fresh-project + non-whitelisted user
tests/test_browse_labels.py          # NEW: #29 RF surfaced + labels not IDs
tests/test_download_full_series.py    # NEW: #25 N files + whole-surgery + count-verify + empty no-op
tests/test_session_metadata_guard.py # NEW: #30 missing-InstanceNumber DICOM
tests/integration/                    # PR #23 harness — re-run round-trip as @real_xnat marked check
```

## Approach (one defect per commit, suite green at every step)

Order = by severity / unblock-dependency. #27 first (push blocker); then #28
(bootstrap), #29 (browse), #25 (download), #30 (cleanup).

**Step 1 — #27 push blocker (P1, critical).**
1. In `publish_to_xnat`, after each `create()` (exp/subj/scan), set the handle's
   datatype so `attrs._get_datatype()` returns the xsiType (e.g.
   `exp_inst.attrs._datatype = f'xnat:{self.schema_prefix_str}SessionData'`), or
   pass attrs in the same `create(**{...})` call. Apply to all three handles.
   Make create **idempotent-upsert** (clarification): `if not exists` create, else
   reuse the handle; then set attrs / fill missing children. A re-publish over an
   orphaned/partial subject reuses it — never duplicates.
2. Extend FakeXNAT to reproduce the empty post-`create()` datatype cache so the
   bug is reproducible offline; add `tests/test_publish_real_contract.py` asserting
   no `TypeError` and full exp+subj+scan+SRC creation.

**Step 2 — #28 ConfigTables bootstrap (P1).**
3. Widen the first-run `except` (`utilities.py:634`) to include
   `pyxnat.core.errors.DataError` (key off "does not exist" semantics) → self-init.
4. Replace the hardcoded `['dmattioli','domattioli','stelong']` whitelist
   (`utilities.py:704`) with: project membership/owner lookup (reuse
   `_verify_login`'s project_handle.users()/owner path) **OR** a config/env-listed
   allowlist (clarification) — the allowlist is the escape hatch for service
   accounts (CI/admin) not on the project roster. No identities hardcoded.
5. `tests/test_configtables_bootstrap.py`: fresh project (no `database_config.json`)
   self-inits; a non-whitelisted user (`admin`) authorizes.

**Step 3 — #29 browse labels + type-agnostic enum (P1).**
6. Resolve subject **labels** (not internal IDs) in `browse._subject_names()` /
   the enumeration path; query downstream by label.
7. Enumerate experiments type-agnostically (project experiments listing with
   `xsiType` column) so `xnat:rfSessionData` surfaces.
8. `tests/test_browse_labels.py`: staged RF experiment appears; Subject column =
   label, not `*_S#####`.

**Step 4 — #25 full-series / whole-surgery download (P2).**
9. Replace synthesized `{subject}_{exp}_{scan}.dcm` (`download.py:222`) with real
   resource-file enumeration (`CObject` / `Resource.get`) + count-verify vs
   server `# Files`; friendly no-op on empty resource.
10. Add whole-experiment expansion: select a surgery once → iterate all scans →
    fetch all files; `app/pages/download.py` one-click affordance. Deliver as a
    **single zip with user-selectable contents** (clarification): content-scope
    picker (source-only / subset / all scans / + derived data), default = full
    source-image set; zip assembled from the enumerated real files.
11. `tests/test_download_full_series.py`: N-file scan → N files; whole-surgery →
    every scan; count-mismatch → FriendlyError; empty → no-op.
    *(Note: this is the same fix Phase 6 routes through the gateway — keep the test
    gateway-portable.)*

**Step 5 — #30 cleanup (P3).**
12. Guard `metadata.InstanceNumber` (`xnat_experiment_data.py:506`) like its
    siblings; default/derive index when absent.
13. Fix `initialize_basic_metatable_items.py` imports to current
    `src.utilities.ConfigTables`, or remove if redundant. Test: missing-tag DICOM
    builds; script imports (or absent).

**Step 6 — Verify.** Full offline suite + `simulate_e2e.py` green; re-run the PR #23
round-trip (`run_roundtrip_push.py` + `run_roundtrip_pull.py`) against the local
XNAT and confirm push completes + pull returns the full series. Update
`specs/README.md` Phase-7 row; annotate #25/#27/#28/#29/#30 with the fix commits.

## Coding dispatch

Per DomI policy, implementation = subagent (orchestrator reviews/integrates):
- **#27 (datatype-cache, fake fidelity), #25 (download semantics), #29 (enumeration)**
  = harder → **sonnet** subagent.
- **#28 (except-widen + membership swap), #30 (guard + import fix)** = **haiku**.
- Orchestrator (this session): plan, review each commit, keep suite green, drive
  the real-XNAT round-trip verification.

## Risks & Mitigations
- *Fix masks the bug instead of closing the blind spot* → each fix's test MUST fail
  at the parent commit (SC-006); enforce via "red first" before applying the patch.
- *#27 `_datatype` is a pyxnat private* → it is the documented minimal unblock;
  Phase 6's gateway + contract test is the durable fix. Comment the rationale; pin
  pyxnat version in the test.
- *#28 membership lookup differs across XNAT roles* → reuse the already-working
  `_verify_login` membership path rather than a new query; test owner + member +
  non-member.
- *#25/#29 overlap with Phase 6* → keep the tests gateway-portable so Phase 6
  re-homes the logic without rewriting them; if Phase 6 lands first, these steps
  collapse to "verify through gateway".
- *Real-XNAT test flakiness in CI* → mark `@real_xnat`, skip when no
  `XNAT_TEST_URL`; the offline higher-fidelity-fake tests are the always-on gate.

## Out of scope (deferred)
- The gateway ABC + conventions module + xnatpy spike (Phase 6).
- Multi-scan-per-experiment modeling beyond what whole-surgery download needs.
- Migrating existing on-server data; reconciling pre-existing orphaned subjects
  beyond "re-publish does not duplicate".
- Label taxonomy sync from DomI (separate `type: chore` / `domi-sync` task noted in #25).
