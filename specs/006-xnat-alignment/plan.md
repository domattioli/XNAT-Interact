# Implementation Plan: XNAT Alignment (Phase 6)

**Branch**: `006-xnat-alignment` | **Date**: 2026-06-05 | **Spec**: [spec.md](spec.md)

## Summary

Behavior-preserving structural refactor. Introduce `XnatGateway` (ABC) wrapping the
16-call pyxnat surface; `PyxnatGateway` implements it; the existing FakeXNAT becomes
the ABC-conforming `FakeGateway`. Route all 6 call-site files through the gateway.
Extract path/label conventions into one module. Add `list_files` /
`download_resource` / `create_assessor` to the contract; use the first two to fix the
#25 download gap. Add an xnatpy spike behind the ABC (evaluation only). No new
user features; no on-server data migration.

## Technical Context

**Language**: Python 3.11. **Deps**: pyxnat (prod), pydicom/numpy/opencv (unchanged);
no new runtime dep. xnatpy added as an *optional/dev* dep for the spike only.
**Testing**: pytest, offline, the existing FakeXNAT (→ FakeGateway). **Reuses**:
`src/services/xnat_gateway.py` (today just `build_server`), `tests/fakes/fake_xnat.py`,
the 718-test suite as the regression net, `scripts/simulate_e2e.py`.

## Constitution Check

- **I PHI**: no new PHI surface; annotator-id + de-id paths untouched; assessor
  writes carry no patient identifiers. ✓
- **II Fail-soft**: gateway methods raise `FriendlyError` (wrap pyxnat exceptions
  once, centrally — better than today's scattered try/except). ✓
- **III Skill floor**: invisible to users (refactor); the one visible change is
  download *working* for whole surgeries — strictly friendlier. ✓
- **IV Offline-testable**: FakeGateway implements the ABC → every path testable
  with no server/PHI; grep test enforces no raw-pyxnat leak. ✓
- **V Config/secrets**: connection/creds flow unchanged (interactive `pwinput`,
  env>file>default); gateway never logs creds. ✓
- **VI Integrity/efficiency**: byte-identical writes (SC-006); download now
  count-verified (SC-004); enumeration replaces synthesized guesses. ✓

## Project Structure

```text
src/services/
├── xnat_gateway.py        # XnatGateway (ABC) + PyxnatGateway + build_gateway()
│                          #   (build_server() kept as thin shim → deprecate)
├── xnat_conventions.py    # NEW: subject_qs/exp_qs/scan_qs/resource labels/filenames
└── xnat_gateway_xnatpy.py # NEW (spike, not default-imported): XnatpyGateway

tests/fakes/
└── fake_xnat.py           # FakeXNAT -> implements XnatGateway (FakeGateway alias)

# Refactored call sites (call gateway + conventions, no raw pyxnat):
src/utilities.py                  # XNATConnection, ConfigTables pull/push, lost-update
src/xnat_experiment_data.py       # _generate_queries/_select_objects/publish_to_xnat
src/xnat_resource_data.py         # ORDataIntakeForm.push_to_xnat
src/annotations/io_xnat.py        # upload/download_annotation_set
src/delete_contents_of_server.py  # subject enumerate/delete, config nuke

app/logic/download.py             # use gateway.list_files/download_resource (#25)
app/pages/download.py             # one-click whole-surgery select (#25)

tests/test_xnat_gateway.py        # NEW: ABC contract + Pyxnat/Fake parity + grep-guard
docs/XNAT_XNATPY_SPIKE.md         # NEW: spike A/B note + go/no-go
```

## Approach (incremental, tests green at every step)

**Stage 1 — Define the contract (no behavior change).**
1. Write `XnatGateway` ABC = the 16 calls (from `docs/XNAT_MODEL.md §3`) +
   `list_files`, `download_resource`, `create_assessor`. Method names match intent,
   not pyxnat (e.g. `put_file`, `get_file_copy`, `set_attrs`).
2. Implement `PyxnatGateway` by *moving* today's raw calls verbatim into it.
   `build_gateway(url,user,password)` returns it; `build_server` becomes a shim.
3. Make `FakeXNAT` declare it implements `XnatGateway` (it already shadows the
   surface); add the 3 new methods. Add `tests/test_xnat_gateway.py`: Pyxnat vs
   Fake method-set parity, recorded-call equivalence on a scripted publish.

**Stage 2 — Extract conventions.**
4. `xnat_conventions.py`: `project_qs(name)`, `subject_qs(uid)`, `experiment_qs`,
   `scan_qs` (the `'0'` lives here), label builders, `ResourceLabel` constants
   (`SRC/INTAKE_FORM/ANNOTATIONS/CONFIG/BACKUPS`), filename builders
   (annotation blob pattern, manifest, intake JSON, config json).

**Stage 3 — Route call sites (one file per commit, suite green each time).**
5. `xnat_experiment_data.py` → gateway + conventions (publish path).
6. `xnat_resource_data.py` → gateway (intake form push).
7. `annotations/io_xnat.py` → gateway (annotation up/down) + conventions filenames.
8. `utilities.py` → gateway for ConfigTables pull/push + the lost-update re-fetch
   (expose the read the guard needs as a gateway method).
9. `delete_contents_of_server.py` → gateway (enumerate/delete).
10. Add grep-guard test (SC-001): fail if `import pyxnat`/`Interface(`/`.put_zip(`
    appears in `src/` outside `xnat_gateway*.py`.

**Stage 4 — Download fix (#25).**
11. Implement `list_files`/`download_resource` in PyxnatGateway via `CObject`
    enumeration / `Resource.get(dest_dir)`; mirror in FakeGateway (seed N files).
12. Rewrite `app/logic/download.py` to enumerate real files + count-verify; add
    whole-experiment iteration. Update `app/pages/download.py` for one-click
    surgery select (+ optional zip). New tests cover N-file + count-mismatch.

**Stage 5 — Assessor seam + xnatpy spike.**
13. `create_assessor` in ABC + Fake (+ Pyxnat impl against
    `.../experiments/{e}/assessors/{a}`); fake test only (no prod caller).
14. `xnat_gateway_xnatpy.py` spike implementing the ABC with `xnat.connect`;
    NOT default-imported. Write `docs/XNAT_XNATPY_SPIKE.md` (A/B + recommendation).

**Stage 6 — Verify.** Full suite + e2e sim green; byte-diff representative writes
(SC-006); update `specs/README.md` Phase-6 row; close/annotate #25.

## Coding dispatch

Per DomI policy, implementation = subagent. Tier by stage complexity:
- **Stages 1, 4 (ABC design, download semantics)** = harder → **sonnet** subagent.
- **Stages 2, 3, 5 (mechanical extraction/routing, seam stub)** = **haiku** subagent.
- Orchestrator (this session): plan, review each commit, integrate, keep suite green.

## Risks & Mitigations
- *Big refactor breaks a path* → one-file-per-commit, the 718-test suite is the net;
  grep-guard prevents silent raw-pyxnat leaks. **(primary mitigation)**
- *Lost-update guard regresses* → it needs a server re-fetch; model it explicitly as
  a gateway read and keep its dedicated test. Treat as a named acceptance check.
- *Byte-drift in writes* → SC-006 byte-diff test on representative put/insert calls
  before/after; PyxnatGateway moves calls verbatim, no reformatting.
- *Singleton XNATConnection coupling* → keep its public surface identical; gateway is
  injected where `Interface` was constructed, nothing else moves.
- *Scope creep into xnatpy migration* → spike is explicitly out-of-prod-path;
  go/no-go is a doc, not a switch, this phase.

## Out of scope (deferred)
- Actually migrating to xnatpy (gated on the spike's go/no-go).
- Migrating existing `ANNOTATIONS`-resource data into assessors.
- Multi-scan-per-experiment support (conventions module makes it a later one-file change).
- STAPLE algorithm (Phase-5 aggregator seam; consensus-as-assessor wires up then).
