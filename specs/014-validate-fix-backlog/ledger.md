# Disposition Ledger: #33/#32 Correctness Backlog

Baseline (feature start) commit: `dea6687bf72632afbc7f18d61a7f14789cc4c0d2` (origin/development tip, also this branch's merge-base with development)
Audit date: 2026-07-07. Reproduce a pre-fix failure: `git worktree add /tmp/prefix <commit-from-row> && cd /tmp/prefix && pytest <test_path> -x`

## Findings

| finding | severity | disposition | test | evidence / fix commit | rationale |
|---|---|---|---|---|---|
| C1 | CRITICAL | fixed-with-test | download resource-label tests | `aff2824` fix: parameterize download resource label | Wrong hardcoded SRC label replaced with per-row `resource_label` override + enumeration fallback |
| C2 | CRITICAL | fixed-with-test | `tests/test_download_path_traversal.py`, `tests/test_putzip_rf.py` | present on development tip | Unsafe filenames blocked with `FriendlyError`, download refused rather than writing outside dest |
| H1 | HIGH | fixed-with-test | UID-generation tests (session/experiment data suite) | per-frame `SOPInstanceUID` via md5 seed; StudyInstanceUID/SeriesInstanceUID preserved in private tags before overwrite | DICOM uniqueness restored |
| H2 | HIGH | fixed-with-test | `tests/test_session_metadata_bugs_cluster3b.py::test_fixed_tag_addresses_are_unique` | distinct private tag addresses 0x1001/0x1003/0x1004/0x1005/0x1006/0x1007/0x1000/0x1008 | No more clobber between StudyTime/StudyInstanceUID stash |
| H3 | HIGH | fixed-with-test | `tests/test_bug_fixes_h3_h4_h5.py::TestH3LostUpdatePropagates` | `LostUpdateError` re-raised distinctly in `push_to_xnat` | Swallowed-exception bug fixed |
| H4 | HIGH | fixed-with-test | `tests/test_bug_fixes_h3_h4_h5.py::TestH4FingerprintRefreshedAfterPush` | `_server_fingerprint_at_load` refreshed post-push (`src/utilities.py:1046-1048`) | TOCTOU stale-baseline half fixed |
| H5 | HIGH | fixed-with-test (partial) | `tests/test_bug_fixes_h3_h4_h5.py::TestH5IsOpenStaleAfterClose` | `is_open`/`close()` unified on `_is_open` | Attribute-mismatch half fixed; singleton-lock sub-issue still open — see follow-up (T019) |
| H6 | HIGH | fixed-with-test | `tests/test_xnat_gateway.py` (assessor op tests) | parent `exists()` guard + `PurePosixPath` label sanitization in `PyxnatGateway` (~779-807) | Guard added; note: happy-path tests only, rejection path not explicitly tested — acceptable, core fix present |
| H7 | HIGH | fixed-with-test | `tests/test_download_scan_enumeration.py`, `tests/test_download_full_series.py::test_whole_surgery_all_scans_downloaded` | scan enumeration fallback in `app/logic/download.py:227-252` | Whole-surgery download now enumerates all scans |
| H8 | HIGH | **still-open** | *(pending T005-T008)* | `app/logic/download.py:520-638` `assemble_zip` still `continue`s on empty resource (no count-verify) and has no try/except around the zip-write loop | Active scope — Phase 3 |
| M1 | MEDIUM | fixed-with-test | `tests/test_filename_instance_cap.py` | `59dd0b1`: supports up to 9999 instances | |
| M2 | MEDIUM | unclear | none found | private-tag VR audit not directly re-verified this pass | Low risk; not in active scope — flagged for a future pass, not blocking |
| M3 | MEDIUM | fixed-with-test | `tests/test_session_df_index_m3.py` | `d18b0dd`: `sort_values(...).reset_index(drop=True)` | |
| M4 | MEDIUM | **still-open** | *(pending T009)* | `src/xnat_experiment_data.py:1171` still `df['IS_VALID'] == False`, misses NaN | Active scope — Phase 4 |
| M5 | MEDIUM | fixed-with-test | `tests/test_create_backup_no_cwd_litter_m5.py`, `tests/test_backup_no_cwd_litter.py` | `0bdee7d`-family: backup no longer writes to CWD | |
| M6 | MEDIUM | fixed-with-test | `tests/test_annotation_manifest_merge_m6.py` | `b7e87f8`: manifest merge preserves prior blobs | |
| M7 | MEDIUM | **fixed-no-test** | *(pending T014)* | `src/services/xnat_gateway.py:700-729` isolates downloads into `_xnat_download` subdir | Fix present, no dedicated regression test — Phase 5 backfill |
| M8 | MEDIUM | **fixed-no-test** | *(pending T015)* | `app/logic/download.py:344-419` computes live per-resource count, falls back to stale count only when unavailable | Fix present, race-window untested — Phase 5 backfill |
| M9 | MEDIUM | fixed-with-test | `tests/test_delete_server_names.py::test_delete_metatables_calls_correct_resource` | `082ed2e`: targets `config`/`database_config.json` | |
| M10 | MEDIUM | **fixed-no-test** | *(pending T016)* | FriendlyError infra wraps most `except Exception` sites; two cited call sites (`app/logic/download.py` legacy path, `src/utilities.py` first-run catch) not dedicated-tested | Phase 5 backfill |
| L1 | LOW | **still-open** | *(pending T010)* | `src/xnat_scan_data.py:159` `ImplementationClassUID` still derived from session/parent UID | Active scope — Phase 4 |
| L2 | LOW | fixed-with-test | `tests/test_metadata_created_by_l2.py` | `8338fb4`: `CREATED_BY` preserved via `setdefault`, `LAST_MODIFIED_BY` added | |
| L3 | LOW | fixed-with-test | `tests/test_get_name_uid_guard_l3.py` | `8338fb4`: `get_name` guards on UID column | |
| L4 | LOW | fixed-with-test | `tests/test_xnat_gateway.py`, `tests/test_xnat_gateway_fake.py` | `3bb125b`: `disconnect()` guards `self.server is None` | |
| L5 | LOW | **unclear** | *(pending T013)* | `app/logic/download.py` legacy fallback + real-download path; zip/cleanup ordering not confirmed safe or unsafe | Active scope — Phase 4 (investigate then disposition) |
| L6 | LOW | fixed-with-test | `tests/test_browse_file_count_l6.py` | `f457a79`: `_file_count` queries all resources via `/scans/{scan}/files` | |
| S1 | SUSPECT | **still-open** | *(pending T011)* | `src/xnat_scan_data.py:183` `ds.Rows, ds.Columns = gray_img.shape` still 2-tuple unpack | Active scope — Phase 4 |
| S2 | SUSPECT | still-open (not actioned) | none | `src/annotations/importers/dicom_seg.py:85` `SegmentationType` still read at dataset level | Out of this feature's scope (research D3) — follow-up issue filed (T019) |
| S3 | SUSPECT | fixed-with-test | `tests/test_annotation_from_manifest_validates_s3.py` | `1b96bb9`: `validate_annotator_id` called in `from_manifest` | |
| S4 | SUSPECT | **still-open** | *(pending T012)* | `src/annotations/io_xnat.py:130` `startswith("/project")` still coincidentally matches `/projects/` | Active scope — Phase 4 |
| #32-identity | — | fixed-with-test | `tests/test_dedup.py`, `test_dedup_evidence.py`, `test_dedup_wiring.py`, `test_image_hash.py`, `test_identity.py` | `src/services/dedup.py`: sha256 authoritative, StudyUID corroborating, pHash advisory-only | Layered identity design fully implemented |
| #32-empty-shells | — | fixed-with-test | `tests/test_publish_no_empty_shells.py` | `src/xnat_experiment_data.py` ~630-716: create+rollback-on-exception pattern | Cleanup-after-creation, not prevent-up-front, but net invariant holds |
| #32-fixture-factory | — | fixed (n/a) | `tests/synthetic_data.py` | Factory functions present (PHI/dicom/multiframe synthetic cases) | No dedicated test needed — it is the fixture |
| CI-duplication | — | fixed-with-test | local suite run 2026-07-07 | `ci-lite.yml` kept canonical (its own header + `python-package.yml`'s pointer both named it); `tests.yml` + `python-package.yml` deleted. Also fixed a masking bug found during consolidation: `ci-lite.yml`'s test step had `continue-on-error: true`, making the job report success regardless of pytest's actual exit code — removed. Local run of the exact `pytest` invocation: 1388 passed, 24 skipped (env-gated: tesseract/presidio/pg absent), 7 xfailed (intentional red-state docs), 0 failures | #45 vs #47: neither PR's literal diff was applied — the repo had already converged on a third shape (keep ci-lite.yml, not delete-into-tests.yml per #45, nor fix-python-package.yml per #47); both PRs closed with pointer to this decision (T017) |

## Fix candidates (draft PRs / issues)

| PR/Issue | Findings claimed | Terminal state | Pointer |
|---|---|---|---|
| #38 (xnat-fable campaign) | Login, download enumeration, #32 dedup, M1/M5/M9, stress lanes | superseded-by-commit | Merged via `5e3060e Merge pull request #38`; content landed on development |
| #40 | M1 | superseded-by-commit | M1 actually landed via `59dd0b1`, not #40 — close with pointer (T017) |
| #41 | M3 | superseded-by-commit | `d18b0dd` |
| #42 | L4 | superseded-by-commit | `3bb125b` |
| #43 | C1 | superseded-by-commit | `aff2824` |
| #45 | CI (delete python-package.yml, fold into tests.yml) | pending decision | T003 will decide vs #47 |
| #46 | M9 | superseded-by-commit | `082ed2e` |
| #47 | CI (keep/fix python-package.yml) | pending decision | T003 will decide vs #45 |
| #48 | L6 | superseded-by-commit | `f457a79` |
| #50 | L2 | superseded-by-commit | `8338fb4` |
| #51 (consolidation map) | meta — tracks all of the above | needs-update | Predates this audit; update or close per T018 |

## Follow-ups filed outside this feature's scope (research D3)

- S2 (DICOM-SEG per-segment `SegmentationType`) — new issue, not in active scope.
- H5 singleton-construction lock (`XNATConnection._instance` unguarded `__new__`) — new issue, not in active scope.
- M2 (private-tag VR) — flagged `unclear`, not actively re-verified this pass; low risk, candidate for a future audit pass rather than blocking this feature.
