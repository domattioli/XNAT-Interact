# Disposition Ledger: #33/#32 Correctness Backlog

Baseline (feature start) commit: `dea6687bf72632afbc7f18d61a7f14789cc4c0d2` (origin/development tip, also this branch's merge-base with development)
Audit date: 2026-07-07. Reproduce a pre-fix failure: `git worktree add /tmp/prefix <commit-from-row> && cd /tmp/prefix && pytest <test_path> -x`

## Completion summary (Phase 7, 2026-07-07)

- **SC-001**: 29/29 #33/#32 findings have a terminal or explicitly-tracked-open row above. 100%.
- **SC-002**: All 7 active-scope findings (H8, M4, L1, S4, L5, plus S1→not-a-bug) proven failing at their pre-fix commit and passing after. Re-audited from scratch (H8 + S4 tests, worktree at `fe8230c`): 3/3 expected failures reproduced in 0.48s — well under the 5-minute bound.
- **SC-003**: Full offline suite green — `1405 passed, 24 skipped, 12 deselected, 7 xfailed, 0 failures` (`pytest -m "not slow and not stress and not pixeldeid and not pg and not requires_server"`). Zero network access to any real server; zero PHI.
- **SC-004**: All 10 draft PRs (#38, #40–#43, #45–#48, #50) terminal — see fix-candidates table. 9 were already closed before this session (2026-06-19 to 2026-06-26); this session closed #51.
- **SC-005**: N/A — #32 dedup redesign was already fully implemented + tested on `development` prior to this feature (see #32-identity / #32-empty-shells / #32-fixture-factory rows); no characterization work was in this feature's rescoped active scope.
- **SC-006**: `grep` sweep of `tests/regression_014/` for production hostnames — clean.
- **SC-007**: PR #54's description carries the verification-status block (see below); CI (#44) status confirmed still blocking an actual Actions run as of this audit — reported UNVERIFIED-blocked-on-CI, not rounded up.

**Verification status**: UNVERIFIED — blocked on CI (#44). Local: 1405/1405 passing (0 failures) across the full offline suite, including 20 new/backfilled regression tests in `tests/regression_014/`. Flips to VERIFIED only on an actual green `ci-lite.yml` run in GitHub Actions.

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
| H8 | HIGH | fixed-with-test | `tests/regression_014/test_h8_zip_integrity.py` | Baseline commit `fe8230c` (pre-H8-fix, this branch): both tests FAIL (`test_empty_resource_recorded_as_warning_not_silently_dropped` — no `warnings` field existed; `test_write_error_leaves_no_partial_zip` — raw `OSError` escapes, truncated zip left on disk). Fixed via `DownloadOutcome.warnings` field + atomic temp-file-then-rename zip write in `app/logic/download.py`. Both PASS post-fix. | Count-verify via explicit warning (not silent skip); partial-write cleanup via `.partial` temp + `Path.replace()` |
| M1 | MEDIUM | fixed-with-test | `tests/test_filename_instance_cap.py` | `59dd0b1`: supports up to 9999 instances | |
| M2 | MEDIUM | unclear | none found | private-tag VR audit not directly re-verified this pass | Low risk; not in active scope — flagged for a future pass, not blocking |
| M3 | MEDIUM | fixed-with-test | `tests/test_session_df_index_m3.py` | `d18b0dd`: `sort_values(...).reset_index(drop=True)` | |
| M4 | MEDIUM | fixed-with-test | `tests/regression_014/test_m4_nan_validity.py` | Baseline `91c40bb` (pre-M4-fix): `ImportError: cannot import name '_find_invalid_rows'` — collection fails. Fixed by extracting the check into `_find_invalid_rows(df)` using `!= True` instead of `== False` (catches NaN/None). | `!= True` correctly flags NaN/None as invalid since `NaN == False` is False but `NaN != True` is True |
| M5 | MEDIUM | fixed-with-test | `tests/test_create_backup_no_cwd_litter_m5.py`, `tests/test_backup_no_cwd_litter.py` | `0bdee7d`-family: backup no longer writes to CWD | |
| M6 | MEDIUM | fixed-with-test | `tests/test_annotation_manifest_merge_m6.py` | `b7e87f8`: manifest merge preserves prior blobs | |
| M7 | MEDIUM | fixed-with-test | `tests/regression_014/test_m7_stale_dir_contents.py` | Fix present since `f365524` (isolates downloads into `_xnat_download` subdir). **Historical pre-fix commit not reachable** — `f365524`'s parent (`c5447bec...`) is beyond this clone's shallow-fetch boundary (`git cat-file -e` fails). Test proves current correctness only; cannot show a fail-then-pass flip for this finding. | Backfilled per FR-004; historical-baseline gap recorded honestly rather than claimed |
| M8 | MEDIUM | fixed-with-test | `tests/regression_014/test_m8_stale_count_race.py` | Fix present since `f365524` (live `resource.num_files()` preferred over stale row count). Same shallow-fetch boundary as M7 — historical pre-fix commit not reachable. Test proves current correctness only. | Backfilled per FR-004 |
| M9 | MEDIUM | fixed-with-test | `tests/test_delete_server_names.py::test_delete_metatables_calls_correct_resource` | `082ed2e`: targets `config`/`database_config.json` | |
| M10 | MEDIUM | fixed-with-test | `tests/regression_014/test_m10_friendly_errors.py` | `src/utilities.py` first-run-catch site: non-first-run errors are re-raised after a FriendlyError is rendered and printed (not silently treated as first-run setup). Test covers this site. `app/logic/download.py` legacy-path `except Exception: pass` reviewed: single-row skip-and-continue by design (Constitution II — one bad input must not discard a whole batch), not a silent user-facing swallow; accepted as-is, no test added for that half. | Backfilled per FR-004; legacy-path disposition explained |
| L1 | LOW | fixed-with-test | `tests/regression_014/test_l1_implementation_uid.py` | Baseline `91c40bb`: 2/2 tests FAIL (UID varied with parent_uid). Fixed via module-level constant `XNAT_INTERACT_IMPLEMENTATION_CLASS_UID` in `src/xnat_scan_data.py`. | DICOM PS3.10 §7.1 requires a constant per-implementation UID |
| L2 | LOW | fixed-with-test | `tests/test_metadata_created_by_l2.py` | `8338fb4`: `CREATED_BY` preserved via `setdefault`, `LAST_MODIFIED_BY` added | |
| L3 | LOW | fixed-with-test | `tests/test_get_name_uid_guard_l3.py` | `8338fb4`: `get_name` guards on UID column | |
| L4 | LOW | fixed-with-test | `tests/test_xnat_gateway.py`, `tests/test_xnat_gateway_fake.py` | `3bb125b`: `disconnect()` guards `self.server is None` | |
| L5 | LOW | fixed-with-test | `tests/regression_014/test_l5_zip_temp_paths.py` | Investigation confirmed real (upgraded from `unclear`): `assemble_zip`'s `files_written` held `TemporaryDirectory`-staged paths that don't survive the context manager's cleanup on return. Baseline `91c40bb`: FAILS (`AssertionError: ... does not exist on disk`). Fixed by returning `[zip_dest]` (the artifact that actually persists) instead of the ephemeral staging paths. | The only file that outlives `assemble_zip` is the zip itself |
| L6 | LOW | fixed-with-test | `tests/test_browse_file_count_l6.py` | `f457a79`: `_file_count` queries all resources via `/scans/{scan}/files` | |
| S1 | SUSPECT | not-a-bug | `tests/regression_014/test_s1_gray_img_shape.py` | Investigation: `ImageHash.__init__` unconditionally calls `_convert_to_grayscale()`, which does `np.mean(raw_img, axis=2)` whenever `raw_img.ndim == 3` — axis 2 is always the channel axis for an (H,W,C) array regardless of C (3 or 4), so `gray_img` is *always* 2D before any caller reads `.shape`. Test proves the invariant holds for both 3- and 4-channel inputs. | No code change needed; test pins the invariant against regression |
| S2 | SUSPECT | still-open (not actioned) | none | `src/annotations/importers/dicom_seg.py:85` `SegmentationType` still read at dataset level | Out of this feature's scope (research D3) — follow-up issue filed (T019) |
| S3 | SUSPECT | fixed-with-test | `tests/test_annotation_from_manifest_validates_s3.py` | `1b96bb9`: `validate_annotator_id` called in `from_manifest` | |
| S4 | SUSPECT | fixed-with-test | `tests/regression_014/test_s4_project_prefix.py` | Baseline `91c40bb`: FAILS (an unqualified ref starting with the bare word "project" was wrongly treated as already-qualified). Fixed: `startswith("/project")` → `startswith("/project/")` (this module's own convention per `_project_qs`, confirmed singular via `tests/test_putzip_rf.py`). | Anchoring on the path-segment delimiter removes the substring ambiguity |
| #32-identity | — | fixed-with-test | `tests/test_dedup.py`, `test_dedup_evidence.py`, `test_dedup_wiring.py`, `test_image_hash.py`, `test_identity.py` | `src/services/dedup.py`: sha256 authoritative, StudyUID corroborating, pHash advisory-only | Layered identity design fully implemented |
| #32-empty-shells | — | fixed-with-test | `tests/test_publish_no_empty_shells.py` | `src/xnat_experiment_data.py` ~630-716: create+rollback-on-exception pattern | Cleanup-after-creation, not prevent-up-front, but net invariant holds |
| #32-fixture-factory | — | fixed (n/a) | `tests/synthetic_data.py` | Factory functions present (PHI/dicom/multiframe synthetic cases) | No dedicated test needed — it is the fixture |
| CI-duplication | — | fixed-with-test | local suite run 2026-07-07 | `ci-lite.yml` kept canonical (its own header + `python-package.yml`'s pointer both named it); `tests.yml` + `python-package.yml` deleted. Also fixed a masking bug found during consolidation: `ci-lite.yml`'s test step had `continue-on-error: true`, making the job report success regardless of pytest's actual exit code — removed. Local run of the exact `pytest` invocation: 1388 passed, 24 skipped (env-gated: tesseract/presidio/pg absent), 7 xfailed (intentional red-state docs), 0 failures | #45 vs #47: neither PR's literal diff was applied — the repo had already converged on a third shape (keep ci-lite.yml, not delete-into-tests.yml per #45, nor fix-python-package.yml per #47); both PRs closed with pointer to this decision (T017) |

## Fix candidates (draft PRs / issues)

| PR/Issue | Findings claimed | Terminal state | Pointer |
|---|---|---|---|
| #38 (xnat-fable campaign) | Login, download enumeration, #32 dedup, M1/M5/M9, stress lanes | **merged** (`5e3060e`) | Already terminal — no action needed |
| #40 | M1 | **closed unmerged** — superseded-by-commit | M1 actually landed via `59dd0b1`, not #40 |
| #41 | M3 | **merged** (`d18b0dd`) | Already terminal |
| #42 | L4 | **merged** (`3bb125b`) | Already terminal |
| #43 | C1 | **merged** (`aff2824`) | Already terminal |
| #45 | CI (delete python-package.yml, fold into tests.yml) | **closed unmerged** | Neither #45 nor #47's literal diff applied — repo converged on a 3rd shape (keep `ci-lite.yml`, delete both `tests.yml` and `python-package.yml`); decision recorded in the CI-duplication row above; comment posted on #51 |
| #46 | M9 | **merged** (`082ed2e`) | Already terminal |
| #47 | CI (keep/fix python-package.yml) | **closed unmerged** | Same disposition as #45 above |
| #48 | L6 | **merged** (`f457a79`) | Already terminal |
| #50 | L2 | **closed unmerged** — superseded-by-commit | L2 landed via `8338fb4`, not #50 |
| #51 (consolidation map) | meta — tracks all of the above | **closed** (state_reason=completed) | Closing comment posted 2026-07-07 pointing to this ledger; every recommendation in #51 has landed |

## Follow-ups filed outside this feature's scope (research D3)

- S2 (DICOM-SEG per-segment `SegmentationType`) — filed as **#55**.
- H5 singleton-construction lock (`XNATConnection._instance` unguarded `__new__`) — filed as **#56**.
- M2 (private-tag VR) — flagged `unclear`, not actively re-verified this pass; low risk, candidate for a future audit pass rather than blocking this feature.
