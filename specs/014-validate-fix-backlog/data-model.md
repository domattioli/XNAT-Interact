# Data Model: Validate the Unverified Fix Backlog (014)

## Finding

One audit item from #33.

| Field | Type | Notes |
|---|---|---|
| id | string | `C1`…`C2`, `H1`…`H8`, `M1`…`M10`, `L*`, `S*` — unique |
| severity | enum | CRITICAL / HIGH / MEDIUM / LOW / SUSPECT |
| summary | string | one line, from the audit |
| affected_paths | list | `src/` modules touched |
| disposition | enum | (rescoped) `fixed-with-test` \| `fixed-no-test` (backfill pending) \| `still-open` (fix pending) \| `not-a-bug` \| `unclear` |
| rationale | string | required when disposition ∈ {wont-fix, not-a-bug} |
| test_path | path | required when `fixed-with-test`; `tests/regression_014/test_<id>_*.py` |
| baseline_evidence | string | verbatim failing assertion line from the pre-fix run |
| fix_commit | sha | commit on consolidated branch |

**State transitions**: `UNVERIFIED → fixed-with-test` requires test_path + baseline_evidence + fix_commit all present. `UNVERIFIED → wont-fix|not-a-bug` requires rationale. A regression test that **passes** at the pre-fix baseline (finding not reproducible) resolves to `not-a-bug` with the passing baseline run recorded as the evidence. No transition out of a terminal state without a new ledger entry (append-only history).

## FixCandidate

A pre-existing proposed change (draft PR / branch).

| Field | Type | Notes |
|---|---|---|
| pr_number | int | #38, #40–#43, #45–#48, #50 |
| findings_claimed | list[Finding.id] | may overlap other candidates |
| terminal_state | enum | `superseded-by-consolidation` \| `dropped-with-rationale` \| `adopted-decision-only` (CI PRs) |
| pointer | string | comment/URL linking to the consolidated work |

**Invariant** (FR-004): for every Finding, at most one landed change exists on the consolidated branch regardless of how many FixCandidates claimed it.

## RegressionTest

| Field | Type | Notes |
|---|---|---|
| path | path | under `tests/regression_014/` or `tests/characterization/` |
| finding_id | Finding.id | named in test docstring |
| lane | enum | `offline` (default) \| `stress` (`@pytest.mark.stress`, deselected by default) |
| dual_run | bool | if true, parameterized over FakeXNAT + `RUN_XNAT_DUAL=1` real connection |

## SeedSet (fixture factory output)

Produced by `tests/synthetic_data.py` factory functions; the only test-data source.

| Case family | Contents | Exercises |
|---|---|---|
| byte_duplicates | same bytes, different filenames/UIDs | #32 layer 1, US5-AS1 |
| near_duplicates | adjacent synthetic fluoro frames | #32 layer 3 advisory, US5-AS2 |
| multi_scan_surgery | surgery with ≥3 scans | H7 completeness |
| big_series | ≥1001 instances (names only, lazy) | M1 boundary 999/1000 |
| malicious_archive | zip entries with `../` and absolute paths | C2 |
| concurrent_writers | two MetaTables sessions | H3 TOCTOU |
| invalid_values | NaN/None in validity-gated fields | M4 |

## Ledger (specs/014-validate-fix-backlog/ledger.md)

Header records the frozen **baseline SHA** (merge-base with `development` at feature start, per research D4). Two tables mirroring Finding and FixCandidate above. Append-only: dispositions change by adding a superseding row, never by editing history.

## FakeXNAT state snapshot (D7)

`FakeXNAT.snapshot() -> StateSnapshot`; `StateSnapshot.diff(other) -> list[Change]`. Empty diff after any rejected/failed upload = the no-empty-shells invariant (SC-006). `Change` = (kind: subject|experiment|scan|resource|file, action: created|deleted|modified, path).
