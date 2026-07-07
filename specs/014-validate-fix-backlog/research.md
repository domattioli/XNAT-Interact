# Research: Validate the Unverified Fix Backlog (014)

All Technical Context unknowns resolved. Decisions below; each maps to a spec requirement.

## D1 — CI consolidation direction (#45 vs #47)

**Decision**: Adopt #45's direction — delete `.github/workflows/python-package.yml`, keep `tests.yml` as the single testing lane (PR + dispatch triggers, ubuntu, matrix ≥3.9 floor, `-m "not slow and not stress and not pixeldeid and not pg and not requires_server"` default selection).
**Rationale**: Repo convention (CLAUDE.md) is one minimal CI lane; two lanes running overlapping pytest invocations is exactly the duplicate-signal problem this feature exists to kill. #47's repair keeps a workflow whose only distinct value (packaging) is moot — the repo has no pip-installable package.
**Alternatives considered**: #47 (repair python-package.yml) — rejected: preserves a second lane with no unique coverage; merging both — rejected: same duplication with more YAML.

## D2 — Fix sourcing (clarified)

**Decision**: Re-implement every fix fresh on the consolidated branch; draft PRs #38/#40–#43/#46/#48/#50 are read as design references only, then closed-with-pointer.
**Rationale**: Session clarification (2026-07-07). Ten stale branches carry conflict noise and three carry the same M1 fix; fresh implementation makes "lands exactly once" trivially auditable.
**Alternatives considered**: cherry-pick (conflict churn, imports untested diffs), hybrid (two provenance regimes complicate the ledger).

## D3 — Pre-fix failure proof mechanism (clarified)

**Decision**: Ledger-recorded run: each regression test is executed once against the pre-fix baseline commit (recorded per-finding in `ledger.md`: baseline SHA + verbatim failure line); no permanent bisect harness.
**Rationale**: Session clarification. Lightweight, auditable, reproducible in <5 min per SC-002 via `git worktree add /tmp/pre-fix <sha> && pytest <test> --no-header -x`.
**Alternatives considered**: automated bisect harness (infra to maintain, slows CI), commit-order proof alone (history rewrites during consolidation would destroy it; kept as *secondary* convention — test commit precedes fix commit where practical).

## D4 — Pre-fix baseline commit

**Decision**: The baseline is the merge-base of the consolidated branch with `development` at feature start (recorded once at the top of `ledger.md`). All "fails pre-fix" runs execute against this single SHA in a throwaway worktree.
**Rationale**: One frozen baseline makes every proof comparable and immune to mid-feature drift on `development`.
**Alternatives considered**: per-finding baselines (incomparable proofs), `main` (too far behind the code being fixed).

## D5 — Stress/real-server lane mechanics

**Decision**: Add `stress` marker to `pytest.ini`. Default lane deselects it. Data-integrity tests promoted to the real-server lane are written FakeXNAT-first and parameterized over the connection fixture; setting `RUN_XNAT_DUAL=1` re-runs them against a disposable local XNAT (never UIowa production — hostname allowlist asserted in the connection fixture).
**Rationale**: FR-006; matches the existing marker taxonomy (`requires_server`, `pg`, `pixeldeid` already model opt-in lanes) and spec 007's dual-run precedent.
**Alternatives considered**: separate test tree for real-server (duplicates test bodies); env-var-only gating without markers (invisible to `-m` selection).

## D6 — Layered identity design (#32)

**Decision**: Three layers, in decision order: (1) raw-byte sha256 → exact duplicate → REJECT + report naming the existing copy; (2) preserved StudyInstanceUID → surgery-set membership/identity; (3) perceptual hash → advisory `similar_to` flag only, never blocking. Upload is transactional-by-ordering: no subject/experiment/scan is created until the payload is fully validated; rejection leaves server state byte-identical (asserted via FakeXNAT state snapshot diff).
**Rationale**: FR-007/008; sha256 has no false merges by construction, satisfying SC-005's zero-false-merge bar; perceptual hashing is the only layer with a false-positive envelope, so it must not gate.
**Alternatives considered**: perceptual-hash-as-blocker (false merges on adjacent fluoro frames — the exact #32 complaint), metadata-only identity (defeated by re-exported byte-identical files with rewritten UIDs — caught by layer 1).

## D7 — No-empty-shells enforcement point

**Decision**: Extend `tests/fakes/fake_xnat.py` with a `snapshot()`/`diff()` state API; an autouse-scoped assertion helper wraps every upload-path test so *any* residual shell fails the test that created it (SC-006). Production code gains a validate-before-create ordering in `push_to_xnat` (H2/H5 fixes share this path).
**Rationale**: Making the invariant an automatic assertion rather than a per-test discipline is the only way SC-006's "zero occurrences across the suite" is checkable.
**Alternatives considered**: per-test manual asserts (forgettable), server-side cleanup sweeper (masks the bug instead of proving its absence).

## D8 — Disposition ledger format

**Decision**: `specs/014-validate-fix-backlog/ledger.md` — one table for findings (ID, severity, disposition, test path, baseline-fail evidence, fix commit), one for fix candidates/PRs (#, findings covered, terminal state, pointer). Committed with every disposition change.
**Rationale**: FR-003/SC-001; markdown in the feature dir is reviewable in the same PR as the work it records; schema in data-model.md.
**Alternatives considered**: GitHub issue checklist (not versioned with code), JSON (hostile to review).

## D9 — Reporting while #44 blocks CI

**Decision**: PR description carries a status block: `Verification status: UNVERIFIED — blocked on CI (#44). Local: <N>/<M> regression tests passing.` Updated on every push; flips to VERIFIED only on a green Actions run of `tests.yml`.
**Rationale**: FR-012, Constitution VII's explicit no-rounding-up rule.
**Alternatives considered**: none viable — silence or "green locally" both violate VII.
