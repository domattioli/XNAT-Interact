# Research: Close the Remaining Verified-Fix Gap (014, rescoped)

## D1 — CI consolidation pick

**Decision**: Keep `ci-lite.yml` as canonical (matches CLAUDE.md: "one minimal CI lane"); delete `tests.yml` if it's the redundant one, or vice versa — decided by diffing the two at implementation time and keeping whichever has broader/more current coverage; delete `python-package.yml` unconditionally (confirmed dormant/superseded by its own header comment).
**Rationale**: CLAUDE.md already names `ci-lite.yml` as "the canonical minimal lane" in prior repo commits' language; audit confirmed both `ci-lite.yml` and `tests.yml` fire on the same trigger today, which is the actual duplication to resolve.
**Alternatives considered**: merge both into one file — more churn than deleting the redundant one.

## D2 — Historical pre-fix commit for backfill tests (M7/M8/M10)

**Decision**: For each backfill target, locate the specific commit that introduced the fix (already identified: M7/M8 fixes are un-attributed to a single commit message in the audit — locate via `git log -p -- app/logic/download.py src/services/xnat_gateway.py` bisection at implementation time; M9's neighbor M10 sites similarly). Record `<parent of fixing commit>` as the pre-fix baseline per finding in the ledger.
**Rationale**: Constitution VII requires the failing run be against genuinely pre-fix code; since the fix already happened, the baseline is historical, not the current tip.
**Alternatives considered**: use the feature's frozen baseline SHA (`dea6687`) for these — wrong, since the fix is already present at that SHA; would falsely show the test passing at "baseline."

## D3 — Scope boundary for S2 and H5-singleton

**Decision**: File both as new GitHub issues rather than pulling into this feature's task list.
**Rationale**: S2 (DICOM-SEG per-segment type) touches an import path none of this feature's other work exercises; H5-singleton-lock is a genuine but separate concurrency hardening item. Neither blocks this feature's success criteria; folding them in would re-inflate scope right after rescoping down.
**Alternatives considered**: fix opportunistically since we're already in the neighborhood — rejected, keeps scope creep out per the lesson just learned.

## D4 — Draft PR closure evidence

**Decision**: For each of #40–#50/#38, cite the specific commit SHA (already identified during the audit, e.g. `aff2824` for C1/#43, `d18b0dd` for M3/#41, `082ed2e` for M9/#46, `f457a79` for L6/#48, `3bb125b` for L4/#42, `0bdee7d`-family for M5, etc.) in the closing comment.
**Rationale**: Makes the closure auditable — not just "already fixed," but exactly where.
**Alternatives considered**: generic "superseded" comment — less useful, fails FR-005's "pointer" requirement.
