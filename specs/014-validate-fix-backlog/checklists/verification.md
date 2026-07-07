# Checklist: Verification & Consolidation Requirements Quality (rescoped)

**Purpose**: Unit-test the rescoped 014 requirements for completeness, clarity, and consistency
**Created**: 2026-07-07 (rescoped)
**Feature**: [spec.md](../spec.md)

## Requirement Completeness

- [ ] CHK001 - Is the complete ~29-row ledger requirement (FR-006) distinguished clearly from the 8-finding active-scope requirement, so a reader doesn't conflate "tracked" with "actively fixed this round"? [Completeness, Spec §FR-006]
- [ ] CHK002 - Are requirements defined for what happens if a "still-open" finding turns out already fixed by a different mechanism during implementation? [Completeness, Spec §Edge Cases]
- [ ] CHK003 - Is the follow-up-issue path for S2 and the H5-singleton sub-issue (explicitly out of scope) specified so they aren't silently dropped? [Completeness, Spec §Assumptions]

## Requirement Clarity

- [ ] CHK004 - Is "historical pre-fix commit" (FR-004) distinguished clearly from the original feature's single frozen baseline SHA, given these findings were fixed at different times? [Clarity, research D2]
- [ ] CHK005 - Is the CI consolidation pick (ci-lite.yml vs tests.yml) resolved by an explicit diff-and-decide step rather than assumed, since audit only established duplication exists, not which lane should survive? [Clarity, Spec §FR-001, research D1]

## Consistency

- [ ] CHK006 - Do spec.md's finding counts (7 still-open, 4 fixed-no-test, 20 fixed-with-test) sum consistently with the ~29 total across all documents (plan, tasks, ledger contract)? [Consistency]
- [ ] CHK007 - Is the disposition vocabulary (fixed-with-test/fixed-no-test/still-open/not-a-bug/unclear) used identically in spec.md, data-model.md, and contracts/ledger.md? [Consistency]

## Acceptance Criteria Quality

- [ ] CHK008 - Is SC-002/SC-003's "shown failing" bound to a concrete reproducible procedure (quickstart.md worktree steps) rather than left abstract? [Measurability]

## Traceability

- [ ] CHK009 - Does every FR trace to at least one task in the rescoped tasks.md? [Traceability]
- [ ] CHK010 - Are the 10 draft-PR closures (FR-005) each backed by a specific commit SHA already identified during the audit, rather than a generic "superseded" claim? [Traceability, research D4]
