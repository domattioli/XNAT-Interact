# Checklist: Verification & Consolidation Requirements Quality

**Purpose**: Unit-test the 014 requirements (spec.md + plan artifacts) for completeness, clarity, and consistency before implementation — reviewer-facing, PR-gate depth
**Created**: 2026-07-07
**Feature**: [spec.md](../spec.md)

## Requirement Completeness

- [ ] CHK001 - Is the authoritative inventory of #33 findings (exact IDs and count for C/H/M/L/S) enumerated somewhere traceable, rather than "~30 findings"? [Completeness, Spec §Assumptions]
- [ ] CHK002 - Are requirements defined for what happens to findings *discovered during* this work (new issue, not silent absorption) and is the boundary testable? [Completeness, Spec §Assumptions]
- [ ] CHK003 - Are ledger content requirements complete for every disposition type (fixed-with-test vs wont-fix vs not-a-bug field obligations)? [Completeness, Spec §FR-003, contracts/ledger.md]
- [ ] CHK004 - Are requirements defined for updating the PR verification-status block on *every* push, including who/what enforces the refresh? [Gap, Spec §FR-012]
- [ ] CHK005 - Is the disposable real-XNAT instance's provisioning (how it comes to exist for RUN_XNAT_DUAL=1 runs) specified or explicitly out of scope? [Gap, Spec §FR-006]

## Requirement Clarity

- [ ] CHK006 - Is "pre-fix baseline" pinned to a single unambiguous commit definition (merge-base at feature start) everywhere it is referenced? [Clarity, Spec §FR-001, research D4]
- [ ] CHK007 - Is "empty shell" precisely defined (which XNAT object kinds count: subject/experiment/scan/resource) so SC-006's zero-occurrence assertion is objective? [Clarity, Spec §FR-007, data-model.md]
- [ ] CHK008 - Is "advisory-only" for perceptual similarity defined operationally (surfaced where, to whom, in what form) rather than just "MUST NOT block"? [Ambiguity, Spec §FR-008]
- [ ] CHK009 - Is "plain-language report naming the conflicting existing data" specified enough to write an assertion against (fields the report must contain)? [Clarity, Spec §FR-007]
- [ ] CHK010 - Are "stress-class" tests given membership criteria (what makes a test stress vs default-lane deterministic) rather than left to author judgment? [Ambiguity, Spec §FR-006]

## Requirement Consistency

- [ ] CHK011 - Do the spec (CI decision "defaults to #45 direction… unless review of #47 surfaces a blocking reason") and research D1 (decision already made) agree on whether #47 review is still an open obligation? [Conflict, Spec §Assumptions vs research D1]
- [ ] CHK012 - Is the fresh-re-implementation rule (FR-004, no cherry-picking) consistent with tasks that "extend, don't duplicate" pre-existing tests like test_annotation_manifest_merge_m6.py? [Consistency, Spec §FR-004, tasks T034]
- [ ] CHK013 - Do the H-finding enumerations agree across spec (H1–H8 listed with 7 descriptions) and tasks (T028's "H6 if distinct from H7") — is the H6/H7 identity resolved against #33's actual numbering? [Conflict, Spec §US3]
- [ ] CHK014 - Are the marker names and `-m` selection expression identical across pytest.ini requirements, contracts/test-lane.md, and quickstart.md? [Consistency, contracts/test-lane.md]
- [ ] CHK015 - Is the ledger's append-only rule consistent with SC-001's "exactly one terminal disposition" (live-row vs superseding-row semantics defined)? [Consistency, data-model.md §Ledger]

## Acceptance Criteria Quality

- [ ] CHK016 - Is SC-002's "<5 minutes" reproduction bound measurable as written (starting state, machine assumptions, what counts as start/stop)? [Measurability, Spec §SC-002]
- [ ] CHK017 - Is SC-005's characterization bar (100% byte-dup catch, zero false merges) tied to a defined seed-set size so "100%" has a denominator? [Measurability, Spec §SC-005]
- [ ] CHK018 - Is SC-004's "drops from ~10 to 0" grounded in an exact PR list so completion is checkable? [Measurability, Spec §SC-004, contracts/ledger.md]
- [ ] CHK019 - Can SC-007 ("zero instances of a fix reported as validated without green CI") be objectively audited — is the set of "reporting surfaces" it covers enumerated? [Measurability, Spec §SC-007]

## Scenario & Edge Case Coverage

- [ ] CHK020 - Are requirements defined for a regression test that *passes* at the pre-fix baseline (finding not reproducible) — disposition path and ledger treatment? [Gap, Edge Case]
- [ ] CHK021 - Are requirements defined for the order-of-landing conflict (US3 fix on a path US5 rewrites) including which test set must be re-run and re-proven? [Coverage, Spec §Edge Cases]
- [ ] CHK022 - Is recovery behavior specified if the consolidated branch itself diverges from development mid-feature (rebase policy vs frozen baseline validity)? [Gap, Recovery]
- [ ] CHK023 - Are requirements for partial completion defined (feature pauses with some findings terminal, some UNVERIFIED — is the ledger + PR state still coherent)? [Coverage, Gap]

## Dependencies & Assumptions

- [ ] CHK024 - Is the assumption that FakeXNAT can represent all needed server behaviors (assessors, resource labels, concurrent sessions) validated against the H/M finding list, with a stated fallback (dual-run lane) where it cannot? [Assumption, Spec §Assumptions]
- [ ] CHK025 - Is the dependency on operator action for #44 (Actions billing) documented with an explicit non-blocking boundary (what proceeds vs what waits)? [Dependency, Spec §Edge Cases, research D9]
- [ ] CHK026 - Is the assumption that draft PRs #38/#40–#50 remain available as design references (not deleted) recorded, given they will be closed during US4? [Assumption, Gap, tasks T039]

## Traceability

- [ ] CHK027 - Does every FR trace to at least one user story acceptance scenario and at least one task, and is that mapping recorded (or derivable) for audit? [Traceability]
- [ ] CHK028 - Are Constitution VII's gate questions each answerable from spec artifacts alone (test location, CI status, duplicate-landing proof, wont-fix rationale) without reading code? [Traceability, plan.md §Constitution Check]
