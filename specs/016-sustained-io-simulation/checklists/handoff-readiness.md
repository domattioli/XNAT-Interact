# Handoff-Readiness Checklist: Sustained Real-World I/O Simulation Against Local XNAT

**Purpose**: Validate that the complete artifact set (spec.md, plan.md, contracts/, quickstart.md) provides sufficient written context for a fresh agent to execute the simulation unattended. This is "requirements unit tests" — interrogating the WRITTEN artifacts, not the future implementation.

**Created**: 2026-07-09
**Feature**: [spec.md](../spec.md) | [plan.md](../plan.md) | [contracts/](../contracts/) | [quickstart.md](../quickstart.md)

---

## 1. Handoff Completeness — Entry Point, Mode Selection, Verdict Interpretation

- [ ] **CHK001** Entry point is explicitly documented in a binding contract (cli.md) with exact invocation form `python -m tests.stress.lane_sim016 [options]` [contracts/cli.md §1]
- [ ] **CHK002** Both run modes (smoke vs sustained) are defined with concrete durations: smoke ≤90 seconds, sustained default 3600 seconds [spec.md FR-003, contracts/cli.md Table]
- [ ] **CHK003** Smoke mode's purpose and when to use it is documented (fast verification, must complete with verdict before sustained run) [quickstart.md §2, plan.md Performance Goals]
- [ ] **CHK004** Sustained mode's default duration (60 minutes) is stated consistently across spec, plan, and contracts [spec.md Clarifications session, plan.md Technical Context, contracts/cli.md Table]
- [ ] **CHK005** The procedure document contract specifies all required sections needed for a cold agent to run unattended [contracts/procedure-doc.md §1-9]
- [ ] **CHK006** Preconditions are documented with verbatim test commands AND remediation steps for each failure case [contracts/procedure-doc.md §2, quickstart.md §1]
- [ ] **CHK007** Exit codes are defined with binding meaning: 0=PASS, 1=FAIL, 2=precondition error, 3=early-stopped partial [contracts/cli.md §5]
- [ ] **CHK008** Console output is machine-greppable: last three lines include `VERDICT: PASS|FAIL`, `REPORT: <path>`, `PARTIAL: true|false` [contracts/cli.md §7]
- [ ] **CHK009** Verdict interpretation is documented as one-line parse (`verdict.overall`) plus meaning of sub-checks [contracts/procedure-doc.md §5, quickstart.md §4]
- [ ] **CHK010** Each verdict outcome (PASS, FAIL, exit 2, exit 3) has a documented next step, never merely "crashes" without remediation [contracts/procedure-doc.md §5]
- [ ] **CHK011** Fresh run-id-scoped project naming (`SIM016_<utc-ts>_<hex4>`) is documented as never-reused [spec.md FR-007, plan.md §6, contracts/cli.md Table]
- [ ] **CHK012** The target XNAT instance is documented as localhost only, with production-hostname refusal as a hard startup error [plan.md Technical Context, contracts/cli.md Constraint checks]

---

## 2. Verdict Integrity — Rule Unambiguity, Tri-State Clarity, Threshold Concreteness

- [ ] **CHK013** The unexpected-failure ratio threshold is stated as a concrete formula `max(1, ceil(0.02 × case-write EVENTS))` — the denominator counts case-write events, NOT individual retry attempts (retries are tallied separately in `retry_total` and never loosen the gate) [spec.md FR-006, plan.md, contracts/run-report.md §2 rule 1]
- [ ] **CHK014** The zero-tolerance integrity clause is explicit: "count 1 integrity failure at any snapshot = FAIL" [spec.md FR-006, contracts/run-report.md §2]
- [ ] **CHK015** The tri-state failure classification (FRIENDLY, ACCEPTED, CRASH) is defined so expected vs unexpected are never conflatable [spec.md Clarifications Q1, contracts/run-report.md §2]
- [ ] **CHK016** Expected FRIENDLY rejections of malformed injections are excluded from the ratio numerator [spec.md Clarifications Q1, plan.md]
- [ ] **CHK017** An injected malformed case that is silently ACCEPTED (should have failed) or CRASHes (unexpected) is counted as an unexpected failure [spec.md Clarifications Q1, plan.md]
- [ ] **CHK018** Resource bounds are concrete numbers, not adjectives: scratch <1 GB, no 3-snapshot monotonic growth, ≤10 open connections [spec.md FR-006, plan.md, contracts/run-report.md §2]
- [ ] **CHK019** Partial-run semantics are defined: `partial`/`stopped_early` are ORTHOGONAL informational flags that never enter the verdict formula — an early-stopped clean run (any mode) PASSes with `partial=true`, and consumers must treat the verdict as covering only the truncated run; `overall` additionally requires `completed_cases >= 1` (reason `no_completed_writes`) and non-empty `snapshots[]` (reason `no_integrity_evidence`) [contracts/run-report.md §2 rules 2+4]
- [ ] **CHK020** The tri-part verdict structure is documented: `ratio_check`, `integrity_check`, `resource_check` each with `pass: true|false` [contracts/run-report.md §1 Schema]
- [ ] **CHK021** Verdict computation rules are normative and unambiguous [contracts/run-report.md §2, items 1-5]
- [ ] **CHK022** The report schema includes `verdict.reasons[]` as machine-readable failure explanations, one sentence per failed check [contracts/run-report.md §1 Schema item 5]

---

## 3. Requirement Consistency — Spec/Plan/Contract Alignment on Concrete Values

- [ ] **CHK023** Default duration (60 minutes = 3600 seconds) appears in spec Clarifications, plan Technical Context, and contracts/cli.md Table [spec.md Q2, plan.md, contracts/cli.md]
- [ ] **CHK024** Mean case inter-arrival (≤~110 seconds) is documented consistently to yield ≥30 cases in 60-min run [spec.md FR-002, plan.md Performance Goals, contracts/cli.md Table]
- [ ] **CHK025** Case-variety proportions (80% normal / 10% malformed / 10% dedup-probe) with absolute floors (≥3 malformed, ≥3 dedup) are consistent [spec.md Clarifications Q3, plan.md Event-scheduler model, contracts/cli.md Table]
- [ ] **CHK026** Integrity-snapshot cadence (≤~15 minutes = ~720 seconds) is documented to yield ≥4 snapshots per 60-min run [spec.md FR-005, plan.md, contracts/cli.md Table]
- [ ] **CHK027** Smoke mode defaults are consistent: duration ≤90 seconds, floors waived, one end-of-run snapshot [plan.md Event-scheduler model, contracts/cli.md Table]
- [ ] **CHK028** The max(1, 2%) threshold for unexpected failures is stated the same way in spec FR-006, plan, and contracts/run-report.md §2 [spec.md, plan.md, contracts/run-report.md]
- [ ] **CHK029** Resource verification happens on every integrity snapshot, not once at end [spec.md FR-006, plan.md §3]
- [ ] **CHK030** Writer pool size (default 3), max connections (default 10), and the constraint `writer_pool_size + 2 ≤ max_connections` are documented [plan.md, contracts/cli.md Constraint checks]

---

## 4. Coverage & Traceability — Success Criteria to Validation Paths

- [ ] **CHK031** Each success criterion (SC-001 through SC-008) has a documented validation method in the procedure or independent test [spec.md User Scenarios, contracts/procedure-doc.md]
- [ ] **CHK032** SC-001 (smoke-mode PASS/FAIL verdict in <5 min, no operator clarification) is testable via the procedure without context [spec.md US1, quickstart.md §2]
- [ ] **CHK033** SC-002 (variable inter-arrival gaps, not uniform) is testable by inspecting event timeline timestamps in the report [spec.md US2, contracts/run-report.md §3 JSONL schema]
- [ ] **CHK034** SC-003 (30+ cases with ≥3 distinct shapes) is testable by counting `by_category` in the report [spec.md US2, contracts/run-report.md §1 Schema]
- [ ] **CHK035** SC-004 (at least one read strictly between write start/completion) is testable by event timeline inspection [spec.md US3, contracts/run-report.md §3 JSONL]
- [ ] **CHK036** SC-005 (≥3 integrity snapshots per run, final report states pass/fail) is testable from report `snapshots[]` array and `integrity_check.pass` [spec.md US4, contracts/run-report.md §1 Schema]
- [ ] **CHK037** SC-006 (early-stopped partial report readable, zero corruption) is testable via the stop procedure [spec.md US4 SC, contracts/cli.md Stop procedure]
- [ ] **CHK038** SC-007 (resource bounds: <1GB, no 3-snapshot monotonic growth, ≤10 connections) is testable from every snapshot's `resources` object [spec.md US4 SC, contracts/run-report.md §1 Schema]
- [ ] **CHK039** SC-008 (unexpected failures ≤ max(1, 2%), tri-state never conflated) is testable from `failures.unexpected_terminal` and `failures.tri_state` [spec.md US4 SC, contracts/run-report.md §1 Schema]
- [ ] **CHK040** Each edge case from spec (interruption, XNAT hiccup, unexpected malformed failure, early stop, unbounded growth) has a handling requirement documented [spec.md Edge Cases, contracts/procedure-doc.md §6-7]

---

## 5. Scope Discipline — FR-013 Gate, STAPLE Stub, FakeXNAT Prohibition

- [ ] **CHK041** FR-013 (design-and-planning only, no implementation code) is stated in the plan preamble and referenced in source-code project structure [plan.md Scope guard, Project Structure]
- [ ] **CHK042** STAPLE remains an untouched stub: the design references only the existing reference-aggregator seam for consolidation activity [spec.md FR-012, plan.md Architecture Sketch §3]
- [ ] **CHK043** FakeXNAT is explicitly prohibited and not a valid substitute for the live Docker-local XNAT [spec.md FR-009, quickstart.md §1]
- [ ] **CHK044** The procedure document does not reference this planning conversation, spec, or plan — it is context-free [contracts/procedure-doc.md Style requirements]
- [ ] **CHK045** The procedure document makes no mention of FakeXNAT or any non-localhost XNAT instance as valid [contracts/procedure-doc.md §1]
- [ ] **CHK046** The implementation targets (lane_sim016.py, sim016_schedule.py, sim016_report.py) are marked [FUTURE] and explicitly not built by this feature [plan.md Project Structure]
- [ ] **CHK047** Clarify-session answers (5 binding defaults) are recorded in spec and propagated to plan and contracts, not scattered across multiple places [spec.md Clarifications, plan.md Technical Context]
- [ ] **CHK048** The report schema explicitly excludes credentials; passwords are never logged or serialized [contracts/cli.md Table, contracts/run-report.md §1 Schema]

---

## 6. Procedural Completeness — Agent-Executable Runbook Structure

- [ ] **CHK049** Precondition checks (container, REST, Python deps) are copy-pasteable from the procedure and machine-verifiable [contracts/procedure-doc.md §2, quickstart.md §1]
- [ ] **CHK050** Each precondition failure has a documented remediation that does not reference this conversation [contracts/procedure-doc.md §2]
- [ ] **CHK051** The smoke-mode invocation is exact and includes the expected wall-clock budget and exit code [contracts/procedure-doc.md §3, quickstart.md §2]
- [ ] **CHK052** The sustained-mode invocation is exact and documents the defaults (duration, inter-arrival, variety, snapshot cadence) inline [contracts/procedure-doc.md §4, quickstart.md §3]
- [ ] **CHK053** Guidance for custom durations is documented (e.g., `--duration-s 7200` for 2 hours) [contracts/procedure-doc.md §4]
- [ ] **CHK054** The "reading the verdict" section covers all three sub-checks and explains what `partial=true` means [contracts/procedure-doc.md §5]
- [ ] **CHK055** The early-stop procedure (pid file + `kill -INT`, grace window, second signal) is documented verbatim [contracts/procedure-doc.md §6, contracts/cli.md Stop procedure]
- [ ] **CHK056** Mid-run server unresponsiveness is documented as recoverable, with retry/backoff/pause-then-resume behavior [contracts/procedure-doc.md §7, spec.md FR-011]
- [ ] **CHK057** Cleanup section documents what is left behind (run project server-side, report + JSONL locally) and that nothing is deleted by the simulation outside per-case scratch [contracts/procedure-doc.md §8]
- [ ] **CHK058** Troubleshooting table covers common failure modes: connection refused, HTTP 401, project-exists refusal, exit 2 variants, and FAIL per sub-check [contracts/procedure-doc.md §9]

---

## 7. Data-Model & Report Fidelity — Schema Binding and Event Log Completeness

- [ ] **CHK059** The report JSON schema is normative draft-07 with required fields: `schema_version`, `run`, `counts`, `failures`, `snapshots`, `verdict`, `timeline_ref` [contracts/run-report.md §1]
- [ ] **CHK060** Run identification fields are documented: `run_id` follows pattern `^SIM016_[0-9]{8}T[0-9]{6}Z_[0-9a-f]{4}$` [contracts/run-report.md §1 Schema]
- [ ] **CHK061** Failure counts separate expected vs unexpected: `expected_friendly`, `unexpected_terminal`, `tri_state{FRIENDLY, ACCEPTED, CRASH}` [contracts/run-report.md §1 Schema]
- [ ] **CHK062** The snapshot history includes `pass`, `resources`, and `resource_ok` — zero integrity failures = zero FAIL [contracts/run-report.md §1 Schema]
- [ ] **CHK063** The JSONL event-log schema includes required keys: `event_id`, `kind`, `scheduled_at`, `started_at`, `completed_at`, `outcome`, `case_ref`, `retries`, `error` [contracts/run-report.md §3]
- [ ] **CHK064** The report is atomically rewritten (`tempfile` + `os.replace`) after each snapshot and at finalization, so a kill at any instant leaves the previous valid report on disk [plan.md Architecture Sketch §4]
- [ ] **CHK065** The JSONL log is append-only and written as events complete, surviving a process kill via truncated-line tolerance [plan.md Architecture Sketch §4, contracts/run-report.md §3]

---

## 8. Exit Code & Verdict Binding — No Ambiguous Outcomes

- [ ] **CHK066** Exit code 0 ALWAYS means overall verdict is PASS, and vice versa [contracts/cli.md §5, contracts/run-report.md §2 item 4]
- [ ] **CHK067** Exit code 1 ALWAYS means overall verdict is FAIL, and vice versa [contracts/cli.md §5]
- [ ] **CHK068** Exit code 2 means precondition failure before any simulated event; report may be absent [contracts/cli.md §5]
- [ ] **CHK069** Exit code 3 means early-stopped; report has `stopped_early=true` and `partial=true` [contracts/cli.md §5]
- [ ] **CHK070** A partial sustained run's verdict is computed from evidence up to the stop, not declared as PASS just because execution halted [contracts/run-report.md §2 item 4]

---

## Authoring Observations

### Quality Issues Flagged While Writing This Checklist

1. **Potential ambiguity in "pre-implementation" boundary**: The plan states "lane_sim016.py" will be future work, but contracts/cli.md specifies exact CLI behavior (exit codes, output format) that must be matched exactly. This is correct (contract-first), but a future implementer must match these bindings precisely. No fix needed; this is the contract's purpose.

2. **FakeXNAT prohibition is clear but could be stricter**: While FR-009 forbids FakeXNAT, the phrase "it MUST NOT rely on or reference the offline FakeXNAT test double" uses "reference" which might be interpreted as "mention it in code" rather than "use as a substitute". The procedure-doc and quickstart make it clear (direct statement "do not substitute FakeXNAT"), so the handoff is correct. No fix needed.

3. **Snapshot resource recording lacks per-process breakdown**: The plan states "simulation-attributable scratch bytes" and "open-connection count" are recorded, but does not specify how to attribute connections when the session-pool processes are reused. This is a future-implementation detail (belongs in code, not contracts), so the spec is correct in not over-specifying. No fix needed; implementer must resolve via process-level introspection.

4. **Retry/outage handling (FR-011) is described narratively in procedure but not as state machine**: The quickstart (§6) and contracts/procedure-doc.md (§7) describe circuit-breaker behavior in prose ("pause, probe, resume or finalize-FAIL"), which is clear enough for a future agent to understand, but the exact backoff/probe schedule is deferred to implementation. This is appropriate for a design-only spec. No fix needed.

5. **Consistency check passes** (re-affirmed after analyze cycles 1-3): All concrete numbers (60 min, 110 s, 80/10/10, ≥3 floors, 1GB raw + leak-signal monotonic rule, ≤10 connections, 720 s cadence, 90 s smoke, max(1, ceil(0.02 × case-write events)) threshold with retries excluded from the denominator, completed_cases ≥ 1 precondition, non-empty snapshots[] requirement) are stated consistently across spec, plan, data-model, and contracts. Exit codes 0/1/2/3 are consistent. CHK013/CHK019 were reconciled to the converged verdict rule during analyze cycle 3.

6. **Traceability complete**: Every SC-001 through SC-008 has a documented path to validation via the report schema or event log. Edge cases have handling requirements. Clarify-session answers are propagated to FR/SC.

---

## Summary

**Item Count**: 70 items across 8 categories
**Checklist Status**: ✓ Ready for execution (no items checked; this is the validation artifact for the analyze phase)

**Categories**:
1. Handoff Completeness — 12 items
2. Verdict Integrity — 10 items
3. Requirement Consistency — 8 items
4. Coverage & Traceability — 10 items
5. Scope Discipline — 8 items
6. Procedural Completeness — 10 items
7. Data-Model & Report Fidelity — 7 items
8. Exit Code & Verdict Binding — 5 items

**Authoring Observations**: 6 notes (5 correct by design, 1 implementation detail appropriately deferred)

