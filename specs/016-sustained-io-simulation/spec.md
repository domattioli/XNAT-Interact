# Feature Specification: Sustained Real-World I/O Simulation Against Local XNAT

**Feature Branch**: `016-sustained-io-simulation`
**Created**: 2026-07-09
**Status**: Draft
**Input**: User description: "Design a robust synthetic dataset and procedure for simulating a real world sustained input/output update of data in the XNAT instance and prepare it such that another agent can run it when I boot up a docker image with the XNAT instance locally."

## Clarifications

### Session 2026-07-09

Answers provided by an adversarial clarify-surrogate agent standing in for the operator (operator-directed); the operator did not personally weigh in on these five defaults. All are reversible design defaults in a design-only spec.

- Q: What error-rate threshold defines the unattended PASS/FAIL verdict, and how are injected/expected failures distinguished from unexpected ones? → A: Run FAILs if unexpected *terminal* failures (failed after the FR-011 retry policy is exhausted) exceed max(1, ceil(2% × case-write events)). *(Refined during analyze from the original "2% of total write attempts" wording: denominator = case-write events, retries tallied separately; and a zero-completed-writes run can never PASS — see FR-006.)* Expected malformed-case rejections (the repo's existing FRIENDLY class) are excluded from the ratio and tracked separately; an injected malformed case that is silently ACCEPTED or produces a CRASH counts as an unexpected failure (reuse the existing ACCEPTED/FRIENDLY/CRASH tri-state, not a binary). Any detected data loss/corruption (integrity-snapshot failure) = FAIL at count 1, independent of the ratio. (Deviates from the drafted 5% default: FR-011's retry policy absorbs transient flakiness before the ratio is computed, so 5% post-retry would mask real regressions; the max(1,…) floor keeps smoke mode's tiny write count from producing guaranteed-FAIL fluke ratios.)
- Q: What is the default sustained-mode duration when the operator doesn't specify one? → A: 60 minutes — with the binding rider that default event-rate targets MUST be coupled to it: mean case inter-arrival ≤ ~110 seconds (so a 60-min run is expected to produce ≥30 cases, making SC-003 non-vacuous) and integrity-snapshot cadence ≤ ~15 minutes (so ≥4 snapshots per default run, clearing SC-005's floor with margin).
- Q: What are the default case-variety proportions? → A: 80% normal / 10% malformed-or-edge-case / 10% dedup-collision probes, with absolute floors of at least 3 malformed injections and at least 3 dedup probes per sustained run regardless of percentage. (Deviates from the drafted 70/20/10: a 1-in-5 malformed rate contradicts the spec's own "realistic, not adversarial" framing — concentrated malformed coverage already lives in the existing malformed stress lane — and the absolute floors close the low-case-count hole where a valid short run would contain zero probes.)
- Q: What concrete resource-growth bound can a future agent verify against (SC-007)? → A: Three parts: (1) per-case temp cleanup on case completion (finally-style, matching the existing stress lanes' convention), not a wall-clock timer; (2) resource verification piggybacked on every integrity snapshot — recording simulation-attributable scratch bytes and open-connection count; (3) FAIL/flag if scratch exceeds 1GB at any sampled point OR grows monotonically across 3+ consecutive snapshots, or open connections exceed the configured session-pool size (default ≤10) at any snapshot. *(Refined during analyze: the monotonic rule is computed on the leak signal — scratch excluding in-flight cases — so concurrent uploads don't false-trip it; the 1GB cap stays on raw scratch as the backstop.)* (Deviates from the drafted 10-min-timer + 500MB cap: the timer is weaker than the repo's existing per-case-cleanup convention, and a fixed 500MB cap can false-fail on 2-3 legitimately concurrent large in-flight cases; the monotonic-growth rule is the duration-independent primary bound, the 1GB ceiling only a gross-runaway backstop.)
- Q: Fresh XNAT project per run, or a fixed reused project name? → A: Fresh, run-id-scoped project per invocation (e.g., SIM016_<timestamp>); never reuse or assume a fixed name. Riders: the procedure records a cheap pre-run inventory of existing server projects in the report preamble (evidence the server wasn't pristine) but never writes to or asserts about non-run projects, and the simulation never deletes anything outside its own run project. (Within-run history accumulation already supplies the "living archive" realism; the repo's own dedup-lane result history shows prior-run residue corrupts dedup verdicts, and cross-run persistence isn't dependable given docker-compose down loses all data.)

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Unattended Long-Run Health Verification (Priority: P1)

An operator boots a real, Dockerized XNAT instance on their local machine and wants a *different* agent — one with no memory of today's planning conversation — to pick up a written procedure, run a sustained simulated-usage session against that live server, and produce a machine-readable verdict on whether the system held up. The operator is not present to babysit the run or answer questions while it executes.

**Why this priority**: This is the entire point of the feature. Without a self-contained, context-free procedure a future agent can execute cold, nothing else in this spec matters — the simulation exists to be handed off, not run in this session.

**Independent Test**: Hand the finished procedure document (with no other context) to a fresh agent session pointed at a booted local XNAT container; confirm the agent can locate the entry point, run it in "smoke" mode (~1-2 minutes) to completion, and read a pass/fail verdict from the produced report without needing to ask the operator anything.

**Acceptance Scenarios**:

1. **Given** a freshly booted local XNAT container and the written procedure, **When** a new agent with no prior context runs the smoke-mode entry point, **Then** the run completes within its bounded time budget and produces a report file stating PASS or FAIL with supporting counts.
2. **Given** the same setup, **When** the agent runs sustained/long-form mode instead, **Then** the run self-terminates at the configured duration (not run indefinitely) and produces the same report shape as smoke mode.
3. **Given** no operator is available mid-run, **When** the simulation hits a recoverable error (e.g., a transient upload failure), **Then** it logs the error, continues the run, and reflects the error in the final error-rate metric rather than halting the whole run.

---

### User Story 2 - Realistic Trickle of Surgical Case Arrivals (Priority: P1)

Rather than a fixed batch of cases uploaded all at once (the pattern of the existing burst-style stress lanes), the simulation must generate an open-ended stream of synthetic surgical cases that arrive at uneven, unpredictable intervals over the run's duration — the way real cases arrive at a surgical imaging archive over a shift, not the way a load-test script fires all its requests at t=0.

**Why this priority**: Uneven arrival timing is what distinguishes this simulation from work that already exists (`tests/stress/lane_volume.py`, `lane_concurrent.py`). Without it, this spec would just be a slower duplicate of an existing lane rather than a genuinely new coverage class (overlapping reads/writes, connection lifecycle over time, gradual resource accumulation).

**Independent Test**: Run the simulation and inspect the event timeline in the report; confirm case-arrival timestamps are spread across the run's duration with variable gaps (not clustered at the start) and that at least a configurable minimum number of distinct case "shapes" (size, modality mix, malformed/edge-case injection) appeared during a long-form run.

**Acceptance Scenarios**:

1. **Given** a sustained-mode run of at least 30 minutes, **When** the run completes, **Then** the recorded case-arrival timestamps show gaps of varying length (not a uniform fixed interval and not all bunched at the start).
2. **Given** a long-form run producing 30+ cases, **When** case variety is inspected, **Then** cases include a mix of normal, malformed/edge-case, and dedup-collision-triggering examples in roughly the configured proportions, not a single repeated shape.
3. **Given** the smoke-mode run (short duration), **When** it completes, **Then** it still produces at least one full write-then-read cycle so the procedure is verifiable quickly without waiting for a long run.

---

### User Story 3 - Concurrent Reads While Writes Are In Flight (Priority: P2)

While synthetic cases, annotations, and segmentation submissions are being uploaded, background read traffic (browsing subjects/experiments, querying server inventory, downloading previously-uploaded cases) happens *concurrently*, simulating a researcher or dashboard user who is looking at the archive while new data continues to stream in — rather than the existing lanes' pattern of "finish all writes, then check state once at the end."

**Why this priority**: Concurrent read/write interleaving is a realistic production condition (a clinician browsing while an upload is mid-flight) that the existing one-shot lanes do not exercise. It is P2 rather than P1 because the sustained-arrival model (User Story 2) is the load-bearing novelty; interleaved reads add realism on top of it.

**Independent Test**: Run the simulation and confirm from the event timeline that read-type events (browse/query/download) have timestamps that fall *between* write-type events' start and completion, not only after the last write of the run.

**Acceptance Scenarios**:

1. **Given** a sustained-mode run, **When** the event timeline is inspected, **Then** at least one read event's timestamp falls strictly between the start and completion timestamps of an in-flight write event.
2. **Given** a read event occurs while a case is still being uploaded, **When** the read completes, **Then** it does not error simply because of a concurrent write (partial/in-progress state is handled gracefully, not treated as corruption).

---

### User Story 4 - Data Integrity Sampled Throughout the Run, Not Only at the End (Priority: P2)

Periodically during the run (not only once at the very end), the procedure re-verifies that previously-uploaded data is still intact — checksums match, annotation/segmentation links to their source images still resolve, and no case has silently vanished or been corrupted by a later concurrent operation.

**Why this priority**: A single end-of-run integrity check cannot distinguish "everything was fine the whole time" from "something broke in the middle and nobody noticed until the end." Periodic sampling is what makes the unattended verdict trustworthy. P2 because it depends on User Stories 1-2 (the run and its data) already existing to sample from.

**Independent Test**: Run the simulation and confirm the report includes multiple timestamped integrity-check snapshots across the run's duration (not a single check at t=end), each recording pass/fail per sampled case.

**Acceptance Scenarios**:

1. **Given** a sustained-mode run of sufficient length, **When** the report is inspected, **Then** it contains at least 3 distinct integrity-check snapshots spread across the run's duration.
2. **Given** an integrity check runs mid-simulation while other uploads are still in flight, **When** it samples already-completed (not in-flight) cases, **Then** it does not falsely flag an in-progress upload as corrupted.
3. **Given** the run completes, **When** the final report is read, **Then** it clearly states whether any integrity check (at any point in the run, not just the last one) ever failed.

---

### Edge Cases

- What happens when the simulation's process is interrupted (killed) partway through a long-form run? The report/log up to that point must remain readable and not corrupt prior evidence.
- What happens when the local XNAT container becomes briefly unresponsive (e.g., under its own resource pressure) mid-run? The simulation must treat this as a recoverable error class, log it, and keep going rather than crashing outright.
- What happens when a malformed/edge-case synthetic file the simulation intentionally injects causes an unexpected (not just an anticipated) failure mode? It must be logged with enough detail to distinguish "expected malformed-input rejection" from "unexpected crash."
- What happens if the run's configured duration is longer than the operator intends to leave the machine on? The procedure must document how to safely stop a long-form run early and still get a partial report.
- What happens when the dataset generator is asked to sustain a run far longer than initially tested (e.g., several hours)? The design must avoid unbounded local disk/memory growth (temp file cleanup, connection reuse) so that duration alone does not become a failure cause independent of the system under test.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The design MUST define a synthetic case generation strategy that reuses/extends the repo's existing synthetic-data and factory generators to produce an open-ended stream of surgical cases (not a fixed small batch), with configurable variety across case size, modality mix, malformed/edge-case injection rate, and dedup-collision injection rate. Default variety proportions when unconfigured: 80% normal / 10% malformed-or-edge-case / 10% dedup-collision probes, with absolute floors of ≥3 malformed injections and ≥3 dedup probes per sustained run regardless of percentage.
- **FR-002**: The design MUST define a time-distribution model governing when simulated events (case uploads, annotation submissions, segmentation submissions, read/browse/query events, config updates) occur across a run, such that arrivals are spread unevenly across the run's duration rather than issued all at once. Default event-rate targets are coupled to the default duration: mean case inter-arrival ≤ ~110 seconds, so a default-length sustained run is expected to produce ≥30 cases.
- **FR-003**: The design MUST support two run modes: a bounded "smoke" mode (on the order of 1-2 minutes) for quick verification, and a "sustained" mode supporting durations from tens of minutes up to multiple hours, both self-terminating at a configured duration. When the operator does not specify a duration, sustained mode defaults to 60 minutes.
- **FR-004**: The design MUST define how read-type events (browsing, querying, downloading) are interleaved to occur concurrently with in-flight write-type events, not only after all writes complete.
- **FR-005**: The design MUST define periodic, in-run data-integrity sampling (checksum + link-resolution checks against previously-completed uploads) occurring multiple times across a run's duration, not only once at the end. Default snapshot cadence: ≤ ~15 minutes, yielding ≥4 snapshots in a default-length (60-minute) sustained run. Each snapshot also records resource observations per FR-006.
- **FR-006**: The design MUST define machine-readable success/health signals a future unattended agent can evaluate without a human present:
  - **Unexpected-failure threshold**: the run FAILs if unexpected *terminal* failures (failures remaining after the FR-011 retry policy is exhausted) exceed max(1, ceil(2% × case-write events)). The denominator counts case-write EVENTS, not individual retry attempts — retries are tracked separately and never loosen the gate. Expected malformed-case rejections are excluded from this ratio and tracked separately using the repo's existing ACCEPTED/FRIENDLY/CRASH tri-state classification; an injected malformed case that is silently ACCEPTED or CRASHes counts as an unexpected failure.
  - **Minimum-activity precondition**: a run in which zero cases ever completed cannot PASS, regardless of the other checks (verdict reason: no completed writes). This closes the vacuous-PASS path where a run whose only write failed would otherwise satisfy the max(1, …) floor.
  - **Zero-tolerance integrity clause**: any detected data loss/corruption (any integrity-snapshot failure at any point in the run) = FAIL at count 1, independent of the ratio above.
  - **Resource bounds**: per-case temp cleanup on case completion (finally-style, matching the existing stress lanes' convention); resource verification piggybacked on every integrity snapshot (simulation-attributable scratch bytes + open-connection count); FAIL/flag if raw scratch exceeds 1GB at any sampled point OR the leak signal (scratch excluding in-flight cases' working directories) grows monotonically across 3+ consecutive snapshots, or open connections exceed the configured session-pool size (default ≤10) at any snapshot. Measuring monotonic growth on the leak signal (not raw scratch) prevents legitimately concurrent in-flight uploads from registering as growth while still catching bytes that outlive their case.
- **FR-007**: The design MUST define a written procedure document, self-contained enough for a different agent with no prior conversational context to execute after a human has manually started the local Dockerized XNAT instance, including how to locate the entry point, how to choose smoke vs. sustained mode, and how to interpret the final report. Each invocation MUST target a fresh, run-id-scoped XNAT project (e.g., SIM016_<timestamp>) — never a reused fixed project name. The procedure records a pre-run inventory of existing server projects in the report preamble but never writes to, asserts about, or deletes anything outside its own run project.
- **FR-008**: The design MUST define the structure of the final run report (or equivalent machine-readable output) including overall PASS/FAIL verdict, event counts by type, error rate, integrity-check history, and resource-growth observations.
- **FR-009**: The design MUST explicitly target the real, Docker-local XNAT instance described in the repo's existing integration test setup; it MUST NOT rely on or reference the offline FakeXNAT test double as a substitute for the live server.
- **FR-010**: The design MUST document how a long-form run can be safely stopped early by an operator (or a future agent) and still yield a readable partial report rather than an unreadable or corrupted one.
- **FR-011**: The design MUST document handling for a mid-run XNAT unavailability/unresponsiveness condition as a recoverable error class distinct from a fatal design flaw, including how the simulation continues or safely pauses/retries.
- **FR-012**: The design MUST NOT require implementation of the actual STAPLE consensus algorithm; any consolidation-adjacent activity the simulation exercises MUST be scoped to the repo's existing reference aggregator / registration seam, not a new STAPLE implementation.
- **FR-013**: This feature's scope is design-and-planning only. It MUST NOT include writing the simulation's implementation code; implementation is explicitly deferred to a future, separately-authorized pass.

### Key Entities

- **Simulated Case Arrival**: A synthetic surgical case (DICOM set, optional video/multiframe content) generated and uploaded at a scheduled point during the run; carries a shape descriptor (size, modality mix, whether it's intentionally malformed or a dedup-collision probe).
- **Event Timeline**: The chronological record of every simulated action (write or read) during a run, including type, start time, completion time, and outcome — the evidence base the final report and the concurrent-interleaving acceptance scenarios are checked against.
- **Integrity Snapshot**: A single point-in-time check confirming that some subset of previously-completed cases still checksum-match and still have resolvable annotation/segmentation links; multiple snapshots occur across one run.
- **Run Report**: The end-of-run (or safely-stopped-early) machine-readable summary: verdict, counts, error rate, integrity-snapshot history, resource-growth notes.
- **Run Mode Configuration**: The operator/agent-selectable parameters distinguishing a smoke run from a sustained run — duration (sustained default: 60 minutes), event-rate targets (default mean inter-arrival ≤ ~110s), variety proportions (default 80/10/10 with ≥3-count floors per injected class), integrity-snapshot cadence (default ≤ ~15 minutes), and the run-id-scoped project name.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A different agent, given only the procedure document and a booted local XNAT container, can start a smoke-mode run and obtain a PASS/FAIL verdict within 5 minutes of wall-clock time, without needing any clarification from the operator.
- **SC-002**: In a sustained-mode run of at least 30 minutes, case arrival timestamps show measurable variability in inter-arrival gaps (not a constant fixed interval) — verifiable by inspecting the event timeline's arrival-time distribution.
- **SC-003**: In a sustained-mode run producing 30 or more simulated cases, at least three distinct case "shapes" (by size/modality/malformed/dedup-collision category) are represented in proportions matching the configured variety targets within a reasonable tolerance.
- **SC-004**: At least one read-type event per run is observed with a timestamp strictly between the start and completion of a concurrently in-flight write-type event.
- **SC-005**: A sustained-mode run of sufficient length produces 3 or more integrity-check snapshots spread across its duration, and the final report clearly states whether any snapshot at any point failed.
- **SC-006**: A run interrupted mid-execution (simulated early stop) still yields a readable partial report reflecting all events completed up to the interruption point, with zero unreadable/corrupted report files.
- **SC-007**: Across a multi-hour design-target run, local resource usage does not grow unbounded, verified against the concrete FR-006 bounds: simulation-attributable scratch stays under 1GB at every snapshot and never grows monotonically across 3+ consecutive snapshots; open connections never exceed the configured session-pool size (default ≤10) at any snapshot.
- **SC-008**: Unexpected terminal failures across a full sustained run stay within max(1, ceil(2% × case-write events)) — retries tracked separately, never inflating the denominator — and every recorded error is attributed in the report to exactly one of the tri-state classes (expected FRIENDLY rejection vs. unexpected ACCEPTED-malformed or CRASH) — the expected and unexpected classes are never conflated.

## Assumptions

- The local XNAT instance is booted by a human operator before the future agent's run begins; booting the container itself is out of scope for this feature (covered by the repo's existing `tests/integration/xnat_local/docker-compose.yml`).
- "Another agent" executing this procedure has full repository access and standard tool capabilities (shell execution, file read/write) but no memory of the conversation that produced this spec — the procedure document is the only context it can rely on.
- Smoke mode's primary purpose is fast feedback during development/verification of the simulation itself, not full coverage; sustained mode is the mode that actually exercises the realistic, uneven-arrival, concurrent-read/write behavior this feature is about.
- The existing repo synthetic-data and stress-lane generators (`tests/synthetic_data.py`, `tests/stress/factory.py`, `tests/stress/driver.py`) are assumed reusable building blocks; this feature extends their variety/timing behavior rather than replacing them.
- STAPLE consensus remains an intentional stub in this repo; any consolidation-related simulated activity is scoped to what the existing reference aggregator/seam already supports.
- "Real-world sustained" load, per the operator's framing, means realistic unevenness and duration, not adversarial maximum-throughput stress testing (that class of test already exists in the burst-style stress lanes).
- No production PHI is ever involved; all case data is synthetic, matching the existing repo convention for stress/integration testing.
