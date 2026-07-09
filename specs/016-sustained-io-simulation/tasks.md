---

description: "Task list for feature 016 - Sustained Real-World I/O Simulation Against Local XNAT"
---

# Tasks: Sustained Real-World I/O Simulation Against Local XNAT

> ## GATED: implementation not authorized yet (FR-013)
>
> This breakdown is prepared so a future authorized pass can execute without re-planning.
> Feature 016 is **design-and-planning only** per spec.md FR-013 and plan.md's scope guard:
> "No simulation code, no lane script, and no procedure document are written by this
> feature — only the planning artifacts in `specs/016-sustained-io-simulation/`." Nothing
> below is a work order for the current session. Do not create, edit, or run any of the
> files or commands referenced in this document until the operator gives explicit
> go-ahead for the implementation pass. This file itself is the only artifact this
> speckit-tasks run produces.

**Input**: Design documents from `specs/016-sustained-io-simulation/`: `spec.md` (4 user
stories, FR-001..013, SC-001..008, 5 binding clarification defaults), `plan.md`
(architecture + Implementation Strategy sequencing 1→2→3→{4,5}), `research.md` (D1-D7),
`data-model.md` (5 entities, state machine, validation rules), `contracts/cli.md`,
`contracts/run-report.md`, `contracts/procedure-doc.md`, `quickstart.md`.

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/ — all present
and read in full before this file was generated.

**Tests**: NOT optional for this feature. Constitution Principle IV ("Testable Offline")
and plan.md's Technical Context require the pure-logic seams (`sim016_schedule.py`,
`sim016_report.py`) to ship with offline unit tests in the default CI gate. Live-server
tasks are explicitly opt-in (they exercise the real Docker-local XNAT per FR-009) and are
labeled accordingly below.

**Organization**: Tasks are grouped by user story (spec.md priority order: US1 P1, US2 P1,
US3 P2, US4 P2) to enable independent implementation and testing of each story, per
`.specify/templates/tasks-template.md`. A Foundational phase precedes all stories because
`sim016_schedule.py` and `sim016_report.py` encode every clarified numeric threshold
(duration, variety, cadence, ratio, resource bounds) that every later phase depends on.

## Format: `[ ] T### [P?] [US#?] Description`

- **[P]**: Can run in parallel (different target file, no dependency on a same-phase task
  not yet done)
- **[US#]**: Which user story this task belongs to (US1-US4); Setup/Foundational/Polish
  tasks carry no story tag because they are shared infrastructure
- Every task cites the contract, research decision (D1-D7), functional requirement
  (FR-00#), or success criterion (SC-00#) it implements or validates
- Exact file paths are given for every task; paths match `plan.md`'s Project Structure
  section verbatim

## Path Conventions (per plan.md Project Structure — single project)

```text
tests/stress/lane_sim016.py            # [FUTURE] coordinator: CLI, scheduler, dispatcher, signals, report
tests/stress/sim016_schedule.py        # [FUTURE] pure logic: draws, variety, floors, timeline
tests/stress/sim016_report.py          # [FUTURE] pure logic: ledger, verdict, atomic report/JSONL writes
tests/stress/test_sim016_schedule.py   # [FUTURE] offline unit tests (default CI gate)
tests/stress/test_sim016_report.py     # [FUTURE] offline unit tests (default CI gate)
tests/stress/results/                  # REUSED convention: sim016_<run_id>.json + .events.jsonl + .pid
docs/SIM016_PROCEDURE.md               # [FUTURE] procedure doc per contracts/procedure-doc.md
docs/STRESS_TEST_PLAN.md               # existing lane inventory + results log — cross-referenced, not replaced
```

Reused as-is, never modified by this feature: `tests/stress/driver.py`,
`tests/stress/factory.py`, `tests/stress/malformed.py`, `tests/stress/lane_annotations.py`,
`tests/stress/lane_concurrent.py`, `tests/synthetic_data.py`,
`tests/integration/xnat_local/docker-compose.yml`, `src/annotations/aggregate/staple.py`
(untouched stub, FR-012).

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Scaffold the three new module files and confirm the repo's existing
conventions (dependency pins, pytest collection rules) already cover this feature with no
new configuration.

- [ ] T001 Confirm no new dependency is required: `pydicom`, `numpy`, `pandas`, `requests`,
  `pyxnat` are already pinned in `requirements.txt`; record this confirmation in a header
  comment of each new module (T002-T004) — per plan.md Technical Context ("No new heavy
  dependencies; any future proposal to add one requires its own justification in
  tasks.md")
- [ ] T002 [P] Create `tests/stress/sim016_schedule.py` module skeleton (module docstring
  citing FR-001/FR-002/FR-005 and research.md D1, no logic yet) — per plan.md Project
  Structure
- [ ] T003 [P] Create `tests/stress/sim016_report.py` module skeleton (module docstring
  citing FR-006/FR-008 and research.md D4, no logic yet) — per plan.md Project Structure
- [ ] T004 [P] Create `tests/stress/lane_sim016.py` module skeleton with an
  `if __name__ == "__main__":` entry point matching the `tests/stress/lane_volume.py` /
  `tests/stress/lane_concurrent.py` `main()` convention (empty body, no logic yet) — per
  plan.md Project Structure and contracts/cli.md's `python -m tests.stress.lane_sim016`
  entry point
- [ ] T005 Confirm pytest collection boundary needs no `pytest.ini` change: `python_files =
  test_*.py` (see `pytest.ini`) will auto-collect `tests/stress/test_sim016_schedule.py`
  and `tests/stress/test_sim016_report.py` into the default offline gate, while
  `sim016_schedule.py`, `sim016_report.py`, and `lane_sim016.py` (no `test_` prefix) stay
  excluded exactly like the existing `lane_*.py` / `factory.py` / `driver.py` / `malformed.py`
  files — per plan.md Testing convention and Constitution IV's offline/live split

**Checkpoint**: Three empty module files exist; collection boundary verified; no CI
surface changed yet.

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Implement the two pure-logic modules — `sim016_schedule.py` and
`sim016_report.py` — that encode every clarified numeric threshold (60-min default
duration, ≤110s mean inter-arrival, 80/10/10 variety with ≥3/≥3 floors, ≤15-min snapshot
cadence, max(1, 2%) ratio, 1 GB / 3-snapshot resource bounds, atomic-write crash safety).
These modules are consumed by every later phase and are unit-testable **without a live
server**, satisfying Constitution IV.

**CRITICAL**: No user-story work (Phase 3+) can begin until this phase is complete —
`lane_sim016.py`'s CLI validation (US1), timeline generation (US2), verdict wiring (US4),
and exit-code/report behavior (US1) all call into these two files.

### `sim016_schedule.py` — pure logic

- [ ] T006 Implement per-stream inter-arrival draw function
  (`random.Random(seed).expovariate(1.0 / mean_s)`, clamped to `[2s, 6×mean_s]`) usable for
  the three independent streams (case writes mean 110s/20s, reads mean 45s/15s, config
  updates mean 900s/disabled) in `tests/stress/sim016_schedule.py` — per research.md D1 and
  data-model.md §1
- [ ] T007 Implement weighted case-variety draw (80% normal / 10% malformed / 10%
  dedup-probe) producing the shape descriptor (`category`, `subtype`, `frames`, `rows`,
  `cols`, `expected_outcome`, and the seeded `has_annotations` draw for ~30% of normal
  cases — decided HERE at timeline generation, not in the worker, to preserve
  determinism-given-seed per data-model.md §2) in `tests/stress/sim016_schedule.py` —
  per FR-001 and data-model.md §2 (depends on T006 for the RNG handle)
- [ ] T008 Implement the deterministic floor top-up pass (re-label the latest normal
  arrivals so a sustained-mode timeline holds ≥3 malformed and ≥3 dedup-probe cases
  regardless of the weighted draw's outcome; floors waived in smoke mode) honoring the
  dedup-probe ordering guarantee: a probe may only be created where an eligible earlier
  base case exists, and every probe's `scheduled_at` must land strictly after its base's
  scheduled time plus the expected-completion margin, in
  `tests/stress/sim016_schedule.py` — per FR-001, research.md D1, and data-model.md §2
  ordering guarantee (depends on T007)
- [ ] T009 Implement full timeline pre-generation: accumulate case-write/read/config-update
  Poisson streams (T006) with per-arrival variety assignment (T007) and floor top-up (T008)
  until `duration_s` is exhausted, plus fixed-cadence (default 720s, ≤900s ceiling) snapshot
  ticks with ±60s jitter and one mandatory final snapshot; smoke-mode compression (90s
  duration, 20s mean, one end-of-run snapshot, floors waived, guaranteed ≥1 full
  write-then-read cycle) in `tests/stress/sim016_schedule.py` — per research.md D1 and
  data-model.md §1 (depends on T006-T008)
- [ ] T010 [P] Implement CLI-config validation-rule functions: variety sums to 1.0 ± 0.001;
  `duration_s` ≥ 60 (sustained) / 30-300 (smoke); `mean_interarrival_s` > 0 and ≤ 120 for
  sustained; `snapshot_cadence_s` ≤ 900 and < `duration_s`; `writer_pool_size + 2 ≤
  max_connections`; `url` host ∈ {localhost, 127.0.0.1, explicit disposable-host env value}
  (production RPACS hostname is a hard error) in `tests/stress/sim016_schedule.py` — per
  data-model.md §1 validation rules, contracts/cli.md startup constraint checks, and
  Constitution I/V

### `sim016_report.py` — pure logic

- [ ] T011 [P] Implement the tri-state failure classification helper (TRANSIENT → retry
  candidate; FRIENDLY → deterministic rejection; ACCEPTED-when-injected-malformed and
  CRASH → unexpected) reusing `lane_malformed.py`'s ACCEPTED/FRIENDLY/CRASH semantics
  verbatim, plus the dedup-probe rule (`DedupReviewRequired` raised = expected; clean
  publish = unexpected) in `tests/stress/sim016_report.py` — per research.md D7
- [ ] T012 Implement the event/case ledger (thread-and-process-safe accumulation of event
  records per data-model.md §3, case records with the `SCHEDULED → GENERATING → UPLOADING →
  (RETRY_WAIT → UPLOADING)* → COMPLETED | REJECTED_FRIENDLY | FAILED_TERMINAL` state machine
  per data-model.md §2, and tri-state tallies) in `tests/stress/sim016_report.py` — per
  data-model.md §2/§3 (depends on T011 for classification inputs)
- [ ] T013 Implement verdict computation exactly per contracts/run-report.md §2: (1)
  `ratio_check.pass` ⇔ `unexpected_terminal ≤ max(1, ceil(0.02 × write_attempt_total))`,
  where `write_attempt_total` counts case-write EVENTS (retries tallied separately in
  `retry_total`, never inflating the denominator); (2) `integrity_check.pass` ⇔ no snapshot
  has `pass = false` (zero-tolerance, independent of ratio); (3) `resource_check.pass` ⇔
  every snapshot has `resource_ok = true` (raw `scratch_bytes < 1_073_741_824` AND
  `scratch_monotonic_run_len < 3` computed over `scratch_leak_bytes` AND
  `open_connections ≤ max_connections`); (4) `overall = "PASS"` ⇔ all three pass AND
  `completed_cases ≥ 1` (else FAIL with reason `no_completed_writes`; `partial`/
  `stopped_early` are orthogonal flags that never enter the formula); one human-readable
  sentence per failed check appended to `reasons[]` in `tests/stress/sim016_report.py` —
  per contracts/run-report.md §2 and FR-006 (depends on T012)
- [ ] T014 [P] Implement the atomic JSON report writer (`tempfile` + `os.replace`, invoked
  after every snapshot and at finalization) and the append-only JSONL event writer (one line
  per completed event, tolerant of a truncated final line on `kill -9`) in
  `tests/stress/sim016_report.py` — per research.md D4/D6
- [ ] T015 [P] Implement report serialization matching contracts/run-report.md §1's draft-07
  schema exactly (`schema_version: "016.1"`, all required top-level keys `run`/`counts`/
  `failures`/`snapshots`/`verdict`/`timeline_ref`, `run.preamble` block, credentials never
  serialized) in `tests/stress/sim016_report.py` — per research.md D4 and data-model.md §5
  (depends on T012, T013)

### Offline unit tests (default CI gate) — encode every clarified threshold

- [ ] T016 [P] Unit tests for inter-arrival draws: assert non-uniform, non-constant gaps and
  correct `[2s, 6×mean]` clamping for all three streams in
  `tests/stress/test_sim016_schedule.py` — offline, default CI gate; pure-logic proof of
  SC-002 (depends on T006)
- [ ] T017 [P] Unit tests for variety draw + floor top-up: assert ≥3 malformed and ≥3 dedup
  cases are present in short/edge-case timelines regardless of the weighted-draw outcome,
  and that floors are waived in smoke mode, in `tests/stress/test_sim016_schedule.py` —
  offline, default CI gate; pure-logic proof of FR-001 (depends on T007, T008)
- [ ] T018 [P] Unit tests for full timeline generation: determinism given a fixed seed,
  exhaustion at `duration_s`, ≥4 snapshot ticks in a default 60-min sustained timeline, and
  the smoke-mode one-full-write-then-read-cycle guarantee, in
  `tests/stress/test_sim016_schedule.py` — offline, default CI gate; pure-logic proof of
  SC-005/SC-003 non-vacuousness (depends on T009)
- [ ] T019 [P] Unit tests for config validation rules: reject a variety tuple not summing to
  1.0, reject a non-localhost URL, reject `writer_pool_size + 2 > max_connections`, in
  `tests/stress/test_sim016_schedule.py` — offline, default CI gate; proof of contracts/cli.md
  startup constraint checks (depends on T010)
- [ ] T020 [P] Unit tests for tri-state classification: every D7 branch (transient →
  retry-eligible, FRIENDLY on injected malformed → expected, ACCEPTED on injected malformed
  → unexpected, CRASH → unexpected, `DedupReviewRequired` on a dedup probe → expected, clean
  publish of a dedup probe → unexpected) in `tests/stress/test_sim016_report.py` — offline,
  default CI gate (depends on T011)
- [ ] T021 [P] Unit tests for verdict computation against contracts/run-report.md §2's
  binding rules: ratio boundary exactly at `max(1, ceil(0.02 × N))` with N = case-write
  events (assert retries do NOT inflate the denominator), zero-tolerance
  single-snapshot-failure FAIL independent of ratio, resource_check as AND-of-all-snapshots
  with monotonic run computed over `scratch_leak_bytes`, the `completed_cases ≥ 1`
  precondition (assert `completed=0, write_attempt_total=1, unexpected_terminal=1,
  snapshots=[{sampled:[], pass:true, resource_ok:true}]` yields FAIL with
  `no_completed_writes` — the closed vacuous-PASS loophole), and `partial`/`stopped_early`
  flag orthogonality (an early-stopped clean sustained run PASSes with `partial=true`), in
  `tests/stress/test_sim016_report.py` — offline, default CI gate; pure-logic proof of
  SC-008 (depends on T013)
- [ ] T022 [P] Unit tests for atomic-write crash safety: simulate an interrupted rewrite and
  assert the prior valid report file is unaffected; assert the JSONL reader tolerates and
  discards a truncated final line, in `tests/stress/test_sim016_report.py` — offline,
  default CI gate; pure-logic proof of SC-006 (depends on T014)
- [ ] T023 [P] Unit tests for report JSON schema conformance: validate a representative
  produced report dict against every required key/type in contracts/run-report.md §1 (manual
  required-key/type walk, or `jsonschema` if already available offline — no new dependency
  without justification per T001), in `tests/stress/test_sim016_report.py` — offline,
  default CI gate (depends on T015)

**Checkpoint**: `sim016_schedule.py` and `sim016_report.py` are complete, unit-tested, and
green in the default offline CI gate. Every clarified numeric default and every verdict
rule now has a pure-logic proof independent of any live server. User-story implementation
may begin.

---

## Phase 3: User Story 1 - Unattended Long-Run Health Verification (Priority: P1) 🎯 MVP (part 1 of 2)

**Goal**: A fresh, context-free agent can locate the entry point, run smoke mode to
completion, and read a PASS/FAIL verdict from the report without asking the operator
anything — the entire point of the feature (spec.md "Why this priority").

**Independent Test**: Hand `docs/SIM016_PROCEDURE.md` alone to a fresh agent session
pointed at a booted local XNAT container; confirm it runs smoke mode to completion and
reads a verdict unassisted.

This phase creates the `lane_sim016.py` coordinator skeleton — CLI, precondition probes,
project bootstrap, signal handling, exit codes — that Phases 4-6 extend with actual
scheduling, reading, and sampling behavior. It must land before those phases begin editing
the same file.

- [ ] T024 [US1] Implement the CLI argument parser in `tests/stress/lane_sim016.py`
  matching contracts/cli.md exactly: every argument, type, and default (`--mode`,
  `--duration-s`, `--seed`, `--mean-interarrival-s`, `--read-mean-interarrival-s`,
  `--variety`, `--min-malformed`, `--min-dedup`, `--snapshot-cadence-s`,
  `--writer-pool-size`, `--max-connections`, `--retry-max`, `--stop-grace-s`, `--url`,
  `--user`, `--password`, `--project`, `--results-dir`, `--verbose`/`-v`) — per
  contracts/cli.md Arguments table
- [ ] T025 [US1] Wire environment-variable overrides (`XNAT_SERVER_URL`, `XNAT_USERNAME`,
  `XNAT_PASSWORD`) and set `XNAT_PROJECT_NAME` to the run project **before** importing
  `src.*`, matching the existing lane convention, in `tests/stress/lane_sim016.py` — per
  contracts/cli.md Environment variables (depends on T024)
- [ ] T026 [US1] Implement startup precondition probes: `GET /data/version` (fallback `GET
  /data/projects`) within 15s, and `python -c "import pydicom, numpy, pandas, requests,
  pyxnat"`-equivalent dependency check; failures exit 2 with a stated reason and a
  next-step fix (Constitution II) in `tests/stress/lane_sim016.py` — per contracts/cli.md
  startup constraint checks and quickstart.md §1 (depends on T024)
- [ ] T027 [US1] Implement the localhost/disposable-host startup guard as a hard error (exit
  2) using the T010 validation function, and call all T010 config-validation rules at
  startup before any server-side write, in `tests/stress/lane_sim016.py` — per Constitution
  I/V, data-model.md §1, and contracts/cli.md ("violations ⇒ exit 2, nothing written
  server-side") (depends on T010, T024)
- [ ] T028 [US1] Implement fresh run-id generation (`SIM016_<UTC-ts>_<hex4-from-seed>`), an
  explicit pre-bootstrap project-existence probe (`GET /data/projects/{project}`; HTTP 200 ⇒
  exit 2 — MUST be new lane code: `driver.connect`'s PUT-project bootstrap treats 409
  already-exists as success and cannot refuse a reused name, including an
  operator-overridden `--project`), then XNAT project bootstrap via `driver.connect`, in
  `tests/stress/lane_sim016.py` — per research.md D5 and contracts/cli.md `--project`
  semantics (depends on T025)
- [ ] T029 [US1] Implement the pre-run server-project inventory preamble: capture
  `xnat_version` and the list of pre-existing projects via `driver.server_inventory` into
  `run.preamble` (recorded as evidence only, never asserted against, never written to) in
  `tests/stress/lane_sim016.py` — per FR-007 and contracts/run-report.md §1 `run.preamble`
  (depends on T028)
- [ ] T030 [US1] Implement pid-file write (`<results-dir>/sim016_<run_id>.pid`) at startup
  and removal at clean exit, plus SIGINT/SIGTERM handlers: first signal stops new-event
  dispatch, applies the `--stop-grace-s` grace window to in-flight writes, runs one final
  integrity snapshot against completed cases, and finalizes the report with
  `stopped_early=true`/`partial=true`/exit 3; second signal (or grace expiry) finalizes
  immediately from the ledger, in `tests/stress/lane_sim016.py` — per research.md D6
  (satisfies FR-010, SC-006, spec Edge Case 1) (depends on T012, T014, T028)
- [ ] T031 [US1] Wire the exit-code contract (0 PASS / 1 FAIL / 2 precondition error / 3
  early-stopped partial) and the three-line greppable console trailer
  (`VERDICT: PASS|FAIL`, `REPORT: <path>`, `PARTIAL: true|false`) on every completion path,
  in `tests/stress/lane_sim016.py` — per contracts/cli.md Exit codes and Console output
  contract (depends on T013, T015, T030)
- [ ] T032 [US1] Implement the `--verbose`/`-v` human-readable progress line format
  (timestamp, event kind, outcome) distinct from the machine-greppable trailer lines, in
  `tests/stress/lane_sim016.py` — per contracts/cli.md `--verbose` flag (depends on T024)
- [ ] T033 [US1] Write `docs/SIM016_PROCEDURE.md` covering all 9 required sections in
  contracts/procedure-doc.md's order (Purpose and scope; Preconditions; Smoke run; Sustained
  run; Reading the verdict; Stopping a run early; Mid-run server unresponsiveness; Cleanup;
  Troubleshooting table), expanding `quickstart.md`'s skeleton into copy-pasteable commands
  with no reference to this planning conversation — per contracts/procedure-doc.md and
  FR-007 (depends on T024-T032 existing so the documented commands are accurate)

### Validation for User Story 1 (opt-in live-server lane — requires a booted Docker-local XNAT; not part of the default CI gate)

- [ ] T034 [US1] [P] Run smoke mode (`python -m tests.stress.lane_sim016 --mode smoke`)
  against a booted local XNAT and confirm completion within 5 minutes wall clock with exit
  code 0 or 1 and all three trailer lines present — proves SC-001 and spec.md US1
  Acceptance Scenario 1 (opt-in: live server, e.g. gated the way spec-014's
  `RUN_XNAT_DUAL=1` stress lanes are)
- [ ] T035 [US1] [P] Hand `docs/SIM016_PROCEDURE.md` alone (no other context) to a fresh
  agent session pointed at the same booted container and confirm it locates the entry
  point, runs smoke mode, and reads PASS/FAIL unassisted — proves the US1 Independent Test
  and FR-007 directly (opt-in, manual/live)
- [ ] T036 [US1] [P] `kill -9` the coordinator process mid-smoke-run and confirm the
  last atomically-written `sim016_<run_id>.json` and the `.events.jsonl` file both remain
  valid, parseable JSON/JSONL — proves the SC-006 worst case and spec.md Edge Case 1 (opt-in,
  live)

**Checkpoint**: A fresh agent can run smoke mode end-to-end unattended and obtain a
machine-readable verdict with zero clarification needed. Half of MVP complete.

---

## Phase 4: User Story 2 - Realistic Trickle of Surgical Case Arrivals (Priority: P1) 🎯 MVP (part 2 of 2)

**Goal**: Cases arrive at uneven, unpredictable intervals across the run's duration with
configurable variety — the property that distinguishes this simulation from the existing
burst-style stress lanes (spec.md "Why this priority").

**Independent Test**: Run the simulation and inspect the event timeline; confirm
case-arrival timestamps are spread with variable gaps (not clustered at t=0) and that at
least the configured minimum number of distinct case shapes appeared during a long-form
run.

- [ ] T037 [US2] Wire the T009 full-timeline pre-generation call into `lane_sim016.py` at
  run start, using the parsed+validated CLI config, and echo the resolved `seed` into the
  report's `run.seed` field for reproducibility, in `tests/stress/lane_sim016.py` — per
  plan.md Architecture §1 and research.md D1 (depends on T009, T027, T029)
- [ ] T038 [US2] Implement the bounded `multiprocessing.Pool` writer pool (default 3
  processes, hard ceiling from `--max-connections`) that dispatches each pre-generated
  case-write event at its scheduled time, in `tests/stress/lane_sim016.py` — per research.md
  D2 (worker-owns-its-connection shape proven by `lane_concurrent.py`) (depends on T037)
- [ ] T039 [US2] Implement per-shape case generation dispatch inside each writer-pool
  worker: `factory.make_surgery` for `normal`, the matching `tests/stress/malformed.py`
  generator (`truncated_dicom`, `no_instance_number`, `three_channel`, `dup_private_tag`,
  `huge_surgery`, `not_a_dicom`) for each `malformed` subtype, and a
  `factory.overlap_cases`-derived probe (`exact`/`subset`/`superset`/`partial`) against a
  designated earlier base case for each `dedup_probe` subtype — with coordinator-side probe
  gating: dispatch a probe only after its base case is `COMPLETED` (hold + record the
  displacement as a lifecycle event); if the base terminated without completing,
  re-designate the probe as a normal case (`probe_redesignated`, never scored unexpected)
  per data-model.md §2 ordering guarantee — writing into a per-case scratch directory, in
  `tests/stress/lane_sim016.py` — per data-model.md §2 shape descriptor and plan.md Project
  Structure (depends on T038)
- [ ] T040 [US2] Implement write-attempt execution: `driver.connect` + `driver.publish_surgery`
  wrapped in the D7 retry policy (exponential backoff, base 5s, factor 2, jitter ±20%, max 3
  retries, 60s per-attempt cap for transient failures; no retry for FRIENDLY/CRASH/dedup
  outcomes), calling the T011 classification helper on every outcome and recording
  `attempts`/`classification`/`unexpected` AND the server-side `experiment_ref` (captured at
  publish completion; required by the integrity sampler per data-model.md §2) on the case
  record, in `tests/stress/lane_sim016.py` — per research.md D7 (depends on T011, T012,
  T039)
- [ ] T041 [US2] Implement the annotation upload path for cases whose timeline-assigned
  shape carries `has_annotations=true` (the ~30% draw happens in T007's seeded shape
  generation, NOT here — the worker only reads the flag), using the `lane_annotations.py`
  `step_build_annotation_set` / `step_upload_v1` pattern, in
  `tests/stress/lane_sim016.py` — per data-model.md §2 `has_annotations` and plan.md §2
  (depends on T040)
- [ ] T042 [US2] Implement per-case `finally`-style scratch-directory cleanup on every
  terminal state (`COMPLETED` / `REJECTED_FRIENDLY` / `FAILED_TERMINAL`), matching the
  existing stress-lanes' cleanup convention, in `tests/stress/lane_sim016.py` — per FR-006
  and data-model.md §2 validation rule ("scratch dir must not exist after a terminal state")
  (depends on T039)
- [ ] T043 [US2] Wire every case-write attempt and its resulting event record into the T012
  ledger, driving the case state machine
  (`SCHEDULED→GENERATING→UPLOADING→(RETRY_WAIT→UPLOADING)*→terminal`), in
  `tests/stress/lane_sim016.py` — per data-model.md §2 state machine (depends on T012, T040)
- [ ] T044 [US2] Implement the sparse config-update event stream (CLI
  `--config-update-mean-s`, default 900 sustained / disabled smoke per contracts/cli.md),
  reusing the `lane_concurrent.py` `_worker_config_race`-style `database_config.json`
  pull/push + lost-update-guard path, with the D7 accounting rules: config-update events
  are EXCLUDED from `write_attempt_total`; a `LostUpdateError` is the guard working
  correctly ⇒ EXPECTED outcome `guarded_lost_update` (never unexpected); only an
  unclassified exception counts as CRASH, in `tests/stress/lane_sim016.py` — per plan.md
  Constitution VI note and research.md D7 config-update classification (depends on T037)

### Validation for User Story 2 (opt-in live-server lane)

- [ ] T045 [US2] [P] Run a sustained-mode run of ≥20-30 minutes and inspect the event
  timeline for measurably non-uniform, non-clustered-at-start inter-arrival gaps — proves
  SC-002 and spec.md US2 Acceptance Scenario 1 (opt-in, live)
- [ ] T046 [US2] [P] Run a sustained-mode run producing ≥30 cases and confirm ≥3 distinct
  case shapes are represented in proportions matching 80/10/10 within a reasonable
  tolerance, with the ≥3-malformed/≥3-dedup floors met — proves SC-003 and spec.md US2
  Acceptance Scenario 2 (opt-in, live)
- [ ] T047 [US2] Confirm a smoke-mode run (compressed timeline) still produces at least one
  full write-then-read cycle despite the short duration — proves spec.md US2 Acceptance
  Scenario 3 (opt-in, live; may reuse the T034 run's evidence)

**Checkpoint**: Cases arrive unevenly with realistic variety and floors satisfied. **MVP
complete: US1 + US2 together are the minimum meaningful sustained run** — a fresh agent can
now run either mode and get a verdict grounded in genuinely uneven, varied traffic.

---

## Phase 5: User Story 3 - Concurrent Reads While Writes Are In Flight (Priority: P2)

**Goal**: Background read traffic (browse/query/download) is interleaved with in-flight
writes, simulating a researcher browsing the archive while new data streams in — a
realistic condition the existing one-shot lanes never exercise (spec.md "Why this
priority").

**Independent Test**: Run the simulation and confirm from the event timeline that
read-type events have timestamps falling *between* a write-type event's start and
completion, not only after the run's last write.

- [ ] T048 [US3] Implement the reader thread in the coordinator, using a single
  `requests.Session` for read-stream events (browse/query via the `driver.server_inventory`
  pattern; periodic re-download of a previously-completed case's files), in
  `tests/stress/lane_sim016.py` — per research.md D2 and plan.md Architecture §2 (depends on
  T037)
- [ ] T049 [US3] Implement benign-partial-read handling: a concurrent-write-induced
  404/409 on a not-yet-finalized resource is caught and logged as `outcome:
  "benign_partial_read"`, never treated as corruption or an error, in
  `tests/stress/lane_sim016.py` — per data-model.md §3 `outcome` enum and spec.md US3
  Acceptance Scenario 2 (depends on T048)
- [ ] T050 [US3] Wire the read-stream's independent Poisson schedule (from T009, mean 45s/15s)
  to dispatch concurrently with the writer pool's in-flight writes, so a read event's
  `started_at` can by construction fall inside a `case_write`'s
  `[started_at, completed_at]` window, in `tests/stress/lane_sim016.py` — per data-model.md
  §3 Relationships (defines the SC-004 check) (depends on T038, T048)

### Validation for User Story 3 (opt-in live-server lane)

- [ ] T051 [US3] [P] Run a sustained-mode run and confirm at least one read event's
  `started_at` falls strictly between the `started_at`/`completed_at` of an in-flight
  `case_write` event in the JSONL timeline — proves SC-004 and spec.md US3 Acceptance
  Scenario 1 (opt-in, live)
- [ ] T052 [US3] [P] Confirm a read event overlapping an in-flight upload completes with
  `outcome: "benign_partial_read"` rather than an error — proves spec.md US3 Acceptance
  Scenario 2 (opt-in, live)

**Checkpoint**: Read and write traffic genuinely interleave; in-flight reads are handled
gracefully rather than misclassified as corruption.

---

## Phase 6: User Story 4 - Data Integrity Sampled Throughout the Run (Priority: P2)

**Goal**: Previously-uploaded data is periodically re-verified throughout the run (not only
at the end), making the unattended verdict trustworthy against mid-run silent corruption
(spec.md "Why this priority").

**Independent Test**: Run the simulation and confirm the report includes multiple
timestamped integrity-check snapshots across the run's duration, each recording pass/fail
per sampled case.

- [ ] T053 [US4] Implement the snapshot-cadence scheduler in the coordinator (fixed 720s
  default with ±60s jitter, ≤900s ceiling, plus the mandatory final snapshot from T009), in
  `tests/stress/lane_sim016.py` — per research.md D1/D3 and FR-005 (depends on T037)
- [ ] T054 [US4] Implement integrity-sampler case selection per tick: `min(3, n_completed)`
  cases = the most-recently-completed case + one case already verified by a prior snapshot
  (drift detection) + one seeded-random other; only `COMPLETED` cases are eligible, in-flight
  cases are never sampled, in `tests/stress/lane_sim016.py` — per research.md D3 and
  data-model.md §4 (satisfies spec.md US4 Acceptance Scenario 2) (depends on T012, T043,
  T053)
- [ ] T055 [US4] Implement per-sampled-case checks: sha256 pixel-hash comparison of
  re-downloaded files against the generation-time `factory.surgery_pixel_hashes` baseline
  (`case.gen_hashes`) — the file re-download is NEW lane code (REST
  `/data/experiments/{case.experiment_ref}/scans/.../resources/SRC/files` pull via the
  reader's `requests.Session`; `driver.py` exposes no download function and stays
  unmodified); annotation link resolution via the `lane_annotations.py`
  `step_download_and_compare` pattern for cases with `has_annotations=true`; presence via
  `driver.server_inventory` + `driver.empty_shells` (a completed case surfacing as an empty
  shell = integrity failure; a `COMPLETED` case with no `experiment_ref` = snapshot
  failure), in `tests/stress/lane_sim016.py` — per research.md D3 and data-model.md §2
  `experiment_ref` (depends on T054, T071)
- [ ] T056 [US4] Implement the FR-006 resource-observation piggyback on every snapshot:
  recursive byte count of the run scratch root (`scratch_bytes`, raw), the leak signal
  (`scratch_leak_bytes` = raw minus in-flight/non-terminal cases' scratch dirs),
  open-connection count (writer-pool size + reader session + coordinator + the sampler's
  own verification connection, cross-checked via `/proc`-based socket counting where
  available), and `scratch_monotonic_run_len` tracking (consecutive snapshots with strictly
  increasing `scratch_leak_bytes` — the leak signal, NOT raw scratch, so concurrent
  in-flight uploads never false-trip the rule), in `tests/stress/lane_sim016.py` —
  per FR-006 and data-model.md §4 (depends on T053)
- [ ] T057 [US4] Wire each snapshot's `pass`/`resources`/`resource_ok` results into the
  T012 ledger and T013 verdict computation (zero-tolerance integrity FAIL on any
  `pass=false`; resource FAIL on raw `scratch_bytes ≥ 1 GB` OR `scratch_monotonic_run_len ≥ 3`
  over the leak signal OR `open_connections > max_connections`), across
  `tests/stress/lane_sim016.py` and `tests/stress/sim016_report.py` — per
  contracts/run-report.md §2 rules 2-3 (depends on T013, T055, T056)

### Validation for User Story 4 (opt-in live-server lane)

- [ ] T071 [US4] Hash-stability gating experiment (run BEFORE trusting the zero-tolerance
  integrity check): publish exactly one `factory.make_surgery` case via
  `driver.publish_surgery` with the stress `auto_confirmer` (ReviewDecision.CONFIRMED — no
  redaction), re-download its DICOM files from the server via the T055 REST pull, and
  assert `sha256(ds.PixelData)` of every re-downloaded file equals the generation-time
  `factory.surgery_pixel_hashes` baseline. If this fails, the integrity baseline MUST move
  to post-publish hashes and research.md D3 be amended before any sustained-mode claim —
  per research.md D3 hash-stability precondition (opt-in, live; blocks T055's checksum
  check from being trusted)

- [ ] T058 [US4] [P] Run a sustained-mode run of sufficient length and confirm the report's
  `snapshots[]` contains ≥3 distinct timestamped entries spread across the run's duration —
  proves SC-005 and spec.md US4 Acceptance Scenario 1 (opt-in, live)
- [ ] T059 [US4] [P] Trigger a snapshot tick while an upload is still `UPLOADING` and
  confirm the in-progress case is never included in `sampled[]` and never flagged — proves
  spec.md US4 Acceptance Scenario 2 (opt-in, live)
- [ ] T060 [US4] [P] Inspect a completed run's `verdict.integrity_check` and confirm it
  reflects whether *any* snapshot at *any* point in the run failed, not only the last one
  (e.g. by injecting a mid-run corruption in a test double of the sampler and confirming the
  final verdict still reports FAIL even though the last snapshot alone would pass) — proves
  spec.md US4 Acceptance Scenario 3 and the SC-005 "clearly states" requirement (opt-in,
  live)
- [ ] T061 [US4] Run a multi-hour design-target duration (or an accelerated
  `--duration-s`/cadence override standing in for it) and confirm scratch stays < 1 GB at
  every snapshot and never grows monotonically across 3+ consecutive snapshots, and open
  connections never exceed `--max-connections` at any snapshot — proves SC-007 (opt-in,
  live, long-running)

**Checkpoint**: All four user stories are independently functional. Periodic integrity and
resource verification are wired into the trustworthy unattended verdict — the property that
makes the whole feature's PASS claim meaningful.

---

## Phase 7: Polish & Cross-Cutting Concerns

**Purpose**: The one true cross-cutting mechanism (the server-outage circuit breaker, which
touches both the writer pool and the coordinator dispatcher and doesn't belong to a single
user story), plus repo-convention hygiene and final acceptance gates across all 8 success
criteria.

- [ ] T062 Implement the server-outage circuit breaker: 3 consecutive transient-class
  failures across distinct events pauses the dispatcher (arrivals are held, not dropped —
  their displacement is recorded), probes `GET /data/version` every 15s for up to 5 minutes;
  on recovery, resumes and logs a `server_outage` lifecycle event (recoverable, per FR-011);
  on expiry, finalizes a partial report with `verdict.overall="FAIL"`, reason
  `server_unreachable`, in `tests/stress/lane_sim016.py` — per research.md D7 and FR-011
  (validates spec.md Edge Case 2 and US1 Acceptance Scenario 3) (depends on T040, T053)
- [ ] T063 [P] Cross-check: confirm no code path in `tests/stress/lane_sim016.py`,
  `tests/stress/sim016_schedule.py`, or `tests/stress/sim016_report.py` imports or
  references the offline `tests/fakes/` FakeXNAT double as a substitute for the live server
  — enforces FR-009 (read-only audit, no target file)
- [ ] T064 [P] Cross-check: confirm `src/annotations/aggregate/staple.py` is never modified
  and no new STAPLE consensus math is added anywhere in the SIM016 code path — enforces
  FR-012 (read-only audit, no target file)
- [ ] T065 [P] Cross-check: confirm `--password`/`XNAT_PASSWORD` is never written to the
  report JSON, the JSONL log, or `--verbose` console output at any point — enforces
  data-model.md §1's "password never serialized" rule and Constitution V (read-only audit
  of `tests/stress/lane_sim016.py` and `tests/stress/sim016_report.py`)
- [ ] T066 [P] Add the SIM016 lane to `docs/STRESS_TEST_PLAN.md`'s lane inventory (alongside
  volume/concurrent/malformed/dedup/annotations) with a one-line description, its opt-in
  invocation, and a reserved results-log row format (`<date> SIM016 <result>
  <evidence-path>`) matching the file's existing append-only "Results log" convention — per
  repo doc-cross-reference convention
- [ ] T067 Run `quickstart.md`'s precondition-check and smoke-run steps verbatim, exactly as
  written, and confirm every command is copy-pasteable with zero modification needed —
  validates that `quickstart.md` stays faithful to the shipped CLI (contracts/cli.md
  conformance) once T024-T032 exist
- [ ] T068 Full end-to-end acceptance: execute the complete US1 Independent Test (fresh
  agent session, `docs/SIM016_PROCEDURE.md` as the *only* context, booted container, smoke
  mode, unassisted PASS/FAIL read) as the final FR-007/SC-001 gate before declaring the
  feature implementation done (opt-in, live; supersedes/confirms T035 under full end-to-end
  conditions)
- [ ] T069 [P] Full SC-008 acceptance: run a sustained-mode run and confirm every recorded
  unexpected-terminal failure is attributed to exactly one tri-state class in
  `failures.tri_state`, with `FRIENDLY`/`ACCEPTED`/`CRASH` counts never double-counted
  against or conflated with `failures.expected_friendly` — proves SC-008 end-to-end against
  the live server (opt-in, live)
- [ ] T070 [P] Full resource-and-integrity acceptance sweep: re-run T058-T061 and T069
  together against a single long sustained run and attach the resulting `sim016_<run_id>.json`
  + `.events.jsonl` as the feature's closing evidence artifact — final consolidated
  SC-005/SC-007/SC-008 gate (opt-in, live, long-running)

**Checkpoint**: Every FR-001..013 and every SC-001..008 has at least one implementation
task and at least one validation task tracing to it. Feature 016 implementation is
complete and demonstrated against the real Docker-local XNAT.

---

## Dependencies & Execution Order

### Phase dependencies

- **Setup (Phase 1)**: no dependencies — start immediately once implementation is
  authorized.
- **Foundational (Phase 2)**: depends on Setup (T002-T004 create the files T006+ edit).
  **Blocks all user stories** — every clarified numeric threshold lives here.
- **User Story 1 (Phase 3)**: depends on Foundational (needs `sim016_report.py`'s verdict/
  exit-code plumbing and `sim016_schedule.py`'s smoke timeline). Practically, Phase 3 must
  land **before** Phases 4-6 begin, because it creates the `lane_sim016.py` coordinator
  skeleton (CLI parser, env wiring, signal handling, project bootstrap) that later phases
  extend in the same file — even though phases are ordered by user-story priority per
  spec-kit convention, not by a flat "any story after Foundational" rule. This mirrors
  plan.md's Implementation Strategy sequencing 1→2→3→{4,5}.
- **User Story 2 (Phase 4)**: depends on Foundational + Phase 3 (adds scheduling/writer-pool
  execution into the coordinator Phase 3 built). US1 + US2 together = MVP.
  Independently testable per its own Independent Test once its tasks land.
- **User Story 3 (Phase 5)**: depends on Foundational + Phase 3; additive to Phase 4's
  writer-pool dispatch (adds the reader thread alongside it) but does not require Phase 4's
  tasks to be individually complete — could be built in parallel with Phase 4 on a separate
  branch and merged, since it touches a mostly-disjoint section of `lane_sim016.py` (reader
  thread vs. writer-pool dispatch).
- **User Story 4 (Phase 6)**: depends on Foundational + Phase 3 + Phase 4 (T054 needs
  `COMPLETED` cases to exist, which only Phase 4's writer pool produces). Not dependent on
  Phase 5.
- **Polish (Phase 7)**: depends on all four user-story phases being complete (T062 needs
  the writer pool AND the snapshot scheduler; T068-T070 are final gates over the whole
  feature).

### Parallel opportunities

- Within Setup: T002, T003, T004 (three different files) in parallel; T001 and T005 are
  read-only checks, parallel with everything.
- Within Foundational: the `sim016_schedule.py` track (T006-T010) and the
  `sim016_report.py` track (T011-T015) touch different files and have no data dependency on
  each other — a two-engineer team could take one track each. Their respective offline-test
  tracks (T016-T019 vs. T020-T023) split the same way.
- Within User Story 1: T033 (the procedure doc, a different file) can be drafted in
  parallel with T024-T032 (all in `lane_sim016.py`, sequential) and finalized once they
  land. T034-T036 (live validation) are parallel with each other.
- User Story 3 (Phase 5) and User Story 4 (Phase 6) touch largely disjoint sections of
  `lane_sim016.py` (reader thread vs. snapshot sampler) and could be developed on separate
  branches in parallel after Phase 4, merging with normal git conflict resolution on the
  shared coordinator file.
- All `[P]`-tagged validation tasks within a phase (different live-server runs, no shared
  file writes) can be executed as independent runs.

---

## Parallel Execution Examples

```bash
# Setup — three new files, no dependencies between them:
Task: "Create tests/stress/sim016_schedule.py module skeleton"
Task: "Create tests/stress/sim016_report.py module skeleton"
Task: "Create tests/stress/lane_sim016.py module skeleton"

# Foundational — schedule.py track and report.py track, two different files:
Task: "Implement inter-arrival draw function in tests/stress/sim016_schedule.py"
Task: "Implement tri-state failure classification helper in tests/stress/sim016_report.py"

# Foundational — offline test tracks, two different test files:
Task: "Unit tests for inter-arrival draws in tests/stress/test_sim016_schedule.py"
Task: "Unit tests for tri-state classification in tests/stress/test_sim016_report.py"

# User Story 1 — live validation runs, independent executions:
Task: "Run smoke mode and confirm SC-001 timing/exit-code/trailer contract"
Task: "Hand docs/SIM016_PROCEDURE.md alone to a fresh agent session (US1 Independent Test)"
Task: "kill -9 mid-smoke-run and confirm report/JSONL remain valid (SC-006 worst case)"
```

---

## MVP Scope

**User Story 1 (Phase 3) + User Story 2 (Phase 4) = the minimum meaningful sustained run.**
US1 alone proves the procedure is executable and produces a verdict; US2 alone would have
no coordinator to run inside. Together they deliver: a context-free agent can invoke either
mode, watch genuinely uneven case arrivals with realistic variety accumulate, and read a
trustworthy PASS/FAIL verdict — satisfying spec.md's own framing ("the entire point of the
feature... the simulation exists to be handed off, not run in this session" for US1, and
"without [uneven arrivals] this spec would just be a slower duplicate of an existing lane"
for US2). US3 (concurrent reads) and US4 (periodic integrity sampling) are additive realism
and trust-verification layers on top of that MVP — valuable, and required by FR-004/FR-005
for the feature to be considered complete, but a US1+US2-only build is already a
independently-useful, independently-testable increment per the Independent Test each story
defines.

---

## Success-Criteria Traceability

Every SC has both a pure-logic (offline, Foundational-phase) proof where the criterion is
expressible without a server, and a live-server validation task proving it against the real
Docker-local XNAT.

| SC | Pure-logic proof (offline) | Live-server proof (opt-in) |
|---|---|---|
| SC-001 (smoke ≤5 min, unassisted) | — (inherently a live-run timing claim) | T034, T068 |
| SC-002 (variable inter-arrival gaps) | T016 | T045 |
| SC-003 (≥3 case shapes in proportion) | T017, T018 | T046 |
| SC-004 (read strictly inside write window) | — (inherently a live-run timing claim) | T051 |
| SC-005 (≥3 integrity snapshots, verdict states any-failure) | T018 | T058, T060 |
| SC-006 (readable partial report after interruption) | T022 | T036 |
| SC-007 (resource bounds hold across a long run) | — (inherently a live-run resource claim) | T061, T070 |
| SC-008 (unexpected-failure ratio + never-conflated tri-state) | T021 | T069, T070 |

---

## Notes

- `[P]` tasks touch a different file (or are read-only audits) from every other
  not-yet-complete task in the same phase — safe to parallelize.
- `[US#]` labels map every implementation and validation task back to its user story for
  traceability; Setup/Foundational/Polish tasks carry no story tag because they are shared
  infrastructure spanning all four stories.
- Live-server validation tasks are explicitly **opt-in** (they require a booted
  `tests/integration/xnat_local/docker-compose.yml` instance) and are never part of the
  default offline CI gate — mirroring the existing `RUN_XNAT_DUAL=1` / `tests/stress/lane_*.py`
  convention this repo already uses for spec-014.
- Every task cites the contract section, research decision (D1-D7), functional requirement,
  or success criterion it implements — use these citations to re-derive intent without
  reloading this planning conversation.
- This file describes work for a FUTURE, separately-authorized implementation pass. Its
  existence does not constitute that authorization.
