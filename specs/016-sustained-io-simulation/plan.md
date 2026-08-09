# Implementation Plan: Sustained Real-World I/O Simulation Against Local XNAT

**Branch**: `016-sustained-io-simulation` | **Date**: 2026-07-09 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `specs/016-sustained-io-simulation/spec.md`

> **Scope guard (FR-013)**: This feature is design-and-planning only. This plan and its
> Phase 0/1 artifacts describe what a FUTURE, separately-authorized implementation pass
> will build. No simulation code, no lane script, and no procedure document are written
> by this feature — only the planning artifacts in `specs/016-sustained-io-simulation/`.

## Summary

Design a sustained, time-distributed I/O simulation that a context-free future agent can
run against the real Docker-local XNAT instance (`tests/integration/xnat_local/docker-compose.yml`,
`http://localhost:8080`, admin/admin) after a human boots the container. Unlike the existing
burst-style stress lanes (`lane_volume.py`, `lane_concurrent.py`), the simulation generates an
open-ended stream of synthetic surgical cases arriving at uneven, seeded-random intervals over
a configurable duration (smoke ~90 s; sustained default 60 min), interleaves concurrent read
traffic with in-flight writes, samples data integrity periodically mid-run (≥4 snapshots per
default run), and emits a machine-readable JSON run report with a tri-part PASS/FAIL verdict
(unexpected-terminal-failure ratio ≤ max(1, ceil(0.02 × case-write events)), a zero-completed-writes run can never PASS; zero tolerance for integrity
failures; resource bounds: scratch <1 GB, no 3-snapshot monotonic growth, ≤10 open connections).
The design maximally reuses existing infrastructure: `tests/stress/driver.py` (connect /
publish_surgery / server_inventory / empty_shells), `tests/stress/factory.py` (make_surgery,
overlap_cases, synth_fluoro_frame), `tests/synthetic_data.py` (PHI DICOM, multiframe, mp4,
intake, xlsx generators), the `lane_annotations.py` AnnotationSet round-trip pattern, and the
`lane_concurrent.py` multiprocessing worker pattern. STAPLE remains a stub (`src/annotations/
aggregate/staple.py`); consolidation-adjacent activity is scoped to the existing reference
aggregator seam only.

## Technical Context

**Language/Version**: Python ≥3.9 (repo floor; unified per spec-019 T067)
**Primary Dependencies**: pydicom, numpy, pandas, requests, pyxnat — all already pinned in
`requirements.txt`. Scheduling, concurrency, signal handling, and report writing use stdlib
only (`random`, `threading`, `multiprocessing`, `signal`, `json`, `argparse`, `tempfile`,
`os.replace`). **No new heavy dependencies**; any future proposal to add one requires its own
justification in tasks.md.
**Storage**: Live Docker-local XNAT (PostgreSQL-backed container, `localhost:8080`); local
scratch under a run-scoped temp directory (per-case cleanup, finally-style); JSON report +
append-only JSONL event log under `tests/stress/results/` (existing lane convention).
**Testing**: Standalone lane script under `tests/stress/` (pytest-adjacent, `python -m`
entry point, excluded from the default offline CI gate — same convention as
`lane_volume.py` / `lane_concurrent.py` / `lane_malformed.py` / `lane_dedup.py` /
`lane_annotations.py`). Pure logic (schedule generation, variety assignment, verdict
computation, report serialization) must be unit-testable offline in the future
implementation.
**Target Platform**: Single local dev machine running the docker-compose XNAT container;
Linux/macOS shells.
**Project Type**: Single project — one new lane module family under `tests/stress/`, one
procedure document under `docs/`, planning artifacts under `specs/016-sustained-io-simulation/`.
**Performance Goals**: Not a throughput test. Realism targets from the clarify session:
mean case inter-arrival ≤ ~110 s (≥30 cases in a default 60-min run, SC-003 non-vacuous);
integrity-snapshot cadence ≤ ~15 min (≥4 snapshots per default run, SC-005 with margin);
smoke mode completes with verdict within 5 min wall clock (SC-001).
**Constraints**: Simulation-attributable scratch <1 GB at every snapshot and never
monotonically growing across 3+ consecutive snapshots; open connections ≤ configured session
pool (default ≤10) at every snapshot; self-terminating at configured duration; SIGINT/SIGTERM
produce a readable partial report (SC-006); fresh run-id-scoped project `SIM016_<ts>_<hex>`
per invocation, never a fixed reused name; never writes to, asserts about, or deletes anything
outside its own run project; never references FakeXNAT as a substitute for the live server
(FR-009).
**Scale/Scope**: Runs from ~90 s (smoke) to multiple hours (sustained); one coordinator
process, a bounded writer pool (default 3 processes), one reader thread, one snapshot
scheduler; expected 30–200 cases per sustained run.

**NEEDS CLARIFICATION remaining: none.** The five open defaults (duration, event rate,
variety proportions, resource bound, project naming) were resolved in the 2026-07-09 clarify
session recorded in spec.md and are treated as binding design defaults throughout.

## Constitution Check

*GATE: evaluated against `.specify/memory/constitution.md` v1.1.0 before Phase 0; re-checked after Phase 1.*

| Principle | Gate question | Status |
|---|---|---|
| I — PHI Safety | Touches image data / moves data off-machine? | ✅ PASS — all case data is synthetic (`tests/synthetic_data.py` fake-PHI datasets scrubbed by the existing de-id path inside `publish_surgery`, identical to current lanes); target is localhost only; the design REQUIRES a localhost/explicit-disposable-host guard in the future lane (production RPACS hostname is a hard startup error) and per-case scratch cleanup. No real PHI exists anywhere in the design. |
| II — Fail Softly | New failure modes messaged with a next step? | ✅ PASS — the simulation is agent/developer tooling, but the same discipline applies: every failure class is caught, classified (transient → retry; FRIENDLY; CRASH), logged with detail, and reflected in the report; the run never dies with a raw traceback for a foreseeable condition (server hiccup, malformed injection, early stop). The procedure doc contract mandates a "how to interpret / what to do next" section for every verdict outcome. |
| III — Skill Floor | Does a first-time student need terminal/Git? | ✅ PASS — no student-facing surface changes. This is an operator/agent-run test lane; the CLI is acceptable because its users are a future agent and the maintainer, not student researchers. |
| IV — Testable Offline | CI with no network/PHI? Where is the fake? | ✅ PASS (with scoping note) — the design splits the future implementation into (a) pure logic — schedule generation, variety floor top-up, retry/verdict computation, report serialization — which MUST sit behind seams unit-testable offline in the default CI gate, and (b) the live-server lane execution, which is opt-in and excluded from the default gate, exactly like the five existing `tests/stress/lane_*.py` lanes. FR-009 explicitly forbids FakeXNAT as a stand-in for the live run — that is the feature's point, and it matches the established, constitution-accepted stress-lane pattern (see 014 plan's stress/dual-run lane contract). No FakeXNAT reference appears anywhere in the design artifacts. |
| V — Config over Hardcoding | Hardcoded endpoints/creds? | ✅ PASS — URL/user/password/project are CLI args + env overrides with throwaway localhost defaults (existing lane convention); credentials are never logged and never accepted where they would enter shell history beyond the documented throwaway admin/admin local pair; production endpoints never appear as defaults. |
| VI — Data Integrity at Scale | Shared state / destructive / long-running ops? | ✅ PASS — this feature exists to verify exactly this principle over time: uploads are verified by mid-run checksum + link-resolution snapshots; config (`database_config.json`) concurrency uses the existing pull/push + lost-update-guard path already exercised by `lane_concurrent.py`; the simulation performs no destructive operation outside its own run-scoped project and deletes nothing outside it. |
| VII — Fix Unverified Until Proven | Failing→passing test + green CI for each fix? | ✅ PASS — no fix is claimed by this feature (design-only). The design obligates the future implementation pass to ship offline unit tests for the pure-logic seams and to validate the lane end-to-end via smoke mode before any sustained-mode claim; any defect the simulation later finds in `src/` falls under Principle VII in its own right. |

**Post-Phase-1 re-check (after research.md / data-model.md / contracts/ / quickstart.md)**:
PASS — no design artifact introduced a hardcoded production endpoint, a PHI path, a FakeXNAT
dependency, or an unverifiable claim. No Complexity Tracking entries required.

## Project Structure

### Documentation (this feature — the ONLY files this feature produces)

```text
specs/016-sustained-io-simulation/
├── spec.md              # Feature specification (input, already exists)
├── plan.md              # This file
├── research.md          # Phase 0: 7 design decisions (Decision/Rationale/Alternatives)
├── data-model.md        # Phase 1: entities, fields, relationships, validation, state machine
├── quickstart.md        # Phase 1: future context-less agent's runbook skeleton
├── contracts/
│   ├── run-report.md    # Machine-readable report JSON schema + verdict computation rules
│   ├── cli.md           # Future lane script CLI contract: args, env, exit codes
│   └── procedure-doc.md # Required sections of the future docs/SIM016_PROCEDURE.md
└── checklists/
    └── requirements.md  # (already exists)
```

### Source Code (repository root) — FUTURE implementation targets, described not created

```text
tests/stress/
├── lane_sim016.py           # [FUTURE] coordinator: CLI, scheduler, dispatcher, signals, report
├── sim016_schedule.py       # [FUTURE] pure logic: inter-arrival draws, variety assignment,
│                            #          floor top-up, timeline pre-generation (offline-testable)
├── sim016_report.py         # [FUTURE] pure logic: ledger, verdict computation, atomic JSON
│                            #          report writes, JSONL event log (offline-testable)
├── driver.py                # REUSED as-is: connect / publish_surgery / server_inventory / empty_shells
├── factory.py               # REUSED as-is: make_surgery / overlap_cases / synth_fluoro_frame /
│                            #          surgery_pixel_hashes
├── malformed.py             # REUSED as-is: malformed-case generators for the 10% injection class
├── lane_annotations.py      # PATTERN reused: AnnotationSet build/upload/download round-trip
├── lane_concurrent.py       # PATTERN reused: multiprocessing writer-worker shape
└── results/                 # REUSED convention: sim016_<run_id>.json + sim016_<run_id>.events.jsonl

tests/
├── synthetic_data.py        # REUSED as-is: PHI DICOM, multiframe, mp4, intake, xlsx generators
└── unit (future)            # [FUTURE] offline tests for sim016_schedule / sim016_report seams

docs/
└── SIM016_PROCEDURE.md      # [FUTURE] self-contained procedure doc per contracts/procedure-doc.md

tests/integration/xnat_local/docker-compose.yml   # REUSED as-is: human boots container (out of scope)
src/annotations/aggregate/staple.py               # UNTOUCHED stub: seam only, no STAPLE math (FR-012)
```

**Structure Decision**: Single-project layout, mirroring the existing stress-lane family.
The future implementation adds exactly three code files (one lane entry, two offline-testable
logic modules) plus one procedure doc; everything else is reuse. Splitting scheduler/report
logic out of the lane entry is what satisfies the Principle IV offline-testability gate.

## Architecture Sketch (design the future pass implements)

### 1. Event-scheduler model — uneven arrivals from a seeded RNG

The full event timeline is **pre-generated at run start** from a single seeded
`random.Random(seed)` (seed defaults to a time-derived value, always echoed in the report for
reproducibility). Case arrivals are drawn as a homogeneous Poisson process:
`gap = rng.expovariate(1.0 / mean_interarrival_s)`, clamped to `[2 s, 6 × mean]`, accumulated
until the configured duration is exhausted. Exponential draws guarantee variable gaps (SC-002)
and couple to the single ≤~110 s mean default. Read events are drawn from an independent
Poisson stream (default mean 45 s) so reads land between write start/completion by
construction (SC-004). Config-update events are a sparse third stream (default mean 15 min).
Integrity snapshots are fixed-cadence (default 12 min ≤ the ~15-min ceiling) with ±60 s
jitter, plus one mandatory final snapshot at run end. Case variety is assigned per arrival by
weighted draw (80/10/10 normal/malformed/dedup-probe), then a deterministic **floor top-up
pass** re-labels the latest normal arrivals if a sustained-mode timeline holds fewer than 3
malformed or 3 dedup probes (FR-001 floors). Smoke mode compresses the same machinery
(duration 90 s, mean inter-arrival 20 s, one snapshot at end, floors waived) and guarantees at
least one full write-then-read-then-verify cycle. Full rationale and rejected alternatives:
`research.md` D1; timeline entity: `data-model.md`.

### 2. Worker / concurrency model — reader thread interleaved with writer pool

- **Coordinator (main process)** owns the pre-generated timeline, sleeps until the next event
  is due, and dispatches it. It also owns the ledger (thread-safe, lock-guarded), the signal
  handlers, and report finalization.
- **Writer pool**: a bounded `multiprocessing.Pool` (default 3 processes; hard ceiling keeps
  total connections ≤10). Each case write runs `factory.make_surgery` (or a
  `tests/stress/malformed.py` generator, or an `overlap_cases`-derived dedup probe) into a
  per-case scratch dir, then `driver.connect` + `driver.publish_surgery`, then—for a random
  ~30% of normal cases—the `lane_annotations.py` AnnotationSet upload pattern; a
  `finally`-style cleanup removes the case scratch dir. Processes, not threads, because
  pyxnat connections are not shareable and `src` config is env-var/global-state based —
  exactly the proven `lane_concurrent.py` shape (research.md D2).
- **Reader thread** (in the coordinator): read events are pure REST GETs via a single
  `requests.Session` — `driver.server_inventory`-style browse/query calls and periodic
  re-downloads of a completed case's files — so a thread suffices and connection accounting
  stays simple. Reads tolerate in-flight partial state gracefully (US3 scenario 2): a
  concurrent-write 404/409 on a not-yet-finalized resource is logged as a benign read
  observation, never as corruption.
- **Snapshot scheduler** runs in the coordinator alongside the reader thread.

### 3. Integrity-snapshot sampler

At each cadence tick the sampler selects only **completed** cases (write ok, config push
done): the most recently completed case + up to 2 seeded-random earlier cases, one of which
must have been verified in a prior snapshot (drift detection across time). For each sampled
case it re-downloads the case's DICOM files (NEW lane code — a REST files pull keyed on the
case's `experiment_ref`, which the worker captures post-publish via a deterministic
uid-keyed lookup, never a list-position walk; see research.md D3 and data-model.md §2) and
compares sha256 pixel hashes against the `factory.surgery_pixel_hashes` values recorded at
generation time, resolves annotation links via the `download_annotation_set` path for cases
that carried annotations, and checks presence via `driver.server_inventory` +
`driver.empty_shells` (a completed case appearing as an empty shell = integrity failure). Each snapshot also records the FR-006 resource
observations: simulation-attributable scratch bytes (recursive size of the run scratch root)
and open-connection count (writer-pool size + reader session + coordinator connections,
cross-checked against `/proc`-based socket counting where available). Any snapshot check
failure sets the zero-tolerance integrity FAIL. Sampling rationale: research.md D3.

### 4. Run-report writer

Two files under `tests/stress/results/` (existing convention), both keyed by run id:
`sim016_<run_id>.events.jsonl` — append-only, one line per completed event, written as events
finish (crash-safe evidence, SC-006) — and `sim016_<run_id>.json` — the report, atomically
rewritten (`tempfile` + `os.replace`) after every snapshot and at finalization, so a kill at
any instant leaves the previous valid report on disk. The report carries the tri-part verdict
(ratio check, zero-tolerance integrity check, resource check → overall PASS/FAIL with
reasons), full config echo, the pre-run server-project inventory preamble (evidence the server
was not pristine; never asserted against), counts by event type and tri-state error class,
and the snapshot history. Schema is the binding contract in `contracts/run-report.md`
(research.md D4).

### 5. Procedure document for the context-less future agent

The future pass writes `docs/SIM016_PROCEDURE.md` (the `docs/` directory is the established
home for runbooks, e.g. `INTEGRATION_TEST_RUNBOOK.md`). Its mandatory section list, ordering,
and content requirements are fixed by `contracts/procedure-doc.md`; `quickstart.md` in this
feature is the skeleton the document must expand. The document must be executable cold:
precondition probes, smoke invocation, sustained invocation, verdict interpretation for every
outcome, early-stop procedure, and cleanup — with zero reliance on conversational context.

### 6. Config surface (CLI args + clarified defaults)

Single entry point `python -m tests.stress.lane_sim016` with arguments fixed by
`contracts/cli.md`. Defaults encode the clarify-session answers: `--mode sustained`
`--duration-s 3600`, `--mean-interarrival-s 110`, `--variety 0.80,0.10,0.10` with floors
`--min-malformed 3 --min-dedup 3`, `--snapshot-cadence-s 720`, `--max-connections 10`,
`--project SIM016_<utc-ts>_<hex4>` (auto-generated, never reused), plus `--url/--user/
--password` defaulting to the throwaway localhost admin/admin pair with env overrides.
Exit codes: 0 PASS, 1 FAIL, 2 precondition error, 3 early-stopped partial.

## Implementation Strategy (for the FUTURE pass — sequencing only, no code now)

1. **Offline logic first**: `sim016_schedule.py` (draws, variety, floors, timeline) and
   `sim016_report.py` (ledger, verdict math, atomic writes) with unit tests in the default
   CI gate — these encode every clarified threshold and are fully testable without a server.
2. **Lane coordinator**: `lane_sim016.py` wiring scheduler → writer pool → reader thread →
   snapshot sampler → report writer; signal handling; localhost guard.
3. **Smoke-mode validation** against a booted container (SC-001) before any sustained run.
4. **Procedure doc** `docs/SIM016_PROCEDURE.md` written to `contracts/procedure-doc.md`,
   then cold-tested by a fresh agent session per US1's independent test.
5. **Sustained-mode validation** (30–60 min) checking SC-002…SC-008.

Dependencies: 1 → 2 → 3 → {4, 5}. Main risks and their mitigations are captured as research
decisions D6 (early stop), D7 (retry/outage), and the connection-budget ceiling in D2.

## Complexity Tracking

No constitution violations — table omitted.
