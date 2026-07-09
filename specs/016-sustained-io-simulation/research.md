# Phase 0 Research: Sustained Real-World I/O Simulation (016)

**Date**: 2026-07-09 | **Plan**: [plan.md](plan.md) | **Spec**: [spec.md](spec.md)

All decisions below are design decisions for the FUTURE implementation pass (FR-013). Each
resolves a question the plan's architecture depends on. No `NEEDS CLARIFICATION` items remain.

---

## D1 — Inter-arrival distribution for simulated events

**Decision**: Homogeneous Poisson process per event stream: inter-arrival gaps drawn as
`random.Random(seed).expovariate(1.0 / mean_s)`, clamped to `[2 s, 6 × mean_s]`, accumulated
until the configured duration is filled. Three independent streams: case writes (default mean
110 s), reads (default mean 45 s), config updates (default mean 900 s). Integrity snapshots
are fixed-cadence (default 720 s) with ±60 s uniform jitter plus a mandatory final snapshot.
The whole timeline is pre-generated at run start from one seed and echoed into the report.

**Rationale**: Independent case arrivals at an archive are the textbook use of a Poisson
process; exponential gaps are memoryless and intrinsically uneven, satisfying SC-002 ("not a
uniform fixed interval and not all bunched at the start") by construction rather than by
tuning. A single parameter (the mean) couples directly to the clarified default (mean ≤ ~110 s
⇒ expected ≥30 cases per 60-min run, keeping SC-003 non-vacuous). `expovariate` is stdlib —
no new dependency. Pre-generating the timeline makes the schedule deterministic given the
seed (reproducible bug reports) and lets the floor top-up pass (≥3 malformed, ≥3 dedup) be
applied before execution — subject to the dedup-probe ordering guarantee (data-model.md §2):
a probe's `scheduled_at` must land strictly after its base case's scheduled time plus an
expected-completion margin, top-up may only create a probe where an eligible earlier base
exists, and the coordinator additionally gates probe dispatch on the base reaching
`COMPLETED` (re-designating the probe as normal if the base terminates without completing).
The clamp floor (2 s) prevents pathological same-instant bursts
that would turn the lane back into `lane_concurrent.py`; the clamp ceiling prevents a single
freak draw from silently emptying a smoke run.

**Alternatives considered**:
- *Lognormal inter-arrivals*: heavier tails and arguably more "human", but two parameters to
  default and document, and the mean no longer maps 1:1 to the clarified 110 s target.
  Rejected as unneeded complexity for no acceptance-criteria gain.
- *Fixed interval with uniform jitter*: near-constant gaps risk failing SC-002's "not a
  uniform fixed interval" on inspection. Rejected.
- *Non-homogeneous Poisson (shift-shaped rate curve)*: more realism than any FR/SC requires;
  deferred as a possible future enhancement, noted here so it is a conscious omission.
- *On-line draws during the run (no pre-generation)*: simpler memory profile but loses
  determinism-given-seed and makes the variety floor top-up a mid-run mutation. Rejected.

---

## D2 — Thread vs. process split for readers and writers

**Decision**: Writers are **processes** (bounded `multiprocessing.Pool`, default 3, ceiling
derived from `--max-connections 10`); readers are **one thread** in the coordinator using a
single `requests.Session`; the snapshot sampler shares the coordinator with the reader.

**Rationale**: The write path (`driver.connect` → `publish_surgery`) pulls in pyxnat and
`src` modules whose configuration is process-global (env vars read at import time, module
state); `lane_concurrent.py` already proved the each-worker-owns-its-connection
multiprocessing shape against this exact stack, and pyxnat interfaces are not shareable
across processes anyway. The read path needs none of that: browse/query/download events are
plain REST GETs (the `server_inventory` pattern), which are network-bound and thread-safe on
a dedicated `requests.Session` — a thread keeps the ledger in-process (simple lock, no IPC)
and keeps the connection budget observable. The 3-writer default keeps worst-case
simultaneous connections (3 writer connects + 1 reader session + 1 coordinator/bootstrap)
comfortably under the clarified ≤10 pool bound while still allowing genuine write overlap.

**Alternatives considered**:
- *All threads*: `src` env-var/global config and pyxnat session state are not demonstrated
  thread-safe; corrupting the system under test's client would invalidate every verdict.
  Rejected.
- *All processes (readers too)*: forces the ledger and event log through IPC or a manager
  process, multiplies connections, and buys nothing for GET traffic. Rejected.
- *asyncio*: the entire `src` stack is synchronous; wrapping it in executors reproduces the
  thread/process question with extra machinery. Rejected.

---

## D3 — Integrity-snapshot sampling strategy

**Decision**: Per snapshot, sample `min(3, n_completed)` cases: (a) the most recently
completed case, (b) one case already verified by a previous snapshot (re-verification /
drift detection), (c) one seeded-random other completed case. Only cases in state
`COMPLETED` (write ok **and** post-write config push finished) are eligible; in-flight cases
are never sampled (US4 scenario 2). Checks per sampled case: sha256 pixel-hash comparison of
re-downloaded files vs. generation-time `surgery_pixel_hashes`, annotation link resolution
via the `download_annotation_set` path where annotations were uploaded, and presence via
`server_inventory` + `empty_shells` (a completed case surfacing as an empty shell fails the
snapshot). **The file re-download is NEW lane code** (a REST
`/data/experiments/{experiment_ref}/scans/.../resources/SRC/files` pull via the reader's
`requests.Session`) — `driver.py` exposes no download function and is reused unmodified.
The sampler locates cases via the `experiment_ref` on the case record, captured by the
worker post-publish through a deterministic uid-keyed lookup (label match on
`GET /data/experiments?project={run_project}`), never through list-position heuristics —
the `lane_annotations.py` "last experiment" walk is safe only for that lane's single-case
publishes and is forbidden here because the concurrent writer pool makes list position
racy (a cross-wired ref would produce false integrity FAILs). Ref-capture determinism is
itself gated by the T072 experiment (one publish, then two concurrent publishes, assert
each case's lookup resolves to its own accession) before the integrity verdict is trusted;
if uid-keyed lookup proves unavailable on the deployed XNAT build, this decision must be
amended with a proven alternative before implementation proceeds. **Hash-stability precondition**: the design assumes the
publish path does not mutate `PixelData` when the pixel-review confirmer returns CONFIRMED
(no redaction applied, matching the existing stress-lane `auto_confirmer`); the
implementation pass MUST validate this with a one-case publish→re-download→sha256-compare
experiment BEFORE trusting the zero-tolerance integrity check — if the round-trip mutates
pixels, the baseline must move to post-publish hashes and this decision be amended. Every
snapshot additionally records scratch bytes (raw + leak signal) and open-connection count
(FR-006 piggyback).

**Rationale**: Constant sample size keeps snapshot cost flat over a multi-hour run — a
growing verification cost would itself bend the resource curves the snapshot is measuring
(confounding the monotonic-growth rule). The recent/re-verified/random triple covers the
three corruption windows that matter: "the write that just happened", "data that was fine
before and silently rotted" (the exact failure a single end-of-run check cannot localize,
US4's motivation), and "anything else", respectively.

**Alternatives considered**:
- *Verify all completed cases every snapshot*: O(n) growth per snapshot; a 3-hour run's late
  snapshots would dominate the event stream and distort resource observations. Rejected.
- *Verify only the latest case*: old-data corruption goes undetected until the end, which is
  the exact blindness US4 exists to remove. Rejected.
- *Fixed pre-registered sample set*: never exercises late-arriving cases. Rejected.

---

## D4 — Run-report schema shape

**Decision**: Two artifacts. (1) `sim016_<run_id>.json`: a single versioned JSON object
(`schema_version: "016.1"`) with sections `run` (identity, mode, timing, seed, config echo,
pre-run server-project inventory preamble), `counts` (by event kind and outcome),
`failures` (tri-state tallies: FRIENDLY expected-rejections; ACCEPTED-malformed and CRASH as
unexpected; unexpected terminal list with truncated error detail), `snapshots` (full history
incl. resource observations), and `verdict` (three named boolean checks + `overall` +
`reasons[]` + `partial`/`stopped_early` flags). (2) `sim016_<run_id>.events.jsonl`: the full
event timeline, one JSON object per line, appended as each event completes; the report
references it by relative path rather than inlining it. Exact schema:
`contracts/run-report.md`.

**Rationale**: The report must be cheap and unambiguous for a context-less agent to parse —
one `json.load` plus `verdict.overall`. Inlining a multi-hour timeline would bloat that file
and put the crash-integrity burden on a large rewrite; JSONL is append-only (each line lands
or it doesn't — no partially-valid file), which is what makes the SC-006 partial-evidence
guarantee cheap. The report itself is small enough to rewrite atomically after every
snapshot. Explicit named verdict sub-checks (ratio / integrity / resource) mean a FAIL is
attributable without log spelunking, and the tri-state taxonomy is carried through from
`lane_malformed.py` unchanged so expected and unexpected classes are never conflated (SC-008).

**Alternatives considered**:
- *Single JSON with inline timeline*: bloat + torn-write risk on kill. Rejected.
- *SQLite ledger*: robust but heavier for an agent to interrogate and alien to the
  `tests/stress/results/*.json` convention every existing lane follows. Rejected.
- *CSV timeline*: no nested structure for error detail/shape descriptors. Rejected.

---

## D5 — Run-id and XNAT project naming

**Decision**: `run_id = "SIM016_" + UTC timestamp ("%Y%m%dT%H%M%SZ") + "_" + 4 hex chars`
from the run seed. The XNAT project name, the report filename, the JSONL filename, the pid
file, and the scratch root directory name are all exactly `run_id`. The project is created
fresh via the existing `driver.connect` PUT-project bootstrap; the simulation never writes
to, asserts about, or deletes anything outside it, and records (but does not judge) the
pre-existing project list in the report preamble.

**Rationale**: Encodes the clarified fresh-project-per-run answer. The timestamp gives
humans sortable provenance; the hex suffix removes the same-second double-invocation
collision; a single token shared across project, files, and scratch means any artifact found
later is traceable to its run with no lookup table. Alphanumeric-plus-underscore stays
within XNAT project-ID constraints. **Freshness is enforced by an explicit pre-bootstrap
existence probe in the lane** (`GET /data/projects/{project}`; HTTP 200 ⇒ exit 2): this must
be new lane code because `driver.connect`'s PUT-project bootstrap treats 409
(already-exists) as success and therefore cannot refuse a reused name — including an
operator-overridden `--project` value.

**Alternatives considered**:
- *Timestamp only*: same-second collision between two invocations (e.g., an agent retrying).
  Rejected.
- *Fixed project name*: explicitly rejected by the clarify session — prior-run residue is
  documented (dedup-lane history) to corrupt dedup verdicts, and docker-compose down loses
  data anyway.
- *Full uuid4*: collision-proof but unreadable and needlessly long for humans scanning a
  project list. Rejected.

---

## D6 — Graceful-stop mechanism (early termination → partial report)

**Decision**: The lane installs SIGINT/SIGTERM handlers and writes
`tests/stress/results/sim016_<run_id>.pid` at startup. First signal: set a stop event — the
dispatcher launches no new events, in-flight writes get a grace window (default 60 s,
`--stop-grace-s`), then one final integrity snapshot runs against completed cases and the
report is finalized with `stopped_early: true`, `partial: true`, exit code 3. Second signal
(or grace expiry): skip the final snapshot, finalize immediately from the ledger. A `kill -9`
worst case is still survivable evidence-wise: the JSONL holds every completed event and the
last atomically-written report snapshot remains valid on disk. The documented operator/agent
gesture is `kill -INT $(cat tests/stress/results/sim016_<run_id>.pid)`.

A run finalized with zero snapshots (double-signal before the first cadence tick) reports
`integrity_check.pass = false` with reason `no_integrity_evidence` — an unverified run is
never reported as verified (see contracts/run-report.md §2 rule 2).

**Rationale**: Directly satisfies FR-010 and SC-006 ("readable partial report … zero
unreadable/corrupted report files"). Signals are the one stop channel every shell-capable
agent has; the pid file removes the "which process?" question for a context-less agent. The
grace window prevents half-published cases from being misread as server-side data loss in
the final snapshot. Atomic `os.replace` report rewrites plus append-only JSONL make
corruption structurally impossible rather than merely unlikely (spec Edge Case 1).

**Alternatives considered**:
- *Stop-file polling as primary*: a second moving part and a polling latency, when signals
  are already universal; also leaves stale sentinel files behind. Rejected (signals win).
- *No handler — rely on JSONL alone*: loses the final snapshot and the verdict-bearing
  report; the agent would have to reconstruct a verdict itself, violating US1. Rejected as
  primary; retained only as the worst-case (`kill -9`) evidence floor.
- *Duration-only termination (no early stop)*: fails FR-010 outright. Rejected.

---

## D7 — Retry policy and the definition of "terminal" (FR-011, feeds the FR-006 ratio)

**Decision**: Per write attempt, classify and act:
- **Transient** (connection errors, timeouts, HTTP 5xx, `requests` transport exceptions):
  retry in-place with exponential backoff — base 5 s, factor 2, jitter ±20%, max 3 retries,
  per-attempt cap 60 s. Exhaustion ⇒ **unexpected terminal failure** (numerator of the
  max(1, 2%) ratio).
- **FRIENDLY** (GatewayError/FriendlyError-shaped rejections, HTTP 4xx semantic rejections,
  `SourceRFSession is_valid=False`): no retry — deterministic rejection. On an *injected
  malformed* case this is the **expected** outcome (excluded from the ratio, tallied
  separately); on a *normal* case it is an unexpected terminal failure.
- **Dedup probes**: `DedupReviewRequired` raised ⇒ expected probe success (excluded from the
  ratio). A dedup probe that publishes cleanly (silently ACCEPTED) ⇒ unexpected failure —
  but ONLY when the probe's dispatch-ordering guarantee held (base case `COMPLETED` before
  probe dispatch, data-model.md §2); a probe whose base never completed is re-designated
  normal (`probe_redesignated`) and never scored as unexpected.
  Symmetrically, an injected malformed case that is silently ACCEPTED, or that CRASHes
  (any unclassified exception), counts as unexpected — the existing ACCEPTED/FRIENDLY/CRASH
  tri-state from `lane_malformed.py` is reused verbatim, per the clarify session.
- **Config-update events**: excluded from `write_attempt_total` and the ratio entirely
  (own `by_kind` tally). A `LostUpdateError` from the lost-update guard is the guard
  working correctly ⇒ EXPECTED outcome (`guarded_lost_update`), never unexpected; only an
  unclassified exception on a config update counts as CRASH/unexpected.
- **CRASH** (any other exception): no retry, unexpected terminal failure, traceback head
  captured in the event record.
- **Server-outage circuit breaker**: 3 consecutive transient-class failures across distinct
  events ⇒ pause the dispatcher (arrivals are held, not dropped, and their displacement is
  recorded), probe `GET /data/version` every 15 s for up to 5 min; on recovery, resume and
  log a `server_outage` lifecycle event (recoverable class, FR-011); on expiry, finalize a
  partial report with verdict FAIL, reason `server_unreachable`.

**Rationale**: The clarified verdict threshold (max(1, 2%) *post-retry*) only regains its
meaning if retries genuinely absorb docker-local flakiness first — hence bounded exponential
backoff rather than none or forever. Distinguishing deterministic rejections (retry is
pointless and would triple-count them) from transport noise keeps the FRIENDLY lane clean
and SC-008's never-conflate requirement enforceable mechanically. The circuit breaker turns
"container briefly unresponsive under its own resource pressure" (spec Edge Case 2) into a
first-class recoverable event instead of a cascade of terminal failures, while the 5-minute
ceiling preserves self-termination (FR-003).

**Alternatives considered**:
- *No retry*: every docker hiccup becomes a terminal failure; with the 2% post-retry
  threshold, smoke and short runs would false-FAIL routinely. Rejected.
- *Unbounded retry*: masks real regressions (the exact reason the clarify session tightened
  5% → 2%) and can stall self-termination. Rejected.
- *Fixed-interval retry*: synchronized retry storms against a struggling container; backoff
  with jitter is strictly safer at identical complexity. Rejected.
- *Treating all malformed-case outcomes as expected*: conflates the classes SC-008 forbids
  conflating. Rejected.
