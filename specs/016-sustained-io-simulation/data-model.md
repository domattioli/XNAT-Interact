# Phase 1 Data Model: Sustained Real-World I/O Simulation (016)

**Date**: 2026-07-09 | **Plan**: [plan.md](plan.md) | **Spec**: [spec.md](spec.md)

Entities the FUTURE implementation materializes (FR-013: nothing is implemented by this
feature). Field names below are binding for the report/JSONL serializations (see
`contracts/run-report.md`); in-memory representations may differ internally.

---

## 1. Run Mode Configuration (`config`)

The operator/agent-selectable parameters distinguishing a smoke run from a sustained run.
Echoed verbatim (minus credentials) into the report's `run.config` block.

| Field | Type | Default (sustained) | Default (smoke) | Notes |
|---|---|---|---|---|
| `mode` | `"smoke" \| "sustained"` | `sustained` | `smoke` | CLI `--mode` |
| `duration_s` | int | 3600 | 90 | Self-termination bound (FR-003) |
| `seed` | int | time-derived | time-derived | Echoed in report; full reproducibility |
| `mean_interarrival_s` | float | 110 | 20 | Case-write Poisson mean (D1) |
| `read_mean_interarrival_s` | float | 45 | 15 | Read-stream Poisson mean |
| `config_update_mean_s` | float | 900 | disabled | Sparse config-update stream |
| `variety` | {normal, malformed, dedup} floats | 0.80 / 0.10 / 0.10 | 1.0 / 0 / 0 | Must sum to 1.0 ± 0.001 |
| `min_malformed` | int | 3 | 0 | Floor top-up (FR-001); sustained only |
| `min_dedup` | int | 3 | 0 | Floor top-up (FR-001); sustained only |
| `snapshot_cadence_s` | int | 720 | end-of-run only | ≤ ~900 ceiling (FR-005) |
| `snapshot_sample_size` | int | 3 | 1 | D3 |
| `writer_pool_size` | int | 3 | 1 | Bounded pool (D2) |
| `max_connections` | int | 10 | 10 | FR-006 resource bound |
| `retry_max` | int | 3 | 3 | D7 |
| `retry_base_s` | float | 5 | 2 | D7 backoff base |
| `outage_probe_window_s` | int | 300 | 60 | D7 circuit breaker ceiling |
| `stop_grace_s` | int | 60 | 15 | D6 |
| `url` / `user` / `password` | str | `http://localhost:8080` / admin / admin | same | Env-overridable; password never serialized |
| `project` | str | auto `SIM016_<ts>_<hex4>` | auto | = `run_id` (D5); never reused |
| `results_dir` | path | `tests/stress/results/` | same | Existing lane convention |

**Validation rules**: variety proportions sum to 1.0; `duration_s` ≥ 60 (sustained) or
30–300 (smoke); `mean_interarrival_s` > 0 and, for sustained, ≤ 120 (keeps the ≥30-case
expectation honest); `snapshot_cadence_s` ≤ 900 and < `duration_s` (sustained);
`writer_pool_size` + 2 ≤ `max_connections`; `url` host ∈ {localhost, 127.0.0.1, explicit
disposable-host env value} — anything else (notably the production RPACS hostname) is a hard
startup error (Constitution I/V).

---

## 2. Simulated Case Arrival (`case`)

A synthetic surgical case generated and uploaded at a scheduled point (spec Key Entity 1).

| Field | Type | Notes |
|---|---|---|
| `case_id` | str | `CASE_<seq:04d>` within the run |
| `scheduled_at` | float (epoch s) | From the pre-generated timeline (D1) |
| `shape` | object | See shape descriptor below |
| `gen_hashes` | {filename: sha256} | From `factory.surgery_pixel_hashes` at generation time; integrity baseline |
| `has_annotations` | bool | ~30% of normal cases run the `lane_annotations` upload pattern |
| `status` | enum | State machine below |
| `attempts` | int | Write attempts incl. retries (D7) |
| `classification` | `null \| "FRIENDLY" \| "ACCEPTED" \| "CRASH"` | Tri-state, `lane_malformed.py` semantics |
| `unexpected` | bool | Per D7 rules (expected FRIENDLY/dedup-rejection ⇒ false) |
| `scratch_dir` | path | Per-case dir under the run scratch root; removed finally-style on completion |

**Shape descriptor** (`shape`): `category` (`normal` | `malformed` | `dedup_probe`);
`subtype` — for malformed: one of the `tests/stress/malformed.py` generator names
(`truncated`, `no_instance_number`, `three_channel`, `dup_private_tag`, `huge_surgery`,
`not_a_dicom`); for dedup probes: one of the `factory.overlap_cases` relations (`exact`,
`subset`, `superset`, `partial`) against a designated earlier base case; `frames` (int,
drawn 3–20 for size variety), `rows`/`cols` (64 default), `expected_outcome`
(`ACCEPT` for normal, `FRIENDLY` for malformed, `DEDUP_REJECT` for probes).

**State machine**:
`SCHEDULED → GENERATING → UPLOADING → (RETRY_WAIT → UPLOADING)* →
COMPLETED | REJECTED_FRIENDLY | FAILED_TERMINAL`
— only `COMPLETED` cases are eligible for integrity sampling (D3) and read-download events;
`REJECTED_FRIENDLY` on an injected malformed case or a dedup probe is the expected outcome;
`FAILED_TERMINAL` always increments the unexpected-terminal numerator.

**Validation rules**: every non-`SCHEDULED` case has ≥1 attempt; a `COMPLETED` case must
have non-empty `gen_hashes`; scratch dir must not exist after a terminal state (cleanup
invariant, FR-006).

---

## 3. Event Timeline record (`event` — one JSONL line each)

The chronological evidence base (spec Key Entity 2); serialization contract in
`contracts/run-report.md` §3.

| Field | Type | Notes |
|---|---|---|
| `event_id` | str | `EVT_<seq:05d>` |
| `kind` | enum | `case_write`, `annotation_write`, `read_inventory`, `read_download`, `read_browse`, `config_update`, `snapshot`, `lifecycle` (start/stop/outage/resume) |
| `scheduled_at` / `started_at` / `completed_at` | float epoch s | `completed_at` null only for the in-flight-at-kill worst case |
| `outcome` | `"ok" \| "friendly" \| "terminal" \| "crash" \| "benign_partial_read"` | `benign_partial_read` = US3 scenario 2 |
| `case_ref` | str \| null | `case_id` for case-linked events |
| `retries` | int | |
| `error` | str \| null | Type + message + traceback head, truncated ≤ 2000 chars |

**Relationships**: `case_write` events 1:1 with `case` records; `snapshot` events 1:1 with
Integrity Snapshot records; SC-004 is checked as: ∃ read-kind event with
`started_at` strictly inside some `case_write`'s `[started_at, completed_at]`.

---

## 4. Integrity Snapshot (`snapshot`)

Point-in-time verification (spec Key Entity 3) + FR-006 resource piggyback.

| Field | Type | Notes |
|---|---|---|
| `snapshot_id` | str | `SNAP_<seq:03d>` |
| `at` | float epoch s | |
| `sampled` | array | Per case: `{case_id, checksum_ok, links_ok, present_ok, detail}` |
| `pass` | bool | AND of all sampled checks |
| `resources` | object | `{scratch_bytes, open_connections, scratch_monotonic_run_len}` |
| `resource_ok` | bool | scratch < 1 GB AND monotonic run < 3 AND connections ≤ `max_connections` |

**Validation rules**: sampled cases must be `COMPLETED` at sampling time (never in-flight,
US4 scenario 2); `scratch_monotonic_run_len` counts consecutive snapshots with strictly
increasing `scratch_bytes` (3 ⇒ resource FAIL, clarified rule); any `pass = false` anywhere
in the run triggers the zero-tolerance integrity FAIL regardless of later snapshots.

---

## 5. Run Report (`report`)

The machine-readable summary (spec Key Entity 4). Full schema: `contracts/run-report.md`.
Top-level sections: `schema_version`, `run` (identity, mode, timestamps, seed, config echo,
`preamble` = pre-run server project inventory + XNAT version string), `counts`,
`failures` (tri-state tallies + unexpected-terminal detail list), `snapshots`,
`verdict` (`ratio_check`, `integrity_check`, `resource_check`, `overall`, `reasons[]`,
`partial`, `stopped_early`), `timeline_ref` (relative JSONL path).

**Verdict computation (binding)**:
- `ratio_check.pass` ⇔ `unexpected_terminal_count ≤ max(1, ceil(0.02 × write_attempt_total))`
- `integrity_check.pass` ⇔ no snapshot in the run has `pass = false`
- `resource_check.pass` ⇔ every snapshot has `resource_ok = true`
- `overall = "PASS"` ⇔ all three pass; else `"FAIL"`, with one human-readable string per
  failed check appended to `reasons[]`.

**Validation rules**: report must parse as JSON after any kill point (guaranteed by atomic
rewrite, D6); `partial = true` whenever `stopped_early` or the configured duration was not
reached; credentials never appear; `run.preamble` is recorded evidence only — the verdict
never depends on pre-existing server state.

---

## Relationships overview

```
Run Mode Configuration 1───1 Run Report
        │ generates                 │ references
        ▼                           ▼
  Event Timeline (JSONL) ◄──1:1── snapshot events
        │ case_write 1:1
        ▼
 Simulated Case Arrival ──(COMPLETED only)──► sampled by Integrity Snapshot
```
