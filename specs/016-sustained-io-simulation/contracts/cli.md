# Contract: CLI for the future SIM016 lane script

Entry point (FUTURE implementation; FR-013 — not built by this feature):

```
python -m tests.stress.lane_sim016 [options]
```

Invoked from the repository root with the repo's pinned requirements installed and a booted
Docker-local XNAT (`tests/integration/xnat_local/docker-compose.yml`). The script is
standalone (stress-lane convention), NOT collected by the default pytest gate.

## Arguments

| Argument | Type / values | Default | Meaning |
|---|---|---|---|
| `--mode` | `smoke` \| `sustained` | `sustained` | Run mode (FR-003) |
| `--duration-s` | int | 3600 (sustained) / 90 (smoke) | Self-termination bound |
| `--seed` | int | time-derived | Timeline RNG seed; echoed in report |
| `--mean-interarrival-s` | float | 110 / 20 | Case-write Poisson mean |
| `--read-mean-interarrival-s` | float | 45 / 15 | Read-stream Poisson mean |
| `--variety` | `f,f,f` (normal,malformed,dedup) | `0.80,0.10,0.10` | Must sum to 1.0 |
| `--min-malformed` | int | 3 (sustained) / 0 (smoke) | Absolute floor (FR-001) |
| `--min-dedup` | int | 3 (sustained) / 0 (smoke) | Absolute floor (FR-001) |
| `--snapshot-cadence-s` | int | 720 | ≤ 900; ignored in smoke (end-of-run snapshot only) |
| `--writer-pool-size` | int | 3 | Bounded write concurrency (D2) |
| `--max-connections` | int | 10 | Resource bound (FR-006) |
| `--retry-max` | int | 3 | D7 |
| `--stop-grace-s` | int | 60 / 15 | D6 grace window |
| `--url` | str | `http://localhost:8080` | Env `XNAT_SERVER_URL` overrides |
| `--user` | str | `admin` | Env `XNAT_USERNAME` overrides |
| `--password` | str | `admin` | Env `XNAT_PASSWORD` overrides; throwaway localhost pair only — never logged, never serialized into the report |
| `--project` | str | auto `SIM016_<utc-ts>_<hex4>` | Overriding is allowed but MUST still be a fresh, run-scoped name; the lane refuses a project that already exists server-side |
| `--results-dir` | path | `tests/stress/results/` | Report/JSONL/pid destination |
| `--verbose` / `-v` | flag | off | Human-readable progress |

Constraint checks at startup (violations ⇒ exit 2, nothing written server-side): variety sums
to 1.0; `writer_pool_size + 2 ≤ max_connections`; URL host is localhost/127.0.0.1/explicit
disposable-host env value (production RPACS hostname is a hard error); server precondition
probe `GET /data/version` (fallback `GET /data/projects`) answers within 15 s.

## Environment variables

`XNAT_SERVER_URL`, `XNAT_PROJECT_NAME` (set BY the lane to the run project before importing
`src.*` — existing lane convention), `XNAT_USERNAME`, `XNAT_PASSWORD`,
`XNAT_IDENTITY_SALT` (defaulted by the lane if unset, as in existing lanes).

## Exit codes (binding)

| Code | Meaning | Report state |
|---|---|---|
| 0 | Run completed; verdict PASS | `verdict.overall = "PASS"`, `partial = false` |
| 1 | Run completed; verdict FAIL | `verdict.overall = "FAIL"`, `partial = false` |
| 2 | Precondition/config error before any simulated event | Report may be absent; stderr states the reason and the fix (Constitution II) |
| 3 | Early-stopped (signal) — partial report finalized | `stopped_early = true`, `partial = true`; verdict reflects evidence up to the stop |

## Stop procedure

`kill -INT $(cat <results-dir>/sim016_<run_id>.pid)` — see D6. Second SIGINT forces immediate
finalization. The report and JSONL remain valid at every instant by construction.

## Console output contract

Last three lines on any completion path (machine-greppable):
```
VERDICT: PASS|FAIL
REPORT: <absolute path to sim016_<run_id>.json>
PARTIAL: true|false
```
