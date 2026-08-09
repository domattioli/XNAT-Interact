# Contract: Procedure Document (future `docs/SIM016_PROCEDURE.md`)

The FUTURE implementation pass writes `docs/SIM016_PROCEDURE.md` (the repo's runbook home,
alongside `INTEGRATION_TEST_RUNBOOK.md`). It is the ONLY context a fresh agent may rely on
(FR-007; spec Assumption 2). This contract fixes its required sections, in order. A document
missing any section, or referencing this planning conversation, FakeXNAT, or any
non-localhost server as a target, does not satisfy FR-007.

## Required sections

1. **Purpose and scope** — one paragraph: what the simulation proves, what it does not
   (not a max-throughput stress test; STAPLE remains a stub — consolidation activity is the
   reference-aggregator seam only, FR-012); explicit statement that the target is the real
   Docker-local XNAT and FakeXNAT is not a substitute (FR-009).
2. **Preconditions** — verbatim commands + expected outputs: container running
   (`docker ps` shows `xnat-local-it`); REST responsive
   (`curl -sf -u admin:admin http://localhost:8080/data/version` or `/data/projects`);
   Python ≥3.9 with `requirements.txt` installed; repository root as working directory.
   Each check paired with its failure remediation (e.g., "container absent → ask the human
   operator to run `docker compose up -d` in `tests/integration/xnat_local/`; booting is the
   human's job, not yours").
3. **Smoke run (do this first)** — exact invocation, expected wall-clock (< 5 min, SC-001),
   expected exit code, and the three greppable trailer lines from `contracts/cli.md`.
4. **Sustained run** — exact invocation with defaults spelled out (60 min, ≤~110 s mean
   inter-arrival, 80/10/10 + floors, ≤15-min snapshot cadence), guidance for choosing a
   different duration, and the note that each invocation creates a fresh `SIM016_*` project
   and never touches anything outside it.
5. **Reading the verdict** — where the report lands (`tests/stress/results/sim016_<run_id>.json`);
   the one-line parse (`verdict.overall`); the meaning of each sub-check
   (`ratio_check`, `integrity_check`, `resource_check`), of `partial`, and of the tri-state
   failure taxonomy; what to do on each outcome (PASS → record report path; FAIL → attach
   report + JSONL and the `reasons[]` strings to the finding; exit 2 → fix precondition and
   rerun). No outcome may be described as "crashes" without a next step (Constitution II).
6. **Stopping a run early** — the pid-file + `kill -INT` procedure, the grace window, the
   guarantee that a partial report is still readable (FR-010, SC-006), and the second-signal
   escalation.
7. **Mid-run server unresponsiveness** — what the circuit breaker does (pause, probe, resume
   or finalize-FAIL with `server_unreachable`), and that this is a recoverable class, not a
   design flaw (FR-011).
8. **Cleanup** — what the run leaves behind (its `SIM016_*` project server-side; report +
   JSONL locally), that scratch self-cleans per case, and that deleting the run project is
   optional and manual (the simulation never deletes anything itself outside per-case
   scratch; `docker compose down` discards all server state anyway).
9. **Troubleshooting table** — symptom → cause → remedy rows for at least: connection
   refused, HTTP 401, project-already-exists refusal, exit 2 variants, and a verdict FAIL
   per sub-check.

## Style requirements

Plain professional prose; every command copy-pasteable from the repo root; no reference to
any conversational context; no credentials other than the documented throwaway localhost
admin/admin pair.
