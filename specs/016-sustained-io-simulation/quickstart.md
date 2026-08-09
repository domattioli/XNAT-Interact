# Quickstart: SIM016 Runbook Skeleton (for the future context-less agent)

**Status**: skeleton only. This feature is design-only (FR-013); the lane script
(`tests/stress/lane_sim016.py`) and the full procedure document
(`docs/SIM016_PROCEDURE.md`, per `contracts/procedure-doc.md`) do not exist yet. This file
is the outline both will be validated against once the future implementation pass runs.

## 0. You are

A fresh agent with repository access and shell tools, no memory of the planning
conversation. A human has already booted the local XNAT container. Everything you need is
in this repo.

## 1. Precondition checks (all must pass before invoking anything)

```bash
# Container up?
docker ps --format '{{.Names}}' | grep -q xnat-local-it && echo CONTAINER_OK

# REST responsive? (throwaway localhost creds admin/admin)
curl -sf -u admin:admin http://localhost:8080/data/version && echo REST_OK
# fallback probe if /data/version is unavailable on this XNAT build:
curl -sf -u admin:admin "http://localhost:8080/data/projects?format=json" >/dev/null && echo REST_OK

# Python + deps
python -c "import pydicom, numpy, pandas, requests, pyxnat" && echo DEPS_OK
```

If the container is down: booting it is the human operator's job
(`docker compose up -d` in `tests/integration/xnat_local/`) — report and wait; do not
substitute FakeXNAT (FR-009).

## 2. Smoke run first (always)

```bash
cd <repo-root>
python -m tests.stress.lane_sim016 --mode smoke --verbose
```

Expect: completion in under ~3 minutes (budget 5, SC-001), exit code 0, trailer lines
`VERDICT: PASS`, `REPORT: …/tests/stress/results/sim016_<run_id>.json`, `PARTIAL: false`.
Do not proceed to a sustained run on a smoke FAIL.

## 3. Sustained run

```bash
python -m tests.stress.lane_sim016 --mode sustained            # 60 min default
# or a custom duration:
python -m tests.stress.lane_sim016 --mode sustained --duration-s 7200
```

The run self-terminates. Each invocation creates a fresh `SIM016_<ts>_<hex>` project and
never reads from, writes to, or deletes anything outside it.

## 4. Read the verdict

```bash
python - <<'EOF'
import json, glob
report = sorted(glob.glob("tests/stress/results/sim016_*.json"))[-1]
v = json.load(open(report))["verdict"]
print(report, v["overall"], "partial=", v["partial"], v["reasons"])
EOF
```

- `overall: PASS` → record the report path; done.
- `overall: FAIL` → the failed sub-check(s) (`ratio_check` / `integrity_check` /
  `resource_check`) and `reasons[]` name the cause; attach the report and the
  `sim016_<run_id>.events.jsonl` timeline to your finding.
- `partial: true` → verdict covers only the portion that ran (early stop / outage).

## 5. Stopping early (safe at any time)

```bash
kill -INT $(cat tests/stress/results/sim016_<run_id>.pid)
```

In-flight writes get a grace window, a final snapshot runs, and a readable partial report is
finalized (exit code 3). A second SIGINT skips the final snapshot. Even a hard kill leaves
the JSONL event log and the last valid report on disk.

## 6. If the server hiccups mid-run

Nothing to do: transient failures are retried with backoff; a sustained outage pauses the
run for up to 5 minutes of probing, then either resumes (logged as a recoverable
`server_outage` event) or finalizes a partial FAIL report with reason `server_unreachable`.
