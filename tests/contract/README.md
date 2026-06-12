# Contract Tests — Dual-Run Parity

## Overview

`tests/contract/` contains behavioral parity tests that verify `FakeXNAT ≈ real XNAT`.

- **Default mode** (no env var): runs fake-side only. Zero Docker requirement. CI-safe.
- **Dual-run mode** (`RUN_XNAT_DUAL=1`): boots a local XNAT container, runs each test
  against both FakeXNAT and the real server, and compares state via `XnatStateComparator`.

## Prerequisites for Dual-Run

- Docker >= 20.10
- Docker Compose v2 (`docker compose` — note: no hyphen)
- ~4 GB RAM available for the XNAT container

## Local Invocation

```bash
# Fake-side only (CI default)
pytest tests/contract/

# Dual-run — boots real XNAT, compares fake vs real state for all 9 contract tests
RUN_XNAT_DUAL=1 pytest tests/contract/
```

## XNAT Local Image

**Source**: custom `Dockerfile` at `tests/integration/xnat_local/Dockerfile`.

The image bundles:
- XNAT 1.9.3 WAR (downloaded from Bitbucket at build time)
- container-service plugin 3.7.3
- batch-launch plugin 0.9.0-xpl
- PostgreSQL 14 (Ubuntu jammy base: `tomcat:9-jdk8-temurin-jammy`)

**Build + boot sequence** (handled automatically by the `real_xnat` fixture):

1. `docker compose up -d` in `tests/integration/xnat_local/`
2. Container starts supervisord → PostgreSQL → Tomcat
3. Fixture polls `http://localhost:8080/xapi/siteConfig` until HTTP 200 (timeout 60 s,
   2 s interval). First-ever boot may take 60–120 s (DB init + WAR deployment).
4. On subsequent runs the image is cached; startup is ~30–60 s.

**Throwaway credentials** (localhost only — do NOT use against any production server):
- XNAT admin: `admin` / `admin`
- DB user: `xnat` / `xnat`

## Per-Test Isolation

The `real_xnat_project` fixture (function-scoped) creates a project named
`ITEST_<8 hex chars>` before each test and deletes it after. The container is reused
across all tests in the session; teardown at session end runs `docker compose down`.

## No CI Lane

Dual-run is local-only this batch (C009). A CI lane (`xnat-dual-run`) is out of scope
and will be added in a future phase once image caching strategy is decided.

## Files

| File | Purpose |
|------|---------|
| `conftest.py` | `fake_xnat`, `real_xnat` (session-scoped), `real_xnat_project` fixtures |
| `comparator.py` | `XnatStateComparator` — capture, normalize, and diff XNAT state |
| `test_comparator.py` | Unit tests for `XnatStateComparator` |
| `test_workflow_contract.py` | 9 workflow contract tests (T001–T007), dual-run seams live |
