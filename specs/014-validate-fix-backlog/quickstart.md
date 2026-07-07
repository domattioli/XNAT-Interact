# Quickstart: Validate the Unverified Fix Backlog (014)

## Run the default (offline) suite — what CI runs

```bash
pip install -r requirements.txt -r requirements-dev.txt
pytest -m "not slow and not stress and not pixeldeid and not pg and not requires_server"
```

No server, no VPN, no PHI. FakeXNAT (`tests/fakes/fake_xnat.py`) stands in for XNAT.

## Run only the 014 regression net

```bash
pytest tests/regression_014/ tests/characterization/
```

## Reproduce a "fails pre-fix" proof (SC-002, <5 min)

```bash
# Baseline SHA is at the top of specs/014-validate-fix-backlog/ledger.md
git worktree add /tmp/prefix <BASELINE_SHA>
cd /tmp/prefix && pytest <test path from ledger row> -x   # expect FAIL
cd - && git worktree remove /tmp/prefix
```

## Opt-in stress / real-server lane

```bash
pytest -m stress                          # concurrency/scale, FakeXNAT
RUN_XNAT_DUAL=1 pytest -m stress          # re-run against a DISPOSABLE local XNAT only
```

The connection fixture hard-errors on any non-localhost/non-disposable host — the UIowa production server is unreachable by construction.

## Where things live

- Disposition ledger: `specs/014-validate-fix-backlog/ledger.md` (every finding + every draft PR accounted for)
- Verification status: PR description block — UNVERIFIED until a green `tests.yml` run exists (#44 outage note applies)
