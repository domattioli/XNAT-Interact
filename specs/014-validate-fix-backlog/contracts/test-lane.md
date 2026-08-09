# Contract: Test Lanes (014)

## Default (offline) lane — `.github/workflows/tests.yml`

- Trigger: PR + workflow_dispatch. Single testing workflow in the repo (D1: `python-package.yml` deleted).
- Invocation: `pytest -m "not slow and not stress and not pixeldeid and not pg and not requires_server"`
- Guarantees: no network egress to any XNAT host; fixtures from `tests/synthetic_data.py` only; completes < 20 min.
- Exit 0 ⇔ every selected regression + characterization + existing test passes.

## Stress / dual-run lane (opt-in)

- Selection: `pytest -m stress`; real-server re-run enabled by `RUN_XNAT_DUAL=1`.
- Connection fixture MUST assert target host ∈ {localhost, 127.0.0.1, explicit disposable-instance env value}; UIowa production hostname is a hard fixture error.
- Flakiness here never gates the default lane.

## Marker additions to pytest.ini

- `stress: concurrency/scale regression test; excluded from the default gate.`

## Verification-status reporting (D9)

- PR description block, updated every push:
  `Verification status: UNVERIFIED — blocked on CI (#44) | Local: <passing>/<total>` → flips to `VERIFIED — tests.yml run <url> green` only on an actual green run.
