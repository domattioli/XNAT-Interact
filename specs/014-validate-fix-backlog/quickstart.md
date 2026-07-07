# Quickstart: Close the Remaining Verified-Fix Gap (014)

## Run the default offline suite

```bash
pip install -r requirements.txt -r requirements-dev.txt
pytest -m "not slow and not stress and not pixeldeid and not pg and not requires_server"
```

## Run only the 014 regression net

```bash
pytest tests/regression_014/
```

## Reproduce a "fails pre-fix" proof for an already-fixed finding (M7/M8/M10)

```bash
# Historical pre-fix commit is recorded per-finding in ledger.md
git worktree add /tmp/prefix <PRE_FIX_COMMIT>
cd /tmp/prefix && pytest <test path from ledger row> -x   # expect FAIL
cd - && git worktree remove /tmp/prefix
```

## Reproduce a "fails at current tip" proof for a still-open finding (H8/M4/L1/S1/S4)

```bash
git worktree add /tmp/tip HEAD
cd /tmp/tip && pytest <new test path> -x   # expect FAIL before the fix lands
```

## Where things live

- Complete disposition ledger (all ~29 #33 findings): `specs/014-validate-fix-backlog/ledger.md`
- Verification status: PR description block — UNVERIFIED until a green CI run exists (#44 outage note applies)
