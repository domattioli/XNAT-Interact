# Test Scaffolding & Fake-Data Manifest

**Purpose**: Catalog every file that exists to *test with synthetic / fake data*
so the throwaway scaffolding can be confidently deleted later without touching the
permanent offline regression suite. "Delete the cruft" cleanup checklist.

**Created**: 2026-06-05 (Phase 7 — real-XNAT round-trip correctness)
**Status**: Phase 7 complete — all KEEP tests landed (742 passed, 7 xfailed). THROWAWAY harness awaits Phase 7 local round-trip verification (T006/T020) + Phase 6 offline contract test before removal.

---

## Classification

- **KEEP (permanent offline fixtures)** — part of the CI regression suite. Runs with
  no server, no PHI. Constitution Principle IV requires these. Do NOT delete.
- **THROWAWAY (real-XNAT scaffolding)** — stood up once to validate against a live
  *local, synthetic* XNAT. Safe to delete after the validated fixes land + Phase 6
  gateway/contract test subsumes the need. Never points at production; no real PHI.

---

## KEEP — permanent offline test fixtures

| File | Role | Notes |
|---|---|---|
| `tests/synthetic_data.py` | Synthetic no-PHI DICOM + dataset generators | Only sanctioned source of test fixtures (constitution IV). |
| `tests/fakes/fake_xnat.py` | `FakeXNAT` test double (offline pyxnat stand-in) | Phase 7 added a **fidelity flag** (reproduces pyxnat post-`create()` empty datatype cache), `seed_existing()` (idempotent-upsert seeding), label-vs-ID enumeration, N-file resources. Phase 6 will rename → `FakeGateway` behind the gateway ABC. |
| `tests/test_*.py` (full suite) | Offline regression suite (726+ tests) | The net that guards every change. |

### Phase 7 offline tests added (KEEP)

| File | Issue | Covers |
|---|---|---|
| `tests/test_publish_real_contract.py` | #27 | push datatype-cache fix + idempotent-upsert re-publish |
| `tests/test_session_metadata_guard.py` | #30 | missing-`InstanceNumber` guard |
| `tests/test_configtables_bootstrap.py` | #28 | fresh-project + non-whitelisted/allowlisted bootstrap (7 tests, 2 xfailed) |
| `tests/test_browse_labels.py` | #29 | RF surfaced, subject labels not internal IDs (5 tests, 2 xfailed) |
| `tests/test_download_full_series.py` | #25 | N-file + whole-surgery zip + count-verify + empty no-op (6 tests, 1 xfailed) |
| `tests/test_stale_bootstrap_removed.py` | #30 | stale initialize_basic_metatable_items.py removed smoke (3 tests) |

> These are red-first regression tests — each FAILS at its parent commit, proving
> the FakeXNAT fidelity gap is closed. They stay forever.

---

## THROWAWAY — real-XNAT validation scaffolding (delete after Phase 6/7 close)

Built to run the one-time real round-trip described in
`docs/INTEGRATION_TEST_RUNBOOK.md`. Used a local, ephemeral XNAT (Docker) with
synthetic no-PHI data. **None of this runs in CI**; it requires a local server.

| File / dir | Role | Delete when |
|---|---|---|
| `tests/integration/xnat_local/` (Dockerfile, build_and_run.sh, launch-xnat.sh, make-xnat-config.sh, set-admin-password.sh, supervisord.conf, XNAT.sql, xnat-prefs-init.ini, README.md) | Local throwaway XNAT image (admin/admin) for round-trip validation | Phase 6 gateway + pyxnat contract test replaces the need for a live local server, OR xnat4tests is adopted. |
| `tests/integration/run_roundtrip_push.py` | One-time real push driver (used original src/ code) | After #27/#28 verified green against the local server (T006/T020) and findings are captured. |
| `tests/integration/run_roundtrip_pull.py` | One-time real pull driver | After #25/#29 verified (T020). |
| `tests/integration/roundtrip_push.log`, `roundtrip_pull.log` | Captured evidence logs (cited in issues #25/#27/#28/#29/#30) | Keep until the issues close; then delete (evidence preserved in the issues + this repo history). |

> **Retention rationale**: keep the THROWAWAY harness until Phase 7 fixes are
> confirmed against the local server (tasks T006 + T020) AND Phase 6's offline
> pyxnat contract test exists. At that point the live-server round-trip is no
> longer the only proof of the real-XNAT contract → safe to delete the whole
> `tests/integration/` tree in one cleanup commit.

---

## Cleanup procedure (when ready)

1. Confirm Phase 7 SC-001..SC-006 met (offline suite green + local round-trip green).
2. Confirm Phase 6 ships the offline pyxnat **contract test** (the durable
   replacement for the live-server check).
3. `git rm -r tests/integration/` in a single `chore: remove real-XNAT validation scaffolding` commit.
4. Leave everything under **KEEP** untouched — that is the permanent offline suite.
5. Update this manifest's status to "scaffolding removed" (or delete it too).

---

## Guardrails (always true)

- No file here ever targets the UIowa production server (`rpacs.iibi.uiowa.edu`).
- All fixtures are synthetic — zero real PHI, in fixtures or logs.
- `admin/admin` is a local throwaway credential only; never a real secret.
