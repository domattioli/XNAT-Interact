# Implementation Plan: Operational Hardening (011)

**Branch**: `development` | **Date**: 2026-06-09 | **Spec**: [`spec.md`](spec.md)
**Input**: Feature specification from `specs/011-operational-hardening/spec.md`

## Summary

Make the 009 identity substrate **safely deployable** and **portable**, and make local-XNAT bring-up
reproducible. Three independent slices, built in priority order:

- **US1 (P1) — 009 deploy**: provision `XNAT_IDENTITY_SALT` (CSPRNG, librarian-only, never logged);
  encrypt `CrosswalkStore` at rest (scrypt-derived key + AEAD); run `migrate_from_configtables`
  against the live JSON with the existing parity+rollback guarantee; archive (not delete) the source.
- **US2 (P2) — Postgres path (#34)**: re-express the 009 registry schema + queries once via
  **SQLAlchemy Core** behind the *unchanged* `Registry` public API; SQLite default, Postgres opt-in;
  parity-checked SQLite→Postgres move; the 009 contract suite passes on both backends.
- **US3 (P3) — local-XNAT boot**: a single `scripts/` entrypoint that boots/detects a **localhost**
  XNAT, runs ordered health probes, emits a HEALTHY/FAILED verdict, and refuses any non-localhost host.

All localhost + synthetic only. No prod (`rpacs.iibi.uiowa.edu`), no real PHI, no
salt/HawkID/passphrase in logs, argv, tests, or repo.

## Technical Context

**Language/Version**: Python 3.11.
**Primary Dependencies**: existing — `pydicom`, `numpy`; **new** — `SQLAlchemy` (Core, US2),
`cryptography` (AEAD + `scrypt` KDF, US1). `psycopg[binary]` only when a Postgres DSN is configured.
All new deps **lazy-imported** + degrade/skip when absent (010 precedent).
**Storage**: SQLite (default registry) → optional Postgres; encrypted JSON crosswalk; salt in a
`0600` file or env hex.
**Testing**: pytest. New markers reused from 010: heavy/opt-in lanes deselected by default.
US2 Postgres contract test **skips** when no `XNAT_REGISTRY_PG_DSN` (no Postgres in offline CI).
**Target Platform**: Linux/macOS localhost (single-librarian today; Postgres path for future server).
**Project Type**: single project (CLI + service library), existing `src/services/` layout.
**Performance Goals**: deploy/migration dominated by JSON size (seconds for current scale);
registry op parity SQLite↔Postgres (no behavior change); boot-verify probes < ~60 s.
**Constraints**: offline-capable default lane; zero secret leakage; additive/opt-in wiring so the
existing ~1101-test suite stays green.
**Scale/Scope**: hundreds–thousands of registry rows; one librarian; one localhost XNAT.

## Constitution Check

*GATE — re-checked after design. All pass.*

- **I PHI Safety**: US1 **strengthens** it — crosswalk (only real-HawkID copy) goes plaintext →
  AEAD-encrypted; salt provisioning keeps the 009 never-log invariant and extends it to the deploy
  command + passphrase. Migration audit records categories/counts only. US3 round-trip is synthetic.
  Tests assert: plaintext grep of crosswalk for a HawkID fails; salt/passphrase absent from all output.
- **II Human-in-the-loop**: deploy is operator-run; migration parity failure HARD-STOPS (rollback),
  never silently proceeds. boot-verify refuses non-localhost rather than guessing.
- **III Reproducibility/Determinism**: migration parity-checked + reversible (009 contract inherited);
  boot-verify idempotent.
- **IV Fail-loud**: every failure path raises `FriendlyError`/non-zero with a named cause (existing
  pattern); no silent fallback that hides a secret-missing or parity-fail condition.
- **V Test-gated**: each story ships tests; default lane stays offline-green; heavy/Postgres lanes
  opt-in. No coverage regression to the 009/010 suites.
- **VI No-secrets-in-repo**: salt, passphrase, HawkID, DSN never committed/logged; `.gitignore`
  covers salt file, crosswalk file, `*.db`, archived JSON.

**Result: PASS** (no new principle violations; US1 net-reduces PHI exposure).

## Project Structure

### Documentation (this feature)

```text
specs/011-operational-hardening/
├── spec.md          # done (clarified)
├── plan.md          # this file
├── tasks.md         # /speckit.tasks output
└── analyze.md       # /speckit.analyze output
```

### Source Code

```text
src/services/
├── crosswalk_crypto.py   # NEW (US1): scrypt-KDF + AEAD encrypt/decrypt envelope for CrosswalkStore
├── registry.py           # MOD (US2): Registry re-expressed on SQLAlchemy Core; same public API
├── registry_backend.py   # NEW (US2): engine factory + 009 schema as SQLAlchemy Core Tables, dialect-agnostic
├── identity.py           # (unchanged) load_identity_salt contract reused
└── deploy_009.py         # NEW (US1): orchestrates provision-salt → encrypt-crosswalk → migrate → archive

src/
└── cli/deploy_009.py     # NEW (US1): thin argparse entrypoint (or scripts/, see below)

scripts/
├── provision_identity_salt.sh   # NEW (US1): CSPRNG salt → 0600 file; refuses to overwrite/echo
└── boot_and_verify_local_xnat.sh# NEW (US3): localhost-guarded boot + ordered health probes

tests/
├── test_011_crosswalk_crypto.py    # US1: encrypt round-trip, plaintext-grep-fails, wrong-key-fails
├── test_011_deploy_009.py          # US1: provision idempotent, migrate parity+rollback, archive, no-leak
├── test_011_registry_backend.py    # US2: 009 contract suite parametrized over sqlite (+pg if DSN)
├── test_011_pg_parity.py           # US2: SQLite→Postgres move parity (skip if no DSN) [pg marker]
└── test_011_boot_verify.py         # US3: localhost-refuse, idempotent, probe-fail names cause
```

**Structure decision**: extend existing `src/services/`. US2 introduces a `registry_backend` seam so
`registry.py`'s public methods (`upsert_*`, `image_exists`, `case_image_hashes`, `record_audit`,
`migrate_from_configtables`) keep identical signatures — callers (`utilities.py:650`,
`xnat_experiment_data.py`) are byte-unchanged. The CrosswalkStore encryption is **format-versioned**:
a magic header distinguishes legacy plaintext JSON (read-migrate-once) from the new AEAD envelope, so
existing crosswalk files upgrade transparently.

## Approach by story

### US1 — 009 deploy (P1)
1. `crosswalk_crypto.py`: `derive_key(passphrase, salt_kdf) -> bytes` (scrypt, params pinned);
   `seal(plaintext_json: dict, key) -> bytes` / `open_envelope(blob, key) -> dict` using
   `cryptography` AEAD (e.g. `Fernet`/`AESGCM`). Envelope = `magic || kdf_salt || nonce || ct`.
   Passphrase from env `XNAT_CROSSWALK_PASSPHRASE` or interactive prompt (never argv).
2. `CrosswalkStore` (registry.py) gains an opt-in `key`/`passphrase` path: when configured, `_load`/
   `_save` go through the envelope; legacy plaintext auto-detected (no magic) → read once, re-seal.
   Default (no key) keeps the existing plaintext behavior so 009 tests pass unchanged.
3. `provision_identity_salt.sh`: generate 32-byte CSPRNG → write hex to a `0600` file; refuse if file
   exists (no clobber), never echo the value; print only the path + a fingerprint (sha256 prefix).
4. `deploy_009.py`: load JSON → `Registry.migrate_from_configtables` (parity+rollback already there) →
   on success `record_audit("librarian","deploy-009-migrate", json_summary)` → move source JSON to
   `*.archived-<ts>.json`. On parity fail: registry untouched, JSON left in place, non-zero exit.

### US2 — Postgres path (P2)
1. `registry_backend.py`: SQLAlchemy `MetaData` + `Table` defs mirroring `_SCHEMA_SQL` (surgeons,
   raters, cases, image_hashes w/ UNIQUE content_hash, audit_log). `make_engine(url)` — `sqlite:///…`
   default; `postgresql+psycopg://…` when `XNAT_REGISTRY_PG_DSN` set.
2. Refactor `Registry` to execute via the Core engine + `Table` inserts with dialect-appropriate
   upsert (`sqlite`/`postgresql` `on_conflict_do_update` via `insert(...).on_conflict_do_update`).
   Public method signatures + `FriendlyError` wrapping preserved.
3. `migrate_sqlite_to_pg(src_path, pg_url) -> MigrationReport`: copy table-by-table, parity-assert,
   rollback on mismatch (mirrors `migrate_from_configtables`).
4. Tests: parametrize the 009 registry contract over `sqlite` always, `postgres` when DSN present;
   `test_011_pg_parity.py` marked `pg` (and `slow`), skipped offline.

### US3 — local-XNAT boot (P3)
1. `boot_and_verify_local_xnat.sh`: read target host from config/env; **refuse** unless
   `localhost`/`127.0.0.1` → exit non-zero, start nothing. Detect running XNAT (port probe) → skip
   boot; else `docker compose up -d` the localhost stack. Ordered probes: up → auth → project-list →
   synthetic round-trip (reuse existing synthetic fixtures). Print per-probe ✓/✗; final HEALTHY or
   non-zero naming the first failed probe.
2. `test_011_boot_verify.py`: drive the script's host-guard + idempotency + probe-fail messaging with
   a stubbed/mocked XNAT (no real container needed for the unit lane; full boot is the opt-in lane).

## Risks & mitigations

- **R1 Registry refactor breaks 992/1101 green suite** → keep public API identical; land US2 behind
  the same engine seam; run the full 009 suite against the refactored SQLite path before any PG work.
- **R2 New deps unavailable offline** (sqlalchemy/cryptography/psycopg) → lazy-import; crosswalk
  encryption + PG are opt-in; default lane never imports them. Add to `requirements.txt` with notes.
- **R3 No Postgres in CI** → contract test skips without `XNAT_REGISTRY_PG_DSN`; correctness proven on
  SQLite via the shared Core schema; PG asserted in the opt-in lane only.
- **R4 Secret leakage via argv/logs** → passphrase/salt only via env or prompt, never argv; scripts
  echo fingerprints not values; tests grep output to assert absence.
- **R5 Plaintext→encrypted crosswalk migration data-loss** → format-versioned envelope, read-legacy-
  once then re-seal; round-trip test + a "wrong key fails closed" test.

## Definition of done

- US1: provision idempotent + no-clobber; crosswalk AEAD round-trips, plaintext-grep fails, wrong key
  fails; live-shaped JSON migrates parity-clean, corrupt-row rolls back, source archived; zero secret
  in output. US2: 009 contract green on SQLite via Core; PG lane green when DSN present; SQLite→PG
  parity. US3: localhost-refuse, idempotent, probe-fail names cause. Full default suite stays green.
