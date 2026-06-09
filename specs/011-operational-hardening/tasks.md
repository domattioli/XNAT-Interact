# Tasks: Operational Hardening (011)

**Spec**: [`spec.md`](spec.md) · **Plan**: [`plan.md`](plan.md) · **Branch**: `development`

Legend: `[P]` parallelizable (distinct files, no ordering dep). Stories are independently shippable;
build in priority order US1 → US2 → US3. Each story ends green before the next starts.

## Stage 0 — Setup

- **T001** Add `SQLAlchemy`, `cryptography` (and commented `psycopg[binary]`) to `requirements.txt`
  with lazy-import + opt-in notes (mirror 010 heavy-dep block). `.gitignore`: salt file,
  `*.crosswalk`, `*.archived-*.json`, `registry.db*`.
- **T002** Register pytest marker `pg` in `pytest.ini` (reuse existing `slow`); document the
  `XNAT_REGISTRY_PG_DSN` / `XNAT_CROSSWALK_PASSPHRASE` / salt-file env contract in spec/plan footnote.

## Stage 1 — US1: 009 deploy (P1) 🎯 MVP

- **T003** [P] `tests/test_011_crosswalk_crypto.py`: AEAD seal/open round-trip; plaintext-grep of a
  known HawkID against the sealed blob FAILS; wrong-passphrase → decrypt error (fail-closed);
  legacy-plaintext detected + re-sealed once. (write first — red)
- **T004** `src/services/crosswalk_crypto.py`: `derive_key(passphrase, kdf_salt)` (scrypt, pinned
  params), `seal(dict,key)->bytes`, `open_envelope(bytes,key)->dict`; magic-header envelope
  `magic||kdf_salt||nonce||ct`; `is_legacy_plaintext(blob)->bool`.
- **T005** Extend `CrosswalkStore` (`registry.py`): opt-in `passphrase`/`key` ctor arg; `_load`/`_save`
  route through the envelope when keyed; auto-detect + one-time re-seal of legacy plaintext; default
  (unkeyed) path byte-unchanged. Make T003 green.
- **T006** [P] `scripts/provision_identity_salt.sh`: 32-byte CSPRNG → hex → `0600` file; no-clobber
  refuse; never echo value; print path + sha256 fingerprint prefix. `bash -n` clean.
- **T007** `src/services/deploy_009.py`: `deploy(config_json_path, registry_path, *, archive=True)` →
  `migrate_from_configtables` (parity+rollback) → `record_audit` (summary, no PHI) → archive source to
  `*.archived-<ts>.json`. Returns `MigrationReport`. Parity-fail → registry untouched + raise.
- **T008** `src/cli/deploy_009.py` (or `scripts/deploy_009.sh` wrapper): argparse entrypoint; passphrase
  via env/prompt only (never argv); HEALTHY/FAILED exit codes.
- **T009** [P] `tests/test_011_deploy_009.py`: provision idempotent/no-clobber; synthetic live-shaped
  JSON migrates with row parity; corrupted-row trial rolls back byte-identical; source archived not
  deleted; assert salt + passphrase absent from captured stdout/stderr.
- **T010** Run US1 tests + the existing 009 suite (`test_registry.py`, `test_identity.py`,
  `test_configtables_registry_writethrough.py`) — confirm green (no regression from CrosswalkStore edit).

## Stage 2 — US2: Postgres path / #34 (P2)

- **T011** [P] `tests/test_011_registry_backend.py`: the 009 registry contract (upsert each table,
  UNIQUE dedup reject, `image_exists`, `case_image_hashes`, `record_audit`) parametrized over backend
  = `sqlite` (always) and `postgres` (skip if no `XNAT_REGISTRY_PG_DSN`). (red)
- **T012** `src/services/registry_backend.py`: SQLAlchemy `MetaData` + `Table` defs mirroring
  `_SCHEMA_SQL` (UNIQUE content_hash, FKs, audit autoincrement); `make_engine(url)` sqlite-default /
  postgres-opt-in; dialect-aware `upsert(...)` helper (`on_conflict_do_update`).
- **T013** (per analyze F6 — **additive, NOT a rewrite**) Keep `registry.py`'s raw-`sqlite3` default
  path byte-unchanged. Add an opt-in Core/Postgres path engaged ONLY when `XNAT_REGISTRY_PG_DSN` is
  set: a `Registry` backend selector routes to `registry_backend` for PG, else the existing sqlite3
  code. **Public signatures + FriendlyError wrapping preserved.** SQLAlchemy lazy-imported (absent →
  sqlite3 path, no crash). Make T011 sqlite params green (unchanged path) + pg params green when DSN.
- **T014** `migrate_sqlite_to_pg(src_path, pg_url) -> MigrationReport`: table-by-table copy, parity
  assert, rollback on mismatch.
- **T015** [P] `tests/test_011_pg_parity.py` [`pg`,`slow`]: populate SQLite → migrate → row-for-row
  parity; UNIQUE enforced on PG. Skips offline.
- **T016** Run full 009 + 010 default suite — confirm the Registry refactor kept ~1101 green
  (additive, API-stable). HARD GATE before Stage 3.

## Stage 3 — US3: local-XNAT boot (P3)

- **T017** [P] `tests/test_011_boot_verify.py`: non-localhost host → refuse + non-zero + nothing
  started; already-running → skip-boot + verify; failing probe → non-zero naming the probe;
  idempotent. (mock/stub XNAT — no real container in unit lane)
- **T018** `scripts/boot_and_verify_local_xnat.sh`: host-guard (localhost/127.0.0.1 only); detect
  running (port probe) else `docker compose up -d`; ordered probes up→auth→project-list→synthetic
  round-trip; per-probe ✓/✗; final HEALTHY or non-zero w/ failed-probe name. `bash -n` + shellcheck.
- **T019** Make T017 green (drive the script's guard/idempotency/probe-fail paths via mocked endpoints).

## Stage 4 — Validation + docs

- **T020** Bounded single full-suite run, default lane (`-m "not slow and not pg"`): confirm green +
  no regression. Spawn no nested pytest loops; reap any orphaned children after (010 lesson).
- **T021** [P] `specs/011-operational-hardening/analyze.md` cross-check: every FR↔task↔test mapped;
  record findings.
- **T022** [P] Update `docs/DATA_MODEL.md` §3.2 (crosswalk now encrypted-at-rest) + §registry
  (SQLite/Postgres backend); `specs/README.md` row 11 → ✅ Built; note #34 advanced from deferred.
- **T023** Handoff + introspection (corpus `docs/introspections/<id>.md`; route telemetry to PR/corpus,
  not a new tracking issue).

## Build status

| Stage | Tasks | Status |
|---|---|---|
| 0 setup | T001–T002 | ⏳ |
| 1 US1 deploy | T003–T010 | ⏳ |
| 2 US2 postgres | T011–T016 | ⏳ |
| 3 US3 boot | T017–T019 | ⏳ |
| 4 validation+docs | T020–T023 | ⏳ |
