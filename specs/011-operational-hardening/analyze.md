# Analyze: Operational Hardening (011)

Cross-artifact consistency check (spec ↔ plan ↔ tasks ↔ existing code) before implementation.

## FR → Task → Test coverage

| FR | Requirement | Task(s) | Test |
|---|---|---|---|
| FR-001 | provision CSPRNG salt, 0600, via load_identity_salt | T006, T008 | T009 (idempotent/no-clobber) |
| FR-002 | salt never logged/argv/repo | T006, T008 | T009 (grep-absent stdout/stderr) |
| FR-003 | crosswalk AEAD-at-rest, scrypt key | T004, T005 | T003 (round-trip, plaintext-grep-fail, wrong-key) |
| FR-004 | migrate live JSON, parity, audit, archive | T007, T008 | T009 (parity + archive) |
| FR-005 | parity fail → full rollback | T007 (reuses 009) | T009 (corrupt-row byte-identical) |
| FR-006 | post-deploy reads/writes target registry | T007 (archive retires JSON) | T010 (009 writethrough green) |
| FR-007 | backend selector, API unchanged | T012, T013 | T011 (sqlite params) |
| FR-008 | 009 schema+UNIQUE on Postgres | T012 | T011/T015 (dup reject on pg) |
| FR-009 | SQLite→PG move parity+reversible | T014 | T015 |
| FR-010 | 009 contract green both backends | T013, T016 | T011 (parametrized) |
| FR-011 | localhost boot + ordered probes + verdict | T018 | T017 |
| FR-012 | refuse non-localhost, start nothing | T018 | T017 (refuse case) |
| FR-013 | idempotent, exit non-zero names probe | T018 | T017 |
| FR-014 | synthetic-only round-trip | T018 | T017 (mocked) |

| SC | Task/test |
|---|---|
| SC-001 plaintext-crosswalk grep empty + salt 0-bytes in logs | T003 + T009 |
| SC-002 100% migration parity + corrupt rollback | T009 |
| SC-003 contract identical sqlite/pg + move parity | T011 + T015 |
| SC-004 boot HEALTHY one-command, refuse non-localhost, name probe | T017 |
| SC-005 no PHI/HawkID/salt/cred in any artifact | T003, T006, T009 (grep asserts) |

**Coverage: every FR + SC has a task and a test. No orphans.**

## Findings

- **F1 — Registry refactor is the dominant regression risk.** `Registry`'s public API is consumed by
  `utilities.py:650` (fail-soft sidecar) and the 009 writethrough tests. Mitigation: T013 preserves
  signatures + `FriendlyError` wrapping exactly; T016 is a HARD GATE re-running 009+010 before Stage 3.
  Build US2 strictly additively — do NOT touch `_SCHEMA_SQL` semantics, only the execution substrate.

- **F2 — CrosswalkStore default path MUST stay byte-unchanged.** 009 tests construct
  `CrosswalkStore(path)` with no key and expect plaintext JSON. T005 makes encryption **opt-in** (key/
  passphrase ctor arg); unkeyed = legacy behavior. The 009 suite (T010) is the guard.

- **F3 — Heavy deps (sqlalchemy/cryptography/psycopg) absent offline.** All lazy-imported; default
  test lane must not import them. Crosswalk-crypto + PG tests skip/guard when the dep or DSN is
  missing — mirror the 010 `pytest.importorskip` pattern so `-m "not slow and not pg"` stays green
  on a bare env. **Risk:** if T013 makes SQLAlchemy a *hard* import of `registry.py`, the whole
  existing suite breaks. Decision: `registry_backend` lazy-imports SQLAlchemy; if absent, `Registry`
  falls back to the current raw-`sqlite3` path. → see F6.

- **F4 — `.gitignore` must cover new secret/artifact files** before any test writes them: salt file,
  `*.crosswalk`, `registry.db*`, `*.archived-*.json`. T001. Prevents accidental secret commit.

- **F5 — Passphrase via argv leaks in process table.** FR-002/SC-005 forbid it. T008 takes passphrase
  ONLY from `XNAT_CROSSWALK_PASSPHRASE` env or an interactive no-echo prompt — never a CLI flag.
  Test asserts no passphrase in captured argv/stdout.

- **F6 — Dual-substrate maintenance cost (decision).** Two options for T013: (a) full cutover to
  SQLAlchemy Core (single schema, but makes SQLAlchemy a hard runtime dep of the registry → breaks
  bare-env default lane, violates F3); (b) keep the raw-`sqlite3` path as default, add the Core/PG
  path behind `make_engine` selected only when `XNAT_REGISTRY_PG_DSN` is set. **Choose (b)**: lowest
  regression risk, honors offline default lane, still delivers FR-007..010 (PG behind the selector,
  contract test parametrized). Pure-Core cutover deferred unless a future hard SQLAlchemy dep is
  acceptable. This narrows US2 scope to an *additive* PG backend, not a registry rewrite.

- **F7 — `docker compose` / real XNAT unavailable in unit lane.** T017 mocks endpoints; the real boot
  is the opt-in lane (not run offline). Script still `bash -n`/shellcheck-clean (CI `shell-lint`).

- **F8 — Numbering.** 011 is unambiguous (010 collision was pixel-deid vs web-reachable; 011 free).

## Net adjustment to plan

Per **F6**, US2 is **additive PG backend behind a DSN selector**, NOT a wholesale `Registry` rewrite.
`registry.py` keeps its `sqlite3` default path; `registry_backend.py` + SQLAlchemy engage only when a
Postgres DSN is configured. This preserves the offline default lane (F3) and the 992/1101 green suite
(F1) while still satisfying FR-007..FR-010. tasks.md T013 reinterpreted accordingly (no behavior
change to the SQLite path).

**Gate to implement: GREEN** — coverage complete, risks mitigated, scope narrowed per F6.
