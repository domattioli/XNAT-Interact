# Session Handoff — domattioli/XNAT-Interact · development@d7b5d9b · 2026-06-09

**Task:** Backlog items 2–4 → speckit specify/clarify/plan/tasks/analyze → implement feature
`011-operational-hardening` (009 deploy + Postgres #34 + local-XNAT boot) → handoff + introspect.
**Phase:** 011 BUILT + verified. **Progress:** complete — 3 user stories, default lane green.
**Branch:** development (pushed). **Tool failures:** 0 push rejections.
**Outcome:** complete.

## What shipped (`011-operational-hardening`)

| Story | Tasks | What | Tests |
|---|---|---|---|
| US1 deploy (P1) | T001–T010 | `crosswalk_crypto.py` (AES-256-GCM + scrypt envelope); `CrosswalkStore` opt-in encryption w/ legacy auto-upgrade; `provision_identity_salt.sh` (CSPRNG/0600/no-clobber); `deploy_009.py` migrate+audit+archive; `cli/deploy_009.py` | 14 (crypto round-trip, plaintext-grep-fail, wrong-key-fail, parity+rollback, archive, no-secret-in-output) |
| US2 postgres #34 (P2) | T011–T016 | `registry_backend.py` (SQLAlchemy Core schema mirror + `CoreRegistry` + `make_engine` + `migrate_sqlite_to_pg`); additive DSN selector in `registry.py` (sqlite3 default **byte-unchanged**) | contract parametrized sqlite3 / sqlite-core / postgres(skip w/o DSN); offline sqlite→core migrate parity |
| US3 boot (P3) | T017–T019 | `boot_and_verify_local_xnat.sh` (localhost-guard, idempotent, ordered probes up→auth→project-list→synthetic-roundtrip, HEALTHY/FAILED) | 12 (non-localhost refuse, idempotent, each probe-fail named) |

**Suite:** 1155 passed / 14 skipped (pg, no DSN) / 12 deselected / 7 xfailed / **0 failed** (was 1101 →
+54). 009 + 010 suites stayed green — additive-only wiring (F1/F6 regression gate T016 + T020).

## Clarifications resolved (this session)
- Crosswalk key → **passphrase + scrypt** (headless-safe, no infra).
- Postgres → **SQLAlchemy Core**, but analyze **F6 narrowed it to additive** (keep sqlite3 default;
  Core/PG only behind `XNAT_REGISTRY_PG_DSN`) — avoids making SQLAlchemy a hard registry dep.
- boot-verify home → **repo script** (`scripts/`), DomI-skill extraction deferred.
- Salt rotation → **out of scope** (manual re-provision only).

## What worked (top 5)
1. **Analyze caught the real scope risk before any code.** F6 turned US2 from a registry *rewrite*
   (would have made SQLAlchemy a hard import → broken the bare-env default lane + risked the 992 suite)
   into an *additive* backend behind a DSN selector. The spec→plan→analyze gate paid for itself.
2. **Parallelized non-overlapping stories.** US1 (edits `registry.py`) + US3 (only new files) ran
   concurrently; US2 (also edits `registry.py`) was held until US1 committed → zero merge conflicts on
   the shared file. File-overlap analysis, not guesswork, drove the schedule.
3. **Verify-first beat narration drift AGAIN (6th+ session).** Both the US1 and US2 subagents claimed
   their new files "already existed / were pre-committed"; `git status` showed them untracked. Trusted
   git, not prose.
4. **Refused an unverified PHI-safety guarantee.** US1's crypto tests SKIPPED (env shipped
   `cryptography` with a broken `cffi` → Rust panic that bypassed `try/except`). Rather than accept the
   skip, installed `cffi`, pinned it, re-ran → FR-003 (plaintext-grep-fail, wrong-key-fail) proven, not
   assumed. The whole point of the feature is that guarantee.
5. **Review caught a credential leak the tests didn't.** A `FriendlyError` interpolated the raw
   `XNAT_REGISTRY_PG_DSN` (which can carry a password) into a user-facing/loggable message. Redacted to
   the env-var name. Code review of the diff, not the green suite, found it.

## What didn't / friction
1. **Env defect (`cffi` missing) masqueraded as a code skip.** A subagent reasonably skip-guarded the
   crypto module when `cryptography` panicked — but a skipped PHI-safety test is a silent FN risk. The
   panic bypassed Python exception handling (Rust `pyo3` abort), so a normal `importorskip` wouldn't
   even catch it; the subagent had to use a subprocess probe. Lesson: a *skip* on a safety-critical
   test is not green — chase the env fix.
2. **Subagent narration drift persists** ("pre-existing files") — unchanged from the 010 session.
   Process mitigation (trust git) works; the behavior itself recurs.
3. **Parity-fail rollback coverage is partial** — full FK-enforced rollback needs real Postgres; only
   the code path + a count-mismatch raise are exercised offline. Documented, deferred to the `pg` lane.

## Pain → skill table
| Pain | Severity | DomI candidate | Saved-min/session |
|---|---|---|---|
| safety-critical test SKIPS on a broken env dep; skip ≠ green, silent FN risk | high | `verify-safety-tests-ran` (fail the gate if a PHI/security test is skipped, not just on red) | 25 |
| secret (DSN w/ password) interpolated into an error/log message; green suite can't catch it | med | secret-in-error-message lint (extend `no-secrets` to f-strings around known secret env vars) | 15 |
| subagent narration drift ("file already existed") — 6th+ session | low | null (process: trust git, not prose) | 8 |

## Pain corpus (machine-readable)
```yaml
session_id: development@d7b5d9b
repo: domattioli/XNAT-Interact
branch: development
date: 2026-06-09
issue_worked: specs/011-operational-hardening (specify -> clarify -> plan -> tasks -> analyze -> build)
phase: 011-built
outcome: complete
tool_failure_count: 0
worked:
  - "analyze F6 narrowed US2 from registry rewrite to additive DSN-selector backend before any code — saved the bare-env default lane + 992 suite"
  - "parallelized US1+US3 (non-overlapping files); held US2 until US1 committed (shared registry.py) -> zero conflicts"
  - "verify-first caught narration drift on both subagents (claimed pre-existing; git showed untracked)"
  - "refused a skipped PHI-safety test: installed missing cffi, re-ran, proved FR-003 plaintext-grep-fail + wrong-key-fail"
  - "diff review caught raw DSN (password) interpolated into a FriendlyError message; redacted"
didnt_work:
  - "env shipped cryptography w/ broken cffi -> pyo3 Rust panic bypassed try/except; crypto tests skip-guarded (skip != green for safety tests)"
  - "subagent narration drift persists (pre-existing-files claim), 6th+ session"
  - "parity-fail rollback only partially covered offline (needs real Postgres for FK-enforced rollback)"
pain_points:
  - pain: "a safety-critical test (PHI crosswalk encryption) SKIPPED due to a broken env dep; a skip on a security test is a silent false-negative, not a pass"
    frequency: this-session
    severity: high
    evidence: "cryptography 41 + missing _cffi_backend -> pyo3_runtime.PanicException; module skip-guarded; fixed by installing cffi, then 14 tests passed"
    existing_skill_should_have_caught_it: none
    missing_skill_would_have_prevented_it: "verify-safety-tests-ran: a release gate that FAILS when a test tagged phi/security is skipped, forcing the env fix instead of a silent skip"
    domi_issue: null
    saved_time_estimate_min: 25
  - pain: "a secret (Postgres DSN with embedded password) was interpolated into a FriendlyError/log message; the green test suite cannot detect this"
    frequency: this-session
    severity: med
    evidence: "registry.py backend-init error used f'...({_pg_dsn!r})...'; caught in diff review, redacted to env-var name only"
    existing_skill_should_have_caught_it: "no-secrets lane (only scans committed file diffs for secret VALUES, not f-strings that would emit env secrets at runtime)"
    missing_skill_would_have_prevented_it: "extend no-secrets to flag f-string/format interpolation of known secret-bearing env vars (DSN/PASSWORD/TOKEN) into error/log strings"
    domi_issue: null
    saved_time_estimate_min: 15
actions_taken:
  votes_cast: []
  new_requests_filed: []
  introspect_design_proposal_on_9: false
introspection_meta:
  what_worked: "spec->analyze gate (F6) prevented a scope blowup; parallel non-overlapping dispatch; verify-first; refusing a skipped safety test; diff-review secret catch"
  what_was_hard: "a broken env crypto dep that panicked past try/except and disguised a safety-test skip as benign"
```

## Definition-of-done (011 SC-001..SC-005)
- SC-001 plaintext-crosswalk grep empty + salt 0 bytes in logs — ✓ (crypto + deploy no-leak tests)
- SC-002 100% migration parity + corrupt rollback — ✓ offline (parity); rollback code-asserted (full FK needs PG)
- SC-003 contract identical sqlite/core (+ pg w/ DSN) + offline migrate parity — ✓
- SC-004 boot HEALTHY one-command, refuse non-localhost, name failed probe — ✓ (12 tests)
- SC-005 no PHI/HawkID/salt/passphrase/DSN-password in any artifact — ✓ (grep asserts + review redaction)

## Next session
- **Run the `pg` lane against a real Postgres** (`XNAT_REGISTRY_PG_DSN`): proves FR-008/009/010 + the
  FK-enforced parity-fail rollback that's only code-asserted offline.
- **DomI skill proposals** (vote/file via `request-from-domi`): `verify-safety-tests-ran` (fail gate on
  a skipped phi/security test) + extend `no-secrets` to runtime f-string secret interpolation.
- **Operational deploy**: actually run `deploy_009.py` against the live ConfigTables with a provisioned
  salt + passphrase (operator/IRB step); wire the encrypted `CrosswalkStore` into the librarian path.
- **CI**: add an opt-in `pg` lane (spins a Postgres service container) running `test_011_pg_parity.py`.
- **Still open from prior sessions**: vendor the CRAFT ONNX model (010 T026); dual-010 renumber.
