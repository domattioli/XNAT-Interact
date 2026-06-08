# Implementation Plan: Data Identity & Deduplication (009)

**Branch**: `claude/codebase-improvement-plan-sGjxz` | **Date**: 2026-06-08 | **Spec**: [spec.md](spec.md)

## Summary

Correct the ingest identity model and replace the JSON pseudo-database with a SQLite registry,
implementing the content-authoritative + layered-dedup design in `docs/DATA_MODEL.md`. Behavior-
correcting (fixes wrong UID handling, dedup, de-id leak) and substrate-changing (registry). No new
user-facing capability beyond the human dedup-evidence decision. Advanced pixel de-id, Postgres,
and the sibling tracks are out of scope.

## Technical Context

**Language**: Python 3.11. **New runtime dep**: none — `sqlite3` is stdlib; `hashlib`/`hmac`
stdlib. Perceptual hash reuses existing `cv2`/`numpy` (no new dep; `imagehash` optional later).
**Testing**: pytest, offline (FakeXNAT/FakeGateway); data-integrity cases promotable to the
`RUN_XNAT_DUAL=1` live lane. **Reuses**: `src/services/xnat_gateway.py`, `xnat_conventions.py`,
`tests/fakes/fake_xnat.py`, the existing suite as the regression net, `tests/synthetic_data.py`
(seed-set dedup fixtures), `docs/DATA_MODEL.md` + `docs/METADATA.md` as the spec.

## Constitution Check

- **I PHI**: strengthens it — patient destroyed, surgeon pseudonymized, `Old_StudyDate` stripped,
  dates hashed. ✓
- **II Fail-soft**: registry-absent/locked and dedup-uncertain paths raise `FriendlyError`. ✓
- **III Skill floor**: invisible except the human dedup-evidence decision (strictly additive). ✓
- **IV Offline-testable**: SQLite is a local file; content hashing is pure; FakeGateway covers the
  XNAT side; seed-set fixtures cover dedup. ✓
- **V Config/secrets**: the keyed-pseudonym salt + crosswalk are librarian-only, never logged or
  committed. ✓
- **VI Integrity/efficiency**: `UNIQUE`-indexed O(1) dedup membership; transactional writes replace
  the TOCTOU fingerprint guard. ✓

## Project Structure

```text
src/services/
├── identity.py          # NEW: content hashing (sha256 PixelData), keyed pseudonym (HMAC),
│                        #   date/device dedup hash, UID-corroboration helpers
├── registry.py          # NEW: SQLite registry (schema, UNIQUE dedup index, transactions,
│                        #   migration from ConfigTables JSON); separate crosswalk store iface
├── dedup.py             # NEW: layered dedup engine — image + case keys, set-algebra
│                        #   (exact/subset/superset/partial/disjoint), evidence package
└── deidentify.py        # MODIFY: strip Old_StudyDate after dedup-hash capture (de-id leak)

src/
├── xnat_experiment_data.py   # MODIFY: stop UID collapse (FR-001/002), unique SOPInstanceUID,
│                             #   preserve Study/Series UID, route dedup + no-empty-shell invariant,
│                             #   user-selectable scan (FR-014)
├── xnat_resource_data.py     # MODIFY: surgeon pseudonym at intake (FR-011); patient destroy (FR-012)
└── utilities.py              # MODIFY: ConfigTables -> registry shim (read path), migration entry

tests/
├── test_identity.py          # NEW: content hash determinism, pseudonym stability/irreversibility
├── test_registry.py          # NEW: schema, UNIQUE membership, transactions, JSON migration
├── test_dedup.py             # NEW: seed-set algebra (exact/subset/superset/partial/disjoint)
├── test_dedup_evidence.py    # NEW: evidence package + no auto-merge + keep-shots + no-empty-shell
└── contract/...              # EXTEND: data-integrity cases on the RUN_XNAT_DUAL live lane
```

## Approach (incremental, suite green at every commit)

**Stage 1 — Identity primitives (no behavior change yet).**
1. `src/services/identity.py`: `image_content_hash(ds)` (`sha256` of raw `PixelData`),
   `surgeon_pseudonym(hawkid, salt)` (`HMAC`), `case_date_hash(date, time, device, salt)`,
   UID-corroboration helpers. Pure, fully offline-tested. `test_identity.py`.

**Stage 2 — SQLite registry + migration (substrate).**
2. `src/services/registry.py`: schema (`surgeons`, `raters`, `cases`, `image_hashes`
   `content_hash UNIQUE`, `audit_log`), transactional CRUD, `exists(content_hash)` O(1), a
   separate crosswalk-store interface (file path injected, librarian-only). Migration importer
   from ConfigTables JSON. `test_registry.py`.
3. `utilities.py`: thin read-path shim so existing `ConfigTables` callers resolve through the
   registry; keep public surface; migration entry point. Suite green.

**Stage 3 — Dedup engine.**
4. `src/services/dedup.py`: image-level (content hash + UID corroborant + perceptual flag) and
   case-level (content-hash set algebra → exact/subset/superset/partial/disjoint) + evidence
   package builder. Never auto-acts. `test_dedup.py`, `test_dedup_evidence.py` with seed-set
   fixtures (per DATA_MODEL §5).

**Stage 4 — Wire identity into ingest (the H1/UID fix).**
5. `xnat_experiment_data.py` metadata-mining block: remove the per-frame UID collapse; assign
   **unique** `SOPInstanceUID`; **preserve** original Study/Series/SOP UIDs as corroborating
   metadata; capture the date dedup-hash then have `deidentify.py` **strip `Old_StudyDate`**;
   route ingest through the dedup engine; enforce the **no-empty-shell** invariant; make `scan`
   user-selectable.
6. `deidentify.py`: strip `Old_StudyDate` (and any readable original date) post-hash.

**Stage 5 — Pseudonym identity + versioning.**
7. `xnat_resource_data.py`: derive surgeon pseudonym at intake; ensure patient identifiers are
   destroyed; write pseudonym (not name) into experiment attribution + registry.
8. Keep-all monotonic versioning for derived/assessor data (extend the annotation pattern).

**Stage 6 — Verify.**
9. Full offline suite + `scripts/simulate_e2e.py` green; SC-001..SC-006 checks; promote the
   data-integrity dedup/identity cases to the `RUN_XNAT_DUAL=1` live lane; byte-diff representative
   writes; update `specs/README.md` + `DATA_MODEL.md` status.

## Dependency graph

- Stage 1 (identity) BLOCKS Stages 2–5 (everyone needs the hashing/pseudonym primitives).
- Stage 2 (registry) BLOCKS Stage 3 (dedup needs the index) and Stage 5 (pseudonym storage).
- Stage 3 (dedup) BLOCKS Stage 4 (ingest routes through it).
- Stages 4 + 5 both edit `xnat_experiment_data.py` / `xnat_resource_data.py` — **serialize** them
  (same files) to avoid churn; Stage 4 before Stage 5.
- Stage 6 last.

## Coding dispatch

Per DomI policy, implementation = subagent; tier by complexity:
- **Stages 1, 2, 3** (new pure modules + schema + algebra) — **sonnet** (design-heavy, but
  self-contained + offline-testable).
- **Stages 4, 5** (surgery on the shared `xnat_experiment_data.py` metadata block) — **sonnet**
  (highest risk; verify-first; the existing suite + dual-run are the net).
- **Stage 6** (verify/docs) — **haiku** for the doc/status edits; orchestrator owns the live run.
- Orchestrator: plan, review each commit, keep suite green, own live verification.

## Risks & Mitigations

- *Metadata-block surgery breaks ingest* → one concern per commit; suite + the seed-set dedup
  tests + live dual-run are the net; verify-first (much of the audit's "bugs" were already fixed).
- *Registry migration loses data* → migrate into a copy, assert row-for-row parity vs the JSON
  before cutover; keep the JSON readable as fallback for one release.
- *Pseudonym salt handling* → salt injected from a librarian-only path, never defaulted, never
  logged; tests use a throwaway salt.
- *Dedup false positives on perceptual hash* → perceptual hash is advisory-only (flag), never a
  gate; authoritative path is content sha256.
- *SQLite concurrency ceiling* → acknowledged; true multi-writer is the Postgres migration (#34),
  out of scope here.

## Out of scope (deferred)

- Advanced automated pixel-PHI de-id (DATA_MODEL §4.2) — separate feature.
- Postgres migration (#34).
- Arthroscopy / simulation sibling tracks.
- Modeling multi-run sessions as distinct scans (only the user-selectable param + SeriesUID
  preservation are in scope).
