# Implementation Plan: Derived-Data Hierarchy (012)

**Spec**: [`spec.md`](spec.md) · **Status**: Draft (plan only — not yet built) · **Created**: 2026-06-12

## Summary

Generalize the one built derived-data kind (005 annotations) into a provenance-scoped placement
layer that every future derived artifact (consensus, metrics, MTurk stills #4, ML splits, model
predictions) shares without core edits. The load-bearing change is **per-producer append-only
manifest shards** replacing 005's single rebuilt manifest — one move that fixes the live-confirmed
#33-M6 version orphaning and removes the manifest write race the concurrency lane exposed (H4).
The gateway already encodes the three stock-XNAT addressing constraints found live
(GAP-001/002/003); this feature makes them contractual for derived data. Behavior-extending +
one breaking storage-format change (migrated idempotently), not a new user surface.

## Technical Context

- **Substrate**: 005's annotation store (RLE blobs + `manifest.json` under an `ANNOTATIONS`
  assessor resource) is live and green; 009's content-hash identity is the linkage key.
- **Live ground truth (PR #38)**: assessor file I/O must be accession-ID-addressed (GAP-001);
  file PUTs label-addressed not numeric-resource-addressed (GAP-003); discovery via global
  `/data/experiments?project=` not scoped listings (GAP-002). All three already in
  `PyxnatGateway`; FakeXNAT extended to match each.
- **Seam**: `app/logic/annotations.py` + `src/annotations/io_xnat.py` own annotation I/O today;
  this feature adds a `src/services/derived/` placement layer they delegate to. No Streamlit in
  logic (offline-test rule).
- **Dual-run**: every storage/retrieval behavior lands a FakeXNAT test AND a `RUN_XNAT_DUAL=1`
  live-lane test — the campaign's fake-green lesson is a hard requirement here.

## Constitution Check

- **I PHI**: DICOM exports pass the same de-id + human pixel gate as source uploads; linkage
  blocks carry content hashes, never patient identifiers; producer IDs PHI-validated. ✓
- **II Fail-soft**: unknown family/scope, unresolvable producer, corrupt shard, parent-missing →
  `FriendlyError`; no empty assessor/resource shells on failure (extends #32-Q3 + #38 cleanup). ✓
- **III Skill floor**: invisible to the novice path — derived placement is an API; the guided UI
  only gains a read-only "view derived data" panel (additive). ✓
- **IV Offline-testable**: shards are server-side JSON; linkage + input-set hashing are pure;
  FakeXNAT covers the assessor surface; placement decisions are table-driven. ✓
- **V Config/secrets**: no new secrets; producer tokens are opaque + validated, never logged. ✓
- **VI Integrity/efficiency**: sharded append-only manifests = no shared-file write contention
  (each producer owns its file); union-at-read index; content-hash linkage is O(1) lookup. ✓

## Project Structure

```text
src/services/derived/
├── __init__.py
├── placement.py        # NEW: family/scope → location; FR-001; closed DerivationFamily enum
├── linkage.py          # NEW: LinkageBlock build/resolve (content-hash authoritative, UID corrob.);
│                        #   InputSetHash for second-order provenance (FR-002, FR-007)
├── manifest_shard.py   # NEW: per-producer append-only shard read/write; union index (FR-003)
├── producer.py         # NEW: Producer model (human|tool|model) + opaque-id validation (FR-005)
└── dicom_export.py     # NEW: derived-still + DICOM-SEG writers w/ SourceImageSequence (FR-008)

src/annotations/
└── io_xnat.py          # MODIFY: route writes through manifest_shard (FR-010 migration shim)

app/logic/
└── derived.py          # NEW: list_derived / by-frame / by-producer retrieval (FR-006), streamlit-free

app/guided/
└── browse_view.py      # MODIFY: read-only derived-data panel reads app/logic/derived (additive)

src/services/xnat_gateway.py  # NO CHANGE expected (GAP-001/002/003 already encoded); add only if a
                              #   new addressing shape appears in a live gate

tests/
├── test_derived_placement.py     # NEW: scope→location, unknown-family FriendlyError (FR-001)
├── test_derived_linkage.py       # NEW: content-hash resolve, UID corroborate, input-set hash
├── test_manifest_shard.py        # NEW: append-only, union index, M6 regression (SC-002)
├── test_derived_producer.py      # NEW: opaque-id validation incl model producers
├── test_dicom_export.py          # NEW: SourceImageSequence/SEG round-trip + PHI gate (SC-005)
├── test_derived_retrieval.py     # NEW: by-case/family/frame/producer (FR-006)
├── test_005_migration.py         # NEW: idempotent migration of current-format store (SC-006, FR-010)
└── contract/test_derived_dual.py # NEW: RUN_XNAT_DUAL live lane — concurrency (SC-003), retrieval (SC-004)
```

## Approach (incremental, suite green at every commit)

1. **Stage 1 — linkage + producer primitives** (pure, no I/O): content-hash/UID linkage block,
   input-set hash, producer validation. Imported nowhere yet.
2. **Stage 2 — manifest shards** (the keystone): per-producer append-only file + union-at-read
   index; M6 regression test is the gate. Still behind the existing io_xnat surface.
3. **Stage 3 — placement layer**: family/scope → location table; no-empty-shell + parent-exists
   guards; FakeXNAT + a single live smoke.
4. **Stage 4 — migrate 005** onto shards with an idempotent one-shot that synthesizes shard
   entries for versions already orphaned on the server; 005's existing tests stay green.
5. **Stage 5 — retrieval API** (`app/logic/derived`): the four query shapes, FakeXNAT + dual-run.
6. **Stage 6 — DICOM exports**: derived still + DICOM-SEG with derivation metadata through the PHI
   gate; importer round-trip.
7. **Stage 7 — concurrency + corpus**: dual-run N-producer concurrent append (SC-003) and the
   `ML_SPLITS` leakage ledger (FR-011); guided read-only panel.

## Dependency graph

```
S1 linkage/producer ─┬─> S2 shards ─> S3 placement ─┬─> S4 005-migration
                     │                              ├─> S5 retrieval ─> S7 concurrency+corpus
                     └──────────────────────────────┴─> S6 dicom-export
```

## Coding dispatch

Per CLAUDE.md Haiku-default. `[S]` = Sonnet (cross-cutting / live-gated concurrency), `[H]` = Haiku
(bounded single-module). Every live gate (`RUN_XNAT_DUAL=1`) is `[S]` with mandatory verbatim
output — the campaign showed stub-green claims must be re-verified against a booted XNAT.

## Risks & Mitigations

- **R1 Fake-green recurrence** — a shard/retrieval test passes on FakeXNAT but fails live. *Mit*:
  every storage behavior is dual-tested; FakeXNAT extended to match the real assessor surface in
  the same commit it diverges.
- **R2 Migration corrupts 005 data** — *Mit*: migration is read-only-then-additive (synthesizes
  missing shard entries; never deletes a blob); idempotent; runs against a live-seeded store in
  the gate (SC-006).
- **R3 Concurrency still races** — sharding removes the shared-file write, but assessor creation
  itself may race. *Mit*: SC-003 is a live N-producer gate; if assessor-create races, fall back to
  pre-create-then-populate with the #38 cleanup-on-failure invariant.
- **R4 DICOM-SEG interop drift across pydicom versions** — *Mit*: round-trip test pins the SEG
  shape; export validated by `pydicom` + the importer in one gate (SC-005).

## Out of scope (deferred)

- Aggregation algorithms (005 seam owns STAPLE/majority/mean).
- Postgres registry (#34) — shards are server-side JSON regardless of registry backend.
- Pixel-PHI automation (010).
- Retention/pruning of superseded versions (append-only now; operator-gated pruning is future).
- Cross-case / longitudinal composite derived objects.
