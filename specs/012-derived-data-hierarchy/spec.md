# Feature Specification: Derived-Data Hierarchy (012)

**Feature Branch**: `012-derived-data-hierarchy`
**Created**: 2026-06-12
**Status**: Draft (spec only — not yet planned/built)
**Input**: [`docs/XNAT_MODEL.md`](../../docs/XNAT_MODEL.md) (platform model + seam),
[`docs/DATA_MODEL.md`](../../docs/DATA_MODEL.md) (science + privacy model), spec 005
(annotations layer — the first derived-data kind), spec 009 (content-authoritative identity),
and the 2026-06-12 live-XNAT campaign findings (PR #38: GAP-001/002/003 platform behaviors,
#33-M6 manifest orphaning confirmed live, H4 concurrency guard misses live contention).
Related issues: [#4](https://github.com/domattioli/XNAT-Interact/issues/4) (derived
OtherDicomSession for MTurk inputs), [#24](https://github.com/domattioli/XNAT-Interact/issues/24)
(trauma batch), [#32](https://github.com/domattioli/XNAT-Interact/issues/32) (identity/dedup),
[#39](https://github.com/domattioli/XNAT-Interact/issues/39) (mixed session types).

## Overview

The repo now has *one* derived-data kind built (005: annotations as RLE blobs + manifest on an
`ANNOTATIONS` assessor resource) and several more queued by the science: consensus masks,
frame/case skill metrics, derived DICOM stills for MTurk (#4), ML split membership, model
predictions. Each was headed for its own ad-hoc resource label and its own manifest convention —
the path to an unqueryable junk drawer.

This feature specifies the **general hierarchy** all derived data follows: **where an artifact
lives is determined by its provenance scope** (frame / scan / case / corpus), **what it links to
is determined by content identity** (sha256 of raw `PixelData`, per 009 — DICOM UIDs
corroborate, never authoritative), and **how it accumulates is append-only and sharded per
producer** (fixing #33-M6 manifest orphaning and sidestepping the live-confirmed manifest
write race in one move). DICOM-typed derived artifacts carry standard DICOM derivation metadata
(`SourceImageSequence`, `DerivationDescription`, `ReferencedSeriesSequence`) so they remain
legible to any DICOM tooling, not just ours.

It is an architecture spec: it constrains 005's existing implementation (one breaking change:
manifest sharding), defines the container for #4, and gives every future derived kind a slot
that requires **no core change** — a new kind is a resource label + codec + linkage block.

## Clarifications (design rulings, 2026-06-12)

1. **Assessors are the derived-data container at experiment level** — `xnat:imageAssessorData`,
   one assessor per **derivation family** (ANNOT, CONSENSUS, METRICS, IMAGES), not per run and
   not per producer. Rationale: XNAT's own model treats assessors as "derived/assessment data
   about a session"; families keep assessor count bounded (4) while resources/files scale
   inside. Per-run assessors were rejected (unbounded listing growth; GAP-002 makes scoped
   assessor discovery fragile on stock servers).
2. **Platform addressing constraints are part of this spec**, live-verified on stock XNAT 1.9.3
   (PR #38): assessor file I/O MUST be accession-ID-addressed (label paths 404 — GAP-001); file
   PUTs MUST use label-addressed resource paths, never numeric resource IDs (GAP-003);
   discovery MUST NOT rely on project/subject-scoped listings (GAP-002 — use
   `/data/experiments?project=` or direct addressing). All three are already encoded in
   `PyxnatGateway`; this spec makes them contractual for derived data.
3. **Linkage is content-first.** Every derived artifact carries a **linkage block**:
   `{source_content_hashes: [sha256(raw PixelData)...], study_uid, series_uid, sop_uids,
   case_uid}` — content hashes authoritative (survive re-export, anonymizers, and dedup-merge
   decisions), UIDs corroborating (009 ruling extended to derived data).
4. **Manifests are sharded per producer and append-only.** `manifest__{producer_id}.json`, each
   shard an append-only version ledger; the readable index is the **union of shards computed at
   read time**. Fixes M6 (no version is ever orphaned — confirmed live: v2 upload currently
   makes v1 invisible) and eliminates manifest write contention (two producers never write the
   same file; the live concurrency lane showed the read-modify-write guard misses real
   interleaving — H4).
5. **Producer is a first-class concept**: human annotator, tool, or **model** — all are
   producers with an opaque PHI-validated `producer_id`, a `tool` string, and a `params_hash`
   for algorithmic producers. "Model-as-annotator" means ML predictions enter the same hierarchy
   as human annotations and are aggregated by the same 005 seam.
6. **Second-order derived data records its input set.** Consensus/aggregate artifacts store the
   sha256 of the *sorted list of input artifact hashes*; staleness = recompute the input set,
   compare. No silent recompute; stale is a visible state.
7. **Derived DICOMs are real DICOMs.** Frame stills for MTurk (#4) and DICOM-SEG exports carry
   `SourceImageSequence` (referencing source `SOPInstanceUID`s), `DerivationDescription`,
   `ImageType[0]='DERIVED'`, and inherit the de-identified study context — they must pass the
   same PHI gate as source uploads. DICOM-SEG remains an **interop adapter** (005 ruling);
   the canonical store stays codec blobs + manifests.

## Hierarchy (normative)

```
project/
├─ resources/config, backups                     (existing)
├─ resources/ML_SPLITS/                          corpus scope: split__{name}__v{n}.json
│                                                + leakage ledger keyed by case content-identity
└─ subject/{case_uid}/
   ├─ resources/INTAKE_FORM                      (existing)
   └─ experiment/SOURCE_DATA-{case_uid}          (rf/esv session)
      ├─ scan/0/resources/SRC                    (existing: de-identified source frames)
      └─ assessors (xnat:imageAssessorData; ID-addressed I/O):
         ├─ DERIVED_ANNOT-{case_uid}
         │   ├─ ANNOTATIONS/  ann__{producer}__{type}__v{n}.rle
         │   ├─ MANIFESTS/    manifest__{producer}.json          (append-only shards)
         │   └─ DICOM_SEG/    seg__{producer}__v{n}.dcm          (interop exports)
         ├─ DERIVED_CONSENSUS-{case_uid}
         │   ├─ CONSENSUS/    {method}__{type}__v{n}.rle
         │   └─ MANIFESTS/    manifest__{method}.json            (+ input-set hash)
         ├─ DERIVED_METRICS-{case_uid}
         │   └─ METRICS/      metrics__{producer}__v{n}.json     (frame- and case-level values)
         └─ DERIVED_IMAGES-{case_uid}                            (#4)
             └─ IMAGES/       derived DICOMs w/ SourceImageSequence
```

Scope rule: **frame-scoped** artifacts live in an experiment assessor with per-frame linkage
blocks; **case-scoped** values live in `DERIVED_METRICS` keyed `frame: null`; **corpus-scoped**
artifacts live at project resources. Scan-level `DERIVED` resources are deprecated for new
kinds (download already enumerates them for back-compat).

## User Scenarios & Testing *(mandatory)*

### User Story 1 — One placement rule for any derived artifact (Priority: P1)

A contributor with a new derived kind (e.g., per-frame wire-tip coordinates) consults the scope
rule, picks the family (METRICS), and lands files + manifest shard with a complete linkage
block — touching no core code.

**Independent Test**: offline (FakeXNAT) + dual-run: write a novel artifact kind through the
placement API; it lands under the correct assessor/resource; its linkage block resolves back to
the exact source frames by content hash; `list_derived(case)` reports it without code changes.

**Acceptance Scenarios**:
1. **Given** a frame-scoped artifact, **When** placed, **Then** it lands in the family assessor
   with `source_content_hashes` matching the frames' raw-PixelData sha256s.
2. **Given** a case-scoped metric, **When** placed, **Then** it lands in `DERIVED_METRICS` with
   `frame: null` linkage and the case's content-identity set.
3. **Given** an unknown family name, **When** placed, **Then** FriendlyError naming the four
   families (no silent new assessor).

### User Story 2 — Append without contention or orphaning (Priority: P1)

Two annotators (or an annotator and a model) upload derived artifacts for the same case
concurrently; both land; every prior version of every producer remains discoverable.

**Independent Test**: concurrent lane (real XNAT, `RUN_XNAT_DUAL=1`): N workers append versions
for distinct producers simultaneously → all shards present, union index complete; re-upload by
one producer adds `v(n+1)` to its shard; v1..vn all remain listed (M6 regression).

**Acceptance Scenarios**:
1. **Given** producer A at v1, **When** A uploads v2, **Then** the union index lists v1 AND v2,
   latest-pointer = v2 (M6 fixed; current behavior live-confirmed broken).
2. **Given** producers A and B writing concurrently, **When** both complete, **Then** both
   shards exist intact (no read-modify-write of a shared manifest occurred).
3. **Given** an identical re-upload (same producer/type/version/bytes), **When** placed,
   **Then** idempotent no-op, not a duplicate ledger entry.

### User Story 3 — Retrieval by context (Priority: P1)

A researcher fetches: everything derived for a case; all latest consensus masks of one type
across the cohort; all artifacts referencing one frame (by content hash *or* `SOPInstanceUID`);
full version history for one producer.

**Independent Test**: seed mixed families/producers/versions; each query shape returns exactly
the expected set, addressed only via GAP-safe paths; per-frame lookup by corroborating
`SOPInstanceUID` returns the same set as by content hash when UIDs are intact.

**Acceptance Scenarios**:
1. **Given** a case with artifacts in 3 families, **When** `list_derived(case)`, **Then** all 3
   families enumerated with producer/version/type per artifact (one call, no scoped-listing
   dependence — GAP-002).
2. **Given** a frame content hash, **When** queried, **Then** every artifact whose linkage block
   contains it is returned, across families.
3. **Given** `latest_only=True`, **Then** exactly one version per (producer, type) per case.

### User Story 4 — DICOM-native derived artifacts (Priority: P2)

MTurk needs flat derived stills (#4); collaborators need DICOM-SEG. Exports are standards-valid
DICOM that any viewer attributes to the right source images.

**Independent Test**: export a derived still + a DICOM-SEG from stored annotations →
`ImageType[0]=DERIVED`, `SourceImageSequence`/`ReferencedSeriesSequence` reference preserved
source UIDs, `DerivationDescription` populated, zero PHI tags (same gate as source uploads);
re-import through `dicom_seg.from_dicom_seg` round-trips to an identical payload.

**Acceptance Scenarios**:
1. **Given** stored frame annotations, **When** exported as DICOM-SEG, **Then** one
   `SegmentSequence` entry per producer and referenced instances resolve to source frames.
2. **Given** a derived still for MTurk, **When** generated, **Then** it passes the PHI gate and
   carries the source linkage; uploading it back as *source* data is rejected (it is
   content-hash-known as derived).

### User Story 5 — Second-order derivation with staleness (Priority: P2)

Consensus is recomputed only when its inputs changed, and consumers can see whether a stored
consensus is stale.

**Acceptance Scenarios**:
1. **Given** consensus computed over input-set hash H, **When** a new annotation arrives for
   that frame, **Then** `is_stale(consensus)` is true without recomputation.
2. **Given** unchanged inputs, **When** recompute requested, **Then** no new version is written
   (params + input-set hash identical → idempotent).

### User Story 6 — Corpus artifacts with leakage ledger (Priority: P3)

ML split membership lives at project scope, keyed by case content-identity, so a case merged or
re-imported under a new UID cannot silently cross the train/test boundary (009's leakage goal).

**Acceptance Scenario**: **Given** a case in `split:test`, **When** the same content set is
re-ingested under a different case UID, **Then** the leakage ledger flags the collision at
placement time.

### Edge Cases

- Source frames dedup-merged after derived data exists → linkage survives (content hashes
  unchanged); per-UID lookups degrade gracefully to hash lookups.
- Derived placement for a nonexistent source experiment → refused with parent-existence check
  (#33-H6 class) — never a dangling assessor.
- Upload fails mid-family → no empty assessor/resource shells (extends the 009/#32-Q3 invariant
  and PR #38's cleanup-on-failure to assessor creation).
- Shard JSON corrupt/unparseable → union index reports the shard as quarantined, other
  producers unaffected; FriendlyError names the file.
- Mixed historical sessions (`mrSessionData`, #39) → families attach identically (assessors are
  session-type-agnostic); discovery must not assume rf typing.
- Artifact larger than single-PUT comfort (video-scale) → zip path obeys GAP-003 label
  addressing; partial upload cleaned up.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: A placement API maps (family, scope, payload, linkage block, producer, version) →
  storage location per the normative hierarchy; unknown family/scope → FriendlyError.
- **FR-002**: Every artifact MUST carry a linkage block with ≥1 source content hash; UIDs
  optional-corroborating; placement without content hashes is refused.
- **FR-003**: Manifests MUST be per-producer shards, append-only; a version entry, once written,
  is never removed or rewritten; the read index = shard union (M6 regression test mandatory).
- **FR-004**: All assessor file I/O via the gateway's ID-addressed rewrite (GAP-001); all file
  PUTs label-addressed (GAP-003); discovery via global listing or direct paths (GAP-002).
- **FR-005**: Producers are validated opaque tokens (existing `validate_annotator_id` PHI
  guard); algorithmic producers additionally carry `tool` + `params_hash`.
- **FR-006**: Retrieval supports: by case, by family, by frame (content hash or SOP UID), by
  producer, latest-only or full history; one public function per shape, FakeXNAT + dual-run
  tested.
- **FR-007**: Second-order artifacts store input-set hash; staleness check requires no payload
  download.
- **FR-008**: DICOM exports carry `ImageType=DERIVED\...`, `SourceImageSequence`,
  `DerivationDescription`, and pass the source-upload PHI gate; DICOM-SEG import/export
  round-trips payload-identical.
- **FR-009**: Derived placement never creates empty shells; failure mid-placement cleans up
  objects it created (and only those).
- **FR-010**: 005's existing annotation store migrates to sharded manifests with a one-shot,
  idempotent migration that preserves every existing blob and synthesizes shard entries for
  orphaned versions found on the server.
- **FR-011**: Corpus-level `ML_SPLITS` artifacts key membership by case content-identity set and
  expose a leakage check callable at ingest time.

### Key Entities

- **DerivationFamily** — ANNOT | CONSENSUS | METRICS | IMAGES; closed set, extension = spec
  amendment (deliberate friction).
- **Producer** — opaque id + kind (human|tool|model) + tool string + optional params_hash.
- **DerivedArtifact** — payload (codec-encoded) + linkage block + producer + type + version +
  created_at.
- **ManifestShard** — append-only per-producer ledger; union of shards = the index.
- **LinkageBlock** — content hashes (authoritative) + Study/Series/SOP UIDs (corroborating) +
  case_uid.
- **InputSetHash** — sha256 over sorted input artifact hashes (second-order provenance).

### Success Criteria *(mandatory)*

- **SC-001**: A new derived kind (novel METRICS payload) lands end-to-end with zero core-code
  changes — demonstrated in the dual-run lane.
- **SC-002**: M6 regression: after v2 upload, v1 remains discoverable; verified live (current
  HEAD fails this today).
- **SC-003**: N=4 concurrent producers append with zero lost entries and zero shared-file
  write conflicts (live lane; current shared-manifest design cannot pass).
- **SC-004**: Every retrieval shape in FR-006 returns correct sets on FakeXNAT AND live XNAT
  (dual-run), with FakeXNAT extended wherever its shape diverged (the campaign's fake-green
  lesson).
- **SC-005**: DICOM-SEG and derived-still exports validate against pydicom + the PHI gate;
  re-import is payload-identical.
- **SC-006**: 005 migration runs idempotently on a server seeded with current-format data,
  orphaned versions included.

### Assumptions

- 009's preserved-UID + content-hash identity work is the substrate; where 009 is not yet fully
  landed, content hashes are computable at placement time regardless.
- Stock XNAT 1.9.3 behaviors (GAP-001/002/003) hold on the UIowa deployment until disproven;
  the gateway hides them either way.
- `icr:roiCollectionData` (XNAT's OHIF ROI plugin type) is **not** assumed present; considered
  and rejected as canonical store (plugin dependency, server-side schema we cannot validate
  locally). Revisit only if UIowa confirms the plugin.

## Out of Scope (this feature)

- Aggregation algorithms themselves (005 seam owns STAPLE/majority/mean).
- Postgres registry migration (#34) — shards are server-side JSON regardless of registry.
- Pixel-level PHI automation (010).
- Cross-case registration / longitudinal composite derived objects.
- Retention/deletion policy for superseded versions (append-only now; pruning is a future
  operator-gated spec).
