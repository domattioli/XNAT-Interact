# Tasks: Derived-Data Hierarchy (012)

**Plan**: [`plan.md`](plan.md) · Execute top-to-bottom. `[S]` Sonnet, `[H]` Haiku (CLAUDE.md
dispatch). Every `GATE` line must pass before the next stage. Live gates need a booted XNAT
(`RUN_XNAT_DUAL=1`, recipe in `docs/STRESS_TEST_PLAN.md`).

## Stage 1 — Linkage + producer primitives (BLOCKS 2–6)

- [ ] T001 [H] `src/services/derived/linkage.py`: `LinkageBlock` dataclass + `build(content_hashes,
      study_uid, series_uid, sop_uids, case_uid)`; `resolve_by_content_hash(index, h)` and
      `resolve_by_sop_uid(...)`. Content hash authoritative; refuse build with zero content hashes.
- [ ] T002 [H] `src/services/derived/linkage.py`: `input_set_hash(artifact_hashes: list) ->
      sha256(sorted)` for second-order provenance (FR-007).
- [ ] T003 [H] `src/services/derived/producer.py`: `Producer(id, kind∈{human,tool,model}, tool,
      params_hash?)`; reuse `validate_annotator_id` PHI guard; algorithmic producers require
      `params_hash` (FR-005).
- [ ] T004 [H] `tests/test_derived_linkage.py` + `tests/test_derived_producer.py`: content-hash
      determinism + resolve; zero-hash refusal; input-set-hash order-independence; producer
      validation incl. model producers.
- [ ] GATE offline suite green; new modules imported nowhere yet.

## Stage 2 — Manifest shards (the keystone; BLOCKS 3–6)

- [ ] T005 [S] `src/services/derived/manifest_shard.py`: `append_version(shard_json, entry)`
      (append-only — never rewrite/remove an entry); `union_index(shards: list) -> index`
      (latest-pointer per (producer,type), all versions retained); `quarantine` a corrupt shard
      without failing the union (FR-003).
- [ ] T006 [S] `tests/test_manifest_shard.py`: **M6 REGRESSION (SC-002)** — append v2 → union lists
      v1 AND v2, latest=v2; two producers' shards independent; identical re-append idempotent;
      corrupt shard quarantined, others intact.
- [ ] GATE offline suite green; M6 regression locks the fix.

## Stage 3 — Placement layer (BLOCKS 4–6)

- [ ] T007 [H] `src/services/derived/placement.py`: closed `DerivationFamily` enum
      (ANNOT|CONSENSUS|METRICS|IMAGES); `place(family, scope, producer, version) -> location`
      per the normative hierarchy; unknown family/scope → `FriendlyError` (FR-001).
- [ ] T008 [H] `src/services/derived/placement.py`: parent-exists check + no-empty-shell /
      cleanup-on-failure (extends #38 invariant to assessor families) (FR-009).
- [ ] T009 [H] `tests/test_derived_placement.py`: frame/case/corpus scope → correct location;
      unknown family FriendlyError; failure mid-place leaves no shell (FakeXNAT).
- [ ] T010 [S] live smoke: place one artifact + shard against booted XNAT via the gateway
      (GAP-001/003 paths); verify file lands + shard reads back. Paste verbatim.
- [ ] GATE offline + the live smoke green.

## Stage 4 — Migrate 005 onto shards — serialize, touches io_xnat

- [ ] T011 [S] `src/annotations/io_xnat.py`: route `upload_annotation_set` writes through
      `manifest_shard.append_version`; reads through `union_index`. Behavior-preserving for a
      single-producer single-version case.
- [ ] T012 [S] `tests/test_005_migration.py`: idempotent one-shot that, given a current-format
      store (single rebuilt manifest, v1 orphaned by a prior v2), synthesizes shard entries for
      every on-server version incl. orphans; re-run is a no-op (SC-006, FR-010).
- [ ] T013 [S] live gate: migrate a DUAL-seeded annotation store (the lane_annotations v1→v2 case
      that confirmed M6) → after migration `union_index` lists BOTH versions. Paste verbatim.
- [ ] GATE 005's existing annotation tests stay green; migration idempotent live.

## Stage 5 — Retrieval API (BLOCKS 7)

- [ ] T014 [H] `app/logic/derived.py` (streamlit-free): `list_derived(server, project, case)`;
      `by_frame(content_hash | sop_uid)`; `by_producer(...)`; `latest_only` flag (FR-006). All
      addressing GAP-safe (global listing / direct paths).
- [ ] T015 [H] `tests/test_derived_retrieval.py`: each query shape on FakeXNAT — by-case (all
      families), by-frame (hash == sop_uid set when UIDs intact), latest-only = one per
      (producer,type).
- [ ] T016 [S] `tests/contract/test_derived_dual.py`: the same four shapes on the live lane
      (SC-004); extend FakeXNAT wherever its shape diverges, in the same commit.
- [ ] GATE offline + dual retrieval green.

## Stage 6 — DICOM exports — serialize, touches dicom_export

- [ ] T017 [S] `src/services/derived/dicom_export.py`: `to_dicom_seg(annotations, source_ds)` +
      `to_derived_still(...)` — `ImageType[0]='DERIVED'`, `SourceImageSequence` → source SOP UIDs,
      `DerivationDescription`, `ReferencedSeriesSequence`; inherit de-identified study context.
- [ ] T018 [S] export passes the source-upload PHI gate (no patient tags); `dicom_seg.from_dicom_seg`
      round-trips payload-identical (SC-005).
- [ ] T019 [H] `tests/test_dicom_export.py`: ImageType/SourceImageSequence assertions; PHI-gate
      pass; SEG round-trip byte-equal; reject re-upload of a derived still as source (content-hash
      known-derived).
- [ ] GATE offline suite green; pydicom validates the SEG.

## Stage 7 — Concurrency + corpus + guided panel

- [ ] T020 [S] `tests/contract/test_derived_dual.py`: **SC-003** N=4 concurrent producers append
      versions for the same case → all shards present, union complete, zero lost entries, zero
      shared-file writes. Live. Paste verbatim.
- [ ] T021 [H] `src/services/derived/` + project resources: `ML_SPLITS` artifacts keyed by case
      content-identity; `leakage_check(case_content_set)` callable at ingest (FR-011).
- [ ] T022 [H] `tests/test_derived_splits.py`: same content set re-ingested under a new case UID →
      leakage flagged at placement (SC-006 corpus case).
- [ ] T023 [H] `app/guided/browse_view.py`: read-only "Derived data for this case" panel reading
      `app/logic/derived.list_derived` (additive; novice path unchanged).
- [ ] T024 [S] FINAL: full fast suite 0 failed; dual lane green; `docs/STRESS_TEST_PLAN.md` results
      line per live gate; update `specs/README.md` row 12 → Built.
- [ ] GATE all green; spec 012 success criteria SC-001..006 demonstrated.

## Notes

- SC-002 (M6) and SC-003 (concurrency) are the two failures this feature exists to fix — both are
  live-confirmed on `xnat-fable`; their gates (T006, T013, T020) are non-negotiable.
- Keep FakeXNAT in lockstep with the real assessor surface — the campaign's nine fake-green bugs
  all came from divergence; any new gateway shape gets a FakeXNAT update in the same commit.
