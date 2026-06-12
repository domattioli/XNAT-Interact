# STRESS_TEST_PLAN.md — live-XNAT stress + readiness campaign (xnat-fable)

Goal: prove src/ + app/logic upload/download/derived-data paths against a REAL local XNAT until confident for real-world data. Resumes #32 (Q5 = option C), executes #33 triage verify-first, follows INTEGRATION_TEST_RUNBOOK.md. Synthetic data only; localhost only; never UIowa prod.

## Env facts (this container, 2026-06-12)

- Docker daemon: start manually `sudo dockerd >/tmp/dockerd.log 2>&1 &`.
- Docker Hub anon pulls 429 (rate limit). Workaround that WORKS: pull base via `mirror.gcr.io/library/<img>:<tag>` then `docker tag` to the canonical name; build proceeds offline. ECR mirror (`public.ecr.aws/docker/library`) 403s on blob GET — do not use.
- Bitbucket war/plugin downloads reachable (runbook hosts OK).
- XNAT: repo harness `tests/integration/xnat_local/` (all-in-one tomcat+pg, admin/admin, :8080). NOT xnat4tests (extra pip dep + hub pulls).
- No VPN needed: local XNAT replaces rpacs.iibi.uiowa.edu via `XNAT_SERVER_URL=http://localhost:8080` + `XNAT_PROJECT_NAME=<test>` (config precedence env > file > default, src/services/config.py).

## Phases

P0 baseline — offline suite green before touching anything (`pytest -m "not slow"`). Record counts; any regression vs reported 1154/34/7 = investigate first.
P1 boot — build+up xnat_local; wait `curl localhost:8080` 200/302; bootstrap project + ConfigTables via `src/initialize_basic_metatable_items.py` path; verify #28-class fresh-project bootstrap.
P2 source data — synthetic fluoro factory (pydicom): realistic RF tags (PatientName/ID PHI-shaped but fake, StudyInstanceUID proper, per-instance SOPInstanceUID, InstanceNumber, mixed photometric), seed-set algebra per #32 Q4 (`make_surgery(uid, seeds)`); precondition assert distinct seeds → distinct hashes.
P3 upload lanes (per #32 Q2 tiering):
  - B volume: 10+ surgeries × up to 50 imgs; one ≥1000-instance case (M1 off-by-one); wall-clock per case.
  - A concurrency: parallel publishes from threads/procs → ConfigTables lost-update (H4 TOCTOU), singleton races (H5).
  - C malformed: truncated DICOM, no-InstanceNumber (#30), 3-channel (S1), weird VR, dup-tag crash (H2-class `add_new` ValueError), oversized names (H6 label injection).
P4 download — whole-surgery fetch (C1/H7 residue), zip assembly counts (H8), round-trip byte/hash compare vs generated source, path-safety regression (C2 fixed 18c9ac3).
P5 derived — annotations (DICOM-SEG via src/annotations, MTurk semantic seg, generic blobs); re-confirm GAP-001 assessor file.put 404 on this XNAT version; upload derived under separate resource labels; download back.
P6 dedup characterization (#32 Q5 = C): baseline current normalize→resize→sha256 envelope (exact-dup / subset / superset / partial / disjoint / brightness / scale cases) → false-pos/neg table; feeds layered-identity redesign (raw-byte sha256 + preserved StudyInstanceUID + pHash flag).
P7 evidence site — `docs/site/` gains a "proven against live XNAT" page: metrics tables, round-trip integrity results, screenshots; deploys via existing pages.yml.
P8 issue sweep — file/comment findings: #32 (Q5 progress), #33 (verify-first verdicts), #30/#25/#12/#24 as touched; new issues for fresh bugs.

## Lanes/gates

- Real-XNAT tests behind `RUN_XNAT_DUAL=1`, mark `@pytest.mark.stress` (#32 Q2).
- Fixture preflight: docker daemon down → skip not fail (#33 harness finding).
- Never create empty subject/experiment/scan shells — invariant check after every lane (#32 Q3).
- Findings recorded with exact file:line + verbatim errors; verify against HEAD before claiming a bug (#33 false-positive lesson).

## Results log

Append-only; one line per lane run: `<date> <phase> <result> <evidence-path>`.

2026-06-12 P0 baseline 1154p/22s/7xf PASS tests run local
2026-06-12 P1 boot xnat_local 1.9.3 live :8080 PASS (mirror.gcr.io workaround for hub 429)
2026-06-12 P3-happy push harness all 10 steps OK after nibabel install + XNAT_IDENTITY_SALT set; subject+experiment+scan+files on server PASS
2026-06-12 P4-pull download_selection empty-scan_type row -> files_written=[] ok=False — #25/C1 residue CONFIRMED live
2026-06-12 BUG xnat_resource_data.py:415 raise FriendlyError (dataclass) -> TypeError; fix in flight
2026-06-12 P3-dual contract suite RUN_XNAT_DUAL=1: 24 passed vs live XNAT (fixture tears down container post-run)
2026-06-12 BUG-CRIT app login dead: auth.py imports nonexistent build_server -> masked as 'VPN unreachable'; FIXED + live login ok=True
2026-06-12 BUG-HIGH pyxnat create(xsiType=...) silently ignored -> uploads land xnat:mrSessionData; main.py queries rf/esvSessionData -> would MISS all uploads; fix in flight
2026-06-12 GAP-002 stock XNAT 1.9.3: rfSessionData not in search elements -> project/subject-scoped listings omit rf experiments; /data/experiments?project=P lists them; browse fallback in flight
2026-06-12 NOTE esvSessionData absent from core XNAT schema (Iowa RPACS must carry custom plugin) — arthro publishes untestable locally as esv
2026-06-12 NOTE ConfigTables in-memory staleness across sequential publishes in one process (driver needed pull_from_xnat per surgery) — batch-mode dedup risk
2026-06-12 NOTE XNAT banner '318 sessions open from one IP' during stress — JSESSION leak (REST curls + per-worker pyxnat connects without disconnect)
2026-06-12 BUG-HIGH publish_to_xnat creates subject/exp/scan BEFORE upload; put_zip failure leaves empty shells (live, VERIFY_E2E) — #32 Q3 invariant violated; needs cleanup-on-failure or upload-first design
2026-06-12 FIX xsiType: create(experiments=...)/create(scans=...) verified live -> xnat:rfSessionData + xnat:rfScanData stored
2026-06-12 GAP-003 stock XNAT 1.9.3: file PUT via NUMERIC resource id 404s under rf-typed sessions (mr ok); label-addressed + extract=true works -> pyxnat put_zip broken for rf; gateway REST rewrite in flight
2026-06-12 P5 GAP-001 CLOSED live: assessor insert/list/get_copy/delete via accession-ID rewrite all green (SMOKE_DRV)
2026-06-12 P3-volume 10 surgeries x 20 frames: 10/10 ok, mean 1.71s, no empty shells, all rfSessionData PASS
2026-06-12 P4-roundtrip browse+download live: 206 files retrieved; raw PixelData sha256 20/20 IDENTICAL upload vs download; PatientName/ID redacted PASS
2026-06-12 P3-concurrent 4 workers: phase1 project-create race -> HTTP 500 (harness); phase2 config push 2/4 raw DatabaseError/AssertionError, 0 LostUpdateError -> H4 guard misses live contention CONFIRMED
2026-06-12 P3-malformed: truncated/not_a_dicom/three_channel CRASH w/ raw asserts (client-side, no server pollution); no_instance_number/dup_private_tag/1005-frame ACCEPTED (fixes hold live)
2026-06-12 P6-dedup DEAD-WIRED: all overlap cases (exact/subset/superset/partial) ACCEPTED, DedupReviewRequired never fired — write_publish_catalog_subroutine:685 omits dedup_registry/incoming_content_hashes
2026-06-12 P4-enum BUG real pyxnat lacks list_files(); resources().get() yields numeric ids w/ silent-empty reads — download enumeration fixed via files().label() + label-iteration
2026-06-12 P3-malformed RERUN post-fix: truncated/not_a_dicom/three_channel -> FRIENDLY (named offender, no crash); others ACCEPTED — lane fully green behavior
2026-06-12 P6-dedup WIRED+GREEN live: exact/subset/superset/partial -> DedupReviewRequired (reject), disjoint -> accept; root cause of historic dead gate = unwired kwargs + lowercase/uppercase hash mismatch (config.add_new_item uppercases; ImageHash lowercase) -> intersection always empty
2026-06-12 FINAL suite 1276 passed / 0 failed / 7 xfailed; campaign phases P0-P8 complete
