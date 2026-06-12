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
