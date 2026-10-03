<!-- provenance: author=domattioli model=claude-fable-5-1 effort=high date=2026-10-02 skill=brainstorm repo=XNAT-Interact session=ca12ae1b-b427-4aea-b26b-42650e9ed4ee -->
# Brainstorm: a full data lifecycle for XNAT-Interact, and one intake cycle for every analysis

**Date**: 2026-10-03 | **Repo**: XNAT-Interact, branch `xnat-fable` | **Status**: brainstorm, not a spec

This document answers one question from the maintainer: how should real-life data move in and out of XNAT, and how do we make sure every new kind of analysis goes through the same intake cycle before its results land on the server? It is grounded in the code that exists today. Every claim about the code cites a file and line as read on 2026-10-03. Where a capability does not exist, the document says "missing" and names the search that failed to find it.

The audience is the maintainer, who is not a strong coder. Wherever a choice exists between a framework and one manifest file plus one command, this document picks the manifest and the command.

## 1. Lifecycle map

Real use of the tool is a loop. A surgical case enters once. Many people then pull it down, study it, and push results back, over months. The loop has eight flows. The table after each flow names the actor, what starts the flow, what code runs today, where the result lives on the server, where PHI could leak, and what identity question the flow raises.

### Flow 1. A new surgical case is added

- **Actor and trigger**: a student with a folder of fluoroscopy DICOM files, arthroscopy videos, or both, right after a surgery.
- **Code today**: the intake form `ORDataIntakeForm` (`src/xnat_resource_data.py:182`, construction at `:206`), the session classes `SourceRFSession` (`src/xnat_experiment_data.py:935`) and `SourceESVSession` (`:1197`), and the single publish entry point `write_publish_catalog_subroutine` (`:813`), which runs the dedup gate, the de-identification write, and `publish_to_xnat` (`:458`). The guided app reaches the same path through `app/guided/publish_impl.py:158` (`_make_real_publish_fn`) and `app/logic/upload.py:160` (`prepare_and_upload`). Batch entry is `app/logic/batch.py:242` (`run_batch`).
- **Server artifact**: one subject per case, one experiment labelled `SOURCE_DATA-<uid>` (`src/services/xnat_conventions.py:119`), one scan `0` (`:10`, "Single-scan-per-experiment design simplification"), resource `SRC` for images and `INTAKE_FORM` for the form (`:200` onward). The case also lands in the project configuration tables `subjects` and `IMAGE_HASHES` (`src/utilities.py:790-792`), pushed with `ConfigTables.push_to_xnat` (`:1044`).
- **PHI exposure**: metadata scrubbing in `deidentify_dataset` (`src/services/deidentify.py:28`) and the pixel review gate in `publish_to_xnat` (`src/xnat_experiment_data.py:522-525`). Two known gaps are open as issues #63 (pixels never rewritten) and #64 (PatientBirthDate kept).
- **Identity**: the content hash is authoritative (`src/services/identity.py:47`). Dedup runs against the server-resident `IMAGE_HASHES` table through `_ConfigTablesRegistryAdapter` (`src/xnat_experiment_data.py:169`). Spec 014 proved this path live.

### Flow 2. A case is pulled down for analysis

- **Actor and trigger**: a student or a lab member who wants to measure, annotate, or train on a case.
- **Code today**: `app/logic/browse.py:250` (`fetch_data_table`) lists subjects, experiments, scans and file counts. `app/logic/download.py:87` (`list_downloadable`) and `:120` (`download_selection`) fetch files per scan and per resource label, defaulting to `SRC` (`:295-297`) and also walking `DERIVED` and other labels (`:135`). `assemble_zip` (`:509`) packs a selection with a `scope` of `source` or `all` (`:527-529`). The gateway primitive is `download_resource` (`src/services/xnat_gateway.py:252`, implemented at `:697`).
- **Server artifact**: none written. The download is recorded locally only, through `app/logic/metrics.py:297` (`record_download_attempt`).
- **PHI exposure**: none new if Flow 1 did its job. The downloaded files are de-identified copies. Local cleanup of the download folder is the student's responsibility today; nothing in the code deletes it.
- **Identity**: the student receives files named `<instance>-<uid>` (`src/xnat_scan_data.py:108-114`). The uid in the name is the case identifier the student must carry into every result they push back. Nothing enforces that today.

### Flow 3. An analysis result is uploaded

- **Actor and trigger**: the same person as Flow 2, after measuring angles, computing a score, or running a model.
- **Code today**: partial. `publish_to_xnat` accepts `assessor=Path` and `assessor_label` (`src/xnat_experiment_data.py:475-476`) and, when given, creates a versioned assessor with `create_assessor` (`:703-716`) using `next_assessor_label` (`src/services/xnat_conventions.py:149`, `__v<n>` suffix, keep-all). The gateway supports `create_assessor`, `list_assessors`, `put_file`, `insert_file` (`src/services/xnat_gateway.py:270-306`, `:767`). But this path is reachable only from inside a source-data publish. A search for a standalone analysis uploader found none: `grep -rn "def upload_analysis\|def publish_derived\|def upload_result" src app` returned nothing. The guided app has no "upload a result" page (`app/guided/` holds `wizard_upload.py`, `browse_view.py`, `home.py`, `demo.py` only).
- **Server artifact**: today, an assessor labelled `SEGMENTATION_CONSENSUS-<uid>__v<n>` under the source experiment, one resource, one file. Nothing records who produced it, from which inputs, with which code version.
- **PHI exposure**: analysis outputs can carry PHI in three ways: a copied source image, a filename holding the original accession, or free-text notes. No gate exists for derived uploads.
- **Identity**: no link from the result back to the content hashes it was computed from. A re-analysis cannot be told apart from a first analysis except by the version suffix.

### Flow 4. A computer-vision training set is assembled and uploaded

- **Actor and trigger**: a lab member building a dataset for model training: selected frames across many cases, plus labels.
- **Code today**: missing as a flow. The pieces exist: `assemble_zip` (`app/logic/download.py:509`) can pull many cases; annotation sets can be read back with `download_annotation_set` (`src/annotations/io_xnat.py:328`); masks can be exported to DICOM SEG with `to_dicom_seg` (`src/annotations/importers/dicom_seg.py:239`). No code assembles a cross-case dataset, writes a split file, or uploads a dataset object. `grep -rn "training\|dataset" src app --include=*.py -l` matched only docstrings in `app/logic/metrics.py` and `src/services/pixel_deid/`.
- **Server artifact**: none today. A training set is a cross-case object; XNAT has no native slot for it (see section 3).
- **PHI exposure**: highest of all flows. A training set is the artifact most likely to leave the lab (shared drive, cloud GPU). It must contain only de-identified frames and must carry a manifest proving each frame's provenance.
- **Identity**: a dataset must list the content hashes of every frame it contains, so that a frame can be traced back and a retraction (Flow 8) can find every dataset that holds it.

### Flow 5. Annotation round trip

- **Actor and trigger**: several annotators label the same frames; a consensus is computed.
- **Code today**: the most complete derived-data path. `AnnotationSet` and `Annotation` (`src/annotations/model.py:97`, `:34`) with manifests (`:154`, `:181`); importers for generic masks, MTurk rows, and DICOM SEG (`src/annotations/importers/generic.py:24`, `mturk.py:117`, `dicom_seg.py:41`); upload and download as blobs plus `manifest.json` under the `ANNOTATIONS` resource of the scan (`src/annotations/io_xnat.py:139`, `:328`); consensus through `aggregate_set` (`src/annotations/aggregate/__init__.py:113`) with a reference aggregator and a STAPLE stub (`aggregate/staple.py:57`, not implemented). The app page is `app/pages/annotations.py:44` with logic in `app/logic/annotations.py:97` (`import_annotations`), `:150` (`run_consensus`), `:202` (`upload_annotations`).
- **Server artifact**: `ANNOTATIONS` resource on scan `0` of the source experiment, versioned blobs per annotator.
- **PHI exposure**: low. Blobs are masks and coordinates. Annotator identifiers are lab members, not patients, but they are still names and should be pseudonyms (`src/services/identity.py:79` already does this for surgeons).
- **Identity**: `image_ref` in the manifest is an XNAT query string, not a content hash. If the scan is re-uploaded under a new experiment, the annotations are orphaned.

### Flow 6. Model outputs or predictions are uploaded

- **Actor and trigger**: a trained model is run over cases; its predictions are pushed back for review.
- **Code today**: missing. Predictions could be shaped as an `AnnotationSet` with annotator id equal to the model name and version, which would reuse Flow 5 unchanged. No code does this.
- **Server artifact**: `ANNOTATIONS` resource (if treated as an annotator) or an assessor (if treated as an analysis). Section 3 recommends the first.
- **PHI exposure**: low, same as Flow 5.
- **Identity**: the model version and the training-set identifier (Flow 4) must be recorded, or a prediction cannot be reproduced.

### Flow 7. Re-analysis of an existing case

- **Actor and trigger**: new method, new software version, or a correction, applied to a case already analysed.
- **Code today**: the `__v<n>` keep-all versioning (`src/services/xnat_conventions.py:149-199`) is the only support. Nothing says why a new version exists or what changed.
- **Server artifact**: a new assessor version or a new annotation blob version.
- **PHI exposure**: same as Flow 3.
- **Identity**: the new version must point at the version it supersedes.

### Flow 8. Retraction or correction of a published item

- **Actor and trigger**: the Data Librarian, after a PHI finding, a consent withdrawal, or a corrupted upload.
- **Code today**: `delete_file` on the gateway (`src/services/xnat_gateway.py:220`, `:636`) and a maintainer-only destructive script (improvement plan, decision 3, `docs/IMPROVEMENT_PLAN.md:439-464`). Spec 014 found that the local XNAT image refuses subject deletes with HTTP 403 (research.md SPEC-ISSUE-15), so even the maintainer path is not proven live.
- **Server artifact**: removal, plus the configuration rows in `subjects` and `IMAGE_HASHES`.
- **PHI exposure**: this is the flow that fixes a leak, so it must be fast and complete: every derived item that holds the retracted frame (annotations, assessors, training sets) must be found. Without content-hash links in the derived artifacts (Flows 3 to 6), that search is impossible.
- **Identity**: the retraction must be recorded so a later re-upload of the same content hash is refused or flagged.

### What the map shows

Flows 1, 2 and 5 exist and are tested. Flow 3 exists as a hook with no front door. Flows 4, 6, 7 and 8 are missing or unsafe. The common gap is provenance: no derived artifact today says which content hashes it came from, who made it, with what code, and what it replaces. Fixing that once, in one place, is the point of section 2.

## 2. Generalized analysis-intake pipeline

### The idea in one paragraph

Every analysis type, whether a measurement spreadsheet, a segmentation, a consensus, a training set, or a model's predictions, is just a folder of output files plus a small text file that describes them. The intake cycle reads that text file, checks it, checks the folder, runs the PHI gate, uploads, downloads the upload back to verify it, and writes a catalog row. The analyst never calls XNAT directly. The analyst runs one command (or clicks one button) on one folder.

### The analysis descriptor

The descriptor is a file named `analysis.yaml` (or `analysis.json`; the loader accepts both) at the root of the output folder. It holds two parts: a type declaration, written once per analysis type by whoever introduces the type, and a run record, written per run by the analyst or their script.

Type declaration fields (one file per type, kept in the repo under `analysis_types/<type_name>.yaml`, versioned with the code):

| Field | Meaning | Example |
|---|---|---|
| `type_name` | short identifier, lowercase | `knee_flexion_angle` |
| `type_version` | integer, bumped when outputs change shape | `2` |
| `description` | one sentence in plain language | "Flexion angle per fluoroscopy frame" |
| `inputs` | what the analysis consumes: `source_frames`, `annotations`, `assessor:<type>`, `dataset` | `[source_frames]` |
| `outputs` | list of expected files with a glob and a format | `angles.csv` (csv), `plots/*.png` (png) |
| `placement` | where results live on XNAT: `scan_resource`, `assessor`, `project_resource` (section 3) | `assessor` |
| `phi_policy` | which PHI checks apply: `no_pixels`, `pixels_from_source_only`, `text_scan` | `[no_pixels, text_scan]` |
| `schema` | optional JSON Schema file for structured outputs | `schemas/angles.schema.json` |

Run record fields (written per run, in the same `analysis.yaml` under `run:`):

| Field | Meaning | Who fills it |
|---|---|---|
| `type_name`, `type_version` | which declaration this run follows | copied by the tool |
| `case_uid` | the case identifier from the downloaded file names | analyst or script |
| `source_hashes` | content hashes of every input frame used | the tool computes them from the input folder |
| `input_refs` | XNAT query strings of inputs (annotations, prior assessors) | the tool, from the download manifest |
| `producer` | pseudonymised HawkID (`surgeon_pseudonym` pattern, `src/services/identity.py:79`) | the tool, from the login |
| `code_ref` | git commit or package version of the analysis code | analyst or script |
| `parameters` | free dictionary of settings | analyst or script |
| `supersedes` | label of the version this replaces, or empty | analyst, when re-analysing |
| `notes` | plain text, scanned for PHI before upload | analyst |

The tool fills everything it can. The analyst supplies `case_uid`, `code_ref`, `parameters` and `notes`. For a student, that is four lines. A complete run record for the simplest type looks like this before the tool fills in the rest:

```yaml
type_name: knee_flexion_angle
type_version: 2
run:
  case_uid: 1_2_840_113619_2_123
  code_ref: flexion-angle@0.3.1
  parameters:
    smoothing_window: 5
  notes: "Angles from the lateral view only."
```

After the tool runs stages 3 and 6, the same file also holds `source_hashes`, `input_refs`, `producer`, and the XNAT label it was published under, so the copy on the server is self-describing.

### The cycle, stage by stage

1. **Register the type**. A new analysis type is a pull request that adds `analysis_types/<type_name>.yaml` and, if the outputs are structured, a JSON Schema. The offline test suite loads every type file and checks it against a meta-schema. This is the only stage that needs a developer, and it happens once per type, not once per run. Existing production types to register on day one: `annotations` (Flow 5, placement `scan_resource`, label `ANNOTATIONS`), `segmentation_consensus` (today's assessor at `src/xnat_experiment_data.py:704`), `dicom_seg` (via `to_dicom_seg`).
2. **Declare inputs and outputs**. The analyst points the tool at an output folder. The tool reads `analysis.yaml`, finds the type declaration, and checks that every declared output glob matches at least one file and that no undeclared file is present (undeclared files are the usual PHI leak: a stray screenshot or a copied source DICOM).
3. **Build the provenance manifest**. The tool reads the inputs the analyst downloaded (the download step in Flow 2 should write a `download_manifest.json` with content hashes and XNAT query strings; today it does not, and this is the first new helper). From it, the tool fills `source_hashes` and `input_refs`. If no download manifest exists, the tool recomputes hashes from the input folder with `image_content_hash` (`src/services/identity.py:47`) and warns that provenance is reconstructed, not recorded.
4. **Validation gate**. Structured outputs are checked against their schema. Counts are checked: an output that claims one row per frame must have as many rows as `source_hashes`. Failures print a plain-language message with the file name and the first failing row, never a traceback (Principle II).
5. **PHI gate**. Three checks selected by `phi_policy`. `no_pixels`: no file in the folder parses as DICOM, PNG, JPG, or MP4. `pixels_from_source_only`: every image file's content hash is in `source_hashes` or is a declared mask (binary or label-valued). `text_scan`: every text file (csv, json, yaml, txt, md) runs through `classify_text` (`src/services/pixel_deid/verdict.py`; the same function behind `tests/test_010_verdict.py`), and any hit blocks the upload with the matching snippet shown. A human confirmation is required when the policy includes pixels, mirroring the source-data gate (Principle I).
6. **Publish**. One function, `publish_analysis(descriptor, folder, gateway)`, dispatches on `placement`: `scan_resource` calls the gateway's `insert_file` (`src/services/xnat_gateway.py:182`) under the declared label on scan `0`; `assessor` calls `create_assessor` (`:282`) with `next_assessor_label` for keep-all versioning; `project_resource` puts files on a project-level resource (section 3). The run record is always uploaded beside the outputs as `analysis.yaml`, and a copy is appended to a project-level catalog (stage 8).
7. **Verify by download**. The tool lists the resource it just wrote, downloads every file to a temporary folder, compares hashes with the local outputs, and deletes the temporary folder. This is the same check spec 014 made the live harness do (`tests/integration/live_xnat/helpers.py:275`, `download_scan_files`). An upload is reported as done only after this stage passes (Principle VI).
8. **Catalog**. One row per published analysis is appended to a new configuration table `ANALYSES` in `ConfigTables` (`src/utilities.py:792` shows how `IMAGE_HASHES` was added with `add_new_table`), with columns `type_name`, `type_version`, `case_uid`, `label`, `producer`, `source_hash_count`, `supersedes`. The table rides the existing `pull_from_xnat` and `push_to_xnat` path (`:958`, `:1044`) and so inherits the concurrency concern already tracked for the config file (Principle VI). Browse (`app/logic/browse.py:250`) gains a column showing analysis count per case, from this table.

### What is generic and what is per type

Generic, written once: descriptor loading and meta-schema, provenance manifest builder, validation gate driver, the three PHI checks, `publish_analysis` dispatch, verify-by-download, the `ANALYSES` catalog, the command and the app page.

Per type, declared not coded: output globs and formats, placement, PHI policy, optional JSON Schema. A type never needs Python unless it needs a custom validator, and even then the plugin is one function `validate(folder, descriptor) -> list[str]` registered the way aggregators are registered today (`register_aggregator`, `src/annotations/aggregate/__init__.py:68`).

### Worked examples

**Annotation set (Flow 5)**. Type `annotations`, placement `scan_resource`, label `ANNOTATIONS`, outputs `manifest.json` plus `*.rle` and `*.json` blobs, PHI policy `no_pixels`. Publish calls the existing `upload_annotation_set` (`src/annotations/io_xnat.py:139`) instead of raw `insert_file`, so the current versioned-blob layout is preserved. Nothing changes for annotators except that `analysis.yaml` now sits beside the blobs and the catalog gains a row.

**Segmentation masks (Flow 5 variant)**. Type `segmentation`, placement `scan_resource`, label `SEG_<producer>`, outputs `masks.npz`, `labels.json`, PHI policy `pixels_from_source_only` (masks are binary; the check verifies dtype and unique values). This is exactly what the spec 014 harness writes today with `put_derived_resource` (`tests/integration/live_xnat/helpers.py:291`), so the live test becomes the first consumer of the real function.

**STAPLE consensus (Flow 5)**. Type `segmentation_consensus`, placement `assessor`, inputs `[annotations]`, outputs `consensus.npz`, `scores.json`, `uncertainty.npz`. `input_refs` lists the three `SEG_*` resources. Publish uses `create_assessor` with `consensus_label` (`src/services/xnat_conventions.py:134`) as the base label, which is what `publish_to_xnat` does today at `src/xnat_experiment_data.py:704-716`, but now callable without a source-data publish.

**Training dataset (Flow 4)**. Type `training_dataset`, placement `project_resource`, inputs `[source_frames, annotations]`, outputs `dataset_manifest.json` (one row per frame: content hash, case uid, scan query string, label source, split), `splits.json`, and optionally the frames themselves as a zip. PHI policy `pixels_from_source_only` plus `text_scan`. `source_hashes` is the union over all cases, which is what makes Flow 8 possible. The dataset does not need to hold frames at all: a manifest of hashes plus query strings lets anyone rebuild it with `download_selection`, which keeps the large and risky object off the server.

**Model predictions (Flow 6)**. Type `model_predictions`, placement `scan_resource`, label `ANNOTATIONS`, with `producer` set to `model:<name>@<version>` and `input_refs` naming the training dataset's project resource. Predictions flow through `upload_annotation_set` like any annotator, so consensus and review tooling work on them unchanged, and the dataset link makes them reproducible.

**Measurement spreadsheet (Flow 3, simplest case)**. Type `knee_flexion_angle`, placement `assessor`, outputs `angles.csv` with a schema of three columns, PHI policy `no_pixels` plus `text_scan`. The analyst writes four lines in `analysis.yaml` and runs the command. This is the case to build first, because it exercises every generic stage with the smallest payload.

### The command and the button

Command line, optional path (Principle III): `python main.py publish-analysis <folder>`. The guided app gets a fourth task on the home page (`app/guided/home.py:13`) beside Upload, Browse and Download: "Share a result". It uses the same step rail as the upload wizard (`app/guided/components.py:39`): pick folder, review descriptor, PHI check, confirm, done. Dropdowns for `type_name` come from the registered type files, following the pattern of `dropdown_options` (`app/logic/upload.py:43`).

## 3. XNAT placement decision

XNAT offers four places to put derived data. The gateway already speaks to three of them.

| Place | What it is | Gateway support today | Fits |
|---|---|---|---|
| Scan resource | a labelled folder on scan `0` of the source experiment | `insert_file`, `list_files`, `download_resource` (`src/services/xnat_gateway.py:182`, `:237`, `:252`) | per-case outputs tied to the images: annotations, masks, predictions |
| Experiment resource | a labelled folder on the experiment, beside the scans | `_ensure_resource` with a REST path (`:479`); not exposed as a public method | intake form today (`INTAKE_FORM`); little reason to add more here |
| Assessor | a typed child object of the experiment, with its own label, date, and resources | `create_assessor`, `list_assessors` (`:282`, `:270`; implemented `:767`, `:656`), versioning in `next_assessor_label` | per-case outputs that are results, not images: measurements, consensus, scores |
| Project resource | a labelled folder on the project itself | `put_file` and `insert_file` take any query string; the config JSON already lives here (`ResourceLabel.CONFIG`, `src/services/xnat_conventions.py:200` onward) | cross-case objects: training datasets, the `ANALYSES` catalog |
| Separate project | a second XNAT project for derived data | nothing; would need new project creation and permissions | rejected: splits provenance across projects and doubles the permission work for the Data Librarian |

Tradeoffs. Scan resources are simple, visible in the XNAT web UI next to the images, and already used by annotations. They cannot carry structured attributes, only files, so a catalog is needed to search them. Assessors can carry attributes and dates and are the XNAT-native idea of "a result about this session", but the local image used by spec 014 showed element-security gaps for custom types (research.md SPEC-ISSUE-15), and the generic `xnat:assessorData` type used today (`src/xnat_experiment_data.py:713`) carries no custom fields anyway, so in practice an assessor is also a labelled folder. Project resources are the only place for cross-case objects.

Recommendation. Keep three placements and let the type declaration choose: `scan_resource` for anything aligned to frames (annotations, masks, predictions), `assessor` for anything that is a result about the case (measurements, consensus), `project_resource` for anything spanning cases (datasets, catalog). Do not create a second project. Do not add new XNAT datatypes; keep `xnat:assessorData` and put structure in the `analysis.yaml` beside the files, where the tool can read it without XNAT schema work. Revisit custom datatypes only if the UIowa server administrators offer to install one.

One check to run before building: confirm with the production server that `create_assessor` with `xnat:assessorData` is permitted for student accounts, since the local image refused related deletes. The spec 014 live harness can test this against the container first.

## 4. Synthetic end-to-end flow for the test bed

Spec 014 already publishes three synthetic cases through production and verifies them by download. The lifecycle rehearsal extends that suite rather than starting a new one. It reuses `tests/integration/live_xnat/conftest.py` for boot, login, reset and teardown, `cases.py` for fixtures, and `helpers.py` for publish and download.

The rehearsal, as one live test module `test_lifecycle_rehearsal.py`, in order:

1. **Add case**: `publish_case_session` (`helpers.py:145`) for KNEE_2025, as today.
2. **Pull**: new helper `download_case(live, result, dest)` built on `download_scan_files` (`helpers.py:275`) that also writes `download_manifest.json` with content hashes and query strings. This helper is the prototype of the production download manifest from section 2, stage 3, and should move into `app/logic/download.py` when the spec is built.
3. **Analyze with a stub**: a deterministic fake analysis in `tests/integration/live_xnat/stub_analyses.py` that reads the downloaded frames and writes `angles.csv` (one row per frame, angle equal to a seeded function of the content hash) plus `analysis.yaml` with a fixed `code_ref`.
4. **Intake and upload**: call the real `publish_analysis` (new, section 2). Assert stage 4 rejects a folder with an undeclared file, assert stage 5 rejects a folder holding a copied source DICOM and a notes file containing a synthetic name from `make_phi_dicom_dataset` (`tests/synthetic_data.py:31`), then assert the clean folder publishes and the verify-by-download stage passes.
5. **Pull again**: list assessors (`gateway.list_assessors`) and confirm one `knee_flexion_angle__v1`; download it; compare hashes.
6. **Re-analyze**: publish a second run with `supersedes: knee_flexion_angle__v1`; confirm `__v2` exists, `__v1` still exists, and the catalog row for v2 names v1.
7. **Annotate and predict**: upload three annotation sets with `upload_annotation_set` as the spec 014 tests do, then upload a fourth with `producer: model:stub@1` through `publish_analysis` type `model_predictions`; confirm `aggregate_set` sees four annotators.
8. **Assemble a training set**: new stub that walks KNEE_2025 and HIP_2024 frames, picks every third, writes `dataset_manifest.json` and `splits.json`, publishes as `training_dataset` to a project resource; download and assert every listed hash is in the server `IMAGE_HASHES` table (`server_identity_rows`, `helpers.py:313`).
9. **Retraction drill**: pick one frame hash from the dataset; run a new `find_derived(hash)` helper that reads the `ANALYSES` catalog and every `analysis.yaml` it points to; assert it returns the dataset, the predictions, and both assessor versions. Do not delete anything in this test; the delete path stays a maintainer-only script with a dry run.
10. **Counts and hashes**: extend the outcome summary (`tests/integration/live_xnat/outcome_summary.py`, schema in `specs/014-real-xnat-integration-testing/contracts/outcome-summary.schema.json`) with `analyses_published`, `analyses_rejected`, `dataset_frames`, so the SC-008 reproducibility comparison covers the lifecycle too.

New fixtures and helpers needed: `download_case` with manifest; `stub_analyses.py` (angle stub, dataset stub); a PHI-bearing "dirty folder" builder reusing `make_phi_dicom_dataset` and `make_synthetic_intake_form_textfile` (`tests/synthetic_data.py:492`); registered type files for the six examples in section 2; the `ANALYSES` table seeded by the conftest the way `IMAGE_HASHES` is today. Offline, every stage except the two gateway calls is unit-tested against folders on disk, and the gateway calls are tested against the existing `FakeXNAT` used by `app/guided/demo.py:99`.

How each intake stage is covered, so no stage depends on the container alone:

| Stage | Offline test | Live test (014 harness) |
|---|---|---|
| 1 register type | meta-schema over every `analysis_types/*.yaml` | none needed |
| 2 declare outputs | folder with a missing and an extra file | rehearsal step 4 |
| 3 provenance | download manifest present, absent, and stale | rehearsal step 2 |
| 4 validation | schema failure, row-count mismatch | rehearsal step 4 |
| 5 PHI gate | dirty folder with copied DICOM and a synthetic name in notes | rehearsal step 4 |
| 6 publish | `FakeXNAT` for all three placements | rehearsal steps 4, 7, 8 |
| 7 verify | `FakeXNAT` returning one altered file | rehearsal step 5 |
| 8 catalog | `ConfigTables` round trip on a temp JSON | rehearsal steps 6, 9 |

## 5. Constitution check

**Principle I, PHI safety**. The intake cycle adds a PHI gate to derived uploads, which have none today (Flow 3). The `text_scan` check reuses the verdict classifier; the `pixels_from_source_only` check refuses any image whose hash is not a known de-identified frame or a declared mask. Tension: `classify_text` has two failing offline tests today (`tests/test_010_verdict.py`), so the text gate inherits whatever weakness those tests expose. The gate must fail closed (block on any uncertainty) until those tests pass.

**Principle II, fail softly**. Every stage returns a `FriendlyError` (`src/services/errors.py` pattern, rendered by `app/guided/components.py:68`) with the offending file name and a next step. One bad output file blocks that analysis only, never the case or the session. The verify stage reports exactly which file mismatched.

**Principle III, skill floor**. The analyst writes four fields in one text file and clicks one button or runs one command. Type registration needs a developer, once per type. Tension: students who write their own analysis scripts must learn to write `analysis.yaml`; a template generator (`publish-analysis --init <type>`) that writes the skeleton removes most of that.

**Principle IV, offline testable**. Every stage except publish and verify runs on folders and is tested offline. Publish and verify run against `FakeXNAT` offline and against the container live. Type files are validated offline by a meta-schema test. No new seam is needed: the gateway abstraction (`src/services/xnat_gateway.py:39`) already exists.

**Principle V, configuration over hardcoding**. Placement labels and type names live in type files, not code. The project name and server come from the existing `AppConfig` (`src/services/config.py:36`). No new credential handling.

**Principle VI, data integrity**. Verify-by-download makes every analysis upload confirmed. Keep-all versioning prevents overwrites. The `ANALYSES` table rides the config file and so shares its lost-update risk under concurrent users; the improvement plan already tracks the config race (`docs/IMPROVEMENT_PLAN.md:219` onward, "reliability & integrity" bundle), and the catalog must land after that fix or accept the same risk explicitly. Retraction stays a confirmed, dry-run-first maintainer operation.

## 6. Open questions for the operator

1. **Should training datasets hold frames or only a manifest of hashes?** Recommended default: manifest only. Anyone with access rebuilds the frames with the download tool; the server never holds a second copy of the images, and the risky object stays small.
2. **Should model predictions be stored as annotations (one more annotator) or as assessors?** Recommended default: annotations, so consensus and review work unchanged, with the producer field marking them as a model.
3. **Who may register a new analysis type?** Recommended default: a pull request reviewed by the maintainer, because a type declares its own PHI policy and a wrong policy is a leak.
4. **Does the production server allow student accounts to create assessors?** Recommended default: test it on the local container first through the spec 014 harness, then ask the UIowa XNAT administrators; if refused, fall back to scan resources for everything per-case and lose nothing except the XNAT web UI's "assessor" tab.
5. **Should the `ANALYSES` catalog wait for the config lost-update fix?** Recommended default: build the catalog now but write it through the same `ConfigTables` path, so the one fix covers both; document the shared risk in the spec.
6. **Should `analysis.yaml` be YAML or JSON?** Recommended default: accept both, write JSON by default from the tool, because the project already stores JSON manifests (`src/annotations/io_xnat.py:63`) and the venv has no YAML parser today (spec 014 delegate report).
7. **How is a retraction executed?** Recommended default: a maintainer-only command with dry run that lists every derived item found by `find_derived(hash)` and asks for confirmation per item; no automatic deletes anywhere else.
8. **Should the pixel-PHI gate for derived uploads require a human confirmation even when the policy is `no_pixels`?** Recommended default: no, because the check is mechanical (no file parses as an image); yes for any policy that allows pixels.

## 7. Suggested spec split

Build order follows dependencies. Issues #63 to #68 are from spec 014; #65 and #66 are fixed on the working tree as of 2026-10-03, the others are open.

| Spec | Scope in one line | Depends on |
|---|---|---|
| 015 download manifest | `download_selection` and `assemble_zip` write `download_manifest.json` with content hashes and query strings; offline tests on `FakeXNAT`; live test in the 014 harness | none |
| 016 analysis descriptor and intake cycle | type files plus meta-schema, descriptor loader, validation gate, PHI gate, `publish_analysis` for `assessor` and `scan_resource`, verify-by-download, `ANALYSES` catalog, command line entry; first type `knee_flexion_angle`; registers `annotations` and `segmentation_consensus` as types | 015; #64 for the text gate's definition of PHI fields |
| 017 guided app "Share a result" page | the wizard for 016, dropdowns from type files, friendly errors | 016 |
| 018 training datasets | type `training_dataset`, `project_resource` placement, manifest-only datasets, `find_derived(hash)` | 016 |
| 019 model predictions as annotators | type `model_predictions` through `upload_annotation_set`, producer convention, dataset link | 016, 018 |
| 020 retraction tooling | maintainer-only dry-run command over `find_derived`, config row purge, live test on the container; needs the HTTP 403 finding from 014 resolved with the server administrators | 018; 014 SPEC-ISSUE-15 |
| 021 lifecycle rehearsal | the ten-step live module from section 4 and the outcome-summary extension | 015 to 019 |

Two things should happen before 016 starts. Issue #63 (pixel redaction at publish) is a source-data gap, not a derived-data one, but the `pixels_from_source_only` check assumes source frames are clean; until #63 closes, that assumption is false and the spec must say so. Issue #68 (automated confirmer quarantines unprofiled devices) decides whether any automated path can publish at all; the intake cycle should reuse whatever decision #68 reaches rather than invent a second confirmer.
