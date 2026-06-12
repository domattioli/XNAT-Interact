# XNAT_MODEL.md — the data model we build on

**Audience:** maintainers + future feature builders. **Purpose:** one place that
explains *what XNAT is*, *how this repo talks to it*, and *where the seams are*, so
new features (annotation types, download modes, new file kinds) are modular —
added at a known layer without relearning the platform each time.

Caveman-lite (engineering doc). Code signatures exact. Compiled 2026-06-05 from a
codebase inventory + external pyxnat/XNAT docs research (sources at bottom).

---

## 1. Canonical XNAT model (the platform, not our code)

XNAT = imaging data-management platform. Everything is a nested container:

```
Project → Subject → Experiment (a.k.a. Session) → Scan → Resource → File
                          ├─ Assessor      (derived/analysis: QC, ROICollection) → Resource → File
                          └─ Reconstruction (legacy derived slot, superseded)     → Resource → File
```

| Level | Means | In our domain |
|---|---|---|
| **Project** | top-level org + access-control unit | `GROK_AHRQ_Data` (one project, all cases) |
| **Subject** | study participant | **one surgical performance** (keyed by `intake.uid`) |
| **Experiment / Session** | one imaging session; schema-typed (`xnat:rfSessionData`, etc.) | **a surgery's source data** (`SOURCE_DATA-{uid}`) |
| **Scan** | one acquisition series in a session | the image/video set (we use a single scan, label `0`) |
| **Assessor** | *derived* data on a session — first-class, searchable in Postgres | **not yet used** — the natural home for consensus/derived annotations (see §6) |
| **Resource** | a **labelled file container**; attaches at ANY level | our extension namespace: `SRC`, `INTAKE_FORM`, `ANNOTATIONS`, `config`, `backups` |
| **File** | one file in a resource | DICOMs, MP4s, JSON, annotation blobs |

**Key insight:** *Resource* attaches at every level and *Assessor* is the
schema-blessed slot for derived results. Those two are the platform's built-in
extension points — most new features map onto one of them, no schema change.

### REST path grammar (what pyxnat builds under the hood)

`/REST`, `/data`, `/data/archive` prefixes are interchangeable (legacy API).
Plural in raw REST, singular in pyxnat's path syntax:

```
/data/archive/projects/{P}/subjects/{S}/experiments/{E}/scans/{SC}/resources/{R}/files/{F}
/data/archive/projects/{P}/.../experiments/{E}/assessors/{A}/resources/{R}/files/{F}
/data/prearchive/projects/{P}/{TIMESTAMP}/{SESSION}/...        # staging, pre-commit
```

pyxnat path syntax (singular) → translates to the plural archive paths internally.

---

## 2. How THIS repo maps onto the model

Our object code lives in `src/xnat_*.py`; it writes the hierarchy top-down.

### Query-string conventions (the addressing scheme — memorize this)

```python
proj_qs     = '/project/{xnat_project_name}'
subj_qs     = '/project/{name}/subject/{intake.uid}'
exp_qs      = subj_qs + '/experiment/SOURCE_DATA-{intake.uid}'
scan_qs     = exp_qs  + '/scan/0'                    # scan label ALWAYS '0'
resource    = <level>.resource('{SRC|INTAKE_FORM|ANNOTATIONS|config|backups}')
file        = resource.file('{filename}')
```

- **Subject UID** = `intake_form.uid` (pydicom UID, `.`→`_`). Doubles as study/series UID.
- **Experiment label** = `SOURCE_DATA-{uid}` (marks raw/intra-op data).
- **Scan label** = `'0'` (single scan per experiment — a design simplification).

### Resource-label registry (the de-facto extension table)

| Label | Holds | Attaches to | Written in |
|---|---|---|---|
| `SRC` | zipped DICOM/MP4 | Scan | `xnat_experiment_data.py` `write()` → `put_zip` |
| `INTAKE_FORM` | `RECONSTRUCTED_OR_DATA_INTAKE_FORM.json` | Subject | `xnat_resource_data.py` `push_to_xnat` |
| `ANNOTATIONS` | per-annotation RLE blobs + `manifest.json` | Scan | `src/annotations/io_xnat.py` |
| `config` | `database_config.json` (ConfigTables) | Project | `utilities.py` ConfigTables |
| `backups` | timestamped config backups | Project | `utilities.py` ConfigTables |

> Adding a new file kind = pick/define a resource label + a codec. No schema work.
> This is why Phase-5 annotations slotted in cleanly.

### Object classes (`src/xnat_*.py`)

- **`xnat_experiment_data.py`** — `ExperimentData` (base) + `SourceRFSession`
  (fluoroscopy) / `SourceESVSession` (arthro JPG+MP4). Owns `_generate_queries`,
  `_select_objects`, `publish_to_xnat` (PHI gate → `put_zip` loop).
- **`xnat_scan_data.py`** — `ScanFile` (base) + `SourceDicomDeIdentified`,
  `ArthroDiagnosticImage` (JPG→DICOM), `ArthroVideo`, `MTurkSemanticSegmentation`
  (now a thin adapter → `annotations.importers.mturk`). De-id + hash-dedup live here.
- **`xnat_resource_data.py`** — `ResourceFile` (base) + `ORDataIntakeForm` (the
  surgical-metadata form; serializes to JSON, uploaded to `INTAKE_FORM`).

### Publish flow (one surgery → XNAT)

```
ORDataIntakeForm(uid) → SourceRFSession/ESVSession
  → select subject/experiment/scan, assert not exists, create()
  → scan.resource('SRC').put_zip(dicom_zip, content=IMAGE, format=DICOM)
  → subject.resource('INTAKE_FORM').file(...).insert(json)
  → ConfigTables.add_new_item('SUBJECTS'/'IMAGE_HASHES', ...) → push_to_xnat()
```

---

## 3. The pyxnat surface we actually use (the whole seam)

**16 distinct calls — that's the entire dependency.** Narrow surface = a clean
gateway ABC can fully wrap it (see §6).

| pyxnat call | Purpose |
|---|---|
| `Interface(server, user, password)` | authenticated connection |
| `server.select(querystring)` | reach subject/experiment/scan by path |
| `server.select.project(name)` | project handle |
| `obj.exists()` | check before create |
| `obj.create(**{level: 'xnat:{schema}…Data'})` | create subject/exp/scan |
| `obj.attrs.mset({xpath: value})` | set schema attributes |
| `obj.resource(label)` | resource handle (any level) |
| `resource.file(name)` | file handle |
| `resource.put_zip(ffn, content=, format=, tags=)` | bulk upload |
| `file.put(ffn, content=, format=, tags=, overwrite=)` | single upload |
| `file.get_copy(dest)` | download (copy out of cache) |
| `file.insert(data, content=, format=, tags=)` | upload raw bytes/text |
| `file.delete()` | delete file |
| `project.label() / .users()` | identity + access validation |
| `server.get('/')` | liveness probe |
| `server.disconnect()` | close JSESSION |

### Beyond what we use — pyxnat API worth knowing for new features

- **Collections** (`CObject`): `select.project(P).subjects()` → lazy iterator;
  `.where([...])`, `.first()`, `.fetchall()`. **We never enumerate** — relevant to
  the download gap (#25): listing a session's *actual* scans/files needs this.
- **`Resource.get(dest_dir, extract=False)`** — downloads **all** files in a
  resource at once. Directly applicable to whole-surgery download (#25) instead of
  our one-synthesized-file-per-row approach.
- **`put_dir`**, **`insert(**fields)`** creating the whole parent chain in one call.
- **`.attrs.mget/get/set`**, **`obj.children()/parent()/xpath()`** for traversal.
- **Search API**: `select(datatype, columns).where(constraints)` — server-side
  query (constraint syntax `[(field, op, value), 'AND']`) **[unverified — docs 403]**.

### pyxnat gotchas (write these into any new code)

- **Cache is non-reentrant / unsynchronized** → parallel jobs must use distinct
  `cachedir`. (We're single-session CLI, so low risk today.)
- `cachedir` lives in the JSON `config` file, not an `Interface` kwarg.
- `get()` returns a cache path; `get_copy()` copies out — we correctly use `get_copy`.
- `verify=` controls SSL (our `utilities.py:445` liveness check catches cert errors).
- Version **1.6.4** (2025-11-06), PyPI status "Beta", lightly maintained, Py3.9/3.10.

---

## 4. Config & connection (where the knobs are)

- **`src/services/config.py`** — `AppConfig.load`: env > file > default.
  - env: `XNAT_SERVER_URL`, `XNAT_PROJECT_NAME`
  - file: `xnat_config.json` / `.xnat-interact.json` (cwd or home)
  - default: `https://rpacs.iibi.uiowa.edu/xnat/`, project `GROK_AHRQ_Data`
- **Credentials NEVER persisted** — interactive `pwinput` at runtime. Keep it that way.
- **`XNATConnection`** (`utilities.py`) — singleton; validates project label +
  user membership before `is_verified`.
- **`ConfigTables`** — a pseudo-DB (JSON on the project `config` resource):
  `REGISTERED_USERS, ACQUISITION_SITES, GROUPS, SUBJECTS, IMAGE_HASHES, SURGEONS`.
  Has a **lost-update guard**: fingerprints the server copy at pull, re-checks at
  push, raises `FriendlyError` if another session changed it. Don't bypass.

---

## 5. Test & abstraction seams (already in place)

- **`src/services/xnat_gateway.py`** — `build_server(url, user, password)` →
  `pyxnat.Interface`. Single named factory; the injection point.
- **`tests/fakes/fake_xnat.py`** — `FakeXNAT` covers the full surface above:
  records `.calls`, round-trips file bytes (`put`→`get_copy`), `set_next_failure()`
  for error paths. Lets every server-touching test run offline, no PHI.
- **`src/annotations/io_xnat.py`** — annotation up/download; keep-all versioned
  blobs (`ann__{annotator}__{type}__v{n}.{ext}`), manifest overwritten, blobs never.

---

## 6. Recommendations for modular future work

Ordered by leverage. These are *findings*, not committed work — file as issues
when you pick them up.

1. **Promote the gateway to a real ABC.** The pyxnat surface is only 16 calls
   (§3). Define `XnatGateway` (ABC) with exactly those methods; `PyxnatGateway`
   implements it, `FakeXNAT` already shadows it. Payoff: (a) a client swap
   (pyxnat→xnatpy) becomes one new impl, not a repo-wide edit; (b) every new
   feature codes against the ABC, never raw pyxnat. **Highest-leverage, low-risk.**

2. **Use the platform's real extension points for new data.**
   - New *file kind* → new **resource label** + codec (the proven Phase-5 path).
   - New *derived result* (consensus/STAPLE output) → store as an **Assessor**
     (`icr:roiCollectionData` is XNAT's blessed annotation assessor), not a loose
     resource. Assessors are Postgres-searchable; loose resources aren't. Our
     current `ANNOTATIONS` resource is fine for raw per-rater blobs; the *consensus*
     is the thing that wants assessor status.

3. **Fix the download leg against the API, not around it (#25).** Replace the
   synthesized one-file-per-scan-row (`app/logic/download.py:222`) with either
   `Resource.get(dest_dir)` (all files in a resource) or a `CObject` enumeration of
   the scan's real files. Add a count-verify. This is an API-shaped fix the gateway
   ABC should expose as `list_files()` / `download_resource()`.

4. **Evaluate an `xnatpy` migration** (don't rush it). `xnatpy` (PyPI `xnat`,
   `xnat.connect(...)`) is the more actively-developed, abstraction-friendly client
   — typed objects, schema introspection, native-feeling traversal
   (`session.projects[P].subjects[S].experiments[E].scans[SC]`). Trade-offs:
   - **For us:** cleaner tree-walking (helps #25 enumeration), less manual xpath
     knowledge, Apache-2.0, the DAX project is migrating (community drift).
   - **Against:** rewriting our label-addressed direct lookups (pyxnat is better at
     "grab *this* scan by label"), re-checking cache/concurrency assumptions.
   - **Path:** the §1 gateway ABC makes this a contained experiment — implement
     `XnatpyGateway` behind the same interface, A/B it, decide on evidence.

5. **Mind the single-scan / `scan='0'` simplification.** Real XNAT sessions hold
   many scans; we collapse to one. Any future multi-series feature must revisit
   `_generate_queries`. Document the assumption at the call site.

### Annotation/segmentation specifics (validate Phase-5 against the platform)

- XNAT-native annotation store = **ROICollection assessor** via the OHIF Viewer
  plugin; formats DICOM **RTSTRUCT**, **AIM 4.0**, **DICOM SEG** (incl.
  fractional/probabilistic), NIFTI (in progress). RTSTRUCT lands under a resource
  named `RT_STRUCT` on the assessor.
- **No native STAPLE / multi-rater consensus in XNAT** — it's an external/pipeline
  step over stored assessors. Confirms our aggregator-seam design: build the seam,
  the algorithm is ours to run. (NVIDIA Clara AI-Assisted Annotation is the only
  in-viewer auto-seg integration; saves back as assessors.)
- Our custom RLE+manifest store is the right call for raw per-rater efficiency;
  add a **DICOM SEG export adapter** (already stubbed in `importers/dicom_seg.py`)
  for interop with the OHIF/assessor world.

---

## 7. Ecosystem map (prior art to borrow from)

GitHub survey of XNAT-working repos (2026-06-05). Closest analogs first.

**Python clients (the model layer):**
- `pyxnat/pyxnat` — what we use. REST-faithful, thin, you build paths/xpaths.
- `xnatpy` (PyPI `xnat`, GitLab radiology/infrastructure) — modern, typed, OO. Migration candidate (§6.4).
- `VUIIS/dax` — mature pyxnat automation framework; reference for heavy data-model use; migrating to xnatpy.
- `harvard-nrg/yaxil`, `ArcanaFramework/*` (frametree, pydra2app-xnat) — alt interface libs / modern pipeline frameworks.

**Same mission as us (anonymize + up/download):**
- `gift-surg/GiftCloudUploader` (Java) — **closest analog**: desktop app that
  de-identifies DICOM then uploads to XNAT. Study its de-id gate.
- `Australian-Imaging-Service/xnatutils` — CLI up/download/list (xnatpy). UX patterns for #25.
- `nimh-dsst/xnat-bids-cli`, `SPMIC-UoN/xnatc` — login→query→download CLIs.
- `Australian-Imaging-Service/msbir-xnat-download-tool` — Win/Mac download installers (packaging reference).

**Annotation / ROI / SEG on XNAT (feeds Phase 5):**
- `xnat-ville/Upload_OHIF_ROI` — CLI uploading ROI/annotations via XNAT REST (our exact path).
- `JamesAPetts/ohif-viewer-XNAT-plugin` — how XNAT natively stores OHIF ROI/SEG (assessor model).

**Raw REST (proves the API is simple, non-pyxnat):**
- `KCL-BMEIS/xnat_python_scripts`, `somnonetz/xnat-access` (requests),
  `juanprietob/xnat-rest` (node), `WilkinsonK/xapi-oxidized` (Rust),
  `NrgXnat/oasis-scripts` (bash curl). The `/data/archive/...` API is callable from anything.

**Testing:**
- `Australian-Imaging-Service/xnat4tests` — stands up a real XNAT in Docker for
  integration tests. Real-server alternative if FakeXNAT fidelity ever bites.

**Official (NrgXnat = XNAT makers):** `xnat-docker-compose`, `docker-images`
(container service), `xnat-jupyterhub-plugin` — server-side model + extension points.

---

## Sources

- **Codebase inventory** — this repo (`src/utilities.py`, `src/xnat_experiment_data.py`,
  `src/xnat_scan_data.py`, `src/xnat_resource_data.py`, `src/services/`, `src/annotations/`,
  `tests/fakes/fake_xnat.py`); file:line refs in the inventory that produced this doc.
- **pyxnat** — github.com/pyxnat/pyxnat (`core/interfaces.py`, `core/resources.py`,
  `doc/features/operations.rst`, `attributes.rst`), pyxnat.github.io/pyxnat, PyPI.
- **XNAT model / REST** — wiki.xnat.org/xnat-api/xnat-rest-api-directory.
- **ROI/SEG** — wiki.xnat.org/xnat-ohif-viewer, NrgXnat/docker-images/rt-struct-assessor.
- **xnatpy** — pypi.org/project/xnat, xnat.readthedocs.io, gitlab.com/radiology/infrastructure/xnatpy, VUIIS/dax#211.

> Flagged **[unverified]** in-text: pyxnat search-constraint syntax + exact xnatpy
> version (upstream doc pages returned 403/404 to automated fetch). Everything else
> confirmed from primary source.
