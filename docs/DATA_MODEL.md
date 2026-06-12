# Data Model — XNAT-Interact

**Status:** Design / specification (authoritative). **Core implementation BUILT** via
[`specs/009-data-identity-dedup`](../specs/009-data-identity-dedup/) (992 tests green offline +
24 live dual-run): unique SOPInstanceUID (H1), UID preservation, `Old_StudyDate` strip + date
hash, content-based layered dedup + evidence package, SQLite registry (write-through facade over
ConfigTables), surgeon keyed-pseudonym + patient destroy, keep-all derived versioning. **Deferred:**
advanced automated pixel de-id (§4.2 — its own feature), Postgres migration
([#34](https://github.com/domattioli/XNAT-Interact/issues/34)). Related:
[#32](https://github.com/domattioli/XNAT-Interact/issues/32) (dedup/identity),
[#33](https://github.com/domattioli/XNAT-Interact/issues/33) (correctness audit).
**Last updated:** 2026-06-08
**Purpose of this document:** Capture *what the data is*, *why the repo exists*, and the
**identity / de-identification / duplicate-detection model** that must drive ingest. This is
the scientific + privacy spec behind the engineering — the deferred fixes (H1 SOPInstanceUID,
StudyInstanceUID handling, the dedup redesign) should be implemented against this, not guessed.

---

## 1. Purpose & Motivation

XNAT-Interact is the **de-identification + curation ingest layer** for a research corpus of
**orthopedic surgical imaging**, feeding a program on **objective, image-based assessment of
surgical skill** (University of Iowa; the Thomas / Anderson / Long line of work — surgical
simulation, fluoroscopy-based skill metrics, virtual coaching for community trauma surgery).

The tool itself is plumbing; the science it serves:

- **Primary goal — surgical skill assessment.** Intra-operative fluoroscopy is an objective,
  repeatable, low-bias substrate for scoring technical performance (fracture reduction quality,
  wire/implant placement). The **prized longitudinal axis is the surgeon/resident learning
  curve**: does an individual's objective image-based score improve across cases over time?
- **Secondary goal — classical ML/AI training.** The same images are also sampled in a
  **randomized, image-level** manner to train/evaluate models (e.g. segmentation). Order-agnostic;
  per-image identity and **train/test leakage avoidance** matter here.

Supporting analyses confirmed in scope:
1. **Radiographic measurement** — geometric skill metrics extracted from images.
2. **Inter-rater agreement** — validating objective assessment against expert raters; STAPLE
   consensus is the ground-truth aggregation.
3. **Fluoroscopy shot count / process** — number of intra-op shots as a direct skill/efficiency
   metric (fewer, better-targeted shots ⇒ more skilled).

**Why these matter for engineering:** the identity and dedup choices below are not cosmetic.
A dedup failure **corrupts learning curves** (primary) and **leaks data across ML train/test
splits** (secondary) — i.e. invalidated results.

---

## 2. What the Data Is

| Stream | Code class | Modality | Priority | PHI |
|---|---|---|---|---|
| Real intra-op trauma fluoroscopy (C-arm sequences) | `SourceRFSession` | RF DICOM | **Primary** | Yes (tag + burned-in pixel) |
| Arthroscopy post-op diagnostic images + intra-op video | `SourceESVSession` | DICOM / MP4 | Secondary | Yes |
| Derived data (segmentations, STAPLE consensus) | `DerivedData` / assessors | NIfTI / DICOM-SEG | — | No |
| Simulation-trial fluoroscopy (surrogate fractures) | *(future)* | RF DICOM | **Future, separate track** | No |

- **Priority is real OR fluoroscopy.** This is the PHI-bearing clinical stream and the reason the
  de-id pipeline exists.
- **Simulation data** is desirable but belongs in a **separate XNAT project / container track** —
  same ingest minus the PHI scrub — so simulation never co-mingles with clinical identity.
- XNAT hierarchy: Project → Subject → Experiment (session) → Scan → Resource → File. Derived
  results attach as **assessors** (which XNAT stores as experiments — see
  `specs/006-xnat-alignment/contract-test.md` GAP-001).

---

## 3. Identity Model

Four identity concerns, each resolved deliberately. **Provenance lives in keys held separately
from the data, not in the data itself** — this is what lets the corpus be de-identified *and*
auditable.

### 3.1 Patient — destroyed (no identity)

The science is about surgeon skill and image content, **not patient outcomes**. No per-patient
linkage is ever required. Patient identifiers are **one-way destroyed** — no pseudonym, no
crosswalk, no salt to leak. Strongest possible de-id.

### 3.2 Surgeon — stable keyed pseudonym

Learning curves require a **stable** surgeon key across cases; but a surgeon's name + HawkID is
**professionally sensitive** (named per-surgeon skill scores are IRB-sensitive and could be
misused). Resolution:

- Dataset stores `pseudonym = HMAC(secret_salt, hawkid)` → e.g. `surgeon_7f3a`.
  - Stable (same surgeon ⇒ same pseudonym) → learning curves intact.
  - Irreversible without the salt → de-identified at the dataset level.
  - Re-derivable by the salt holder → provenance / audit preserved.
- The `HawkID ↔ pseudonym` crosswalk + salt live **only** with the Data Librarian, under access
  control — **never** in the image dataset, derived products, or source/git.

> **Known exposure (remediate):** real surgeon names are currently hard-coded in
> `src/utilities.py` (the `Surgeons` config table) — committed to a public repo. Mitigation:
> make the repo private at minimum; move the name map out of source into a librarian-only
> restricted table holding pseudonyms server-side. Note: git **history** retains the names even
> after deletion (a true scrub needs history rewrite).

### 3.3 Case — content-derived identity, original UIDs corroborating

A "case" = one OR session's fluoroscopy sequence. Identity is needed to dedup re-uploads (so a
surgeon's case isn't double-counted, and so one case's frames don't span ML train/test).

- **Authoritative key:** the **set of per-image content hashes** (see §5). Content-based because
  the source DICOM UIDs cannot be fully trusted (re-export / anonymizer regenerates them).
- **Corroborating evidence (not authority):** original `StudyInstanceUID` (`Old_StudyInstanceUID`
  private tag) and `HMAC(salt, StudyDate + StudyTime + Manufacturer)`. The exact date is a HIPAA
  identifier, so it is **hashed for dedup, never stored readable**.

### 3.4 Image — raw-pixel content + unique instance id

- **Authoritative key:** `sha256(raw PixelData)` computed **before** any tag rewrite or
  normalization (survives de-id; identifies the same acquired frame).
- **Corroborating:** original `SOPInstanceUID` (`Old_SOPInstanceUID`).
- **Per-instance uniqueness MUST hold** — see the H1 defect in §6.

---

## 4. De-identification Model

Two layers. Design target: **no human review per case, much less per image.**

### 4.1 Tag de-identification (exists)

`src/services/deidentify.py::deidentify_dataset` scrubs PN-VR elements, private tags, overlays,
curves. (See §6 for ordering/UID defects in the surrounding mining code.)

### 4.2 Burned-in pixel PHI — fully automated, rare upload-time quarantine

Fluoroscopy burns PHI into **fixed peripheral overlay regions**; the diagnostic/skill content is
the **central anatomy** — so generous over-redaction of edge bands costs nothing.

- **Default (automated, no review):** redact known overlay regions via a **per-C-arm-model
  device profile** (keyed on `Manufacturer` `0008,0070` / `ManufacturerModelName` `0008,1090`),
  honoring `BurnedInAnnotation` `0028,0301`.
- **Safety net (automated):** an OCR / text-detector pass *after* masking; any frame with
  **residual text** is **auto-quarantined** (excluded from upload), not routed to a person.
- **Rare exception flags, handled at upload/quarantine time:** residual text, **unknown device**
  (no profile yet), or low-confidence redaction surface a flag into a **quarantine the uploader
  clears in-session**. Most uploads have zero flags. No per-case gate, no per-image review pass.
- **Human touch is one-time, not per-case:** define a mask when a genuinely new C-arm model first
  appears, and validate the pipeline's false-negative rate once (so de-id is IRB-defensible).

**Status — BUILT** (`010-pixel-deid`, `src/services/pixel_deid/`). The always-`True`
`needs_pixel_review` placeholder is replaced by a tiered, CPU-only verdict engine returning
`clean | redacted | quarantine`:

- **Tier 0 — device-profile blind mask** (`profiles.py`): contrast-independent zero-FN anchor,
  keyed on `Manufacturer`/`ManufacturerModelName` (+ private `0019` block).
- **Tier 0′ — cross-frame variance consensus** (`consensus.py`): static bright overlay pops out
  across a multi-frame case regardless of text contrast.
- **Tier 2 — multipass detector** (`detect.py`): Tesseract over `{orig,invert,stretch,clahe}`
  (`--psm 3`, hard-timeout, graceful-degrade) + an optional ONNX CRAFT detector
  (`models/craft/`, vendored later — absent ⇒ degrades to `[]`).
- **Tier 3/4 — PHI classification** (`verdict.py`): Presidio NER + a benign allow-list
  (laterality/view/kVp markers preserved).
- **Fail-closed routing:** unprofiled device with no positive clean evidence, PHI text outside the
  mask, or profile/detector disagreement → **quarantine** (held with evidence, never auto-uploaded;
  `quarantine.py`). Profiled-and-masked → redacted; profiled-and-empty → clean.

FN-safety (SC-001): every residual-PHI case is fully masked **or** quarantined — no clean/redacted
verdict leaks PHI pixels. Throughput (SC-005): ~2.7 s/case serial → 200 cases ≈ 9 min (< 15 min
budget). Audit entries carry categories/counts only — never raw PHI text. Deferred: advanced
learned-detector decoding (vendored CRAFT ONNX) and a validated-zero-FN holdout on real device
samples.

---

## 5. Duplicate Detection

Layered, cheap-first, grounded in the actual DICOM. **Never auto-remove data; flag and provide
evidence.**

### 5.1 Image-level

| Layer | Key | Role |
|---|---|---|
| Exact frame | `sha256(raw PixelData)` (pre-processing) | **authoritative** image identity |
| Corroborate | original `SOPInstanceUID` | confidence boost (Q9: not sole authority) |
| Re-export / near-dup | perceptual hash (current normalize+resize `ImageHash`) | **advisory flag only** — false-positive prone |
| Already-de-id'd file | `sha256(file bytes)` | catches re-upload of the same de-id'd file |

### 5.2 Within-case near-duplicate shots — keep, just note

Surgeons routinely fire several **near-identical** position-check shots of the same view. These
are **legitimate distinct data — never removed** — because **shot count is itself a skill metric**.
The compromise between vigilance and scope creep:

- Within-case near-dup detection is a **free byproduct** of the content hashing already done for
  case-level dedup.
- Reuse the **existing** `IS_QUESTIONABLE` mechanism → private tag `(0x0019,0x1007)` as a
  lightweight informational note on the image; optionally a per-case summary at upload
  ("N near-identical shot-pairs flagged").
- **Build nothing more.** Richer near-dup handling is YAGNI until a specific analysis needs it.

### 5.3 Case-level — set algebra, evidence package, human decides

- **Case key:** the set of per-image content hashes; **overlap detection = set algebra** over
  those sets against a dedup index. Corroborated by original `StudyInstanceUID` + hashed
  date/device.
- Relationships: exact-dup (`B == A`), subset (`B ⊂ A`), superset (`B ⊃ A`), partial
  (`A ∩ B ≠ ∅`, neither ⊆), disjoint (control — must import clean).
- **On any overlap with a *different* subject:** never auto-import, never auto-merge, never
  auto-reject. **Generate an evidence package** — which existing case(s) overlap, the overlap
  ratio, the specific matching shots, whether original UIDs/date/device corroborate — and route to
  a **human reviewer** who chooses **import both / combine / other**. Rare, case-level event.
- **Invariant (unconditional):** never create empty Subject/Experiment/Scan shells — filter images
  first, create objects only if ≥1 file actually lands (kills orphaned-shell pollution).

### 5.4 Ingest flow that falls out

1. Read original UIDs + date from the source DICOM (or the `Old_*` stash).
2. Compute keys: image = `sha256(PixelData)` (+ original `SOPInstanceUID` corroborant);
   case = content-hash set (+ `StudyInstanceUID` / `HMAC(salt, date+device)` corroborant).
3. Register in the dedup index; run image- and case-level overlap checks.
4. **Strip the re-identifying `Old_StudyDate`** private tag — its dedup value is already captured
   as a hash; leaving it readable is a re-id leak.
5. Ensure per-instance `SOPInstanceUID` stays unique (fix H1).

---

## 6. Engineering Implications (deferred fixes mapped to this model)

These were found in the [#33](https://github.com/domattioli/XNAT-Interact/issues/33) audit and
the [#32](https://github.com/domattioli/XNAT-Interact/issues/32) dedup grill. They are now driven
by §3–§5, not guesswork:

- **H1 — all DICOM UIDs collapse to one value** (`src/xnat_experiment_data.py:510/513/516` set
  Study = Series = SOPInstanceUID = `intake_form.uid` for every image). Violates per-instance
  uniqueness → corrupts **frame order** (skill metric) and **image identity** (ML). Fix:
  per-instance unique `SOPInstanceUID`; preserve/standardize Study & Series deliberately.
- **StudyInstanceUID clobber** — original case key is overwritten (stashed in `Old_StudyInstanceUID`).
  Capture it as a **corroborating** dedup signal; do not rely on it as authority (Q9).
- **Dedup redesign** — replace the normalize+resize pixel hash as the *primary* key with the
  layered model in §5 (raw-pixel content authoritative; perceptual hash advisory only).
- **`Old_StudyDate` re-id leak** — the exact surgery date is stashed readable in a private tag;
  strip it after computing the date-hash dedup key.
- **Surgeon cleartext** — see §3.2 exposure note.

These remain **deferred** pending implementation; this document is their spec.

---

## 7. Resolved Decisions (grill close-out)

- **Case grouping / multi-run (Q12):** do **not** auto-model runs — not worth the complexity.
  Keep the single-scan `'0'` default; make `scan` a **user-selectable parameter** so an uploader
  can put a distinct run in a second scan *by judgment*, and **preserve the original
  `SeriesInstanceUID` as metadata** (free, from the stop-clobbering fix) so run boundaries are
  reconstructible later without re-ingest.
- **Derived-data revisions (Q13):** **keep-all, monotonic versioning** for derived/assessor data
  too (not just annotations) — re-analysis writes `v(n+1)`, never overwrites `vn`, so provenance
  of which derived result fed which downstream analysis stays reproducible.
- **STAPLE rater pool (Q14):** **tiered** raters (expert / trained-analyst / crowd). Track an
  opaque `rater_id` + `expertise_tier` + `reliability_weight` in the registry (§8) — supports
  STAPLE weighting, inter-rater-agreement analysis, and the expert-vs-novice research question.
- **Arthroscopy (ESV) (Q16):** a **sibling track** — same goals/framework, **separate XNAT
  project/container**, distinct granular derived-data metrics. Not co-mingled with the trauma-RF
  corpus (like the future simulation track).
- **De-id validation bar (Q15):** no external/IRB standard imposed. Bar = **validated-zero
  residual PHI on a representative labeled set**, achieved by conservative over-masking + the OCR
  quarantine net, re-validated per new device profile. The current primitive pixel processing
  should be **upgraded to the advanced automated technique** in §4.2.

---

## 8. Persistence & Registry

The metadata that supports ingest — the surgeon/rater registry, the dedup index, the identity
crosswalks, and the provenance/audit trail — currently lives in `ConfigTables`: hand-rolled JSON
"tables" stored as one blob on XNAT, guarded by a sha256 fingerprint lost-update check.

**Why that must change:** whole-blob read-modify-write has a TOCTOU/lost-update window (#33 H4)
and serializes writers; `IMAGE_HASHES` as a JSON list is O(n) membership + full rewrite per add
(an unworkable scale wall); there is no schema, foreign keys, or transactions; and identity
crosswalks sit in the same shared store as operational data despite needing a separate access tier.

**Decision — SQLite sidecar now (Q17 = B); Postgres later (tracked in #34).**

- **Now: a single-file SQLite registry** replaces the ConfigTables JSON. Gains schema, foreign
  keys, **`UNIQUE`-indexed O(1) dedup membership**, in-session transactions, and real queries —
  the big wins over JSON. Tables:
  | Table | Key | Holds |
  |---|---|---|
  | `surgeons` | `pseudonym` | keyed-HMAC pseudonym, role (**no real names**) |
  | `raters` | `rater_id` | opaque id, `expertise_tier`, `reliability_weight` |
  | `cases` | `case_key` | surgeon pseudonym (FK), procedure, `date_hash`, device |
  | `image_hashes` | `content_hash` UNIQUE | `case_key` (FK), `orig_sopuid` (corroborant), `instance_number` |
  | `audit_log` | append-only | timestamp, actor pseudonym, action, target |
- **Identity crosswalks** (`pseudonym ↔ real HawkID` + salt) live in a **separate,
  access-controlled, encrypted store — librarian-only**, never in the operational DB.
  **Status — BUILT (011 US1):** `CrosswalkStore` now encrypts at rest via an AEAD envelope
  (AES-256-GCM, scrypt-derived key from a librarian passphrase; `src/services/crosswalk_crypto.py`).
  Encryption is opt-in (passphrase/key ctor arg); the unkeyed legacy plaintext path is unchanged and
  auto-upgrades on first keyed write. The salt is provisioned by `scripts/provision_identity_salt.sh`
  (CSPRNG, `0600`, no-clobber, never echoed) and loaded via `load_identity_salt`. Salt/passphrase/
  HawkID never appear in logs, argv, tests, or the repo.
- **PostgreSQL** ([#34](https://github.com/domattioli/XNAT-Interact/issues/34)) for genuine
  **concurrent multi-writer** scale — SQLite does not give safe concurrent multi-writer semantics;
  Postgres provides real server-side transactions/locking (closes #32 A). XNAT already runs on
  Postgres. **Status — BUILT (011 US2, opt-in):** an additive SQLAlchemy Core backend
  (`src/services/registry_backend.py`) engages only when `XNAT_REGISTRY_PG_DSN` is set; the default
  raw-`sqlite3` path is byte-unchanged. The 009 registry contract passes identically on sqlite3,
  sqlite-via-Core, and Postgres (when a DSN is present). `migrate_sqlite_to_pg` does a parity-checked
  table-by-table move.
- **Migration:** `src/services/deploy_009.py` (`deploy()`) runs the one-time ConfigTables-JSON →
  SQLite import via `migrate_from_configtables` (parity-asserted, full rollback on mismatch), records
  an audit entry (counts only, no PHI), and archives — not deletes — the source JSON.

---

## 9. References

---

## 8. References

- Research program: UI orthopedic simulator & fluoroscopy-based skill assessment (Thomas,
  Anderson, Long et al.) — e.g. *Minimally Trained Analysts Can Perform Fast, Objective Assessment
  of Orthopedic Technical Skill from Fluoroscopic Images* (PMC9488091); *A Vision for Using
  Simulation & Virtual Coaching to Improve the Community Practice of Orthopedic Trauma Surgery*
  (PMID 32742205).
- Internal: `specs/006-xnat-alignment/` (gateway + assessor contract), `docs/XNAT_MODEL.md`
  (pyxnat surface), issues #32 (dedup/identity), #33 (correctness audit).
