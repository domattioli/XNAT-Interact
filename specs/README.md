# XNAT-Interact Specs (Spec-Kit)

Spec-driven dev artifacts. Each phase of `docs/IMPROVEMENT_PLAN.md` → a folder
with the trio:

- **`spec.md`** — what + why (user stories, requirements, success criteria). No
  implementation.
- **`plan.md`** — how (technical context, constitution check, structure, approach).
- **`tasks.md`** — ordered, checkboxed, parallelizable tasks. Execute top-to-bottom.

Gates: [`.specify/memory/constitution.md`](../.specify/memory/constitution.md).
Agent delegation + token rules: [`../AGENTS.md`](../AGENTS.md).
Data model we build on (XNAT hierarchy, pyxnat surface, seams, extension points):
[`../docs/XNAT_MODEL.md`](../docs/XNAT_MODEL.md) — read before adding a new data
type, file kind, or download mode.

| # | Phase | Folder | Status |
|---|---|---|---|
| 0 | Make code testable | *(shipped PR #23 — `tests/`)* | ✅ Done |
| 1 | Reliability & safe failure (backend) | [`001-phase-1-reliability-and-safe-failure`](001-phase-1-reliability-and-safe-failure/) | ✅ Built |
| 2 | Streamlit "see-your-data" app | [`002-phase-2-streamlit-app`](002-phase-2-streamlit-app/) | ✅ Built (`app/`) |
| 3 | Packaging & install (detect-or-bundle Python; Software Center) | [`003-phase-3-packaging-and-install`](003-phase-3-packaging-and-install/) | ✅ Built (`installer/`) — OPS items deferred¹ |
| 4 | Onboarding checklist & static site | [`004-phase-4-onboarding-and-docs`](004-phase-4-onboarding-and-docs/) | ✅ Built — enable Pages² |
| 5 | Annotations & segmentations (multi-user, extensible, RLE storage; aggregator seam) | [`005-annotations-segmentations`](005-annotations-segmentations/) | ✅ Built (`src/annotations/`) — STAPLE deferred to aggregator seam |
| 6 | XNAT alignment (gateway ABC, conventions module, download fix #25, assessor seam, xnatpy spike) | [`006-xnat-alignment`](006-xnat-alignment/) | ✅ Built (commit a65a69b, 820 tests green; gateway ABC + conventions + assessor seam shipped via batch build 008; real-XNAT dual-run infrastructure landed; xnatpy spike deferred operator-gated) |
| 7 | Real-XNAT round-trip correctness (#27 push blocker, #28 bootstrap, #29 browse labels, #25 full-series download, #30 cleanup) | [`007-real-xnat-roundtrip-correctness`](007-real-xnat-roundtrip-correctness/) | ✅ Built (commit d0d80ae, 742 tests green; real-XNAT fidelity verification deferred to Phase 6 contract test) |
| 8 | Data identity & deduplication (unique SOPInstanceUID/H1, preserve UIDs, strip date re-id leak, content-based layered dedup + evidence package, SQLite registry replacing ConfigTables, surgeon keyed-pseudonym + patient destroy, keep-all derived versioning) | [`009-data-identity-dedup`](009-data-identity-dedup/) | ✅ Built (992 tests green offline + 24 live dual-run; spec from `docs/DATA_MODEL.md`; advanced pixel de-id + Postgres #34 deferred) |
| 9 | Burned-in PHI pixel de-id (tiered CPU pipeline: device-profile blind mask + cross-frame variance consensus + multipass Tesseract / optional CRAFT detector + Presidio PHI-NER; fail-closed `clean\|redacted\|quarantine`; quarantine store; replaces the always-`True` `needs_pixel_review`) | [`010-pixel-deid`](010-pixel-deid/) | ✅ Built (1101 tests green offline; FN=0 holdout + serial ≈9 min/200 < 15 min SC-005; CRAFT ONNX model vendored-later)³ |
| 11 | Operational hardening: deploy 009 identity layer (provision `XNAT_IDENTITY_SALT` + AES-256-GCM/scrypt encrypt crosswalk at rest + migrate live `ConfigTables` JSON with parity/rollback + archive), additive SQLAlchemy-Core Postgres backend behind `XNAT_REGISTRY_PG_DSN` (#34), localhost-guarded `boot-and-verify-local-xnat` | [`011-operational-hardening`](011-operational-hardening/) | ✅ Built (1155 tests green offline; 26 new 011 tests; PG lane skips w/o DSN; sqlite3 default byte-unchanged; localhost + synthetic only) |
| 12 | Derived-data hierarchy: provenance-scoped placement (frame/scan/case/corpus → 4 assessor families), content-first linkage blocks (009 extension), per-producer append-only manifest shards (fixes #33-M6 orphaning + live-confirmed manifest write race), model-as-annotator producers, DICOM-native exports (`SourceImageSequence`/DICOM-SEG, #4), second-order staleness via input-set hash, `ML_SPLITS` leakage ledger; GAP-001/002/003 addressing contractual | [`012-derived-data-hierarchy`](012-derived-data-hierarchy/) | 📝 Spec drafted (2026-06-12, from PR #38 live findings; plan/tasks pending) |

¹ Code + build specs + runbooks shipped. Operator/ITS-only items tracked in [`docs/OPS_CHECKLIST.md`](../docs/OPS_CHECKLIST.md): ITS Software Center packaging request, Windows code-signing cert, Apple Developer ID + notarization, app-control allowlisting.
² Static site + Pages workflow shipped; operator must enable Pages (Settings → Pages → Source = GitHub Actions).
³ **Numbering note:** two specs share ordinal **010** — this `010-pixel-deid` and a parallel-session `010-web-reachable-app` (#37). Distinct slugs, no file overlap; operator may renumber one to deconflict.

## Build in bulk

1. Lowest-numbered Draft phase.
2. Read `spec.md` (contract) → `plan.md` (approach) → `tasks.md`.
3. Work `tasks.md` top-to-bottom; `[P]` tasks run parallel.
4. Every server-touching task → fake-XNAT layer (built first in Phase 1). Every
   image task → preserve de-identification + burned-in-PHI review. No task adds
   a hardcoded endpoint or credential-in-argv.

> Sequencing: Phase 1 is a hard prereq for Phase 2 — the GUI reuses the Phase 1
> service layer (logic split from `input()`/`print()`, friendly-error helper,
> fake-XNAT seam).
