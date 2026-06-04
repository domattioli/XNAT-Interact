# XNAT-Interact Specs (Spec-Kit)

Spec-driven development artifacts. Each phase from `docs/IMPROVEMENT_PLAN.md` has
a folder containing the spec-kit trio:

- **`spec.md`** — *what* and *why* (user stories, requirements, success criteria).
  Implementation-free.
- **`plan.md`** — *how* (technical context, constitution check, structure,
  approach).
- **`tasks.md`** — ordered, checkboxed, parallelizable tasks an implementer (or
  a coding agent) can execute top-to-bottom.

Governing principles: [`.specify/memory/constitution.md`](../.specify/memory/constitution.md).

| # | Phase | Folder | Status |
|---|---|---|---|
| 0 | Make the code testable | *(shipped in PR #23 — `tests/`)* | ✅ Done |
| 1 | Reliability & safe failure (backend) | [`001-phase-1-reliability-and-safe-failure`](001-phase-1-reliability-and-safe-failure/) | Draft |
| 2 | Streamlit "see-your-data" app | [`002-phase-2-streamlit-app`](002-phase-2-streamlit-app/) | Draft |
| 3 | Packaging & install (bundle-or-detect Python; Software Center) | [`003-phase-3-packaging-and-install`](003-phase-3-packaging-and-install/) | Draft |
| 4 | Onboarding checklist & static site | [`004-phase-4-onboarding-and-docs`](004-phase-4-onboarding-and-docs/) | Draft |

## How to build in bulk

1. Pick the lowest-numbered Draft phase.
2. Read its `spec.md` (the contract) → `plan.md` (the approach) → `tasks.md`.
3. Work `tasks.md` top-to-bottom; `[P]`-tagged tasks can run in parallel.
4. Every server-touching task uses the **fake-XNAT layer** (built first in
   Phase 1); every image task preserves de-identification + the burned-in-PHI
   review. No task may introduce hardcoded endpoints or credential-in-argv.

> Sequencing note: Phase 1 is a hard prerequisite for Phase 2 — the GUI reuses
> the Phase 1 service layer (logic separated from `input()`/`print()`, the
> friendly-error helper, and the fake-XNAT test seam).
