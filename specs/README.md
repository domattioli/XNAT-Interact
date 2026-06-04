# XNAT-Interact Specs (Spec-Kit)

Spec-driven dev artifacts. Each phase of `docs/IMPROVEMENT_PLAN.md` → a folder
with the trio:

- **`spec.md`** — what + why (user stories, requirements, success criteria). No
  implementation.
- **`plan.md`** — how (technical context, constitution check, structure, approach).
- **`tasks.md`** — ordered, checkboxed, parallelizable tasks. Execute top-to-bottom.

Gates: [`.specify/memory/constitution.md`](../.specify/memory/constitution.md).
Agent delegation + token rules: [`../AGENTS.md`](../AGENTS.md).

| # | Phase | Folder | Status |
|---|---|---|---|
| 0 | Make code testable | *(shipped PR #23 — `tests/`)* | ✅ Done |
| 1 | Reliability & safe failure (backend) | [`001-phase-1-reliability-and-safe-failure`](001-phase-1-reliability-and-safe-failure/) | Draft |
| 2 | Streamlit "see-your-data" app | [`002-phase-2-streamlit-app`](002-phase-2-streamlit-app/) | Draft |
| 3 | Packaging & install (detect-or-bundle Python; Software Center) | [`003-phase-3-packaging-and-install`](003-phase-3-packaging-and-install/) | Draft |
| 4 | Onboarding checklist & static site | [`004-phase-4-onboarding-and-docs`](004-phase-4-onboarding-and-docs/) | Draft |

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
