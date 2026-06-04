# AGENTS.md — XNAT-Interact

Guide for AI agents (and humans) working this repo. Goal: responsible
delegation + token budgeting, without weakening the project's safety or
readability rules. Governing principles: [`.specify/memory/constitution.md`](.specify/memory/constitution.md).

> Style: this file written caveman-lite (terse, articles/filler dropped, still
> grammatical). See "Caveman levels" below for where each level applies.

---

## Delegation: pick the cheapest tier that won't get it wrong

Before any subagent spawn, choose **model tier** by task complexity — do NOT
clone the parent's model.

| Task class | Examples | Model |
|---|---|---|
| Mechanical | rename, typo, locate code, format tweak, single-fn rewrite | `haiku` |
| Standard | scoped multi-file edit, tests for known behavior, doc drafting, research synthesis | `sonnet` |
| Hard | algorithm-critical, cross-cutting refactor (3+ files), arch decision, ambiguous, security/PHI-sensitive | `opus` |

Rule: cheapest tier that won't err. Correctness/PHI/security rounds **up**, never
down. Known one-line answer → no subagent.

## Token budget: caveman level by who reads the output

| Output consumer | Caveman level |
|---|---|
| Parent context only (machine-read, aggregated, discarded) | **ultra** |
| Mixed (parent reads + may quote to human) | full |
| Human artifact (PR body, commit msg, release notes) | **none** (normal prose) |

Subagent **report back to parent** → default ultra (saves main context).
The **artifact** the subagent writes follows the doc rules below.

## Doc caveman split (this repo's ratified choice)

| Surface | Level | Why |
|---|---|---|
| Chat + machine-read tool-results | ultra | token budget |
| Engineering specs/plans/tasks, AGENTS.md, internal notes | lite | terse but buildable |
| `docs/IMPROVEMENT_PLAN.md`, `README`, onboarding site, student-facing copy | none (plain prose) | mission = lower skill floor; maintainer not a strong coder |
| Precision artifacts: FR/SC lists, Given/When/Then, task IDs, code blocks, commit msgs | exact, never compressed | meaning must not drift |

Hard stop: never caveman-compress human-facing docs or precision artifacts. This
overrides a blanket "caveman everything" instruction — see constitution + the
dispatch policy that shipped this split.

## Compose a dispatch

```
Agent(
  subagent_type: <Explore | general-purpose | specific>,
  model: <haiku | sonnet | opus>,            # by complexity, not inherited
  description: "<3-5 words>",
  prompt: "<if report-back should be terse: 'Report back caveman-ultra:
            drop articles/filler, fragments OK, code/precision exact. Then:'>
           <task + what to read + exact output paths>"
)
```

Parallel wave → each member picks its own tier+level independently.

---

## Repo-specific hard rules (always, every agent)

- **PHI never** in logs, tests, telemetry, or this repo. Synthetic data only
  (`tests/synthetic_data.py`). See constitution Principle I.
- **Tests run offline** — no XNAT server, no VPN, no PHI. Server paths sit behind
  a seam a fake replaces (Phase 1 `FakeXNAT`). Principle IV.
- **No credentials in argv/commits/logs.** No hardcoded server URL/project.
  Principle V.
- **Fail softly**: no raw traceback for foreseeable problems; give the user a next
  step. Principle II.
- Build order = `specs/` by number; each phase's `tasks.md` top-to-bottom, `[P]`
  parallelizable. Phase 1 is prereq for Phase 2.

## Map

- `docs/IMPROVEMENT_PLAN.md` — narrative plan (plain prose).
- `.specify/memory/constitution.md` — 6 gates.
- `specs/NNN-*/` — per-phase spec + plan + tasks.
- `tests/` — offline suite + synthetic data + (Phase 1) `fakes/fake_xnat.py`.
