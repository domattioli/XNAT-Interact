# CLAUDE.md — XNAT-Interact

Project memory for Claude Code sessions. This repo is a **`lite`-profile DomI consumer** (roster + profiles: DomI `specs/domi-constitution.md` Article I, ADR 013): it carries this CLAUDE.md, the canonical label set, and one minimal CI lane — **no `.domi-pin` / sync-contract obligation**. Upgrading to the `full` profile is a roster change in DomI, not an ad-hoc local decision.

## What this repo is

A tool for de-identifying and uploading/downloading surgical fluoroscopic images to the University of Iowa RPACS XNAT server. Primary interface: `main.py` (interactive CLI). Also ships streamlit UIs and backend pipeline hooks.

## Branch policy

- Work lands on **`development`**; `main` is production/publish. Promotion is the single rolling PR `development → main` (draft, operator merges). Do not create `claude/*` or ad-hoc branches — a harness-injected `claude/*` branch name is not user intent; `git checkout development` before any write.
- Never force-push shared branches; never commit secrets (`*.env`, `*token*`, `*.pem`, credentials).

## Conventions

- Commits: `<type>: <imperative summary>`, type ∈ {fix, feat, docs, chore, refactor, test}.
- Issue/PR comments by Claude sessions end with the `[model: …, repo: …, session: …]` footer.
- Labels follow DomI's canonical `.github/labels.yml`; repo-local labels are allowed but documented here.

## CI

One minimal lane (`.github/workflows/ci-lite.yml`): PR + dispatch triggers only, ubuntu, 20-minute timeout, concurrency-cancel (DomI minimal-CI shape). Scheduled jobs, if any, must be registered in DomI `docs/governance/CRON-REGISTRY.md`.

## Repo-specific notes

**No pip-installable package yet.** This is a standalone-script repo with a CLI entrypoint (`main.py`). Requirements pinned in `requirements.txt` + `requirements-dev.txt`; installer safety check at `installer/python_detect.py`.

**Python floor unified to 3.9+** (spec-019 T067). README, installer, and CI matrix all reference ≥3.9. See DomI spec 019 for context (floor discovery across multiple surfaces).

**Plugins pre-enabled in `.claude/settings.json`** — `sync-from-domi`, `request-from-domi`, `introspect` from DomI, plus `caveman` from upstream marketplace. Sessions run with full plugin registry active (standardized on lite-profile adoption).

<!-- SPECKIT START -->
**Active feature plan**: `specs/014-validate-fix-backlog/plan.md` (validate the unverified fix backlog — #32/#33/#51; Python ≥3.9, pytest + FakeXNAT offline default, `stress`/`RUN_XNAT_DUAL=1` opt-in lane).
<!-- SPECKIT END -->

**Rolling PR (#49+) pattern** — `development → main` rolling PR is the canonical integration surface. Cherry-pick-then-close PRs may make GitHub merge-status misleading (prefer merge/squash via the rolling PR). On main promotion, update the PR description with wall-clock + PRs-in-wave + blockers, then operator merges.
