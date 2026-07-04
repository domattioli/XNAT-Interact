<!--
PR Template for DomI and downstream repos.
Fill out each section. Keep PRs small and atomic.
-->

## Summary

<!-- 1-3 bullets: what this PR changes and why -->

## Type

<!-- Check one. Mirrors the "type:" labels. -->

- [ ] bug — fixes broken behavior
- [ ] feat — new capability
- [ ] docs — documentation only
- [ ] chore — tooling/dependencies/maintenance
- [ ] refactor — internal restructure, no behavior change
- [ ] test — adds or improves tests

## Scope

<!-- Check one or more -->

- [ ] skill — affects skills/
- [ ] plugin — affects plugins/
- [ ] ci — affects .github/workflows/
- [ ] docs — affects README/CLAUDE.md/MANIFEST
- [ ] claude-md — modifies CLAUDE.md instructions

## Compliance Checklist

<!-- All boxes must be checked before merging. -->

- [ ] I read CLAUDE.md before starting
- [ ] `bash scripts/instructions_on_start.sh` reports HEALTHY (if applicable)
- [ ] If I added a skill: it's documented in MANIFEST.md
- [ ] If I changed a skill: SKILL.md frontmatter is valid (name + description)
- [ ] No unrelated changes mixed in (single-purpose PR)
- [ ] No new dependencies added without explicit need
- [ ] No PATs, secrets, or .env contents in commits

## Test Plan

<!-- How did you verify this works? Failing here = blocked. -->

- [ ] ...

## Related Issues

<!-- Link any related issues, e.g. "Closes #14" -->

---

If this PR was Claude-driven, add the `claude-driven` label.
