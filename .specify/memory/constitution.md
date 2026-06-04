# XNAT-Interact Project Constitution

**Version**: 1.0.0
**Ratification Date**: 2026-06-04
**Last Amended**: 2026-06-04
**Status**: Draft (pending maintainer ratification)

---

## Purpose

XNAT-Interact helps **student researchers** move surgical/fluoroscopic image data
to and from the UIowa XNAT RPACS server. The mission is **adoption**: make the
tool easy enough that students actually use it to stay organized, and reliable
enough that it keeps a human in the loop instead of failing loudly. This
constitution defines the non-negotiable principles every feature spec is checked
against (the "Constitution Check" gate in each `plan.md`).

---

## Principle I — Patient Privacy Is Sacrosanct (PHI Safety First)

No feature MAY upload data that has not been de-identified, and de-identification
MUST cover **both metadata and burned-in pixel content**. Any code path that
sends data off the machine MUST first pass a de-identification step; where
automated pixel-PHI removal is not yet available, a **human-in-the-loop
confirmation** is mandatory. Local PHI artifacts (intake forms, zips, downloaded
config) MUST be cleaned up after use. PHI MUST NEVER be written to logs,
telemetry, tests, or this repository.

**Rationale**: This is a medical tool handling identifiable patient images. A
single privacy breach is more costly than any missed feature. Metadata-only
scrubbing is a known gap (the burned-in-pixel case) and must be closed before it
can be trusted.

**Gate**: Does this feature touch image data or move data off-machine? If so, it
MUST show how de-identification (incl. pixel review) and local cleanup are
preserved, and ship a test proving PHI is removed.

---

## Principle II — Fail Softly, Keep the Human in the Loop

The tool MUST NOT crash with a raw traceback for any foreseeable problem (no VPN,
wrong password, bad path, expired cert, malformed input). Every failure MUST
produce a **plain-language message + a next step**: retry, skip, save progress,
or contact the Data Librarian. Bare `except:` that hides the real error is
forbidden. `assert` MUST NOT be used for user-facing validation. One bad input
MUST NOT discard a whole batch or session.

**Rationale**: The target users are not developers. A confusing crash with no
recourse is the #1 reason they abandon the tool — which directly defeats the
adoption mission.

**Gate**: For each new failure mode, what does the user see, and what are their
options? "Crashes" or "prints a traceback" is not an acceptable answer.

---

## Principle III — Lower the Skill Floor

Features MUST reduce, never increase, the terminal/Git/Python knowledge required
of a student. Prefer GUIs, file pickers, dropdowns (sourced from existing config)
and dated/known options over free-text typing and remembered commands. The
command line MAY remain available as an **optional** path for those who want to
learn it, never as the only path.

**Rationale**: The mission is adoption by non-technical student researchers.
Every avoidable terminal step is a place we lose a user.

**Gate**: Does a first-time student need to open a terminal, run Git, or type a
path by hand to use this feature? If yes, justify why no friendlier affordance
is possible.

---

## Principle IV — Testable Offline, Without a Server or PHI

Every feature MUST be testable **without** the live XNAT server, the UIowa VPN,
or any real patient data. Logic that talks to the server MUST sit behind a seam
that a fake/mock can replace. Synthetic data generators (`tests/synthetic_data.py`)
are the only source of test fixtures. New behavior ships with tests; CI runs them.

**Rationale**: The tool's dependency on a VPN-gated PHI server historically made
it untestable, which made every change risky. Offline testability is what lets
us improve the tool safely and in bulk.

**Gate**: Can this feature's tests run in CI with no network and no PHI? If a
path needs the server, where is the fake that stands in for it?

---

## Principle V — Configuration Over Hardcoding; No Secrets in Code

Server URLs, project names, and environment specifics MUST live in configuration,
not source, so test and production are switchable without code edits. Credentials
MUST NEVER be accepted as command-line arguments, committed, or logged; they live
only in a session prompt / secure field (and optionally the OS keychain).

**Rationale**: Hardcoded endpoints block testing and safe rollout; credentials in
argv/history/logs are an avoidable leak.

**Gate**: Does this feature introduce any hardcoded endpoint, project name, or
credential handling? If so, it does not pass.

---

## Principle VI — Data Integrity at Scale

Shared state (notably the `MetaTables.json` config "database") MUST be updated in
a way that tolerates concurrent users without silent lost updates. Uploads MUST
be verifiable (the tool confirms the server received what was sent) and, where
feasible, resumable. Destructive operations MUST require explicit confirmation
and offer a dry-run.

**Rationale**: Multiple students upload concurrently. Last-write-wins on shared
metadata silently corrupts the catalog; unverified uploads erode trust.

**Gate**: Does this feature read or write shared state, or perform a destructive
or long-running network operation? Show how concurrency, verification, and
confirmation are handled.

---

## Amendment Process

This constitution is amended by the maintainer (Data Librarian / repo owner) via
a PR that updates this file and bumps the version (semver: MAJOR for a
principle removed/redefined, MINOR for a new principle/section, PATCH for
clarifications). Each phase's `plan.md` MUST include a "Constitution Check"
section that maps the feature to the gates above.
