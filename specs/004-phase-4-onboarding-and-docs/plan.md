# Implementation Plan: Onboarding & Static Docs Site (Phase 4)

**Branch**: `004-phase-4-onboarding-and-docs` | **Date**: 2026-06-04 | **Spec**: [spec.md](spec.md)

## Summary

Reduce the first adoption cliff — getting access — and give the mission a way to
measure itself. Add an **in-app onboarding checklist** (① account ② added to
project ③ VPN) that reuses the Phase 1 services and sits on the Phase 2 login
screen, with a one-action **access-request draft** to the Data Librarian. Ship a
**small static onboarding site** on GitHub Pages that stores **no data**. Add a
**PHI-free, opt-in, aggregate-only** local metrics log for the four adoption
metrics. No analytics service; no PHI anywhere.

## Technical Context

**Language/Version**: Python 3.11 (CI) for the in-app checklist + metrics; the
static site is plain Markdown/HTML (no Python runtime needed to view it).
**Primary Dependencies**: existing — Phase 1 `src/services/` (`preflight`,
`errors`, `config`, `xnat_gateway`) and Phase 2 login screen for the checklist;
the static site uses GitHub Pages with either raw Markdown or a lightweight static
generator (e.g. Jekyll/MkDocs) — no app framework, no JS analytics. `pytest`
(existing).
**Storage**: metrics = a **local append-only JSON** file (opt-in); no server, no
database. The static site stores **nothing**.
**Testing**: `pytest`, offline, against `tests/synthetic_data.py` + Phase 1
`FakeXNAT` + a stubbed reachability check; a static-site content/link check that
runs against the built output with no network.
**Target Platform**: Windows + macOS desktops for the app; any browser for the
static site.
**Project Type**: Single-project app + a static-docs sub-tree (`docs/site/`).
**Constraints**: No network in tests; no PHI in logs/telemetry/repo; no
credentials anywhere; metrics opt-in + aggregate-only; the site is truly static.
**Scale/Scope**: 3 onboarding checks, 1 access-request draft, ~5 static content
blocks, 1 metrics module covering 4 success metrics.

## Constitution Check

*GATE: must pass before and after design.*

- **I — PHI Safety**: The static site stores **no data** (FR-007) so it cannot
  hold PHI. Metrics are PHI-free by construction — allowlisted non-PHI fields only,
  opt-in, aggregate-only (FR-008, FR-009), with a test asserting no
  patient/subject/credential field can appear. ✅ Central guard of this phase.
- **II — Fail Softly**: The checklist surfaces a clear next step for every state
  and never dead-ends — including VPN-check-offline (FR-003) and no-email-client
  (spec Edge Cases / SC-002); reuses the Phase 1 friendly-error helper. ✅ Central.
- **III — Lower Skill Floor**: Onboarding is the *first* cliff; the checklist +
  one-action access-request draft + the 2-minute static explainer remove
  terminal/README guesswork from getting started. ✅ Central.
- **IV — Offline Testable**: All checks run against `FakeXNAT` + a stubbed
  reachability check; the static-site check runs against built output; no network
  (FR-005, FR-012, SC-006). ✅
- **V — Config/No Secrets**: No credentials handled; the access-request draft
  carries only a HawkID + project name (no password); server URL/project come from
  Phase 1 `config.py`. Metrics log carries no secrets (FR-009). ✅
- **VI — Data Integrity**: Metrics are local append-only (no shared-state race);
  no destructive or shared-server writes introduced. The static site is read-only.
  ✅ (no new shared-state surface).

No violations. No complexity-tracking exceptions needed.

## Project Structure

```text
specs/004-phase-4-onboarding-and-docs/
├── spec.md
├── plan.md      # this file
└── tasks.md

src/
└── services/                 # reuses Phase 1 seam
    ├── onboarding.py         # NEW: OnboardingStatus checks + AccessRequest draft
    └── metrics.py            # NEW: opt-in, PHI-free, aggregate-only event log + summary

docs/
└── site/                     # NEW: static GitHub Pages onboarding site (NO data)
    ├── index.md              # what-is-this / how-to-start (≤2 min) + download link
    ├── _config / mkdocs.yml  # lightweight generator config (or raw HTML if no gen)
    └── assets/               # static images/css only — no JS analytics, no forms

tests/
├── test_onboarding_status.py   # NEW: 3-check status matrix + next-step strings
├── test_access_request_draft.py# NEW: draft addresses Librarian, HawkID/project filled
├── test_metrics_phi_free.py    # NEW: only allowlisted fields; opt-out = zero writes
├── test_metrics_summary.py     # NEW: aggregates the 4 success metrics
└── test_site_content.py        # NEW: required blocks present; build is static-only

.github/workflows/
└── pages.yml                 # NEW: build + publish docs/site/ to GitHub Pages
```

## Approach (phased within the phase)

1. **Onboarding checks first** (US1): build `onboarding.py` on top of the Phase 1
   `preflight`/`xnat_gateway`/`errors` seam — three status checks + the
   `AccessRequest` draft. Wire it into the Phase 2 login screen. Test the full
   status matrix and the no-email-client fallback offline.
2. **Static site** (US2): author `docs/site/` content (the five blocks), pick the
   lightest build path (raw Markdown or a thin generator), add `pages.yml`, and a
   content/link check against the built output.
3. **Metrics** (US3): build `metrics.py` as an opt-in, append-only JSON log with a
   strict field allowlist and a maintainer summary for the four metrics; prove
   PHI-free and opt-out-silent by test.

Each step lands as its own commit with tests green. US1 depends on the Phase 1
services; US2 and US3 are independent and can proceed in parallel.

## Risks & Mitigations

- *Metrics drifting into PHI*: the single biggest risk → enforce a strict field
  **allowlist** at the write boundary and assert it in `test_metrics_phi_free.py`;
  reject any record carrying a non-allowlisted key rather than silently dropping
  it. Opt-out is the default.
- *Site stops being truly static*: a form or analytics snippet would reintroduce a
  data-collection surface → `test_site_content.py` fails the build if any form,
  endpoint, or analytics tag appears; keep the site to static assets only.
- *VPN-check false confidence*: a timed-out reachability probe must read as
  "unknown", not "done" → model three states (done/not-done/unknown) and test the
  offline-check path explicitly.
- *Link rot to the Phase 3 deliverable*: the download link could break →
  link-check the built site; keep the checklist independent of the site so the app
  never depends on Pages being up.
