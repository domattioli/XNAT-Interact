# Feature Specification: Onboarding & Static Docs Site (Phase 4)

**Feature Branch**: `004-phase-4-onboarding-and-docs`
**Created**: 2026-06-04
**Status**: Draft
**Input**: `docs/IMPROVEMENT_PLAN.md` Phase 4 (static onboarding site) + "Adoption & process" refinement (onboarding checklist) + "Success metrics" section

## Overview

Attack the *first* cliff a new student hits — getting access — and give the
mission (adoption) a way to measure itself. Two parts: (a) an **in-app
onboarding checklist** that shows, before any real work, whether the student has
an XNAT account, has been added to the project by the Data Librarian, and is on
the UIowa VPN — and can **draft the access-request email** to the Librarian; and
(b) a **small static onboarding website** (GitHub Pages) that explains the tool
in two minutes and links to the download/setup. A lightweight, **PHI-free,
opt-in, aggregate-only** local event log captures the adoption success metrics.
The static site is the *only* truly-hosted piece and stores **no data**.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Am I set up? The checklist tells me, and drafts the email (Priority: P1)

A brand-new student opens the app and, before being asked to do anything, sees a
plain status screen: **① account ② added to project ③ VPN connected**, each
marked done / not-done / unknown with a clear next step. If access is missing,
the app can **draft an access-request email** to the Data Librarian (HawkID
prefilled) so the student doesn't have to know who to ask or what to say.

**Why this priority**: First-week access friction (account → project add → VPN)
is a documented adoption killer — a student who can't get past it never uploads
anything. This is the very first skill-floor cliff (Constitution III) and the
single highest-leverage onboarding fix.

**Independent Test**: Drive each combination of the three checks against the
fake-XNAT layer and a stubbed VPN/reachability check; assert the checklist shows
the correct per-item status and a concrete next step, and that the email-draft
path produces a Librarian-addressed draft with the HawkID filled in — fully
offline, no real email sent.

**Acceptance Scenarios**:

1. **Given** a student with no XNAT account, **When** the checklist runs, **Then**
   item ① shows "not done" with "register at <XNAT> with your HawkID" and offers
   to draft the access-request email.
2. **Given** an account exists but the student is not on the project, **When** the
   checklist runs, **Then** item ② shows "not done" with "ask the Data Librarian
   to add you" and the draft email names the project.
3. **Given** the VPN is disconnected, **When** the checklist runs, **Then** item ③
   shows "not connected — connect to the UIowa VPN", never a raw connection
   traceback.
4. **Given** all three items pass, **When** the checklist runs, **Then** it shows
   "you're set up" and points to the next step (upload), not a dead end.

---

### User Story 2 - A friendly page explains the tool and gets me started (Priority: P2)

A prospective student is pointed at a single URL (GitHub Pages) before they ever
touch the app. In about two minutes it tells them what the tool is, how to get
started (download/setup link to the Phase 3 deliverable), reminds them they need
the UIowa VPN, notes the annual SSL-cert caveat, and says exactly who to contact
(the Data Librarian).

**Why this priority**: Lowers the very first skill-floor barrier — orientation —
before any install. Valuable but ranks below the in-app checklist because it is
informational, not on the upload path. It is the only hosted piece and stores
**no user data**, so the PHI constraint does not apply.

**Independent Test**: Build the site locally; assert each required content block
is present (what-is-this, get-started/download link, VPN reminder, SSL-cert
caveat, who-to-contact) and that the build emits only static assets — no forms,
no analytics, no server endpoint. Link-check runs offline against the built
output.

**Acceptance Scenarios**:

1. **Given** the published site, **When** a student opens it, **Then** they see a
   ≤2-minute "what is this / how do I get started" explainer and a working
   download/setup link to the Phase 3 deliverable.
2. **Given** the site, **When** a student reads it, **Then** the VPN reminder, the
   SSL-cert renewal caveat, and the Data Librarian contact are all present.
3. **Given** the built site, **When** it is inspected, **Then** it contains only
   static assets and collects no user input or telemetry.

---

### User Story 3 - We can tell whether adoption is improving (Priority: P3)

The maintainer wants to know, before/after the improvement work, whether students
adopt the tool: time-to-first-upload, upload error rate, active uploaders per
term, and support tickets to the Librarian. The app keeps a **local, opt-in,
aggregate-only, PHI-free** event log the maintainer can summarize — no analytics
service, no PHI ever.

**Why this priority**: The mission is adoption; without a measurement it is
guesswork. It ranks P3 because it improves the *project's* feedback loop, not the
individual student's immediate experience, and must never compromise P1/P2 or
PHI safety.

**Independent Test**: Emit each event kind through the metrics module; assert the
written records contain only the allowlisted non-PHI fields (event name, coarse
timestamp, success/fail, duration bucket), that opt-out writes nothing, and that
a summary aggregates the four metrics — all offline, asserting no PHI field can
appear.

**Acceptance Scenarios**:

1. **Given** metrics are opted in, **When** an upload completes, **Then** a record
   with event kind, coarse timestamp, and outcome is appended — and contains no
   patient/subject/credential field.
2. **Given** metrics are opted out (default until consented), **When** events
   occur, **Then** nothing is written.
3. **Given** a log of events, **When** the maintainer runs the summary, **Then** it
   reports time-to-first-upload, upload error rate, active uploaders, and a
   ticket proxy — all aggregate, none per-patient.

---

### Edge Cases

- **Student already fully onboarded**: checklist shows all-green and gets out of
  the way immediately (no nagging, no forced re-run), pointing straight to upload.
- **VPN/reachability check itself is offline or times out**: item ③ shows
  "couldn't check — are you on the VPN?" as a *recoverable unknown*, never a crash
  or a false "done".
- **Metrics opt-out / never consented**: absolutely nothing is logged; the app
  works identically. Opt-in is revocable and the prior log can be cleared.
- **No email client / can't send**: if a draft cannot be handed to a mail app, the
  checklist shows the ready-to-copy address + body text so the student can send it
  manually — never a dead end and never a traceback.
- **GitHub Pages down / link rot**: the in-app checklist does not depend on the
  static site being reachable; the site is orientation, not a runtime dependency.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST present an onboarding checklist with three items —
  ① XNAT account ② added to project by the Data Librarian ③ UIowa VPN connected —
  each showing done / not-done / unknown and a concrete next step.
- **FR-002**: For any not-done item, the checklist MUST offer to **draft (or send)
  an access-request message** to the Data Librarian, prefilled with the student's
  HawkID and the project name. (Implementation-agnostic: drafting a copyable
  message satisfies this; sending via an available mail path is an allowed
  extension.)
- **FR-003**: The VPN/reachability check MUST fail **softly** — an offline or
  timed-out check shows a recoverable "couldn't check / are you on the VPN?"
  state, never a raw traceback and never a false "done" (Constitution II).
- **FR-004**: When all three items pass, the checklist MUST surface the **next
  step** (proceed to upload) and MUST NOT block or nag an already-onboarded
  student.
- **FR-005**: The onboarding checks MUST be testable offline against the fake-XNAT
  layer and a stubbed reachability check — no live server, VPN, or real email.
- **FR-006**: System MUST publish a **static** onboarding site (GitHub Pages)
  containing: a ≤2-minute what-is-this / how-to-start explainer, the
  download/setup link to the Phase 3 deliverable, the UIowa VPN reminder, the
  annual SSL-cert renewal caveat, and the Data Librarian contact.
- **FR-007**: The static site MUST store **no user data** — no forms, no
  credentials, no telemetry, no server-side endpoint; build output is static
  assets only.
- **FR-008**: System MAY record adoption metrics locally; if it does, metrics MUST
  be **opt-in** (off until the student consents), **aggregate-only**, and
  **PHI-free** — records limited to an allowlist (event kind, coarse timestamp,
  success/fail, duration bucket).
- **FR-009**: The metrics log MUST NEVER contain PHI, patient/subject identifiers,
  file paths, credentials, or secrets; a test MUST assert only allowlisted fields
  are present.
- **FR-010**: Metrics MUST be capturable for the four success metrics —
  time-to-first-upload, upload error rate, active uploaders per term, and a
  support-ticket proxy — and summarizable by the maintainer **without** any
  analytics service.
- **FR-011**: Opting out (the default) MUST result in **zero** writes; opt-in MUST
  be revocable and the existing log clearable.
- **FR-012**: All Phase 4 code (checklist + metrics) MUST ship with offline tests;
  no path may require the live server, VPN, or real PHI.

### Key Entities

- **OnboardingStatus**: the three-item state — account / project-membership / VPN —
  each with a status (done / not-done / unknown) and a next-step string.
- **AccessRequest**: a draft message to the Data Librarian — recipient, student
  HawkID, project name, body — independent of how (or whether) it is sent.
- **MetricEvent**: one PHI-free, aggregate-only record — event kind, coarse
  timestamp, outcome (success/fail), duration bucket; nothing patient-identifying.
- **SitePage**: a static onboarding page's content blocks (explainer, get-started
  link, VPN reminder, SSL-cert caveat, contact) rendered to static assets only.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A new student can determine their access status (account / project /
  VPN) from the in-app checklist in **under a minute**, without reading the README
  or asking a person — verified by walkthrough + test of all status combinations.
- **SC-002**: For every not-done item, the student is given a concrete next step
  and a one-action access-request draft to the Librarian — zero dead-ends
  (enforced by test, including the no-email-client path).
- **SC-003**: The static site presents all five required content blocks and a
  working download/setup link, and a build inspection confirms **zero** user-data
  collection (no forms / telemetry / endpoint).
- **SC-004**: The metrics log, across all event kinds, contains **only**
  allowlisted non-PHI fields — a test asserts no patient/subject/credential field
  can ever appear; opt-out yields zero records.
- **SC-005**: The maintainer can produce the four adoption metrics
  (time-to-first-upload, upload error rate, active uploaders/term, ticket proxy)
  from the local log with no analytics service.
- **SC-006**: All Phase 4 tests run in CI with no network and no PHI.

## Assumptions

- The fake-XNAT layer from Phase 1 can report whether a (fake) account exists and
  whether the student is a project member, so onboarding checks are faithfully
  testable offline.
- "Draft/send an access request" is satisfied by producing a copyable,
  Librarian-addressed message; a send-capable mail path (an ecosystem skill
  exists) is an allowed enhancement, not a requirement, keeping the spec
  implementation-agnostic.
- GitHub Pages is an acceptable host because the site stores no data and the PHI
  constraint therefore does not apply; the site is never a runtime dependency of
  the app.
- Metrics are opt-in and default-off; until a student consents, the tool behaves
  exactly as if metrics did not exist.
- The "support-ticket proxy" is an in-app signal (e.g. a "contact the Librarian"
  action count), not access to the Librarian's real ticket queue.
