# Feature Specification: Web-Reachable App Interface & Cloud Prototyping (010)

**Feature Branch**: `development`
**Created**: 2026-06-09
**Status**: Draft (spec only — hosting model OPEN, pending discussion on [#37](https://github.com/domattioli/XNAT-Interact/issues/37))
**Input**: [#37](https://github.com/domattioli/XNAT-Interact/issues/37) (onboarding-site vs. interface clarification + web-reachable plan). Builds on spec `002-phase-2-streamlit-app` (the local Streamlit app), `003-phase-3-packaging-and-install` (desktop delivery), and `004-phase-4-onboarding-and-docs` (the static Pages site). Uses the existing `tests/fakes/fake_xnat.py` FakeXNAT + the `app/logic/auth.py` `connect_factory` dependency-injection seam.

## Overview

Today the interface is the **local** Streamlit app (`app/`, spec 002) delivered as a desktop install (spec 003); the only hosted artifact is the **static** onboarding page (`docs/site/`, spec 004). There is no clickable, web-reachable interface — and there cannot be a naive one, because two constraints are load-bearing:

1. **VPN reachability** — the UIowa XNAT server is only reachable on the UIowa VPN. A public host is not on the VPN and cannot reach XNAT.
2. **PHI confinement** — surgical fluoroscopic images + credentials are PHI; the design keeps PHI on the student's machine ("PHI never leaves the machine except to the XNAT server", spec 002). Routing PHI through a third-party host inverts the threat model and triggers IRB/HIPAA/UIowa-security review.

This spec does two separable things:
- **(A) Pick and implement a hosting model** for a web-reachable *entry point* to the interface that preserves both constraints.
- **(B) Establish a cloud-session prototyping workflow** (FakeXNAT, offline) so the interface can be built and validated from ephemeral Claude Code routine sessions that have no VPN, no real XNAT, and no PHI.

(B) is independent of (A) and should land first — it is how we build (A) safely.

## User Scenarios & Testing *(mandatory)*

### User Story 1 — Prototype the interface from a cloud session with no XNAT/VPN/PHI (Priority: P1)

A Claude Code routine session running in an ephemeral cloud container needs to build and validate a Streamlit page without a live XNAT server, the VPN, or any PHI. It runs the real `app/pages/*.py` against the injected FakeXNAT and asserts the UI behaves, fully offline, deterministically, in CI.

**Why this priority**: This is the precondition for *all* interface work in the routine-session model. Without it, every UI change is unverifiable until a human runs it on a VPN-connected machine. It also has zero PHI/VPN risk.

**Independent Test**: With no network and no real credentials, drive `app/pages/onboarding.py` via `streamlit.testing.v1.AppTest` with a FakeXNAT-backed `connect_factory`; assert the three-step checklist renders the correct per-item status and the draft-email action produces a Librarian-addressed draft — all in-process, no browser, no server.

### User Story 2 — A reviewer clicks a link and sees the working interface (synthetic data) (Priority: P2)

A stakeholder (maintainer, Data Librarian, prospective student) opens a URL and interacts with a live instance of the app populated with **synthetic** data, to evaluate UX — without installing anything and without touching real XNAT/PHI.

**Why this priority**: Turns "trust the screenshots" into "click and try it." Drives adoption/feedback. Safe because it is FakeXNAT + synthetic data only.

**Independent Test**: Deploy the demo build to a host; load the URL; confirm a visible "DEMO — synthetic data, not connected to XNAT" banner, that login/browse/upload flows work against FakeXNAT, and that supplying real XNAT credentials/URL is refused (fail-closed).

### User Story 3 — A student opens the real interface from the onboarding page (Priority: P2)

From the static Pages site, a student clicks "Open the app" and lands in the **real** interface for real work — without breaking PHI confinement.

**Why this priority**: Closes the loop between the info page and the tool. Must preserve the PHI/VPN posture, so the *mechanism* depends on the hosting decision (see Hosting Models).

**Independent Test**: For the chosen model, assert the "Open the app" affordance routes to the correct target (b1: localhost/installed-app launch; b2: the UIowa-internal URL) and that no PHI path is exposed to a public host.

### Edge Cases
- Real XNAT URL/credentials supplied to a demo/hosted instance → must refuse to start or hard-disable the real `connect_factory` (fail-closed), never silently connect.
- Demo instance left running indefinitely → synthetic data only; no PHI can accumulate; banner always present.
- Localhost deep-link (b1) when the app isn't installed/running → graceful "install or launch the app" fallback, not a dead link.
- Cloud session has no outbound tunnel capability (network policy) → prototyping must not depend on tunnels; rely on push-triggered deploy + in-process AppTest.

## Hosting Models (decision OPEN — pick one in review of #37)

- **b1 — localhost deep-link (recommended; smallest blast radius)**: app stays local/desktop (spec 002/003); the Pages "Open the app" button deep-links to the locally-installed app (`http://localhost:<port>` / OS launcher). No hosting; PHI stays local. Web-reachable *entry point*, local *execution*.
- **b2 — UIowa-internal host**: a Streamlit container on a UIowa-managed, VPN-side server. Web-reachable within UIowa; PHI stays in UIowa infra. Requires IT/security sign-off + infra access.
- **b3 — public host (Streamlit Cloud / Render / Fly) for REAL data**: **rejected** — breaks VPN reachability and PHI confinement. (Public hosting is permitted ONLY for the FakeXNAT synthetic demo of User Story 2, never real data.)

## Requirements *(mandatory)*

### Functional Requirements
- **FR-001**: A `DEMO_MODE` switch MUST select the FakeXNAT `connect_factory` and disable the real `xnat_gateway.build_server` path. When `DEMO_MODE` is on and a real XNAT URL/credential is supplied, the app MUST fail closed (refuse to start), not connect.
- **FR-002**: The app MUST be exercisable headless via `streamlit.testing.v1.AppTest` with an injected FakeXNAT factory, with no browser, server, or network.
- **FR-003**: A demo build MUST display a persistent, non-dismissable "DEMO — synthetic data, not connected to XNAT" banner on every page.
- **FR-004**: The demo deploy MUST be push-triggered from a designated branch (host pulls from GitHub) so it does not depend on a long-lived process inside an ephemeral routine session, nor on outbound tunnels.
- **FR-005**: The demo branch/build MUST contain only synthetic fixtures; CI MUST secret-scan it (no real credentials, URLs, or PHI).
- **FR-006**: A per-session screenshot artifact MUST be producible headlessly (`streamlit run --server.headless true` + headless browser) and attachable to the PR / committed to a `docs/screens/` gallery.
- **FR-007**: The Pages site "Open the app" affordance MUST route per the chosen hosting model (b1 localhost/launcher, or b2 internal URL) and MUST NOT expose any real-data path to a public host.
- **FR-008**: No change to the PHI/VPN posture for **real** data: real work stays on the VPN-connected machine / UIowa-internal infra. [NEEDS CLARIFICATION: b1 vs b2 — final model from #37.]

### Key Entities
- **DEMO_MODE config** — flag + the factory-selection + fail-closed guard.
- **FakeXNAT factory** — existing `tests/fakes/fake_xnat.py`, adapted as a `connect_factory` for demo/test injection.
- **Demo fixtures** — synthetic cases/images/users for the hosted demo.
- **AppTest suites** — `tests/test_app_<page>_ui.py`, one per page, complementing the existing `test_app_*_logic.py`.

## Success Criteria *(mandatory)*

### Measurable Outcomes
- **SC-001**: 100% of `app/pages/*.py` have at least one `AppTest` that passes offline in CI (no network, no real creds).
- **SC-002**: A reviewer can reach a working synthetic-data demo URL and complete login→browse→(simulated)upload in under 2 minutes, with the demo banner visible throughout.
- **SC-003**: Supplying real XNAT credentials/URL to any hosted/demo instance results in a refusal (fail-closed) — verified by an automated test.
- **SC-004**: A routine session, with no VPN/XNAT/PHI, produces updated AppTest results + page screenshots and pushes them, with the demo URL redeploying automatically.
- **SC-005**: Zero real PHI or credentials present in the demo branch (secret-scan + fixture audit clean).

## Assumptions
- `streamlit` is available as an app dependency (now listed in `requirements.txt`, unpinned) → `streamlit.testing.v1.AppTest` is usable in CI. A pin should be added for reproducibility.
- The existing `app/logic` / `app/pages` separation and the `connect_factory` seam are stable enough to inject FakeXNAT without refactoring locked logic.
- The hosted synthetic demo (User Story 2) is acceptable to publish publicly *because* it is FakeXNAT + synthetic data; this is distinct from hosting real data.
- The b1-vs-b2 choice for real-data reachability is an operator/IT decision tracked in #37; this spec implements (B) prototyping regardless, and (A) once the model is chosen.

## Out of Scope
- Hosting **real** data on a public service (b3) — rejected.
- IRB/HIPAA paperwork itself (this spec records the constraints; approval is an external process).
- Changes to the de-identification / upload logic (spec 001/002 own that).
