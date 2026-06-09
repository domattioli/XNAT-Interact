# Session Handoff: web-reachable-app spec + CI/plugin fixes

**Date:** 2026-06-09  **Project:** /home/user/XNAT-Interact (`development`)  **Phase:** planning + delivery

## What We Did
Clarified the Pages site is a static info page by design (spec 004), not the interface (the Streamlit `app/`, spec 002). Wrote the web-reachable-app plan as **`specs/010-web-reachable-app/spec.md`**. Filed **issue #37** for brainstorm/critique. Shipped CI/plugin fixes on **PR #31**.

## Where Things Are
- **Plan**: `specs/010-web-reachable-app/spec.md` (Status: Draft — hosting model b1/b2 OPEN, pending #37).
- **Discussion**: issue #37 (site clarification + plan + open questions).
- **PR #31** (`development → main`): plugin enablement + conda→pip CI migration; CI green.
- **Tag**: blocked by env git proxy (no `refs/tags/*` push); workaround branch `baseline-v0.1.0` @ `1ea13e0`. Operator to cut real `v0.1.0`.

## Decisions
- Pages ≠ interface; can't host Streamlit on static Pages → split is intentional.
- Prototype via FakeXNAT (`tests/fakes/fake_xnat.py` + `app/logic/auth.py connect_factory`), never real XNAT — keeps PHI/VPN posture.
- Recommend b1 (localhost deep-link) for real-data reachability; public host only for the synthetic FakeXNAT demo.

## Next Steps
1. [ ] Resolve b1-vs-b2 on #37.
2. [ ] Land L1 walking skeleton: one onboarding-page `AppTest` vs FakeXNAT, wired into CI (spec 010 FR-002, SC-001).
3. [ ] Then `plan.md`/`tasks.md` for spec 010.
4. [ ] Operator: cut `v0.1.0` tag at `1ea13e0`; delete `baseline-v0.1.0`.
5. [ ] Confirm Pages Source = "GitHub Actions" so `pages.yml` stays green.

## Files to Review on Resume
- `specs/010-web-reachable-app/spec.md` — the plan.
- `app/main.py`, `app/pages/onboarding.py`, `app/logic/onboarding.py` — interface + offline logic.
- `tests/fakes/fake_xnat.py`, `app/logic/auth.py` — the prototyping seam.
- Issue #37, PR #31.

## Notes
- caveman plugin not loaded this container (`Skill caveman:caveman` → Unknown); fixed in `.claude/settings.json` for next session (DomI #168); emulated ultra meanwhile.
- `streamlit` now in `requirements.txt` (unpinned); `pytest.ini` reworked by operator (testpaths=tests, markers).
