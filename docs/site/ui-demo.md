---
layout: default
title: XNAT-Interact — Application UI
---

# The XNAT-Interact desktop application, screen by screen

These screenshots are captured from the actual Streamlit application running against a live XNAT 1.9.3 server with synthetic data — not mockups. Each screen below was rendered by `streamlit run streamlit_app.py` and photographed by an automated Playwright driver ([`tests/stress/screenshot_ui.py`](https://github.com/domattioli/XNAT-Interact/blob/xnat-fable/tests/stress/screenshot_ui.py)) during the 2026-06-12 validation campaign. The same run surfaced three release-blocking launch defects the offline test suite could not see; those are documented at the bottom.

The application is a thin, friendly layer over the de-identification and transfer pipeline a non-programmer researcher uses to move surgical fluoroscopy to and from the lab's XNAT project. Eight screens, one authenticated session.

## Log in

Authentication is the only gate; every data screen sits behind it. The login screen states the three access prerequisites inline (XNAT account, project membership, network reachability) rather than failing opaquely later. Credentials are never echoed or logged.

![Login screen](assets/ui/01_login.png)

## Browse

After connect, the session lands on a live table of the project's sessions, queried through the same `app/logic/browse` path the validation campaign exercised against the real server. The two rows shown are synthetic hip-fluoroscopy cases published earlier in the session; the search box filters by subject, experiment, date, or scan type.

![Browse screen](assets/ui/04_browse.png)

## Upload

Upload is a three-step wizard — intake form, preview, PHI confirmation — that mirrors the de-identification gate of the underlying `SourceRFSession.publish_to_xnat` pipeline. The surgeon, institution, and procedure dropdowns are populated from the server's configuration registry, so the operator selects rather than free-types; no patient name or MRN field exists on the form by construction.

![Upload screen](assets/ui/05_upload.png)

## Batch upload

The batch screen drives the same per-case pipeline from an Excel manifest, with continue-on-error semantics so one malformed row does not abort the run.

![Batch upload screen](assets/ui/06_batch.png)

## Download

Download retrieves whole surgeries through the `app/logic/download` path whose scan- and resource-enumeration defects were found and fixed live this session (a download that previously returned zero of three staged files now returns every frame, byte-identical to its source).

![Download screen](assets/ui/07_download.png)

## Annotations

The annotations screen loads and round-trips derived data — binary segmentation masks stored as compressed run-length-encoded blobs, addressed by image reference. The underlying assessor file path was the GAP-001 fix of this campaign (label-addressed assessor I/O returns HTTP 404 on stock XNAT 1.9.3; the gateway now resolves to the accession ID).

![Annotations screen](assets/ui/08_annotations.png)

## Learn and Onboarding

A command-line reference screen and an access-checklist screen complete the set; the onboarding page is reachable before login so a new user can read the access requirements without credentials.

![Learn screen](assets/ui/09_learn.png)

![Onboarding screen](assets/ui/10_onboarding.png)

## What capturing these screens found

Running the real application — rather than the offline AppTest harness, which injects a fake backend and a test `sys.path` — exposed three defects that made the documented launch command unusable, every one invisible to the green test suite:

| Defect | Symptom | Fix |
|---|---|---|
| `streamlit run app/main.py` could not import the `app` package | `ModuleNotFoundError: No module named 'app'` on every launch | a repo-root launcher (`streamlit_app.py`) that puts the root on `sys.path` and relocates the entrypoint so Streamlit's page auto-discovery stops hijacking `app/pages/` |
| The router rendered twice | `StreamlitDuplicateElementKey` on every screen | guard the module-level `main()` call so importing the app does not also run it |
| The browse table assigned five column names to six columns | `ValueError: Length mismatch: Expected axis has 6 elements, new values have 5` | a name-mapped display frame that is resilient to column-set changes |

Each fix shipped with a regression test. The supported launch is now `streamlit run streamlit_app.py` from the repository root.

[Back to the validation campaign overview.](validation.html)
