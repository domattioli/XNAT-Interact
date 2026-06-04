# XNAT-Interact test suite

These tests run **completely offline**: no XNAT server, no UIowa VPN, and **no
real patient data**. Every input is synthetic and generated on the fly by
`tests/synthetic_data.py`.

## Why this exists

The app talks to a live medical-imaging server behind a VPN and handles PHI, so
historically it was hard to test anything without the real system. That made it
risky to change code. This suite carves out the pieces that *can* be tested in
isolation and pins their behavior, so future improvements (see
`docs/IMPROVEMENT_PLAN.md`) can be made with confidence.

## Run it

```bash
pip install -r requirements.txt -r requirements-dev.txt
pytest
```

That's it — it should pass on any machine with the dependencies installed.

## What's covered today

| Test file | What it protects |
|---|---|
| `test_dicom_deidentification.py` | **PHI safety** — patient name, physician, accession #, study ID, private tags, overlays, and curves are scrubbed before upload; pixel data survives. |
| `test_image_hash.py` | The duplicate-image detector (deterministic, resizes, grayscales, rejects bad input). |
| `test_us_central_datetime.py` | Date/time normalization used throughout the upload path. |
| `test_prompt_validation.py` | The reused menu-prompt logic (driven by fake keyboard input). |
| `test_batch_upload_helpers.py` | The string validators that decide if a spreadsheet cell is well-formed. |
| `test_scan_file_classification.py` | File-type detection (incl. a documented footgun). |
| `test_synthetic_data.py` | Sanity-checks the generators themselves. |

## Markers

- `known_issue` — pins *current* (sometimes buggy) behavior the improvement
  plan will change. When you fix the behavior, update the test; the marker makes
  these easy to find: `pytest -m known_issue`.
- `requires_server` — reserved for tests that genuinely need the live server
  (none yet); skipped in normal runs.

## How synthetic data works

`tests/synthetic_data.py` can fabricate:

- DICOM files (with fake PHI, for de-identification tests)
- JPG images and MP4 videos (arthroscopy inputs)
- a filled-in intake-form text file
- a batch-upload `.xlsx` spreadsheet

Use these instead of ever putting real patient files in the repo.
