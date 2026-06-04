"""
De-identification helpers for DICOM data.

SAFETY NOTE — metadata scrubbing alone is insufficient.
Fluoroscopy / OR source images routinely have patient name, date-of-birth,
and other identifiers *burned into the pixel data* at acquisition time.
`deidentify_dataset` only removes/redacts DICOM tag metadata; it does NOT
alter pixels. Any image that has passed through de-id must still be reviewed
by a human before upload to confirm no burned-in PHI remains visible.

`needs_pixel_review` / `apply_redaction` are the interim human-in-the-loop
helpers for that review step:
  - `needs_pixel_review` conservatively returns True for every image (no OCR
    capability yet; every scan needs a human look).
  - `apply_redaction` zeros out caller-specified pixel boxes so a reviewer
    can mask out identified PHI regions before the image is transmitted.

The upload-flow confirmation gate (T014) that blocks upload until review is
confirmed is wired in a later task and is NOT part of this module.
"""
from __future__ import annotations

from typing import List, Tuple

import numpy as np


def deidentify_dataset(ds, redacted_string: str):
    """
    Scrub PHI from a pydicom Dataset in-place and return it.

    Removes/redacts exactly the same tags as
    SourceDicomDeIdentified._deidentify_dicom (byte-identical behaviour):
      - All PN (person-name) VR elements  → ``redacted_string``
      - All curve groups (0x5000 family)  → deleted
      - Private tags                      → removed via remove_private_tags()
      - Overlay data (0x6000–0x60FF, 0x3000) → deleted
      - AccessionNumber                   → "REDACTED 4 XNAT"
      - StudyID                           → "REDACTED 4 XNAT"

    Pixels are NOT touched.  See module docstring for why that matters.

    Parameters
    ----------
    ds:
        A pydicom Dataset (FileDataset or plain Dataset).
    redacted_string:
        The placeholder string to write into PN elements (e.g.
        "REDACTED PYTHON-TO-XNAT UPLOAD SCRIPT").

    Returns
    -------
    ds
        The same dataset object, modified in-place, returned for convenience.
    """
    def _person_names_cb(dcm_data, data_element):
        if data_element.VR == "PN":
            data_element.value = redacted_string

    def _curves_cb(dcm_data, data_element):
        if data_element.tag.group & 0xFF00 == 0x5000:
            del dcm_data[data_element.tag]

    ds.walk(_person_names_cb)
    ds.walk(_curves_cb)
    ds.remove_private_tags()

    for i in range(0x6000, 0x60FF, 2):
        tag = (i, 0x3000)
        if tag in ds:
            del ds[tag]

    if hasattr(ds, "AccessionNumber"):
        ds.AccessionNumber = "REDACTED 4 XNAT"
    if hasattr(ds, "StudyID"):
        ds.StudyID = "REDACTED 4 XNAT"

    return ds


# --------------------------------------------------------------------------- #
# Pixel-review helpers
# --------------------------------------------------------------------------- #

def needs_pixel_review(pixel_array: np.ndarray) -> bool:
    """
    Return True if the pixel array requires human review for burned-in PHI.

    Currently always returns True (conservative placeholder). Future versions
    may use OCR to skip review for images that provably contain no text, but
    until that capability exists every image must be reviewed.

    Parameters
    ----------
    pixel_array:
        A numpy ndarray representing the image pixels (any dtype/shape).

    Returns
    -------
    bool
        Always True in this implementation.
    """
    return True


def apply_redaction(
    pixel_array: np.ndarray,
    boxes: List[Tuple[int, int, int, int]],
) -> np.ndarray:
    """
    Zero out rectangular regions in ``pixel_array`` and return the result.

    Pure function: operates on a *copy* of the input array — the caller's
    original is not modified.

    Parameters
    ----------
    pixel_array:
        numpy ndarray of any integer or float dtype; shape (H, W) or (H, W, C).
    boxes:
        List of (x, y, w, h) tuples where (x, y) is the top-left corner,
        ``w`` is width in pixels, and ``h`` is height in pixels.
        Out-of-bounds coordinates are silently clipped to the array boundary.

    Returns
    -------
    np.ndarray
        Copy of ``pixel_array`` with each specified box region set to 0.
    """
    result = pixel_array.copy()
    h_total, w_total = result.shape[0], result.shape[1]

    for (x, y, w, h) in boxes:
        x0 = max(0, x)
        y0 = max(0, y)
        x1 = min(w_total, x + w)
        y1 = min(h_total, y + h)
        result[y0:y1, x0:x1] = 0

    return result
