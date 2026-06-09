"""
Synthetic data generators for the XNAT-Interact test suite.

Goal: let the whole test suite run with **no live XNAT server, no VPN, and no
real patient data (PHI)**. Everything produced here is fake, deterministic, and
safe to commit / regenerate.

The generators intentionally mirror the *shape* of the data the real program
ingests:

  * DICOM files (trauma / radio-fluoroscopic source images)
  * JPG diagnostic images (arthroscopy)
  * MP4 videos (arthroscopy)
  * A filled-out intake-form text file
  * A batch-upload spreadsheet (.xlsx)

They are written so a non-expert can read them and understand what a "real"
input looks like.
"""
from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional, Sequence

import numpy as np


# --------------------------------------------------------------------------- #
# DICOM
# --------------------------------------------------------------------------- #
def make_phi_dicom_dataset(
    *,
    rows: int = 16,
    cols: int = 16,
    seed: int = 0,
):
    """
    Build an **in-memory** pydicom Dataset that is deliberately stuffed with
    fake PHI (patient name, accession number, private tags, an overlay, etc.).

    Returned dataset is what the de-identification routine is supposed to scrub.
    Kept in-memory (not written to disk) so de-id logic can be tested directly.
    """
    import pydicom
    from pydicom.dataset import FileDataset, FileMetaDataset
    from pydicom.uid import (
        ExplicitVRLittleEndian,
        SecondaryCaptureImageStorage,
        generate_uid,
    )

    file_meta = FileMetaDataset()
    file_meta.MediaStorageSOPClassUID = SecondaryCaptureImageStorage
    file_meta.MediaStorageSOPInstanceUID = generate_uid()
    file_meta.TransferSyntaxUID = ExplicitVRLittleEndian
    file_meta.ImplementationClassUID = generate_uid()

    ds = FileDataset(None, {}, file_meta=file_meta, preamble=b"\0" * 128)

    # --- identity / study metadata (the bits that matter for de-id) ---
    ds.SOPClassUID = SecondaryCaptureImageStorage
    ds.SOPInstanceUID = file_meta.MediaStorageSOPInstanceUID
    ds.Modality = "XC"
    ds.ContentDate = "20240101"
    ds.ContentTime = "120000"

    # --- FAKE PHI we expect de-identification to remove/redact ---
    ds.PatientName = "DOE^JOHN"                 # VR = PN  -> must be redacted
    ds.PatientID = "MRN-0001234"
    ds.ReferringPhysicianName = "SMITH^JANE"    # VR = PN  -> must be redacted
    ds.AccessionNumber = "ACC-987654"           # -> "REDACTED 4 XNAT"
    ds.StudyID = "STUDY-42"                      # -> "REDACTED 4 XNAT"
    ds.InstitutionName = "UIOWA HOSPITAL"

    # A private tag block (must be removed by remove_private_tags()).
    block = ds.private_block(0x000B, "XNAT-INTERACT TEST", create=True)
    block.add_new(0x01, "LO", "super-secret-phi")

    # An overlay group 0x6000,0x3000 (must be deleted by the de-id loop).
    ds.add_new(0x60003000, "OW", b"\x00\x01\x02\x03")

    # A "curve" group 0x5000 (must be deleted by the curves callback).
    ds.add_new(0x50000005, "US", 1)

    # --- pixel data ---
    rng = np.random.default_rng(seed)
    arr = rng.integers(0, 4096, size=(rows, cols), dtype=np.uint16)
    ds.Rows, ds.Columns = rows, cols
    ds.SamplesPerPixel = 1
    ds.PhotometricInterpretation = "MONOCHROME2"
    ds.BitsAllocated = 16
    ds.BitsStored = 12
    ds.HighBit = 11
    ds.PixelRepresentation = 0
    ds.PixelData = arr.tobytes()

    return ds


def make_synthetic_dicom(
    path: Path,
    *,
    rows: int = 16,
    cols: int = 16,
    seed: int = 0,
) -> Path:
    """Write a synthetic DICOM file (with fake PHI) to ``path`` and return it."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    ds = make_phi_dicom_dataset(rows=rows, cols=cols, seed=seed)
    # We set an explicit preamble + file-meta above, so a plain save_as writes a
    # readable Part-10 file on both pydicom 2.x and 3.x (the 2.x `write_like_original`
    # / 3.x `enforce_file_format` keywords are intentionally avoided for portability).
    ds.save_as(str(path))
    return path


# --------------------------------------------------------------------------- #
# Burned-in PHI pixel arrays (pixel-review tests)
# --------------------------------------------------------------------------- #
def make_burned_in_phi_pixel_array(
    text: str = "PATIENT NAME 01/02/1980",
    *,
    rows: int = 64,
    cols: int = 256,
    dtype=None,
) -> "np.ndarray":
    """
    Return a numpy array (uint8 by default) with ``text`` rendered into the
    pixel data using cv2.putText — simulating the kind of burned-in PHI that
    fluoroscopy / OR acquisition systems write directly onto the image.

    The rendered text region will contain non-zero pixel values; the surrounding
    area is black (zero).  Tests that use this array can:
      1. Confirm that non-zero pixels exist inside the text bounding box before
         any redaction (proving the "PHI" is present).
      2. Call ``apply_redaction`` with the bounding box and assert those pixels
         are zeroed afterward.

    Parameters
    ----------
    text:
        The string to render (fake PHI for testing — never real patient data).
    rows, cols:
        Pixel dimensions of the output array.
    dtype:
        numpy dtype for the output array.  Defaults to ``np.uint8``.  Pass
        ``np.uint16`` to simulate 16-bit DICOM pixel data.

    Returns
    -------
    np.ndarray of shape (rows, cols) with dtype ``dtype``.
    """
    import cv2

    if dtype is None:
        dtype = np.uint8

    arr = np.zeros((rows, cols), dtype=np.uint8)  # cv2.putText needs uint8
    font = cv2.FONT_HERSHEY_SIMPLEX
    font_scale = 0.5
    thickness = 1
    color = 200  # near-white on black background
    origin = (4, rows // 2)  # (x, y) — left-aligned, vertically centred

    cv2.putText(arr, text, origin, font, font_scale, color, thickness, cv2.LINE_AA)

    return arr.astype(dtype)


def make_faint_burned_in_phi_pixel_array(
    text: str = "PATIENT NAME 01/02/1980",
    *,
    rows: int = 64,
    cols: int = 256,
    dtype=None,
    bg: int = 60,
    fg: int = 90,
) -> "np.ndarray":
    """
    Return a numpy array (uint8 by default) with ``text`` rendered as LOW-CONTRAST
    burned-in PHI.

    Unlike ``make_burned_in_phi_pixel_array`` which renders near-white text on a
    black background, this function fills the background with a constant ``bg``
    value and renders the text in a near-background ``fg`` color, creating a
    faint overlay that simulates low-contrast burned-in labels that single-pass
    OCR or simple thresholding may miss.

    Parameters
    ----------
    text:
        The string to render (fake PHI for testing — never real patient data).
    rows, cols:
        Pixel dimensions of the output array.
    dtype:
        numpy dtype for the output array.  Defaults to ``np.uint8``.  Pass
        ``np.uint16`` to simulate 16-bit DICOM pixel data.
    bg:
        Background constant fill value.
    fg:
        Foreground (text) color value — near-background, faint.

    Returns
    -------
    np.ndarray of shape (rows, cols) with dtype ``dtype``.
    """
    import cv2

    if dtype is None:
        dtype = np.uint8

    arr = np.full((rows, cols), bg, dtype=np.uint8)  # Fill with bg, not zero
    font = cv2.FONT_HERSHEY_SIMPLEX
    font_scale = 0.5
    thickness = 1
    color = fg  # Faint text color near background
    origin = (4, rows // 2)

    cv2.putText(arr, text, origin, font, font_scale, color, thickness, cv2.LINE_AA)

    return arr.astype(dtype)


def make_multiframe_phi_case(
    text: str = "MRN 00471123",
    *,
    n_frames: int = 8,
    rows: int = 128,
    cols: int = 256,
    dtype=None,
    seed: int = 0,
    faint: bool = False,
) -> "np.ndarray":
    """
    Return a numpy array of shape (n_frames, rows, cols) with a STATIC burned-in
    text overlay (same pixel position every frame) composited over MOVING synthetic
    anatomy.

    The text banner is identical across all frames (low variance). The background
    anatomy is random per frame (high variance), simulating a realistic fluoroscopy
    or OR video acquisition. This fixture is designed for tests of cross-frame
    consensus / variance-based de-identification.

    Parameters
    ----------
    text:
        The burned-in PHI text to render (fake for testing).
    n_frames:
        Number of frames in the output sequence.
    rows, cols:
        Pixel dimensions of each frame.
    dtype:
        numpy dtype for the output array. Defaults to ``np.uint8``.
    seed:
        Random seed for anatomy generation (background noise).
    faint:
        If True, render the text as low-contrast (via ``make_faint_burned_in_phi_pixel_array``).
        If False, render as near-white (via ``make_burned_in_phi_pixel_array``).

    Returns
    -------
    np.ndarray of shape (n_frames, rows, cols) with dtype ``dtype``.
    """
    if dtype is None:
        dtype = np.uint8

    rng = np.random.default_rng(seed)
    frames = np.zeros((n_frames, rows, cols), dtype=np.uint8)

    # Render the static text overlay once (either faint or bright)
    if faint:
        text_overlay = make_faint_burned_in_phi_pixel_array(
            text, rows=rows, cols=cols, dtype=np.uint8
        )
    else:
        text_overlay = make_burned_in_phi_pixel_array(
            text, rows=rows, cols=cols, dtype=np.uint8
        )

    # Generate per-frame anatomy (random shifted noise) and composite text
    for i in range(n_frames):
        # Random anatomy: shifted noise blob simulating moving structure
        anatomy = rng.integers(20, 80, size=(rows, cols), dtype=np.uint8)
        # Optional: add a small noise shift per frame for more realism
        shift = rng.integers(-5, 6, size=2)
        anatomy = np.roll(anatomy, shift, axis=(0, 1))

        # Composite: text overlay (nonzero pixels) on top of anatomy
        frames[i] = np.where(text_overlay > 0, text_overlay, anatomy)

    return frames.astype(dtype)


def make_unprofiled_device_dataset(
    text: str = "DOE^JOHN MRN 00471123",
    *,
    rows: int = 128,
    cols: int = 256,
    seed: int = 0,
):
    """
    Build an **in-memory** pydicom Dataset with burned-in PHI pixels, Modality
    "XA" (fluoroscopy), and **NO private device-profile block** (0x0019).

    This represents an acquisition from a device with no registered profile,
    so tests can verify behavior on unrecognized equipment. The burned-in PHI
    is rendered into the pixel data via ``make_burned_in_phi_pixel_array``.

    Parameters
    ----------
    text:
        The fake PHI string to burn into pixels (never real patient data).
    rows, cols:
        Pixel dimensions of the image.
    seed:
        Random seed (not used in this version, but kept for consistency).

    Returns
    -------
    pydicom.dataset.FileDataset
        A DICOM dataset with burned-in PHI, no device profile.
    """
    import pydicom
    from pydicom.dataset import FileDataset, FileMetaDataset
    from pydicom.uid import (
        ExplicitVRLittleEndian,
        SecondaryCaptureImageStorage,
        generate_uid,
    )

    file_meta = FileMetaDataset()
    file_meta.MediaStorageSOPClassUID = SecondaryCaptureImageStorage
    file_meta.MediaStorageSOPInstanceUID = generate_uid()
    file_meta.TransferSyntaxUID = ExplicitVRLittleEndian
    file_meta.ImplementationClassUID = generate_uid()

    ds = FileDataset(None, {}, file_meta=file_meta, preamble=b"\0" * 128)

    # --- identity / study metadata ---
    ds.SOPClassUID = SecondaryCaptureImageStorage
    ds.SOPInstanceUID = file_meta.MediaStorageSOPInstanceUID
    ds.Modality = "XA"  # Fluoroscopy
    ds.ContentDate = "20240101"
    ds.ContentTime = "120000"

    # --- FAKE PHI ---
    ds.PatientName = "DOE^JOHN"
    ds.PatientID = "MRN-0001234"
    ds.ReferringPhysicianName = "SMITH^JANE"
    ds.AccessionNumber = "ACC-987654"
    ds.StudyID = "STUDY-42"
    ds.InstitutionName = "UIOWA HOSPITAL"

    # NOTE: NO private tag block (0x0019 device profile) — represents unprofiled device

    # --- pixel data with burned-in PHI ---
    phi_arr = make_burned_in_phi_pixel_array(text, rows=rows, cols=cols, dtype=np.uint8)
    ds.Rows, ds.Columns = rows, cols
    ds.SamplesPerPixel = 1
    ds.PhotometricInterpretation = "MONOCHROME2"
    ds.BitsAllocated = 8
    ds.BitsStored = 8
    ds.HighBit = 7
    ds.PixelRepresentation = 0
    ds.PixelData = phi_arr.tobytes()

    return ds


def make_profiled_device_dataset(
    text: str = "DOE^JOHN MRN 00471123",
    *,
    rows: int = 128,
    cols: int = 256,
    manufacturer: str = "Siemens",
    model_name: str = "AXIOM_Artis",
    seed: int = 0,
):
    """
    Build an **in-memory** pydicom Dataset with burned-in PHI pixels, Modality
    "XA" (fluoroscopy), and Manufacturer/ManufacturerModelName tags set for
    device-profile matching.

    This represents an acquisition from a device with a registered profile,
    so tests can verify profile-based masking. The burned-in PHI is rendered
    into the pixel data via ``make_burned_in_phi_pixel_array``.

    Parameters
    ----------
    text:
        The fake PHI string to burn into pixels (never real patient data).
    rows, cols:
        Pixel dimensions of the image.
    manufacturer:
        Manufacturer tag value (e.g., "Siemens", "GE", "Philips").
    model_name:
        ManufacturerModelName tag value (e.g., "AXIOM_Artis").
    seed:
        Random seed (not used in this version, but kept for consistency).

    Returns
    -------
    pydicom.dataset.FileDataset
        A DICOM dataset with burned-in PHI and device identification.
    """
    import pydicom
    from pydicom.dataset import FileDataset, FileMetaDataset
    from pydicom.uid import (
        ExplicitVRLittleEndian,
        SecondaryCaptureImageStorage,
        generate_uid,
    )

    file_meta = FileMetaDataset()
    file_meta.MediaStorageSOPClassUID = SecondaryCaptureImageStorage
    file_meta.MediaStorageSOPInstanceUID = generate_uid()
    file_meta.TransferSyntaxUID = ExplicitVRLittleEndian
    file_meta.ImplementationClassUID = generate_uid()

    ds = FileDataset(None, {}, file_meta=file_meta, preamble=b"\0" * 128)

    # --- identity / study metadata ---
    ds.SOPClassUID = SecondaryCaptureImageStorage
    ds.SOPInstanceUID = file_meta.MediaStorageSOPInstanceUID
    ds.Modality = "XA"  # Fluoroscopy
    ds.ContentDate = "20240101"
    ds.ContentTime = "120000"

    # --- Device identification (for profile matching) ---
    ds.Manufacturer = manufacturer
    ds.ManufacturerModelName = model_name

    # --- FAKE PHI ---
    ds.PatientName = "DOE^JOHN"
    ds.PatientID = "MRN-0001234"
    ds.ReferringPhysicianName = "SMITH^JANE"
    ds.AccessionNumber = "ACC-987654"
    ds.StudyID = "STUDY-42"
    ds.InstitutionName = "UIOWA HOSPITAL"

    # --- pixel data with burned-in PHI ---
    phi_arr = make_burned_in_phi_pixel_array(text, rows=rows, cols=cols, dtype=np.uint8)
    ds.Rows, ds.Columns = rows, cols
    ds.SamplesPerPixel = 1
    ds.PhotometricInterpretation = "MONOCHROME2"
    ds.BitsAllocated = 8
    ds.BitsStored = 8
    ds.HighBit = 7
    ds.PixelRepresentation = 0
    ds.PixelData = phi_arr.tobytes()

    return ds


# --------------------------------------------------------------------------- #
# JPG / MP4 (arthroscopy)
# --------------------------------------------------------------------------- #
def make_synthetic_jpg(path: Path, *, size: int = 64, seed: int = 0) -> Path:
    """Write a small random JPG and return its path."""
    import cv2

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(seed)
    img = rng.integers(0, 256, size=(size, size, 3), dtype=np.uint8)
    cv2.imwrite(str(path), img)
    return path


def make_synthetic_mp4(path: Path, *, size: int = 64, n_frames: int = 5, seed: int = 0) -> Path:
    """Write a tiny random MP4 (mp4v) and return its path."""
    import cv2

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(seed)
    fourcc = cv2.VideoWriter_fourcc(*"mp4v")
    writer = cv2.VideoWriter(str(path), fourcc, 5.0, (size, size))
    try:
        for _ in range(n_frames):
            frame = rng.integers(0, 256, size=(size, size, 3), dtype=np.uint8)
            writer.write(frame)
    finally:
        writer.release()
    return path


# --------------------------------------------------------------------------- #
# Intake form text file
# --------------------------------------------------------------------------- #
def make_synthetic_intake_form_textfile(
    parent_dir: Path,
    *,
    fields: Optional[Dict[str, str]] = None,
) -> Path:
    """
    Write a minimal ``RECONSTRUCTED_OR_DATA_INTAKE_FORM.txt`` inside ``parent_dir``
    using KEY: VALUE lines (mirrors how the running-text-file is persisted).
    Returns the path to the written file.
    """
    parent_dir = Path(parent_dir)
    parent_dir.mkdir(parents=True, exist_ok=True)
    default = {
        "FILER_HAWKID": "TESTUSER",
        "FORM_AVAILABLE_FOR_PERFORMANCE": "True",
        "OPERATION_DATE": "2024-01-01",
        "INSTITUTION_NAME": "UNIVERSITY_OF_IOWA",
        "PROCEDURE_TYPE": "ARTHROSCOPY",
        "PROCEDURE_NAME": "1A_KNEE_ARTHROSCOPY",
        "SCAN_QUALITY": "usable",
    }
    if fields:
        default.update(fields)
    out = parent_dir / "RECONSTRUCTED_OR_DATA_INTAKE_FORM.txt"
    out.write_text("\n".join(f"{k}: {v}" for k, v in default.items()), encoding="utf-8")
    return out


# --------------------------------------------------------------------------- #
# Batch-upload spreadsheet
# --------------------------------------------------------------------------- #
# A representative subset of the real template's columns. The real column set
# lives in utilities.py -> required_batch_upload_columns; tests that need the
# exact set should read it from there rather than hard-coding it here.
DEFAULT_BATCH_COLUMNS: Sequence[str] = (
    "Filer HawkID",
    "Operation Date",
    "Institution Name",
    "Procedure Type",
    "Procedure Name",
    "Performer HawkID-Task",
    "Quality",
)


def make_synthetic_batch_xlsx(
    path: Path,
    *,
    rows: Optional[List[Dict[str, str]]] = None,
    columns: Sequence[str] = DEFAULT_BATCH_COLUMNS,
) -> Path:
    """
    Write a synthetic batch-upload spreadsheet to ``path``.

    ``rows`` is a list of {column: value} dicts. When omitted, a single valid
    arthroscopy row is written.
    """
    import pandas as pd

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if rows is None:
        rows = [
            {
                "Filer HawkID": "TESTUSER",
                "Operation Date": "2024-01-01",
                "Institution Name": "UNIVERSITY_OF_IOWA",
                "Procedure Type": "ARTHROSCOPY",
                "Procedure Name": "1A_KNEE_ARTHROSCOPY",
                "Performer HawkID-Task": "{testuser: lead}",
                "Quality": "usable",
            }
        ]
    df = pd.DataFrame(rows, columns=list(columns))
    df.to_excel(path, index=False)
    return path


def make_mixed_validity_batch_xlsx(
    path: Path,
    *,
    n_good: int = 3,
    n_bad: int = 2,
) -> Path:
    """
    Write a batch-upload xlsx whose rows alternate good/bad entries.

    The file has the minimal column set understood by the continue-on-error
    tests.  Rows labelled "bad" have an intentionally blank ``Procedure Name``
    so the pre-upload validation marks them as errors.

    Parameters
    ----------
    path
        Destination .xlsx file path.
    n_good
        Number of rows that should pass validation (no errors).
    n_bad
        Number of rows that should fail validation (blank Procedure Name).

    Returns
    -------
    Path
        The written file path.
    """
    import pandas as pd

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    good_row = {
        "Filer HawkID": "TESTUSER",
        "Operation Date": "2024-01-01",
        "Institution Name": "UNIVERSITY_OF_IOWA",
        "Procedure Type": "ARTHROSCOPY",
        "Procedure Name": "1A_KNEE_ARTHROSCOPY",
        "Performer HawkID-Task": "{testuser: lead}",
        "Quality": "usable",
    }
    bad_row = {
        "Filer HawkID": "TESTUSER",
        "Operation Date": "2024-01-01",
        "Institution Name": "UNIVERSITY_OF_IOWA",
        "Procedure Type": "ARTHROSCOPY",
        "Procedure Name": "",           # intentionally blank — triggers validation error
        "Performer HawkID-Task": "{testuser: lead}",
        "Quality": "usable",
    }

    # Interleave good and bad rows deterministically
    rows: List[Dict[str, str]] = []
    g, b = 0, 0
    for i in range(n_good + n_bad):
        if g < n_good and (b >= n_bad or i % 2 == 0):
            rows.append(dict(good_row))
            g += 1
        else:
            rows.append(dict(bad_row))
            b += 1

    df = pd.DataFrame(rows, columns=list(good_row.keys()))
    df.to_excel(path, index=False)
    return path
