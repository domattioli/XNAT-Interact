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


# --------------------------------------------------------------------------- #
# Real-XNAT integration test cases (live-server fixtures)
# --------------------------------------------------------------------------- #
def corrupt_dicom_file(path: Path, mode: str = "truncate_pixels") -> Path:
    """
    Corrupt an already-written .dcm file on disk.

    Parameters
    ----------
    path
        Path to an existing .dcm file (must be written via ds.save_as() first).
    mode
        One of:
        - "truncate_pixels": truncate raw pixel bytes to half their original length,
          producing an incomplete/unreadable pixel data block.
        - "corrupt_header": corrupt the DICOM header bytes (flip bits in the file
          preamble/meta section), rendering the file unreadable.

    Returns
    -------
    Path
        The path to the corrupted file (modified in-place).

    Raises
    ------
    ValueError
        If mode is not one of the recognized strings.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"DICOM file does not exist: {path}")

    if mode == "truncate_pixels":
        # Truncate the file to approximately half its original size.
        # This leaves the header mostly intact but destroys pixel data.
        with open(path, "r+b") as f:
            f.seek(0, 2)  # seek to end
            original_size = f.tell()
            new_size = original_size // 2
            f.truncate(new_size)
    elif mode == "corrupt_header":
        # Flip bits in the first 256 bytes (DICOM preamble + file meta).
        # This makes the file unreadable but keeps some disk structure.
        with open(path, "r+b") as f:
            f.seek(0)
            header = bytearray(f.read(256))
            # Flip every other byte's MSB
            for i in range(len(header)):
                if i % 2 == 0:
                    header[i] ^= 0x80
            f.seek(0)
            f.write(header)
    else:
        raise ValueError(f"Unknown corruption mode: {mode}")

    return path


# --- Live-XNAT case helpers (spec 014, FR-003) ------------------------------ #
# Every case below has a fixed, seeded composition so that two builds produce
# byte-identical files (SC-008). UIDs are derived from fixed strings, never
# random. All "PHI" is fabricated.

_CASE_UID_ROOT = "1.2.826.0.1.3680043.10.1014."  # private test root (synthetic)


def _det_uid(*parts) -> str:
    """Deterministic DICOM UID derived from ``parts`` (synthetic only)."""
    from pydicom.uid import generate_uid

    return generate_uid(prefix=_CASE_UID_ROOT, entropy_srcs=[str(p) for p in parts])


def _phi_box(mask: "np.ndarray", pad: int = 2) -> tuple:
    """Bounding box (x0, y0, x1, y1), exclusive end, of the non-zero ``mask``."""
    ys, xs = np.nonzero(mask)
    if len(xs) == 0:
        return (0, 0, 0, 0)
    h, w = mask.shape[-2:]
    return (
        max(int(xs.min()) - pad, 0),
        max(int(ys.min()) - pad, 0),
        min(int(xs.max()) + 1 + pad, w),
        min(int(ys.max()) + 1 + pad, h),
    )


def _set_case_uids(ds, case: str, idx, study_uid: str, series_uid: str, sop_uid: str = None) -> None:
    """Overwrite every random UID that make_phi_dicom_dataset generated."""
    sop = sop_uid or _det_uid(case, "sop", idx)
    ds.file_meta.MediaStorageSOPInstanceUID = sop
    ds.file_meta.ImplementationClassUID = _det_uid("impl")
    ds.SOPInstanceUID = sop
    ds.StudyInstanceUID = study_uid
    ds.SeriesInstanceUID = series_uid


def _bright_phi_frame(text: str, rows: int, cols: int, seed: int):
    """16-bit frame: low anatomy noise (<= 1000) with bright text (4000)."""
    rng = np.random.default_rng(seed)
    base = rng.integers(0, 1000, size=(rows, cols), dtype=np.uint16)
    text_mask = make_burned_in_phi_pixel_array(text, rows=rows, cols=cols) > 0
    base[text_mask] = 4000
    return base, text_mask


# Pixels above this value inside a PHI box count as "rendered text" for the
# bright case; the anatomy noise never exceeds 1000.
BRIGHT_TEXT_THRESHOLD = 3000


def make_knee_2025_case(tmp_dir: Path) -> dict:
    """
    KNEE_2025 (FR-003): 42 RF fluoroscopy frames with bright burned-in PHI and
    18 CT pre-op archive files (one study, two series) in ``rf/``, plus 5 MP4
    arthroscopy clips in ``esv/``. 65 files. InstanceNumber is unique across
    the case (1-60).

    Returns a dict with ``source_files``, ``rf_files``, ``esv_files``,
    ``phi_boxes`` (file name to box), ``series`` (file name to
    SeriesInstanceUID), ``expected_absent_tags`` (empty) and
    ``expected_frames`` (empty).
    """
    tmp_dir = Path(tmp_dir)
    rf_dir, esv_dir = tmp_dir / "rf", tmp_dir / "esv"
    rf_dir.mkdir(parents=True, exist_ok=True)
    esv_dir.mkdir(parents=True, exist_ok=True)

    study_uid = _det_uid("KNEE_2025", "study")
    fluoro_series = _det_uid("KNEE_2025", "series", "fluoro")
    ct_series = _det_uid("KNEE_2025", "series", "ct")
    rf_files, esv_files, phi_boxes, series = [], [], {}, {}

    for i in range(42):
        p = rf_dir / f"knee_fluoro_{i:03d}.dcm"
        ds = make_phi_dicom_dataset(rows=256, cols=256, seed=1000 + i)
        arr, mask = _bright_phi_frame("DOE^JOHN MRN-0001234 01/02/1980", 256, 256, seed=1000 + i)
        ds.PixelData = arr.tobytes()
        ds.InstanceNumber = str(i + 1)
        ds.StudyDate, ds.StudyTime = "20250115", "100000"
        ds.PatientBirthDate = "19800102"
        ds.Modality = "RF"
        _set_case_uids(ds, "KNEE_2025", f"fluoro{i}", study_uid, fluoro_series)
        ds.save_as(str(p), enforce_file_format=True)
        rf_files.append(p)
        phi_boxes[p.name] = _phi_box(mask)
        series[p.name] = fluoro_series

    for i in range(18):
        p = rf_dir / f"knee_preop_ct_{i:03d}.dcm"
        ds = make_phi_dicom_dataset(rows=128, cols=128, seed=2000 + i)
        ds.InstanceNumber = str(43 + i)
        ds.StudyDate, ds.StudyTime = "20250101", "140000"
        ds.PatientBirthDate = "19800102"
        ds.Modality = "CT"
        _set_case_uids(ds, "KNEE_2025", f"ct{i}", study_uid, ct_series)
        ds.save_as(str(p), enforce_file_format=True)
        rf_files.append(p)
        series[p.name] = ct_series

    for i in range(5):
        p = esv_dir / f"knee_arthroscopy_{i:01d}.mp4"
        make_synthetic_mp4(p, size=64, n_frames=10, seed=3000 + i)
        esv_files.append(p)

    return {
        "case_name": "KNEE_2025",
        "subject_label": "knee_2025_subject",
        "experiment_label": "knee_2025_experiment",
        "capture_date": "2025-01-15",
        "rf_dir": rf_dir,
        "esv_dir": esv_dir,
        "rf_files": rf_files,
        "esv_files": esv_files,
        "source_files": rf_files + esv_files,
        "phi_boxes": phi_boxes,
        "phi_kind": "bright",
        "series": series,
        "expected_absent_tags": {},
        "expected_frames": {},
        "expected_dispositions": {},
        "injected_characteristics": {"burned_in_phi": "bright"},
        "expected_min_file_count": 65,
        "expected_max_file_count": 65,
    }


def make_hip_2024_case(tmp_dir: Path) -> dict:
    """
    HIP_2024 (FR-003): 18 single-frame XA frames with faint burned-in PHI, one
    8-frame XA instance and one 9-frame US instance, all in ``rf/``. Every
    third single-frame file lacks InstitutionName; the US instance lacks
    ReferringPhysicianName. 20 files.
    """
    tmp_dir = Path(tmp_dir)
    rf_dir = tmp_dir / "rf"
    rf_dir.mkdir(parents=True, exist_ok=True)
    study_uid = _det_uid("HIP_2024", "study")
    rf_files, phi_boxes, absent, frames_expected = [], {}, {}, {}

    for i in range(18):
        p = rf_dir / f"hip_fluoro_faint_{i:03d}.dcm"
        faint = make_faint_burned_in_phi_pixel_array(
            "HIP PATIENT 06/20/2024", rows=256, cols=256, dtype=np.uint16
        )
        # Per-frame anatomy outside the text keeps every frame's hash distinct.
        rng = np.random.default_rng(4000 + i)
        noise = rng.integers(0, 20, size=faint.shape, dtype=np.uint16)
        text_mask = faint != 60
        arr = np.where(text_mask, faint, faint + noise).astype(np.uint16)
        ds = make_phi_dicom_dataset(rows=256, cols=256, seed=4000 + i)
        ds.PixelData = arr.tobytes()
        ds.InstanceNumber = str(i + 1)
        ds.StudyDate, ds.StudyTime = "20240620", "150000"
        ds.PatientBirthDate = "19700101"
        ds.Modality = "XA"
        _set_case_uids(ds, "HIP_2024", f"xa{i}", study_uid, _det_uid("HIP_2024", "series", "xa"))
        if i % 3 == 0:
            del ds.InstitutionName
            absent[p.name] = ["InstitutionName"]
        ds.save_as(str(p), enforce_file_format=True)
        rf_files.append(p)
        phi_boxes[p.name] = _phi_box(text_mask)

    for j, (modality, n_frames) in enumerate((("XA", 8), ("US", 9))):
        p = rf_dir / f"hip_multiframe_{modality.lower()}.dcm"
        frames = make_multiframe_phi_case(
            "HIP PATIENT MRN 00471123", n_frames=n_frames, faint=True,
            dtype=np.uint16, seed=5000 + j,
        )
        ds = make_phi_dicom_dataset(rows=128, cols=256, seed=5000 + j)
        ds.PixelData = frames.tobytes()
        ds.NumberOfFrames = n_frames
        ds.InstanceNumber = str(19 + j)
        ds.StudyDate, ds.StudyTime = "20240620", "150000"
        ds.PatientBirthDate = "19700101"
        ds.Modality = modality
        _set_case_uids(ds, "HIP_2024", f"mf{modality}", study_uid,
                       _det_uid("HIP_2024", "series", f"mf{modality}"))
        if modality == "US":
            del ds.ReferringPhysicianName
            absent[p.name] = ["ReferringPhysicianName"]
        assert len(ds.PixelData) == n_frames * 128 * 256 * 2
        ds.save_as(str(p), enforce_file_format=True)
        rf_files.append(p)
        frames_expected[p.name] = n_frames

    return {
        "case_name": "HIP_2024",
        "subject_label": "hip_2024_subject",
        "experiment_label": "hip_2024_experiment",
        "capture_date": "2024-06-20",
        "rf_dir": rf_dir,
        "esv_dir": None,
        "rf_files": rf_files,
        "esv_files": [],
        "source_files": list(rf_files),
        "phi_boxes": phi_boxes,
        "phi_kind": "faint",
        "series": {},
        "expected_absent_tags": absent,
        "expected_frames": frames_expected,
        "expected_dispositions": {},
        "injected_characteristics": {
            "burned_in_phi": "faint",
            "missing_tags": sorted({t for v in absent.values() for t in v}),
        },
        "expected_min_file_count": 20,
        "expected_max_file_count": 20,
    }


def make_radiofluoro_2026_case(tmp_dir: Path) -> dict:
    """
    RADIOFLUORO_2026 (FR-003): exactly 7 RF files in ``rf/``:

    - ``rfl_dup_a.dcm`` / ``rfl_dup_b.dcm``: same SOPInstanceUID, byte-identical
      pixel content (a true duplicate);
    - ``rfl_coll_a.dcm`` / ``rfl_coll_b.dcm``: same SOPInstanceUID, different
      pixel content (a UID collision);
    - ``rfl_corrupt_header.dcm``: header bytes flipped;
    - ``rfl_truncated.dcm``: pixel data truncated;
    - ``rfl_valid.dcm``: valid.
    """
    tmp_dir = Path(tmp_dir)
    rf_dir = tmp_dir / "rf"
    rf_dir.mkdir(parents=True, exist_ok=True)
    study_uid = _det_uid("RADIOFLUORO_2026", "study")
    series_uid = _det_uid("RADIOFLUORO_2026", "series")
    dup_uid = _det_uid("RADIOFLUORO_2026", "dup")
    coll_uid = _det_uid("RADIOFLUORO_2026", "coll")

    plan = [
        ("rfl_dup_a.dcm", 6000, dup_uid),
        ("rfl_dup_b.dcm", 6000, dup_uid),
        ("rfl_coll_a.dcm", 6010, coll_uid),
        ("rfl_coll_b.dcm", 6011, coll_uid),
        ("rfl_corrupt_header.dcm", 6020, None),
        ("rfl_truncated.dcm", 6030, None),
        ("rfl_valid.dcm", 6040, None),
    ]
    files = []
    for n, (name, seed, sop) in enumerate(plan):
        p = rf_dir / name
        ds = make_phi_dicom_dataset(rows=256, cols=256, seed=seed)
        ds.InstanceNumber = str(n + 1)
        ds.StudyDate, ds.StudyTime = "20260310", "110000"
        ds.Modality = "RF"
        _set_case_uids(ds, "RADIOFLUORO_2026", name, study_uid, series_uid, sop_uid=sop)
        ds.save_as(str(p), enforce_file_format=True)
        files.append(p)

    corrupt_dicom_file(rf_dir / "rfl_corrupt_header.dcm", mode="corrupt_header")
    corrupt_dicom_file(rf_dir / "rfl_truncated.dcm", mode="truncate_pixels")

    return {
        "case_name": "RADIOFLUORO_2026",
        "subject_label": "radiofluoro_2026_subject",
        "experiment_label": "radiofluoro_2026_experiment",
        "capture_date": "2026-03-10",
        "rf_dir": rf_dir,
        "esv_dir": None,
        "rf_files": files,
        "esv_files": [],
        "source_files": list(files),
        "phi_boxes": {},
        "phi_kind": None,
        "series": {},
        "expected_absent_tags": {},
        "expected_frames": {},
        "duplicate_pair": ("rfl_dup_a.dcm", "rfl_dup_b.dcm"),
        "collision_pair": ("rfl_coll_a.dcm", "rfl_coll_b.dcm"),
        "corrupted_file": "rfl_corrupt_header.dcm",
        "truncated_file": "rfl_truncated.dcm",
        "expected_dispositions": {
            "rfl_corrupt_header.dcm": "rejected",
            "rfl_truncated.dcm": "failed_recoverable",
        },
        "injected_characteristics": {
            "uid_collision_pairs": [str(rf_dir / "rfl_coll_a.dcm"), str(rf_dir / "rfl_coll_b.dcm")],
            "duplicate_pairs": [str(rf_dir / "rfl_dup_a.dcm"), str(rf_dir / "rfl_dup_b.dcm")],
            "corrupted_files": [str(rf_dir / "rfl_corrupt_header.dcm")],
            "truncated_files": [str(rf_dir / "rfl_truncated.dcm")],
        },
        "expected_min_file_count": 7,
        "expected_max_file_count": 7,
    }
