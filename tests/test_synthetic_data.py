"""
Sanity tests for the synthetic-data generators themselves.

If these fail, the rest of the suite can't be trusted — so we verify each
generator produces a file the real code can actually read.
"""
from __future__ import annotations

from pathlib import Path

from tests import synthetic_data as sd


def test_synthetic_dicom_is_readable(tmp_path: Path):
    import pydicom

    p = sd.make_synthetic_dicom(tmp_path / "case" / "img.dcm")
    ds = pydicom.dcmread(str(p))
    assert ds.PatientName == "DOE^JOHN"          # PHI present pre-de-id
    assert ds.pixel_array.shape == (16, 16)


def test_synthetic_jpg_is_readable(tmp_path: Path):
    import cv2

    p = sd.make_synthetic_jpg(tmp_path / "a.jpg")
    img = cv2.imread(str(p))
    assert img is not None
    assert img.shape[2] == 3


def test_synthetic_mp4_opens(tmp_path: Path):
    import cv2
    import pytest

    p = sd.make_synthetic_mp4(tmp_path / "v.mp4")
    # The mp4v codec isn't guaranteed on every (headless) CI image; if the file
    # couldn't be encoded, skip rather than fail — this generator is a
    # convenience, not safety-critical.
    if not p.exists() or p.stat().st_size == 0:
        pytest.skip("mp4v codec unavailable in this environment")
    cap = cv2.VideoCapture(str(p))
    try:
        if not cap.isOpened():
            pytest.skip("mp4v codec unavailable in this environment")
    finally:
        cap.release()


def test_synthetic_intake_form_written(tmp_path: Path):
    p = sd.make_synthetic_intake_form_textfile(tmp_path / "case")
    text = p.read_text(encoding="utf-8")
    assert "OPERATION_DATE: 2024-01-01" in text
    assert p.name == "RECONSTRUCTED_OR_DATA_INTAKE_FORM.txt"


def test_synthetic_batch_xlsx_readable(tmp_path: Path):
    import pandas as pd

    p = sd.make_synthetic_batch_xlsx(tmp_path / "batch.xlsx")
    df = pd.read_excel(p)
    assert len(df) == 1
    assert "Filer HawkID" in df.columns
