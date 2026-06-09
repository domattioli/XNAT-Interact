"""
Tests for file-type classification in src.xnat_scan_data.

Includes a documented footgun: `is_dicom` treats any *extensionless* file as a
DICOM. The improvement plan fixes this with real magic-byte sniffing
(DICM at byte offset 128). The test pins the current behavior so the fix is
visibly a change.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from src.xnat_scan_data import ScanFile


@pytest.fixture
def scan_stub():
    stub = ScanFile.__new__(ScanFile)
    stub._image = 0  # keep ScanFile.__del__ quiet during GC of this bare instance
    return stub


def test_dcm_extension_is_recognized(scan_stub, tmp_path):
    """A .dcm file with proper DICOM content is recognized as DICOM."""
    from tests.synthetic_data import make_synthetic_dicom

    dcm_file = tmp_path / "image.dcm"
    make_synthetic_dicom(dcm_file)
    assert scan_stub.is_dicom(dcm_file) is True


def test_jpg_extension_is_not_dicom(scan_stub, tmp_path):
    """A .jpg file without DICM marker is not recognized as DICOM."""
    from tests.synthetic_data import make_synthetic_jpg

    jpg_file = tmp_path / "photo.jpg"
    make_synthetic_jpg(jpg_file)
    assert scan_stub.is_dicom(jpg_file) is False


def test_extensionless_non_dicom_file_is_not_dicom(scan_stub, tmp_path):
    """
    FIXED behavior: a file with no extension and no DICM marker is NOT classified
    as DICOM. An extensionless 'notes' file (or any non-DICOM bytes) must fail the
    content-based check.
    """
    # Create a non-DICOM extensionless file (e.g., plain text)
    notes_file = tmp_path / "notes"
    notes_file.write_text("This is just notes, not DICOM data")
    assert scan_stub.is_dicom(notes_file) is False


def test_extensionless_dicom_file_is_dicom(scan_stub, tmp_path):
    """
    FIXED behavior: a file with no extension but WITH the DICM magic bytes at
    offset 128 IS classified as DICOM. Content-based detection catches DICOMs
    regardless of extension.
    """
    from tests.synthetic_data import make_synthetic_dicom

    # Create a real DICOM file (no extension)
    dicom_file = tmp_path / "no_extension_dicom"
    make_synthetic_dicom(dicom_file)
    assert scan_stub.is_dicom(dicom_file) is True
