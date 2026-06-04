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


def test_dcm_extension_is_recognized(scan_stub):
    assert scan_stub.is_dicom(Path("image.dcm")) is True


def test_jpg_extension_is_not_dicom(scan_stub):
    assert scan_stub.is_dicom(Path("photo.jpg")) is False


@pytest.mark.known_issue
def test_extensionless_file_is_treated_as_dicom_footgun(scan_stub):
    """
    CURRENT (risky) behavior: a file with no extension is classified as DICOM,
    purely from its name, without inspecting its bytes. A stray 'notes' or
    '.DS_Store'-style file could be mistaken for imaging data. The plan replaces
    this with content-based detection.
    """
    assert scan_stub.is_dicom(Path("some_random_file")) is True
