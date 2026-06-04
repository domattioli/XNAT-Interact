"""
Tests for DICOM de-identification (src.xnat_scan_data.SourceDicomDeIdentified).

This is the safety-critical path: before any trauma/fluoroscopic image leaves a
student's machine, identifying metadata must be scrubbed. We exercise the REAL
`_deidentify_dicom` method against a synthetic dataset full of fake PHI.

Because `SourceDicomDeIdentified.__init__` needs a live ConfigTables + intake
form, we construct a bare instance via __new__ and drive just the de-id method.
The improvement plan calls for extracting this into a standalone
`deidentify_dataset(ds, redacted_string)` function so this becomes trivial to
test directly — these tests are written to survive that refactor.
"""
from __future__ import annotations

import pytest

from src.xnat_scan_data import SourceDicomDeIdentified
from tests.synthetic_data import make_phi_dicom_dataset

REDACTED = "REDACTED PYTHON-TO-XNAT UPLOAD SCRIPT"


@pytest.fixture
def deidentified_dataset(monkeypatch):
    """A synthetic PHI dataset run through the real de-identification routine."""
    ds = make_phi_dicom_dataset()

    obj = SourceDicomDeIdentified.__new__(SourceDicomDeIdentified)
    obj._metadata = ds
    obj._image = 0  # keep ScanFile.__del__ quiet during GC of this bare instance
    # `redacted_string` is normally a property sourced from LocalVariables;
    # shadow it at class level so the bare instance can resolve it.
    monkeypatch.setattr(
        SourceDicomDeIdentified, "redacted_string", REDACTED, raising=False
    )

    obj._deidentify_dicom()
    return ds


def test_patient_name_is_redacted(deidentified_dataset):
    assert str(deidentified_dataset.PatientName) == REDACTED


def test_referring_physician_name_is_redacted(deidentified_dataset):
    assert str(deidentified_dataset.ReferringPhysicianName) == REDACTED


def test_accession_number_is_redacted(deidentified_dataset):
    assert deidentified_dataset.AccessionNumber == "REDACTED 4 XNAT"


def test_study_id_is_redacted(deidentified_dataset):
    assert deidentified_dataset.StudyID == "REDACTED 4 XNAT"


def test_private_tags_are_removed(deidentified_dataset):
    # The private block we added (group 0x000B) must be gone.
    private_tags = [el for el in deidentified_dataset if el.tag.is_private]
    assert private_tags == []


def test_overlay_data_is_removed(deidentified_dataset):
    # Overlay data lives at (0x6000-0x60FF, 0x3000); we added (0x6000,0x3000).
    assert (0x6000, 0x3000) not in deidentified_dataset


def test_curve_group_is_removed(deidentified_dataset):
    # Curve groups (0x5000 family) must be deleted.
    assert (0x5000, 0x0005) not in deidentified_dataset


def test_pixel_data_is_preserved(deidentified_dataset):
    # De-identification must NOT destroy the actual image.
    assert "PixelData" in deidentified_dataset
    assert len(deidentified_dataset.PixelData) > 0
