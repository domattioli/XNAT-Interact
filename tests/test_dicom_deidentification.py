"""
Tests for DICOM de-identification (src.xnat_scan_data.SourceDicomDeIdentified
and src.services.deidentify.deidentify_dataset).

This is the safety-critical path: before any trauma/fluoroscopic image leaves a
student's machine, identifying metadata must be scrubbed. We exercise both the
class method (`_deidentify_dicom`) and the extracted pure function
(`deidentify_dataset`) against a synthetic dataset full of fake PHI.

NOTE (T014): the upload-flow confirmation gate that blocks upload until a human
confirms pixel-PHI review is complete is wired in a later task and is NOT part
of this test file.
"""
from __future__ import annotations

import pytest

from src.services.deidentify import apply_redaction, deidentify_dataset
from src.xnat_scan_data import SourceDicomDeIdentified
from tests.synthetic_data import make_burned_in_phi_pixel_array, make_phi_dicom_dataset

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


def test_redaction_masks_burned_in_phi():
    """
    `apply_redaction` must zero out the pixel region containing burned-in PHI.

    Verifies the interim pixel-redaction path (T013): the reviewer identifies
    the bounding box of burned-in text and `apply_redaction` blacks it out.

    NOTE: the upload-flow confirmation gate (T014) — which blocks upload until a
    human confirms all PHI regions have been reviewed and redacted — is wired in
    a later task and is NOT asserted here.
    """
    arr = make_burned_in_phi_pixel_array(text="DOE^JOHN 01/01/1970")

    # Confirm the simulated burned-in PHI is actually present before redaction.
    assert arr.max() > 0, (
        "Synthetic burned-in array should have non-zero pixels (text not rendered?)"
    )

    # Redact the full image (worst-case: operator blanks everything).
    x, y, w, h = 0, 0, arr.shape[1], arr.shape[0]
    result = apply_redaction(arr, [(x, y, w, h)])

    # After redaction the PHI region must be zeroed.
    assert result[y : y + h, x : x + w].max() == 0


# --------------------------------------------------------------------------- #
# Direct tests for deidentify_dataset (pure function, parametrized over tags)
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("attr,expected", [
    ("PatientName",           REDACTED),
    ("ReferringPhysicianName", REDACTED),
    ("AccessionNumber",       "REDACTED 4 XNAT"),
    ("StudyID",               "REDACTED 4 XNAT"),
])
def test_deidentify_dataset_scrubs_tag(attr, expected):
    """deidentify_dataset scrubs each PHI tag to the correct placeholder value."""
    ds = make_phi_dicom_dataset()
    deidentify_dataset(ds, REDACTED)
    assert str(getattr(ds, attr)) == expected


def test_deidentify_dataset_removes_private_tags():
    """deidentify_dataset removes all private tags."""
    ds = make_phi_dicom_dataset()
    deidentify_dataset(ds, REDACTED)
    private_tags = [el for el in ds if el.tag.is_private]
    assert private_tags == []


def test_deidentify_dataset_removes_overlay():
    """deidentify_dataset removes overlay group (0x6000, 0x3000)."""
    ds = make_phi_dicom_dataset()
    deidentify_dataset(ds, REDACTED)
    assert (0x6000, 0x3000) not in ds


def test_deidentify_dataset_removes_curve_group():
    """deidentify_dataset removes curve group (0x5000 family)."""
    ds = make_phi_dicom_dataset()
    deidentify_dataset(ds, REDACTED)
    assert (0x5000, 0x0005) not in ds


def test_deidentify_dataset_preserves_pixel_data():
    """deidentify_dataset must not destroy the actual image pixels."""
    ds = make_phi_dicom_dataset()
    pixel_before = bytes(ds.PixelData)
    deidentify_dataset(ds, REDACTED)
    assert bytes(ds.PixelData) == pixel_before
