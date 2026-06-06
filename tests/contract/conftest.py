"""
tests/contract/conftest.py — Fixtures for contract-test framework.

Provides:
- fake_xnat: FakeXNAT with fidelity_mode=True for behavioral parity testing.
- real_xnat: pytest.skip() scaffold for deferred dual-run phase (requires Docker XNAT).
- @pytest.mark.contract: marker for contract tests.
- Synthetic data factories as fixtures for easy DICOM generation.
"""
from __future__ import annotations

import zipfile
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional

import pytest

from tests.fakes.fake_xnat import FakeXNAT
from tests import synthetic_data


# ---------------------------------------------------------------------------
# Marker registration
# ---------------------------------------------------------------------------

def pytest_configure(config):
    """Register @pytest.mark.contract marker."""
    config.addinivalue_line(
        "markers",
        "contract: contract test ensuring FakeXNAT ≈ real XNAT behavioral parity (Phase 6)."
    )


# ---------------------------------------------------------------------------
# FakeXNAT fixture (fidelity mode enabled)
# ---------------------------------------------------------------------------

@pytest.fixture
def fake_xnat() -> FakeXNAT:
    """
    FakeXNAT with fidelity_mode=True for realistic pyxnat behavior reproduction.

    Used throughout Stage 2 test cases (T001–T007) to assert behavioral parity
    with real XNAT.
    """
    return FakeXNAT(project_name="TEST_PROJECT", fidelity_mode=True)


# ---------------------------------------------------------------------------
# Real XNAT fixture (scaffolded, skip-marked for Stage 1)
# ---------------------------------------------------------------------------

@pytest.fixture
def real_xnat(request):
    """
    Scaffold for dual-run phase (deferred).

    Currently skip-marked because Stage 1 tests run against FakeXNAT only.
    Stage 2 (Phase 6 dual-run) will populate this with a Docker XNAT instance.

    Parametrization:
      test_workflow_contract.py uses @pytest.mark.parametrize((fake_xnat, real_xnat))
      so real_xnat-parameterized runs cleanly skip with this reason.
    """
    pytest.skip(
        "real-XNAT dual-run deferred — requires Docker XNAT (see contract-test.md Stage 1). "
        "Stage 2 (Phase 6) will implement this with xnat_local/ image."
    )


# ---------------------------------------------------------------------------
# Synthetic data generators as fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def synthetic_rf_dicom(tmp_path: Path) -> Path:
    """Generate a minimal synthetic RF DICOM file."""
    dcm_path = tmp_path / "test_rf.dcm"
    return synthetic_data.make_synthetic_dicom(dcm_path, rows=16, cols=16, seed=0)


@pytest.fixture
def synthetic_rf_dicom_missing_instance_number(tmp_path: Path) -> Path:
    """
    Generate an RF DICOM with no InstanceNumber tag (for T007 missing-tag guard test).

    This requires custom generation since make_synthetic_dicom always includes all tags.
    """
    import pydicom
    from pydicom.dataset import FileDataset, FileMetaDataset
    from pydicom.uid import (
        ExplicitVRLittleEndian,
        SecondaryCaptureImageStorage,
        generate_uid,
    )
    import numpy as np

    file_meta = FileMetaDataset()
    file_meta.MediaStorageSOPClassUID = SecondaryCaptureImageStorage
    file_meta.MediaStorageSOPInstanceUID = generate_uid()
    file_meta.TransferSyntaxUID = ExplicitVRLittleEndian
    file_meta.ImplementationClassUID = generate_uid()

    ds = FileDataset(None, {}, file_meta=file_meta, preamble=b"\0" * 128)
    ds.SOPClassUID = SecondaryCaptureImageStorage
    ds.SOPInstanceUID = file_meta.MediaStorageSOPInstanceUID
    ds.Modality = "RF"
    ds.ContentDate = "20240101"
    ds.ContentTime = "120000"

    # Intentionally omit InstanceNumber

    # Pixel data
    rng = np.random.default_rng(0)
    arr = rng.integers(0, 4096, size=(16, 16), dtype=np.uint16)
    ds.Rows, ds.Columns = 16, 16
    ds.SamplesPerPixel = 1
    ds.PhotometricInterpretation = "MONOCHROME2"
    ds.BitsAllocated = 16
    ds.BitsStored = 12
    ds.HighBit = 11
    ds.PixelRepresentation = 0
    ds.PixelData = arr.tobytes()

    dcm_path = tmp_path / "test_rf_no_instance.dcm"
    dcm_path.parent.mkdir(parents=True, exist_ok=True)
    ds.save_as(str(dcm_path))
    return dcm_path


@pytest.fixture
def synthetic_ct_dicom(tmp_path: Path) -> Path:
    """Generate a synthetic CT DICOM file (for mixed-modality test T005)."""
    # Reuse make_synthetic_dicom but change modality
    import pydicom
    from pydicom.dataset import FileDataset, FileMetaDataset
    from pydicom.uid import (
        ExplicitVRLittleEndian,
        CTImageStorage,
        generate_uid,
    )
    import numpy as np

    file_meta = FileMetaDataset()
    file_meta.MediaStorageSOPClassUID = CTImageStorage
    file_meta.MediaStorageSOPInstanceUID = generate_uid()
    file_meta.TransferSyntaxUID = ExplicitVRLittleEndian
    file_meta.ImplementationClassUID = generate_uid()

    ds = FileDataset(None, {}, file_meta=file_meta, preamble=b"\0" * 128)
    ds.SOPClassUID = CTImageStorage
    ds.SOPInstanceUID = file_meta.MediaStorageSOPInstanceUID
    ds.Modality = "CT"
    ds.ContentDate = "20240101"
    ds.ContentTime = "120000"
    ds.InstanceNumber = 1

    rng = np.random.default_rng(1)
    arr = rng.integers(0, 4096, size=(16, 16), dtype=np.uint16)
    ds.Rows, ds.Columns = 16, 16
    ds.SamplesPerPixel = 1
    ds.PhotometricInterpretation = "MONOCHROME2"
    ds.BitsAllocated = 16
    ds.BitsStored = 12
    ds.HighBit = 11
    ds.PixelRepresentation = 0
    ds.PixelData = arr.tobytes()

    dcm_path = tmp_path / "test_ct.dcm"
    dcm_path.parent.mkdir(parents=True, exist_ok=True)
    ds.save_as(str(dcm_path))
    return dcm_path


@pytest.fixture
def fake_zip_file(tmp_path: Path) -> tuple[Path, Dict[str, Any]]:
    """
    Create a zip file with a single placeholder DICOM and return (path, zipped_data_dict).

    zipped_data_dict is the format expected by publish_to_xnat:
      {str(zip_path): {"CONTENT": "IMAGE", "FORMAT": "DICOM", "TAG": "INTRA_OP"}}
    """
    zip_path = tmp_path / "dicom_files.zip"
    with zipfile.ZipFile(str(zip_path), "w") as zf:
        zf.writestr("placeholder.dcm", b"FAKE")
    zipped_data = {str(zip_path): {"CONTENT": "IMAGE", "FORMAT": "DICOM", "TAG": "INTRA_OP"}}
    return zip_path, zipped_data


@pytest.fixture
def multi_file_zip(tmp_path: Path, synthetic_rf_dicom: Path) -> tuple[Path, Dict[str, Any]]:
    """
    Create a zip with multiple real DICOM files (for multi-file resource tests).
    """
    zip_path = tmp_path / "multi_dicom_files.zip"
    with zipfile.ZipFile(str(zip_path), "w") as zf:
        # Add the synthetic DICOM multiple times with different names
        dcm_bytes = synthetic_rf_dicom.read_bytes()
        for i in range(5):
            zf.writestr(f"frame_{i:03d}.dcm", dcm_bytes)
    zipped_data = {str(zip_path): {"CONTENT": "IMAGE", "FORMAT": "DICOM", "TAG": "INTRA_OP"}}
    return zip_path, zipped_data


# ---------------------------------------------------------------------------
# Connection/form helpers (reused from test_publish_real_contract.py)
# ---------------------------------------------------------------------------

@pytest.fixture
def xnat_connection(fake_xnat: FakeXNAT) -> SimpleNamespace:
    """XNAT connection stub for publish_to_xnat."""
    return SimpleNamespace(server=fake_xnat, gateway=fake_xnat, xnat_project_name="TEST_PROJECT")


@pytest.fixture
def xnat_login() -> SimpleNamespace:
    """XNAT login stub for publish_to_xnat."""
    return SimpleNamespace(validated_username="testuser")


@pytest.fixture
def intake_form(tmp_path: Path) -> SimpleNamespace:
    """Intake form stub for publish_to_xnat (minimal)."""
    saved_ffn = tmp_path / "RECONSTRUCTED_OR_DATA_INTAKE_FORM.json"
    saved_ffn.write_text("{}", encoding="utf-8")
    dt_stub = SimpleNamespace(date="2024-01-01", time="120000")
    form = SimpleNamespace(
        uid="TEST_UID_001",
        group="TEST_GROUP",
        acquisition_site="TEST_SITE",
        ortho_procedure_type="TEST_PROCEDURE",
        scan_quality="usable",
        datetime=dt_stub,
        relevant_folder=tmp_path,
        saved_ffn=saved_ffn,
        saved_ffn_str=str(saved_ffn),
    )

    def push_to_xnat(subj_inst, verbose=False):
        subj_inst.resource("INTAKE_FORM").file("form.json").insert(
            "{}", content="TEXT", format="JSON", tags="DOC"
        )

    form.push_to_xnat = push_to_xnat
    return form
